#!/usr/bin/env python
# coding: utf-8

# ## nb_eso1_gold_fact_sales_order_freight
#
# **Gold `fact_sales_order_freight` processor** for Extended Sales Order 1 (Billable v
# Payable Freight). Builds and continuously refreshes ONE table —
# `lh_jde_gold.rpt.fact_sales_order_freight` (order-line grain; freight denormalized) —
# from the Silver sales-order detail (F4211) and freight-audit (F4981) Change Data Feed
# streams. Split out of nb_eso1_gold_streaming so the fact and `dim_item` run as
# independent jobs (own table, own checkpoint root, own OVERWRITE switch). Relates to the
# REUSED dims (`rpt.dim_address_book` role views, `rpt.dim_plant`) and to `dim_item`
# (built by nb_eso1_gold_dim_item) — those are NOT touched here.
#
# Flow: constants → transforms → reused-dim preflight → seed-if-missing →
#       start CDF streams (foreachBatch MERGE) → refresh every 30 seconds.
#
# Streaming model mirrors nb_silver_to_gold_eso7_v2 (CDF incremental):
#   • continuous trigger(processingTime="30 seconds") + awaitAnyTermination
#   • per-source foreachBatch handler factories make_fact_*_handler(init_ver): skip the
#     seed rows (_commit_version <= init_ver), then act only on real change rows
#     (_change_type insert/update_postimage/delete) and CDC-write to Gold (fact = delete
#     the affected order scope, then APPEND freshly recomputed lines)
#   • checkpoint namespaced per ENV; startingVersion knob
# Design: docs/ESO1_gold_layer_design.md


# In[1]:


# =============================================================================
# CONFIG + CONSTANTS  (names per Fabric_Naming_Convention_Guidelines.pdf)
# =============================================================================
import threading
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

GOLD_SCHEMA = "lh_jde_gold.rpt"
RPT_SCHEMA  = "lh_jde_gold.rpt"

# ── refresh / runtime config (CDF concept adopted from nb_silver_to_gold_eso7_v2) ──
ENV             = "dev"                              # checkpoint namespacing — envs never collide
TRIGGER         = {"processingTime": "30 seconds"}   # ← continuous; refresh every 30 s
CKPT            = f"Files/checkpoints/eso1_fact_{ENV}"  # OWN root — independent of the dim_item notebook
# ── manual reprocess switch (== ESO7 v2 MANUAL_OVERWRITE) ─────────────────────
#   OVERWRITE = True  -> full load: drop + rebuild the fact from the full Silver snapshot,
#                        snapshot each source's Delta version as init_ver, clear checkpoints.
#   OVERWRITE = False -> resume: keep the table + checkpoints, streams catch up from where
#                        they left off (init_ver = -1, no version filtering).
OVERWRITE       = True    # ⚠ ONE-OFF full reprocess (cdf schema) — set back to False after this run

# Serialises all fact-table writes: foreachBatch handlers run in separate driver
# threads, so this lock makes the F4211 and F4981 streams take turns (delete+append
# on the same Gold fact never overlaps). (Pattern from ESO7 v2.)
_FACT_LOCK      = threading.Lock()

# ── Silver sources ────────────────────────────────────────────────────────────
SILVER_SCHEMA   = "jde" # Silver schema for ESO1 — the F4211/F4981[/F42119] CDF sources are
                         #   exposed here by the user (table properties). `jde` (2026-07-26, was `jde_cdc`)
                         #   (2026-07-22) — the same schema ESO4/ESO5 read from lh_jde_silver
                         #   (was `cdf`). CDF must be enabled on every STREAMED source read below.
SRC_LAKEHOUSE = "lh_jde_silver"
F4211_TBL    = "f4211_sales_order_detail_file"
F4201_TBL    = "f4201_sales_order_header_file"
F0101_TBL    = "f0101_address_book_master"
F4101_TBL    = "f4101_item_master"
F41002_TBL   = "f41002_item_units_of_measure_conversion_factors"
# F41003 (standard UoM conversion) is no longer sourced here — the standard-UoM fallback is
# served by the reused Gold dim lh_jde_gold.eso7.dim_uom_conversion (from_uom -> std_factor),
# built/maintained by nb_silver_to_gold_dim_f41003.py, and applied as the Tier-B leg of the
# Total Tons DAX measure via RELATED. Matches the ESO7 v2 fact approach.
F4074_TBL    = "f4074_price_adjustment_ledger_file"
F4981_TBL    = "f4981_freight_audit_history"
F5642B01_TBL = "f5642b01_custom_sales_order_entry_screen_header"
F5642B11_TBL = "f5642b11_custom_sales_order_entry_screen_detail"
F4941_TBL    = "f4941_shipment_routing_steps"
# ── ADDED 2026-07-22: residual-gap sources (all confirmed present in full_metadata.json — see the
#    query-to-notebook gap analysis M1/M4/M5). These close every structural gap in the notebook. ──
F4106_TBL    = "f4106_item_base_price_file"           # M1 — item base price (has_effective_price flag)
F5549002_TBL = "f5549002_mxp_bol_interface_detail"    # M5 — BOL interface weigh-ticket weights
F03012_TBL   = "f03012_customer_master_by_line_of_business"  # sold-to LOB category (Mak AIAC05='E26')
F49211_TBL   = "f49211_sales_order_detail_file_tag_file"     # SO-line tag file (SOP0006/000x UDDEFF deferred flag)
# ── ADDED 2026-07-15 for the ESO1 Filter Capture variations (page-level filtering) ──
F0116_TBL    = "f0116_address_by_date"                # ship-to postal address (effective-dated; static)
F42119_TBL   = "f42119_sales_order_history_file"      # Sales Order History — CONFIRMED via full_metadata.json
                                                      # (table_name=sales_order_history_file, identical 268-col
                                                      # SD* schema to F4211). The ~40 open-order variations
                                                      # UNION ALL F42119 with F4211 for closed/purged lines.
                                                      # Still guarded by _load_optional so the notebook runs
                                                      # whether or not the table is ingested to Silver yet.

# ── Gold target BUILT here (new, rpt) ──────────────────────────────────────────
T_FACT      = f"{GOLD_SCHEMA}.fact_sales_order_freight"

# ── REUSED dims (read-only; owned by old_nb jobs) ─────────────────────────────
R_DIM_AB      = f"{RPT_SCHEMA}.dim_address_book"
R_DIM_PLANT   = f"{RPT_SCHEMA}.dim_plant"
R_DIM_SHIP_TO = f"{RPT_SCHEMA}.dim_address_ship_to"
R_DIM_SOLD_TO = f"{RPT_SCHEMA}.dim_address_sold_to"
R_DIM_CARRIER = f"{RPT_SCHEMA}.dim_address_carrier"
R_DIM_PARENT = f"{RPT_SCHEMA}.dim_address_parent"

# ── report scaling (business WHERE filters removed — fact now carries ALL rows) ──
# (former Hubble hard filters COMPANIES / ALAST_WHITELIST / ship-to address-type gate /
#  line_type='S' / status<>980 / vendor_invoice were dropped per the no-filter requirement.)
SHIFT_FACTOR    = 1.0    # placeholder — see design §11 (ShiftFactor open item)

print(f"ESO1 Gold fact processor — trigger {TRIGGER}  target {T_FACT}")


# In[2]:


# =============================================================================
# HELPERS
# =============================================================================
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def sname(table_name):
    """Fully-qualified Silver source name (== ESO7 v2 sname)."""
    return f"{SRC_LAKEHOUSE}.{SILVER_SCHEMA}.{table_name}"

def _load_optional(table_name):
    """Load a Silver table only if it exists; else warn and return None. Used for F42119 (Sales Order
    History) — its Silver name f42119_sales_order_history_file is CONFIRMED in full_metadata.json, but the
    table may not be ingested to Silver yet; the guard lets the fact run with OR without it, unioning the
    closed/purged history rows when present and running F4211-only otherwise."""
    try:
        if spark.catalog.tableExists(sname(table_name)):
            return load_silver_table(table_name)
    except Exception as e:
        print(f"  ⚠ optional source check failed for {sname(table_name)}: {e}")
    print(f"  ⚠ optional source not found, skipping union: {sname(table_name)}")
    return None

def load_silver_table(table_name):
    df = spark.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

# ── date hygiene ──────────────────────────────────────────────────────────────
# JDE carries junk/sentinel dates (e.g. 1952-12-31 zero-dates, 2824-08-29 corrupt
# Julian values). With NO date dimension, these would otherwise appear as selectable
# values in raw-date slicers. Null anything outside a plausible business window so the
# stored raw dates (and any derived buckets/keys) stay clean.
VALID_DATE_LO     = "2000-01-01"   # fixed lower bound
VALID_YEARS_AHEAD = 25             # upper bound = Dec 31 of (current year + 25); self-extends each run

# every raw date column stored on the fact (guarded by clean_date, then used to derive
# ship_year_week + the *_date_key ints)
_RAW_DATE_COLS = [
    "order_date", "requested_date", "scheduled_pick_date", "promised_ship_date",
    "actual_ship_date", "gl_date", "invoice_date", "cancel_date",
    "line_price_effective_date", "header_price_effective_date",
    "date_earliest_pickup", "date_latest_delivery",
]

def clean_date(col):
    """Null out implausible sentinel/corrupt dates (outside the valid business window:
    VALID_DATE_LO .. Dec 31 of current year + VALID_YEARS_AHEAD — the upper bound
    self-extends each run so future promised/delivery dates are never clipped)."""
    hi = F.make_date(F.year(F.current_date()) + F.lit(VALID_YEARS_AHEAD), F.lit(12), F.lit(31))
    return F.when(col.between(F.lit(VALID_DATE_LO).cast("date"), hi), col)

def date_key(col):
    return F.when(col.isNotNull(), F.date_format(col, "yyyyMMdd").cast("int"))

def sk(*cols):
    return F.sha2(F.concat_ws("||", *[F.col(c).cast("string") if isinstance(c, str) else c.cast("string")
                                       for c in cols]), 256)

def current_version(silver_table):
    """Latest committed Delta version of a Silver source (for init_ver seed-skip)."""
    return spark.sql(f"DESCRIBE HISTORY {sname(silver_table)}").select(F.max("version")).first()[0]

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return (c, True)
    return (candidates[0], False)

def _exists(fqn):
    try:
        if spark.catalog.tableExists(fqn):
            return True
    except Exception:
        pass
    try:
        spark.read.table(fqn).limit(1).collect()
        return True
    except Exception:
        return False


# In[3]:


# =============================================================================
# CDC WRITE HELPER — NO audit columns (CDF concept from nb_silver_to_gold_eso7_v2)
#   Gold table stores business columns only — no record_hash / is_deleted /
#   source_commit_timestamp / gold_updated_timestamp.
#   • fact : delete the affected order scope, then APPEND freshly recomputed lines
#            (handles insert/update/delete uniformly); writes serialised by _FACT_LOCK.
# =============================================================================
def _write_new_table(df, target, cdf=True):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if cdf:
        w = w.option("delta.enableChangeDataFeed", "true")   # Gold CDF on for downstream
    w.saveAsTable(target)

def recompute_fact(orders):
    """CDC for the fact: delete the affected order scope then append the recomputed
    lines (== ESO7 v2 recompute_fact). `orders` = distinct
    company_key_order_no/order_type/order_number. Returns rows written."""
    if orders.rdd.isEmpty():
        return 0
    src = transform_fact(restrict_orders=orders)
    with _FACT_LOCK:
        if not spark.catalog.tableExists(T_FACT):
            _write_new_table(src, T_FACT)
            return src.count()
        scope = orders.select(sk("company_key_order_no", "order_type", "order_number")
                              .alias("order_scope_key")).distinct()
        (DeltaTable.forName(spark, T_FACT).alias("t")
            .merge(scope.alias("s"), "t.order_scope_key = s.order_scope_key")
            .whenMatchedDelete().execute())               # drop the order's old lines
        src.write.format("delta").mode("append").saveAsTable(T_FACT)   # append current lines
    return src.count()


# In[4]:


# =============================================================================
# UoM -> TN cascades + freight buckets (F4981 -> shipment grain)
# =============================================================================
def build_uom_cascades():
    # ESO7 v2 approach: item-specific F41002 conversion ONLY (fwd + reciprocal rev union).
    # The F41003 standard-UoM fallback is served downstream by the reused dim_uom_conversion
    # dim (built by nb_silver_to_gold_dim_f41003.py) via DAX RELATED, so it is intentionally
    # not cascaded here.
    f41002 = load_silver_table(F41002_TBL)
    item_fwd = (f41002.filter((F.trim("related_uom") == "TN") & (F.col("conversion_factor") != 0))
                .select(F.col("identifier_short_item").alias("itm"), F.trim("uom").alias("from_uom"),
                        F.col("conversion_factor").cast("double").alias("conv_factor")))
    item_rev = (f41002.filter((F.trim("uom") == "TN") & (F.col("conversion_factor") != 0))
                .select(F.col("identifier_short_item").alias("itm"), F.trim("related_uom").alias("from_uom"),
                        (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_factor")))
    return item_fwd.unionByName(item_rev)

def transform_freight_buckets(restrict_ship=None):
    f4981 = load_silver_table(F4981_TBL)
    if restrict_ship is not None:
        f4981 = f4981.join(restrict_ship.alias("r"),
                           f4981["shipment_number"] == F.col("r.shipment_number"), "left_semi")
    fr = f4981   # no vendor-invoice filter — all freight-audit rows included
    bp, cgc, amt = F.trim("billable_payable"), F.trim("charge_code_01"), F.col("net_amount")
    # freight location (FHCTY1/FHADDS/FHADDZ) denormalized at shipment grain (first non-null)
    return (fr.groupBy("shipment_number").agg(
                F.round(F.sum(F.when((bp == "B") & (cgc == "BFR"),            amt).otherwise(0.0)), 2).alias("billable_freight"),
                F.round(F.sum(F.when((bp == "B") & (cgc.isin("FSC", "FSB")), amt).otherwise(0.0)), 2).alias("billable_fuel"),
                F.round(F.sum(F.when((bp == "P") & (cgc == "PFR"),            amt).otherwise(0.0)), 2).alias("payable_freight"),
                F.round(F.sum(F.when((bp == "P") & (cgc == "FSC"),            amt).otherwise(0.0)), 2).alias("payable_fuel"),
                # total_freight = ALL F4981 net_amount for the shipment, regardless of billable_payable / charge_code
                # (H2 fix — gap analysis 2026-07-22): the combined-freight reports (Baseline Finance, DE Orders, BP
                # Freight) sum the whole shipment's FHNAMT; the billable/payable buckets above UNDER-count that total if
                # any charge code falls outside {BFR,FSC,FSB,PFR}. Denormalized at shipment grain like the buckets —
                # dedup in DAX with SUMX(VALUES(shipment_number), CALCULATE(MAX(total_freight))), never a raw SUM.
                F.round(F.sum(amt), 2).alias("total_freight"),
                F.first(F.trim("city"), ignorenulls=True).alias("freight_city"),
                F.first(F.trim("state"), ignorenulls=True).alias("freight_state"),
                F.first(F.trim("zip_code_postal"), ignorenulls=True).alias("freight_zip"))
            .withColumn("total_billable",   F.round(F.col("billable_freight") + F.col("billable_fuel"), 2))
            .withColumn("total_payable",    F.round(F.col("payable_freight")  + F.col("payable_fuel"), 2))
            .withColumn("freight_variance", F.round(F.col("billable_freight") - F.col("payable_freight"), 2))
            .withColumn("total_variance",   F.round(F.col("total_billable")   - F.col("total_payable"), 2))
            .withColumn("shift_factor_applied", F.lit(SHIFT_FACTOR).cast("double")))


# In[5]:


# =============================================================================
# FACT  fact_sales_order_freight  (order-line grain; freight denormalized)
# =============================================================================
FACT_BUSINESS_COLS = [
    # ── degenerate / order identifiers ──
    "company", "company_key_order_no", "order_type", "order_number", "line_number",
    "shipment_number", "bol_number", "invoice_number",
    "original_document_type", "original_po_so_number", "original_document_no",
    "reference_01", "user_reserved_reference",
    # ── status / handling / transport ──
    "hold_orders_code", "status_code_last", "status_code_next", "next_status_num",
    "freight_handling_code", "freight_handling_code_audit",
    "mode_of_transport", "route_number", "container_id", "transaction_originator",
    "delivery_instruct_line_01", "delivery_instruct_line_02", "gl_class",
    "sales_reporting_code_01", "sales_reporting_code_03",
    # ── date keys (retained but unused — no date dimension; slice raw dates) ──
    "order_date_key", "requested_date_key", "scheduled_pick_date_key",
    "promised_ship_date_key", "ship_date_key", "gl_date_key", "invoice_date_key",
    "cancel_date_key", "line_price_effective_date_key", "header_price_effective_date_key",
    "earliest_pickup_date_key", "latest_delivery_date_key",
    # ── address / dimension FKs ──
    "ship_to", "bill_to", "carrier_number", "address_number_parent",
    "item_number_short", "branch_plant",
    # ── raw event dates (sentinels nulled via clean_date; sliced directly — NO date dim) ──
    "order_date", "requested_date", "scheduled_pick_date", "promised_ship_date",
    "actual_ship_date", "gl_date", "invoice_date", "cancel_date",
    "line_price_effective_date", "header_price_effective_date",
    "date_earliest_pickup", "date_latest_delivery",
    # weekly grouping bucket (Mon–Sun ISO week label off actual_ship_date) — replaces
    # dim_date[year_week] now that there is no date dimension
    "ship_year_week",
    # ── item / uom ──
    "second_item_number", "line_type", "item_name", "uom", "uom_primary", "uom_pricing",
    "conversion_to_tons_rate", "missing_conversion_flag",
    # ── filter-only attributes (denormalized for Power BI slicers; per
    #    "Filter Selections and Report Locations (OTC)" — ESO1 sheet) ──
    "price_adjustment_type", "standard_industry_code", "category_code_05", "category_code_14",
    "search_type", "uom_structure", "payment_terms", "item_segment_04",
    # ── measures / numerics (line grain) ──
    "quantity_shipped", "quantity_shipped_tons", "primary_quantity_ordered",
    "transaction_quantity", "price_per_unit", "price_quantity_shipped",
    "major_prod_code", "minor_prod_code", "freight_factor_value",
    # ── denormalized booking / ocean (shipment grain) ──
    "seal_no", "booking_no", "destination_port",
    "no_of_container", "ocean_del_terms", "vessel_name",
    # ── denormalized freight location + buckets (shipment grain) ──
    "freight_city", "freight_state", "freight_zip",
    "billable_freight", "billable_fuel", "total_billable",
    "payable_freight", "payable_fuel", "total_payable", "total_freight",
    "freight_variance", "total_variance", "shift_factor_applied",
    "is_primary_shipment_line",
    # ── ADDED 2026-07-15 for the ESO1 Filter Capture variations (page-level filtering; README §3) ──
    "extended_price", "extended_cost", "currency_code",              # SDAEXP / SDECST / SDBCRC (F4211)
    "backorder_qty", "cancelled_qty", "qty_to_date", "open_qty",     # SDSOBK / SDSOCN / SDQTYT / SDUOPN
    "line_description_1", "line_description_2", "date_updated",       # SDDSC1 / SDDSC2 / SDUPMJ (line, not item)
    "address_rate",                                                  # ABURAT — ship-to F0101 rate
    "sold_to_name", "sold_to_search_type",                          # ABALPH / ABAT1 — sold-to F0101 (SDAN8)
    "sold_to_category_05", "sold_to_category_10",                   # ABAC05 / ABAC10 — sold-to F0101
    "ship_to_city", "ship_to_state", "ship_to_zip",                 # F0116 latest-effective ship-to address
    "ship_to_address_1", "ship_to_address_2", "ship_to_country",
    # ── ADDED 2026-07-22: residual-gap columns (M1–M5 + deferred display; sources verified in full_metadata.json) ──
    "zone_number", "line_hold",                                     # M2 SDZON / M3 SDHOLD (LINE hold ≠ header hold_orders_code)
    "is_ocean_route", "route_container_count",                      # M4 F4941 RSMOT='OCE' flag / SUM(RSNCTR)
    "gross_weight", "catch_weight", "max_weight",                   # M5 F5549002 BOL weigh-ticket weights (MIGRWT/MICTWT/MIMXWT)
    "has_effective_price",                                          # M1 F4106 base-price existence (Zero-Price Branch B)
    "pull_signal", "reference_02", "reference_03", "vendor_number", # deferred F4211 display (SDPSIG/SDVR02/SDVR03/SDVEND)
    "price_adjustment_schedule", "user_reserved_code", "price_override_code",  # SDASN / SDURCD / SDPROV
    "user_id", "lot_number", "serial_number", "location", "sales_reporting_code_05",  # SDUSER/SDLOTN/SDSERN/SDLOCN/SDSRP5
    "sold_to_lob_category_05", "deferred_entries_flag",             # F03012 AIAC05 (sold-to LOB) / F49211 UDDEFF (SO-tag)
    # ── remaining niche display source-columns (2026-07-22) ──
    "related_po_so_number", "time_of_day", "original_promised_date", "related_address_3",  # SDRORN/SDTDAY/SHOPDJ/ABAN83
    "adj_gl_class", "adj_based_on_value", "adj_uom", "adj_factor_value",                    # F4074 ALGLC/ALBSDVAL/ALUOM/ALFVTR
    "voyage_number", "loading_port", "ocean_carrier",                                       # F5642B01 ocean-booking
    "booking_reference_1", "booking_reference_2", "booking_reference_3", "date_latest_pickup",
]
# NOTE: gl_class, delivery_instruct_line_01/02 were ALREADY present; user_reserved_number (SDURAB) is
# already surfaced as `bol_number`. SDUORG/SDPQOR/SDSOQS = transaction_quantity/primary_quantity_ordered/
# quantity_shipped (all present). See ESO1 Filter Capture/README.md §3-E.

def _add_effective_price_flag(df):
    """M1 — has_effective_price: 'Y' when an F4106 item-base-price row EXISTS for the line's
    (second_item_number, branch_plant, ship_to) with a non-zero price whose effective window covers the
    line's actual_ship_date. This is the inverse of Hubble's Orders-with-Zero-Unit-Price Branch-B
    `NOT EXISTS` (F4106 keyed BPLITM=SDLITM, BPMCU=SDMCU, BPAN8=SDSHAN, BPEFTJ<=SDADDJ, BPEXDJ>=SDADDJ,
    BPUPRC<>0). A left_semi to the distinct line key keeps the line grain (one flag per line, no fan-out)."""
    f4106 = load_silver_table(F4106_TBL)
    bp = (f4106.filter(F.col("amt_price_per_unit_02") != 0)
          .select(F.trim(F.col("identifier_2nd_item")).alias("bp_item2"),   # BPLITM (2nd item = SDLITM)
                  F.trim(F.col("cost_center")).alias("bp_plant"),           # BPMCU
                  F.col("address_number").alias("bp_an8"),                  # BPAN8 (ship-to)
                  F.col("date_effective_julian_01").alias("bp_eff"),        # BPEFTJ (decoded to date in Silver)
                  F.col("date_expired_julian_01").alias("bp_exp")))         # BPEXDJ
    matched = (df.select("sales_order_line_key", "second_item_number", "branch_plant",
                         "ship_to", "actual_ship_date")
               .join(bp,
                     (F.trim(F.col("second_item_number")) == F.col("bp_item2")) &
                     (F.col("branch_plant") == F.col("bp_plant")) &
                     (F.col("ship_to") == F.col("bp_an8")) &
                     (F.col("bp_eff") <= F.col("actual_ship_date")) &
                     (F.col("bp_exp") >= F.col("actual_ship_date")), "left_semi")
               .select("sales_order_line_key").distinct()
               .withColumn("has_effective_price", F.lit("Y")))
    return (df.join(matched, "sales_order_line_key", "left")
              .withColumn("has_effective_price", F.coalesce(F.col("has_effective_price"), F.lit("N"))))

def transform_fact(restrict_orders=None):
    f4211 = load_silver_table(F4211_TBL)
    # UNION ALL F4211 (live) + F42119 (Sales Order History) exactly as the ~40 open-order variations do,
    # so closed/purged lines are included. Guarded: if F42119 is absent from Silver, the fact is F4211
    # only. Same SD* schema; allowMissingColumns tolerates minor schema drift. Any live/history overlap
    # is collapsed by the final dropDuplicates(["sales_order_line_key"]).  (user direction 2026-07-15)
    _hist = _load_optional(F42119_TBL)
    if _hist is not None:
        # F42119 snake-names SDLITM as `identifier_2nd_item` (≠ F4211 `identifier_second_item`), so an
        # unqualified unionByName would NULL second_item_number for every history row — item2 is a heavily
        # used filter (item2<>'MISC BILLING', Ottowa whitelists), so rename to match before the union.
        if "identifier_2nd_item" in _hist.columns and "identifier_second_item" not in _hist.columns:
            _hist = _hist.withColumnRenamed("identifier_2nd_item", "identifier_second_item")
        f4211 = f4211.unionByName(_hist, allowMissingColumns=True)
    f4201 = load_silver_table(F4201_TBL)
    f0101 = load_silver_table(F0101_TBL); f4101 = load_silver_table(F4101_TBL)
    adr   = load_silver_table(F0116_TBL)                       # F0116 ship-to postal address (effective-dated)
    b01   = load_silver_table(F5642B01_TBL); b11 = load_silver_table(F5642B11_TBL)
    f4074 = load_silver_table(F4074_TBL); f4941 = load_silver_table(F4941_TBL)
    f41002 = load_silver_table(F41002_TBL)
    f5549 = load_silver_table(F5549002_TBL)                    # M5 — BOL interface weigh-ticket weights
    f03012 = load_silver_table(F03012_TBL); f49211 = load_silver_table(F49211_TBL)  # sold-to LOB cat / SO-line tag flag
    conv_item = build_uom_cascades()

    gl_name, gl_present = pick_col(f4211, ["dt_for_gl_and_vouch_01",   # F4211.SDDGL in Silver (verified vs full_metadata)
        "date_for_g_l_julian", "date_g_l_julian", "date_general_ledger_julian",
        "general_ledger_date", "date_for_g_land_voucher_julian", "gl_date"])
    gl_expr = F.col(f"sd.{gl_name}") if gl_present else F.lit(None).cast("date")

    sd = f4211   # business WHERE filters removed (company / line_type='S' / status<>980)
    if restrict_orders is not None:
        sd = sd.join(restrict_orders.alias("ro"),
                     (sd["company_key_order_no"] == F.col("ro.company_key_order_no")) &
                     (sd["order_type"] == F.col("ro.order_type")) &
                     (sd["document_order_invoice_e"] == F.col("ro.order_number")), "left_semi")

    # F4074 price-adjustment ledger — ONE actual row per order line (no GROUP BY /
    # aggregation): a deterministic row_number pick keeps a real ALAST/ALUPRC value while
    # guaranteeing the line grain, so the fact stays one row per sales_order_line_key.
    #   • price_adjustment_type -> ALAST as-is (e.g. "A03", "FRTHIDE")
    #   • freight_factor_value  -> ALUPRC as-is (report ReportColumn14)
    _alw = (Window.partitionBy("company_key_order_no", "document_order_invoice_e",
                               "order_type", "line_number")
            .orderBy(F.col("price_adjustment_type").asc_nulls_last()))
    f4074w = (f4074.select(
                  F.col("company_key_order_no"), F.col("document_order_invoice_e"),
                  F.col("order_type"), F.col("line_number"),
                  F.trim(F.col("price_adjustment_type")).alias("price_adjustment_type"),
                  F.col("amt_price_per_unit_02").cast("double").alias("freight_factor_value"),
                  F.col("gl_class").alias("adj_gl_class"),                   # ALGLC (SOP0007)
                  F.col("based_on_value").alias("adj_based_on_value"),       # ALBSDVAL (SOP0007/0008/0006)
                  F.trim(F.col("uom_as_input")).alias("adj_uom"),           # ALUOM (SOP0006/000x)
                  F.col("factor_value").alias("adj_factor_value"))           # ALFVTR (BP Freight)
              .withColumn("_alrn", F.row_number().over(_alw))
              .where(F.col("_alrn") == 1).drop("_alrn"))

    # F41002 UOM structure (UMUSTR) — read from the item's TN-conversion row, mirroring Hubble's
    # F41002 join (UMITM=SDITM AND UMRUM='TN' AND UMUM=SDUOM). Without the related_uom='TN' gate the
    # (item, uom) key matches multiple F41002 rows → an arbitrary/non-TN uom_structure + a pre-dedup
    # fan-out. Gate on TN and dedup to one row per (item, input-uom).
    uom_str = (f41002.filter(F.trim(F.col("related_uom")) == "TN")
               .select(F.col("identifier_short_item").alias("us_itm"),
                       F.trim(F.col("uom")).alias("us_uom"),
                       F.col("uom_structure").alias("uom_structure"))
               .dropDuplicates(["us_itm", "us_uom"]))
    # F4941 shipment routing → route_number + M4 ocean-mode / container count. is_ocean_route='Y' if ANY
    # routing step is OCE (04a Export / AP Minerals / Luhe filter RSMOT='OCE'); route_container_count =
    # SUM(RSNCTR) (04a). Kept as page-filterable attributes — no report filter applied.
    route = (f4941.groupBy("shipment_number").agg(
                 F.first("route_number", ignorenulls=True).alias("route_number"),
                 F.coalesce(F.max(F.when(F.trim(F.col("mode_of_transport")) == "OCE", F.lit("Y"))),
                            F.lit("N")).alias("is_ocean_route"),
                 F.round(F.sum(F.col("number_of_containers").cast("double")), 0).alias("route_container_count")))
    freight = transform_freight_buckets()

    # M5 — BOL weigh-ticket weights (F5549002) collapsed to ONE row per order line (gross/catch/max), so the
    # LEFT join can't fan the line grain out. Silver is pre-decoded, so no /10000 or /100 scaling.
    wt = (f5549.groupBy("company_key_order_no", "document_order_invoice_e", "order_type", "line_number")
             .agg(F.first("gross_weight",   ignorenulls=True).alias("gross_weight"),
                  F.first("catch_weight",   ignorenulls=True).alias("catch_weight"),
                  F.first("maximum_weight", ignorenulls=True).alias("max_weight")))

    # F03012 customer-master-by-LOB → sold-to LOB category (Mak joins F4211.SDAN8 = F03012.AIAN8). Collapse to one
    # row per address so the LEFT join can't fan the line grain out (a customer may have several LOB rows).
    lob = (f03012.groupBy("address_number")
              .agg(F.first("report_code_add_book_005", ignorenulls=True).alias("sold_to_lob_category_05")))
    # F49211 SO-detail tag file → deferred_entries_flag (SOP0006 / SOP000x); 1:1 with the line — collapse defensively.
    tag = (f49211.groupBy("company_key_order_no", "document_order_invoice_e", "order_type", "line_number")
              .agg(F.first("deferred_entries_flag", ignorenulls=True).alias("deferred_entries_flag")))

    # collapse booking sources to ONE row per join key so denormalizing seal/booking/ocean
    # attributes can't fan the line grain out (b11 = booking detail, b01 = booking header)
    b11d = (b11.groupBy("company_key_order_no", "document_order_invoice_e", "order_type",
                        "line_number", "shipment_number")
               .agg(F.first("seal_no", ignorenulls=True).alias("seal_no")))
    b01d = (b01.groupBy("company_key_order_no", "document_order_invoice_e", "order_type", "shipment_number")
               .agg(F.first("booking_no",           ignorenulls=True).alias("booking_no"),
                    F.first("destination_port",      ignorenulls=True).alias("destination_port"),
                    F.first("no_of_container",       ignorenulls=True).alias("no_of_container"),
                    F.first("ocean_del_terms",       ignorenulls=True).alias("ocean_del_terms"),
                    F.first("vessel_name",           ignorenulls=True).alias("vessel_name"),
                    F.first("date_earliest_pickup",  ignorenulls=True).alias("date_earliest_pickup"),
                    F.first("date_latest_delivery",  ignorenulls=True).alias("date_latest_delivery"),
                    # extended ocean-booking display (04a Export + export reports) — BA55VONO/LODP/OCCR/REF1-3/BADLPU
                    F.first("voyage_no",             ignorenulls=True).alias("voyage_number"),
                    F.first("loading_port",          ignorenulls=True).alias("loading_port"),
                    F.first("ocean_carrier",         ignorenulls=True).alias("ocean_carrier"),
                    F.first("reference_01",          ignorenulls=True).alias("booking_reference_1"),
                    F.first("reference_02",          ignorenulls=True).alias("booking_reference_2"),
                    F.first("reference_03",          ignorenulls=True).alias("booking_reference_3"),
                    F.first("date_latest_pickup",    ignorenulls=True).alias("date_latest_pickup")))

    # F0116 is effective-dated (many rows per address). Collapse to the LATEST-effective row per address
    # (ALEFTB desc) so the LEFT join to the line can't fan the grain out — same guard as b11d/b01d.
    _adrw = Window.partitionBy("address_number").orderBy(F.col("date_beginning_effective").desc_nulls_last())
    adrd = (adr.select("address_number", "city", "state", "zip_code_postal",
                       "address_line_01", "address_line_02", "country", "date_beginning_effective")
               .withColumn("_arn", F.row_number().over(_adrw)).where(F.col("_arn") == 1).drop("_arn"))

    j = (sd.alias("sd")
         .join(f4201.alias("sh"),
               (F.col("sd.company_key_order_no") == F.col("sh.company_key_order_no")) &
               (F.col("sd.document_order_invoice_e") == F.col("sh.document_order_invoice_e")) &
               (F.col("sd.order_type") == F.col("sh.order_type")), "inner")
         .join(b11d.alias("b11"),
               (F.col("sd.company_key_order_no") == F.col("b11.company_key_order_no")) &
               (F.col("sd.document_order_invoice_e") == F.col("b11.document_order_invoice_e")) &
               (F.col("sd.order_type") == F.col("b11.order_type")) &
               (F.col("sd.line_number") == F.col("b11.line_number")) &
               (F.col("sd.shipment_number") == F.col("b11.shipment_number")), "left")
         .join(b01d.alias("b01"),
               (F.col("sd.company_key_order_no") == F.col("b01.company_key_order_no")) &
               (F.col("sd.document_order_invoice_e") == F.col("b01.document_order_invoice_e")) &
               (F.col("sd.order_type") == F.col("b01.order_type")) &
               (F.col("sd.shipment_number") == F.col("b01.shipment_number")), "left")
         .join(f0101.alias("st"), F.col("st.address_number") == F.col("sd.address_number_ship_to"), "left")  # ship-to F0101 filter attrs (SIC / cat05 / cat14 / search type / rate)
         .join(f0101.alias("so"), F.col("so.address_number") == F.col("sd.address_number"), "left")  # sold-to F0101 (SDAN8): name / search-type / category (Days-Since-Invoice, SM Inside Sales, Mak, Orders-on-Hold)
         .join(f4101.alias("im"), F.col("sd.identifier_short_item") == F.col("im.identifier_short_item"), "left")
         .join(uom_str.alias("us"),
               (F.col("us.us_itm") == F.col("sd.identifier_short_item")) &
               (F.col("us.us_uom") == F.trim(F.col("sd.uom_as_input"))), "left")
         .join(conv_item.alias("ci"),
               (F.col("ci.itm") == F.col("sd.identifier_short_item")) &
               (F.col("ci.from_uom") == F.trim(F.col("sd.uom_as_input"))), "left")
         .join(f4074w.alias("al"),
               (F.col("al.company_key_order_no") == F.col("sd.company_key_order_no")) &
               (F.col("al.document_order_invoice_e") == F.col("sd.document_order_invoice_e")) &
               (F.col("al.order_type") == F.col("sd.order_type")) &
               (F.col("al.line_number") == F.col("sd.line_number")), "left")
         .join(route.alias("rt"), F.col("sd.shipment_number") == F.col("rt.shipment_number"), "left")
         .join(freight.alias("fr"), F.col("sd.shipment_number") == F.col("fr.shipment_number"), "left")
         .join(adrd.alias("adr"), F.col("adr.address_number") == F.col("sd.address_number_ship_to"), "left")  # F0116 ship-to postal address (latest-effective)
         .join(wt.alias("wt"),                                                                                # M5 — F5549002 BOL weigh-ticket weights (one row/line)
               (F.col("wt.company_key_order_no") == F.col("sd.company_key_order_no")) &
               (F.col("wt.document_order_invoice_e") == F.col("sd.document_order_invoice_e")) &
               (F.col("wt.order_type") == F.col("sd.order_type")) &
               (F.col("wt.line_number") == F.col("sd.line_number")), "left")
         .join(lob.alias("lob"), F.col("lob.address_number") == F.col("sd.address_number"), "left")  # F03012 sold-to LOB cat (SDAN8=AIAN8)
         .join(tag.alias("tag"),                                                                      # F49211 SO-line tag (deferred flag)
               (F.col("tag.company_key_order_no") == F.col("sd.company_key_order_no")) &
               (F.col("tag.document_order_invoice_e") == F.col("sd.document_order_invoice_e")) &
               (F.col("tag.order_type") == F.col("sd.order_type")) &
               (F.col("tag.line_number") == F.col("sd.line_number")), "left"))

    # ESO7 v2 approach: TN passes through as 1.0, else the item-specific F41002 factor.
    # Unresolved conversions stay NULL (no blanket 1.0 default) so the F41003 fallback
    # resolves downstream via DAX RELATED; missing_conversion_flag marks those rows.
    conv_rate = F.coalesce(F.when(F.trim(F.col("sd.uom_as_input")) == "TN", F.lit(1.0)),
                           F.col("ci.conv_factor"))
    conv_found = conv_rate

    sel = j.select(
        # ── degenerate / order identifiers ──
        F.col("sd.company").alias("company"),
        F.col("sd.company_key_order_no").alias("company_key_order_no"),
        F.col("sd.order_type").alias("order_type"),
        F.col("sd.document_order_invoice_e").alias("order_number"),
        F.col("sd.line_number").alias("line_number"),
        F.col("sd.shipment_number").alias("shipment_number"),
        F.col("sd.user_reserved_number").alias("bol_number"),
        F.col("sd.doc_voucher_invoice_e").alias("invoice_number"),
        F.col("sd.original_document_type").alias("original_document_type"),
        F.col("sd.original_po_so_number").alias("original_po_so_number"),
        F.col("sd.original_document_no").alias("original_document_no"),
        F.col("sd.reference_01").alias("reference_01"),
        F.col("sd.user_reserved_reference").alias("user_reserved_reference"),
        # ── status / handling / transport ──
        F.col("sh.hold_orders_code").alias("hold_orders_code"),
        F.col("sd.status_code_last").alias("status_code_last"),
        F.col("sd.status_code_next").alias("status_code_next"),
        # next_status_num (M6 — gap analysis 2026-07-22): physical INT copy of status_code_next. Direct Lake can't
        # reliably range-filter the STRING status (5 reports use next < 561/575/620 or BETWEEN 574 AND 620); a
        # blank/non-numeric status casts to NULL and is excluded — matching Hubble, where a non-numeric SDNXTR fails
        # the numeric comparison. Filter next_status_num in Power BI; keep displaying the string status_code_next.
        F.trim(F.col("sd.status_code_next")).cast("int").alias("next_status_num"),
        F.col("sd.freight_handling_code").alias("freight_handling_code"),
        F.col("sd.freight_handling_code").alias("freight_handling_code_audit"),
        F.col("sd.mode_of_transport").alias("mode_of_transport"),
        F.col("rt.route_number").alias("route_number"),
        F.col("sd.container_id").alias("container_id"),
        F.col("sd.transaction_originator").alias("transaction_originator"),
        F.col("sh.delivery_instruct_line_01").alias("delivery_instruct_line_01"),
        F.col("sh.delivery_instruct_line_02").alias("delivery_instruct_line_02"),
        F.col("sd.gl_class").alias("gl_class"),
        F.col("sd.sales_reporting_code_01").alias("sales_reporting_code_01"),
        F.col("sd.sales_reporting_code_03").alias("sales_reporting_code_03"),
        # ── address / dimension FKs ──
        F.col("sd.address_number_ship_to").alias("ship_to"),
        F.col("sd.address_number").alias("bill_to"),
        F.col("sd.carrier").alias("carrier_number"),
        F.col("sd.address_number_parent").alias("address_number_parent"),
        F.col("sd.identifier_short_item").alias("item_number_short"),
        F.trim(F.col("sd.cost_center")).alias("branch_plant"),
        # ── raw event dates ──
        F.col("sd.date_transaction_julian").alias("order_date"),
        F.col("sd.date_requested_julian").alias("requested_date"),
        F.col("sd.scheduled_pick_date").alias("scheduled_pick_date"),
        F.col("sd.date_promised_ship_julian").alias("promised_ship_date"),
        F.col("sd.actual_ship_date").alias("actual_ship_date"),
        gl_expr.alias("gl_date"),
        F.col("sd.date_invoice_julian").alias("invoice_date"),
        F.col("sd.cancel_date").alias("cancel_date"),
        F.col("sd.date_price_effective_date").alias("line_price_effective_date"),
        F.col("sh.date_price_effective_date").alias("header_price_effective_date"),
        F.col("b01.date_earliest_pickup").alias("date_earliest_pickup"),
        F.col("b01.date_latest_delivery").alias("date_latest_delivery"),
        # ── item / uom ──
        F.col("sd.identifier_second_item").alias("second_item_number"),
        F.col("sd.line_type").alias("line_type"),
        F.col("im.description_line_01").alias("item_name"),
        F.col("sd.uom_as_input").alias("uom"),
        F.col("sd.uom_primary").alias("uom_primary"),
        F.col("sd.uom_pricing").alias("uom_pricing"),
        conv_rate.alias("conversion_to_tons_rate"),
        F.when(conv_found.isNull(), F.lit("Y")).otherwise(F.lit("N")).alias("missing_conversion_flag"),
        # ── filter-only attributes (Power BI slicers; Filter Selections xlsx — ESO1) ──
        F.col("al.price_adjustment_type").alias("price_adjustment_type"),       # ALAST (F4074, actual)
        F.col("st.standard_industry_code").alias("standard_industry_code"),     # ABSIC (ship-to F0101)
        F.col("st.report_code_add_book_005").alias("category_code_05"),         # ABAC05 (ship-to F0101)
        F.col("st.report_code_add_book_014").alias("category_code_14"),         # ABAC14 (ship-to F0101)
        F.col("st.address_type_01").alias("search_type"),                       # ABAT1 (ship-to F0101)
        F.col("us.uom_structure").alias("uom_structure"),                       # UMUSTR (F41002)
        F.col("sd.payment_terms_code_01").alias("payment_terms"),               # SDPTC (F4211)
        F.col("im.segment_04").alias("item_segment_04"),                        # IMSEG4 (F4101)
        # ── measures / numerics (line grain) ──
        F.col("sd.units_quantity_shipped").alias("quantity_shipped"),
        F.col("sd.units_primary_qty_order").alias("primary_quantity_ordered"),
        F.col("sd.units_transaction_qty").alias("transaction_quantity"),
        F.col("sd.amt_price_per_unit_02").alias("price_per_unit"),
        F.col("sd.sales_reporting_code_02").alias("major_prod_code"),
        F.col("sd.sales_reporting_code_04").alias("minor_prod_code"),
        F.col("al.freight_factor_value").alias("freight_factor_value"),
        # ── denormalized booking / ocean (shipment grain) ──
        F.col("b11.seal_no").alias("seal_no"),
        F.col("b01.booking_no").alias("booking_no"),
        F.col("b01.destination_port").alias("destination_port"),   # FK -> dim_address_book_destination[address_number]
        F.col("b01.no_of_container").alias("no_of_container"),
        F.col("b01.ocean_del_terms").alias("ocean_del_terms"),
        F.col("b01.vessel_name").alias("vessel_name"),
        # ── denormalized freight location + buckets (shipment grain) ──
        F.col("fr.freight_city").alias("freight_city"),
        F.col("fr.freight_state").alias("freight_state"),
        F.col("fr.freight_zip").alias("freight_zip"),
        F.col("fr.shift_factor_applied"),
        F.coalesce(F.col("fr.billable_freight"), F.lit(0.0)).alias("billable_freight"),
        F.coalesce(F.col("fr.billable_fuel"),    F.lit(0.0)).alias("billable_fuel"),
        F.coalesce(F.col("fr.total_billable"),   F.lit(0.0)).alias("total_billable"),
        F.coalesce(F.col("fr.payable_freight"),  F.lit(0.0)).alias("payable_freight"),
        F.coalesce(F.col("fr.payable_fuel"),     F.lit(0.0)).alias("payable_fuel"),
        F.coalesce(F.col("fr.total_payable"),    F.lit(0.0)).alias("total_payable"),
        F.coalesce(F.col("fr.total_freight"),    F.lit(0.0)).alias("total_freight"),    # H2 — all-charge-code shipment freight total
        F.coalesce(F.col("fr.freight_variance"), F.lit(0.0)).alias("freight_variance"),
        F.coalesce(F.col("fr.total_variance"),   F.lit(0.0)).alias("total_variance"),
        # ── ADDED 2026-07-15 (ESO1 Filter Capture variations; README §3) ──
        F.col("sd.amount_extended_price").alias("extended_price"),           # SDAEXP — THE sales-amount measure
        F.col("sd.amount_extended_cost").alias("extended_cost"),             # SDECST
        F.col("sd.currency_code_base").alias("currency_code"),              # SDBCRC (line domestic currency)
        F.col("sd.units_quan_backor_held").alias("backorder_qty"),           # SDSOBK
        F.col("sd.units_quantity_canceled").alias("cancelled_qty"),          # SDSOCN
        F.col("sd.quantity_shipped_to_date").alias("qty_to_date"),           # SDQTYT
        F.col("sd.units_open_quantity").alias("open_qty"),                   # SDUOPN
        F.col("sd.description_line_01").alias("line_description_1"),          # SDDSC1 (the LINE's text, not item_name)
        F.col("sd.description_line_02").alias("line_description_2"),          # SDDSC2
        F.col("sd.date_updated").alias("date_updated"),                      # SDUPMJ
        F.col("st.user_reserved_amount").alias("address_rate"),              # ABURAT — ship-to F0101 (sole measure of the rate reports)
        F.col("so.name_alpha").alias("sold_to_name"),                        # ABALPH — sold-to F0101
        F.col("so.address_type_01").alias("sold_to_search_type"),            # ABAT1  — sold-to (Days-Since-Invoice filters this)
        F.col("so.report_code_add_book_005").alias("sold_to_category_05"),   # ABAC05 — sold-to
        F.col("so.report_code_add_book_010").alias("sold_to_category_10"),   # ABAC10 — sold-to
        F.col("adr.city").alias("ship_to_city"),                             # ALCTY1 (F0116)
        F.col("adr.state").alias("ship_to_state"),                           # ALADDS
        F.col("adr.zip_code_postal").alias("ship_to_zip"),                   # ALADDZ
        F.col("adr.address_line_01").alias("ship_to_address_1"),             # ALADD1
        F.col("adr.address_line_02").alias("ship_to_address_2"),             # ALADD2
        F.col("adr.country").alias("ship_to_country"),                       # ALCTR
        # ── ADDED 2026-07-22: residual-gap columns (M2–M5 + deferred F4211 display) ──
        F.col("sd.zone_number").alias("zone_number"),                        # SDZON (M2 — Orders on Hold for Pricing)
        F.col("sd.hold_orders_code").alias("line_hold"),                     # SDHOLD LINE hold (M3) — ≠ header hold_orders_code (sh)
        F.col("rt.is_ocean_route").alias("is_ocean_route"),                  # F4941 RSMOT='OCE' any step (M4)
        F.col("rt.route_container_count").alias("route_container_count"),    # F4941 SUM(RSNCTR) (M4)
        F.col("wt.gross_weight").alias("gross_weight"),                      # F5549002 MIGRWT (M5)
        F.col("wt.catch_weight").alias("catch_weight"),                      # F5549002 MICTWT (M5)
        F.col("wt.max_weight").alias("max_weight"),                          # F5549002 MIMXWT (M5)
        F.col("sd.pull_signal").alias("pull_signal"),                        # SDPSIG (7 load reports)
        F.col("sd.reference_02_vendor").alias("reference_02"),               # SDVR02
        F.col("sd.reference_ucis_no").alias("reference_03"),                 # SDVR03 (IFS order no)
        F.col("sd.primary_last_vendor_no").alias("vendor_number"),           # SDVEND (SBX Unbilled AR)
        F.col("sd.price_adjustment_schedule_n").alias("price_adjustment_schedule"),  # SDASN
        F.col("sd.user_reserved_code").alias("user_reserved_code"),          # SDURCD
        F.col("sd.price_override_code").alias("price_override_code"),         # SDPROV
        F.col("sd.user_id").alias("user_id"),                                # SDUSER
        F.col("sd.lot").alias("lot_number"),                                 # SDLOTN
        F.col("sd.serial_number_lot").alias("serial_number"),                # SDSERN
        F.col("sd.location").alias("location"),                              # SDLOCN
        F.col("sd.sales_reporting_code_05").alias("sales_reporting_code_05"),# SDSRP5
        F.col("lob.sold_to_lob_category_05").alias("sold_to_lob_category_05"),# F03012 AIAC05 (sold-to LOB, on SDAN8) — Mak
        F.col("tag.deferred_entries_flag").alias("deferred_entries_flag"),    # F49211 UDDEFF (SOP0006 / SOP000x)
        # ── ADDED 2026-07-22: remaining niche display source-columns (all present in Silver) ──
        F.col("sd.related_po_so_number").alias("related_po_so_number"),      # SDRORN (Columbia/Pacific/Sherwin)
        F.col("sd.time_of_day").alias("time_of_day"),                        # SDTDAY (SOP000x-620)
        F.col("sh.date_original_promisde").alias("original_promised_date"),  # SHOPDJ (Daily NPO Aging) — distinct header date
        F.col("st.address_number_third").alias("related_address_3"),         # ABAN83 (SOP0020 display)
        F.col("al.adj_gl_class").alias("adj_gl_class"),                      # F4074 ALGLC
        F.col("al.adj_based_on_value").alias("adj_based_on_value"),          # F4074 ALBSDVAL
        F.col("al.adj_uom").alias("adj_uom"),                                # F4074 ALUOM
        F.col("al.adj_factor_value").alias("adj_factor_value"),              # F4074 ALFVTR
        F.col("b01.voyage_number").alias("voyage_number"),                   # F5642B01 BA55VONO
        F.col("b01.loading_port").alias("loading_port"),                     # F5642B01 BA55LODP
        F.col("b01.ocean_carrier").alias("ocean_carrier"),                   # F5642B01 BA55OCCR
        F.col("b01.booking_reference_1").alias("booking_reference_1"),       # F5642B01 BA55REF1
        F.col("b01.booking_reference_2").alias("booking_reference_2"),       # F5642B01 BA55REF2
        F.col("b01.booking_reference_3").alias("booking_reference_3"),       # F5642B01 BA55REF3 (Orders on Hold)
        F.col("b01.date_latest_pickup").alias("date_latest_pickup"),         # F5642B01 BADLPU
    ).distinct()

    # clean sentinel/junk dates on the raw date columns BEFORE deriving buckets/keys,
    # so both the stored raw dates and everything derived from them stay clean.
    _base = sel
    for _dc in _RAW_DATE_COLS:
        if _dc in _base.columns:
            _base = _base.withColumn(_dc, clean_date(F.col(_dc)))

    # weekly bucket off the (cleaned) actual_ship_date — Mon–Sun ISO week label "YYYY-Www"
    # (replaces dim_date[year_week]; ISO week-numbering year corrected at Jan/Dec boundaries)
    _wk    = F.weekofyear(F.col("actual_ship_date"))
    _wyr   = F.year(F.col("actual_ship_date"))
    _wmth  = F.month(F.col("actual_ship_date"))
    _isoyr = (F.when((_wmth == 1) & (_wk > 50), _wyr - 1)
                .when((_wmth == 12) & (_wk == 1), _wyr + 1)
                .otherwise(_wyr))

    df = (_base
          .withColumn("quantity_shipped_tons", F.col("quantity_shipped") * F.col("conversion_to_tons_rate"))
          .withColumn("price_quantity_shipped", F.col("price_per_unit") * F.col("quantity_shipped"))
          .withColumn("ship_year_week",
                      F.when(F.col("actual_ship_date").isNotNull(),
                             F.concat(_isoyr, F.lit("-W"), F.lpad(_wk.cast("string"), 2, "0"))))
          # ── role-play date keys (yyyyMMdd). Date dimension REMOVED (2026-07-23) — these
          #    ints are retained but unused; dates are sliced via the raw date columns above. ──
          .withColumn("order_date_key",                   date_key(F.col("order_date")))
          .withColumn("requested_date_key",               date_key(F.col("requested_date")))
          .withColumn("scheduled_pick_date_key",          date_key(F.col("scheduled_pick_date")))
          .withColumn("promised_ship_date_key",           date_key(F.col("promised_ship_date")))
          .withColumn("ship_date_key",                    date_key(F.col("actual_ship_date")))
          .withColumn("gl_date_key",                      date_key(F.col("gl_date")))
          .withColumn("invoice_date_key",                 date_key(F.col("invoice_date")))
          .withColumn("cancel_date_key",                  date_key(F.col("cancel_date")))
          .withColumn("line_price_effective_date_key",    date_key(F.col("line_price_effective_date")))
          .withColumn("header_price_effective_date_key",  date_key(F.col("header_price_effective_date")))
          .withColumn("earliest_pickup_date_key",         date_key(F.col("date_earliest_pickup")))
          .withColumn("latest_delivery_date_key",         date_key(F.col("date_latest_delivery")))
          .withColumn("shift_factor_applied", F.coalesce(F.col("shift_factor_applied"), F.lit(SHIFT_FACTOR))))

    w = Window.partitionBy("shipment_number").orderBy("order_number", "line_number")
    df = (df.withColumn("_rn", F.row_number().over(w))
            .withColumn("is_primary_shipment_line",
                        F.when((F.col("shipment_number").isNotNull()) & (F.col("_rn") == 1), F.lit("Y")).otherwise(F.lit("N")))
            .drop("_rn"))

    df = (df.withColumn("sales_order_line_key",
                        sk("company_key_order_no", "order_type", "order_number", "line_number"))
            .withColumn("order_scope_key", sk("company_key_order_no", "order_type", "order_number")))
    # one row per order line — no CDF/audit columns stored in the fact
    df = df.dropDuplicates(["sales_order_line_key"])
    df = _add_effective_price_flag(df)                        # M1 — has_effective_price (1:1, no fan-out)
    return df.select("sales_order_line_key", "order_scope_key", *FACT_BUSINESS_COLS)


# In[6]:


# =============================================================================
# REUSED-DIMENSION PREFLIGHT — abort if a base reused dim is missing/empty
# =============================================================================
_errs = []
for tbl in [R_DIM_AB, R_DIM_PLANT]:
    if not _exists(tbl):
        _errs.append(f"MISSING {tbl}"); print(f"  MISSING : {tbl}"); continue
    n = spark.read.table(tbl).count()
    if n == 0:
        _errs.append(f"EMPTY {tbl}")
    print(f"  OK      : {tbl:38s} rows={n:,}")
for v in [R_DIM_SHIP_TO, R_DIM_SOLD_TO, R_DIM_CARRIER, R_DIM_PARENT]:
    print(f"  {'OK     ' if _exists(v) else 'no-spark'} : {v}  (reused role view)")
if _errs:
    raise Exception("Reused-dimension preflight FAILED: " + "; ".join(_errs)
                    + ". Build via old_nb (nb_dim_address_book, nb_dim_plant) first.")
print("✓ reused-dimension preflight passed")


# In[7]:


# =============================================================================
# FULL LOAD vs RESUME  (streaming approach from nb_silver_to_gold_eso7_v2)
#   1) Stop any of our streams left alive from a previous run in this session
#      (stopping a cell does NOT stop Spark streaming queries).
#   2) FULL LOAD when OVERWRITE, or the fact is missing, or the checkpoints are gone:
#      drop + rebuild the fact from the full Silver snapshot, snapshot each streamed
#      source's current Delta version as init_ver, and clear the checkpoints. Streams
#      then start at init_ver and skip _commit_version <= init_ver (the seed rows,
#      already in Gold).
#   3) Otherwise RESUME (init_ver = -1; the checkpoint drives the offset and the
#      streams catch up any missed changes).
# Reused rpt dims and dim_item (nb_eso1_gold_dim_item) are owned elsewhere — never touched here.
# =============================================================================
# the per-stream checkpoint dirs (names must match the queryName()s in the start cell). F42119 (Sales
# Order History) joins the stream set only if it exists in Silver (its name is inferred — see §helpers).
_F42119_PRESENT = False
try:
    _F42119_PRESENT = spark.catalog.tableExists(sname(F42119_TBL))
except Exception:
    _F42119_PRESENT = False
_STREAMED = [F4211_TBL, F4981_TBL] + ([F42119_TBL] if _F42119_PRESENT else [])
_CKPT_PATHS = [f"{CKPT}/fact__{t}" for t in _STREAMED]

def _checkpoints_exist():
    """True iff EVERY per-stream checkpoint has a COMMITTED offset (its offsets/ dir is
    non-empty). Checking offsets/ — not merely that the checkpoint dir exists and is
    non-empty — matters because a checkpoint dir can hold metadata/ + sources/ yet never
    have committed a batch (e.g. the query died on its first read). The old dir-only check
    let such an INCOMPLETE checkpoint fool the gate into RESUME; Delta then found no usable
    offset and cold-started the CDF reader at startingVersion=0 (version 0 predates CDF
    enablement) -> DELTA_MISSING_CHANGE_DATA. Requiring a committed offset treats an
    incomplete checkpoint as ABSENT, forcing a FULL LOAD that re-establishes init_ver at a
    CDF-valid version. Also per-stream (not just the root, as ESO7 does) so a rename or a
    stale root can't route the run into RESUME either."""
    for p in _CKPT_PATHS:
        try:
            if not mssparkutils.fs.ls(f"{p}/offsets"):
                return False
        except Exception:
            return False
    return True

# ── (1) stop leftover streams from a previous run in this Spark session ──────────
_STREAM_NAMES = {"fact__" + t for t in _STREAMED}
_stopped = []
for _q in list(spark.streams.active):
    if _q.name in _STREAM_NAMES:
        _q.stop()
        _stopped.append(_q.name)
if _stopped:
    print(f"Stopped leftover streams: {_stopped}")

# ── (2/3) full-load gate — also full-load when the checkpoints are missing ───────
_FULL_LOAD = OVERWRITE or not spark.catalog.tableExists(T_FACT) or not _checkpoints_exist()

if _FULL_LOAD:
    print("== FULL LOAD ==")
    spark.sql(f"DROP TABLE IF EXISTS {T_FACT}")
    _write_new_table(transform_fact(), T_FACT)
    print(f"  ✓ seeded {T_FACT}")
    # snapshot each streamed source's current Delta version — streams start here and the
    # handlers skip anything at or below it (that data is the seed, already in Gold).
    _init_ver = {t: current_version(t) for t in _STREAMED}
    print(f"  init versions: {_init_ver}")
    try:
        mssparkutils.fs.rm(CKPT, True)
        print("  checkpoints cleared")
    except Exception as e:
        print(f"  checkpoint clear skipped: {e}")
    print("✓ full load complete")
else:
    print("== RESUME from checkpoint ==")
    _init_ver = {}   # .get(src, -1) -> -1 everywhere; no version filtering, checkpoint drives


# In[8]:


# =============================================================================
# STREAM BATCH HANDLERS  (structure from nb_silver_to_gold_eso7_v2)
#   init_ver = the source's Delta version at full-load time. Any batch row with
#   _commit_version <= init_ver is seed data (already in Gold via the full load), so
#   the handler skips it. On resume (init_ver = -1) nothing is filtered — the
#   checkpoint drives the offset. Each handler then acts only on the real change rows
#   (_change_type insert / update_postimage / delete) and CDC-writes to Gold.
# =============================================================================
def make_fact_f4211_handler(init_ver):
    def handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        if init_ver >= 0:
            batch_df = batch_df.filter(F.col("_commit_version") > init_ver)
        if batch_df.rdd.isEmpty():
            return
        orders = (batch_df.filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
                  .select(F.col("company_key_order_no"), F.col("order_type"),
                          F.col("document_order_invoice_e").alias("order_number")).distinct())
        n = recompute_fact(orders)   # delete order scope + append recomputed lines (self-locks)
        print(f"[{F4211_TBL[:12]}] fact batch={batch_id} rows={n}")
    return handler

def make_fact_f4981_handler(init_ver):
    def handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        if init_ver >= 0:
            batch_df = batch_df.filter(F.col("_commit_version") > init_ver)
        if batch_df.rdd.isEmpty():
            return
        ships = (batch_df.filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
                 .select("shipment_number").where(F.col("shipment_number").isNotNull()).distinct())
        if ships.rdd.isEmpty():
            return
        f4211  = load_silver_table(F4211_TBL)   # map changed shipments back to their orders
        orders = (f4211.join(ships.alias("s"), f4211["shipment_number"] == F.col("s.shipment_number"), "left_semi")
                  .select(F.col("company_key_order_no"), F.col("order_type"),
                          F.col("document_order_invoice_e").alias("order_number")).distinct())
        n = recompute_fact(orders)   # recompute the whole order (freight lives on its lines)
        print(f"[{F4981_TBL[:12]}] freight batch={batch_id} rows={n}")
    return handler


# In[9]:


# =============================================================================
# START STREAMS — Silver Change Data Feed -> foreachBatch -> CDC write, every 30 s.
# Inline per source (structure from nb_silver_to_gold_eso7_v2). Each stream starts at
# init_ver (full load) — the seed-time version, which EXISTS and (with CDF enabled)
# carries change data. The first batch is that seed version's own changes, which the
# handler discards via `_commit_version <= init_ver`; everything after is processed.
#   • startingVersion must be an EXISTING version: init_ver + 1 is beyond the latest at
#     seed time and Delta rejects it ("Cannot time travel to version N").
#   • init_ver must have CDF recorded (delta.enableChangeDataFeed=true at or before it),
#     otherwise Delta raises DELTA_MISSING_CHANGE_DATA for that version.
# On resume (init_ver = -1) the checkpoint's committed offset drives; startingVersion is a
# fallback set to the source's CURRENT version (never 0 — version 0 predates CDF enablement).
# REQUIRES delta.enableChangeDataFeed = true on every streamed source (F4211/F4981).
# =============================================================================
def _start_ver(iv, tbl):
    """The version each stream starts at. Full load: init_ver (the seed-time version — it
    exists and carries CDF); the handler skips _commit_version <= init_ver. Resume (iv < 0):
    startingVersion is ignored once the checkpoint has a committed offset, but if Delta ever
    has to cold-start (an offset-less checkpoint slipped through) it must NOT read from 0 —
    version 0 predates CDF enablement and raises DELTA_MISSING_CHANGE_DATA. So fall back to
    the source's CURRENT version, which always exists and is >= the version CDF was enabled
    at, hence always a valid CDF start."""
    return iv if iv >= 0 else current_version(tbl)

iv_fact = _init_ver.get(F4211_TBL, -1)
_sv_fact = _start_ver(iv_fact, F4211_TBL)
(spark.readStream.format("delta")
     .option("readChangeFeed",  "true")
     .option("startingVersion", _sv_fact)
     .table(sname(F4211_TBL))
 .writeStream
     .foreachBatch(make_fact_f4211_handler(iv_fact))
     .option("checkpointLocation", f"{CKPT}/fact__{F4211_TBL}")
     .trigger(**TRIGGER)
     .queryName("fact__" + F4211_TBL)
     .start())
print(f"  fact__{F4211_TBL}  startingVersion={_sv_fact}  init_ver={iv_fact}")

iv_freight = _init_ver.get(F4981_TBL, -1)
_sv_freight = _start_ver(iv_freight, F4981_TBL)
(spark.readStream.format("delta")
     .option("readChangeFeed",  "true")
     .option("startingVersion", _sv_freight)
     .table(sname(F4981_TBL))
 .writeStream
     .foreachBatch(make_fact_f4981_handler(iv_freight))
     .option("checkpointLocation", f"{CKPT}/fact__{F4981_TBL}")
     .trigger(**TRIGGER)
     .queryName("fact__" + F4981_TBL)
     .start())
print(f"  fact__{F4981_TBL}  startingVersion={_sv_freight}  init_ver={iv_freight}")

# ── (3rd stream) F42119 Sales Order History — only if present in Silver. Same order-keyed handler as
#    F4211 (history rows carry company_key_order_no / order_type / order_number), so a change to a
#    closed/purged line recomputes its order scope exactly like a live-line change. ──
if _F42119_PRESENT:
    iv_hist  = _init_ver.get(F42119_TBL, -1)
    _sv_hist = _start_ver(iv_hist, F42119_TBL)
    (spark.readStream.format("delta")
         .option("readChangeFeed",  "true")
         .option("startingVersion", _sv_hist)
         .table(sname(F42119_TBL))
     .writeStream
         .foreachBatch(make_fact_f4211_handler(iv_hist))
         .option("checkpointLocation", f"{CKPT}/fact__{F42119_TBL}")
         .trigger(**TRIGGER)
         .queryName("fact__" + F42119_TBL)
         .start())
    print(f"  fact__{F42119_TBL}  startingVersion={_sv_hist}  init_ver={iv_hist}")
else:
    print(f"  (F42119 not in Silver — history stream skipped; fact = live F4211 only)")

print(f"== started {len(_STREAMED)} streams — continuous, trigger {TRIGGER}. Target {T_FACT}. "
      "Reused dims (rpt) + dim_item refresh via their own jobs. ==")
spark.streams.awaitAnyTermination()
