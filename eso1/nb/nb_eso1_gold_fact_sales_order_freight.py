#!/usr/bin/env python
# coding: utf-8

# ## nb_eso1_gold_fact_sales_order_freight
#
# **Gold `fact_sales_order_freight` processor** for Extended Sales Order 1 (Billable v
# Payable Freight). Builds ONE table — `lh_jde_gold.rpt.fact_sales_order_freight`
# (order-line grain; freight denormalized) — from the Silver sales-order detail (F4211),
# freight-audit (F4981) and their supporting sources.
#
# ── BUILD (BATCH) ─────────────────────────
#   • read the full Silver snapshot of every source, run build_fact() ONCE, overwrite the fact.
#   • MANUAL_OVERWRITE = True → drop + rebuild; False → build only if the fact is missing (re-run to refresh).
#
# Sections:  1) CONFIG   2) FACT BUILDER   3) FACT SOURCES   4) RUN


# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------

# CONFIG + CONSTANTS
import json, time
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"          # Silver schema — static batch snapshots
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

# ── refresh / runtime config (BATCH build) ──
#   MANUAL_OVERWRITE = True  -> full load: drop + rebuild the fact from the full Silver snapshot.
#   MANUAL_OVERWRITE = False -> build only if the fact is missing (re-run to refresh).
MANUAL_OVERWRITE = True    # ⚠ set back to False after the first successful run

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

# ── Silver sources ────────────────────────────────────────────────────────────
F4211    = "f4211_sales_order_detail_file"
F4201    = "f4201_sales_order_header_file"
F0101    = "f0101_address_book_master"
F4101    = "f4101_item_master"
F41002   = "f41002_item_units_of_measure_conversion_factors"
# F41003 (standard UoM conversion) is not sourced here — this notebook uses F41002 item-specific
# conversion only.
F4981    = "f4981_freight_audit_history"
F5642B01 = "f5642b01_custom_sales_order_entry_screen_header"
F5642B11 = "f5642b11_custom_sales_order_entry_screen_detail"
F4941    = "f4941_shipment_routing_steps"
# ── additional sources ──
F4106    = "f4106_item_base_price_file"           # item base price (has_effective_price flag)
F5549002 = "f5549002_mxp_bol_interface_detail"    # BOL interface weigh-ticket weights
F03012   = "f03012_customer_master_by_line_of_business"  # sold-to LOB category (AIAC05)
F49211   = "f49211_sales_order_detail_file_tag_file"     # SO-line tag file (UDDEFF deferred flag)
# ── page-level filtering sources ──
F42119   = "f42119_sales_order_history_file"      # Sales Order History
                                                      # (table_name=sales_order_history_file, identical 268-col
                                                      # SD* schema to F4211).
                                                      # UNION ALL F42119 with F4211 for closed/purged lines.
                                                      # Still guarded by _load_optional so the notebook runs
                                                      # whether or not the table is ingested to Silver yet.

# ── Gold target BUILT here (new, rpt) ──────────────────────────────────────────
FACT         = "fact_sales_order_freight"

# ── report scaling — business WHERE filters removed; the fact carries ALL rows (no-filter design) ──
SHIFT_FACTOR    = 1.0    # placeholder (Silver pre-decoded → 1.0)

print(f"ESO1 Gold fact processor (batch build) — target {gname(FACT)}")


# HELPERS
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def _load_optional(table_name):
    """Load a Silver table only if it exists; else warn and return None. Used for F42119 (Sales Order
    History) — the table may not be ingested to Silver yet; the guard lets the fact run with OR without it,
    unioning the
    closed/purged history rows when present and running F4211-only otherwise."""
    try:
        if spark.catalog.tableExists(sname(table_name)):
            return load_silver_table(table_name)
    except Exception as e:
        print("  ⚠ optional source check failed for {}: {}".format(sname(table_name), e))
    print("  ⚠ optional source not found, skipping union: {}".format(sname(table_name)))
    return None

def load_silver_table(table_name):
    df = spark.read.table(sname(table_name))
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
    "date_earliest_pickup", "date_earliest_delivery", "date_latest_delivery", "release_date", "date_requested_ship",
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
    """Surrogate key — pipe-separated string from one or more column names."""
    return F.concat_ws(
        "|",
        *[
            F.col(c).cast("string") if isinstance(c, str) else c.cast("string")
            for c in cols
        ],
    )

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return (c, True)
    return (candidates[0], False)


# ----------------------------------------------------------------------------
# 2) FACT BUILDER
# ----------------------------------------------------------------------------


# UoM -> TN cascades + freight buckets (F4981 -> shipment grain)
def build_uom_cascades():
    # item-specific F41002 conversion ONLY (fwd + reciprocal rev union).
    # The standard-UoM (F41003) fallback is handled downstream, not cascaded here.
    f41002 = load_silver_table(F41002)
    item_fwd = (f41002.filter((F.trim("related_uom") == "TN") & (F.col("conversion_factor") != 0))
                .select(F.col("identifier_short_item").alias("itm"), F.trim("uom").alias("from_uom"),
                        F.col("conversion_factor").cast("double").alias("conv_factor")))
    item_rev = (f41002.filter((F.trim("uom") == "TN") & (F.col("conversion_factor") != 0))
                .select(F.col("identifier_short_item").alias("itm"), F.trim("related_uom").alias("from_uom"),
                        (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_factor")))
    return item_fwd.unionByName(item_rev)

def transform_freight_buckets():
    f4981 = load_silver_table(F4981)   # no vendor-invoice filter — all freight-audit rows included
    bp, cgc, amt = F.trim("billable_payable"), F.trim("charge_code_01"), F.col("net_amount")
    # freight location (FHCTY1/FHADDS/FHADDZ) denormalized at shipment grain (first non-null)
    return (f4981.groupBy("shipment_number").agg(
                F.round(F.sum(F.when((bp == "B") & (cgc == "BFR"),            amt).otherwise(0.0)), 2).alias("billable_freight"),
                F.round(F.sum(F.when((bp == "B") & (cgc.isin("FSC", "FSB")), amt).otherwise(0.0)), 2).alias("billable_fuel"),
                F.round(F.sum(F.when((bp == "P") & (cgc == "PFR"),            amt).otherwise(0.0)), 2).alias("payable_freight"),
                F.round(F.sum(F.when((bp == "P") & (cgc == "FSC"),            amt).otherwise(0.0)), 2).alias("payable_fuel"),
                # total_freight = ALL F4981 net_amount for the shipment, regardless of billable_payable / charge_code
                # The billable/payable buckets above UNDER-count that total if any charge code falls
                # outside {BFR,FSC,FSB,PFR}. Denormalized at shipment grain like the buckets —
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


# FACT  fact_sales_order_freight  (order-line grain; freight denormalized)
FACT_BUSINESS_COLS = [
    # ── degenerate / order identifiers ──
    "company", "company_key_order_no", "order_type", "document_type", "order_number", "line_number",
    "shipment_number", "bol_number", "invoice_number",
    "original_document_type", "original_po_so_number", "original_document_no",
    "reference_01", "user_reserved_reference",
    # ── status / handling / transport ──
    "hold_orders_code", "status_code_last", "status_code_next", "next_status_num", "last_status_num",
    "freight_handling_code",
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
    "date_earliest_pickup", "date_earliest_delivery", "date_latest_delivery",
    # weekly grouping bucket (Mon–Sun ISO week label off actual_ship_date) — replaces
    # dim_date[year_week] now that there is no date dimension
    "ship_year_week",
    # ── item / uom ──
    "second_item_number", "third_item_number", "line_type", "uom", "uom_primary", "uom_pricing",
    "conversion_to_tons_rate", "missing_conversion_flag",
    # ── filter-only attributes ──
    "uom_structure", "payment_terms",   # UMUSTR (F41002) / SDPTC (F4211)
    # ── measures / numerics (line grain) ──
    "quantity_shipped", "quantity_shipped_tons", "primary_quantity_ordered",
    "transaction_quantity", "price_per_unit", "unit_price_primary", "price_quantity_shipped",
    "major_prod_code", "minor_prod_code",
    # ── denormalized booking / ocean (shipment grain) ──
    "seal_no", "production_code", "production_ship_notes", "booking_no", "booking_status", "destination_port",
    "no_of_container", "ocean_del_terms", "vessel_name",
    # ── denormalized freight location + buckets (shipment grain) ──
    "freight_city", "freight_state", "freight_zip",
    "billable_freight", "billable_fuel", "total_billable",
    "payable_freight", "payable_fuel", "total_payable", "total_freight",
    "freight_variance", "total_variance", "shift_factor_applied",
    "is_primary_shipment_line",
    # ── page-level filtering attributes ──
    "extended_price", "extended_cost", "currency_code",              # SDAEXP / SDECST / SDBCRC (F4211)
    "backorder_qty", "cancelled_qty", "qty_to_date", "open_qty",     # SDSOBK / SDSOCN / SDQTYT / SDUOPN
    "line_description_1", "line_description_2", "date_updated",       # SDDSC1 / SDDSC2 / SDUPMJ (line, not item)
    # ── additional display columns ──
    "zone_number", "line_hold",                                     # SDZON / SDHOLD (LINE hold ≠ header hold_orders_code)
    "is_ocean_route", "route_container_count",                      # F4941 RSMOT='OCE' flag / SUM(RSNCTR)
    "gross_weight", "catch_weight", "max_weight",                   # F5549002 BOL weigh-ticket weights (MIGRWT/MICTWT/MIMXWT)
    "has_effective_price",                                          # F4106 base-price existence
    "pricing_issue_remark",                                        # derived: 'Unit Price Zero' / 'No effective price'
    "pull_signal", "reference_02", "reference_03", "vendor_number", # deferred F4211 display (SDPSIG/SDVR02/SDVR03/SDVEND)
    "price_adjustment_schedule", "user_reserved_code", "price_override_code",  # SDASN / SDURCD / SDPROV
    "user_id", "lot_number", "serial_number", "location", "sales_reporting_code_05",  # SDUSER/SDLOTN/SDSERN/SDLOCN/SDSRP5
    "sold_to_lob_category_05", "deferred_entries_flag",             # F03012 AIAC05 (sold-to LOB) / F49211 UDDEFF (SO-tag)
    # ── remaining niche display source-columns ──
    "related_po_so_number", "time_of_day", "original_promised_date",  # SDRORN/SDTDAY/SHOPDJ
    "voyage_number", "loading_port", "ocean_carrier",                                       # F5642B01 ocean-booking
    "booking_reference_1", "booking_reference_2", "booking_reference_3", "date_latest_pickup",
    "order_reference", "routing_notes", "equipment_type", "inland_delterms", "incoterms",   # F5642B01
    "date_requested_ship", "release_date",                                                  # BARQSJ (F5642B01) / SDRSDJ (F4211)
    # ── header-level (F4201) display columns ──
    "header_sold_to", "header_order_date", "header_carrier_number", "header_payment_terms",  # SHAN8/SHTRDJ/SHCARS/SHPTC
    "header_parent",                                                # SHPA8 (header parent)
]

def _add_effective_price_flag(df):
    """has_effective_price: 'Y' when an F4106 item-base-price row EXISTS for the line's
    (second_item_number, branch_plant, ship_to) with a non-zero price whose effective window covers the
    line's actual_ship_date. F4106 keyed BPLITM=SDLITM, BPMCU=SDMCU, BPAN8=SDSHAN, BPEFTJ<=SDADDJ, BPEXDJ>=SDADDJ,
    BPUPRC<>0. A left_semi to the distinct line key keeps the line grain (one flag per line, no fan-out)."""
    f4106 = load_silver_table(F4106)
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

def build_fact():
    f4211 = load_silver_table(F4211)
    # UNION ALL F4211 (live) + F42119 (Sales Order History),
    # so closed/purged lines are included. Guarded: if F42119 is absent from Silver, the fact is F4211
    # only. Same SD* schema; allowMissingColumns tolerates minor schema drift. Any live/history overlap
    # is collapsed by the final dropDuplicates(["sales_order_line_key"]).
    _hist = _load_optional(F42119)
    if _hist is not None:
        # F42119 snake-names SDLITM as `identifier_2nd_item` (≠ F4211 `identifier_second_item`), so an
        # unqualified unionByName would NULL second_item_number for every history row — rename to match
        # before the union.
        if "identifier_2nd_item" in _hist.columns and "identifier_second_item" not in _hist.columns:
            _hist = _hist.withColumnRenamed("identifier_2nd_item", "identifier_second_item")
        # F42119 likewise snake-names SDAITM as `identifier_3rd_item` (≠ F4211 `identifier_third_item`),
        # so rename to match before the union or third_item_number is NULL for every history row.
        if "identifier_3rd_item" in _hist.columns and "identifier_third_item" not in _hist.columns:
            _hist = _hist.withColumnRenamed("identifier_3rd_item", "identifier_third_item")
        f4211 = f4211.unionByName(_hist, allowMissingColumns=True)
    f4201 = load_silver_table(F4201)
    f0101 = load_silver_table(F0101)
    b01   = load_silver_table(F5642B01); b11 = load_silver_table(F5642B11)
    f4941 = load_silver_table(F4941)
    f41002 = load_silver_table(F41002)
    f5549002 = load_silver_table(F5549002)                 # BOL interface weigh-ticket weights
    f03012 = load_silver_table(F03012); f49211 = load_silver_table(F49211)  # sold-to LOB cat / SO-line tag flag
    conv_item = build_uom_cascades()

    gl_name, gl_present = pick_col(f4211, ["dt_for_gl_and_vouch_01",   # F4211.SDDGL in Silver
        "date_for_g_l_julian", "date_g_l_julian", "date_general_ledger_julian",
        "general_ledger_date", "date_for_g_land_voucher_julian", "gl_date"])
    gl_expr = F.col(f"sd.{gl_name}") if gl_present else F.lit(None).cast("date")

    sd = f4211   # business WHERE filters removed — fact carries all rows

    # F41002 UOM structure (UMUSTR) — read from the item's TN-conversion row (join
    # UMITM=SDITM AND UMRUM='TN' AND UMUM=SDUOM). Without the related_uom='TN' gate the
    # (item, uom) key matches multiple F41002 rows → an arbitrary/non-TN uom_structure + a pre-dedup
    # fan-out. Gate on TN and dedup to one row per (item, input-uom).
    uom_str = (f41002.filter(F.trim(F.col("related_uom")) == "TN")
               .select(F.col("identifier_short_item").alias("us_itm"),
                       F.trim(F.col("uom")).alias("us_uom"),
                       F.col("uom_structure").alias("uom_structure"))
               .dropDuplicates(["us_itm", "us_uom"]))
    # F4941 shipment routing → route_number + ocean-mode / container count. is_ocean_route='Y' if ANY
    # routing step is OCE (RSMOT='OCE'); route_container_count = SUM(RSNCTR) over OCE steps ONLY, so
    # non-ocean legs of a shipment are excluded from the container count.
    route = (f4941.groupBy("shipment_number").agg(
                 F.first("route_number", ignorenulls=True).alias("route_number"),
                 F.coalesce(F.max(F.when(F.trim(F.col("mode_of_transport")) == "OCE", F.lit("Y"))),
                            F.lit("N")).alias("is_ocean_route"),
                 F.round(F.sum(F.when(F.trim(F.col("mode_of_transport")) == "OCE",
                                      F.col("number_of_containers").cast("double")).otherwise(0.0)), 0).alias("route_container_count")))  # RSNCTR summed over OCE steps ONLY (matches report RSMOT='OCE')
    freight = transform_freight_buckets()

    # BOL weigh-ticket weights (F5549002) collapsed to ONE row per order line (gross/catch/max), so the
    # LEFT join can't fan the line grain out. Silver is pre-decoded, so no /10000 or /100 scaling.
    wt = (f5549002.groupBy("company_key_order_no", "document_order_invoice_e", "order_type", "line_number")
             .agg(F.first("gross_weight",   ignorenulls=True).alias("gross_weight"),
                  F.first("catch_weight",   ignorenulls=True).alias("catch_weight"),
                  F.first("maximum_weight", ignorenulls=True).alias("max_weight")))

    # F03012 customer-master-by-LOB → sold-to LOB category (joins F4211.SDAN8 = F03012.AIAN8). Collapse to one
    # row per address so the LEFT join can't fan the line grain out (a customer may have several LOB rows).
    lob = (f03012.groupBy("address_number")
              .agg(F.first("report_code_add_book_005", ignorenulls=True).alias("sold_to_lob_category_05")))
    # F49211 SO-detail tag file → deferred_entries_flag; 1:1 with the line — collapse defensively.
    tag = (f49211.groupBy("company_key_order_no", "document_order_invoice_e", "order_type", "line_number")
              .agg(F.first("deferred_entries_flag", ignorenulls=True).alias("deferred_entries_flag")))

    # collapse booking sources to ONE row per join key so denormalizing seal/booking/ocean
    # attributes can't fan the line grain out (b11 = booking detail, b01 = booking header)
    b11d = (b11.groupBy("company_key_order_no", "document_order_invoice_e", "order_type",
                        "line_number", "shipment_number")
               .agg(F.first("seal_no", ignorenulls=True).alias("seal_no"),
                    F.first("production_code",       ignorenulls=True).alias("production_code"),        # AK55PDCD
                    F.first("production_ship_notes", ignorenulls=True).alias("production_ship_notes"))) # AK55PDSHNT
    # A booking whose destination port (BA55DSTPT joined to F0101 ABAN8) is not
    # in the address book contributes NO booking attributes. Implemented as a left-semi (destination_port must
    # exist in F0101) that drops those booking rows BEFORE the aggregate.
    # Line grain is unaffected: b01d stays <=1 row per key and is LEFT-joined below.
    _dest_ab = f0101.select(F.col("address_number").alias("_dest_an8")).dropDuplicates()
    b01 = b01.join(_dest_ab, F.col("destination_port") == F.col("_dest_an8"), "left_semi")
    b01d = (b01.groupBy("company_key_order_no", "document_order_invoice_e", "order_type", "shipment_number")
               .agg(F.first("booking_no",           ignorenulls=True).alias("booking_no"),
                    F.first("bookingstatus",         ignorenulls=True).alias("booking_status"),   # BA55BKSTAT
                    F.first("destination_port",      ignorenulls=True).alias("destination_port"),
                    F.first("no_of_container",       ignorenulls=True).alias("no_of_container"),
                    F.first("ocean_del_terms",       ignorenulls=True).alias("ocean_del_terms"),
                    F.first("vessel_name",           ignorenulls=True).alias("vessel_name"),
                    F.first("date_earliest_pickup",  ignorenulls=True).alias("date_earliest_pickup"),
                    F.first("date_earliest_delivery", ignorenulls=True).alias("date_earliest_delivery"),  # BADEDL
                    F.first("date_latest_delivery",  ignorenulls=True).alias("date_latest_delivery"),
                    # extended ocean-booking display — BA55VONO/LODP/OCCR/REF1-3/BADLPU
                    F.first("voyage_no",             ignorenulls=True).alias("voyage_number"),
                    F.first("loading_port",          ignorenulls=True).alias("loading_port"),
                    F.first("ocean_carrier",         ignorenulls=True).alias("ocean_carrier"),
                    F.first("reference_01",          ignorenulls=True).alias("booking_reference_1"),
                    F.first("reference_02",          ignorenulls=True).alias("booking_reference_2"),
                    F.first("reference_03",          ignorenulls=True).alias("booking_reference_3"),
                    F.first("date_latest_pickup",    ignorenulls=True).alias("date_latest_pickup"),
                    # BA55ODREF/ROUT/EQTY/INDLT/INCO + BARQSJ
                    F.first("order_reference",       ignorenulls=True).alias("order_reference"),       # BA55ODREF
                    F.first("routing_notes",         ignorenulls=True).alias("routing_notes"),         # BA55ROUT
                    F.first("equipment_type",        ignorenulls=True).alias("equipment_type"),        # BA55EQTY
                    F.first("inland_delterms",       ignorenulls=True).alias("inland_delterms"),       # BA55INDLT
                    F.first("incoterms",             ignorenulls=True).alias("incoterms"),             # BA55INCO
                    F.first("date_requested_ship",   ignorenulls=True).alias("date_requested_ship")))  # BARQSJ

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
         .join(uom_str.alias("us"),
               (F.col("us.us_itm") == F.col("sd.identifier_short_item")) &
               (F.col("us.us_uom") == F.trim(F.col("sd.uom_as_input"))), "left")
         .join(conv_item.alias("ci"),
               (F.col("ci.itm") == F.col("sd.identifier_short_item")) &
               (F.col("ci.from_uom") == F.trim(F.col("sd.uom_as_input"))), "left")
         .join(route.alias("rt"), F.col("sd.shipment_number") == F.col("rt.shipment_number"), "left")
         .join(freight.alias("fr"), F.col("sd.shipment_number") == F.col("fr.shipment_number"), "left")
         .join(wt.alias("wt"),                                                                                # F5549002 BOL weigh-ticket weights (one row/line)
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

    # TN passes through as 1.0, else the item-specific F41002 factor.
    # Unresolved conversions stay NULL (no blanket 1.0 default); missing_conversion_flag marks those rows.
    conv_rate = F.coalesce(F.when(F.trim(F.col("sd.uom_as_input")) == "TN", F.lit(1.0)),
                           F.col("ci.conv_factor"))

    sel = j.select(
        # ── degenerate / order identifiers ──
        F.col("sd.company").alias("company"),
        F.col("sd.company_key_order_no").alias("company_key_order_no"),
        F.col("sd.order_type").alias("order_type"),
        F.col("sd.document_type").alias("document_type"),                    # SDDCT — invoice document type (≠ order_type SDDCTO)
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
        # next_status_num: physical INT copy of status_code_next. Direct Lake can't
        # reliably range-filter the STRING status; a blank/non-numeric status casts to NULL and is excluded.
        # Filter next_status_num in Power BI; keep displaying the string status_code_next.
        F.trim(F.col("sd.status_code_next")).cast("int").alias("next_status_num"),
        # last_status_num: physical INT copy of status_code_last (SDLTTR) — same purpose as
        # next_status_num, for range-filtering the last status in Power BI. Blank/non-numeric -> NULL.
        F.trim(F.col("sd.status_code_last")).cast("int").alias("last_status_num"),
        F.col("sd.freight_handling_code").alias("freight_handling_code"),
        F.trim(F.col("sd.mode_of_transport")).alias("mode_of_transport"),
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
        F.col("sd.date_release_julian").alias("release_date"),               # SDRSDJ
        F.col("sd.actual_ship_date").alias("actual_ship_date"),
        gl_expr.alias("gl_date"),
        F.col("sd.date_invoice_julian").alias("invoice_date"),
        F.col("sd.cancel_date").alias("cancel_date"),
        F.col("sd.date_price_effective_date").alias("line_price_effective_date"),
        F.col("sh.date_price_effective_date").alias("header_price_effective_date"),
        F.col("b01.date_earliest_pickup").alias("date_earliest_pickup"),
        F.col("b01.date_earliest_delivery").alias("date_earliest_delivery"),   # F5642B01 BADEDL
        F.col("b01.date_latest_delivery").alias("date_latest_delivery"),
        # ── item / uom ──
        F.col("sd.identifier_second_item").alias("second_item_number"),
        F.col("sd.identifier_third_item").alias("third_item_number"),   # SDAITM (3rd item number)
        F.col("sd.line_type").alias("line_type"),
        F.col("sd.uom_as_input").alias("uom"),
        F.col("sd.uom_primary").alias("uom_primary"),
        F.col("sd.uom_pricing").alias("uom_pricing"),
        conv_rate.alias("conversion_to_tons_rate"),
        F.when(conv_rate.isNull(), F.lit("Y")).otherwise(F.lit("N")).alias("missing_conversion_flag"),
        F.col("us.uom_structure").alias("uom_structure"),                       # UMUSTR (F41002)
        F.col("sd.payment_terms_code_01").alias("payment_terms"),               # SDPTC (F4211)
        # ── measures / numerics (line grain) ──
        F.col("sd.units_quantity_shipped").alias("quantity_shipped"),
        F.col("sd.units_primary_qty_order").alias("primary_quantity_ordered"),
        F.col("sd.units_transaction_qty").alias("transaction_quantity"),
        F.col("sd.amt_price_per_unit_02").alias("price_per_unit"),
        F.col("sd.uom_ent_up").alias("unit_price_primary"),                  # SDAPUM — unit price in primary/entered UOM (≠ price_per_unit SDUPRC)
        F.col("sd.sales_reporting_code_02").alias("major_prod_code"),
        F.col("sd.sales_reporting_code_04").alias("minor_prod_code"),
        # ── denormalized booking / ocean (shipment grain) ──
        F.col("b11.seal_no").alias("seal_no"),
        F.col("b11.production_code").alias("production_code"),                # F5642B11 AK55PDCD
        F.col("b11.production_ship_notes").alias("production_ship_notes"),    # F5642B11 AK55PDSHNT
        F.col("b01.booking_no").alias("booking_no"),
        F.col("b01.booking_status").alias("booking_status"),       # F5642B01 BA55BKSTAT
        F.col("b01.destination_port").alias("destination_port"),   # F5642B01 destination point
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
        F.coalesce(F.col("fr.total_freight"),    F.lit(0.0)).alias("total_freight"),    # all-charge-code shipment freight total
        F.coalesce(F.col("fr.freight_variance"), F.lit(0.0)).alias("freight_variance"),
        F.coalesce(F.col("fr.total_variance"),   F.lit(0.0)).alias("total_variance"),
        # ── page-level filtering attributes ──
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
        # ── additional display columns ──
        F.col("sd.zone_number").alias("zone_number"),                        # SDZON
        F.col("sd.hold_orders_code").alias("line_hold"),                     # SDHOLD LINE hold — ≠ header hold_orders_code (sh)
        F.col("rt.is_ocean_route").alias("is_ocean_route"),                  # F4941 RSMOT='OCE' any step
        F.col("rt.route_container_count").alias("route_container_count"),    # F4941 SUM(RSNCTR)
        F.col("wt.gross_weight").alias("gross_weight"),                      # F5549002 MIGRWT
        F.col("wt.catch_weight").alias("catch_weight"),                      # F5549002 MICTWT
        F.col("wt.max_weight").alias("max_weight"),                          # F5549002 MIMXWT
        F.col("sd.pull_signal").alias("pull_signal"),                        # SDPSIG
        F.col("sd.reference_02_vendor").alias("reference_02"),               # SDVR02
        F.col("sd.reference_ucis_no").alias("reference_03"),                 # SDVR03 (IFS order no)
        F.col("sd.primary_last_vendor_no").alias("vendor_number"),           # SDVEND
        F.col("sd.price_adjustment_schedule_n").alias("price_adjustment_schedule"),  # SDASN
        F.col("sd.user_reserved_code").alias("user_reserved_code"),          # SDURCD
        F.col("sd.price_override_code").alias("price_override_code"),         # SDPROV
        F.col("sd.user_id").alias("user_id"),                                # SDUSER
        F.col("sd.lot").alias("lot_number"),                                 # SDLOTN
        F.col("sd.serial_number_lot").alias("serial_number"),                # SDSERN
        F.col("sd.location").alias("location"),                              # SDLOCN
        F.col("sd.sales_reporting_code_05").alias("sales_reporting_code_05"),# SDSRP5
        F.col("lob.sold_to_lob_category_05").alias("sold_to_lob_category_05"),# F03012 AIAC05 (sold-to LOB, on SDAN8)
        F.col("tag.deferred_entries_flag").alias("deferred_entries_flag"),    # F49211 UDDEFF
        # ── remaining niche display source-columns (all present in Silver) ──
        F.col("sd.related_po_so_number").alias("related_po_so_number"),      # SDRORN
        F.col("sd.time_of_day").alias("time_of_day"),                        # SDTDAY
        F.col("sh.date_original_promisde").alias("original_promised_date"),  # SHOPDJ — distinct header date
        F.col("b01.voyage_number").alias("voyage_number"),                   # F5642B01 BA55VONO
        F.col("b01.loading_port").alias("loading_port"),                     # F5642B01 BA55LODP
        F.col("b01.ocean_carrier").alias("ocean_carrier"),                   # F5642B01 BA55OCCR
        F.col("b01.booking_reference_1").alias("booking_reference_1"),       # F5642B01 BA55REF1
        F.col("b01.booking_reference_2").alias("booking_reference_2"),       # F5642B01 BA55REF2
        F.col("b01.booking_reference_3").alias("booking_reference_3"),       # F5642B01 BA55REF3
        F.col("b01.date_latest_pickup").alias("date_latest_pickup"),         # F5642B01 BADLPU
        F.col("b01.order_reference").alias("order_reference"),                # F5642B01 BA55ODREF
        F.col("b01.routing_notes").alias("routing_notes"),                   # F5642B01 BA55ROUT
        F.col("b01.equipment_type").alias("equipment_type"),                 # F5642B01 BA55EQTY
        F.col("b01.inland_delterms").alias("inland_delterms"),               # F5642B01 BA55INDLT
        F.col("b01.incoterms").alias("incoterms"),                           # F5642B01 BA55INCO
        F.col("b01.date_requested_ship").alias("date_requested_ship"),       # F5642B01 BARQSJ
        # ── header-level (F4201) display columns — header AND line carrier/payment-terms differ, so the header
        #    values can't be substituted by the line columns. All from the already-inner-joined `sh`
        #    (1:1 with the order) — purely additive. ──
        F.col("sh.address_number").alias("header_sold_to"),                  # SHAN8  (header sold-to; ≠ line bill_to SDAN8)
        F.col("sh.date_transaction_julian").alias("header_order_date"),      # SHTRDJ (header order date; ≠ line order_date SDTRDJ)
        F.col("sh.carrier").alias("header_carrier_number"),                  # SHCARS (header carrier; ≠ line carrier_number SDCARS)
        F.col("sh.payment_terms_code_01").alias("header_payment_terms"),     # SHPTC  (header pay-terms; ≠ line payment_terms SDPTC)
        F.col("sh.address_number_parent").alias("header_parent"),            # SHPA8  (header parent; ≠ line address_number_parent SDPA8)
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
          # ── role-play date keys (yyyyMMdd). Date dimension REMOVED — these
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
    # one row per order line
    df = df.dropDuplicates(["sales_order_line_key"])
    df = _add_effective_price_flag(df)                        # has_effective_price (1:1, no fan-out)
    # pricing_issue_remark — derived label (two disjoint cases):
    #   'Unit Price Zero'    : SDUPRC=0 on an open line (next status < 620)
    #   'No effective price' : SDUPRC<>0, line shipped (actual_ship_date not null), but no active F4106 base
    #                          price covers it (has_effective_price='N')
    # NULL = no pricing issue.
    df = df.withColumn(
        "pricing_issue_remark",
        F.when((F.col("price_per_unit") == 0) & (F.col("next_status_num") < 620),
               F.lit("Unit Price Zero"))
         .when((F.col("price_per_unit") != 0) & F.col("actual_ship_date").isNotNull() & (F.col("has_effective_price") == "N"),
               F.lit("No effective price"))
         .otherwise(F.lit(None).cast("string")))
    return df.select("sales_order_line_key", "order_scope_key", *FACT_BUSINESS_COLS)


# ----------------------------------------------------------------------------
# 3) FACT SOURCES
# ----------------------------------------------------------------------------

# Declares each Silver source and how it relates to the F4211 spine.
# This fact's join graph is rich (header / address / item / booking /
# routing / freight / weigh-ticket / LOB / tag legs) and is applied inside build_fact() (and its
# helpers build_uom_cascades / transform_freight_buckets / _add_effective_price_flag), so join_pairs
# are [] here — the joins are not simple FK pairs. The list documents the source inventory and drives
# the RUN source preflight.  join: spine=F4211 order-line driver, union=F42119 history (optional),
# static=lookup / denormalized attribute source.
FACT_SOURCES = [
    {"silver": F4211,    "join": "spine",  "join_pairs": []},                    # SX order-line driver (grain)
    {"silver": F42119,   "join": "union",  "join_pairs": [], "optional": True},  # Sales Order History — unioned via _load_optional if present
    {"silver": F4201,    "join": "static", "join_pairs": []},                    # order header (hold / delivery instr / price-eff / orig-promise)
    {"silver": F0101,    "join": "static", "join_pairs": []},                    # destination address dedup only (_dest_ab)
    {"silver": F41002,   "join": "static", "join_pairs": []},                    # UoM->TN conversion + UMUSTR structure
    {"silver": F4941,    "join": "static", "join_pairs": []},                    # shipment routing (route_number / OCE flag / container count)
    {"silver": F4981,    "join": "static", "join_pairs": []},                    # freight-audit buckets (billable / payable / total) at shipment grain
    {"silver": F5642B01, "join": "static", "join_pairs": []},                    # ocean booking header (booking / vessel / voyage / ports / refs / pickup-delivery dates)
    {"silver": F5642B11, "join": "static", "join_pairs": []},                    # ocean booking detail (seal_no)
    {"silver": F5549002, "join": "static", "join_pairs": []},                    # BOL weigh-ticket weights (gross / catch / max)
    {"silver": F03012,   "join": "static", "join_pairs": []},                    # sold-to LOB category (AIAC05)
    {"silver": F49211,   "join": "static", "join_pairs": []},                    # SO-line tag (deferred_entries_flag)
    {"silver": F4106,    "join": "static", "join_pairs": []},                    # item base-price existence (has_effective_price)
]


# ----------------------------------------------------------------------------
# 4) RUN
# ----------------------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

# ── SOURCE PREFLIGHT — confirm every declared Silver source exists before building.
#    F42119 (Sales Order History) is optional: build_fact() unions it via _load_optional only if present. ──
for _s in FACT_SOURCES:
    _ok = spark.catalog.tableExists(sname(_s["silver"]))
    _tag = "OK" if _ok else ("OPTIONAL-missing (skipped)" if _s.get("optional") else "MISSING")
    print("  source {:<44s} {}".format(_s["silver"], _tag))

# ── BATCH BUILD — read the full Silver snapshot, run build_fact() ONCE, overwrite the fact.
#   MANUAL_OVERWRITE = True  -> drop + rebuild from the full Silver snapshot.
#   MANUAL_OVERWRITE = False -> build only if the fact is missing (re-run to refresh).
#   Plain overwrite (no Gold CDF).
_run_start = time.time()
if MANUAL_OVERWRITE or not spark.catalog.tableExists(gname(FACT)):
    print("== FULL LOAD ==")
    new = build_fact()
    (new.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(gname(FACT)))
    _rows   = new.count()
    _status = "built"
    print("  {} rows={:,}".format(gname(FACT), _rows))
else:
    print("== skip — {} exists and MANUAL_OVERWRITE=False ==".format(gname(FACT)))
    _rows, _status = None, "skipped"

_elapsed = round(time.time() - _run_start, 1)
_exit_payload = {
    "status":       _status,
    "table":        gname(FACT),
    "rows":         _rows,
    "elapsed_sec":  _elapsed,
    "end_time_utc": datetime.now(timezone.utc).isoformat(),
}
print("exit payload:", json.dumps(_exit_payload))
notebookutils.notebook.exit(json.dumps(_exit_payload))
