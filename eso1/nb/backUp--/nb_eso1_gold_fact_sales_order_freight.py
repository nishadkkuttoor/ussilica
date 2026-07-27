#!/usr/bin/env python
# coding: utf-8

# ## nb_eso1_gold_fact_sales_order_freight
# 
# null

# In[1]:


# ## nb_eso1_gold_fact_sales_order_freight
#
# **Gold `fact_sales_order_freight` processor** for Extended Sales Order 1 (Billable v
# Payable Freight). Builds and continuously refreshes ONE table —
# `lh_jde_gold.eso1.fact_sales_order_freight` (order-line grain; freight denormalized) —
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
OVERWRITE       = False    # ⚠ ONE-OFF full reprocess (cdf schema) — set back to False after this run

# Serialises all fact-table writes: foreachBatch handlers run in separate driver
# threads, so this lock makes the F4211 and F4981 streams take turns (delete+append
# on the same Gold fact never overlaps). (Pattern from ESO7 v2.)
_FACT_LOCK      = threading.Lock()

# ── Silver sources ────────────────────────────────────────────────────────────
SRC_SCHEMA   = "cdf"     # Silver Change Data Feed schema (was "jde"); CDF must be
                         #   enabled on every STREAMED source (F4211/F4981) read below.
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

# ── Gold target BUILT here (new, eso1) ─────────────────────────────────────────
T_FACT      = f"{GOLD_SCHEMA}.fact_sales_order_freight"

# ── REUSED dims (read-only; owned by old_nb jobs) ─────────────────────────────
R_DIM_AB      = f"{RPT_SCHEMA}.dim_address_book"
R_DIM_PLANT   = f"{RPT_SCHEMA}.dim_plant"
R_DIM_SHIP_TO = f"{GOLD_SCHEMA}.dim_address_ship_to"
R_DIM_SOLD_TO = f"{GOLD_SCHEMA}.dim_address_sold_to"
R_DIM_CARRIER = f"{GOLD_SCHEMA}.dim_address_carrier"
R_DIM_PARENT = f"{GOLD_SCHEMA}.dim_address_parent"

# ── report scaling (business WHERE filters removed — fact now carries ALL rows) ──
# (former Hubble hard filters COMPANIES / ALAST_WHITELIST / ship-to address-type gate /
#  line_type='S' / status<>980 / vendor_invoice were dropped per the no-filter requirement.)
SHIFT_FACTOR    = 1.0    # placeholder — see design §11 (ShiftFactor open item)

print(f"ESO1 Gold fact processor — trigger {TRIGGER}  target {T_FACT}")


# In[2]:


# In[2]:


# =============================================================================
# HELPERS
# =============================================================================
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def sname(table_name):
    """Fully-qualified Silver source name (== ESO7 v2 sname)."""
    return f"{SRC_LAKEHOUSE}.{SRC_SCHEMA}.{table_name}"

def load_silver_table(table_name):
    df = spark.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

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
                F.first(F.trim("city"), ignorenulls=True).alias("freight_city"),
                F.first(F.trim("state"), ignorenulls=True).alias("freight_state"),
                F.first(F.trim("zip_code_postal"), ignorenulls=True).alias("freight_zip"))
            .withColumn("total_billable",   F.round(F.col("billable_freight") + F.col("billable_fuel"), 2))
            .withColumn("total_payable",    F.round(F.col("payable_freight")  + F.col("payable_fuel"), 2))
            .withColumn("freight_variance", F.round(F.col("billable_freight") - F.col("payable_freight"), 2))
            .withColumn("total_variance",   F.round(F.col("total_billable")   - F.col("total_payable"), 2))
            .withColumn("shift_factor_applied", F.lit(SHIFT_FACTOR).cast("double")))


# In[5]:


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
    "hold_orders_code", "status_code_last", "status_code_next",
    "freight_handling_code", "freight_handling_code_audit",
    "mode_of_transport", "route_number", "container_id", "transaction_originator",
    "delivery_instruct_line_01", "delivery_instruct_line_02", "gl_class",
    "sales_reporting_code_01", "sales_reporting_code_03",
    # ── date keys (role-play FKs to a reused/conformed dim_date, owned elsewhere) ──
    "order_date_key", "requested_date_key", "scheduled_pick_date_key",
    "promised_ship_date_key", "ship_date_key", "gl_date_key", "invoice_date_key",
    "cancel_date_key", "line_price_effective_date_key", "header_price_effective_date_key",
    "earliest_pickup_date_key", "latest_delivery_date_key",
    # ── address / dimension FKs ──
    "ship_to", "bill_to", "carrier_number", "address_number_parent",
    "item_number_short", "branch_plant",
    # ── raw event dates ──
    "order_date", "requested_date", "scheduled_pick_date", "promised_ship_date",
    "actual_ship_date", "gl_date", "invoice_date", "cancel_date",
    "line_price_effective_date", "header_price_effective_date",
    "date_earliest_pickup", "date_latest_delivery",
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
    "seal_no", "booking_no", "destination_port", "dest_point_name_alpha",
    "no_of_container", "ocean_del_terms", "vessel_name",
    # ── denormalized freight location + buckets (shipment grain) ──
    "freight_city", "freight_state", "freight_zip",
    "billable_freight", "billable_fuel", "total_billable",
    "payable_freight", "payable_fuel", "total_payable",
    "freight_variance", "total_variance", "shift_factor_applied",
    "is_primary_shipment_line",
]

def transform_fact(restrict_orders=None):
    f4211 = load_silver_table(F4211_TBL); f4201 = load_silver_table(F4201_TBL)
    f0101 = load_silver_table(F0101_TBL); f4101 = load_silver_table(F4101_TBL)
    b01   = load_silver_table(F5642B01_TBL); b11 = load_silver_table(F5642B11_TBL)
    f4074 = load_silver_table(F4074_TBL); f4941 = load_silver_table(F4941_TBL)
    f41002 = load_silver_table(F41002_TBL)
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
                  F.col("amt_price_per_unit_02").cast("double").alias("freight_factor_value"))
              .withColumn("_alrn", F.row_number().over(_alw))
              .where(F.col("_alrn") == 1).drop("_alrn"))

    # F41002 UOM structure (UMUSTR) — actual value, joined directly (no aggregation)
    uom_str = f41002.select(F.col("identifier_short_item").alias("us_itm"),
                            F.trim(F.col("uom")).alias("us_uom"),
                            F.col("uom_structure").alias("uom_structure"))
    route = f4941.groupBy("shipment_number").agg(F.first("route_number", ignorenulls=True).alias("route_number"))
    freight = transform_freight_buckets()

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
                    F.first("date_latest_delivery",  ignorenulls=True).alias("date_latest_delivery")))

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
         .join(f0101.alias("dp"), F.col("dp.address_number") == F.col("b01.destination_port"), "left")  # F0101_1 dest-point name
         .join(f0101.alias("st"), F.col("st.address_number") == F.col("sd.address_number_ship_to"), "left")  # ship-to F0101 filter attrs (SIC / cat05 / cat14 / search type)
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
         .join(freight.alias("fr"), F.col("sd.shipment_number") == F.col("fr.shipment_number"), "left"))

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
        F.col("b01.destination_port").alias("destination_port"),
        F.col("dp.name_alpha").alias("dest_point_name_alpha"),
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
        F.coalesce(F.col("fr.freight_variance"), F.lit(0.0)).alias("freight_variance"),
        F.coalesce(F.col("fr.total_variance"),   F.lit(0.0)).alias("total_variance"),
    ).distinct()

    df = (sel
          .withColumn("quantity_shipped_tons", F.col("quantity_shipped") * F.col("conversion_to_tons_rate"))
          .withColumn("price_quantity_shipped", F.col("price_per_unit") * F.col("quantity_shipped"))
          # ── role-play date keys (yyyyMMdd) → reused/conformed dim_date (owned elsewhere) ──
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
    return df.select("sales_order_line_key", "order_scope_key", *FACT_BUSINESS_COLS)


# In[6]:


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
# the two per-stream checkpoint dirs (names must match the queryName()s in the start cell)
_CKPT_PATHS = [f"{CKPT}/fact__{F4211_TBL}", f"{CKPT}/fact__{F4981_TBL}"]

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
_STREAM_NAMES = {"fact__" + F4211_TBL, "fact__" + F4981_TBL}
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
    _init_ver = {t: current_version(t) for t in (F4211_TBL, F4981_TBL)}
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

print(f"== started 2 streams — continuous, trigger {TRIGGER}. Target {T_FACT}. "
      "Reused dims (rpt) + dim_item refresh via their own jobs. ==")
spark.streams.awaitAnyTermination()

