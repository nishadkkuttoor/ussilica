#!/usr/bin/env python
# coding: utf-8

# ## nb_eso1_gold_streaming
# 
# null

# In[1]:


# ## nb_eso1_gold_streaming
#
# **Single self-contained Gold processor** for Extended Sales Order 1 (Billable v
# Payable Freight). Merges the transform library + initial seed + continuous
# streaming into ONE notebook — no `# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %run` dependency. Builds/refreshes the new
# tables (`fact_sales_order_freight`, `dim_date`, `dim_item`) and relates to the
# REUSED dims (`rpt.dim_address_book` role views, `rpt.dim_plant`).
#
# Flow: constants → transforms → reused-dim preflight → seed-if-missing →
#       start CDF streams (foreachBatch MERGE) → refresh every 30 seconds.
#
# Streaming model adopted from the production CDC entrypoint nb_stream_prod.py:
#   • continuous trigger(processingTime="30 seconds") + awaitAnyTermination
#   • robust foreachBatch — persist(MEMORY_ONLY), per-batch failure isolation with
#     a QUARANTINE table (slice routed there so the checkpoint still advances; only
#     RAISE if quarantine also fails = infra outage), UTC+local log banners
#   • checkpoint namespaced per ENV (prod/test never collide); startingVersion knob
# Design: docs/ESO1_gold_layer_design.md


# In[2]:


# In[1]:


# =============================================================================
# CONFIG + CONSTANTS  (names per Fabric_Naming_Convention_Guidelines.pdf)
# =============================================================================
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

GOLD_SCHEMA = "lh_jde_gold.eso1"
RPT_SCHEMA  = "lh_jde_gold.rpt"

# ── refresh / runtime config (streaming concepts from nb_stream_prod.py) ──────
ENV             = "dev"                              # checkpoint namespacing — envs never collide
TRIGGER         = {"processingTime": "30 seconds"}   # ← continuous; refresh every 30 s
CKPT            = f"Files/checkpoints/eso1_{ENV}"    # one sub-path per stream, per env
STARTING_VERSION = None                              # None = latest CDF; set 0 to replay all changes
SEED_ON_START   = True                                # full batch seed if a table is missing
HOLD_SESSION    = True                                # awaitAnyTermination (always-on)
# ── manual reprocess switch ───────────────────────────────────────────────────
#   OVERWRITE = True  -> drop the Gold tables AND clear the stream checkpoints, then
#                        the seed rebuilds every table from the full Silver snapshot
#                        (use for a one-off full reprocess / backfill).
#   OVERWRITE = False -> incremental: keep tables + checkpoints, process only new CDC data.
OVERWRITE       = True
QUARANTINE      = f"{GOLD_SCHEMA}.eso1_stream_quarantine"   # failed micro-batch slices land here
LOCAL_TZ        = ZoneInfo("America/New_York")        # log banners show UTC + this zone

# ── Silver sources ────────────────────────────────────────────────────────────
SRC_SCHEMA   = "jde"
SRC_LAKEHOUSE = "lh_jde_silver"
F4211_TBL    = "f4211_sales_order_detail_file"
F4201_TBL    = "f4201_sales_order_header_file"
F0101_TBL    = "f0101_address_book_master"
F4101_TBL    = "f4101_item_master"
F41002_TBL   = "f41002_item_units_of_measure_conversion_factors"
F41003_TBL   = "f41003_unit_of_measure_standard_conversion"
F4074_TBL    = "f4074_price_adjustment_ledger_file"
F4981_TBL    = "f4981_freight_audit_history"
F5642B01_TBL = "f5642b01_custom_sales_order_entry_screen_header"
F5642B11_TBL = "f5642b11_custom_sales_order_entry_screen_detail"
F4941_TBL    = "f4941_shipment_routing_steps"

# ── Gold targets BUILT here (new, eso1) ────────────────────────────────────────
T_FACT      = f"{GOLD_SCHEMA}.fact_sales_order_freight"
T_DIM_DATE  = f"{GOLD_SCHEMA}.dim_date"
T_DIM_ITEM  = f"{GOLD_SCHEMA}.dim_item"

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

print(f"ESO1 Gold streaming processor — trigger {TRIGGER}  target {T_FACT}")


# In[3]:


# In[2]:


# =============================================================================
# HELPERS
# =============================================================================
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def load_silver_table(table_name):
    # df = spark.read.table(table_name)
    df = spark.table(f"{SRC_LAKEHOUSE}.{SRC_SCHEMA}.{table_name}")
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

def date_key(col):
    return F.when(col.isNotNull(), F.date_format(col, "yyyyMMdd").cast("int"))

def sk(*cols):
    return F.sha2(F.concat_ws("||", *[F.col(c).cast("string") if isinstance(c, str) else c.cast("string")
                                       for c in cols]), 256)

def record_hash(business_cols):
    return F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in business_cols]), 256)

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


# In[4]:


# In[3]:


# =============================================================================
# CDC MERGE — insert / update-on-change / soft-delete (design §5)
# =============================================================================
def cdc_merge(src_df, target_table, key_col, run_dt, cluster_by=None, soft_delete_scope_col=None):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    if not spark.catalog.tableExists(target_table):
        w = (src_df.write.format("delta").option("delta.enableDeletionVectors", "true")
             .mode("overwrite").option("overwriteSchema", "true"))
        # if cluster_by:
        #     w = w.clusterBy(*cluster_by)
        w.saveAsTable(target_table)
        print(f"  ✓ created {target_table} ({src_df.count():,} rows)")
        return
    tgt = DeltaTable.forName(spark, target_table)
    m = (tgt.alias("t").merge(src_df.alias("s"), f"t.{key_col} = s.{key_col}")
         .whenMatchedUpdateAll(condition="t.record_hash <> s.record_hash")
         .whenNotMatchedInsertAll())
    soft = {"is_deleted": F.lit(True), "gold_updated_timestamp": F.lit(run_dt).cast("timestamp")}
    if soft_delete_scope_col is not None:
        vals = [r[0] for r in src_df.select(soft_delete_scope_col).distinct().collect()]
        if vals:
            m = m.whenNotMatchedBySourceUpdate(condition=F.col(f"t.{soft_delete_scope_col}").isin(vals), set=soft)
    else:
        m = m.whenNotMatchedBySourceUpdate(set=soft)
    m.execute()
    print(f"  ✓ merged into {target_table}")


# In[6]:


# In[4]:


# =============================================================================
# DIM transforms — dim_date (static) + dim_item (F4101, natural PK)
# =============================================================================
def build_dim_date(start="2010-01-01", end="2051-12-31"):  # span covers JDE future sentinel dates (promised/cancel/delivery)
    df = (spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date")
          .withColumn("date_key",   F.date_format("date", "yyyyMMdd").cast("int"))
          .withColumn("year", F.year("date")).withColumn("quarter", F.quarter("date"))
          .withColumn("month", F.month("date")).withColumn("month_name", F.date_format("date", "MMMM"))
          .withColumn("day", F.dayofmonth("date")).withColumn("day_of_week", F.dayofweek("date"))
          .withColumn("day_name", F.date_format("date", "EEEE")).withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
          .withColumn("week_of_year", F.weekofyear("date")).withColumn("iso_week", F.dayofweek("date"))
          .withColumn("week_start_date", F.date_sub("date", (F.dayofweek("date") + 5) % 7))
          .withColumn("week_end_date", F.date_add(F.col("week_start_date"), 6))
          .withColumn("year_week", F.concat_ws("-", F.year("date"),
                                               F.concat(F.lit("W"), F.lpad(F.weekofyear("date"), 2, "0"))))
          # ── added reporting attributes ──
          .withColumn("year_month", F.date_format("date", "yyyyMM").cast("int"))
          .withColumn("month_short_name", F.date_format("date", "MMM"))
          .withColumn("month_year_label", F.date_format("date", "MMM yyyy"))
          .withColumn("quarter_name", F.concat(F.lit("Q"), F.quarter("date")))
          .withColumn("year_quarter", F.concat_ws("-", F.year("date"),
                                                  F.concat(F.lit("Q"), F.quarter("date"))))
          .withColumn("day_of_year", F.dayofyear("date"))
          .withColumn("is_weekday", ~F.dayofweek("date").isin(1, 7)))
    return df.select("date_key", "date", "year", "quarter", "quarter_name", "year_quarter",
                     "month", "month_name", "month_short_name", "month_year_label", "year_month",
                     "day", "day_of_year", "day_of_week", "day_name", "is_weekend", "is_weekday",
                     "week_of_year", "iso_week", "year_week", "week_start_date", "week_end_date")

def transform_dim_item(run_dt, restrict_item=None):
    f4101 = load_silver_table(F4101_TBL)
    if restrict_item is not None:
        f4101 = f4101.join(restrict_item.alias("r"),
                           f4101["identifier_short_item"] == F.col("r.identifier_short_item"), "left_semi")
    df = (f4101.select(F.col("identifier_short_item").alias("item_number_short"),
                       F.col("description_line_01").alias("item_name"),
                       F.col("uom_weight").alias("uom_weight"),
                       F.col("date_updated").alias("_src_ts"))
          .dropDuplicates(["item_number_short"]))
    bcols = ["item_number_short", "item_name", "uom_weight"]
    df = (df.withColumn("is_deleted", F.lit(False))
            .withColumn("source_commit_timestamp", F.col("_src_ts").cast("timestamp"))
            .withColumn("gold_updated_timestamp", F.lit(run_dt).cast("timestamp"))
            .withColumn("record_hash", record_hash(bcols)).drop("_src_ts"))
    return df.select(*bcols, "is_deleted", "source_commit_timestamp", "gold_updated_timestamp", "record_hash")


# In[7]:


# In[5]:


# =============================================================================
# UoM -> TN cascades + freight buckets (F4981 -> shipment grain)
# =============================================================================
def build_uom_cascades():
    f41002 = load_silver_table(F41002_TBL); f41003 = load_silver_table(F41003_TBL)
    item_fwd = (f41002.filter((F.trim("related_uom") == "TN") & (F.col("conversion_factor") != 0))
                .select(F.col("identifier_short_item").alias("itm"), F.trim("uom").alias("from_uom"),
                        F.col("conversion_factor").cast("double").alias("conv_factor")))
    item_rev = (f41002.filter((F.trim("uom") == "TN") & (F.col("conversion_factor") != 0))
                .select(F.col("identifier_short_item").alias("itm"), F.trim("related_uom").alias("from_uom"),
                        (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_factor")))
    std_fwd = (f41003.filter((F.trim("related_uom") == "TN") & (F.col("conversion_factor") != 0))
               .select(F.trim("uom").alias("from_uom"), F.col("conversion_factor").cast("double").alias("conv_factor")))
    std_rev = (f41003.filter((F.trim("uom") == "TN") & (F.col("conversion_factor") != 0))
               .select(F.trim("related_uom").alias("from_uom"),
                       (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_factor")))
    return item_fwd.unionByName(item_rev), std_fwd.unionByName(std_rev)

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


# In[8]:


# In[6]:


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
    # ── date keys (role-played to dim_date) ──
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

def transform_fact(run_dt, restrict_orders=None):
    f4211 = load_silver_table(F4211_TBL); f4201 = load_silver_table(F4201_TBL)
    f0101 = load_silver_table(F0101_TBL); f4101 = load_silver_table(F4101_TBL)
    b01   = load_silver_table(F5642B01_TBL); b11 = load_silver_table(F5642B11_TBL)
    f4074 = load_silver_table(F4074_TBL); f4941 = load_silver_table(F4941_TBL)
    f41002 = load_silver_table(F41002_TBL)
    conv_item, conv_std = build_uom_cascades()

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

    # F4074 price-adjustment ledger — joined DIRECTLY (no GROUP BY / aggregation) so the
    # filter fields carry their ACTUAL row-level values for slicing:
    #   • price_adjustment_type -> ALAST as-is (e.g. "A03", "FRTHIDE")
    #   • freight_factor_value  -> ALUPRC as-is (report ReportColumn14)
    f4074w = f4074.select(
        F.col("company_key_order_no"), F.col("document_order_invoice_e"),
        F.col("order_type"), F.col("line_number"),
        F.trim(F.col("price_adjustment_type")).alias("price_adjustment_type"),
        F.col("amt_price_per_unit_02").cast("double").alias("freight_factor_value"))

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
         .join(conv_std.alias("cs"), F.col("cs.from_uom") == F.trim(F.col("sd.uom_as_input")), "left")
         .join(f4074w.alias("al"),
               (F.col("al.company_key_order_no") == F.col("sd.company_key_order_no")) &
               (F.col("al.document_order_invoice_e") == F.col("sd.document_order_invoice_e")) &
               (F.col("al.order_type") == F.col("sd.order_type")) &
               (F.col("al.line_number") == F.col("sd.line_number")), "left")
         .join(route.alias("rt"), F.col("sd.shipment_number") == F.col("rt.shipment_number"), "left")
         .join(freight.alias("fr"), F.col("sd.shipment_number") == F.col("fr.shipment_number"), "left"))

    conv_rate = F.coalesce(F.when(F.trim(F.col("sd.uom_as_input")) == "TN", F.lit(1.0)),
                           F.col("ci.conv_factor"), F.col("cs.conv_factor"), F.lit(1.0))
    conv_found = F.coalesce(F.when(F.trim(F.col("sd.uom_as_input")) == "TN", F.lit(1.0)),
                            F.col("ci.conv_factor"), F.col("cs.conv_factor"))

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
        F.col("sd.date_updated").alias("_src_ts"),
    ).distinct()

    df = (sel
          .withColumn("quantity_shipped_tons", F.col("quantity_shipped") * F.col("conversion_to_tons_rate"))
          .withColumn("price_quantity_shipped", F.col("price_per_unit") * F.col("quantity_shipped"))
          # ── role-playing date keys (yyyyMMdd) → dim_date ──
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
            .withColumn("order_scope_key", sk("company_key_order_no", "order_type", "order_number"))
            .withColumn("record_hash", record_hash(FACT_BUSINESS_COLS))
            .withColumn("is_deleted", F.lit(False))
            .withColumn("source_commit_timestamp", F.col("_src_ts").cast("timestamp"))
            .withColumn("gold_updated_timestamp", F.lit(run_dt).cast("timestamp")).drop("_src_ts"))
    audit = ["is_deleted", "source_commit_timestamp", "gold_updated_timestamp", "record_hash"]
    return df.select("sales_order_line_key", "order_scope_key", *FACT_BUSINESS_COLS, *audit)


# In[9]:


# In[7]:


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


# In[10]:


# In[7b]:


# =============================================================================
# MANUAL OVERWRITE RESET (controlled by OVERWRITE flag in In[1])
#   • OVERWRITE=True  -> drop the Gold tables BUILT here + clear the stream
#     checkpoints. The seed cell below then recreates each table from the full
#     Silver snapshot, and the streams (started in In[10]) replay from a clean
#     checkpoint. Use for a one-off full reprocess / backfill.
#   • OVERWRITE=False -> no-op: tables + checkpoints are retained and only new
#     (incremental) CDC data is merged. (default)
# Reused rpt dims are owned elsewhere and are never touched here.
# =============================================================================
def _clear_checkpoint(path):
    """Recursively remove a checkpoint dir via the available Fabric fs util."""
    import importlib
    for mod in ("notebookutils", "mssparkutils"):
        try:
            fs = importlib.import_module(mod).fs
        except Exception:
            continue
        try:
            fs.rm(path, True)
            print(f"  ✓ cleared checkpoint {path}")
        except Exception as e:
            print(f"  (no checkpoint to clear at {path}: {e})")
        return
    print(f"  WARN: no fs util (notebookutils/mssparkutils) available — clear {path} manually")

if OVERWRITE:
    print("OVERWRITE=True — full reprocess: dropping Gold tables and clearing checkpoints")
    for t in [T_FACT, T_DIM_ITEM, T_DIM_DATE]:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
        print(f"  ✓ dropped {t}")
    _clear_checkpoint(CKPT)
    print("✓ overwrite reset complete — seed will rebuild from the full Silver snapshot")
else:
    print("OVERWRITE=False — incremental mode: tables + checkpoints retained")


# In[11]:


# In[8]:


# =============================================================================
# SEED-IF-MISSING — full batch load so the streams have base tables to MERGE into
# (after an OVERWRITE reset the tables are gone, so this performs a full rebuild)
# =============================================================================
if SEED_ON_START:
    run_dt = datetime.now()
    if not spark.catalog.tableExists(T_DIM_DATE):
        (build_dim_date().write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(T_DIM_DATE))
        print(f"  ✓ seeded {T_DIM_DATE}")
    else:
        print(f"  exists, skip : {T_DIM_DATE}")
    if not spark.catalog.tableExists(T_DIM_ITEM):
        cdc_merge(transform_dim_item(run_dt), T_DIM_ITEM, "item_number_short", run_dt)
    else:
        print(f"  exists, skip : {T_DIM_ITEM}")
    if not spark.catalog.tableExists(T_FACT):
        cdc_merge(transform_fact(run_dt), T_FACT, "sales_order_line_key", run_dt,
                  cluster_by=["branch_plant", "shipment_number"])
    else:
        print(f"  exists, skip : {T_FACT}")
print("✓ seed-if-missing complete")


# In[12]:


# In[9]:


# =============================================================================
# foreachBatch — robust micro-batch handler (pattern from nb_stream_prod.py):
#   • empty-batch guard  • persist(MEMORY_ONLY) so the recompute isn't re-run
#   • per-batch try/except → on failure, QUARANTINE the slice (don't drop events)
#     and let the checkpoint advance; only RAISE if quarantine ALSO fails (infra)
#   • UTC + local log banner per batch  • unpersist in finally
# =============================================================================
def _banner(name, batch_id, rows):
    now_utc = datetime.now(timezone.utc); now_local = now_utc.astimezone(LOCAL_TZ)
    print(f">>> {name} batch={batch_id} rows={rows}  "
          f"[UTC {now_utc:%Y-%m-%d %H:%M:%S} | {now_local:%Y-%m-%d %H:%M:%S %Z}]")

def _quarantine(name, batch_df, batch_id, err):
    msg = (getattr(err, "java_exception", None).getMessage()
           if getattr(err, "java_exception", None) else repr(err))[:1000]
    q = (batch_df.select(F.to_json(F.struct(*batch_df.columns)).alias("event_json"))
         .withColumn("stream", F.lit(name))
         .withColumn("quarantine_reason", F.lit(msg))
         .withColumn("quarantine_batch_id", F.lit(str(batch_id)))
         .withColumn("quarantined_at", F.current_timestamp()))
    q.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(QUARANTINE)

def make_handler(name, fn):
    """Wrap a (batch_df -> rows) transform+MERGE fn with the robust pattern."""
    def handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        batch_df.persist(StorageLevel.MEMORY_ONLY)
        try:
            rows = fn(batch_df)
            _banner(name, batch_id, rows)
        except Exception as e:
            print(f"  ERROR {name} batch={batch_id}: {e!r} — quarantining slice")
            try:
                _quarantine(name, batch_df, batch_id, e)
                print(f"  WARN {name}: slice routed to {QUARANTINE} (checkpoint advances)")
            except Exception as qe:
                print(f"  CRITICAL {name}: quarantine also failed: {qe!r} — failing batch")
                raise
        finally:
            batch_df.unpersist()
    return handler

# ── per-source transforms (return merged row count) ───────────────────────────
def _dim_item(batch_df):
    run_dt = datetime.now()
    itm = batch_df.select("identifier_short_item").distinct().where(F.col("identifier_short_item").isNotNull())
    if itm.rdd.isEmpty():
        return 0
    src = transform_dim_item(run_dt, restrict_item=itm)
    cdc_merge(src, T_DIM_ITEM, "item_number_short", run_dt, soft_delete_scope_col="item_number_short")
    return src.count()

def _fact_from_f4211(batch_df):
    run_dt = datetime.now()
    orders = (batch_df.select(F.col("company_key_order_no"), F.col("order_type"),
                              F.col("document_order_invoice_e").alias("order_number")).distinct())
    if orders.rdd.isEmpty():
        return 0
    src = transform_fact(run_dt, restrict_orders=orders)
    cdc_merge(src, T_FACT, "sales_order_line_key", run_dt,
              cluster_by=["branch_plant", "shipment_number"], soft_delete_scope_col="order_scope_key")
    return src.count()

def _fact_from_f4981(batch_df):
    run_dt = datetime.now()
    ships = batch_df.select("shipment_number").distinct().where(F.col("shipment_number").isNotNull())
    if ships.rdd.isEmpty():
        return 0
    f4211 = load_silver_table(F4211_TBL)   # no company filter — map all shipments to orders
    orders = (f4211.join(ships.alias("s"), f4211["shipment_number"] == F.col("s.shipment_number"), "left_semi")
              .select(F.col("company_key_order_no"), F.col("order_type"),
                      F.col("document_order_invoice_e").alias("order_number")).distinct())
    if orders.rdd.isEmpty():
        return 0
    src = transform_fact(run_dt, restrict_orders=orders)
    cdc_merge(src, T_FACT, "sales_order_line_key", run_dt,
              cluster_by=["branch_plant", "shipment_number"], soft_delete_scope_col=None)
    return src.count()


# In[13]:


# In[10]:


# =============================================================================
# START STREAMS — Silver Change Data Feed -> foreachBatch -> MERGE, every 30 s
# =============================================================================
def start_cdf_stream(silver_table, ckpt_name, name, fn):
    st = f"{SRC_LAKEHOUSE}.{SRC_SCHEMA}.{silver_table}"
    rs = (spark.readStream.format("delta").option("readChangeFeed", "true")
          .option("ignoreDeletes", "false"))
    if STARTING_VERSION is not None:                  # replay from a version (else latest)
        rs = rs.option("startingVersion", STARTING_VERSION)
    q = (rs.table(st).writeStream
         .option("checkpointLocation", f"{CKPT}/{ckpt_name}")
         .trigger(**TRIGGER).foreachBatch(make_handler(name, fn)).start())
    print(f"  started: {name}  (source {st}, ckpt {CKPT}/{ckpt_name}, query {q.id})")
    return q

q_item    = start_cdf_stream(F4101_TBL, "dim_item",   "dim_item",   _dim_item)
q_fact    = start_cdf_stream(F4211_TBL, "fact_f4211", "fact_f4211", _fact_from_f4211)
q_freight = start_cdf_stream(F4981_TBL, "fact_f4981", "fact_f4981", _fact_from_f4981)
print(f"Started 3 streams — continuous, trigger {TRIGGER}. Failed slices -> {QUARANTINE}. "
      "Reused dims (rpt) refresh via their own jobs.")


# In[30]:


# In[11]:


# =============================================================================
# HOLD THE SESSION (always-on). For a scheduled drain-and-stop run, set the
# TRIGGER to {"availableNow": True} and HOLD_SESSION = False above.
# =============================================================================
if HOLD_SESSION:
    spark.streams.awaitAnyTermination()

