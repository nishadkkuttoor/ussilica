#!/usr/bin/env python
# coding: utf-8

# ## nb_eso1_transforms
#
# Shared transform + CDC-MERGE library for the Extended Sales Order 1
# (Billable v Payable Freight) Gold layer. **Defines functions only — no side
# effects.** `%run` this from `nb_backfill_gold_eso1` and `nb_stream_silver_to_gold`
# so the batch backfill and the continuous stream use ONE copy of the logic.
#
# Design: docs/ESO1_gold_layer_design.md
# Sources: lh_jde_silver.jde.* (already decoded — no Julian/decimal scaling here)

# In[1]:


# =============================================================================
# CONSTANTS  (all names per Fabric_Naming_Convention_Guidelines.pdf)
# =============================================================================
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

GOLD_SCHEMA = "lh_jde_gold.otc"

# ── Silver sources ────────────────────────────────────────────────────────────
F4211_TBL    = "jde.f4211_sales_order_detail_file"          # driver — order line
F4201_TBL    = "jde.f4201_sales_order_header_file"          # SO header
F0101_TBL    = "jde.f0101_address_book_master"              # address (ship-to / carrier / bill-to)
F0116_TBL    = "jde.f0116_address_by_date"                  # address detail (city/state/zip)
F4101_TBL    = "jde.f4101_item_master"                      # item
F41002_TBL   = "jde.f41002_item_units_of_measure_conversion_factors"
F41003_TBL   = "jde.f41003_unit_of_measure_standard_conversion"
F4074_TBL    = "jde.f4074_price_adjustment_ledger_file"     # freight factor
F4981_TBL    = "jde.f4981_freight_audit_history"            # freight $
F5642B01_TBL = "jde.f5642b01_custom_sales_order_entry_screen_header"
F5642B11_TBL = "jde.f5642b11_custom_sales_order_entry_screen_detail"
F4941_TBL    = "jde.f4941_shipment_routing_steps"           # route #

# ── Gold targets BUILT by this solution (new, otc) ───────────────────────────
T_FACT          = f"{GOLD_SCHEMA}.fact_sales_order_freight"
T_DIM_DATE      = f"{GOLD_SCHEMA}.dim_date"
T_DIM_ITEM      = f"{GOLD_SCHEMA}.dim_item"

# ── REUSED conformed dims (built/maintained by old_nb jobs; READ-ONLY here) ───
# Do NOT rebuild these — relate the fact to them on their NATURAL keys.
RPT_SCHEMA      = "lh_jde_gold.rpt"
R_DIM_PLANT     = f"{RPT_SCHEMA}.dim_plant"               # PK plant_code
R_DIM_AB        = f"{RPT_SCHEMA}.dim_address_book"        # PK address_number
R_DIM_SHIP_TO   = f"{RPT_SCHEMA}.dim_address_ship_to"     # role view over dim_address_book
R_DIM_SOLD_TO   = f"{RPT_SCHEMA}.dim_address_sold_to"     # role view (bill-to)
R_DIM_CARRIER   = f"{RPT_SCHEMA}.dim_address_carrier"     # role view (carrier)

# ── Spec §4.2 hard filters (exactly per Hubble WHERE / Silver analysis §6.1) ──
COMPANIES         = ["00640", "00645"]
ALAST_WHITELIST   = ["A03", "FRTHIDE", "FRTTAXN", "FRTTAXY"]
DLV_DISCOUNT      = 0.253910373    # DLV freight-handling discount factor
SHIFT_FACTOR      = 1.0            # placeholder — see design §11 (ShiftFactor open item)


# In[2]:


# ── Helper: load a Silver table, filter is_delete=0 where present, drop CDC cols
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def load_silver_table(spark, table_name):
    """Read a Silver table. F4074/F4101/F4981 carry is_delete -> filter to 0."""
    df = spark.read.table(table_name)
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])


def date_key(col):
    """Smart date key yyyyMMdd (int) for dim_date relationships. NULL-safe."""
    return F.when(col.isNotNull(), F.date_format(col, "yyyyMMdd").cast("int"))


def sk(*cols):
    """Deterministic surrogate / business key = sha2 of business columns (replay-safe)."""
    return F.sha2(F.concat_ws("||", *[F.col(c).cast("string") if isinstance(c, str) else c.cast("string")
                                       for c in cols]), 256)


def record_hash(df, business_cols):
    """sha2 over business columns — lets MERGE skip no-op writes."""
    return F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in business_cols]), 256)


def pick_col(df, candidates, alias):
    """First-present-column resolver. Silver names for a few JDE aliases (e.g.
    F4211 SDDGL = GL Date) aren't 100% confirmed; resolve at runtime, else NULL.
    Pass plain column names (no table alias) — used before the table is aliased."""
    for c in candidates:
        if c in df.columns:
            return (c, True)
    return (candidates[0], False)   # name to alias-to, but column is absent -> emit NULL


# In[3]:


# =============================================================================
# CDC MERGE — single idempotent template for fact + all dims (design §5)
# =============================================================================
def cdc_merge(spark, src_df, target_table, key_col, run_dt,
              cluster_by=None, soft_delete_scope_col=None):
    """
    Insert / update-on-change / soft-delete MERGE.
      • first run            -> create table (+ liquid clustering, deletion vectors)
      • whenMatched + hash≠  -> UPDATE
      • whenNotMatched       -> INSERT
      • whenNotMatchedBySource -> is_deleted = true  (rows absent from src)

    soft_delete_scope_col: for STREAMING microbatches, restrict the not-matched-by-
      source soft-delete to the keys present in this batch's scope (e.g. the changed
      shipments / orders) instead of the whole table. Pass None for a full backfill.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    if not spark.catalog.tableExists(target_table):
        writer = (src_df.write.format("delta")
                  .option("delta.enableDeletionVectors", "true")
                  .mode("overwrite").option("overwriteSchema", "true"))
        if cluster_by:
            writer = writer.clusterBy(*cluster_by)
        writer.saveAsTable(target_table)
        print(f"✓ created {target_table} ({src_df.count():,} rows)")
        return

    tgt = DeltaTable.forName(spark, target_table)
    merge = (tgt.alias("t")
             .merge(src_df.alias("s"), f"t.{key_col} = s.{key_col}")
             .whenMatchedUpdateAll(condition="t.record_hash <> s.record_hash")
             .whenNotMatchedInsertAll())

    soft = {"is_deleted": F.lit(True), "gold_updated_timestamp": F.lit(run_dt).cast("timestamp")}
    if soft_delete_scope_col is not None:
        # only soft-delete within the scope keys this batch actually recomputed
        scope_vals = [r[0] for r in src_df.select(soft_delete_scope_col).distinct().collect()]
        if scope_vals:
            cond = F.col(f"t.{soft_delete_scope_col}").isin(scope_vals)
            merge = merge.whenNotMatchedBySourceUpdate(condition=cond, set=soft)
    else:
        merge = merge.whenNotMatchedBySourceUpdate(set=soft)
    merge.execute()
    print(f"✓ merged into {target_table}")


# In[4]:


# =============================================================================
# DIM_DATE — static calendar (design §3.1).  Built once; not streamed.
# =============================================================================
def build_dim_date(spark, start="2015-01-01", end="2031-12-31"):
    df = (spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date")
          .withColumn("date_key",      F.date_format("date", "yyyyMMdd").cast("int"))
          .withColumn("year",          F.year("date"))
          .withColumn("quarter",       F.quarter("date"))
          .withColumn("month",         F.month("date"))
          .withColumn("month_name",    F.date_format("date", "MMMM"))
          .withColumn("day",           F.dayofmonth("date"))
          .withColumn("day_of_week",   F.dayofweek("date"))
          .withColumn("day_name",      F.date_format("date", "EEEE"))
          .withColumn("is_weekend",    F.dayofweek("date").isin(1, 7))
          .withColumn("week_of_year",  F.weekofyear("date"))
          .withColumn("iso_week",      F.date_format("date", "w").cast("int"))
          .withColumn("week_start_date", F.date_sub("date", (F.dayofweek("date") + 5) % 7))   # Monday
          .withColumn("week_end_date",   F.date_add(F.col("week_start_date"), 6))
          .withColumn("year_week",     F.concat_ws("-", F.year("date"),
                                                   F.concat(F.lit("W"), F.lpad(F.weekofyear("date"), 2, "0")))))
    cols = ["date_key", "date", "year", "quarter", "month", "month_name", "day",
            "day_of_week", "day_name", "is_weekend", "week_of_year", "iso_week",
            "year_week", "week_start_date", "week_end_date"]
    return df.select(*cols)


# In[5]:


# =============================================================================
# DIM_ITEM (F4101) — the only NEW conformed dim besides dim_date.
# REUSED dims (dim_address_book + role views, dim_plant) are NOT rebuilt here;
# they are maintained by their existing old_nb jobs and the fact relates to them
# on NATURAL keys (address_number, plant_code). See design §3.
# =============================================================================
def transform_dim_item(spark, run_dt, restrict_item=None):
    f4101 = load_silver_table(spark, F4101_TBL)
    if restrict_item is not None:
        f4101 = f4101.join(restrict_item.alias("r"),
                           f4101["identifier_short_item"] == F.col("r.identifier_short_item"), "left_semi")
    df = (f4101.select(
              F.col("identifier_short_item").alias("item_number_short"),   # PK (natural)
              F.col("description_line_01").alias("item_name"),
              F.col("uom_weight").alias("uom_weight"),
              F.col("date_updated").alias("_src_ts"))
          .dropDuplicates(["item_number_short"]))
    bcols = ["item_number_short", "item_name", "uom_weight"]
    df = (df.withColumn("is_deleted", F.lit(False))
            .withColumn("source_commit_timestamp", F.col("_src_ts").cast("timestamp"))
            .withColumn("gold_updated_timestamp",  F.lit(run_dt).cast("timestamp"))
            .withColumn("record_hash", record_hash(df, bcols))
            .drop("_src_ts"))
    return df.select(*bcols, "is_deleted",
                     "source_commit_timestamp", "gold_updated_timestamp", "record_hash")


# In[7]:


# =============================================================================
# UoM -> TN conversion cascades (design §4.5) — bidirectional, item then standard
# =============================================================================
def build_uom_cascades(spark):
    f41002 = load_silver_table(spark, F41002_TBL)
    f41003 = load_silver_table(spark, F41003_TBL)

    item_fwd = (f41002.filter((F.trim("related_uom") == "TN") & (F.col("conversion_factor") != 0))
                .select(F.col("identifier_short_item").alias("itm"),
                        F.trim("uom").alias("from_uom"),
                        F.col("conversion_factor").cast("double").alias("conv_factor")))
    item_rev = (f41002.filter((F.trim("uom") == "TN") & (F.col("conversion_factor") != 0))
                .select(F.col("identifier_short_item").alias("itm"),
                        F.trim("related_uom").alias("from_uom"),
                        (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_factor")))
    conv_item = item_fwd.unionByName(item_rev)

    std_fwd = (f41003.filter((F.trim("related_uom") == "TN") & (F.col("conversion_factor") != 0))
               .select(F.trim("uom").alias("from_uom"),
                       F.col("conversion_factor").cast("double").alias("conv_factor")))
    std_rev = (f41003.filter((F.trim("uom") == "TN") & (F.col("conversion_factor") != 0))
               .select(F.trim("related_uom").alias("from_uom"),
                       (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_factor")))
    conv_std = std_fwd.unionByName(std_rev)
    return conv_item, conv_std


# In[8]:


# =============================================================================
# FREIGHT BUCKETS (F4981 -> shipment grain)   (design §4.3, Silver analysis §6.2)
# =============================================================================
def transform_freight_buckets(spark, restrict_ship=None):
    """Returns ONE row per shipment_number with the billable/payable buckets."""
    f4981 = load_silver_table(spark, F4981_TBL)
    if restrict_ship is not None:
        f4981 = f4981.join(restrict_ship.alias("r"),
                           f4981["shipment_number"] == F.col("r.shipment_number"), "left_semi")
    # gate: invoiced freight only (literal text "NULL", space-padded)
    fr = f4981.filter(F.trim("vendor_invoice_number") != "NULL")
    bp, cgc, amt = F.trim("billable_payable"), F.trim("charge_code_01"), F.col("net_amount")

    df = (fr.groupBy("shipment_number").agg(
            F.round(F.sum(F.when((bp == "B") & (cgc == "BFR"),            amt).otherwise(0.0)), 2).alias("billable_freight"),
            F.round(F.sum(F.when((bp == "B") & (cgc.isin("FSC", "FSB")), amt).otherwise(0.0)), 2).alias("billable_fuel"),
            F.round(F.sum(F.when((bp == "P") & (cgc == "PFR"),            amt).otherwise(0.0)), 2).alias("payable_freight"),
            F.round(F.sum(F.when((bp == "P") & (cgc == "FSC"),            amt).otherwise(0.0)), 2).alias("payable_fuel"))
          .withColumn("total_billable",   F.round(F.col("billable_freight") + F.col("billable_fuel"), 2))
          .withColumn("total_payable",    F.round(F.col("payable_freight")  + F.col("payable_fuel"), 2))
          .withColumn("freight_variance", F.round(F.col("billable_freight") - F.col("payable_freight"), 2))
          .withColumn("total_variance",   F.round(F.col("total_billable")   - F.col("total_payable"), 2))
          .withColumn("shift_factor_applied", F.lit(SHIFT_FACTOR).cast("double")))
    return df


# In[9]:


# =============================================================================
# FACT  fact_sales_order_freight  (design §4) — order-line grain, freight joined
# =============================================================================
FACT_BUSINESS_COLS = [
    # degenerate dims + company
    "company", "company_key_order_no", "order_type", "order_number", "line_number",
    "shipment_number", "bol_number", "invoice_number", "freight_handling_code",
    "freight_handling_code_audit", "mode_of_transport", "route_number",
    # dim_date smart keys (yyyyMMdd)
    "ship_date_key", "gl_date_key", "invoice_date_key",
    # NATURAL FK values — relate to REUSED dims (dim_address_book role views /
    # dim_plant) + new dim_item on these keys (no surrogate keys, no duplicate dims)
    "ship_to", "bill_to", "carrier_number", "item_number_short", "branch_plant",
    # dates
    "order_date", "requested_date", "scheduled_pick_date", "promised_ship_date",
    "actual_ship_date", "gl_date", "invoice_date",
    # item / qty / price
    "second_item_number", "line_type", "item_name", "uom", "uom_primary",
    "conversion_to_tons_rate", "missing_conversion_flag",
    "quantity_shipped", "quantity_shipped_tons", "price_per_unit", "price_quantity_shipped",
    "major_prod_code", "minor_prod_code", "freight_factor_value",
    # freight buckets (denormalized to line; deduped per shipment in DAX)
    "billable_freight", "billable_fuel", "total_billable",
    "payable_freight", "payable_fuel", "total_payable",
    "freight_variance", "total_variance", "shift_factor_applied",
    "is_primary_shipment_line",
]


def transform_fact(spark, run_dt, restrict_orders=None):
    """
    Build fact_sales_order_freight rows (design §4). If restrict_orders (distinct
    company_key_order_no, order_type, order_number) is given, only those orders are
    recomputed (streaming microbatch); else the full open universe (backfill).
    Returns a df with sales_order_line_key + business cols + audit cols.
    """
    f4211 = load_silver_table(spark, F4211_TBL)
    f4201 = load_silver_table(spark, F4201_TBL)
    f0101 = load_silver_table(spark, F0101_TBL)
    f4101 = load_silver_table(spark, F4101_TBL)
    b01   = load_silver_table(spark, F5642B01_TBL)
    b11   = load_silver_table(spark, F5642B11_TBL)
    f4074 = load_silver_table(spark, F4074_TBL)
    f4941 = load_silver_table(spark, F4941_TBL)
    conv_item, conv_std = build_uom_cascades(spark)

    # F4211 GL-date (SDDGL) Silver name not confirmed -> resolve, else NULL
    gl_name, gl_present = pick_col(
        f4211, ["date_for_g_l_julian", "date_g_l_julian", "date_general_ledger_julian",
                "general_ledger_date", "date_for_g_land_voucher_julian", "gl_date"], "gl_date")
    gl_expr = F.col(f"sd.{gl_name}") if gl_present else F.lit(None).cast("date")

    # ── Spec §4.2 driver filters (EXACTLY four — Hubble WHERE) ────────────────
    sd = (f4211
          .filter(F.col("company").isin(*COMPANIES))            # SDCO (deployed WHERE)
          .filter(F.col("line_type") == "S")
          .filter(F.col("status_code_last") != "980"))
    if restrict_orders is not None:
        sd = sd.join(restrict_orders.alias("ro"),
                     (sd["company_key_order_no"] == F.col("ro.company_key_order_no")) &
                     (sd["order_type"]           == F.col("ro.order_type")) &
                     (sd["document_order_invoice_e"] == F.col("ro.order_number")), "left_semi")

    # ── Ship-To DQ gate (INNER, ABAT1 A..P / R..ZZZ), no columns brought ──────
    ship_gate = (f0101.filter((F.trim("address_type_01").between("A", "P")) |
                              (F.trim("address_type_01").between("R", "ZZZ")))
                 .select(F.col("address_number").alias("dq_an8")).distinct())

    # ── F4074 freight factor: ALAST whitelist (or NULL) BEFORE join ───────────
    f4074w = f4074.filter(F.trim("price_adjustment_type").isin(*ALAST_WHITELIST) |
                          F.col("price_adjustment_type").isNull())
    # F4941 route number per shipment (often 0)
    route = (f4941.groupBy("shipment_number")
             .agg(F.first("route_number", ignorenulls=True).alias("route_number")))
    # freight buckets per shipment
    freight = transform_freight_buckets(spark)

    j = (sd.alias("sd")
         .join(f4201.alias("sh"),
               (F.col("sd.company_key_order_no") == F.col("sh.company_key_order_no")) &
               (F.col("sd.document_order_invoice_e") == F.col("sh.document_order_invoice_e")) &
               (F.col("sd.order_type") == F.col("sh.order_type")), "inner")
         .join(ship_gate.alias("g"), F.col("sd.address_number_ship_to") == F.col("g.dq_an8"), "inner")
         .join(b11.alias("b11"),
               (F.col("sd.company_key_order_no") == F.col("b11.company_key_order_no")) &
               (F.col("sd.document_order_invoice_e") == F.col("b11.document_order_invoice_e")) &
               (F.col("sd.order_type") == F.col("b11.order_type")) &
               (F.col("sd.line_number") == F.col("b11.line_number")) &
               (F.col("sd.shipment_number") == F.col("b11.shipment_number")), "left")
         .join(b01.alias("b01"),
               (F.col("sd.company_key_order_no") == F.col("b01.company_key_order_no")) &
               (F.col("sd.document_order_invoice_e") == F.col("b01.document_order_invoice_e")) &
               (F.col("sd.order_type") == F.col("b01.order_type")) &
               (F.col("sd.shipment_number") == F.col("b01.shipment_number")), "left")
         .join(f4101.alias("im"), F.col("sd.identifier_short_item") == F.col("im.identifier_short_item"), "left")
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

    # ── Conversion picker: TN literal -> F41002 -> F41003 -> 1.0 ──────────────
    conv_rate = F.coalesce(F.when(F.trim(F.col("sd.uom_as_input")) == "TN", F.lit(1.0)),
                           F.col("ci.conv_factor"), F.col("cs.conv_factor"), F.lit(1.0))
    conv_found = F.coalesce(F.when(F.trim(F.col("sd.uom_as_input")) == "TN", F.lit(1.0)),
                            F.col("ci.conv_factor"), F.col("cs.conv_factor"))

    sel = j.select(
        # degenerate + company
        F.col("sd.company").alias("company"),
        F.col("sd.company_key_order_no").alias("company_key_order_no"),
        F.col("sd.order_type").alias("order_type"),
        F.col("sd.document_order_invoice_e").alias("order_number"),
        F.col("sd.line_number").alias("line_number"),
        F.col("sd.shipment_number").alias("shipment_number"),
        F.col("sd.user_reserved_number").alias("bol_number"),
        F.col("sd.doc_voucher_invoice_e").alias("invoice_number"),
        F.col("sd.freight_handling_code").alias("freight_handling_code"),
        F.col("fr.shift_factor_applied"),     # placeholder carried for hash stability via freight df
        # natural FK values
        F.col("sd.address_number_ship_to").alias("ship_to"),
        F.col("sd.address_number").alias("bill_to"),
        F.col("sd.carrier").alias("carrier_number"),
        F.col("sd.identifier_short_item").alias("item_number_short"),
        F.trim(F.col("sd.cost_center")).alias("branch_plant"),
        F.col("sd.mode_of_transport").alias("mode_of_transport"),
        F.col("rt.route_number").alias("route_number"),
        # dates
        F.col("sd.date_transaction_julian").alias("order_date"),
        F.col("sd.date_requested_julian").alias("requested_date"),
        F.col("sd.scheduled_pick_date").alias("scheduled_pick_date"),
        F.col("sd.date_promised_ship_julian").alias("promised_ship_date"),
        F.col("sd.actual_ship_date").alias("actual_ship_date"),
        gl_expr.alias("gl_date"),
        F.col("sd.date_invoice_julian").alias("invoice_date"),
        # item / qty / price
        F.col("sd.identifier_second_item").alias("second_item_number"),
        F.col("sd.line_type").alias("line_type"),
        F.col("im.description_line_01").alias("item_name"),
        F.col("sd.uom_as_input").alias("uom"),
        F.col("sd.uom_primary").alias("uom_primary"),
        conv_rate.alias("conversion_to_tons_rate"),
        F.when(conv_found.isNull(), F.lit("Y")).otherwise(F.lit("N")).alias("missing_conversion_flag"),
        F.col("sd.units_quantity_shipped").alias("quantity_shipped"),
        F.col("sd.amt_price_per_unit_02").alias("price_per_unit"),
        F.col("sd.sales_reporting_code_02").alias("major_prod_code"),
        F.col("sd.sales_reporting_code_04").alias("minor_prod_code"),
        # freight handling from F4981 (audit) — exposed alongside SDFRTH
        F.col("sd.freight_handling_code").alias("freight_handling_code_audit"),
        # F4074 freight factor
        F.col("al.amt_price_per_unit_02").alias("freight_factor_value"),
        # freight buckets (denormalized)
        F.coalesce(F.col("fr.billable_freight"), F.lit(0.0)).alias("billable_freight"),
        F.coalesce(F.col("fr.billable_fuel"),    F.lit(0.0)).alias("billable_fuel"),
        F.coalesce(F.col("fr.total_billable"),   F.lit(0.0)).alias("total_billable"),
        F.coalesce(F.col("fr.payable_freight"),  F.lit(0.0)).alias("payable_freight"),
        F.coalesce(F.col("fr.payable_fuel"),     F.lit(0.0)).alias("payable_fuel"),
        F.coalesce(F.col("fr.total_payable"),    F.lit(0.0)).alias("total_payable"),
        F.coalesce(F.col("fr.freight_variance"), F.lit(0.0)).alias("freight_variance"),
        F.coalesce(F.col("fr.total_variance"),   F.lit(0.0)).alias("total_variance"),
        # NRT source-change proxy
        F.col("sd.date_updated").alias("_src_ts"),
    ).distinct()    # collapse F4074 multi-adjustment fan-out

    # ── Derived: tons, extended price, dim_date keys, anchor flag ─────────────
    # No surrogate FK columns: the fact relates to the REUSED dims (dim_address_book
    # role views, dim_plant) and dim_item on their natural keys (ship_to, bill_to,
    # carrier_number, branch_plant, item_number_short) already selected above.
    df = (sel
          .withColumn("quantity_shipped_tons", F.col("quantity_shipped") * F.col("conversion_to_tons_rate"))
          .withColumn("price_quantity_shipped", F.col("price_per_unit") * F.col("quantity_shipped"))
          .withColumn("ship_date_key",        date_key(F.col("actual_ship_date")))
          .withColumn("gl_date_key",          date_key(F.col("gl_date")))
          .withColumn("invoice_date_key",     date_key(F.col("invoice_date")))
          .withColumn("shift_factor_applied", F.coalesce(F.col("shift_factor_applied"), F.lit(SHIFT_FACTOR))))

    # is_primary_shipment_line = 'Y' on exactly ONE line per shipment (freight anchor)
    from pyspark.sql.window import Window
    w = Window.partitionBy("shipment_number").orderBy("order_number", "line_number")
    df = (df.withColumn("_rn", F.row_number().over(w))
            .withColumn("is_primary_shipment_line",
                        F.when((F.col("shipment_number").isNotNull()) & (F.col("_rn") == 1), F.lit("Y")).otherwise(F.lit("N")))
            .drop("_rn"))

    # ── keys + audit ──────────────────────────────────────────────────────────
    # order_scope_key groups all lines of one order — lets a streaming microbatch
    # soft-delete removed lines WITHIN the changed orders (cdc_merge scope col).
    df = (df.withColumn("sales_order_line_key",
                        sk("company_key_order_no", "order_type", "order_number", "line_number"))
            .withColumn("order_scope_key", sk("company_key_order_no", "order_type", "order_number"))
            .withColumn("record_hash", record_hash(df, FACT_BUSINESS_COLS))
            .withColumn("is_deleted", F.lit(False))
            .withColumn("source_commit_timestamp", F.col("_src_ts").cast("timestamp"))
            .withColumn("gold_updated_timestamp",  F.lit(run_dt).cast("timestamp"))
            .drop("_src_ts"))

    audit = ["is_deleted", "source_commit_timestamp", "gold_updated_timestamp", "record_hash"]
    return df.select("sales_order_line_key", "order_scope_key", *FACT_BUSINESS_COLS, *audit)


print("nb_eso1_transforms loaded — functions defined (no side effects).")
