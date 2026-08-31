# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "915ea8b7-e01a-4182-b41a-c283df48a086",
# META       "default_lakehouse_name": "lh_jde_silver",
# META       "default_lakehouse_workspace_id": "9ea13355-c802-4ca5-883f-e5dbf8ecc720",
# META       "known_lakehouses": [
# META         {
# META           "id": "915ea8b7-e01a-4182-b41a-c283df48a086"
# META         },
# META         {
# META           "id": "bed869e4-f15b-4cc1-9368-c7a9b3e08a83"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ## nb_silver_to_gold_fact_sales_order_detail
#
# Gold layer — F4211 (Sales Order Detail) at its natural line grain.
#
# Gold Layer Design Rule:
#   - Complete, clean dataset. Universal exclusions only (soft-delete).
#   - No business-specific filters (no line_type restriction, no BU/plant/date filter).
#   - Reusable across current and future reports without requiring changes.
#
# F4211 is the source table behind most Module 1 reports. This is the
# conformed, reusable line-detail entity for the module — any report
# needing a line-type-specific value (e.g. a particular line's TORG)
# computes it via a DAX measure against this table.


# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — Imports & constants
# ─────────────────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from datetime import datetime

F4211_TBL  = "lh_jde_silver.jde.f4211_sales_order_detail_file"
F41002_TBL = "lh_jde_silver.jde.f41002_item_units_of_measure_conversion_factors"
F41003_TBL = "lh_jde_silver.jde.f41003_unit_of_measure_standard_conversion"
GOLD_TABLE = "lh_jde_gold.rpt.fact_sales_order_detail"

print(f"Run timestamp : {datetime.now()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 2 — Helper: load silver, drop soft-deleted rows + pipeline metadata
# ─────────────────────────────────────────────────────────────────────────────
_EXCLUDE_COLS = ["is_delete", "deleted_date_time"]

def load_silver(table_name):
    df = spark.read.table(table_name)
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _EXCLUDE_COLS])


def build_uom_cascades():
    # SDUOM->TN cascade — item-specific F41002 then standard F41003, each fwd + inverse, resolved directly
    # to TN. Ambiguous keys (>1 distinct factor) collapse to NULL so the join can never fan the grain out.
    f2 = load_silver(F41002_TBL)
    i_fwd = (f2.filter((F.trim("related_uom") == "TN") & (F.col("conversion_factor") != 0))
             .select(F.col("identifier_short_item").alias("itm"), F.trim("uom").alias("from_uom"),
                     F.col("conversion_factor").cast("double").alias("f")))
    i_rev = (f2.filter((F.trim("uom") == "TN") & (F.col("conversion_factor") != 0))
             .select(F.col("identifier_short_item").alias("itm"), F.trim("related_uom").alias("from_uom"),
                     (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("f")))
    item = (i_fwd.unionByName(i_rev).dropDuplicates(["itm", "from_uom", "f"])
            .groupBy("itm", "from_uom").agg(F.count("f").alias("n"), F.min("f").alias("f"))
            .withColumn("conv_factor", F.when(F.col("n") > 1, F.lit(None).cast("double")).otherwise(F.col("f")))
            .select("itm", "from_uom", "conv_factor"))
    f3 = load_silver(F41003_TBL)
    s_fwd = (f3.filter((F.trim("related_uom") == "TN") & (F.col("conversion_factor") != 0))
             .select(F.trim("uom").alias("from_uom"), F.col("conversion_factor").cast("double").alias("f")))
    s_rev = (f3.filter((F.trim("uom") == "TN") & (F.col("conversion_factor") != 0))
             .select(F.trim("related_uom").alias("from_uom"), (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("f")))
    std = (s_fwd.unionByName(s_rev).dropDuplicates(["from_uom", "f"])
           .groupBy("from_uom").agg(F.count("f").alias("n"), F.min("f").alias("f"))
           .withColumn("conv_std", F.when(F.col("n") > 1, F.lit(None).cast("double")).otherwise(F.col("f")))
           .select("from_uom", "conv_std"))
    return item, std


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 3 — Load F4211 (universal exclusion only — is_delete, handled above)
# ─────────────────────────────────────────────────────────────────────────────
df_f4211 = load_silver(F4211_TBL)
print(f"F4211 silver rows : {df_f4211.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Select conformed columns
#
# Silver column mapping (confirmed via F4211 table metadata):
#   SDKCOO → company_key_order_no       (primary key — company)
#   SDDOCO → document_order_invoice_e   (primary key — order_number)
#   SDDCTO → order_type                 (primary key)
#   SDLNID → line_number                (primary key — implied_decimals=3,
#                                        already scaled correctly in silver)
#   SDLNTY → line_type                  (NO filter — every line_type kept)
#   SDTORG → transaction_originator     (order_originator)
#   SDSHPN → shipment_number
#   SDMCU  → cost_center
#   SDAN8  → address_number             (customer / bill-to)
#   SDSHAN → address_number_ship_to
#   SDLTTR → status_code_last
#   SDNXTR → status_code_next
#   SDPPDJ → date_promised_ship_julian
#   SDDRQJ → date_requested_julian
#   SDPDDJ → scheduled_pick_date
#   SDRSDJ → date_release_julian        (line-level — distinct from F4941's own release date)
#   SDLITM → identifier_second_item     (item number — display column)
#   SDITM  → identifier_short_item      (matches F41002.UMITM — needed for
#                                        the UOM conversion relationship,
#                                        distinct from SDLITM above)
#   SDUOM  → uom_as_input
#   SDFRTH → freight_handling_code
#   SDCARS → carrier                    (line-level — distinct from F4941's carrier concept)
#   SDMOT  → mode_of_transport          (F4211 has its OWN MOT column, separate from F4941's)
#   SDCNID → container_id
#   SDVR01 → reference_01               (line-level customer reference — distinct from
#                                        F4201's own VR01, already resolved via dim_order)
#   SDUORG → units_transaction_qty
#
# (company_key_order_no, document_order_invoice_e, order_type, line_number)
# is F4211's true JDE primary key 
#

# ─────────────────────────────────────────────────────────────────────────────
df_fact = (
    df_f4211
    .select(
        F.col("company_key_order_no").alias("company"),
        F.col("document_order_invoice_e").alias("order_number"),
        F.col("order_type").alias("order_type"),
        F.col("line_number").alias("line_number"),
        F.col("line_type").alias("line_type"),
        F.col("transaction_originator").alias("order_originator"),
        F.col("shipment_number").alias("shipment_number"),
        F.col("cost_center").alias("cost_center"),
        F.col("address_number").alias("address_number"),
        F.col("address_number_ship_to").alias("address_number_ship_to"),
        F.col("status_code_last").alias("status_code_last"),
        F.col("status_code_next").alias("status_code_next"),
        F.col("date_promised_ship_julian").alias("date_promised_ship_julian"),
        F.col("date_requested_julian").alias("date_requested_julian"),
        F.col("scheduled_pick_date").alias("scheduled_pick_date"),
        F.col("date_release_julian").alias("date_release_julian"),
        F.col("identifier_second_item").alias("identifier_second_item"),
        F.col("identifier_short_item").alias("identifier_short_item"),
        F.col("uom_as_input").alias("uom_as_input"),
        F.col("uom_pricing").alias("uom_pricing"),                          # SDUOM4 — Source UoM (Price)
        F.col("amt_price_per_unit_02").alias("unit_price"),                 # SDUPRC — Source Price
        F.col("freight_handling_code").alias("freight_handling_code"),
        F.col("carrier").alias("carrier"),
        F.col("mode_of_transport").alias("mode_of_transport"),
        F.col("container_id").alias("container_id"),
        F.col("reference_01").alias("reference_01"),
        F.col("units_transaction_qty").alias("units_transaction_qty"),
    )
    .distinct()   # removes literal duplicate rows only — does NOT collapse distinct lines
)

# order_key — composite surrogate matching dim_order's join column
# (must use the identical column order: order_number, order_type, company).
df_fact = df_fact.withColumn(
    "order_key",
    F.concat_ws("|", "order_number", "order_type", "company")
)

# line_key — composite surrogate matching F4211's true primary key, required
# to relate fact_shipment_line_detail (F5642B11) at the same line grain.
# df_fact = df_fact.withColumn(
#     "line_key",
#     F.concat_ws("|", "company", "order_number", "order_type", "line_number")
# )
df_fact = df_fact.withColumn(
    "line_shipment_key",
    F.concat_ws("|", "company", "order_number", "order_type", "line_number", "shipment_number")
)

# uom_key — composite surrogate matching dim_uom_conversion's key exactly
# (cost_center, identifier_short_item, uom — must use this identical order). 
df_fact = df_fact.withColumn(
    "uom_key",
    F.concat_ws("|", "cost_center", "identifier_short_item", "uom_as_input")
)

# For Extended sales order 7 tables
df_fact = df_fact.withColumn("shipment_order_key",
    F.concat_ws("|", "shipment_number", "order_number", "order_type","company")
)

# For creating relationship with dim_order_activity table
df_fact = df_fact.withColumn("order_activity_key",
    F.concat_ws("|", "order_type", "line_type", "status_code_last")
)

# 2-column surrogate key for F41002
df_fact = df_fact.withColumn("item_uom_key",
F.concat_ws("|", "identifier_short_item", "uom_as_input"))

# ── standard UoM conversion (baked): SDUOM->TN volume factor/tons/flag + SDUOM4->TN price fields ──
_keep_cols = df_fact.columns
_vi, _vs = build_uom_cascades()
df_fact = (
    df_fact
    .join(_vi.alias("civ"), (F.col("civ.itm") == F.col("identifier_short_item"))
                            & (F.col("civ.from_uom") == F.trim(F.col("uom_as_input"))), "left")
    .join(_vs.alias("csv"), (F.col("csv.from_uom") == F.trim(F.col("uom_as_input"))), "left")
    .join(_vi.alias("cip"), (F.col("cip.itm") == F.col("identifier_short_item"))
                            & (F.col("cip.from_uom") == F.trim(F.col("uom_pricing"))), "left")
    .join(_vs.alias("csp"), (F.col("csp.from_uom") == F.trim(F.col("uom_pricing"))), "left")
)
_vol_raw = F.coalesce(F.when(F.trim(F.col("uom_as_input")) == "TN", F.lit(1.0)),
                      F.col("civ.conv_factor"), F.col("csv.conv_std"))
_prc_raw = F.coalesce(F.when(F.trim(F.col("uom_pricing")) == "TN", F.lit(1.0)),
                      F.col("cip.conv_factor"), F.col("csp.conv_std"))
_prc_fac = F.coalesce(_prc_raw, F.lit(1.0))
df_fact = (
    df_fact
    .withColumn("conversion_to_tons_rate", F.coalesce(_vol_raw, F.lit(1.0)))
    .withColumn("quantity_shipped_tons", F.col("units_transaction_qty").cast("double") * F.coalesce(_vol_raw, F.lit(1.0)))
    .withColumn("missing_conversion_flag", F.when(_vol_raw.isNull(), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn("price_conversion_factor", _prc_fac)
    .withColumn("converted_price_per_ton",
        F.when((F.col("status_code_last").cast("int") != F.lit(980))
               & (F.col("status_code_next").cast("int") == F.lit(999)),
               F.col("unit_price").cast("double") / F.when(_prc_fac != 0, _prc_fac)))
    .withColumn("price_missing_conversion_flag", F.when(_prc_raw.isNull(), F.lit("Y")).otherwise(F.lit("N")))
    # keep only the fact's own columns + the 6 derived (drop the cascade join helpers)
    .select(*_keep_cols, "conversion_to_tons_rate", "quantity_shipped_tons", "missing_conversion_flag",
            "price_conversion_factor", "converted_price_per_ton", "price_missing_conversion_flag")
)


print(f"fact_sales_order_detail rows (pre-write) : {df_fact.count():,}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — Validate: true F4211 primary key is unique
#
# (company, order_number, order_type, line_number) is F4211's confirmed
# JDE primary key. Any duplicate here means either a silver-layer defect
# or an unexpected CDC replay artifact — this must be zero before write.
# ─────────────────────────────────────────────────────────────────────────────
key_cols = ["company", "order_number", "order_type", "line_number"]

dup_check = df_fact.groupBy(*key_cols).count().filter(F.col("count") > 1)
dup_count = dup_check.count()
assert dup_count == 0, (
    f"F4211 primary key violated — {dup_count} duplicate "
    f"(company, order_number, order_type, line_number) combinations found. "
    f"Investigate before proceeding."
)
print("✓ F4211 primary key uniqueness verified.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 6 — Write Gold fact table
# ─────────────────────────────────────────────────────────────────────────────
df_fact.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"✓ {GOLD_TABLE}  →  {spark.read.table(GOLD_TABLE).count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"OPTIMIZE {GOLD_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Task complete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
