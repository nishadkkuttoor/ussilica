# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bed869e4-f15b-4cc1-9368-c7a9b3e08a83",
# META       "default_lakehouse_name": "lh_jde_gold",
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

#!/usr/bin/env python
# coding: utf-8

# ## nb_validate_eso2_v2
#
# Validation notebook for fact_extended_sales_order_2
# ============================================================================
# Run this notebook AFTER nb_silver_to_gold_eso2_v2 has completed its full
# load and all 6 streams are running.
#
# PURPOSE
#   Compare the Gold fact table (lh_jde_gold.eso2.fact_extended_sales_order_2)
#   against the original batch notebook output to confirm:
#     1. Row counts match expected Hubble counts
#     2. Key business report filters return correct totals
#     3. No duplicate LINE_KEYS exist in Gold
#     4. No nulls in grain key columns
#     5. UOM conversion (quantity_loaded) is working correctly
#
# HOW TO USE
#   Step 1 : Start nb_silver_to_gold_eso2_v2 (MANUAL_OVERWRITE=True first run)
#   Step 2 : Wait for "== all 6 streams running ==" in the streaming notebook
#   Step 3 : Run this notebook top to bottom
#   Step 4 : Compare printed totals against Hubble exports
# ============================================================================

from pyspark.sql import functions as F
from datetime import datetime

print("Validation notebook started : {}".format(datetime.now()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# CONFIG — point at the Gold fact table
# ----------------------------------------------------------------------------
GOLD_FACT = "lh_jde_gold.eso2.fact_extended_sales_order_2"

# Validation date range — adjust to match your Hubble export window
VAL_DATE_FROM = "2026-07-01"
VAL_DATE_TO   = "2026-07-02"

print("Gold fact  : {}".format(GOLD_FACT))
print("Val window : {} to {}".format(VAL_DATE_FROM, VAL_DATE_TO))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# LOAD GOLD FACT
# ----------------------------------------------------------------------------
df_gold = spark.read.table(GOLD_FACT)

print("Gold fact schema:")
df_gold.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# CHECK 1 — Total row count
# ----------------------------------------------------------------------------
# Compare this number against the Hubble full extract row count.
# If they differ, use CHECK 5 (duplicates) and CHECK 6 (nulls) to diagnose.
# ----------------------------------------------------------------------------
total_rows = df_gold.count()
total_cols = len(df_gold.columns)

print("=" * 60)
print("CHECK 1 — Total row count")
print("=" * 60)
print("  Rows    : {:,}".format(total_rows))
print("  Columns : {}".format(total_cols))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# CHECK 2 — Duplicate LINE_KEYS
# ----------------------------------------------------------------------------
# The composite PK (key_company_order, order_number, order_type, line_number)
# must be unique.  Any duplicates indicate a fan-out bug in build_fact()
# (e.g. a multi-row LEFT join without deduplication).
# Expected: zero duplicate rows.
# ----------------------------------------------------------------------------
LINE_KEYS = ["key_company_order", "order_number", "order_type", "line_number"]

df_dupes = (
    df_gold
    .groupBy(LINE_KEYS)
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 1)
)

dupe_count = df_dupes.count()

print("=" * 60)
print("CHECK 2 — Duplicate LINE_KEYS")
print("=" * 60)
print("  Duplicate LINE_KEY groups : {:,}".format(dupe_count))

if dupe_count > 0:
    print("  WARNING — duplicates found. Sample:")
    df_dupes.orderBy(F.col("cnt").desc()).show(20, truncate=False)
else:
    print("  PASS — no duplicates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# CHECK 3 — Null grain key columns
# ----------------------------------------------------------------------------
# None of the 4 LINE_KEY columns should ever be null.
# A null key_company_order or order_number usually means the F0010 or F4211
# join failed silently or a soft-delete filter was applied incorrectly.
# Expected: zero nulls in all 4 columns.
# ----------------------------------------------------------------------------
print("=" * 60)
print("CHECK 3 — Null grain key columns")
print("=" * 60)

null_check = df_gold.agg(
    *[F.count(F.when(F.col(k).isNull(), 1)).alias("{}_nulls".format(k))
      for k in LINE_KEYS]
)
null_check.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# CHECK 4 — Column-level null / zero summary
# ----------------------------------------------------------------------------
# Quick scan of key measure and date columns to spot unexpected nulls.
# quantity_loaded nulls would indicate a UOM conversion issue.
# actual_ship_date nulls would indicate a date conversion problem in Silver.
# ----------------------------------------------------------------------------
print("=" * 60)
print("CHECK 4 — Key column null summary")
print("=" * 60)

df_gold.agg(
    F.count("*").alias("total_rows"),
    F.count(F.when(F.col("actual_ship_date").isNull(),      1)).alias("actual_ship_date_nulls"),
    F.count(F.when(F.col("ship_to").isNull(),               1)).alias("ship_to_nulls"),
    F.count(F.when(F.col("quantity_loaded").isNull(),       1)).alias("quantity_loaded_nulls"),
    F.count(F.when(F.col("quantity_shipped_uom").isNull(),  1)).alias("qty_shipped_nulls"),
    F.count(F.when(F.col("plant").isNull(),                 1)).alias("plant_nulls"),
    F.count(F.when(F.col("company").isNull(),               1)).alias("company_nulls"),
    F.count(F.when(F.col("gross_weight").isNull(),          1)).alias("gross_weight_nulls"),
    F.count(F.when(F.col("tare_weight").isNull(),           1)).alias("tare_weight_nulls"),
    F.count(F.when(F.col("net_weight").isNull(),            1)).alias("net_weight_nulls"),
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# CHECK 5 — UOM conversion spot-check
# ----------------------------------------------------------------------------
# Shows distinct transactional_uom values and what quantity_loaded looks like
# per UOM.  TN rows should have quantity_loaded = quantity_shipped_uom exactly.
# Non-TN rows should have quantity_loaded != quantity_shipped_uom (conversion
# applied).  Rows where they are equal but UOM is not TN suggest a missing
# F41002 factor (fallback 1.0 used).
# ----------------------------------------------------------------------------
print("=" * 60)
print("CHECK 5 — UOM conversion summary by transactional_uom")
print("=" * 60)

(
    df_gold
    .groupBy("transactional_uom")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("quantity_shipped_uom").alias("total_qty_shipped"),
        F.sum("quantity_loaded").alias("total_qty_loaded_tn"),
        F.count(
            F.when(
                (F.col("quantity_loaded") == F.col("quantity_shipped_uom"))
                & (F.trim(F.col("transactional_uom")) != "TN"),
                1
            )
        ).alias("possible_missing_conversion"),
    )
    .orderBy(F.col("row_count").desc())
    .show(30, truncate=False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# CHECK 6 — Date range coverage
# ----------------------------------------------------------------------------
# Shows the min and max actual_ship_date in Gold.
# If max date is stale, the streaming notebook may have stopped or the
# source Silver table has not received new CDF events yet.
# ----------------------------------------------------------------------------
print("=" * 60)
print("CHECK 6 — Date range in Gold fact")
print("=" * 60)

df_gold.agg(
    F.min("actual_ship_date").alias("min_ship_date"),
    F.max("actual_ship_date").alias("max_ship_date"),
    F.countDistinct("actual_ship_date").alias("distinct_ship_dates"),
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# CHECK 7 — Core report validation
# ----------------------------------------------------------------------------
# Matches the Core report filter from the original batch notebook.
# Compare total_quantity_loaded against your Hubble Core report export.
#
# Hubble filters applied:
#   parent_number = 10100242
#   plant IN ('061', '501', '571')
#   order_type = 'SO'
#   actual_ship_date = VAL_DATE_FROM
# ----------------------------------------------------------------------------
print("=" * 60)
print("CHECK 7 — Core report")
print("=" * 60)

df_core = (
    df_gold
    .filter(F.col("parent_number") == 10100242)
    .filter(F.trim(F.col("plant")).isin("061", "501", "571"))
    .filter(F.col("order_type").isin("SO"))
    .filter(F.col("actual_ship_date").between(
        F.to_date(F.lit(VAL_DATE_FROM)),
        F.to_date(F.lit(VAL_DATE_FROM)),
    ))
)

df_core.agg(
    F.count("*").alias("total_rows"),
    F.sum("quantity_loaded").alias("total_quantity_loaded"),
    F.sum("quantity_shipped_uom").alias("total_qty_shipped_uom"),
).show(truncate=False)

display(df_core)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# CHECK 8 — Halliburton report validation
# ----------------------------------------------------------------------------
# Hubble filters applied:
#   parent_number = 10043240
#   order_type    = 'SO'
#   company       = '00400'
#   actual_ship_date = VAL_DATE_FROM
# ----------------------------------------------------------------------------
print("=" * 60)
print("CHECK 8 — Halliburton report")
print("=" * 60)

df_halliburton = (
    df_gold
    .filter(F.col("parent_number") == 10043240)
    .filter(F.col("order_type").isin("SO"))
    .filter(F.col("company") == "00400")
    .filter(F.col("actual_ship_date").between(
        F.to_date(F.lit(VAL_DATE_FROM)),
        F.to_date(F.lit(VAL_DATE_FROM)),
    ))
)

df_halliburton.agg(
    F.count("*").alias("total_rows"),
    F.sum("quantity_loaded").alias("total_quantity_loaded"),
    F.sum("quantity_shipped_uom").alias("total_qty_shipped_uom"),
).show(truncate=False)

display(df_halliburton)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# CHECK 9 — Nextier report validation
# ----------------------------------------------------------------------------
# Hubble filters applied:
#   parent_number = 10100242
#   plant IN ('061', '501', '571')
#   order_type    = 'SO'
#   actual_ship_date = VAL_DATE_FROM
# ----------------------------------------------------------------------------
print("=" * 60)
print("CHECK 9 — Nextier report")
print("=" * 60)

df_nextier = (
    df_gold
    .filter(F.col("parent_number") == 10100242)
    .filter(F.trim(F.col("plant")).isin("061", "501", "571"))
    .filter(F.col("order_type").isin("SO"))
    .filter(F.col("actual_ship_date").between(
        F.to_date(F.lit(VAL_DATE_FROM)),
        F.to_date(F.lit(VAL_DATE_FROM)),
    ))
)

df_nextier.agg(
    F.count("*").alias("total_rows"),
    F.sum("quantity_loaded").alias("total_quantity_loaded"),
    F.sum("quantity_shipped_uom").alias("total_qty_shipped_uom"),
).show(truncate=False)

display(df_nextier)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# CHECK 10 — Pioneer WTX report validation
# ----------------------------------------------------------------------------
# Hubble filters applied:
#   parent_number = 10112037
#   ship_to IN (10112039, 10115878)
#   plant IN ('321', '341', '161', '181')
#   order_type    = 'SO'
#   actual_ship_date = VAL_DATE_TO  (note: uses the second date)
# ----------------------------------------------------------------------------
print("=" * 60)
print("CHECK 10 — Pioneer WTX report")
print("=" * 60)

df_pioneer = (
    df_gold
    .filter(F.col("parent_number") == 10112037)
    .filter(F.col("ship_to").isin(10112039, 10115878))
    .filter(F.trim(F.col("plant")).isin("321", "341", "161", "181"))
    .filter(F.col("order_type").isin("SO"))
    .filter(F.col("actual_ship_date").between(
        F.to_date(F.lit(VAL_DATE_TO)),
        F.to_date(F.lit(VAL_DATE_TO)),
    ))
)

df_pioneer.agg(
    F.count("*").alias("total_rows"),
    F.sum("quantity_loaded").alias("total_quantity_loaded"),
    F.sum("quantity_shipped_uom").alias("total_qty_shipped_uom"),
).show(truncate=False)

display(df_pioneer)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# CHECK 11 — Ascent Load report validation
# ----------------------------------------------------------------------------
# Hubble filters applied:
#   next_status >= 560
#   order_type IN ('SO', 'CO')
#   actual_ship_date = VAL_DATE_FROM
# ----------------------------------------------------------------------------
print("=" * 60)
print("CHECK 11 — Ascent Load report")
print("=" * 60)

df_ascent = (
    df_gold
    .filter(F.col("next_status").cast("int") >= 560)
    .filter(F.col("order_type").isin("SO", "CO"))
    .filter(F.col("actual_ship_date").between(
        F.to_date(F.lit(VAL_DATE_FROM)),
        F.to_date(F.lit(VAL_DATE_FROM)),
    ))
)

df_ascent.agg(
    F.count("*").alias("total_rows"),
    F.sum("quantity_loaded").alias("total_quantity_loaded"),
    F.sum("quantity_shipped_uom").alias("total_qty_shipped_uom"),
).show(truncate=False)

display(df_ascent)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# CHECK 12 — Live stream lag check
# ----------------------------------------------------------------------------
# Compares the most recent actual_ship_date in Gold against today.
# If the Gold max date is many days behind, the streaming notebook may have
# stopped, or Silver is not receiving new Bronze commits.
#
# Also shows row counts by ship date for the validation window so you can
# spot any date with unexpectedly low or zero rows.
# ----------------------------------------------------------------------------
print("=" * 60)
print("CHECK 12 — Row distribution over validation window")
print("=" * 60)

(
    df_gold
    .filter(F.col("actual_ship_date").between(
        F.to_date(F.lit(VAL_DATE_FROM)),
        F.to_date(F.lit(VAL_DATE_TO)),
    ))
    .groupBy("actual_ship_date")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("quantity_loaded").alias("total_quantity_loaded"),
    )
    .orderBy("actual_ship_date")
    .show(30, truncate=False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# CHECK 13 — Plant distribution
# ----------------------------------------------------------------------------
# Shows row count and total quantity_loaded by plant.
# Cross-reference against the Hubble plant-level summary to catch any plant
# that is missing from Gold entirely (e.g. F0010 INNER join dropping it).
# ----------------------------------------------------------------------------
print("=" * 60)
print("CHECK 13 — Distribution by plant")
print("=" * 60)

(
    df_gold
    .filter(F.col("actual_ship_date").between(
        F.to_date(F.lit(VAL_DATE_FROM)),
        F.to_date(F.lit(VAL_DATE_TO)),
    ))
    .groupBy("plant")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("quantity_loaded").alias("total_quantity_loaded"),
    )
    .orderBy(F.col("row_count").desc())
    .show(50, truncate=False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# CHECK 14 — Order type distribution
# ----------------------------------------------------------------------------
# Shows which order_type values are present in Gold.
# Unexpected order types (e.g. test types) may indicate the source Silver
# table contains records that should have been filtered upstream.
# ----------------------------------------------------------------------------
print("=" * 60)
print("CHECK 14 — Distribution by order_type")
print("=" * 60)

(
    df_gold
    .groupBy("order_type")
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("row_count").desc())
    .show(30, truncate=False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# CHECK 15 — Validation summary
# ----------------------------------------------------------------------------
# Final pass/fail summary printed at the end so you have one place to look.
# ----------------------------------------------------------------------------
print("=" * 60)
print("VALIDATION SUMMARY")
print("=" * 60)

issues = []

if dupe_count > 0:
    issues.append("FAIL  CHECK 2 — {:,} duplicate LINE_KEY groups found".format(
        dupe_count))
else:
    print("PASS  CHECK 2 — No duplicate LINE_KEYs")

null_results = null_check.collect()[0]
null_found   = any(null_results[k] > 0 for k in null_results.__fields__)
if null_found:
    issues.append("FAIL  CHECK 3 — Null values in grain key columns")
else:
    print("PASS  CHECK 3 — No null grain keys")

if total_rows == 0:
    issues.append("FAIL  CHECK 1 — Gold fact table is empty")
else:
    print("PASS  CHECK 1 — Gold fact has {:,} rows".format(total_rows))

if issues:
    print("")
    print("Issues found:")
    for i in issues:
        print("  " + i)
else:
    print("")
    print("All automated checks passed.")
    print("Cross-check the report totals in CHECK 7-11 against Hubble exports.")

print("")
print("Validation completed : {}".format(datetime.now()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
