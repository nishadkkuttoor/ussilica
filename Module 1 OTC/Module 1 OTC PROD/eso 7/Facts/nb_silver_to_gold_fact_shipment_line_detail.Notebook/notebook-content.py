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

#!/usr/bin/env python
# coding: utf-8

# ## nb_silver_to_gold_fact_shipment_line_detail
#
# Gold layer — F5642B11 (Custom Sales Order Entry Screen Detail) at its
# natural line grain.
#
# F5642B11 is the custom shipping DETAIL companion to F5642B01 (custom
# shipping HEADER, already covered by fact_shipment_booking_detail).
# Confirmed JDE primary key (unique_index_name F5642B11_0) is (company,
# order_number, order_type, line_number, shipment_number) — a genuine
# 5-column key, shipment_number is NOT redundant with the line's own
# identity per JDE's own table design. Cell 5 still checks whether
# line_key alone (without shipment_number) happens to be independently
# unique in practice — if so, this table can relate to
# fact_sales_order_detail via the simpler line_key; if not, the
# relationship needs shipment_number folded in too.



# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — Imports & constants
# ─────────────────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from datetime import datetime

F5642B11_TBL = "lh_jde_silver.jde.f5642b11_custom_sales_order_entry_screen_detail"
GOLD_TABLE   = "lh_jde_gold.rpt.fact_shipment_line_detail"

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 3 — Load F5642B11 (universal exclusion only — is_delete, handled above)
# ─────────────────────────────────────────────────────────────────────────────
df_f5642b11 = load_silver(F5642B11_TBL)
print(f"F5642B11 silver rows : {df_f5642b11.count():,}")

# One-time check — print all columns to confirm the 3 business fields'
# exact silver names before finalizing Cell 4 below.
print("Available columns:")
for c in df_f5642b11.columns:
    print(" ", c)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Select conformed columns
#
# Silver column mapping (confirmed via F5642B11 table metadata):
#   AKKCOO → company_key_order_no       (primary key — company)
#   AKDOCO → document_order_invoice_e   (primary key — order_number)
#   AKDCTO → order_type                 (primary key)
#   AKLNID → line_number                (primary key)
#   AKSHPN → shipment_number            (primary key)
#   AK55SELN   → seal_no
#   AK55PDCD   → production_code
#   AK55PDSHNT → production_ship_notes
# ─────────────────────────────────────────────────────────────────────────────
df_fact = (
    df_f5642b11
    .select(
        F.col("company_key_order_no").alias("company"),
        F.col("document_order_invoice_e").alias("order_number"),
        F.col("order_type").alias("order_type"),
        F.col("line_number").alias("line_number"),
        F.col("shipment_number").alias("shipment_number"),
        F.col("seal_no").alias("seal_no"),
        F.col("production_code").alias("production_code"),
        F.col("production_ship_notes").alias("production_ship_notes"),
    )
    .distinct()
)

# # line_key — matches fact_sales_order_detail's surrogate exactly, so this
# # table can relate to it if line_key alone proves independently unique
# # (see Cell 5).
# df_fact = df_fact.withColumn(
#     "line_key",
#     F.concat_ws("|", "company", "order_number", "order_type", "line_number")
# )

# line_shipment_key — matches fact_sales_order_detail's surrogate exactly.
# Confirmed via Cell 5: line_key alone is NOT unique, so relate on this
# 5-column key instead (shipment_number folded in).
df_fact = df_fact.withColumn(
    "line_shipment_key",
    F.concat_ws("|", "company", "order_number", "order_type", "line_number", "shipment_number")
)

print(f"fact_shipment_line_detail rows (pre-write) : {df_fact.count():,}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — Validate: full join key AND line_key-alone uniqueness
#
# The source SQL's join key includes shipment_number — verify that first.
# Then separately check whether line_key ALONE (without shipment_number)
# is also unique — if so, this table can relate to fact_sales_order_detail
# as a clean one-to-one via line_key, since shipment_number is redundant
# with the line's own identity. If NOT, the relationship needs
# shipment_number folded in too — do not assume, let this check decide.
# ─────────────────────────────────────────────────────────────────────────────
full_key_cols = ["company", "order_number", "order_type", "line_number", "shipment_number"]

dup_full = df_fact.groupBy(*full_key_cols).count().filter(F.col("count") > 1)
dup_full_count = dup_full.count()
assert dup_full_count == 0, (
    f"F5642B11 join key violated — {dup_full_count} duplicate "
    f"(company, order_number, order_type, line_number, shipment_number) "
    f"combinations found. Investigate before proceeding."
)
print("✓ Full join key (incl. shipment_number) is unique.")

dup_line_key = df_fact.groupBy("line_shipment_key").count().filter(F.col("count") > 1)
dup_line_key_count = dup_line_key.count()
if dup_line_key_count > 0:
    print(
        f"⚠ NOTE — line_shipment_key alone is NOT unique ({dup_line_key_count} duplicates). "
        f"This table cannot relate to fact_sales_order_detail via line_shipment_key alone — "
        f"shipment_number must be part of the relationship key too."
    )
else:
    print("✓ line_shipment_key alone is also unique — safe to relate via line_shipment_key.")



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

spark.sql(f"OPTIMIZE {GOLD_TABLE}")
print(f"✓ {GOLD_TABLE}  →  {spark.read.table(GOLD_TABLE).count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
