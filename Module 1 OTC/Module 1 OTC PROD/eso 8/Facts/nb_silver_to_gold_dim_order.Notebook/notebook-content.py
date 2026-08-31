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

# ## nb_silver_to_gold_dim_order
#
# Gold layer — F4201 (Sales Order Header) — conformed order dimension.
#
# F4201 is JDE's sales order master — one row per order, keyed by
# (company_key_order_no, document_order_invoice_e, order_type) (JDE
# primary key, unique_index_name F4201_0). This is an independent Gold
# conformed dimension, not derived from any fact table — any fact needing
# order-level attributes relates to this table directly.



# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — Imports & constants
# ─────────────────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from datetime import datetime

F4201_TBL  = "lh_jde_silver.jde.f4201_sales_order_header_file"
GOLD_TABLE = "lh_jde_gold.rpt.dim_order"

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
# CELL 3 — Load F4201 (universal exclusion only — is_delete, handled above)
# ─────────────────────────────────────────────────────────────────────────────
df_f4201 = load_silver(F4201_TBL)
print(f"F4201 silver rows : {df_f4201.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Select conformed columns
#
# Silver column mapping (confirmed via F4201 table metadata):
#   SHKCOO → company_key_order_no       (primary key — company)
#   SHDOCO → document_order_invoice_e   (primary key — order_number)
#   SHDCTO → order_type                 (primary key)
#   SHAN8  → address_number             (customer / bill-to)
#   SHSHAN → address_number_ship_to
#   SHMOT  → mode_of_transport
#   SHDRQJ → date_requested_julian
#   SHTRDJ → date_transaction_julian
#   SHORBY → ordered_by
#   SHTKBY → order_taken_by
#   SHUPMJ → date_updated
#   SHHOLD → hold_orders_code
#   SHOPDJ → date_original_promisde     (snake_case_field spelling matches the
#                                        confirmed silver column exactly — a
#                                        JDE-metadata-inherited typo, not ours)
#

# ─────────────────────────────────────────────────────────────────────────────
df_dim_order = (
    df_f4201
    .select(
        F.col("company_key_order_no").alias("company"),
        F.col("document_order_invoice_e").alias("order_number"),
        F.col("order_type").alias("order_type"),
        F.col("address_number").alias("address_number"),
        F.col("address_number_ship_to").alias("address_number_ship_to"),
        F.col("mode_of_transport").alias("mode_of_transport"),
        F.col("date_requested_julian").alias("date_requested"),
        F.col("date_transaction_julian").alias("date_transaction"),
        F.col("ordered_by").alias("ordered_by"),
        F.col("order_taken_by").alias("order_taken_by"),
        F.col("date_updated").alias("date_updated"),
        F.col("hold_orders_code").alias("hold_orders_code"),
        F.col("date_original_promisde").alias("date_original_promisde"),
    )
    .distinct()
)

# order_key — composite surrogate matching the join column already carried
# on fact_extended_sales_order_8 and fact_sales_order_detail (must use the
# identical column order: order_number, order_type, company).
df_dim_order = df_dim_order.withColumn(
    "order_key",
    F.concat_ws("|", "order_number", "order_type", "company")
)

print(f"dim_order rows (pre-write) : {df_dim_order.count():,}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — Validate: true F4201 primary key is unique
#
# (company, order_number, order_type) is F4201's confirmed JDE primary
# key. Any duplicate here means a silver-layer defect — this must be zero
# before write, and the Power BI relationships to fact_extended_sales_order_8
# and fact_sales_order_detail depend on it.
# ─────────────────────────────────────────────────────────────────────────────
key_cols = ["company", "order_number", "order_type"]

dup_check = df_dim_order.groupBy(*key_cols).count().filter(F.col("count") > 1)
dup_count = dup_check.count()
assert dup_count == 0, (
    f"F4201 primary key violated — {dup_count} duplicate "
    f"(company, order_number, order_type) combinations found. "
    f"Investigate before proceeding."
)
print("✓ F4201 primary key uniqueness verified.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 6 — Write Gold dimension table
# ─────────────────────────────────────────────────────────────────────────────
df_dim_order.write \
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

spark.sql(f"OPTIMIZE {GOLD_TABLE}")
print(f"✓ {GOLD_TABLE}  →  {spark.read.table(GOLD_TABLE).count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
