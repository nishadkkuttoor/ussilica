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

# ## nb_silver_to_gold_dim_shipment
#
# Gold layer — F4215 (Shipment Header) — conformed shipment dimension.
#
# F4215 is JDE's shipment master — one row per shipment, keyed by
# shipment_number alone (JDE primary key, unique_index_name F4215_0).
# This is an independent Gold conformed dimension, not derived from any
# fact table — any fact needing shipment-level attributes (status,
# carrier, routing summary, etc.) relates to this table directly.



# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — Imports & constants
# ─────────────────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from datetime import datetime

F4215_TBL  = "lh_jde_silver.jde.f4215_shipment_header"
GOLD_TABLE = "lh_jde_gold.rpt.dim_shipment"

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
# CELL 3 — Load F4215 (universal exclusion only — is_delete, handled above)
# ─────────────────────────────────────────────────────────────────────────────
df_f4215 = load_silver(F4215_TBL)
print(f"F4215 silver rows : {df_f4215.count():,}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Select conformed columns
#
# Silver column mapping (confirmed via F4215 table metadata):
#   XHSHPN → shipment_number         (primary key — unique_index_name F4215_0)
#   XHSSTS → shipment_status
#   XHMOT  → mode_of_transport
#   XHCAR1 → carrier_01
#   XHCAR2 → carrier_02
#   XHCAR3 → carrier_03
#   XHORGN → origin_address_number
#   XHAN8  → address_number
#   XHSHAN → address_number_ship_to
#   XHMCU  → cost_center
#   XHCTY1 → city
#   XHADDS → state
#   XHCTR  → country
#   XHCTYO → origin_city
#   XHADSO → origin_state
#   XHCTRO → origin_country
#   XHNRTS → number_of_routing_steps
#   XHWGTS → shipment_weight
#   XHDRQJ → date_requested_julian
#   XHRSDJ → date_release_julian
#   XHUPMJ → date_updated
#
# Includes the F4215 columns needed by current reports. Confirm the Silver column name in the Fabric portal before adding further columns.
# ─────────────────────────────────────────────────────────────────────────────
df_dim_shipment = (
    df_f4215
    .select(
        F.col("shipment_number").alias("shipment_number"),
        F.col("shipment_status").alias("shipment_status"),
        F.col("mode_of_transport").alias("mode_of_transport"),
        F.col("carrier_01").alias("carrier_01"),
        F.col("carrier_02").alias("carrier_02"),
        F.col("carrier_03").alias("carrier_03"),
        F.col("origin_address_number").alias("origin_address_number"),
        F.col("address_number").alias("address_number"),
        F.col("address_number_ship_to").alias("address_number_ship_to"),
        F.col("cost_center").alias("cost_center"),
        F.col("city").alias("city"),
        F.col("state").alias("state"),
        F.col("country").alias("country"),
        F.col("origin_city").alias("origin_city"),
        F.col("origin_state").alias("origin_state"),
        F.col("origin_country").alias("origin_country"),
        F.col("number_of_routing_steps").alias("number_of_routing_steps"),
        F.col("shipment_weight").alias("shipment_weight"),
        F.col("date_requested_julian").alias("date_requested"),
        F.col("date_release_julian").alias("date_release"),
        F.col("date_updated").alias("date_updated"),
    )
    .distinct()
)

print(f"dim_shipment rows (pre-write) : {df_dim_shipment.count():,}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — Validate: true F4215 primary key is unique
#
# shipment_number is F4215's confirmed JDE primary key. Any duplicate here
# means a silver-layer defect — this must be zero before write, and the
# Power BI relationship to fact_extended_sales_order_8 depends on it.
# ─────────────────────────────────────────────────────────────────────────────
dup_check = df_dim_shipment.groupBy("shipment_number").count().filter(F.col("count") > 1)
dup_count = dup_check.count()
assert dup_count == 0, (
    f"F4215 primary key violated — {dup_count} duplicate shipment_number "
    f"values found. Direct Lake relationships will fail if this table is "
    f"written with duplicates. Investigate before proceeding."
)
print("✓ F4215 primary key uniqueness verified.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 6 — Write Gold dimension table
# ─────────────────────────────────────────────────────────────────────────────
df_dim_shipment.write \
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
