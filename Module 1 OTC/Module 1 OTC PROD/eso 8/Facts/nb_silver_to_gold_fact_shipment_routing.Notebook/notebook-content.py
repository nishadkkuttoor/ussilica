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

# ## nb_silver_to_gold_fact_shipment_routing
#
# Gold layer — F4941 (Shipment Routing Steps) at its natural leg grain.
#
# Gold Layer Design Rule:
#   - Complete, clean dataset. Universal exclusions only (soft-delete).
#   - No business-specific filters (no mode_of_transport restriction).
#   - Reusable across current and future reports without requiring changes.
#
# A shipment can have multiple routing legs (rail, ocean, truck, etc.) —
# this table keeps every leg as its own row. Any report needing a
# specific mode's value (e.g. the OCE leg's load port) computes it via
# a DAX measure against this table.



# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — Imports & constants
# ─────────────────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from datetime import datetime

F4941_TBL  = "lh_jde_silver.jde.f4941_shipment_routing_steps"
GOLD_TABLE = "lh_jde_gold.rpt.fact_shipment_routing"

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
# CELL 3 — Load F4941 (universal exclusion only — is_delete, handled above)
# ─────────────────────────────────────────────────────────────────────────────
df_f4941 = load_silver(F4941_TBL)
print(f"F4941 silver rows : {df_f4941.count():,}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Select conformed columns
#
# Silver column mapping:
#   RSSHPN → shipment_number        (join key)
#   RSRSSN → routing_step_number    (leg/step sequence within the shipment's
#                                    route — see note below on confidence)
#   RSMOT  → mode_of_transport      (NO filter — every leg's mode kept)
#   RSORGN → origin_address_number  (load_port — FK exposed to Power BI dim_address_book)
#   RSANCC → address_number_deconsolida  (destination/deconsolidation address —
#                                          FK exposed to Power BI "destination" shortcut)
#   RSRSDJ → date_release_julian    (leg-level release date, distinct from F4211's own)
#
# routing_step_number is treated as the leg-sequence key based on JDE column ordering 
# (RSSHPN=col 1, RSRSSN=col 2, matching F4211's confirmed key pattern) and the 
# RoutingStepNumber label. Cell 5 validates the assumption with a soft check; 
# if the warning fires, verify against the source metadata before treating this 
# table as production-ready.
#
# ─────────────────────────────────────────────────────────────────────────────
df_fact = (
    df_f4941
    .select(
        F.col("shipment_number").alias("shipment_number"), # primary-key
        F.col("routing_step_number").alias("routing_step_number"), # primary-key
        F.col("mode_of_transport").alias("mode_of_transport"),
        F.col("origin_address_number").alias("load_port"),
        F.col("address_number_deconsolida").alias("address_number_deconsolida"),
        F.col("date_release_julian").alias("date_release_julian"),
    )
    .distinct()   # removes literal duplicate rows only — does NOT collapse distinct legs
)

print(f"fact_shipment_routing rows (pre-write) : {df_fact.count():,}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — Validate: candidate key uniqueness (soft check — see Cell 4 note)
#
# (shipment_number, routing_step_number) is F4941's candidate leg-sequence key, 
# not metadata-confirmed as the primary key. Duplicates here indicate the assumption is wrong; 
# investigate against the source metadata before treating the table as production-ready.
# ─────────────────────────────────────────────────────────────────────────────
key_cols = ["shipment_number", "routing_step_number"]

dup_check = df_fact.groupBy(*key_cols).count().filter(F.col("count") > 1)
dup_count = dup_check.count()
if dup_count > 0:
    print(
        f"WARNING — {dup_count} duplicate (shipment_number, routing_step_number) "
        f"combinations found. routing_step_number may not be F4941's true leg-sequence "
        f"key — verify against the source metadata before treating this table as production-ready."
    )
else:
    print("✓ (shipment_number, routing_step_number) is unique — candidate key holds.")



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
