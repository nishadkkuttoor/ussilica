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
# META           "id": "86734eff-8f7a-4aa7-bcdc-37ef946622e0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#!/usr/bin/env python
# coding: utf-8

# ## nb_silver_to_gold_fact_shipment_booking_detail
#
# Gold layer — F5642B01 (Custom Sales Order Entry Screen Header) at its
# natural shipment/order grain.
#
# F5642B01's confirmed JDE primary key (unique_index_name F5642B01_0) is
# (shipment_number, order_number, order_type, company). This is an
# independent Gold conformed fact, not derived from any other table — any
# report needing export booking/routing detail relates to this table via
# the shared dim_shipment / dim_order bridge dimensions.




# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — Imports & constants
# ─────────────────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from datetime import datetime

F5642B01_TBL = "lh_jde_silver.jde.f5642b01_custom_sales_order_entry_screen_header"
GOLD_TABLE   = "lh_jde_gold.rpt.fact_shipment_booking_detail"

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
# CELL 3 — Load F5642B01 (universal exclusion only — is_delete, handled above)
# ─────────────────────────────────────────────────────────────────────────────
df_f5642b01 = load_silver(F5642B01_TBL)
print(f"F5642B01 silver rows : {df_f5642b01.count():,}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Select conformed columns
#
# Silver column mapping (confirmed via F5642B01 table metadata):
#   BASHPN → shipment_number        (primary key — unique_index_name F5642B01_0)
#   BADOCO → document_order_invoice_e (primary key — order_number)
#   BADCTO → order_type             (primary key)
#   BAKCOO → company_key_order_no   (primary key — company)
#   BA55BKNO   → booking_no
#   BA55BKSTAT → bookingstatus
#   BA55ROUT   → routing_notes
#   BARQSJ     → date_requested_ship
#   BA55DSTPT  → destination_port           (address number — aliased
#                                            destination_port_address to
#                                            avoid name collision with the
#                                            "destination" address-book shortcut)
#   BADLDL     → date_latest_delivery
#   BA55VLNO   → vessel_name                (distinct from BA55VONO — confirmed
#                                            two genuinely separate fields)
#   BA55VONO   → voyage_no
#   BALOAD     → date_loaded                (a date, not a flag)
#   BARSDJ     → date_release_julian
#   BADATE01   → date_01
#   BA55LODP   → loading_port               (address number — aliased
#                                            loading_port_address, same
#                                            collision-avoidance reasoning)
#   BA55OCCR   → ocean_carrier              (address number — aliased
#                                            ocean_carrier_address)
#   BA55OCDLT  → ocean_del_terms
#   BA55NCON   → no_of_container
#   BA55REF1   → reference_01
#   BA55REF2   → reference_02
#   BA55INDLT  → inland_delterms
#   BA55INCO   → incoterms
#   BA55EQTY   → equipment_type             (a type code, not a quantity)
#   BADLPU     → date_latest_pickup         (already used inline in ESO8;
#                                            included here too for the
#                                            reusable table)
#
# NOTE: Scoped to columns already validated against silver metadata. Add
# more F5642B01 columns later as needed — confirm exact silver names
# with Arun via Fabric portal before adding.
# ─────────────────────────────────────────────────────────────────────────────
df_fact = (
    df_f5642b01
    .select(
        F.col("shipment_number").alias("shipment_number"),
        F.col("document_order_invoice_e").alias("order_number"),
        F.col("order_type").alias("order_type"),
        F.col("company_key_order_no").alias("company"),
        F.col("booking_no").alias("booking_no"),
        F.col("bookingstatus").alias("bookingstatus"),
        F.col("routing_notes").alias("routing_notes"),
        F.col("date_requested_ship").alias("date_requested_ship"),
        F.col("destination_port").alias("destination_port_address"),
        F.col("date_latest_delivery").alias("date_latest_delivery"),
        F.col("vessel_name").alias("vessel_name"),
        F.col("voyage_no").alias("voyage_no"),
        F.col("date_loaded").alias("date_loaded"),
        F.col("date_release_julian").alias("date_release_julian"),
        F.col("date_01").alias("date_01"),
        F.col("loading_port").alias("loading_port_address"),
        F.col("ocean_carrier").alias("ocean_carrier_address"),
        F.col("ocean_del_terms").alias("ocean_del_terms"),
        F.col("no_of_container").alias("no_of_container"),
        F.col("reference_01").alias("reference_01"),
        F.col("reference_02").alias("reference_02"),
        F.col("inland_delterms").alias("inland_delterms"),
        F.col("incoterms").alias("incoterms"),
        F.col("equipment_type").alias("equipment_type"),
        F.col("date_latest_pickup").alias("date_latest_pickup"),
    )
    .distinct()
)

# For Extended sales order 7 tables
df_fact = df_fact.withColumn("shipment_order_key",
    F.concat_ws("|", "shipment_number", "order_number", "order_type","company")
)

print(f"fact_shipment_booking_detail rows (pre-write) : {df_fact.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — Validate: true F5642B01 primary key is unique
# ─────────────────────────────────────────────────────────────────────────────
key_cols = ["shipment_number", "order_number", "order_type", "company"]

dup_check = df_fact.groupBy(*key_cols).count().filter(F.col("count") > 1)
dup_count = dup_check.count()
assert dup_count == 0, (
    f"F5642B01 primary key violated — {dup_count} duplicate "
    f"(shipment_number, order_number, order_type, company) combinations found. "
    f"Investigate before proceeding."
)
print("✓ F5642B01 primary key uniqueness verified.")


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
