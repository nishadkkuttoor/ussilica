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

# ── Filter Responsibility ─────────────────────────────────────────────────────
# The following filter is intentionally NOT applied in this notebook.
# It is applied as a Power BI page-level filter instead:
#
#   shipment_status = '70'   → XHSSTS
#
# The Gold table stores ALL shipments regardless of status.
# Power BI page-level filter narrows to status 70 for the ESO3 report.
# ─────────────────────────────────────────────────────────────────────────────

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — Imports & constants
# ─────────────────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
 
# ── Silver source tables ──────────────────────────────────────────────────────
F4211_TBL  = "lh_jde_silver.jde.f4211_sales_order_detail_file"
F4215_TBL  = "lh_jde_silver.jde.f4215_shipment_header"
 
# ── Gold output table ─────────────────────────────────────────────────────────
FACT_TABLE = "lh_jde_gold.rpt.fact_extended_sales_order_3"
 
print(f"Run timestamp : {datetime.now()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 2 — Helper: load silver
# ─────────────────────────────────────────────────────────────────────────────
_EXCLUDE_COLS = ["is_delete", "deleted_date_time"]
 
def load_silver(table_name):
    """Read silver table, drop soft-deleted rows, drop pipeline metadata."""
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
# CELL 3 — Load all silver tables
# ─────────────────────────────────────────────────────────────────────────────
df_f4211  = load_silver(F4211_TBL)
df_f4215  = load_silver(F4215_TBL)
 
print("All silver tables loaded.")
print(f"  F4211    : {df_f4211.count():,}")
print(f"  F4215    : {df_f4215.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Select required columns from F4215 (Shipment Header)
#
#   #   Heading                Bronze Col   Silver snake_case_field
#   1   Shipment Number        XHSHPN       shipment_number          ← JOIN key
#   2   Shipment Status        XHSSTS       shipment_status          ← PBI filter
#   4   Business Unit          XHMCU        cost_center
#   8   Freight Handling Code  XHFRTH       freight_handling_code
#   9   Mode of Transport      XHMOT        mode_of_transport
#   10  State                  XHADDS       state
#   12  URRF                   XHURRF       user_reserved_reference
#   14  Update Date            XHUPMJ       date_updated
#   15  User ID                XHUSER       user_id
# ─────────────────────────────────────────────────────────────────────────────
df_f4215_sel = df_f4215.select(
    F.col("shipment_number"),          # XHSHPN — JOIN key
    F.col("shipment_status"),          # XHSSTS — Power BI page level filter
    F.col("cost_center"),              # XHMCU
    F.col("freight_handling_code"),    # XHFRTH
    F.col("mode_of_transport"),        # XHMOT
    F.col("state"),                    # XHADDS
    F.col("user_reserved_reference"),  # XHURRF
    F.col("date_updated"),             # XHUPMJ
    F.col("user_id"),                  # XHUSER
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — Select required columns from F4211 (Sales Order Detail)
#
#   #   Heading          Bronze Col   Silver snake_case_field
#   3   Company          SDCO         company
#   5   Order Number     SDDOCO       document_order_invoice_e
#   7   Order Type       SDDCTO       order_type
#   11  Related Order #  SDRORN       related_po_so_number
#   13  Actual Ship Date SDADDJ       actual_ship_date
#   —   SUM target       SDUORG       units_transaction_qty
#   —   JOIN key         SDSHPN       shipment_number
# ─────────────────────────────────────────────────────────────────────────────
df_f4211_sel = df_f4211.select(
    F.col("shipment_number"),           # SDSHPN — JOIN key
    F.col("company"),                   # SDCO
    F.col("document_order_invoice_e"),  # SDDOCO
    F.col("order_type"),                # SDDCTO
    F.col("related_po_so_number"),      # SDRORN
    F.col("actual_ship_date"),          # SDADDJ
    F.col("units_transaction_qty"),     # SDUORG — SUM target
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 6 — INNER JOIN F4215 → F4211
#           Bronze : F4215.XHSHPN = F4211.SDSHPN
#           Silver : F4215.shipment_number = F4211.shipment_number
# ─────────────────────────────────────────────────────────────────────────────
df_joined = df_f4215_sel.alias("F4215").join(
    df_f4211_sel.alias("F4211"),
    F.col("F4215.shipment_number") == F.col("F4211.shipment_number"),
    how="inner"
)

print(f"  Joined row count (pre-aggregation) : {df_joined.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 7 — Project all output columns post-join
#           Final column names = silver snake_case_field (no bronze aliases)
#           Ambiguous shipment_number resolved via table alias
# ─────────────────────────────────────────────────────────────────────────────
df_projected = df_joined.select(

    # ── From F4215 ───────────────────────────────────────────────────────────
    F.col("F4215.shipment_number"),          # XHSHPN  | Shipment Number
    F.col("F4215.shipment_status"),          # XHSSTS  | Shipment Status
    F.col("F4215.cost_center"),              # XHMCU   | Business Unit
    F.col("F4215.freight_handling_code"),    # XHFRTH  | Freight Handling Code
    F.col("F4215.mode_of_transport"),        # XHMOT   | Mode of Transport
    F.col("F4215.state"),                    # XHADDS  | State
    F.col("F4215.user_reserved_reference"),  # XHURRF  | URRF
    F.col("F4215.date_updated"),             # XHUPMJ  | Update Date
    F.col("F4215.user_id"),                  # XHUSER  | User ID

    # ── From F4211 ───────────────────────────────────────────────────────────
    F.col("F4211.company"),                   # SDCO   | Company
    F.col("F4211.document_order_invoice_e"),  # SDDOCO | Order Number
    F.col("F4211.order_type"),                # SDDCTO | Order Type
    F.col("F4211.related_po_so_number"),      # SDRORN | Related Order #
    F.col("F4211.actual_ship_date"),          # SDADDJ | Actual Ship Date
    F.col("F4211.units_transaction_qty"),     # SDUORG | SUM target
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 8 — GROUP BY + SUM aggregation
#           All group-by keys use silver snake_case_field names directly
#           Mirrors SQL GROUP BY exactly
# ─────────────────────────────────────────────────────────────────────────────
GROUP_BY_COLS = [
    "shipment_number",           # XHSHPN / SDSHPN
    "shipment_status",           # XHSSTS
    "company",                   # SDCO
    "cost_center",               # XHMCU
    "document_order_invoice_e",  # SDDOCO
    "order_type",                # SDDCTO
    "freight_handling_code",     # XHFRTH
    "mode_of_transport",         # XHMOT
    "state",                     # XHADDS
    "related_po_so_number",      # SDRORN
    "user_reserved_reference",   # XHURRF
    "actual_ship_date",          # SDADDJ
    "date_updated",              # XHUPMJ
    "user_id",                   # XHUSER
]

df_aggregated = df_projected.groupBy(GROUP_BY_COLS).agg(
    F.sum("units_transaction_qty").alias("units_transaction_qty_sum")  # SUM(SDUORG)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ─────────────────────────────────────────────────────────────────────────────
# CELL 9 — ORDER BY
#           Mirrors SQL ORDER BY sequence exactly
#           Uses silver snake_case_field names
# ─────────────────────────────────────────────────────────────────────────────
df_fact = df_aggregated.orderBy(
    F.col("shipment_number").asc(),           # F4215_XHSHPN
    F.col("shipment_status").asc(),           # F4215_XHSSTS
    F.col("document_order_invoice_e").asc(),  # F4211_SDDOCO
    F.col("order_type").asc(),                # F4211_SDDCTO
    F.col("company").asc(),                   # F4211_SDCO
    F.col("related_po_so_number").asc(),      # F4211_SDRORN
    F.col("date_updated").asc(),              # F4215_XHUPMJ
    F.col("user_id").asc(),                   # F4215_XHUSER
    F.col("freight_handling_code").asc(),     # F4215_XHFRTH
    F.col("actual_ship_date").asc(),          # F4211_SDADDJ
    F.col("user_reserved_reference").asc(),   # F4215_XHURRF
    F.col("state").asc(),                     # F4215_XHADDS
    F.col("cost_center").asc(),               # F4215_XHMCU
    F.col("mode_of_transport").asc(),         # F4215_XHMOT
)

print(f"  Final fact row count : {df_fact.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# DATA VALIDATION
# ─────────────────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F

validation = (
    df_fact
    .filter(
        #(F.col("date_updated").isin('2026-07-21')) &
        (F.col("cost_center") == "081") &
        (F.col("shipment_status") == '70') 
    )
)

print(validation.count())
display(validation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 15 — Write fact table
# ─────────────────────────────────────────────────────────────────────────────
df_fact.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(FACT_TABLE)

spark.sql(f"OPTIMIZE {FACT_TABLE}")
print(f"✓ {FACT_TABLE}  →  {spark.read.table(FACT_TABLE).count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
