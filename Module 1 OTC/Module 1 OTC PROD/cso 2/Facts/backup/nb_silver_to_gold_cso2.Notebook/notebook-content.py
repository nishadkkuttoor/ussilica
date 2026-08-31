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

# What is this report trying to do?
# "Find all Sales Orders where the Item does NOT have a Cross Reference record set up"

# Think of it like this:

# Every item sold should have a "cross reference" record in a lookup table (F4104)
# If that cross reference is missing, the order appears in this report
# This helps the Logistics/Supply Chain/IT team fix missing setup data

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
F4211_TBL    = "lh_jde_silver.jde_cdc.f4211_sales_order_detail_file"
F4104_TBL    = "lh_jde_silver.jde_cdc.f4104_item_cross_reference_file"

# ── Gold output table ─────────────────────────────────────────────────────────
FACT_TABLE = "lh_jde_gold.rpt.fact_custom_sales_order_2"

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
df_f4211    = load_silver(F4211_TBL)
df_f4104    = load_silver(F4104_TBL)

print("All silver tables loaded.")
print(f"  F4211    : {df_f4211.count():,}")
print(f"  F4104    : {df_f4104.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Prep: F4211 — Filter & Distinct
#
# Replicates Hubble subquery:
#   SELECT DISTINCT sddoco, sddcto, sdmcu, sdnxtr, sdlttr,
#                   sddoc,  sdlitm, sdan8, sdshan
#   FROM   proddta.F4211
#   WHERE  sddcto IN ('S1','SE','SZ')
#   AND    sdlnty  = 'S'
#   AND    sdlttr <> 980
#
# Outer WHERE replicates:
#   AND    sdnxtr NOT IN ('999')
#   AND    sdlttr NOT IN ('980')   ← applied again at outer level in Hubble
#
# Silver column mapping (from metadata):
#   SDDOCO → document_order_invoice_e   (Order #)
#   SDDCTO → order_type                 (Order Type — filter + output)
#   SDMCU  → cost_center                (Plant)
#   SDNXTR → status_code_next           (Next Status Code — filter + output)
#   SDLTTR → status_code_last           (Last Status Code — filter + output)
#   SDDOC  → doc_voucher_invoice_e      (Invoice Number — used in Hubble SELECT)
#   SDLITM → identifier_second_item     (2nd Item Number — JOIN KEY to F4104)
#   SDAN8  → address_number             (Sold To / Customer)
#   SDSHAN → address_number_ship_to     (Ship To)
#   SDLNTY → line_type                  (filter only — not in output)
# ─────────────────────────────────────────────────────────────────────────────
df_f4211_filtered = (
    df_f4211
    .filter(F.col("order_type").isin("SE", "SZ", "S1"))        # SDDCTO IN ('SE','SZ','S1')
    .filter(F.trim(F.col("line_type")) == "S")                  # SDLNTY = 'S'
    # .filter(F.col("status_code_last") != 980)                   # SDLTTR <> 980
    # .filter(F.col("status_code_next") != 999)                   # SDNXTR <> 999
    .select(
        F.col("document_order_invoice_e"),   # SDDOCO
        F.col("order_type"),                 # SDDCTO
        F.col("cost_center"),                # SDMCU
        F.col("status_code_next"),           # SDNXTR
        F.col("status_code_last"),           # SDLTTR
        F.col("doc_voucher_invoice_e"),      # SDDOC
        F.col("identifier_second_item"),     # SDLITM — JOIN KEY
        F.col("address_number"),             # SDAN8
        F.col("address_number_ship_to"),     # SDSHAN
    )
    .distinct()                              # Matches DISTINCT in Hubble subquery
)

print(f"F4211 after filters + distinct : {df_f4211_filtered.count():,}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — Prep: F4104 — Filter & Distinct
#
# Replicates Hubble NOT EXISTS subquery conditions on F4104:
#   WHERE ivxrt  = 'C'
#   AND   ivlitm = F4211.sdlitm
#   AND   ivdsc1 <> ' '
#
# Silver column mapping (from metadata):
#   IVXRT  → type_cross_refer_type_c   (Cross Reference Type — filter = 'C')
#   IVLITM → identifier_2nd_item       (JOIN KEY to F4211.SDLITM)
#   IVDSC1 → description_line_01       (Description — filter <> ' ')
# ─────────────────────────────────────────────────────────────────────────────
df_f4104_filtered = (
    df_f4104
    .filter(F.trim(F.col("type_cross_refer_type_c")) == "C")    # IVXRT = 'C'
    .filter(F.col("description_line_01") != " ")                # IVDSC1 <> ' '
    .filter(F.col("description_line_01").isNotNull())           # safety null guard
    .select(
        F.col("identifier_2nd_item"),     # IVLITM — JOIN KEY only
    )
    .distinct()
)

print(f"F4104 after filters + distinct : {df_f4104_filtered.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 6 — LEFT OUTER JOIN: F4211 → F4104
#
# Spec  : F4211 LEFT OUTER JOIN F4104
# ON    : F4211.SDLITM (identifier_second_item)
#       = F4104.IVLITM (identifier_2nd_item)
#
# Post-join filter: KEEP ONLY rows where F4104 side IS NULL
# This replicates the Hubble NOT EXISTS subquery exactly:
#   "not exists (select '1' from F4104
#                where  ivxrt  = 'C'
#                and    ivlitm = F4211.sdlitm
#                and    ivdsc1 <> ' ')"
#
# Report Spec confirms: "Return records where F4104 = NULL"
# ─────────────────────────────────────────────────────────────────────────────
df_joined = (
    df_f4211_filtered
    .join(
        df_f4104_filtered,
        df_f4211_filtered["identifier_second_item"] == df_f4104_filtered["identifier_2nd_item"],
        "left",                          # LEFT OUTER JOIN — per report spec
    )
)

print(f"after joining : {df_joined.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 7 — SELECT DISTINCT
#
# Column mapping (exact spec match — all 8 report columns + invoice number):
#
# ┌────┬──────────────────────┬────────────┬──────────────────────────────────┬──────────────┐
# │ #  │ Fact Column          │ Bronze Col │ Silver Column                    │ Source Table │
# ├────┼──────────────────────┼────────────┼──────────────────────────────────┼──────────────┤
# │  1 │ order_number         │ SDDOCO     │ document_order_invoice_e         │ F4211        │
# │  2 │ order_type           │ SDDCTO     │ order_type                       │ F4211        │
# │  3 │ customer             │ SDAN8      │ address_number                   │ F4211        │
# │    │ Invoice_number       │ SDDOC      │ doc_voucher_invoice_e            │ F4211        │
# │  4 │ second_item_number   │ SDLITM     │ identifier_second_item           │ F4211        │
# │  5 │ last_status_code     │ SDLTTR     │ status_code_last                 │ F4211        │
# │  6 │ next_status_code     │ SDNXTR     │ status_code_next                 │ F4211        │
# │  7 │ plant                │ SDMCU      │ cost_center                      │ F4211        │
# │  8 │ ship_to              │ SDSHAN     │ address_number_ship_to           │ F4211        │
# └────┴──────────────────────┴────────────┴──────────────────────────────────┴──────────────┘
#
# NOTE: report_column1_short_ton is retained as NULL double to match
#       Hubble: MAX(FLOOR(TO_NUMBER(NULL))) — Standard Short Ton conversion placeholder
# ─────────────────────────────────────────────────────────────────────────────
df_fact = (
    df_joined
    .filter(F.col("identifier_2nd_item").isNull())   # F4104 NULL = no X-Ref found

    .select(

        # ── Col 1  : SDDOCO — Order Number ───────────────────────────────────
        F.col("document_order_invoice_e").alias("order_number"),

        # ── Col 2  : SDDCTO — Order Type ─────────────────────────────────────
        F.col("order_type").alias("order_type"),

        # ── Col 3  : SDAN8 — Customer (Sold To) ──────────────────────────────
        F.col("address_number").alias("customer"),

        # ── SDDOC  : Invoice Number — present in Hubble SELECT ──────────────
        F.col("doc_voucher_invoice_e").alias("Invoice_number"),

        # ── Col 4  : SDLITM — 2nd Item Number ────────────────────────────────
        F.col("identifier_second_item").alias("second_item_number"),

        # ── Col 5  : SDLTTR — Last Status Code ───────────────────────────────
        F.col("status_code_last").alias("last_status_code"),

        # ── Col 6  : SDNXTR — Next Status Code ───────────────────────────────
        F.col("status_code_next").alias("next_status_code"),

        # ── Col 7  : SDMCU — Plant ────────────────────────────────────────────
        F.col("cost_center").alias("plant"),

        # ── Col 8  : SDSHAN — Ship To ─────────────────────────────────────────
        F.col("address_number_ship_to").alias("ship_to"),
    )
    .distinct()
)

print(f"Final Table : {df_joined.count():,}")

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

filtered_df = (
    df_fact
    .filter(
        (F.col("order_type").isin("SE", "SZ", "S1")) &
        (F.col("last_status_code") != "980") &
        (F.col("next_status_code") != "999")
    )
)

print(filtered_df.count())
display(filtered_df)


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
# META   "frozen": true,
# META   "editable": false
# META }
