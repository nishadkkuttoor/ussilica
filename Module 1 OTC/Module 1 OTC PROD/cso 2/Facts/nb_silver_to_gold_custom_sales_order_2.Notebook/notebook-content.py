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

# ── Filter Responsibility ─────────────────────────────────────────────────────
# The following filters are intentionally NOT applied in this notebook.
# They are applied as Power BI page-level filters instead:
#
#   order_type      IN ('SE','SZ','S1')   → SDDCTO
#   line_type        = 'S'               → SDLNTY
#   last_status_code <> 980              → SDLTTR
#   next_status_code <> 999              → SDNXTR
#
# The Gold table stores ALL sales order lines that are missing a cross reference
# Power BI page-level filters then slice the data as needed by the end user
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
F4211_TBL = "lh_jde_silver.jde.f4211_sales_order_detail_file"
F4104_TBL = "lh_jde_silver.jde.f4104_item_cross_reference_file"

# ── Gold output table ─────────────────────────────────────────────────────────
FACT_TABLE = "lh_jde_gold.rpt.fact_custom_sales_order_2"

# ── NOTE: Silver layer date columns are already converted to calendar DateType
#          Silver layer decimal implied conversions are already applied
# ─────────────────────────────────────────────────────────────────────────────

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
df_f4211 = load_silver(F4211_TBL)
df_f4104 = load_silver(F4104_TBL)

# print("All silver tables loaded.")
# print(f"  F4211 : {df_f4211.count():,}")
# print(f"  F4104 : {df_f4104.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Prep: F4211 — Select Required Columns & Distinct
#
# NO filters applied on F4211 in this cell.
# All filtering (order_type, line_type, last_status_code, next_status_code)
# is intentionally delegated to Power BI page-level filters.
#
# We only SELECT the columns needed for the report output and the JOIN key.
#
# Silver column mapping (from metadata):
#   SDDOCO → document_order_invoice_e   (Order #)
#   SDDCTO → order_type                 (Order Type — PBI filter + output)
#   SDMCU  → cost_center                (Plant)
#   SDNXTR → status_code_next           (Next Status Code — PBI filter + output)
#   SDLTTR → status_code_last           (Last Status Code — PBI filter + output)
#   SDDOC  → doc_voucher_invoice_e      (Invoice Number — used in Hubble SELECT)
#   SDLITM → identifier_second_item     (2nd Item Number — JOIN KEY to F4104)
#   SDAN8  → address_number             (Sold To / Customer)
#   SDSHAN → address_number_ship_to     (Ship To)
#   SDLNTY → line_type                  (Line Type — PBI filter + output)
# ─────────────────────────────────────────────────────────────────────────────
df_f4211_prep = (
    df_f4211
    .select(
        # ── Merge Keys ───────────────────────────────
        F.col("company_key_order_no"),   # SDKCOO — Merge key
        F.col("line_number"),            # SDLNID — Merge key

        F.col("document_order_invoice_e"),   # SDDOCO — Order #
        F.col("order_type"),                 # SDDCTO — PBI filter + output
        F.col("cost_center"),                # SDMCU  — Plant
        F.col("status_code_next"),           # SDNXTR — PBI filter + output
        F.col("status_code_last"),           # SDLTTR — PBI filter + output
        F.col("doc_voucher_invoice_e"),      # SDDOC  — Invoice Number
        F.col("identifier_second_item"),     # SDLITM — JOIN KEY to F4104
        F.col("address_number"),             # SDAN8  — Customer
        F.col("address_number_ship_to"),     # SDSHAN — Ship To
        F.col("line_type"),                  # SDLNTY — PBI filter + output
    )
    .distinct()                              # Remove duplicate rows
)

# print(f"F4211 selected + distinct : {df_f4211_prep.count():,}")

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
#   AND   ivlitm = F4211.sdlitm   ← correlated — handled by the JOIN key
#   AND   ivdsc1 <> ' '
#
# These F4104 filters ARE kept in the notebook because they define
# what counts as a valid cross reference record — this is core business logic,
# not a user-facing filter.
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

# print(f"F4104 after filters + distinct : {df_f4104_filtered.count():,}")

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
#
# How the LEFT JOIN works:
#   F4211 row has a matching F4104 row → identifier_2nd_item is populated → EXCLUDE
#   F4211 row has NO matching F4104 row → identifier_2nd_item is NULL     → KEEP
# ─────────────────────────────────────────────────────────────────────────────
df_joined = (
    df_f4211_prep
    .join(
        df_f4104_filtered,
        df_f4211_prep["identifier_second_item"] ==
        df_f4104_filtered["identifier_2nd_item"],
        "left",                          # LEFT OUTER JOIN — per report spec
    )
)

print(f"After joining : {df_joined.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 7 — SELECT DISTINCT — Final Fact Table
#
# Filter: Keep ONLY rows where F4104 side is NULL
#         = Sales order lines with NO valid cross reference (the report purpose)
#
# All 8 report columns + line_type + invoice number included.
# No status or order type filters applied here — handled in Power BI.
#
# Column mapping (full output):
#
# ┌────┬──────────────────────┬────────────┬──────────────────────────────────┬──────────────┐
# │ #  │ Fact Column          │ Bronze Col │ Silver Column                    │ Source Table │
# ├────┼──────────────────────┼────────────┼──────────────────────────────────┼──────────────┤
# │  1 │ order_number         │ SDDOCO     │ document_order_invoice_e         │ F4211        │
# │  2 │ order_type           │ SDDCTO     │ order_type                       │ F4211        │
# │  3 │ customer             │ SDAN8      │ address_number                   │ F4211        │
# │    │ invoice_number       │ SDDOC      │ doc_voucher_invoice_e            │ F4211        │
# │  4 │ second_item_number   │ SDLITM     │ identifier_second_item           │ F4211        │
# │  5 │ last_status_code     │ SDLTTR     │ status_code_last                 │ F4211        │
# │  6 │ next_status_code     │ SDNXTR     │ status_code_next                 │ F4211        │
# │  7 │ plant                │ SDMCU      │ cost_center                      │ F4211        │
# │  8 │ ship_to              │ SDSHAN     │ address_number_ship_to           │ F4211        │
# │  + │ line_type            │ SDLNTY     │ line_type                        │ F4211        │
# └────┴──────────────────────┴────────────┴──────────────────────────────────┴──────────────┘
#
# Power BI page-level filters (applied in report — NOT here):
#   order_type      IN ('SE','SZ','S1')
#   line_type        = 'S'
#   last_status_code <> 980
#   next_status_code <> 999
# ─────────────────────────────────────────────────────────────────────────────
df_fact = (
    df_joined
    .filter(F.col("identifier_2nd_item").isNull())   # F4104 NULL = no X-Ref found

    .select(

        # # ── Merge / Grain Keys ────────────────
        F.col("company_key_order_no"),
        F.col("line_number"),

        # ── Col 1  : SDDOCO — Order Number ───────────────────────────────────
        F.col("document_order_invoice_e").alias("order_number"),

        # ── Col 2  : SDDCTO — Order Type — PBI page-level filter ─────────────
        F.col("order_type").alias("order_type"),

        # ── Col 3  : SDAN8 — Customer (Sold To) ──────────────────────────────
        F.col("address_number").alias("customer"),

        # ── SDDOC  : Invoice Number — present in Hubble SELECT ───────────────
        F.col("doc_voucher_invoice_e").alias("invoice_number"),

        # ── Col 4  : SDLITM — 2nd Item Number ────────────────────────────────
        F.col("identifier_second_item").alias("second_item_number"),

        # ── Col 5  : SDLTTR — Last Status Code — PBI page-level filter ───────
        F.col("status_code_last").alias("last_status_code"),

        # ── Col 6  : SDNXTR — Next Status Code — PBI page-level filter ───────
        F.col("status_code_next").alias("next_status_code"),

        # ── Col 7  : SDMCU — Plant ───────────────────────────────────────────
        F.col("cost_center").alias("plant"),

        # ── Col 8  : SDSHAN — Ship To ─────────────────────────────────────────
        F.col("address_number_ship_to").alias("ship_to"),

        # ── SDLNTY : Line Type — PBI page-level filter ────────────────────────
        F.col("line_type").alias("line_type")        
    )
    .distinct()
)

print(f"Final fact table row count : {df_fact.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_fact)

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
        (F.col("line_type") == "S") &
        (F.col("last_status_code") != "980") &
        (F.col("next_status_code") != "999")
    )
)

# print(filtered_df.count())
display(filtered_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
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
