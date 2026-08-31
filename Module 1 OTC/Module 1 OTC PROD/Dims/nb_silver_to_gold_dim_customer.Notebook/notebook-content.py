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
# META           "id": "bed869e4-f15b-4cc1-9368-c7a9b3e08a83"
# META         },
# META         {
# META           "id": "915ea8b7-e01a-4182-b41a-c283df48a086"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "e8fc6e8d-6c62-a450-4c29-1771fea37e17",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

# ## nb_silver_to_gold_dim_customer
#
# Gold layer — Customer dimension sourced from F03012 + F0101 + F0111
#
# KEY DESIGN DECISION — two name columns:
#   customer_name_abbreviated  = F0111.WWMLNM (line_number_id = 0)
#                                The mailing name — what Hubble displays.
#                                Use this in PBI visuals for Customer Name.
#                                Fixes SOSTMEIER LOGISTICS vs LUHE MINERALS GMBH mismatch.
#
#   name_alpha                 = F0101.ABALPH
#                                The address book alpha name. Different from
#                                mailing name when a freight agent is involved.
#
# WHY F03012 AS BASE (not F0101 directly):
#   F03012 contains only A/R billing customer records.
#   F0101 includes ALL address types (ship-to children, carriers, ports, etc.).
#   Using F03012 as base prevents ship-to child records (e.g. PROFILTRA B.V. - PPG)
#   from appearing as sold-to customer names.
#
# Sources:
#   F03012  Customer Master by Line of Business  → billing/sold-to records only
#   F0101   Address Book Master                  → name_alpha, search_type, category codes
#   F0111   Who's Who (line 0 only)              → customer_name_abbreviated (mailing name)
#   F0004   UDC Header                           → UDC type descriptions
#   F0005   UDC Detail                           → UDC code descriptions
#
# NO BUSINESS FILTERS ARE APPLIED IN THIS NOTEBOOK.

# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — Imports & constants
# ─────────────────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from datetime import datetime

F03012_TBL = "lh_jde_silver.jde.f03012_customer_master_by_line_of_business"
F0101_TBL  = "lh_jde_silver.jde.f0101_address_book_master"
F0111_TBL  = "lh_jde_silver.jde.f0111_address_book_who_is_who"
F0004_TBL  = "lh_jde_silver.jde.f0004_user_defined_code_types"
F0005_TBL  = "lh_jde_silver.jde.f0005_user_defined_code_values"
GOLD_TABLE = "lh_jde_gold.rpt.dim_customer"

run_dt = datetime.now()
print(f"Run timestamp : {run_dt}")
print(f"Target table  : {GOLD_TABLE}")

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
# CELL 3 — Build UDC lookup tables (F0004 + F0005)
#
# UDC join: F0004 (header) LEFT JOIN F0005 (codes)
#   ON product_code (DTSY=DRSY) AND user_defined_codes (DTRT=DRRT)
#
# F0004.description_001 = the human-readable UDC type name (e.g. 'Territory')
# F0005.user_defined_code = the code value (e.g. '01')
# F0005.description_001   = the code description (e.g. 'NORTHEAST')
# ─────────────────────────────────────────────────────────────────────────────
df_f0004 = load_silver(F0004_TBL)
df_f0005 = load_silver(F0005_TBL)

df_udc = (
    df_f0004.alias("hdr")
    .join(
        df_f0005.alias("det"),
        (F.col("hdr.product_code")     == F.col("det.product_code")) &
        (F.col("hdr.user_defined_codes") == F.col("det.user_defined_codes")),
        how="left"
    )
    .select(
        F.col("hdr.product_code").alias("udc_product_code"),
        F.col("hdr.description_001").alias("udc_type_name"),
        F.trim(F.col("det.user_defined_code")).alias("udc_code"),
        F.trim(F.col("det.description_001")).alias("udc_description"),
    )
)

# Billing Address Type (H42)
lu_billing_type = (
    df_udc
    .filter((F.col("udc_product_code") == "H42") & (F.col("udc_type_name") == "Billing Address Type"))
    .select(F.col("udc_code").alias("bat_code"), F.col("udc_description").alias("bat_description"))
)

# Territory (01)
lu_territory = (
    df_udc
    .filter((F.col("udc_product_code") == "01") & (F.col("udc_type_name") == "Territory"))
    .select(F.col("udc_code").alias("terr_code"), F.col("udc_description").alias("terr_description"))
)

print(f"UDC billing address types : {lu_billing_type.count():,}")
print(f"UDC territory codes       : {lu_territory.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Load F03012 (Customer Master)
#
# Base table — only A/R billing customer records exist here.
# Ship-to child records, carriers, and ports are NOT in F03012.
# This is why we use F03012 as the base (not F0101 directly).
#
# NOTE: The original US Silica query had WHERE AIAN8 = 10162057 — this was
# a test filter and is intentionally NOT included here.
# ─────────────────────────────────────────────────────────────────────────────
df_f03012 = (
    load_silver(F03012_TBL)
    .filter(F.col("company") == "00000")
    .select(
        F.col("address_number"),                           # AIAN8  — join key
        F.col("company"),                                  # AICO
        F.col("billing_address_type"),                     # AIBADT
        F.col("territory_id"),                             # AITERRID
        F.col("hold_orders_code"),                         # AIHOLD
        F.col("customer_status"),                          # AICUSTS
        F.trim(F.col("report_code_add_book_016")).alias("territory_code"),    # AIAC16
        F.trim(F.col("report_code_add_book_004")).alias("credit_analyst_code"), # AIAC04
        F.trim(F.col("report_code_add_book_005")).alias("sales_rep_code"),     # AIAC05
        F.trim(F.col("report_code_add_book_009")).alias("basin_code"),         # AIAC09
        F.trim(F.col("report_code_add_book_012")).alias("formation_code"),     # AIAC12
    )
    .distinct()
)

print(f"F03012 rows : {df_f03012.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — Load F0101 (Address Book)
#
# Provides the alpha name and address type for each address number.
# Joined to F03012 on address_number (ABAN8 = AIAN8).
# ─────────────────────────────────────────────────────────────────────────────
df_f0101 = (
    load_silver(F0101_TBL)
    .select(
        F.col("address_number").alias("f0101_address_number"),  # ABAN8
        F.trim(F.col("name_alpha")).alias("name_alpha"),        # ABALPH
        F.trim(F.col("address_type_01")).alias("search_type"),  # ABAT1
        F.trim(F.col("descrip_compressed")).alias("name_compressed"),  # ABDC
        F.col("standard_industry_code"),                        # ABSIC
    )
    .distinct()
)

print(f"F0101 rows : {df_f0101.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 6 — Load F0111 (Who's Who) — line_number_id = 0 only
#
# F0111 stores multiple name/contact entries per address number.
# line_number_id = 0 is the PRIMARY mailing name of the company —
# the name JDE prints on documents and what Hubble displays as Customer Name.
#
# WHY THIS FIXES SOSTMEIER LOGISTICS:
#   For SOSTMEIER orders, F0101.name_alpha = 'LUHE MINERALS GMBH' (JDE company record)
#   but F0111.name_mailing = 'SOSTMEIER LOGISTICS' (the mailing/agent name).
#   Hubble resolves customer names via F0111.WWMLNM — not F0101.ABALPH.
#   Using customer_name_abbreviated in PBI visuals matches Hubble exactly.
#
# replace(name_mailing, 'PARENT', '') removes the 'PARENT' suffix that JDE
# appends to parent account mailing name entries.
# ─────────────────────────────────────────────────────────────────────────────
df_f0111 = (
    load_silver(F0111_TBL)
    .filter(F.col("line_number_id") == 0)          # WWIDLN = 0 → primary mailing name
    .select(
        F.col("address_number").alias("f0111_address_number"),  # WWAN8
        F.trim(
            F.regexp_replace(F.col("name_mailing"), "PARENT", "")
        ).alias("customer_name_abbreviated"),                   # WWMLNM (cleaned)
    )
    .distinct()
)

print(f"F0111 rows (line 0 only) : {df_f0111.count():,}")

# Fan-out guard: F0111 line_number_id=0 should be one row per address_number
dup = df_f0111.groupBy("f0111_address_number").count().filter(F.col("count") > 1).count()
print(f"F0111 duplicates on address_number : {dup} (must be 0)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 7 — Join all sources into dim_customer
#
# Join order:
#   F03012  LEFT JOIN  F0101       ON address_number  → adds name_alpha, search_type
#           LEFT JOIN  F0111       ON address_number  → adds customer_name_abbreviated
#           LEFT JOIN  lu_billing  ON billing_address_type
#           LEFT JOIN  lu_territory ON territory_code
# ─────────────────────────────────────────────────────────────────────────────
df_dim = (
    df_f03012
    # ── address book (F0101) ─────────────────────────────────────────────────
    .join(
        df_f0101,
        df_f03012.address_number == df_f0101.f0101_address_number,
        how="left"
    )
    .drop("f0101_address_number")
    # ── mailing name (F0111 line 0) ──────────────────────────────────────────
    .join(
        df_f0111,
        df_f03012.address_number == df_f0111.f0111_address_number,
        how="left"
    )
    .drop("f0111_address_number")
    # ── billing address type description ────────────────────────────────────
    .join(
        lu_billing_type,
        df_f03012.billing_address_type == lu_billing_type.bat_code,
        how="left"
    )
    .drop("bat_code")
    # ── territory description ────────────────────────────────────────────────
    .join(
        lu_territory,
        F.col("territory_code") == lu_territory.terr_code,
        how="left"
    )
    .drop("terr_code")
    # ── final column order ───────────────────────────────────────────────────
    .select(
        "address_number",
        "customer_name_abbreviated",   # F0111.WWMLNM — use this in PBI visuals
        "name_alpha",                  # F0101.ABALPH — alternate/secondary name
        "name_compressed",
        "company",
        "search_type",
        "standard_industry_code",
        "billing_address_type",
        "bat_description",
        "territory_code",
        "terr_description",
        "territory_id",
        "hold_orders_code",
        "customer_status",
        "credit_analyst_code",
        "sales_rep_code",
        "basin_code",
        "formation_code",
    )
)

print(f"dim_customer rows : {df_dim.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 8 — Validate
# ─────────────────────────────────────────────────────────────────────────────
# 1. Row count must equal F03012 row count (LEFT joins preserve base)
f03012_count = df_f03012.count()
dim_count    = df_dim.count()
assert dim_count == f03012_count, (
    f"Row count mismatch — F03012: {f03012_count:,}, dim_customer: {dim_count:,}. "
    f"A join caused fan-out."
)


print(f"\u2713 Row count matches F03012 : {dim_count:,}")

# 2. No duplicate address_numbers
dup_count = (
    df_dim.groupBy("address_number").count()
    .filter(F.col("count") > 1).count()
)
assert dup_count == 0, f"Primary key violated — {dup_count} duplicate address_numbers found."
print(f"\u2713 No duplicate address_numbers")

# 3. Coverage of customer_name_abbreviated
total    = df_dim.count()
with_name = df_dim.filter(F.col("customer_name_abbreviated").isNotNull()).count()
print(f"customer_name_abbreviated coverage : {with_name:,} / {total:,} ({100*with_name//total}%)")

print("\nSample rows:")
df_dim.select("address_number", "customer_name_abbreviated", "name_alpha", "billing_address_type", "territory_code").show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 9 — Write Gold dim table
# ─────────────────────────────────────────────────────────────────────────────
df_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_TABLE)

spark.sql(f"OPTIMIZE {GOLD_TABLE}")
print(f"\u2713 {GOLD_TABLE}  \u2192  {spark.read.table(GOLD_TABLE).count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
