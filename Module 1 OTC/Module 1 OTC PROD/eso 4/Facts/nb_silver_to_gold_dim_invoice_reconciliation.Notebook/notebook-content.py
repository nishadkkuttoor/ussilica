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
# META     }
# META   }
# META }

# MARKDOWN ********************

# # dim_invoice_reconciliation
# 
# Gold layer — invoice-grain dim that carries all F4211-derived attributes needed by the ESO4 Sales Tax Reconciliation report.
# 
# **Grain:** one row per `(document_company, invoice_number, document_type)` — the F4211-side keys that correspond to F03B11's `(RPKCO, RPODOC, RPODCT)`.
# 
# **Why this dim exists:**
# The reconciliation report needs both:
# - Tax measures from `fact_customer_ledger` (F03B11 pay-item grain)
# - Order-side attributes from F4211 (plant, ship-to, sold-to, parent, business stream)
# 
# F03B11 and F4211 are many-to-many on the invoice — one invoice can consolidate many F4211 orders, and one invoice can have many F03B11 pay items. Direct fact-to-fact relationships in Direct Lake are hard to reason about. This dim pre-aggregates F4211 to invoice grain and carries every F4211-derived attribute the reconciliation report needs; `fact_customer_ledger` relates many-to-one to this dim on `invoice_scope_key`, and the report reads invoice-level attributes from here alongside the pay-item measures from the fact.
# 
# **Consolidated invoices:** an invoice can consolidate hundreds of F4211 orders. The dim shows a **representative order** picked as `MIN(SDDOCO)` — matches Hubble's Reconciliation Version pick and prevents fan-out.
# 
# **Gold Layer Design Rule adherence:**
# - No business-specific filters applied to F4211 (no line_type restriction, no plant/BU filter).
# - Universal exclusions only (soft-delete via `is_delete = 0`).
# - The one filter that IS applied: `doc_voucher_invoice_e > 0` — excludes uninvoiced F4211 lines that have no corresponding F03B11 pay item and therefore cannot participate in reconciliation. This mirrors what the INNER JOIN in the v1/v2 report already did.
# - The ABAT1 band `('A '-'P ' OR 'R '-'ZZZ')` is applied only to the F0101 ship-to address lookup, not to F4211 rows. A ship-to outside the band still gets a dim row with NULL sic_code/jurisdiction/county.
# 
# **Downstream relationships (built in the semantic model):**
# - `fact_customer_ledger[invoice_scope_key]` -> `dim_invoice_reconciliation[invoice_scope_key]`
# - `dim_invoice_reconciliation[ship_to]` -> `dim_address_book_ship_to[address_number]`
# - `dim_invoice_reconciliation[sold_to]` -> `dim_address_book_sold_to[address_number]`
# - `dim_invoice_reconciliation[parent_number]` -> `dim_address_book_parent[address_number]`
# - `dim_invoice_reconciliation[plant]` -> `dim_plant[plant_code]`
# - `dim_invoice_reconciliation[jurisdiction]` -> `dim_state[state_code]`
# - `dim_invoice_reconciliation[sic_code]` -> `dim_sic[standardindustrycode_code]`


# CELL ********************

# ---------------------------------------------------------------------------
# 1) CONFIG
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F, Window
import json, time
from datetime import datetime, timezone

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

MANUAL_OVERWRITE = True   # True: drop + rebuild from the full snapshot; False: build only if missing

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

F4211 = "f4211_sales_order_detail_file"
F4201 = "f4201_sales_order_header_file"
F0006 = "f0006_business_unit_master"
F0101 = "f0101_address_book_master"
F0116 = "f0116_address_by_date"

DIM = "dim_invoice_reconciliation"

_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def load_silver_table(t):
    """Read a Silver table and strip soft-deleted rows and audit columns."""
    df = spark.read.table(sname(t))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

def sk(*cols):
    return F.sha2(F.concat_ws("||", *[F.col(c).cast("string") if isinstance(c, str) else c.cast("string")
                                       for c in cols]), 256)

print(f"Run timestamp : {datetime.now()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# 2) LOAD Silver sources
# ---------------------------------------------------------------------------
sd  = load_silver_table(F4211)
hdr = load_silver_table(F4201)
bu  = load_silver_table(F0006)
ab  = load_silver_table(F0101)
adr = load_silver_table(F0116)

print(f"F4211 silver rows : {sd.count():,}")
print(f"F4201 silver rows : {hdr.count():,}")
print(f"F0006 silver rows : {bu.count():,}")
print(f"F0101 silver rows : {ab.count():,}")
print(f"F0116 silver rows : {adr.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# 3) LOOKUP tables — address (F0101 x F0116) and business unit (F0006)
#
# Address lookup: F0101 INNER F0116 gated to ABAT1 search-type band, then
# row_number over date_beginning_effective DESC picks the latest-effective
# address per ABAN8. Deterministic across runs.
# ---------------------------------------------------------------------------
_adr_w = Window.partitionBy(F.col("ad.address_number")).orderBy(
    F.col("ad.date_beginning_effective").desc_nulls_last(),
    F.col("ad.date_updated").desc_nulls_last())

_abat1 = F.rpad(F.rtrim(F.col("ab.address_type_01")), 3, " ")
_abat1_band = (((_abat1 >= F.lit("A  ")) & (_abat1 <= F.lit("P  "))) |
               ((_abat1 >= F.lit("R  ")) & (_abat1 <= F.lit("ZZZ"))))

address = (ab.alias("ab")
           .join(adr.alias("ad"), F.col("ab.address_number") == F.col("ad.address_number"), "inner")
           .where(_abat1_band)
           .withColumn("_arn", F.row_number().over(_adr_w))
           .where(F.col("_arn") == 1)
           .select(F.col("ab.address_number").alias("addr_key"),
                   F.trim(F.col("ab.standard_industry_code")).alias("sic_code"),   # ABSIC
                   F.trim(F.col("ad.state")).alias("jurisdiction"),                 # ALADDS
                   F.trim(F.col("ad.county_address")).alias("county")))              # ALCOUN

# Business unit collapse: one row per MCU providing MCRP20 for the business_stream calc
bunit = (bu.groupBy(F.col("cost_center").alias("bu_key"))
         .agg(F.first(F.trim(F.col("category_code_cost_ct_020")), ignorenulls=True)
              .alias("business_stream_code")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# 4) Pre-aggregate F4211 to invoice grain
#
# Consolidated invoices carry many order_numbers (SDDOCO) sharing one invoice.
# MIN(SDDOCO) picks a deterministic representative — matches Hubble's
# Reconciliation Version arbitrary pick (verified against the 4 SBX invoices
# where 371 / 249 / 121 / 2 orders each collapse to 1 row).
#
# first(ignorenulls=True) on ship_to / sold_to / parent / plant is safe because
# every order under a single invoice ships to the same address (verified in
# Silver during the ESO4 v2 investigation).
# ---------------------------------------------------------------------------
sd_invoice = (
    sd.filter(F.col("doc_voucher_invoice_e") > 0)   # skip uninvoiced F4211 lines
      .groupBy(
          F.col("doc_voucher_invoice_e").alias("_inv_no"),      # SDDOC
          F.col("document_type").alias("_inv_doc_type"),        # SDDCT
          F.col("company_key").alias("_inv_co_key"),            # SDKCO
      ).agg(
          F.min("document_order_invoice_e").alias("order_number"),  # SDDOCO representative
          F.min("order_type").alias("order_type"),                  # SDDCTO representative
          F.min("line_number").alias("line_number"),                # SDLNID representative
          F.first("cost_center",             ignorenulls=True).alias("cost_center"),
          F.first("address_number",          ignorenulls=True).alias("sold_to"),
          F.first("address_number_ship_to",  ignorenulls=True).alias("ship_to"),
          F.first("address_number_parent",   ignorenulls=True).alias("parent_number"),
          F.first("tax_explanation_code_01", ignorenulls=True).alias("tax_explanation_code"),
          F.max("dt_for_gl_and_vouch_01").alias("gl_date"),
      )
)
print(f"invoice-grain rows after F4211 pre-agg : {sd_invoice.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# 5) Enrich with F0006 (business_stream_code) and F0101/F0116 (SIC + address)
# ---------------------------------------------------------------------------
j = (sd_invoice.alias("sd")
     .join(bunit.alias("bu"),   F.col("sd.cost_center") == F.col("bu.bu_key"),      "left")
     .join(address.alias("ad"), F.col("sd.ship_to")     == F.col("ad.addr_key"),    "left"))

# Business Stream — ABSIC (ship-to F0101) x MCRP20 (F0006 business unit)
_absic  = F.trim(F.col("ad.sic_code"))
_mcrp20 = F.trim(F.col("bu.business_stream_code"))
business_stream = (F.when((_absic == "F") & (_mcrp20 == "ENG"), F.lit("O&G"))
                    .when((_absic != "F") & (_mcrp20 == "ENG"), F.lit("ISP"))
                    .when((_absic != "F") & (_mcrp20 == "SHR"), F.lit("ISP"))
                    .when((_absic == "F") & (_mcrp20 == "SHR"), F.lit("O&G"))
                    .when(~_mcrp20.isin("ENG", "SHR"), F.lit("ISP")))

# Avalara Code — RTRIM(LTRIM(NVL(SDDOC,-999999999))) || RTRIM(LTRIM(NVL(SDDCT,''))) || RTRIM(LTRIM(NVL(SDKCO,'')))
avalara_code = F.concat(
    F.coalesce(F.trim(F.col("sd._inv_no").cast("long").cast("string")), F.lit("-999999999")),
    F.coalesce(F.trim(F.col("sd._inv_doc_type")),                       F.lit("")),
    F.coalesce(F.trim(F.col("sd._inv_co_key").cast("string")),          F.lit("")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# 6) Final projection — one row per invoice with all attributes
# ---------------------------------------------------------------------------
df_dim = (j.select(
    F.col("sd._inv_co_key").alias("document_company"),
    F.col("sd._inv_no").alias("invoice_number"),
    F.col("sd._inv_doc_type").alias("document_type"),
    F.col("sd.order_number"),
    F.col("sd.order_type"),
    F.col("sd.line_number"),
    F.trim(F.col("sd.cost_center")).alias("plant"),
    F.col("sd.sold_to"),
    F.col("sd.ship_to"),
    F.col("sd.parent_number"),
    F.col("sd.tax_explanation_code"),
    avalara_code.alias("avalara_code"),
    F.trim(F.col("bu.business_stream_code")).alias("plant_invoice"),
    business_stream.alias("business_stream"),
    F.col("ad.sic_code"),
    F.col("ad.jurisdiction"),
    F.col("ad.county"),
    F.col("sd.gl_date"),
)
.withColumn("invoice_scope_key",
            sk("document_company", "invoice_number", "document_type"))
.withColumn("order_scope_key",
            F.when(
                F.col("order_number").isNotNull()
                  & F.col("order_type").isNotNull()
                  & F.col("document_company").isNotNull(),
                sk("document_company", "order_number", "order_type")
            ))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# 7) VALIDATE — invoice grain uniqueness
#
# (document_company, invoice_number, document_type) must be unique for the
# Direct Lake relationship on invoice_scope_key to work. Any duplicate here
# means the F4211 pre-aggregation failed and needs investigation.
# ---------------------------------------------------------------------------
key_cols = ["document_company", "invoice_number", "document_type"]
dup_check = df_dim.groupBy(*key_cols).count().filter(F.col("count") > 1)
dup_count = dup_check.count()
assert dup_count == 0, (
    f"Invoice grain violated — {dup_count} duplicate "
    f"(document_company, invoice_number, document_type) combinations found. "
    f"Investigate before proceeding."
)
print("Invoice grain uniqueness verified.")

# Also verify invoice_scope_key is unique (should be by construction)
isk_dup = df_dim.groupBy("invoice_scope_key").count().filter(F.col("count") > 1).count()
assert isk_dup == 0, f"invoice_scope_key not unique — {isk_dup} collisions."
print("invoice_scope_key uniqueness verified.")

print(f"dim_invoice_reconciliation rows (pre-write) : {df_dim.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# 8) WRITE Gold dim table
# ---------------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

_run_start = time.time()
if MANUAL_OVERWRITE or not spark.catalog.tableExists(gname(DIM)):
    print("== FULL LOAD ==")
    (df_dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(gname(DIM)))
    _rows   = df_dim.count()
    _status = "built"
    print("  {} rows={}".format(DIM, _rows))
else:
    print("== skip — {} exists and MANUAL_OVERWRITE=False ==".format(gname(DIM)))
    _rows, _status = None, "skipped"

spark.sql(f"OPTIMIZE {gname(DIM)}")

_elapsed = round(time.time() - _run_start, 1)
_exit_payload = {
    "status":       _status,
    "table":        gname(DIM),
    "rows":         _rows,
    "elapsed_sec":  _elapsed,
    "end_time_utc": datetime.now(timezone.utc).isoformat(),
}
print("exit payload:", json.dumps(_exit_payload))
notebookutils.notebook.exit(json.dumps(_exit_payload))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
