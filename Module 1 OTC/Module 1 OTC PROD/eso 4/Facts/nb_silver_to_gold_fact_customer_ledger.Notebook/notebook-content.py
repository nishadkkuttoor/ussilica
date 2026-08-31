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
# META       "environmentId": "06801a99-1abf-9498-4472-27df5088d778",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # fact_customer_ledger
# 
# Gold layer — F03B11 (Customer Ledger) at its natural pay-item grain.
# 
# **Grain:** one row per F03B11 pay item, keyed by JDE's confirmed primary key `(RPKCO, RPDOC, RPDCT, RPSFX)` -> `(company_key, doc_voucher_invoice_e, document_type, document_pay_item)`.
# 
# **Gold Layer Design Rule adherence:**
# - Complete, clean dataset. Universal exclusions only (soft-delete via `is_delete = 0`).
# - No business-specific filters (no doc-type restriction, no date filter, no plant filter).
# - Single-source fact — reads only F03B11, so no join fan-out risk.
# - Reusable across every AR-domain report (tax reconciliation, receivables aging, unapplied cash, etc.).
# 
# **Why this fact exists:**
# F03B11 stores every accounts-receivable pay item (invoices, credit memos, receipts, adjustments). Downstream reports that need AR amounts related to sales orders (e.g. ESO4 Sales Tax Reconciliation) link this fact to `dim_invoice_reconciliation` via `invoice_scope_key` at invoice grain. This keeps the F03B11 pay-item measures separate from the F4211-derived invoice attributes and prevents the invoice-consolidation fan-out that caused the ESO4 v1 amount inflation.
# 
# **Downstream relationships (built in the semantic model, not here):**
# - `fact_customer_ledger[invoice_scope_key]` -> `dim_invoice_reconciliation[invoice_scope_key]`  (invoice-grain bridge to F4211-derived attributes)
# - `fact_customer_ledger[tax_area]` -> `dim_tax_area[tax_area_code]` (future dim)


# CELL ********************

# ---------------------------------------------------------------------------
# 1) CONFIG
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F
import json, time
from datetime import datetime, timezone

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

MANUAL_OVERWRITE = True   # True: drop + rebuild from the full snapshot; False: build only if missing

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

F03B11 = "f03b11_customer_ledger"

FACT = "fact_customer_ledger"

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
# 2) LOAD F03B11 (universal exclusion only — is_delete)
# ---------------------------------------------------------------------------
df_f03b11 = load_silver_table(F03B11)
print(f"F03B11 silver rows : {df_f03b11.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# 3) SELECT conformed columns
#
# Silver column mapping — confirmed against F03B11 Silver metadata used by
# nb_silver_to_gold_extended_sales_order_4_v2.ipynb.
#
# JDE column mapping (RPKCO / RPDOC / RPDCT / RPSFX is F03B11's PK):
#   RPKCO   -> company_key                (primary key — company for the AR entry)
#   RPDOC   -> doc_voucher_invoice_e      (primary key — invoice / document number)
#   RPDCT   -> document_type              (primary key — RI / RM / RC / etc.)
#   RPSFX   -> document_pay_item          (primary key — pay item suffix e.g. 001, 002)
#   RPOKCO  -> company_key_original       (originating F4211 company)
#   RPODOC  -> original_document_no       (originating F4211 order document)
#   RPODCT  -> original_document_type     (originating F4211 order type)
#   RPLNID  -> line_number                (originating F4211 line — see NOTE)
#   RPATXA  -> amount_taxable
#   RPATXN  -> amount_tax_exempt
#   RPSTAM  -> amt_tax_02                 (Silver name inherited from JDE metadata)
#   RPAG    -> amount_gross
#   RPTXA1  -> tax_area_01
#   RPDSVJ  -> date_service_currency      (Service / Tax date)
#
# NOTE on RPLNID: F03B11's Original Line Number is populated with the F4211
# line that originated the pay item. For consolidated invoices where many
# F4211 orders roll into one invoice/pay item, RPLNID identifies only ONE of
# those F4211 lines — the others do not appear here. Downstream reports that
# need per-order tax attribution must aggregate at the invoice level, not
# rely on RPLNID for line-level splits (see the ESO4 v1 -> v2 investigation).
# ---------------------------------------------------------------------------
df_fact = (
    df_f03b11
    .select(
        # primary key
        F.col("company_key").alias("company_key"),
        F.col("doc_voucher_invoice_e").alias("invoice_number"),
        F.col("document_type").alias("document_type"),
        F.col("document_pay_item").alias("document_pay_item"),
        # originating F4211 order link
        F.col("company_key_original").alias("order_company"),
        F.col("original_document_no").alias("order_number"),
        F.col("original_document_type").alias("order_type"),
        F.col("line_number").alias("originating_line_number"),
        # measures
        F.col("amount_taxable").alias("amount_taxable"),
        F.col("amount_tax_exempt").alias("amount_tax_exempt"),
        F.col("amt_tax_02").alias("amount_tax"),
        F.col("amount_gross").alias("amount_gross"),
        # tax + date attributes
        F.col("tax_area_01").alias("tax_area"),
        F.col("date_service_currency").alias("service_tax_date"),
    )
    .distinct()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# 4) SURROGATE KEYS
#
# customer_ledger_key: SHA-256 of the JDE PK. Stable across runs. Used as the
#                      PBI Direct Lake relationship key when the fact is on
#                      the many side of a dim relationship.
#
# invoice_scope_key:   SHA-256 of (company_key, invoice_number, document_type).
#                      Coarser than the PK — collapses multiple pay items on
#                      one invoice. Used to relate this fact to
#                      dim_invoice_reconciliation at invoice grain, so tax
#                      amounts can be sliced by the F4211-derived invoice.
#
# order_scope_key:     SHA-256 of (order_company, order_number, order_type).
#                      Retained for future reports that link the AR entry
#                      back to its originating F4211 order. Populated only when
#                      RPODOC / RPODCT / RPOKCO are non-null (they are null
#                      for AR-only entries that don't originate from a sales
#                      order — receipts, manual adjustments, etc.).
# ---------------------------------------------------------------------------
df_fact = (
    df_fact
    .withColumn("customer_ledger_key",
                sk("company_key", "invoice_number", "document_type", "document_pay_item"))
    .withColumn("invoice_scope_key",
                sk("company_key", "invoice_number", "document_type"))
    .withColumn("order_scope_key",
                F.when(
                    F.col("order_number").isNotNull()
                      & F.col("order_type").isNotNull()
                      & F.col("order_company").isNotNull(),
                    sk("order_company", "order_number", "order_type")
                ))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# 5) VALIDATE — F03B11 primary key uniqueness
#
# (company_key, invoice_number, document_type, document_pay_item) is F03B11's
# confirmed JDE primary key. Any duplicate is a Silver-layer defect and would
# break the Direct Lake relationship on customer_ledger_key downstream.
# ---------------------------------------------------------------------------
key_cols = ["company_key", "invoice_number", "document_type", "document_pay_item"]
dup_check = df_fact.groupBy(*key_cols).count().filter(F.col("count") > 1)
dup_count = dup_check.count()
assert dup_count == 0, (
    f"F03B11 primary key violated — {dup_count} duplicate "
    f"(company_key, invoice_number, document_type, document_pay_item) "
    f"combinations found. Investigate before proceeding."
)
print("F03B11 primary key uniqueness verified.")
print(f"fact_customer_ledger rows (pre-write) : {df_fact.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# 6) WRITE Gold fact table
# ---------------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

_run_start = time.time()
if MANUAL_OVERWRITE or not spark.catalog.tableExists(gname(FACT)):
    print("== FULL LOAD ==")
    (df_fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(gname(FACT)))
    _rows   = df_fact.count()
    _status = "built"
    print("  {} rows={}".format(FACT, _rows))
else:
    print("== skip — {} exists and MANUAL_OVERWRITE=False ==".format(gname(FACT)))
    _rows, _status = None, "skipped"

spark.sql(f"OPTIMIZE {gname(FACT)}")

_elapsed = round(time.time() - _run_start, 1)
_exit_payload = {
    "status":       _status,
    "table":        gname(FACT),
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
