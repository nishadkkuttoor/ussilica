#!/usr/bin/env python
# coding: utf-8

# ## nb_silver_to_gold_extended_sales_order_4
# 
# New notebook

# # fact_sales_tax_reconciliation (ESO4)
# 
# **Grain:** one row per (document_company, document_type, invoice_number) + tax + address dimension attributes.
# The reconciliation report is invoice-level, matching Hubble's `TX002_Sales Tax with Business Stream Summary (Reconciliation Version)`.
# 
# **Consolidated invoices**: an invoice can carry many orders (one F03B11 pay item, many F4211 order lines). The build pre-aggregates F03B11 to invoice grain (SUM of amounts) and pre-aggregates F4211 to invoice grain (MIN of order_number for a deterministic representative). The 1:1 join at invoice grain prevents amount fan-out and preserves Hubble's row layout.
# 
# **Filter Responsibility**: the spec's `gl_date BETWEEN '2026-05-01' AND '2026-05-31'` filter is NOT applied here. Power BI page-level filter narrows to the current reconciliation month. The Gold table holds every invoice-line regardless of GL date.

# In[1]:


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

F4211  = "f4211_sales_order_detail_file"
F03B11 = "f03b11_customer_ledger"
F0006  = "f0006_business_unit_master"
F0101  = "f0101_address_book_master"
F0116  = "f0116_address_by_date"

FACT = "fact_sales_tax_reconciliation"

# Silver amounts are already decoded (raw JDE integer amounts carried an implied
# 0.01 scale). SHIFT_FACTOR is the placeholder for future currency conversion.
SHIFT_FACTOR = 1.0

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


# In[2]:


# ---------------------------------------------------------------------------
# 2) FACT COLUMN CONTRACT
# ---------------------------------------------------------------------------
# Stored fact columns in report order. One row per invoice; order_number,
# order_type, and line_number are representative (MIN of the F4211 lines that
# make up each invoice) so the report can display them without fanning out
# consolidated invoices.
FACT_BUSINESS_COLS = [
    "document_company", "invoice_number", "document_type",
    "order_number", "order_type", "line_number",
    "plant", "ship_to", "sold_to", "parent_number",       # FKs -> dim_plant / dim_address_*
    "tax_explanation_code", "tax_area", "avalara_code",
    "business_stream", "sic_code",                        # sic_code FK -> dim_sic
    "jurisdiction", "county",                             # jurisdiction FK -> dim_state; county degenerate
    "gl_date", "service_tax_date",
    "taxable_amount", "non_taxable_amount", "tax_amount", "gross_amount",
    "tax_status",                                         # derived post-aggregate from the SUMmed tax_amount
    "shift_factor_applied",
]
# 25 FACT_BUSINESS_COLS + sales_tax_line_key + document_scope_key = 27 stored.


# In[3]:


# ---------------------------------------------------------------------------
# 3) FACT BUILDER
# ---------------------------------------------------------------------------
def build_fact():
    sd  = load_silver_table(F4211)       # sales order detail
    ar  = load_silver_table(F03B11)      # customer ledger
    bu  = load_silver_table(F0006)       # business unit master (business_stream calc input)
    ab  = load_silver_table(F0101)       # address book master  (sic_code)
    adr = load_silver_table(F0116)       # address by date      (jurisdiction/county)

    # ── Address lookup: F0101 INNER F0116, gated to ABAT1 search-type band,
    #    then row_number over date_beginning_effective DESC to pick the
    #    latest-effective address per ABAN8. Deterministic across runs.
    #    The ABAT1 band ('A '-'P ' OR 'R '-'ZZZ') gates the address lookup
    #    only; a ship-to outside the band keeps its fact row and gets NULL
    #    sic_code / jurisdiction / county via the LEFT join below.
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
                       F.trim(F.col("ab.standard_industry_code")).alias("sic_code"),   # ABSIC  (FK dim_sic)
                       F.trim(F.col("ad.state")).alias("jurisdiction"),                 # ALADDS (FK dim_state)
                       F.trim(F.col("ad.county_address")).alias("county")))              # ALCOUN

    # ── Business unit collapse: one row per MCU, providing the MCRP20 code
    #    used in the business_stream calc below.
    bunit = (bu.groupBy(F.col("cost_center").alias("bu_key"))
             .agg(F.first(F.trim(F.col("category_code_cost_ct_020")), ignorenulls=True).alias("business_stream_code")))

    # ── STEP 1: Pre-aggregate F03B11 to invoice grain ─────────────────────
    # An invoice can carry many pay items. SUM the four measures per invoice
    # so the downstream join is 1:1 and amounts cannot be multiplied by the
    # F4211 line fan-out on consolidated invoices.
    ar_invoice = (
        ar.groupBy(
            F.col("original_document_no").alias("_orig_doc_no"),        # RPODOC
            F.col("original_document_type").alias("_orig_doc_type"),    # RPODCT
            F.col("company_key_original").alias("_orig_co_key"),        # RPOKCO
        ).agg(
            F.sum("amount_taxable").alias("taxable_amount"),            # RPATXA
            F.sum("amount_tax_exempt").alias("non_taxable_amount"),     # RPATXN
            F.sum("amt_tax_02").alias("tax_amount"),                    # RPSTAM
            F.sum("amount_gross").alias("gross_amount"),                # RPAG
            F.max("tax_area_01").alias("tax_area"),                     # RPTXA1
            F.max("date_service_currency").alias("service_tax_date"),   # RPDSVJ
            F.max("doc_voucher_invoice_e").alias("ar_document_no"),     # RPDOC
            F.max("document_type").alias("ar_document_type"),           # RPDCT
            F.max("company_key").alias("ar_company_key"),               # RPKCO
            F.max("document_pay_item").alias("ar_pay_item"),            # RPSFX
        )
    )

    # ── STEP 2: Pre-aggregate F4211 to invoice grain ──────────────────────
    # Consolidated invoices carry many order_numbers sharing one invoice.
    # MIN(SDDOCO) gives a deterministic representative order per invoice —
    # matches Hubble's Reconciliation Version arbitrary pick (verified against
    # the 4 SBX invoices where 371/249/121/2 orders each collapse to 1 row).
    # first(ignorenulls=True) on ship_to / sold_to / parent / plant is safe
    # because a single invoice ships to one address (verified in Silver).
    sd_invoice = (
        sd.groupBy(
            F.col("doc_voucher_invoice_e").alias("_inv_no"),            # SDDOC
            F.col("document_type").alias("_inv_doc_type"),              # SDDCT
            F.col("company_key").alias("_inv_co_key"),                  # SDKCO
        ).agg(
            F.min("document_order_invoice_e").alias("order_number"),    # SDDOCO — representative
            F.min("order_type").alias("order_type"),                    # SDDCTO — representative
            F.min("line_number").alias("line_number"),                  # SDLNID — representative
            F.first("cost_center", ignorenulls=True).alias("cost_center"),
            F.first("address_number", ignorenulls=True).alias("sold_to"),
            F.first("address_number_ship_to", ignorenulls=True).alias("ship_to"),
            F.first("address_number_parent", ignorenulls=True).alias("parent_number"),
            F.first("tax_explanation_code_01", ignorenulls=True).alias("tax_explanation_code"),
            F.max("dt_for_gl_and_vouch_01").alias("gl_date"),
        )
    )

    # ── STEP 3: 1:1 join at invoice grain ─────────────────────────────────
    j = (sd_invoice.alias("sd")
         .join(ar_invoice.alias("ar"),
               (F.col("sd._inv_no")       == F.col("ar._orig_doc_no")) &
               (F.col("sd._inv_doc_type") == F.col("ar._orig_doc_type")) &
               (F.col("sd._inv_co_key")   == F.col("ar._orig_co_key")),
               "inner")
         .join(bunit.alias("bu"),   F.col("sd.cost_center") == F.col("bu.bu_key"),      "left")
         .join(address.alias("ad"), F.col("sd.ship_to")     == F.col("ad.addr_key"),    "left"))

    # ── STEP 4: Derived columns ───────────────────────────────────────────
    # business_stream — ABSIC (F0101) x MCRP20 (F0006)
    _absic  = F.trim(F.col("ad.sic_code"))
    _mcrp20 = F.trim(F.col("bu.business_stream_code"))
    business_stream = (F.when((_absic == "F") & (_mcrp20 == "ENG"), F.lit("O&G"))
                        .when((_absic != "F") & (_mcrp20 == "ENG"), F.lit("ISP"))
                        .when((_absic != "F") & (_mcrp20 == "SHR"), F.lit("ISP"))
                        .when((_absic == "F") & (_mcrp20 == "SHR"), F.lit("O&G"))
                        .when(~_mcrp20.isin("ENG", "SHR"), F.lit("ISP")))

    # avalara_code — concat formula from the Hubble query:
    #   RTRIM(LTRIM(NVL(SDDOC,-999999999))) || RTRIM(LTRIM(NVL(SDDCT,''))) || RTRIM(LTRIM(NVL(SDKCO,'')))
    # doc_voucher_invoice_e is int64 in Silver; cast to string via long normalises
    # the concat ("11843107") and matches Oracle's NUMBER-to-string behaviour.
    avalara_code = F.concat(
        F.coalesce(F.trim(F.col("sd._inv_no").cast("long").cast("string")), F.lit("-999999999")),
        F.coalesce(F.trim(F.col("sd._inv_doc_type")),                       F.lit("")),
        F.coalesce(F.trim(F.col("sd._inv_co_key").cast("string")),          F.lit("")))

    # ── STEP 5: Final projection — one row per invoice, no aggregation ────
    df = (j.select(
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
        F.col("ar.tax_area"),
        avalara_code.alias("avalara_code"),
        business_stream.alias("business_stream"),
        F.col("ad.sic_code"),
        F.col("ad.jurisdiction"),
        F.col("ad.county"),
        F.col("sd.gl_date"),
        F.col("ar.service_tax_date"),
        (F.col("ar.taxable_amount")     * F.lit(SHIFT_FACTOR)).alias("taxable_amount"),
        (F.col("ar.non_taxable_amount") * F.lit(SHIFT_FACTOR)).alias("non_taxable_amount"),
        (F.col("ar.tax_amount")         * F.lit(SHIFT_FACTOR)).alias("tax_amount"),
        (F.col("ar.gross_amount")       * F.lit(SHIFT_FACTOR)).alias("gross_amount"),
        F.lit(SHIFT_FACTOR).cast("double").alias("shift_factor_applied"),
    )
    # tax_status is stored physically; Direct Lake tables cannot carry
    # DAX calculated columns. Computed here from the SUMmed tax_amount at
    # invoice grain so it reflects the invoice's overall taxability.
    .withColumn("tax_status", F.when(F.col("tax_amount") > 0, F.lit("Taxable"))
                               .otherwise(F.lit("Non-Taxable")))
    .withColumn("document_scope_key",
                sk("document_company", "document_type", "invoice_number"))
    .withColumn("sales_tax_line_key",
                sk("document_company", "invoice_number", "document_type",
                   "plant", "ship_to", "sold_to", "parent_number",
                   "tax_explanation_code", "tax_area", "avalara_code",
                   "business_stream", "sic_code", "jurisdiction", "county",
                   "gl_date", "service_tax_date"))
    )

    # dropDuplicates on sales_tax_line_key — defensive: the pre-aggregation
    # guarantees invoice-grain uniqueness, this catches any Silver-level
    # anomaly that would break the Direct Lake relationship.
    df = df.dropDuplicates(["sales_tax_line_key"])
    return df.select("sales_tax_line_key", "document_scope_key", *FACT_BUSINESS_COLS)


# In[4]:


# ---------------------------------------------------------------------------
# 4) FACT SOURCES
# ---------------------------------------------------------------------------
# Declared Silver sources. Both F4211 and F03B11 are pre-aggregated to invoice
# grain inside build_fact before joining, so the join_pairs shown here describe
# the LOGICAL relationship (SDDOC=RPODOC etc.), not the runtime join predicate.
FACT_SOURCES = [
    {"silver": F4211,  "join": "spine", "join_pairs": []},
    {"silver": F03B11, "join": "inner", "join_pairs": [("original_document_no",   "doc_voucher_invoice_e"),
                                                        ("original_document_type", "document_type"),
                                                        ("company_key_original",   "company_key")]},
    {"silver": F0006,  "join": "left",  "join_pairs": [("cost_center",             "cost_center")]},
    {"silver": F0101,  "join": "left",  "join_pairs": [("address_number",          "address_number_ship_to")]},
    {"silver": F0116,  "join": "left",  "join_pairs": [("address_number",          "address_number")]},   # via F0101 (ABAN8 = ALAN8)
]


# In[5]:


# ---------------------------------------------------------------------------
# 5) RUN
# ---------------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

# preflight — confirm every declared Silver source exists before building
for _s in FACT_SOURCES:
    print("  source {:<40s} {}".format(_s["silver"],
                                       "OK" if spark.catalog.tableExists(sname(_s["silver"])) else "MISSING"))

_run_start = time.time()
if MANUAL_OVERWRITE or not spark.catalog.tableExists(gname(FACT)):
    print("== FULL LOAD ==")
    new = build_fact()
    (new.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(gname(FACT)))
    _rows   = new.count()
    _status = "built"
    print("  {} rows={}".format(FACT, _rows))
else:
    print("== skip — {} exists and MANUAL_OVERWRITE=False ==".format(gname(FACT)))
    _rows, _status = None, "skipped"

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

