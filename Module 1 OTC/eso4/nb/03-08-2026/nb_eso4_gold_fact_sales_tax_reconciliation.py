#!/usr/bin/env python
# coding: utf-8

# Fabric notebook · nb_eso4_gold_fact_sales_tax_reconciliation
# ============================================================================
# ESO4 Gold fact table — Sales Tax with Business Stream Summary (Avalara
# reconciliation). Dims are built by separate notebooks.
# Gold schema: `rpt`, lakehouse: `lh_jde_gold`.
#
# ARCHITECTURE
# ─────────────────────────────────────────────────────────────────────────────
# Batch build. Reads the full Silver snapshot of each source, runs build_fact()
# once, and overwrite-writes the Gold fact. No streaming / CDF.
#
# JOINS  (NO fact-row filters)
# ─────────────────────────────────────────────────────────────────────────────
#   Source        Role             Join
#   F4211         Spine            (spine)
#   F03B11        Customer ledger  INNER  (SDDOC=RPODOC, SDDCT=RPODCT, SDKCO=RPOKCO, SDLNID=RPLNID)
#   F0006         Business unit    LEFT   (SDMCU=MCMCU) → business_stream calc input
#   F0101 ⋈ F0116 Address          LEFT   (SDSHAN=ABAN8 ; ABAN8=ALAN8) latest-effective + ABAT1 band
#   (F0005 is NOT read here — dim_sic / dim_state resolve descriptions in the model.)
#
# SOFT DELETES
# ─────────────────────────────────────────────────────────────────────────────
# load_silver_table() strips is_delete = 1 rows (and the audit columns) before any join.
#
# GRAIN
# ─────────────────────────────────────────────────────────────────────────────
# One row per outer GROUP BY tuple: the inner SELECT DISTINCT (incl. the F03B11 PK)
# → groupBy(FACT_GROUP_BY_COLS).sum(4 amounts). tax_status is derived post-aggregate.
#
# FIRST RUN / OVERWRITE
# ─────────────────────────────────────────────────────────────────────────────
# MANUAL_OVERWRITE = True  → drop + rebuild from the full Silver snapshot.
# MANUAL_OVERWRITE = False → build only if the table is missing; else leave it untouched.
# ============================================================================

from pyspark.sql import functions as F, Window
import json, time
from datetime import datetime, timezone

# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------
SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

MANUAL_OVERWRITE = True   # True = drop + rebuild; False = build only if missing

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

F4211  = "f4211_sales_order_detail_file"
F03B11 = "f03b11_customer_ledger"
F0006  = "f0006_business_unit_master"
F0101  = "f0101_address_book_master"
F0116  = "f0116_address_by_date"

FACT = "fact_sales_tax_reconciliation"

# Silver is already decoded (the RAW JDE integer amounts carried an implied 0.01 scale),
# so this scaling placeholder is 1.0 (carried for lineage).
SHIFT_FACTOR = 1.0

_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def load_silver_table(t):
    """Read a Silver table and strip soft-deleted rows (is_delete = 1) + the audit columns."""
    df = spark.read.table(sname(t))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

def sk(*cols):
    return F.sha2(F.concat_ws("||", *[F.col(c).cast("string") if isinstance(c, str) else c.cast("string")
                                       for c in cols]), 256)

# ----------------------------------------------------------------------------
# 2) FACT BUILDER
# ----------------------------------------------------------------------------
# ESO4 is PRE-AGGREGATED to the outer GROUP BY grain (one row per display tuple, the four
# amounts SUMmed across F03B11 pay items) — so build_fact() ends in groupBy(*FACT_GROUP_BY_COLS)
# .agg(sum(...)). That GROUP BY needs an explicit key/measure/carry/projection column set, which is
# what the FACT_* lists below declare (FACT_GROUP_BY_COLS also seeds the sales_tax_line_key hash;
# FACT_BUSINESS_COLS drives the final .select).
#
# The GROUP BY key set (de-duplicated: RPOKCO=SDKCO, RPODOC=SDDOC,
# RPODCT=SDDCT and the C_/duplicate copies are redundant; XID_CUSTOM_8501fca6bf83d39 =
# concat(SDDOC,SDDCT) is functionally dependent on invoice_number+document_type already here).
FACT_GROUP_BY_COLS = [
    # ── degenerate / document identifiers ──
    "document_company",        # XF4211_SDKCO (= XF03B11_RPOKCO)
    "invoice_number",          # XF4211_SDDOC (= XF03B11_RPODOC)
    "document_type",           # XF4211_SDDCT (= XF03B11_RPODCT)
    "order_number",            # XF4211_SDDOCO
    "order_type",              # XF4211_SDDCTO
    # ── dimension FKs ──
    "plant",                   # XF4211_SDMCU (= XC_F4211_SDMCU)
    "ship_to",                 # XF4211_SDSHAN
    "sold_to",                 # XF4211_SDAN8
    "parent_number",           # XF4211_SDPA8
    # ── tax attributes (degenerate) ──
    "tax_explanation_code",    # XF4211_SDEXR1
    "tax_area",                # XF03B11_RPTXA1
    "avalara_code",            # XID_CUSTOM_8501fecff7aa51 (calc)
    # ── classification (calc + denormalized slicer attrs) ──
    "business_stream",         # XID_CUSTOM_84c76537583683 (calc; degenerate — needs ABSIC×MCRP20)
    "sic_code",                # XF0101_ABSIC — FK → dim_sic (description resolved in the model)
    # ── address / FK codes ──
    "jurisdiction",            # XF0116_ALADDS raw code (e.g. "CO") — FK → dim_state (name in dim)
    "county",                  # XF0116_ALCOUN (degenerate — not in the reused address dim)
    # ── raw event dates ──
    "gl_date",                 # XF4211_SDDGL
    "service_tax_date",        # XF03B11_RPDSVJ
]
# Attributes functionally dependent on the grain (carried through the aggregation, not grouped on) —
# shift_factor_applied (constant) and line_number (SDLNID; one F4211 line per recon tuple, so the
# per-group MAX is that line's number and adding it changes neither the grain, the row count, nor any SUM).
# Full star schema: plant_name (reused dim_plant, keyed by plant_code = SDMCU), sic_description (dim_sic),
# and the state name (dim_state) all live in dimensions now — only the constant remains on the fact.
# The plant's business-stream code (MCRP20) is available on dim_plant (category_code_cost_ct_020); the
# fact still reads it straight from F0006 for the business_stream CALC (below), so it is neither grouped
# on nor carried as a stored column.
# Stored fact columns, in report order — degenerate dims + FK codes + measures ONLY (star schema).
FACT_BUSINESS_COLS = [
    "document_company", "invoice_number", "document_type", "order_number", "order_type", "line_number",
    "plant", "ship_to", "sold_to", "parent_number",   # FKs → dim_plant / dim_address_*
    "tax_explanation_code", "tax_area", "avalara_code",
    "business_stream", "sic_code",                    # business_stream calc (degenerate); sic_code FK → dim_sic
    "jurisdiction", "county",                         # jurisdiction FK → dim_state; county degenerate
    "gl_date", "service_tax_date",
    "taxable_amount", "non_taxable_amount", "tax_amount", "gross_amount",
    "tax_status",                                     # derived from the SUMMED tax_amount — see build_fact
    "shift_factor_applied",
]
# Column arithmetic: 25 FACT_BUSINESS_COLS + sales_tax_line_key + document_scope_key = 27 stored.

def build_fact():
    sd  = load_silver_table(F4211)       # sales order detail
    ar  = load_silver_table(F03B11)      # customer ledger
    bu  = load_silver_table(F0006)       # business unit master (business_stream calc input)
    ab  = load_silver_table(F0101)       # address book master  (sic_code)
    adr = load_silver_table(F0116)       # address by date      (jurisdiction/county)

    # F0101 INNER F0116 (ABAN8 = ALAN8) collapsed to one row per address so the LEFT join
    # to F4211 can't fan the grain out. sic_code + jurisdiction stay RAW FK CODES here — their
    # descriptions are resolved in the model via dim_sic (F0005 01/SC) and dim_state (F0005 00/S),
    # built by nb_eso4_gold_dim_udc.py. (Full star schema — F0005 is no longer read on the fact.)
    #
    # LATEST-EFFECTIVE pick (NOT groupBy/first): F0116 is effective-dated (ALEFTB), so an address that
    # has moved has >1 row. An unordered first() would pick a row non-deterministically — two runs could
    # assign the same invoice a different jurisdiction/county, and (via ABSIC) flip its business_stream.
    # row_number() over date_beginning_effective DESC makes the pick STABLE.
    # date_updated (ALUPMJ) breaks ties on equal effective dates.
    _adr_w = Window.partitionBy(F.col("ad.address_number")).orderBy(
        F.col("ad.date_beginning_effective").desc_nulls_last(),
        F.col("ad.date_updated").desc_nulls_last())
    # The F0101 address subquery is gated to the ABAT1 search-type band
    #   WHERE (ABAT1 BETWEEN 'A  ' AND 'P  ') OR (ABAT1 BETWEEN 'R  ' AND 'ZZZ')   (excludes the 'Q' band).
    # Implemented as a VALUE QUALIFICATION, not a fact-row filter. It gates ONLY the F0101 ⋈ F0116
    # address lookup, which is LEFT-joined to F4211 — so an out-of-band ship-to KEEPS its fact row and
    # simply gets NULL sic_code / jurisdiction / county. NO F4211 line is dropped, so the Gold
    # "no business filters" rule is preserved.
    # ABAT1 = address_type_01; rpad to 3 mirrors the space-padded 'A  '/'P  '/'ZZZ' bounds.
    _abat1 = F.rpad(F.rtrim(F.col("ab.address_type_01")), 3, " ")
    _abat1_band = (((_abat1 >= F.lit("A  ")) & (_abat1 <= F.lit("P  "))) |
                   ((_abat1 >= F.lit("R  ")) & (_abat1 <= F.lit("ZZZ"))))
    address = (ab.alias("ab")
               .join(adr.alias("ad"), F.col("ab.address_number") == F.col("ad.address_number"), "inner")
               .where(_abat1_band)                                                        # F0101 ABAT1 band (address lookup only)
               .withColumn("_arn", F.row_number().over(_adr_w))
               .where(F.col("_arn") == 1)
               .select(F.col("ab.address_number").alias("addr_key"),
                       F.trim(F.col("ab.standard_industry_code")).alias("sic_code"),      # ABSIC  (FK dim_sic)
                       F.trim(F.col("ad.state")).alias("jurisdiction"),                   # ALADDS (FK dim_state)
                       F.trim(F.col("ad.county_address")).alias("county")))               # ALCOUN

    # F0006 collapsed to one row per business unit — business_stream_code (MCRP20) feeds the
    # Business Stream calc only; plant_name lives in the reused dim_plant (not carried on the fact).
    bunit = (bu.groupBy(F.col("cost_center").alias("bu_key"))
             .agg(F.first(F.trim(F.col("category_code_cost_ct_020")), ignorenulls=True).alias("business_stream_code")))  # MCRP20

    # ── joins (every join used; NO fact-row filters — the only predicate is the F0101
    #    ABAT1 address-band above, which qualifies the LEFT-joined address lookup, not the fact) ──
    j = (sd.alias("sd")
         .join(ar.alias("ar"),                                                     # INNER
               (F.col("sd.doc_voucher_invoice_e") == F.col("ar.original_document_no")) &   # SDDOC = RPODOC
               (F.col("sd.document_type")         == F.col("ar.original_document_type")) & # SDDCT = RPODCT
               (F.col("sd.company_key")           == F.col("ar.company_key_original")) &   # SDKCO = RPOKCO
               (F.col("sd.line_number")           == F.col("ar.line_number")), "inner")    # SDLNID = RPLNID
         .join(bunit.alias("bu"), F.col("sd.cost_center") == F.col("bu.bu_key"), "left")   # SDMCU = MCMCU
         .join(address.alias("ad"), F.col("sd.address_number_ship_to") == F.col("ad.addr_key"), "left"))  # SDSHAN = ABAN8

    # ── Business Stream (calculation) — ABSIC (F0101) × MCRP20 (F0006) ──────
    _absic  = F.trim(F.col("ad.sic_code"))
    _mcrp20 = F.trim(F.col("bu.business_stream_code"))
    business_stream = (F.when((_absic == "F") & (_mcrp20 == "ENG"), F.lit("O&G"))
                        .when((_absic != "F") & (_mcrp20 == "ENG"), F.lit("ISP"))
                        .when((_absic != "F") & (_mcrp20 == "SHR"), F.lit("ISP"))
                        .when((_absic == "F") & (_mcrp20 == "SHR"), F.lit("O&G"))
                        .when(~_mcrp20.isin("ENG", "SHR"), F.lit("ISP")))

    # ── Avalara Code (calculation) ──
    # Concat formula: RTRIM(LTRIM(NVL(SDDOC,-999999999)))
    #          || RTRIM(LTRIM(NVL(SDDCT,''))) || RTRIM(LTRIM(NVL(SDKCO,''))).
    # SDDOC (doc_voucher_invoice_e) is int64 in Silver; cast to string for the concat (the long cast is a
    # harmless normalization — matches Oracle's integer NUMBER concat, "11843107"). SDKCO stays a string so
    # leading zeros ("00400") are preserved.
    avalara_code = F.concat(
        F.coalesce(F.trim(F.col("sd.doc_voucher_invoice_e").cast("long").cast("string")), F.lit("-999999999")),
        F.coalesce(F.trim(F.col("sd.document_type")),                                      F.lit("")),
        F.coalesce(F.trim(F.col("sd.company_key").cast("string")),                         F.lit("")))

    sel = j.select(
        # ── degenerate / document identifiers ──
        F.col("sd.company_key").alias("document_company"),               # SDKCO ("Document Company")
        # the JDE numeric identifiers below are NOT cast — Silver already stores them as int64, so an
        # explicit .cast("long") is unnecessary and the columns match the TMDL int64 (and the int64
        # dim_address_* PKs) natively. (avalara_code still casts internally; document_company /
        # document_type stay STRING so JDE leading zeros survive.)
        F.col("sd.doc_voucher_invoice_e").alias("invoice_number"),       # SDDOC
        F.col("sd.document_type").alias("document_type"),                # SDDCT
        F.col("sd.document_order_invoice_e").alias("order_number"),      # SDDOCO
        F.col("sd.order_type").alias("order_type"),                      # SDDCTO
        F.col("sd.line_number").alias("line_number"),                    # SDLNID — carried, NOT grouped (one F4211 line per recon tuple)
        F.col("ar.doc_voucher_invoice_e").alias("ar_document_no"),       # RPDOC  (F03B11 PK)
        F.col("ar.document_type").alias("ar_document_type"),             # RPDCT  (F03B11 PK)
        F.col("ar.company_key").alias("ar_company_key"),                 # RPKCO  (F03B11 PK)
        F.col("ar.document_pay_item").alias("ar_pay_item"),              # RPSFX  (F03B11 PK)
        # ── dimension FKs ──
        F.trim(F.col("sd.cost_center")).alias("plant"),                  # SDMCU -> dim_plant.plant_code
        F.col("sd.address_number").alias("sold_to"),                # SDAN8
        F.col("sd.address_number_ship_to").alias("ship_to"),        # SDSHAN
        F.col("sd.address_number_parent").alias("parent_number"),   # SDPA8
        # ── tax attributes ──
        F.col("sd.tax_explanation_code_01").alias("tax_explanation_code"),  # SDEXR1
        F.col("ar.tax_area_01").alias("tax_area"),                          # RPTXA1
        avalara_code.alias("avalara_code"),
        # ── classification (FK codes; descriptions resolved in the model) ──
        business_stream.alias("business_stream"),                        # calc (§7) — degenerate
        F.col("ad.sic_code").alias("sic_code"),                          # ABSIC (F0101) — FK → dim_sic
        # ── address FK code + degenerate county ──
        F.col("ad.jurisdiction").alias("jurisdiction"),                  # ALADDS (F0116) raw code — FK → dim_state
        F.col("ad.county").alias("county"),                              # ALCOUN (F0116) — degenerate
        # ── raw event dates ──
        F.col("sd.dt_for_gl_and_vouch_01").alias("gl_date"),             # SDDGL
        F.col("ar.date_service_currency").alias("service_tax_date"),     # RPDSVJ
        # ── measures (× SHIFT_FACTOR) ──
        (F.col("ar.amount_taxable")    * F.lit(SHIFT_FACTOR)).alias("taxable_amount"),      # RPATXA
        (F.col("ar.amount_tax_exempt") * F.lit(SHIFT_FACTOR)).alias("non_taxable_amount"),  # RPATXN
        (F.col("ar.amt_tax_02")        * F.lit(SHIFT_FACTOR)).alias("tax_amount"),          # RPSTAM
        (F.col("ar.amount_gross")      * F.lit(SHIFT_FACTOR)).alias("gross_amount"),        # RPAG
        F.lit(SHIFT_FACTOR).cast("double").alias("shift_factor_applied"),
    ).distinct()                                            # SELECT DISTINCT (incl F03B11 PK)

    # ── GROUP BY (outer query): collapse pay items sharing a display tuple, SUM the
    #    four amounts. The F03B11 PK columns (ar_*) were kept in `sel` only to make the inner
    #    DISTINCT count each pay item once; they are dropped here (not in the GROUP BY). ──
    agg = (sel.groupBy(*FACT_GROUP_BY_COLS)
           .agg(F.sum("taxable_amount").alias("taxable_amount"),
                F.sum("non_taxable_amount").alias("non_taxable_amount"),
                F.sum("tax_amount").alias("tax_amount"),
                F.sum("gross_amount").alias("gross_amount"),
                F.first("shift_factor_applied").alias("shift_factor_applied"),          # constant
                F.max("line_number").alias("line_number")))                             # carried: one F4211 line per recon tuple (max = that line; deterministic)

    df = (agg
          # Tax Status — a PHYSICAL column, not a DAX calculated column: Direct Lake tables cannot
          # carry calculated columns (the same constraint that bars calculated columns everywhere in
          # this model). Computed HERE, after the aggregate, from the SUMMED tax_amount —
          # exactly what the retired DAX `IF(fact[tax_amount] > 0, ...)` evaluated. It must NOT move
          # into `sel`: pre-aggregation it would be derived from a single pay item's amt_tax_02 and
          # would have to join FACT_GROUP_BY_COLS, changing the grain.
          .withColumn("tax_status", F.when(F.col("tax_amount") > 0, F.lit("Taxable"))
                                     .otherwise(F.lit("Non-Taxable")))
          # a coarser invoice-document scope key + the unique key at the GROUP BY grain
          .withColumn("document_scope_key",
                      sk("document_company", "document_type", "invoice_number"))
          .withColumn("sales_tax_line_key", sk(*FACT_GROUP_BY_COLS)))

    df = df.dropDuplicates(["sales_tax_line_key"])         # unique by construction (GROUP BY); defensive
    return df.select("sales_tax_line_key", "document_scope_key", *FACT_BUSINESS_COLS)

# ----------------------------------------------------------------------------
# 3) FACT SOURCES
# ----------------------------------------------------------------------------
# Declares each Silver source and how it joins to the F4211 spine. build_fact() applies these
# joins directly — there is no CDF affected-lines machinery here (batch build); the RUN preflight
# below uses this list to confirm every source exists before the build. join_pairs: (source_col, f4211_col) tuples.
FACT_SOURCES = [
    {"silver": F4211,  "join": "spine", "join_pairs": []},
    {"silver": F03B11, "join": "inner", "join_pairs": [("original_document_no",   "doc_voucher_invoice_e"),
                                                        ("original_document_type", "document_type"),
                                                        ("company_key_original",   "company_key"),
                                                        ("line_number",            "line_number")]},
    {"silver": F0006,  "join": "left",  "join_pairs": [("cost_center",             "cost_center")]},
    {"silver": F0101,  "join": "left",  "join_pairs": [("address_number",          "address_number_ship_to")]},
    {"silver": F0116,  "join": "left",  "join_pairs": [("address_number",          "address_number")]},  # via F0101 (ABAN8 = ALAN8)
]

# ----------------------------------------------------------------------------
# 4) RUN
# ----------------------------------------------------------------------------
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
