#!/usr/bin/env python
# coding: utf-8

# In[1]:

# ## nb_eso4_gold_fact_sales_tax_reconciliation
#
# **Gold `fact_sales_tax_reconciliation` processor** for Extended Sales Order 4 (Sales Tax
# with Business Stream Summary — Avalara reconciliation). Batch build of ONE table —
# `lh_jde_gold.rpt.fact_sales_tax_reconciliation` (invoice sales-tax-line grain) — from the
# Silver sales-order detail (F4211) and customer-ledger (F03B11), joined to the business-unit
# (F0006) and address (F0101 ⋈ F0116) snapshots.
#
# JOINS — every join from Extended Sales Order 4.docx §4 is applied (NO report filters):
#   F4211 INNER F03B11 : SDDOC=RPODOC, SDDCT=RPODCT, SDKCO=RPOKCO, SDLNID=RPLNID
#   F4211 LEFT  F0006  : SDMCU=MCMCU
#   F4211 LEFT (F0101 INNER F0116) : SDSHAN=ABAN8 ; ABAN8=ALAN8
#   (Hubble-only F03B11→company ShiftFactor join is a constant here — see SHIFT_FACTOR.)
# STAR SCHEMA: the fact stores FK codes only (sic_code, jurisdiction) — SIC/State descriptions
#   resolve through dim_sic / dim_state (F0005, built by nb_eso4_gold_dim_udc.py); plant_name /
#   business_stream_code resolve through dim_business_unit. F0005 is NOT read on the fact.
# Design: eso4/docs/ESO4_gold_layer_design.md



# =============================================================================
# CONFIG + CONSTANTS  (names per Fabric_Naming_Convention_Guidelines.pdf)
# =============================================================================
from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD_SCHEMA = "lh_jde_gold.rpt"

# ── manual reprocess switch ──────────────────────────────────────────────────
#   OVERWRITE = True  -> drop + rebuild the fact from the full Silver snapshot.
#   OVERWRITE = False -> build only if the table is missing; otherwise leave it untouched.
OVERWRITE       = True

# ── Silver sources ────────────────────────────────────────────────────────────
SRC_SCHEMA    = "jde_cdc"     # Silver Change Data Feed schema; CDF must be enabled on every
                          #   STREAMED source (F4211 / F03B11) read below.
SRC_LAKEHOUSE = "lh_jde_silver"
F4211_TBL   = "f4211_sales_order_detail_file"      # streamed
F03B11_TBL  = "f03b11_customer_ledger"             # streamed
F0006_TBL   = "f0006_business_unit_master"         # static snapshot (business_stream calc input MCRP20)
F0101_TBL   = "f0101_address_book_master"          # static snapshot (sic_code FK)
F0116_TBL   = "f0116_address_by_date"              # static snapshot (jurisdiction FK / county)
# F0005 (UDC) is NOT read here — dim_sic (01/SC) + dim_state (00/S) are built by nb_eso4_gold_dim_udc.py.

# ── Gold target BUILT here (new, eso4) ─────────────────────────────────────────
T_FACT      = f"{GOLD_SCHEMA}.fact_sales_tax_reconciliation"

# ── report scaling ─────────────────────────────────────────────────────────────
# Hubble multiplied the four amounts by NVL(company.ShiftFactor, 0.01) to de-scale RAW JDE
# integer amounts (stored ×100). Our Silver is already decoded (implied decimals resolved), so
# the placeholder is 1.0 — same treatment as ESO1's SHIFT_FACTOR. `shift_factor_applied` is
# carried on the fact for lineage. (See design §"ShiftFactor".)
SHIFT_FACTOR    = 1.0

print(f"ESO4 Gold fact processor — target {T_FACT}  OVERWRITE={OVERWRITE}")

# In[2]:


# =============================================================================
# HELPERS  (identical to nb_eso1_gold_fact_sales_order_freight)
# =============================================================================
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def sname(table_name):
    """Fully-qualified Silver source name (== ESO7 v2 sname)."""
    return f"{SRC_LAKEHOUSE}.{SRC_SCHEMA}.{table_name}"

def load_silver_table(table_name):
    df = spark.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

def sk(*cols):
    return F.sha2(F.concat_ws("||", *[F.col(c).cast("string") if isinstance(c, str) else c.cast("string")
                                       for c in cols]), 256)

# In[3]:

def _write_table(df, target):
    """Overwrite-write a Gold table (schema + data replaced)."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(target))

# In[4]:


# =============================================================================
# FACT  fact_sales_tax_reconciliation  (Hubble reconciliation grain)
#   Grain = one row per Hubble GROUP BY tuple (see `hubble query.txt`): the report display
#   columns below. Hubble's outer query SUMs the four amounts across F03B11 pay items — the
#   F03B11 PK (RPDOC/RPDCT/RPKCO/RPSFX) lives in the inner SELECT DISTINCT only, NOT the
#   GROUP BY — so pay items sharing a display tuple collapse into one summed row. Business
#   Stream + Avalara Code are row-level calculations (§7). NO report filters applied (docx §5
#   filters are report slicers, denormalized onto the fact instead).
# =============================================================================
# The GROUP BY key set (== Hubble `GROUP BY`, de-duplicated: RPOKCO=SDKCO, RPODOC=SDDOC,
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
# Attributes functionally dependent on the grain (carried through the aggregation, not grouped on).
# Full star schema: plant_name / business_stream_code (dim_business_unit), sic_description (dim_sic),
# and the state name (dim_state) all live in dimensions now — only the constant remains on the fact.
# business_stream_code is dropped from the GROUP BY too (functionally dependent on plant → dim_business_unit).
FACT_CARRY_COLS = ["shift_factor_applied"]
# SUMmed measures (Hubble ReportColumn1-4).
FACT_MEASURE_COLS = ["taxable_amount", "non_taxable_amount", "tax_amount", "gross_amount"]
# Stored fact columns, in report order — degenerate dims + FK codes + measures ONLY (star schema).
FACT_BUSINESS_COLS = [
    "document_company", "invoice_number", "document_type", "order_number", "order_type",
    "plant", "ship_to", "sold_to", "parent_number",   # FKs → dim_business_unit / dim_address_*
    "tax_explanation_code", "tax_area", "avalara_code",
    "business_stream", "sic_code",                    # business_stream calc (degenerate); sic_code FK → dim_sic
    "jurisdiction", "county",                         # jurisdiction FK → dim_state; county degenerate
    "gl_date", "service_tax_date",
    "taxable_amount", "non_taxable_amount", "tax_amount", "gross_amount",
    "tax_status",                                     # derived from the SUMMED tax_amount — see transform_fact
    "shift_factor_applied",
]
# Column arithmetic: 18 FACT_GROUP_BY_COLS + 1 FACT_CARRY_COLS + 4 FACT_MEASURE_COLS + 1 derived
# (tax_status) = 24 FACT_BUSINESS_COLS, + sales_tax_line_key + document_scope_key = 26 stored.

def transform_fact():
    sd  = load_silver_table(F4211_TBL)       # sales order detail
    ar  = load_silver_table(F03B11_TBL)      # customer ledger
    bu  = load_silver_table(F0006_TBL)       # business unit master (business_stream calc input)
    ab  = load_silver_table(F0101_TBL)       # address book master  (sic_code)
    adr = load_silver_table(F0116_TBL)       # address by date      (jurisdiction/county)

    # F0101 INNER F0116 (docx §4: ABAN8 = ALAN8) collapsed to one row per address so the LEFT join
    # to F4211 can't fan the grain out. sic_code + jurisdiction stay RAW FK CODES here — their
    # descriptions are resolved in the model via dim_sic (F0005 01/SC) and dim_state (F0005 00/S),
    # built by nb_eso4_gold_dim_udc.py. (Full star schema — F0005 is no longer read on the fact.)
    #
    # LATEST-EFFECTIVE pick (NOT groupBy/first): F0116 is effective-dated (ALEFTB), so an address that
    # has moved has >1 row. An unordered first() would pick a row non-deterministically — two runs could
    # assign the same invoice a different jurisdiction/county, and (via ABSIC) flip its business_stream.
    # row_number() over date_beginning_effective DESC makes the pick STABLE and matches the convention
    # the reused rpt.dim_address_book already implements ("F0101 ⋈ F0116 latest-effective", design §3)
    # and ESO1's F0116 join. date_updated (ALUPMJ) breaks ties on equal effective dates.
    _adr_w = Window.partitionBy(F.col("ad.address_number")).orderBy(
        F.col("ad.date_beginning_effective").desc_nulls_last(),
        F.col("ad.date_updated").desc_nulls_last())
    # Hubble gates the F0101 address subquery to the ABAT1 search-type band
    #   WHERE (ABAT1 BETWEEN 'A  ' AND 'P  ') OR (ABAT1 BETWEEN 'R  ' AND 'ZZZ')   (excludes the 'Q' band).
    # Reproduced the SAME WAY as ESO5 (nb_eso5_gold_fact_extended_sales_order_5, §3a-bis): a VALUE
    # QUALIFICATION, not a fact-row filter. It gates ONLY the F0101 ⋈ F0116 address lookup, which is
    # LEFT-joined to F4211 — so an out-of-band ship-to KEEPS its fact row and simply gets NULL
    # sic_code / jurisdiction / county, EXACTLY as Hubble's LEFT-JOIN-of-a-WHERE-filtered-subquery does.
    # NO F4211 line is dropped, so the Gold "no business filters" rule is preserved. This CLOSES the
    # gate-review Finding 5 variance (ESO4 now matches Hubble on business_stream/jurisdiction/county).
    # ABAT1 = address_type_01; rpad to 3 mirrors the space-padded 'A  '/'P  '/'ZZZ' bounds.
    _abat1 = F.rpad(F.rtrim(F.col("ab.address_type_01")), 3, " ")
    _abat1_band = (((_abat1 >= F.lit("A  ")) & (_abat1 <= F.lit("P  "))) |
                   ((_abat1 >= F.lit("R  ")) & (_abat1 <= F.lit("ZZZ"))))
    address = (ab.alias("ab")
               .join(adr.alias("ad"), F.col("ab.address_number") == F.col("ad.address_number"), "inner")
               .where(_abat1_band)                                                        # Hubble F0101 ABAT1 band (address lookup only)
               .withColumn("_arn", F.row_number().over(_adr_w))
               .where(F.col("_arn") == 1)
               .select(F.col("ab.address_number").alias("addr_key"),
                       F.trim(F.col("ab.standard_industry_code")).alias("sic_code"),      # ABSIC  (FK dim_sic)
                       F.trim(F.col("ad.state")).alias("jurisdiction"),                   # ALADDS (FK dim_state)
                       F.trim(F.col("ad.county_address")).alias("county")))               # ALCOUN

    # F0006 collapsed to one row per business unit — business_stream_code (MCRP20) feeds the
    # Business Stream calc only; plant_name lives in dim_business_unit (not carried on the fact).
    bunit = (bu.groupBy(F.col("cost_center").alias("bu_key"))
             .agg(F.first(F.trim(F.col("category_code_cost_ct_020")), ignorenulls=True).alias("business_stream_code")))  # MCRP20

    # ── joins (docx §4, every join used; NO fact-row filters — the only predicate is the F0101
    #    ABAT1 address-band above, which qualifies the LEFT-joined address lookup, not the fact) ──
    j = (sd.alias("sd")
         .join(ar.alias("ar"),                                                     # INNER
               (F.col("sd.doc_voucher_invoice_e") == F.col("ar.original_document_no")) &   # SDDOC = RPODOC
               (F.col("sd.document_type")         == F.col("ar.original_document_type")) & # SDDCT = RPODCT
               (F.col("sd.company_key")           == F.col("ar.company_key_original")) &   # SDKCO = RPOKCO
               (F.col("sd.line_number")           == F.col("ar.line_number")), "inner")    # SDLNID = RPLNID
         .join(bunit.alias("bu"), F.col("sd.cost_center") == F.col("bu.bu_key"), "left")   # SDMCU = MCMCU
         .join(address.alias("ad"), F.col("sd.address_number_ship_to") == F.col("ad.addr_key"), "left"))  # SDSHAN = ABAN8

    # ── Business Stream (docx §7 calculation) — ABSIC (F0101) × MCRP20 (F0006) ──────
    _absic  = F.trim(F.col("ad.sic_code"))
    _mcrp20 = F.trim(F.col("bu.business_stream_code"))
    business_stream = (F.when((_absic == "F") & (_mcrp20 == "ENG"), F.lit("O&G"))
                        .when((_absic != "F") & (_mcrp20 == "ENG"), F.lit("ISP"))
                        .when((_absic != "F") & (_mcrp20 == "SHR"), F.lit("ISP"))
                        .when((_absic == "F") & (_mcrp20 == "SHR"), F.lit("O&G"))
                        .when(~_mcrp20.isin("ENG", "SHR"), F.lit("ISP")))

    # ── Avalara Code (docx §6 col 16 "Calculation — See Below"; §7 does not define it) ──
    # INFERRED from Hubble XID_CUSTOM_8501fecff7aa51 = RTRIM(LTRIM(NVL(SDDOC,-999999999)))
    #          || RTRIM(LTRIM(NVL(SDDCT,''))) || RTRIM(LTRIM(NVL(SDKCO,''))). Flagged in design.
    # SDDOC (doc_voucher_invoice_e) is a Silver decimal; cast via long to drop the fractional part
    # ("11843107.000000000000000000" → "11843107") — matches Oracle's integer NUMBER concat. SDKCO
    # stays a string so leading zeros ("00400") are preserved.
    avalara_code = F.concat(
        F.coalesce(F.trim(F.col("sd.doc_voucher_invoice_e").cast("long").cast("string")), F.lit("-999999999")),
        F.coalesce(F.trim(F.col("sd.document_type")),                                      F.lit("")),
        F.coalesce(F.trim(F.col("sd.company_key").cast("string")),                         F.lit("")))

    sel = j.select(
        # ── degenerate / document identifiers ──
        F.col("sd.company_key").alias("document_company"),               # SDKCO ("Document Company")
        # ⚠ .cast("long") on every JDE numeric identifier below: these are Silver DECIMALs
        # ("11843107.000000000000000000"), while the TMDL declares them int64 — an uncast decimal
        # either fails the Direct Lake load or renders "11843107.000000000000000000" in slicers, and
        # ship_to/sold_to/parent_number would relate to dim_address_*.address_number (int64) across a
        # type boundary. Same treatment avalara_code already applies below. document_company /
        # document_type stay STRING so JDE leading zeros ("00400") survive.
        # NOTE: invoice_number feeds document_scope_key via sk() (which stringifies) — the two stream
        # handlers cast the SAME column identically so the delete-scope hash matches. Keep in sync.
        F.col("sd.doc_voucher_invoice_e").cast("long").alias("invoice_number"),       # SDDOC
        F.col("sd.document_type").alias("document_type"),                # SDDCT
        F.col("sd.document_order_invoice_e").cast("long").alias("order_number"),      # SDDOCO
        F.col("sd.order_type").alias("order_type"),                      # SDDCTO
        F.col("ar.doc_voucher_invoice_e").alias("ar_document_no"),       # RPDOC  (F03B11 PK)
        F.col("ar.document_type").alias("ar_document_type"),             # RPDCT  (F03B11 PK)
        F.col("ar.company_key").alias("ar_company_key"),                 # RPKCO  (F03B11 PK)
        F.col("ar.document_pay_item").alias("ar_pay_item"),              # RPSFX  (F03B11 PK)
        # ── dimension FKs ──
        F.trim(F.col("sd.cost_center")).alias("plant"),                  # SDMCU -> dim_business_unit
        F.col("sd.address_number").cast("long").alias("sold_to"),                # SDAN8  (docx col 19 "Sold To")
        F.col("sd.address_number_ship_to").cast("long").alias("ship_to"),        # SDSHAN (docx col 20 "Ship To")
        F.col("sd.address_number_parent").cast("long").alias("parent_number"),   # SDPA8
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
    ).distinct()                                            # == Hubble inner SELECT DISTINCT (incl F03B11 PK)

    # ── Hubble GROUP BY (outer query): collapse pay items sharing a display tuple, SUM the
    #    four amounts. The F03B11 PK columns (ar_*) were kept in `sel` only to make the inner
    #    DISTINCT count each pay item once; they are dropped here (not in the GROUP BY). ──
    agg = (sel.groupBy(*FACT_GROUP_BY_COLS)
           .agg(F.sum("taxable_amount").alias("taxable_amount"),           # SUM ReportColumn1
                F.sum("non_taxable_amount").alias("non_taxable_amount"),   # SUM ReportColumn2
                F.sum("tax_amount").alias("tax_amount"),                   # SUM ReportColumn3
                F.sum("gross_amount").alias("gross_amount"),               # SUM ReportColumn4
                F.first("shift_factor_applied").alias("shift_factor_applied")))         # constant

    df = (agg
          # Tax Status — a PHYSICAL column, not a DAX calculated column: Direct Lake tables cannot
          # carry calculated columns (same constraint that forces dim_business_unit.business_unit_display
          # to be built in Spark). Computed HERE, after the aggregate, from the SUMMED tax_amount —
          # exactly what the retired DAX `IF(fact[tax_amount] > 0, ...)` evaluated. It must NOT move
          # into `sel`: pre-aggregation it would be derived from a single pay item's amt_tax_02 and
          # would have to join FACT_GROUP_BY_COLS, changing the grain.
          .withColumn("tax_status", F.when(F.col("tax_amount") > 0, F.lit("Taxable"))
                                     .otherwise(F.lit("Non-Taxable")))
          # delete-scope key (invoice document) + unique key at the GROUP BY grain
          .withColumn("document_scope_key",
                      sk("document_company", "document_type", "invoice_number"))
          .withColumn("sales_tax_line_key", sk(*FACT_GROUP_BY_COLS)))

    df = df.dropDuplicates(["sales_tax_line_key"])         # unique by construction (GROUP BY); defensive
    return df.select("sales_tax_line_key", "document_scope_key", *FACT_BUSINESS_COLS)

# In[5]:


# =============================================================================
# BUILD — batch full load, gated by OVERWRITE. Results are identical to a full OVERWRITE run
#   of the previous streaming version (same transform, same overwrite write). dim_business_unit /
#   dim_sic / dim_state have their own notebooks; dim_address is REUSED (rpt.dim_address_book
#   role views) — none are touched here.
# =============================================================================
if OVERWRITE or not spark.catalog.tableExists(T_FACT):
    spark.sql(f"DROP TABLE IF EXISTS {T_FACT}")
    _write_table(transform_fact(), T_FACT)
    print(f"✓ built {T_FACT}")
else:
    print(f"skip — {T_FACT} exists and OVERWRITE=False")

