#!/usr/bin/env python
# coding: utf-8


# ## nb_eso4_gold_fact_sales_tax_reconciliation
#
# **Gold `fact_sales_tax_reconciliation` processor** for Extended Sales Order 4 (Sales Tax
# with Business Stream Summary — Avalara reconciliation). Builds and continuously refreshes
# ONE table — `lh_jde_gold.eso4.fact_sales_tax_reconciliation` (invoice sales-tax-line grain)
# — from the Silver sales-order detail (F4211) and customer-ledger (F03B11) Change Data Feed
# streams, joined to the business-unit (F0006) and address (F0101 ⋈ F0116) static snapshots.
#
# Streaming architecture is identical to ESO1's
# nb/nb_eso1_gold_fact_sales_order_freight.py (which adopts nb_silver_to_gold_eso7_v2):
#   • continuous trigger(processingTime="30 seconds") + awaitAnyTermination
#   • per-source foreachBatch handler factories make_fact_*_handler(init_ver): skip the seed
#     rows (_commit_version <= init_ver), then act only on real change rows (_change_type
#     insert/update_postimage/delete) and CDC-write to Gold (fact = delete the affected
#     invoice-document scope, then APPEND freshly recomputed lines; writes serialised by a lock)
#   • per-stream checkpoint gate (offsets/ committed) + startingVersion=init_ver knob
#
# JOINS — every join from Extended Sales Order 4.docx §4 is applied (NO report filters):
#   F4211 INNER F03B11 : SDDOC=RPODOC, SDDCT=RPODCT, SDKCO=RPOKCO, SDLNID=RPLNID
#   F4211 LEFT  F0006  : SDMCU=MCMCU
#   F4211 LEFT (F0101 INNER F0116) : SDSHAN=ABAN8 ; ABAN8=ALAN8
#   (Hubble-only F03B11→company ShiftFactor join is a constant here — see SHIFT_FACTOR.)
# Design: eso4/docs/ESO4_gold_layer_design.md


# In[1]:


# =============================================================================
# CONFIG + CONSTANTS  (names per Fabric_Naming_Convention_Guidelines.pdf)
# =============================================================================
import threading
from pyspark.sql import functions as F
from delta.tables import DeltaTable

GOLD_SCHEMA = "lh_jde_gold.rpt"

# ── refresh / runtime config (CDF concept adopted from nb_silver_to_gold_eso7_v2) ──
ENV             = "dev"                              # checkpoint namespacing — envs never collide
TRIGGER         = {"processingTime": "30 seconds"}   # ← continuous; refresh every 30 s
CKPT            = f"Files/checkpoints/eso4_fact_{ENV}"  # OWN root — independent of the dim notebooks
# ── manual reprocess switch (== ESO7 v2 MANUAL_OVERWRITE) ─────────────────────
#   OVERWRITE = True  -> full load: drop + rebuild the fact from the full Silver snapshot,
#                        snapshot each streamed source's Delta version as init_ver, clear checkpoints.
#   OVERWRITE = False -> resume: keep the table + checkpoints, streams catch up from where
#                        they left off (init_ver = -1, no version filtering).
OVERWRITE       = True    # ⚠ ONE-OFF full reprocess — set back to False after a healthy run

# Serialises all fact-table writes: foreachBatch handlers run in separate driver threads, so
# this lock makes the F4211 and F03B11 streams take turns (delete+append on the same Gold fact
# never overlaps). (Pattern from ESO7 v2 / ESO1.)
_FACT_LOCK      = threading.Lock()

# ── Silver sources ────────────────────────────────────────────────────────────
SRC_SCHEMA    = "jde_cdc"     # Silver Change Data Feed schema; CDF must be enabled on every
                          #   STREAMED source (F4211 / F03B11) read below.
SRC_LAKEHOUSE = "lh_jde_silver"
F4211_TBL   = "f4211_sales_order_detail_file"      # streamed
F03B11_TBL  = "f03b11_customer_ledger"             # streamed
F0006_TBL   = "f0006_business_unit_master"         # static snapshot (Plant / Business Stream)
F0101_TBL   = "f0101_address_book_master"          # static snapshot (Ship/Sold/Parent, SIC)
F0116_TBL   = "f0116_address_by_date"              # static snapshot (Jurisdiction / County)
F0005_TBL   = "f0005_user_defined_code_values"     # static snapshot (UDC 01/SC → SIC Description)

# ── Gold target BUILT here (new, eso4) ─────────────────────────────────────────
T_FACT      = f"{GOLD_SCHEMA}.fact_sales_tax_reconciliation"

# ── report scaling ─────────────────────────────────────────────────────────────
# Hubble multiplied the four amounts by NVL(company.ShiftFactor, 0.01) to de-scale RAW JDE
# integer amounts (stored ×100). Our Silver is already decoded (implied decimals resolved), so
# the placeholder is 1.0 — same treatment as ESO1's SHIFT_FACTOR. `shift_factor_applied` is
# carried on the fact for lineage. (See design §"ShiftFactor".)
SHIFT_FACTOR    = 1.0

print(f"ESO4 Gold fact processor — trigger {TRIGGER}  target {T_FACT}")

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

def current_version(silver_table):
    """Latest committed Delta version of a Silver source (for init_ver seed-skip)."""
    return spark.sql(f"DESCRIBE HISTORY {sname(silver_table)}").select(F.max("version")).first()[0]

# In[3]:


# =============================================================================
# CDC WRITE HELPER — NO audit columns (CDF concept from nb_silver_to_gold_eso7_v2)
#   Gold table stores business columns only — no record_hash / is_deleted /
#   source_commit_timestamp / gold_updated_timestamp.
#   • fact : delete the affected invoice-document scope, then APPEND freshly recomputed lines
#            (handles insert/update/delete uniformly); writes serialised by _FACT_LOCK.
# =============================================================================
def _write_new_table(df, target, cdf=True):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if cdf:
        w = w.option("delta.enableChangeDataFeed", "true")   # Gold CDF on for downstream
    w.saveAsTable(target)

def recompute_fact(docs):
    """CDC for the fact: delete the affected invoice-document scope then append the recomputed
    lines (== ESO7 v2 / ESO1 recompute_fact). `docs` = distinct
    company_key / document_type / invoice_number. Returns rows written."""
    if docs.rdd.isEmpty():
        return 0
    src = transform_fact(restrict_docs=docs)
    with _FACT_LOCK:
        if not spark.catalog.tableExists(T_FACT):
            _write_new_table(src, T_FACT)
            return src.count()
        scope = docs.select(sk("company_key", "document_type", "invoice_number")
                            .alias("document_scope_key")).distinct()
        (DeltaTable.forName(spark, T_FACT).alias("t")
            .merge(scope.alias("s"), "t.document_scope_key = s.document_scope_key")
            .whenMatchedDelete().execute())               # drop the document's old lines
        src.write.format("delta").mode("append").saveAsTable(T_FACT)   # append current lines
    return src.count()

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
    "business_stream",         # XID_CUSTOM_84c76537583683 (calc)
    "business_stream_code",    # XF0006_MCRP20
    "sic_code",                # XF0101_ABSIC (= XC_F0101_ABSIC)
    # ── address denorm ──
    "jurisdiction",            # XF0116_ALADDS resolved via F0005 00/S → state name ("Colorado")
    "county",                  # XF0116_ALCOUN
    # ── raw event dates ──
    "gl_date",                 # XF4211_SDDGL
    "service_tax_date",        # XF03B11_RPDSVJ
]
# Attributes functionally dependent on the grain (carried through the aggregation, not grouped
# on): plant_name←plant, sic_description←sic_code, plus the constant shift_factor_applied.
FACT_CARRY_COLS = ["plant_name", "sic_description", "shift_factor_applied"]
# SUMmed measures (Hubble ReportColumn1-4).
FACT_MEASURE_COLS = ["taxable_amount", "non_taxable_amount", "tax_amount", "gross_amount"]
# Stored fact columns, in report order.
FACT_BUSINESS_COLS = [
    "document_company", "invoice_number", "document_type", "order_number", "order_type",
    "plant", "ship_to", "sold_to", "parent_number",
    "tax_explanation_code", "tax_area", "avalara_code",
    "business_stream", "business_stream_code", "sic_code", "sic_description",
    "jurisdiction", "county", "plant_name",
    "gl_date", "service_tax_date",
    "taxable_amount", "non_taxable_amount", "tax_amount", "gross_amount", "shift_factor_applied",
]

def transform_fact(restrict_docs=None):
    sd  = load_silver_table(F4211_TBL)       # sales order detail (streamed source)
    ar  = load_silver_table(F03B11_TBL)      # customer ledger    (streamed source)
    bu  = load_silver_table(F0006_TBL)       # business unit master (static)
    ab  = load_silver_table(F0101_TBL)       # address book master  (static)
    adr = load_silver_table(F0116_TBL)       # address by date      (static)
    udc = load_silver_table(F0005_TBL)       # user-defined codes   (static; SIC Description)

    # scope restriction (CDC): keep only the changed invoice documents' F4211 lines
    if restrict_docs is not None:
        sd = sd.join(restrict_docs.alias("rd"),
                     (sd["company_key"] == F.col("rd.company_key")) &
                     (sd["document_type"] == F.col("rd.document_type")) &
                     (sd["doc_voucher_invoice_e"] == F.col("rd.invoice_number")), "left_semi")

    # F0005 UDC lookup (system 01 / type SC) → SIC Description (docx §6 col 25, F0005 DRDL01).
    # F0005 is NOT in the docx §4 join table; the join (DRKY = ABSIC) is inferred from the JDE
    # 01/SC UDC that validates ABSIC — same lookup shape ESO7 uses for 40/AT (design §5, flagged).
    sic_desc = (udc.where((F.trim(F.col("product_code")) == "01") &
                          (F.trim(F.col("user_defined_codes")) == "SC"))
                .select(F.trim(F.col("user_defined_code")).alias("sic_key"),        # DRKY
                        F.trim(F.col("description_001")).alias("sic_description"))   # DRDL01
                .where(F.col("sic_key") != "")
                .dropDuplicates(["sic_key"]))

    # F0005 UDC lookup (system 00 / type S) → State/Province name — the JDE state UDC that
    # validates ALADDS. Resolves the raw 2-char jurisdiction code ("CO") to its description
    # ("Colorado") to match the Hubble jurisdiction display. INFERRED system/type (00/S) — flagged
    # in design §5 like the 01/SC SIC lookup.
    state_desc = (udc.where((F.trim(F.col("product_code")) == "00") &
                            (F.trim(F.col("user_defined_codes")) == "S"))
                  .select(F.trim(F.col("user_defined_code")).alias("state_key"),        # DRKY
                          F.trim(F.col("description_001")).alias("state_description"))   # DRDL01
                  .where(F.col("state_key") != "")
                  .dropDuplicates(["state_key"]))

    # F0101 INNER F0116 (docx §4: ABAN8 = ALAN8) collapsed to one row per address so the
    # left join to F4211 can't fan the grain out; SIC Description resolved off ABSIC via F0005.
    address = (ab.alias("ab")
               .join(adr.alias("ad"), F.col("ab.address_number") == F.col("ad.address_number"), "inner")
               .groupBy(F.col("ab.address_number").alias("addr_key"))
               .agg(F.first(F.col("ab.standard_industry_code"), ignorenulls=True).alias("sic_code"),
                    F.first(F.trim(F.col("ad.state")),           ignorenulls=True).alias("jurisdiction"),  # ALADDS
                    F.first(F.trim(F.col("ad.county_address")),  ignorenulls=True).alias("county")))       # ALCOUN
    address = (address.join(sic_desc.alias("s5"),
                            F.trim(F.col("sic_code")) == F.col("s5.sic_key"), "left")   # ABSIC = DRKY
                      .drop("sic_key"))
    # Resolve jurisdiction code (ALADDS "CO") → description ("Colorado") via F0005 00/S; keep the
    # raw code when the UDC has no match so nothing is lost. Single "Jurisdiction" column (docx §6
    # col 17) now carries the friendly name, matching Hubble.
    address = (address.join(state_desc.alias("s0"),
                            F.col("jurisdiction") == F.col("s0.state_key"), "left")     # ALADDS = DRKY
                      .withColumn("jurisdiction",
                                  F.coalesce(F.col("state_description"), F.col("jurisdiction")))
                      .drop("state_key", "state_description"))

    # F0006 collapsed to one row per business unit (Plant / Plant Name / Business Stream code).
    bunit = (bu.groupBy(F.col("cost_center").alias("bu_key"))
             .agg(F.first(F.col("description_001"),          ignorenulls=True).alias("plant_name"),        # MCDL01
                  F.first(F.trim(F.col("category_code_cost_ct_020")), ignorenulls=True).alias("business_stream_code")))  # MCRP20

    # ── joins (docx §4, every join used; NO filters) ──────────────────────────────
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
        F.col("sd.doc_voucher_invoice_e").alias("invoice_number"),       # SDDOC
        F.col("sd.document_type").alias("document_type"),                # SDDCT
        F.col("sd.document_order_invoice_e").alias("order_number"),      # SDDOCO
        F.col("sd.order_type").alias("order_type"),                      # SDDCTO
        F.col("ar.doc_voucher_invoice_e").alias("ar_document_no"),       # RPDOC  (F03B11 PK)
        F.col("ar.document_type").alias("ar_document_type"),             # RPDCT  (F03B11 PK)
        F.col("ar.company_key").alias("ar_company_key"),                 # RPKCO  (F03B11 PK)
        F.col("ar.document_pay_item").alias("ar_pay_item"),              # RPSFX  (F03B11 PK)
        # ── dimension FKs ──
        F.trim(F.col("sd.cost_center")).alias("plant"),                  # SDMCU -> dim_business_unit
        F.col("sd.address_number").alias("sold_to"),                     # SDAN8  (docx col 19 "Sold To")
        F.col("sd.address_number_ship_to").alias("ship_to"),             # SDSHAN (docx col 20 "Ship To")
        F.col("sd.address_number_parent").alias("parent_number"),        # SDPA8
        # ── tax attributes ──
        F.col("sd.tax_explanation_code_01").alias("tax_explanation_code"),  # SDEXR1
        F.col("ar.tax_area_01").alias("tax_area"),                          # RPTXA1
        avalara_code.alias("avalara_code"),
        # ── classification ──
        business_stream.alias("business_stream"),                        # calc (§7)
        F.col("bu.business_stream_code").alias("business_stream_code"),   # MCRP20 (F0006)
        F.col("ad.sic_code").alias("sic_code"),                          # ABSIC (F0101)
        F.col("ad.sic_description").alias("sic_description"),             # DRDL01 (F0005 UDC 01/SC)
        # ── address / plant denorm ──
        F.col("ad.jurisdiction").alias("jurisdiction"),                  # ALADDS (F0116)
        F.col("ad.county").alias("county"),                              # ALCOUN (F0116)
        F.col("bu.plant_name").alias("plant_name"),                      # MCDL01 (F0006)
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
                F.first("plant_name",      ignorenulls=True).alias("plant_name"),       # dep. on plant
                F.first("sic_description", ignorenulls=True).alias("sic_description"),  # dep. on sic_code
                F.first("shift_factor_applied").alias("shift_factor_applied")))         # constant

    df = (agg
          # delete-scope key (invoice document) + unique key at the GROUP BY grain
          .withColumn("document_scope_key",
                      sk("document_company", "document_type", "invoice_number"))
          .withColumn("sales_tax_line_key", sk(*FACT_GROUP_BY_COLS)))

    df = df.dropDuplicates(["sales_tax_line_key"])         # unique by construction (GROUP BY); defensive
    return df.select("sales_tax_line_key", "document_scope_key", *FACT_BUSINESS_COLS)

# In[5]:


# =============================================================================
# FULL LOAD vs RESUME  (streaming approach from nb_silver_to_gold_eso7_v2 / ESO1)
#   1) Stop any of our streams left alive from a previous run in this session.
#   2) FULL LOAD when OVERWRITE, the fact is missing, or a checkpoint is incomplete: drop +
#      rebuild the fact, snapshot each streamed source's Delta version as init_ver, clear
#      checkpoints. Streams start at init_ver and skip _commit_version <= init_ver (seed rows).
#   3) Otherwise RESUME (init_ver = -1; the committed checkpoint offset drives).
#   dim_business_unit is owned by its own notebook; dim_address is REUSED
#   (rpt.dim_address_book role views) — none are touched here. F0101/F0116/F0005 are still read as
#   static snapshots ONLY to denormalize sic_code / sic_description / jurisdiction / county onto the
#   fact (same pattern ESO1 uses: reuse the rpt address dim for relationships, read F0101 for denorm).
# =============================================================================
_CKPT_PATHS = [f"{CKPT}/fact__{F4211_TBL}", f"{CKPT}/fact__{F03B11_TBL}"]

def _checkpoints_exist():
    """True iff EVERY per-stream checkpoint has a COMMITTED offset (its offsets/ dir is
    non-empty). A checkpoint dir can hold metadata/ + sources/ yet never have committed a
    batch; requiring offsets/ treats such an INCOMPLETE checkpoint as ABSENT and forces a
    FULL LOAD (re-establishing init_ver) rather than cold-starting the CDF reader at
    startingVersion=0 (version 0 predates CDF enablement -> DELTA_MISSING_CHANGE_DATA)."""
    for p in _CKPT_PATHS:
        try:
            if not mssparkutils.fs.ls(f"{p}/offsets"):
                return False
        except Exception:
            return False
    return True

# ── (1) stop leftover streams from a previous run in this Spark session ──────────
_STREAM_NAMES = {"fact__" + F4211_TBL, "fact__" + F03B11_TBL}
_stopped = []
for _q in list(spark.streams.active):
    if _q.name in _STREAM_NAMES:
        _q.stop()
        _stopped.append(_q.name)
if _stopped:
    print(f"Stopped leftover streams: {_stopped}")

# ── (2/3) full-load gate — also full-load when the checkpoints are missing/incomplete ──
_FULL_LOAD = OVERWRITE or not spark.catalog.tableExists(T_FACT) or not _checkpoints_exist()

if _FULL_LOAD:
    print("== FULL LOAD ==")
    spark.sql(f"DROP TABLE IF EXISTS {T_FACT}")
    _write_new_table(transform_fact(), T_FACT)
    print(f"  ✓ seeded {T_FACT}")
    _init_ver = {t: current_version(t) for t in (F4211_TBL, F03B11_TBL)}
    print(f"  init versions: {_init_ver}")
    try:
        mssparkutils.fs.rm(CKPT, True)
        print("  checkpoints cleared")
    except Exception as e:
        print(f"  checkpoint clear skipped: {e}")
    print("✓ full load complete")
else:
    print("== RESUME from checkpoint ==")
    _init_ver = {}   # .get(src, -1) -> -1 everywhere; no version filtering, checkpoint drives

# In[6]:


# =============================================================================
# STREAM BATCH HANDLERS  (structure from nb_silver_to_gold_eso7_v2 / ESO1)
#   init_ver = the source's Delta version at full-load time. Rows with
#   _commit_version <= init_ver are seed data (already in Gold), so the handler skips them.
#   On resume (init_ver = -1) nothing is filtered. Each handler maps its change rows to the
#   affected invoice-document scope (company_key / document_type / invoice_number) and
#   recompute_fact()s that scope (delete + append).
# =============================================================================
def make_fact_f4211_handler(init_ver):
    def handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        if init_ver >= 0:
            batch_df = batch_df.filter(F.col("_commit_version") > init_ver)
        if batch_df.rdd.isEmpty():
            return
        docs = (batch_df.filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
                .select(F.col("company_key"), F.col("document_type"),
                        F.col("doc_voucher_invoice_e").alias("invoice_number")).distinct())
        n = recompute_fact(docs)   # delete document scope + append recomputed lines (self-locks)
        print(f"[{F4211_TBL[:12]}] fact batch={batch_id} rows={n}")
    return handler

def make_fact_f03b11_handler(init_ver):
    def handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        if init_ver >= 0:
            batch_df = batch_df.filter(F.col("_commit_version") > init_ver)
        if batch_df.rdd.isEmpty():
            return
        # F03B11 change rows point back to the F4211 invoice via the ORIGINAL-document keys
        # (RPOKCO/RPODCT/RPODOC == SDKCO/SDDCT/SDDOC) — recompute those documents' lines.
        docs = (batch_df.filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
                .select(F.col("company_key_original").alias("company_key"),
                        F.col("original_document_type").alias("document_type"),
                        F.col("original_document_no").alias("invoice_number"))
                .where(F.col("invoice_number").isNotNull()).distinct())
        n = recompute_fact(docs)
        print(f"[{F03B11_TBL[:12]}] fact batch={batch_id} rows={n}")
    return handler

# In[7]:


# =============================================================================
# START STREAMS — Silver Change Data Feed -> foreachBatch -> CDC write, every 30 s.
# Each stream starts at init_ver (full load) — the seed-time version, which EXISTS and (with
# CDF enabled) carries change data; the handler discards that seed version via
# _commit_version <= init_ver. On resume (init_ver = -1) the committed offset drives, and
# startingVersion falls back to the source's CURRENT version (never 0 — v0 predates CDF).
# REQUIRES delta.enableChangeDataFeed = true on every streamed source (F4211 / F03B11).
# =============================================================================
def _start_ver(iv, tbl):
    """Full load: init_ver (exists, carries CDF; handler skips <= it). Resume (iv < 0):
    fall back to the source's CURRENT version, never 0 (v0 predates CDF enablement and would
    raise DELTA_MISSING_CHANGE_DATA); on a genuine resume the committed offset drives anyway."""
    return iv if iv >= 0 else current_version(tbl)

iv_fact = _init_ver.get(F4211_TBL, -1)
_sv_fact = _start_ver(iv_fact, F4211_TBL)
(spark.readStream.format("delta")
     .option("readChangeFeed",  "true")
     .option("startingVersion", _sv_fact)
     .table(sname(F4211_TBL))
 .writeStream
     .foreachBatch(make_fact_f4211_handler(iv_fact))
     .option("checkpointLocation", f"{CKPT}/fact__{F4211_TBL}")
     .trigger(**TRIGGER)
     .queryName("fact__" + F4211_TBL)
     .start())
print(f"  fact__{F4211_TBL}  startingVersion={_sv_fact}  init_ver={iv_fact}")

iv_ar = _init_ver.get(F03B11_TBL, -1)
_sv_ar = _start_ver(iv_ar, F03B11_TBL)
(spark.readStream.format("delta")
     .option("readChangeFeed",  "true")
     .option("startingVersion", _sv_ar)
     .table(sname(F03B11_TBL))
 .writeStream
     .foreachBatch(make_fact_f03b11_handler(iv_ar))
     .option("checkpointLocation", f"{CKPT}/fact__{F03B11_TBL}")
     .trigger(**TRIGGER)
     .queryName("fact__" + F03B11_TBL)
     .start())
print(f"  fact__{F03B11_TBL}  startingVersion={_sv_ar}  init_ver={iv_ar}")

print(f"== started 2 streams — continuous, trigger {TRIGGER}. Target {T_FACT}. "
      "dim_business_unit refreshes via its own job; dim_address reused (rpt). ==")
spark.streams.awaitAnyTermination()

