#!/usr/bin/env python
# coding: utf-8

# ## nb_eso1_gold_fact_sales_commission
#
# **Gold `fact_sales_commission` processor** for Extended Sales Order 1 — the SOP0027
# Commission report (Family 10). This is the ONE report the order-line freight fact
# (`fact_sales_order_freight`) cannot serve: its measures come from the JDE Sales
# Commission ledger **F42005**, whose grain (one row per commission record —
# `SCKCOO+SCDCTO+SCDOCO+SCLNID+SCCMLN`, i.e. sales line × salesperson × commission
# rule) is **finer than the sales line**. Splitting it by grain is the correct
# dimensional call (see ESO1_Query_to_Notebook_Gap_Analysis.docx §Q3 / Part 4 OUT-OF-SCOPE).
#
# Builds and continuously refreshes ONE table — `lh_jde_gold.rpt.fact_sales_commission`
# (commission-line grain; sales-line context denormalized) — from the Silver commission
# (F42005) and sales-order-detail (F4211) Change Data Feed streams. Same conventions as
# nb_eso1_gold_fact_sales_order_freight (own table, own checkpoint root, own OVERWRITE,
# no report filters — all filtering is Power BI page-level).
#
# Flow: constants → transforms → preflight → seed-if-missing → start CDF streams
#       (foreachBatch delete-scope+append) → refresh every 30 seconds.
#
# Streaming model mirrors nb_silver_to_gold_eso7_v2 / the ESO1 freight fact:
#   • continuous trigger(processingTime="30 seconds") + awaitAnyTermination
#   • per-source foreachBatch handler factories make_*_handler(init_ver): skip the seed
#     rows (_commit_version <= init_ver), act only on real change rows, CDC-write to Gold
#     (delete the affected order scope, then APPEND freshly recomputed commission lines)
# Design: docs/ESO1_gold_layer_design.md · Query: eso1/Filter Capture/SOP0027 - Commission.sql


# In[1]:


# =============================================================================
# CONFIG + CONSTANTS  (names per Fabric_Naming_Convention_Guidelines.pdf)
# =============================================================================
import threading
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

GOLD_SCHEMA = "lh_jde_gold.rpt"
RPT_SCHEMA  = "lh_jde_gold.rpt"

# ── refresh / runtime config (CDF concept adopted from the ESO1 freight fact) ──
ENV       = "dev"
TRIGGER   = {"processingTime": "30 seconds"}          # continuous; refresh every 30 s
CKPT      = f"Files/checkpoints/eso1_commission_{ENV}"  # OWN root — independent of the freight-fact notebook
# ── manual reprocess switch ───────────────────────────────────────────────────
#   OVERWRITE = True  -> full load: drop + rebuild from the full Silver snapshot,
#                        snapshot each source's Delta version as init_ver, clear checkpoints.
#   OVERWRITE = False -> resume: keep table + checkpoints, streams catch up (init_ver = -1).
OVERWRITE = True    # ⚠ INITIAL BUILD — set back to False after the first successful run

# Serialises all fact writes: the F42005 and F4211 foreachBatch handlers run in separate
# driver threads, so this lock makes them take turns (delete+append never overlaps).
_FACT_LOCK = threading.Lock()

# ── Silver sources ────────────────────────────────────────────────────────────
SRC_SCHEMA    = "jde_cdc"             # aligned to jde_cdc (2026-07-22, was `cdf`) — same as ESO4/ESO5; CDF required on every STREAMED source
SRC_LAKEHOUSE = "lh_jde_silver"
F42005_TBL = "f42005_sales_commission_file"       # DRIVER — commission ledger (confirmed in full_metadata.json)
F4211_TBL  = "f4211_sales_order_detail_file"      # sales-line context (amounts, item, dates, plant, ship-to)
F4201_TBL  = "f4201_sales_order_header_file"      # header — sold-to (SHAN8)
F0101_TBL  = "f0101_address_book_master"          # sold-to category (ABAC10)
# F42119 (Sales Order History) — OPTIONAL context: SOP0027 is a completed-order report (next=999), so a
# commission's sales line may already be purged to history. Unioned into the line-context lookup when present
# (guarded); NOT streamed here (F42005 changes drive the recompute — see §handlers).
F42119_TBL = "f42119_sales_order_history_file"

# ── Gold target BUILT here ─────────────────────────────────────────────────────
T_FACT = f"{GOLD_SCHEMA}.fact_sales_commission"

# ── REUSED dims (read-only; owned by old_nb jobs) ─────────────────────────────
R_DIM_AB      = f"{RPT_SCHEMA}.dim_address_book"
R_DIM_PLANT   = f"{RPT_SCHEMA}.dim_plant"
R_DIM_SHIP_TO = f"{RPT_SCHEMA}.dim_address_ship_to"
R_DIM_SOLD_TO = f"{RPT_SCHEMA}.dim_address_sold_to"
# salesperson (SCSLSP) is an address-book number — relate it to dim_address_book (a salesperson role view can be
# added later, mirroring ship_to/sold_to/carrier). No new dim is built here.

# ── report scaling (business WHERE filters removed — fact carries ALL commission rows) ──
# Hubble scaled SDAEXP/SDECST by NVL(company.ShiftFactor, 0.01) and the commission amounts (SCTOTL/SCLRCS/SCCOMA)
# by 0.01 and SCCPCT by /1000 — all implied-decimal decoding that Silver has ALREADY applied. So NO scaling here.
SHIFT_FACTOR = 1.0   # lineage placeholder on the line amounts — see design §11 (ShiftFactor open item / H1)

print(f"ESO1 Gold commission-fact processor — trigger {TRIGGER}  target {T_FACT}")


# In[2]:


# =============================================================================
# HELPERS  (== ESO1 freight fact)
# =============================================================================
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def sname(table_name):
    return f"{SRC_LAKEHOUSE}.{SRC_SCHEMA}.{table_name}"

def load_silver_table(table_name):
    df = spark.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)          # CDC soft-delete, NOT a report filter
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

def _load_optional(table_name):
    """Load a Silver table only if it exists; else warn and return None (used for F42119 history context)."""
    try:
        if spark.catalog.tableExists(sname(table_name)):
            return load_silver_table(table_name)
    except Exception as e:
        print(f"  ⚠ optional source check failed for {sname(table_name)}: {e}")
    print(f"  ⚠ optional source not found, skipping union: {sname(table_name)}")
    return None

# JDE carries junk/sentinel dates (1952 zero-dates, 2824 corrupt Julians). With no date
# dimension, null anything outside a plausible business window so raw-date slicers stay clean.
VALID_DATE_LO     = "2000-01-01"   # fixed lower bound
VALID_YEARS_AHEAD = 25             # upper bound = Dec 31 of (current year + 25); self-extends each run
_RAW_DATE_COLS = ["commission_paid_date", "gl_date", "actual_ship_date"]

def clean_date(col):
    """Null out implausible sentinel/corrupt dates (outside the valid business window:
    VALID_DATE_LO .. Dec 31 of current year + VALID_YEARS_AHEAD — the upper bound
    self-extends each run so future promised/delivery dates are never clipped)."""
    hi = F.make_date(F.year(F.current_date()) + F.lit(VALID_YEARS_AHEAD), F.lit(12), F.lit(31))
    return F.when(col.between(F.lit(VALID_DATE_LO).cast("date"), hi), col)

def date_key(col):
    return F.when(col.isNotNull(), F.date_format(col, "yyyyMMdd").cast("int"))

def sk(*cols):
    return F.sha2(F.concat_ws("||", *[F.col(c).cast("string") if isinstance(c, str) else c.cast("string")
                                       for c in cols]), 256)

def current_version(silver_table):
    return spark.sql(f"DESCRIBE HISTORY {sname(silver_table)}").select(F.max("version")).first()[0]

def _exists(fqn):
    try:
        if spark.catalog.tableExists(fqn):
            return True
    except Exception:
        pass
    try:
        spark.read.table(fqn).limit(1).collect()
        return True
    except Exception:
        return False


# In[3]:


# =============================================================================
# CDC WRITE HELPER — NO audit columns (== ESO1 freight fact)
#   fact = delete the affected order scope, then APPEND freshly recomputed commission
#   lines (handles insert/update/delete uniformly); writes serialised by _FACT_LOCK.
# =============================================================================
def _write_new_table(df, target, cdf=True):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if cdf:
        w = w.option("delta.enableChangeDataFeed", "true")
    w.saveAsTable(target)

def recompute_fact(orders):
    """CDC: delete the affected order scope, then append the recomputed commission lines.
    `orders` = distinct company_key_order_no / order_type / order_number. Returns rows written."""
    if orders.rdd.isEmpty():
        return 0
    src = transform_fact(restrict_orders=orders)
    with _FACT_LOCK:
        if not spark.catalog.tableExists(T_FACT):
            _write_new_table(src, T_FACT)
            return src.count()
        scope = orders.select(sk("company_key_order_no", "order_type", "order_number")
                              .alias("order_scope_key")).distinct()
        (DeltaTable.forName(spark, T_FACT).alias("t")
            .merge(scope.alias("s"), "t.order_scope_key = s.order_scope_key")
            .whenMatchedDelete().execute())                 # drop the order's old commission lines
        src.write.format("delta").mode("append").saveAsTable(T_FACT)   # append current
    return src.count()


# In[4]:


# =============================================================================
# FACT  fact_sales_commission  (commission-line grain; sales-line context denormalized)
# =============================================================================
FACT_BUSINESS_COLS = [
    # ── commission identity / degenerate (grain = one row per F42005 commission record) ──
    "company", "company_key_order_no", "order_type", "order_number", "line_number",
    "commission_line_number", "salesperson", "commission_code_type",
    # ── FK / dimension columns ──
    "ship_to", "sold_to", "branch_plant", "item_number_short",
    # ── date keys (retained but unused — no date dimension; slice raw dates) ──
    "commission_paid_date_key", "gl_date_key", "ship_date_key",
    # ── raw event dates ──
    "commission_paid_date", "gl_date", "actual_ship_date",
    # ── commission measures (F42005; Silver pre-decoded — no ×0.01 / ÷1000) ──
    "percent_commission", "amount_commission", "amount_related_commission",
    "percent_related_commission", "flat_commission_amount", "amount_per_unit",
    "amount_sales_total_line", "amount_sales_line_total_cost",
    "amount_line_gross_margin", "amount_line_eligible_margin",
    # ── sales-line context (F4211; denormalized per commission record — DAX-dedup by sales line) ──
    "extended_price", "extended_cost", "quantity_shipped", "primary_quantity_ordered",
    "invoice_number", "second_item_number", "line_type",
    "uom_primary", "uom_pricing", "sales_reporting_code_05",
    # ── status filter attributes (page-level; SOP0027 WHERE next='999' / last<>'980') ──
    "status_code_next", "status_code_last",
    # ── filter / display attributes ──
    "category_code_10", "sold_to_search_type",
    # ── flags / lineage ──
    "is_primary_commission_line", "shift_factor_applied",
]

def _line_context():
    """One row per sales line (KCOO+DCTO+DOCO+LNID) with the F4211 columns the commission fact denormalizes.
    F4211 ∪ F42119 (optional) so a commission whose line has been purged to history still gets its context."""
    keep = ["company_key_order_no", "order_type", "document_order_invoice_e", "line_number",
            "company", "cost_center", "address_number_ship_to", "identifier_short_item",
            "identifier_second_item", "line_type", "uom_primary", "uom_pricing",
            "sales_reporting_code_05", "doc_voucher_invoice_e",
            "status_code_next", "status_code_last",
            "amount_extended_price", "amount_extended_cost",
            "units_quantity_shipped", "units_primary_qty_order",
            "actual_ship_date", "dt_for_gl_and_vouch_01"]
    ln = load_silver_table(F4211_TBL)
    hist = _load_optional(F42119_TBL)
    if hist is not None:
        # F42119 snake-names SDLITM as `identifier_2nd_item` (≠ F4211 `identifier_second_item`) — rename
        # before the union so second_item_number isn't NULLed for lines already purged to history.
        if "identifier_2nd_item" in hist.columns and "identifier_second_item" not in hist.columns:
            hist = hist.withColumnRenamed("identifier_2nd_item", "identifier_second_item")
        ln = ln.unionByName(hist, allowMissingColumns=True)
    ln = ln.select(*[c for c in keep if c in ln.columns])
    return ln.dropDuplicates(["company_key_order_no", "order_type", "document_order_invoice_e", "line_number"])

def transform_fact(restrict_orders=None):
    sc  = load_silver_table(F42005_TBL)      # DRIVER — commission ledger
    ln  = _line_context()                    # F4211 (∪ F42119) sales-line context
    sh  = load_silver_table(F4201_TBL)       # header — sold-to (SHAN8)
    cc  = load_silver_table(F0101_TBL)       # sold-to category (ABAC10) — ABAT1 gate RELAXED (no filter)

    # F42005 key snake-names: SCKCOO=company_key_order_no, SCDCTO=order_type,
    # SCDOCO=document_order_invoice_e, SCLNID=line_number, SCCMLN=commission_line_number.
    # ⚠ SCKCOO: Silver normalizes it to `company_key_order_no` (same as F4211 SDKCOO / F4201 SHKCOO),
    #   NOT the `order_company_order_number` that full_metadata.json lists — runtime table wins. Do NOT
    #   "correct" this back to the metadata name; it will fail with UNRESOLVED_COLUMN.
    # (alias AFTER the semi-join — left_semi does not carry an alias through, == ESO1 freight fact.)
    if restrict_orders is not None:
        sc = sc.join(restrict_orders.alias("ro"),
                     (sc["company_key_order_no"] == F.col("ro.company_key_order_no")) &
                     (sc["order_type"]                == F.col("ro.order_type")) &
                     (sc["document_order_invoice_e"]  == F.col("ro.order_number")), "left_semi")

    j = (sc.alias("sc")
         .join(ln.alias("ln"),
               (F.col("sc.company_key_order_no") == F.col("ln.company_key_order_no")) &
               (F.col("sc.order_type")                == F.col("ln.order_type")) &
               (F.col("sc.document_order_invoice_e")  == F.col("ln.document_order_invoice_e")) &
               (F.col("sc.line_number")               == F.col("ln.line_number")), "left")
         .join(sh.alias("sh"),
               (F.col("sc.company_key_order_no") == F.col("sh.company_key_order_no")) &
               (F.col("sc.document_order_invoice_e")  == F.col("sh.document_order_invoice_e")) &
               (F.col("sc.order_type")                == F.col("sh.order_type")), "left")
         .join(cc.alias("cc"), F.col("cc.address_number") == F.col("sh.address_number"), "left"))

    sel = j.select(
        # ── commission identity / degenerate (the 5 GRAIN keys come from the F42005 DRIVER — never null;
        #    `company` (SDCO) comes from the line context, so it is null when the sales line is absent) ──
        F.col("ln.company").alias("company"),                                        # SDCO (line context; F42005 has no company field)
        F.col("sc.company_key_order_no").alias("company_key_order_no"),        # SCKCOO
        F.col("sc.order_type").alias("order_type"),                                  # SCDCTO
        F.col("sc.document_order_invoice_e").alias("order_number"),                  # SCDOCO
        F.col("sc.line_number").alias("line_number"),                                # SCLNID
        F.col("sc.commission_line_number").alias("commission_line_number"),          # SCCMLN
        F.col("sc.salesperson").alias("salesperson"),                                # SCSLSP → FK dim_address_book
        F.col("sc.commission_code_type").alias("commission_code_type"),              # SCCCTY
        # ── FK / dimension columns ──
        F.col("ln.address_number_ship_to").alias("ship_to"),                         # SDSHAN → dim_address_ship_to
        F.col("sh.address_number").alias("sold_to"),                                 # SHAN8  → dim_address_sold_to
        F.trim(F.coalesce(F.col("ln.cost_center"), F.col("sc.cost_center"))).alias("branch_plant"),  # SDMCU / SCMCU (Silver: cost_center, NOT metadata's business_unit) → dim_plant
        F.coalesce(F.col("ln.identifier_short_item"), F.col("sc.identifier_short_item")).alias("item_number_short"),  # SDITM/SCITM → dim_item
        # ── raw event dates ──
        F.col("sc.date_commission_paid").alias("commission_paid_date"),             # SCCMDJ
        F.col("ln.dt_for_gl_and_vouch_01").alias("gl_date"),                          # SDDGL
        F.col("ln.actual_ship_date").alias("actual_ship_date"),                      # SDADDJ
        # ── commission measures (F42005 — Silver pre-decoded) ──
        F.col("sc.percent_commission").alias("percent_commission"),                  # SCCPCT
        F.col("sc.amount_commission").alias("amount_commission"),                    # SCCOMA (ReportColumn3)
        F.col("sc.amt_related_commission").alias("amount_related_commission"),       # SCCOMR (Silver: amt_related_commission)
        F.col("sc.percent_related_commiss").alias("percent_related_commission"),     # SCCPCR (Silver: percent_related_commiss, 22-char trunc)
        F.col("sc.flat_commission_amount").alias("flat_commission_amount"),          # SCFCA
        F.col("sc.amount_per_unit").alias("amount_per_unit"),                        # SCAPUN
        F.col("sc.amount_sales_total_line").alias("amount_sales_total_line"),        # SCTOTL (ReportColumn1)
        F.col("sc.amount_total_line_cost").alias("amount_sales_line_total_cost"),  # SCLRCS (Silver: amount_total_line_cost) (ReportColumn2)
        F.col("sc.amount_line_gross_margin").alias("amount_line_gross_margin"),      # SCMRGL
        F.col("sc.amt_line_eligible_margin").alias("amount_line_eligible_margin"),# SCELIL (Silver: amt_line_eligible_margin)
        # ── sales-line context (F4211 — repeated per commission record; DAX-dedup by sales line) ──
        F.col("ln.amount_extended_price").alias("extended_price"),                   # SDAEXP (ReportColumn5, ×ShiftFactor→1.0)
        F.col("ln.amount_extended_cost").alias("extended_cost"),                     # SDECST (ReportColumn6)
        F.col("ln.units_quantity_shipped").alias("quantity_shipped"),               # SDSOQS (ReportColumn4)
        F.col("ln.units_primary_qty_order").alias("primary_quantity_ordered"),      # SDPQOR (ReportColumn7)
        F.col("ln.doc_voucher_invoice_e").alias("invoice_number"),                  # SDDOC
        F.col("ln.identifier_second_item").alias("second_item_number"),             # SDLITM
        F.col("ln.line_type").alias("line_type"),                                   # SDLNTY
        F.col("ln.uom_primary").alias("uom_primary"),                               # SDUOM1
        F.col("ln.uom_pricing").alias("uom_pricing"),                               # SDUOM4
        F.col("ln.sales_reporting_code_05").alias("sales_reporting_code_05"),       # SDSRP5
        # ── status filter attributes (page-level; SOP0027 WHERE — carried so PBI reproduces the scope) ──
        F.col("ln.status_code_next").alias("status_code_next"),                     # SDNXTR (page filter: completed order = '999')
        F.col("ln.status_code_last").alias("status_code_last"),                     # SDLTTR (page filter: exclude cancelled '980')
        # ── filter / display attributes ──
        F.col("cc.report_code_add_book_010").alias("category_code_10"),             # ABAC10 (sold-to F0101)
        F.col("cc.address_type_01").alias("sold_to_search_type"),                   # ABAT1 (sold-to) — Hubble INNER-gates
                                                                                    #   the sold-to F0101 on this band; relaxed
                                                                                    #   to LEFT here + carried so PBI can reproduce
                                                                                    #   the exclusion at page level (D3 fidelity)
        # ── lineage ──
        F.lit(SHIFT_FACTOR).cast("double").alias("shift_factor_applied"),
    ).distinct()

    # is_primary_commission_line: 'Y' on ONE commission record per sales line, so the denormalized
    # sales-line amounts (extended_price/cost/qty) dedup in DAX via SUMX(VALUES(sales_order_line),…).
    _cw = (Window.partitionBy("company_key_order_no", "order_type", "order_number", "line_number")
           .orderBy(F.col("commission_line_number").asc_nulls_last()))
    _sel = sel
    for _dc in _RAW_DATE_COLS:                      # null sentinel/junk dates before deriving keys
        if _dc in _sel.columns:
            _sel = _sel.withColumn(_dc, clean_date(F.col(_dc)))
    df = (_sel.withColumn("_crn", F.row_number().over(_cw))
             .withColumn("is_primary_commission_line",
                         F.when(F.col("_crn") == 1, F.lit("Y")).otherwise(F.lit("N")))
             .drop("_crn")
             # role-play date keys (yyyyMMdd). No date dimension — ints retained but unused;
             # dates are sliced via the raw date columns.
             .withColumn("commission_paid_date_key", date_key(F.col("commission_paid_date")))
             .withColumn("gl_date_key",              date_key(F.col("gl_date")))
             .withColumn("ship_date_key",            date_key(F.col("actual_ship_date"))))

    # business + scope keys; one row per commission record (F42005 PK)
    df = (df.withColumn("sales_commission_key",
                        sk("company_key_order_no", "order_type", "order_number",
                           "line_number", "commission_line_number"))
            .withColumn("order_scope_key",
                        sk("company_key_order_no", "order_type", "order_number")))
    df = df.dropDuplicates(["sales_commission_key"])
    return df.select("sales_commission_key", "order_scope_key", *FACT_BUSINESS_COLS)


# In[5]:


# =============================================================================
# PREFLIGHT — reused dims must exist/non-empty; F42005 driver must be present in Silver
# =============================================================================
_errs = []
for tbl in [R_DIM_AB, R_DIM_PLANT]:
    if not _exists(tbl):
        _errs.append(f"MISSING {tbl}"); print(f"  MISSING : {tbl}"); continue
    n = spark.read.table(tbl).count()
    if n == 0:
        _errs.append(f"EMPTY {tbl}")
    print(f"  OK      : {tbl:38s} rows={n:,}")
for v in [R_DIM_SHIP_TO, R_DIM_SOLD_TO]:
    print(f"  {'OK     ' if _exists(v) else 'no-spark'} : {v}  (reused role view)")
if not _exists(sname(F42005_TBL)):
    _errs.append(f"MISSING driver {sname(F42005_TBL)} — F42005 must be ingested to Silver with CDF enabled")
    print(f"  MISSING : {sname(F42005_TBL)}  (commission ledger — the fact driver)")
else:
    print(f"  OK      : {sname(F42005_TBL)}  (commission driver)")
if _errs:
    raise Exception("Commission-fact preflight FAILED: " + "; ".join(_errs)
                    + ". Build reused dims via old_nb (nb_dim_address_book, nb_dim_plant); "
                    + "ensure F42005 is ingested to lh_jde_silver.jde_cdc with delta.enableChangeDataFeed=true.")
print("✓ preflight passed")


# In[6]:


# =============================================================================
# FULL LOAD vs RESUME  (== ESO1 freight fact)
# =============================================================================
_STREAMED   = [F42005_TBL, F4211_TBL]                       # F42119 is optional context, NOT streamed
_CKPT_PATHS = [f"{CKPT}/fact__{t}" for t in _STREAMED]

def _checkpoints_exist():
    """True iff EVERY per-stream checkpoint has a COMMITTED offset (its offsets/ dir is non-empty).
    An incomplete checkpoint is treated as ABSENT → forces a FULL LOAD that re-establishes init_ver at a
    CDF-valid version (avoids a cold-start at startingVersion=0, which predates CDF enablement)."""
    for p in _CKPT_PATHS:
        try:
            if not mssparkutils.fs.ls(f"{p}/offsets"):
                return False
        except Exception:
            return False
    return True

# ── stop leftover streams from a previous run in this Spark session ──
_STREAM_NAMES = {"fact__" + t for t in _STREAMED}
_stopped = []
for _q in list(spark.streams.active):
    if _q.name in _STREAM_NAMES:
        _q.stop(); _stopped.append(_q.name)
if _stopped:
    print(f"Stopped leftover streams: {_stopped}")

_FULL_LOAD = OVERWRITE or not spark.catalog.tableExists(T_FACT) or not _checkpoints_exist()

if _FULL_LOAD:
    print("== FULL LOAD ==")
    spark.sql(f"DROP TABLE IF EXISTS {T_FACT}")
    _write_new_table(transform_fact(), T_FACT)
    print(f"  ✓ seeded {T_FACT}")
    _init_ver = {t: current_version(t) for t in _STREAMED}
    print(f"  init versions: {_init_ver}")
    try:
        mssparkutils.fs.rm(CKPT, True); print("  checkpoints cleared")
    except Exception as e:
        print(f"  checkpoint clear skipped: {e}")
    print("✓ full load complete")
else:
    print("== RESUME from checkpoint ==")
    _init_ver = {}   # .get(src, -1) -> -1 everywhere; checkpoint drives the offset


# In[7]:


# =============================================================================
# STREAM BATCH HANDLERS  (== ESO1 freight fact)
#   Both streams map their changed rows → the affected ORDERS (company_key_order_no /
#   order_type / order_number), then recompute_fact deletes that order scope and appends
#   the freshly recomputed commission lines. A commission change (F42005) or a line-context
#   change (F4211) both recompute the whole order's commission rows.
# =============================================================================
def make_fact_f42005_handler(init_ver):
    def handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        if init_ver >= 0:
            batch_df = batch_df.filter(F.col("_commit_version") > init_ver)
        if batch_df.rdd.isEmpty():
            return
        orders = (batch_df.filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
                  .select(F.col("company_key_order_no").alias("company_key_order_no"),
                          F.col("order_type"),
                          F.col("document_order_invoice_e").alias("order_number")).distinct())
        n = recompute_fact(orders)
        print(f"[{F42005_TBL[:14]}] commission batch={batch_id} rows={n}")
    return handler

def make_fact_f4211_handler(init_ver):
    def handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        if init_ver >= 0:
            batch_df = batch_df.filter(F.col("_commit_version") > init_ver)
        if batch_df.rdd.isEmpty():
            return
        orders = (batch_df.filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
                  .select(F.col("company_key_order_no"), F.col("order_type"),
                          F.col("document_order_invoice_e").alias("order_number")).distinct())
        n = recompute_fact(orders)   # a line-context change recomputes the order's commission rows
        print(f"[{F4211_TBL[:14]}] line batch={batch_id} rows={n}")
    return handler


# In[8]:


# =============================================================================
# START STREAMS — Silver Change Data Feed -> foreachBatch -> CDC write, every 30 s.
#   Each stream starts at init_ver (full load, the seed-time version) and the handler
#   discards _commit_version <= init_ver. Resume (init_ver = -1): the checkpoint's committed
#   offset drives; startingVersion falls back to the source CURRENT version (never 0).
#   REQUIRES delta.enableChangeDataFeed = true on F42005 and F4211.
# =============================================================================
def _start_ver(iv, tbl):
    return iv if iv >= 0 else current_version(tbl)

iv_comm = _init_ver.get(F42005_TBL, -1)
_sv_comm = _start_ver(iv_comm, F42005_TBL)
(spark.readStream.format("delta")
     .option("readChangeFeed",  "true")
     .option("startingVersion", _sv_comm)
     .table(sname(F42005_TBL))
 .writeStream
     .foreachBatch(make_fact_f42005_handler(iv_comm))
     .option("checkpointLocation", f"{CKPT}/fact__{F42005_TBL}")
     .trigger(**TRIGGER)
     .queryName("fact__" + F42005_TBL)
     .start())
print(f"  fact__{F42005_TBL}  startingVersion={_sv_comm}  init_ver={iv_comm}")

iv_line = _init_ver.get(F4211_TBL, -1)
_sv_line = _start_ver(iv_line, F4211_TBL)
(spark.readStream.format("delta")
     .option("readChangeFeed",  "true")
     .option("startingVersion", _sv_line)
     .table(sname(F4211_TBL))
 .writeStream
     .foreachBatch(make_fact_f4211_handler(iv_line))
     .option("checkpointLocation", f"{CKPT}/fact__{F4211_TBL}")
     .trigger(**TRIGGER)
     .queryName("fact__" + F4211_TBL)
     .start())
print(f"  fact__{F4211_TBL}  startingVersion={_sv_line}  init_ver={iv_line}")

print(f"== started {len(_STREAMED)} streams — continuous, trigger {TRIGGER}. Target {T_FACT}. "
      "Reused dims (rpt) refresh via their own jobs. ==")
spark.streams.awaitAnyTermination()
