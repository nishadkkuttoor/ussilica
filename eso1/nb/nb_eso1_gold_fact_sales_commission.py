#!/usr/bin/env python
# coding: utf-8

# ## nb_eso1_gold_fact_sales_commission
#
# **Gold `fact_sales_commission` processor** for Extended Sales Order 1 — the SOP0027
# Commission report (Family 10). This is the ONE report the order-line freight fact
# (`fact_sales_order_freight`) cannot serve: its commission measures come from the JDE
# Sales Commission ledger **F42005**.
#
# JOIN MODEL matches SOP0027 - Commission.sql exactly (2026-07-24): **F4211 is the DRIVER**,
# F42005 is **LEFT-joined**. So the grain is one row per **sales line × commission record** —
# a line with no commission appears once with NULL commission columns (like Hubble); a line
# with N commission records fans to N rows (the F4211 line metrics dedup in DAX via
# is_primary_commission_line). This replaced an earlier F42005-driven build, which silently
# dropped non-commissioned sales lines that Hubble shows.
#
# Builds ONE table — `lh_jde_gold.rpt.fact_sales_commission` — from the Silver
# sales-order-detail (F4211, ∪ F42119 history) and commission (F42005) sources. Same
# conventions as nb_eso1_gold_fact_sales_order_freight (own table, own OVERWRITE switch,
# **no report filters — all filtering is Power BI page-level**; the query's INNER
# F4201/F0101 + ABAT1 band become page filters: sold_to IS NOT NULL, sold_to_search_type
# in A–P / R–ZZZ).
#
# ── BUILD (BATCH, architecture like ESO4 / ESO5 / the ESO1 freight fact) ─────────────────
#   • read the full Silver snapshot of every source, run build_fact() ONCE, overwrite the fact.
#   • MANUAL_OVERWRITE = True → drop + rebuild; False → build only if the fact is missing (re-run to refresh).
#
# Sections:  1) CONFIG   2) FACT BUILDER   3) FACT SOURCES   4) RUN
# Design: docs/ESO1_gold_layer_design.md · Query: eso1/Filter Capture/SOP0027 - Commission.sql


# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------

# CONFIG + CONSTANTS  (names per Fabric_Naming_Convention_Guidelines.pdf)
import json, time
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"          # Silver schema — static batch snapshots, same as ESO4/ESO5
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

# ── refresh / runtime config (BATCH build, like ESO4 / ESO5 / the ESO1 freight fact) ──
#   MANUAL_OVERWRITE = True  -> full load: drop + rebuild from the full Silver snapshot.
#   MANUAL_OVERWRITE = False -> build only if the fact is missing (re-run to refresh).
MANUAL_OVERWRITE = True    # ⚠ set back to False after the first successful run

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

# ── Silver sources ────────────────────────────────────────────────────────────
F42005 = "f42005_sales_commission_file"       # DRIVER — commission ledger (confirmed in full_metadata.json)
F4211  = "f4211_sales_order_detail_file"      # sales-line context (amounts, item, dates, plant, ship-to)
F4201  = "f4201_sales_order_header_file"      # header — sold-to (SHAN8)
F0101  = "f0101_address_book_master"          # sold-to category (ABAC10)
# ABAC10 (category_code_10) description is resolved by a SEPARATE reused dim — dim_category_code_10
# (UDC 01/10, built by nb_eso1_gold_dim_category_code_10.py). The fact carries only the FK code.
# F42119 (Sales Order History) — OPTIONAL context: SOP0027 is a completed-order report (next=999), so a
# commission's sales line may already be purged to history. Unioned into the line-context lookup when present
# (guarded via _load_optional); the fact is F4211-only if F42119 is absent.
F42119 = "f42119_sales_order_history_file"

# ── Gold target BUILT here ─────────────────────────────────────────────────────
FACT = "fact_sales_commission"

# REUSED rpt address/plant dims are read-only (owned by old_nb jobs); salesperson (SCSLSP) is an
# address-book number related to dim_address_book in the model. No reused dim is built or checked here.

# ── report scaling (business WHERE filters removed — fact carries ALL commission rows) ──
# Hubble scaled SDAEXP/SDECST by NVL(company.ShiftFactor, 0.01) and the commission amounts (SCTOTL/SCLRCS/SCCOMA)
# by 0.01 and SCCPCT by /1000 — all implied-decimal decoding that Silver has ALREADY applied. So NO scaling here.
SHIFT_FACTOR = 1.0   # lineage placeholder on the line amounts — see design §11 (ShiftFactor open item / H1)

print(f"ESO1 Gold commission-fact processor (batch build) — target {gname(FACT)}")


# HELPERS  (== ESO1 freight fact)
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def load_silver_table(table_name):
    df = spark.read.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)          # CDC soft-delete, NOT a report filter
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

def _load_optional(table_name):
    """Load a Silver table only if it exists; else warn and return None (used for F42119 history context)."""
    try:
        if spark.catalog.tableExists(sname(table_name)):
            return load_silver_table(table_name)
    except Exception as e:
        print("  ⚠ optional source check failed for {}: {}".format(sname(table_name), e))
    print("  ⚠ optional source not found, skipping union: {}".format(sname(table_name)))
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


# ----------------------------------------------------------------------------
# 2) FACT BUILDER
# ----------------------------------------------------------------------------


# FACT  fact_sales_commission  (F4211-DRIVEN: one row per sales line × commission record; commission
#   columns LEFT-joined and NULL when a line earns no commission. Matches SOP0027 - Commission.sql,
#   which drives F4211 and LEFT-joins F42005 — so non-commissioned lines appear, like Hubble.)
FACT_BUSINESS_COLS = [
    # ── grain / degenerate (grain = sales line × commission record; commission_* NULL when none) ──
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
    ln = load_silver_table(F4211)
    hist = _load_optional(F42119)
    if hist is not None:
        # F42119 snake-names SDLITM as `identifier_2nd_item` (≠ F4211 `identifier_second_item`) — rename
        # before the union so second_item_number isn't NULLed for lines already purged to history.
        if "identifier_2nd_item" in hist.columns and "identifier_second_item" not in hist.columns:
            hist = hist.withColumnRenamed("identifier_2nd_item", "identifier_second_item")
        ln = ln.unionByName(hist, allowMissingColumns=True)
    ln = ln.select(*[c for c in keep if c in ln.columns])
    return ln.dropDuplicates(["company_key_order_no", "order_type", "document_order_invoice_e", "line_number"])

def build_fact():
    ln  = _line_context()                    # DRIVER — F4211 (∪ F42119): ALL sales lines (matches the query's F4211 driver)
    sc  = load_silver_table(F42005)      # commission ledger — LEFT-joined (a sales line has 0..N commission records)
    sh  = load_silver_table(F4201)       # header — sold-to (SHAN8)
    cc  = load_silver_table(F0101)       # sold-to category (ABAC10) — ABAT1 gate RELAXED (no filter)

    # JOIN MODEL matches SOP0027 - Commission.sql: F4211 is the DRIVER, F42005 is LEFT-joined.
    #   • a sales line with NO commission → one row, commission_* columns NULL (Hubble shows these too);
    #   • a sales line with N commission records → N rows (fan-out; the F4211 line metrics dedup in DAX
    #     via is_primary_commission_line). All joins are LEFT so Gold stays filter-free (the query's INNER
    #     F4201/F0101 + ABAT1 band are reproduced as Power BI page filters: sold_to IS NOT NULL, and
    #     sold_to_search_type in A–P / R–ZZZ).
    # F42005 key snake-names: SCKCOO=company_key_order_no, SCDCTO=order_type,
    # SCDOCO=document_order_invoice_e, SCLNID=line_number, SCCMLN=commission_line_number.
    # ⚠ SCKCOO / SDKCOO: Silver normalizes both to `company_key_order_no` (NOT the metadata's
    #   `order_company_order_number`) — runtime table wins; do NOT "correct" it (UNRESOLVED_COLUMN).

    j = (ln.alias("ln")
         .join(sc.alias("sc"),
               (F.col("ln.company_key_order_no") == F.col("sc.company_key_order_no")) &
               (F.col("ln.order_type")                == F.col("sc.order_type")) &
               (F.col("ln.document_order_invoice_e")  == F.col("sc.document_order_invoice_e")) &
               (F.col("ln.line_number")               == F.col("sc.line_number")), "left")
         .join(sh.alias("sh"),
               (F.col("ln.company_key_order_no") == F.col("sh.company_key_order_no")) &
               (F.col("ln.document_order_invoice_e")  == F.col("sh.document_order_invoice_e")) &
               (F.col("ln.order_type")                == F.col("sh.order_type")), "left")
         .join(cc.alias("cc"), F.col("cc.address_number") == F.col("sh.address_number"), "left"))

    sel = j.select(
        # ── grain / degenerate (grain = sales line × commission record; the 4 line keys come from the
        #    F4211 DRIVER — never null; commission_line_number is from the LEFT F42005 — NULL when the
        #    line earns no commission; `company` (SDCO) is also from the driver, so always present) ──
        F.col("ln.company").alias("company"),                                        # SDCO (F4211 driver — always present)
        F.col("ln.company_key_order_no").alias("company_key_order_no"),               # SDKCOO (grain key from the driver)
        F.col("ln.order_type").alias("order_type"),                                  # SDDCTO
        F.col("ln.document_order_invoice_e").alias("order_number"),                  # SDDOCO
        F.col("ln.line_number").alias("line_number"),                                # SDLNID
        F.col("sc.commission_line_number").alias("commission_line_number"),          # SCCMLN (LEFT — null when no commission)
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
        F.col("cc.report_code_add_book_010").alias("category_code_10"),             # ABAC10 (sold-to F0101) → FK dim_category_code_10
        F.col("cc.address_type_01").alias("sold_to_search_type"),                   # ABAT1 (sold-to) — Hubble INNER-gates
                                                                                    #   the sold-to F0101 on this band; relaxed
                                                                                    #   to LEFT here + carried so PBI can reproduce
                                                                                    #   the exclusion at page level (D3 fidelity)
        # ── lineage ──
        F.lit(SHIFT_FACTOR).cast("double").alias("shift_factor_applied"),
    ).distinct()

    # is_primary_commission_line: 'Y' on exactly ONE row per sales line (min commission_line_number; the
    # single row when the line has no commission — asc_nulls_last makes the null-CMLN row primary too).
    # The driver's sales-line amounts (extended_price/cost/qty) repeat across a line's commission rows, so
    # they dedup in DAX via CALCULATE(SUM(...), is_primary_commission_line="Y").
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

    # business + scope keys; one row per sales-line × commission record. commission_line_number is NULL
    # for a no-commission line, so coalesce it to a sentinel to keep the key deterministic + unique
    # (a line has either N commission rows with distinct CMLN, or exactly one no-commission row).
    df = (df.withColumn("sales_commission_key",
                        sk("company_key_order_no", "order_type", "order_number", "line_number",
                           F.coalesce(F.col("commission_line_number").cast("string"), F.lit("__NOCOMM__"))))
            .withColumn("order_scope_key",
                        sk("company_key_order_no", "order_type", "order_number")))
    df = df.dropDuplicates(["sales_commission_key"])
    return df.select("sales_commission_key", "order_scope_key", *FACT_BUSINESS_COLS)


# ----------------------------------------------------------------------------
# 3) FACT SOURCES
# ----------------------------------------------------------------------------

# Declares each Silver source and how it relates to the F4211 driver (mirrors the ESO4 / ESO5 / freight-fact
# FACT_SOURCES structure). The joins are applied inside build_fact() / _line_context(), so join_pairs are []
# here; the list documents the source inventory and drives the RUN source preflight.  join: spine=F4211 driver
# (grain), left=F42005 commission ledger (0..N per line), union=F42119 history (optional line-context),
# static=lookup. The REUSED rpt address/plant dims are read-only (owned by old_nb jobs) and
# dim_category_code_10 is owned by nb_eso1_gold_dim_category_code_10 — not built or checked here.
FACT_SOURCES = [
    {"silver": F4211,  "join": "spine",  "join_pairs": []},                    # sales-order detail — the fact DRIVER (grain)
    {"silver": F42005, "join": "left",   "join_pairs": []},                    # commission ledger — LEFT-joined (0..N commission records per line)
    {"silver": F42119, "join": "union",  "join_pairs": [], "optional": True},  # Sales Order History — line-context union via _load_optional if present
    {"silver": F4201,  "join": "static", "join_pairs": []},                    # order header — sold-to (SHAN8)
    {"silver": F0101,  "join": "static", "join_pairs": []},                    # sold-to F0101 — category_code_10 (ABAC10) + search_type (ABAT1)
]


# ----------------------------------------------------------------------------
# 4) RUN
# ----------------------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

# ── PREFLIGHT — F4211 driver + F42005 commission must be present in Silver ──
_errs = []
if not _exists(sname(F4211)):
    _errs.append("MISSING driver {} — F4211 is the fact DRIVER; must be ingested to Silver".format(sname(F4211)))
    print("  MISSING : {}  (sales-order detail — the fact DRIVER)".format(sname(F4211)))
else:
    print("  OK      : {}  (sales-order detail — the fact DRIVER)".format(sname(F4211)))
if not _exists(sname(F42005)):
    _errs.append("MISSING {} — F42005 commission ledger must be ingested to Silver".format(sname(F42005)))
    print("  MISSING : {}  (commission ledger — LEFT-joined)".format(sname(F42005)))
else:
    print("  OK      : {}  (commission ledger — LEFT-joined)".format(sname(F42005)))
if _errs:
    raise Exception("Commission-fact preflight FAILED: " + "; ".join(_errs)
                    + ". Ensure F4211 and F42005 are ingested to lh_jde_silver.jde.")
print("✓ preflight passed")

# ── SOURCE PREFLIGHT — visibility over every declared Silver source (F42119 optional). ──
for _s in FACT_SOURCES:
    _ok = spark.catalog.tableExists(sname(_s["silver"]))
    _tag = "OK" if _ok else ("OPTIONAL-missing (skipped)" if _s.get("optional") else "MISSING")
    print("  source {:<40s} {}".format(_s["silver"], _tag))

# ── BATCH BUILD — read the full Silver snapshot, run build_fact() ONCE, overwrite the fact.
#   Plain overwrite (no Gold CDF) — same execution pattern as ESO4 / ESO5 / the ESO1 freight fact.
#   Address dims are REUSED (rpt.dim_address_book role views) + rpt.dim_plant; the ABAC10 description
#   resolves via dim_category_code_10 (built by nb_eso1_gold_dim_category_code_10). None are touched here.
_run_start = time.time()
if MANUAL_OVERWRITE or not spark.catalog.tableExists(gname(FACT)):
    print("== FULL LOAD ==")
    new = build_fact()
    (new.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(gname(FACT)))
    _rows   = new.count()
    _status = "built"
    print("  {} rows={:,}".format(gname(FACT), _rows))
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
