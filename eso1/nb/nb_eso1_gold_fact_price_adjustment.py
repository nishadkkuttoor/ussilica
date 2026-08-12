#!/usr/bin/env python
# coding: utf-8

# ## nb_eso1_gold_fact_price_adjustment
#
# **Gold `fact_price_adjustment` processor** — builds ONE table,
# `lh_jde_gold.rpt.fact_price_adjustment`, at **order-line × price-adjustment** grain.
#
# ── GRAIN ──
# One row per F4211 order line × F4074 price-adjustment record (LEFT join): a line with no F4074
# adjustment still yields ONE row (adjustment columns NULL). F4074 is a line×adjustment relation, so
# keeping it here isolates the fan-out — the order-line fact stays strictly one row per line.
#
# ── SELF-CONTAINED ──
# The order-line context (key + extended_price / tons / item / line_type / deferred flag) is rebuilt
# HERE from Silver F4211 (∪ F42119), using the SAME derivations as the order-line fact — same
# `sales_order_line_key`, same F41002 TN-conversion, same F49211 deferred flag — so the keys match and
# the bucket measures reconcile. No dependency on any Gold fact.
#
# ── SOURCES ──
#   • Silver F4211 (∪ F42119 history, optional) — the order-line spine.
#   • Silver F41002 — item TN-conversion (ordered tons).
#   • Silver F49211 — SO-line tag (deferred_entries_flag / UDDEFF).
#   • Silver F4074 (price-adjustment ledger) — per-adjustment detail (ALAST / ALUPRC / ALAPRP1 / ALUOM /
#     ALBSDVAL / ALGLC / ALFVTR).
#
# ── NO FILTERS ──
# NO ALAST whitelist and no business WHERE is applied — the fact carries EVERY F4074 adjustment for a
# line; the whitelist/print-code selection is a downstream page filter. Bucket money is a MEASURE over
# this fact, not a stored bucket column.
#
# ── LINE-LEVEL vs ADJUSTMENT-LEVEL amounts ──
# `is_line_primary='Y'` marks exactly ONE row per line so line-level buckets (Product Price / Freight /
# Car Charges / Total Tons / Deferred Revenue) count the line amount ONCE and never fan out. Adjustment
# buckets (Non Product / AL Severance / Misc Billing / Freight Hide / Dryer Freight) sum per adjustment
# row = `adj_unit_price (ALUPRC) × ordered_tons`.
#
# ── BUILD (BATCH) ──
#   • read the Silver sources, run build_fact() ONCE, overwrite the fact.
#   • MANUAL_OVERWRITE = True -> drop + rebuild; False -> build only if the fact is missing.
#   • all sources read as static snapshots — no CDF / checkpoints / streams.

# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json, time
from datetime import datetime, timezone

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

# ── refresh / runtime config (BATCH build) ──
MANUAL_OVERWRITE = True    # ⚠ set back to False after the first successful run

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

# ── Silver sources ─────────────────────────────────────────────────────────────
F4211    = "f4211_sales_order_detail_file"                   # order-line spine
F42119   = "f42119_sales_order_history_file"                 # closed/purged history (optional UNION)
F41002   = "f41002_item_units_of_measure_conversion_factors" # item TN-conversion (ordered tons)
F49211   = "f49211_sales_order_detail_file_tag_file"         # SO-line tag (UDDEFF deferred flag)
F4074    = "f4074_price_adjustment_ledger_file"              # price-adjustment ledger

# ── Gold target BUILT here ─────────────────────────────────────────────────────
FACT = "fact_price_adjustment"

print(f"ESO1 Gold fact_price_adjustment processor (batch build) — target {gname(FACT)}")

# HELPERS
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def load_silver_table(table_name):
    df = spark.read.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

def _load_optional(table_name):
    """Load a Silver table only if it exists; else warn and return None. Used for F42119 (history) — the
    fact runs with OR without it, unioning the closed/purged rows when present and F4211-only otherwise."""
    try:
        if spark.catalog.tableExists(sname(table_name)):
            return load_silver_table(table_name)
    except Exception as e:
        print("  ⚠ optional source check failed for {}: {}".format(sname(table_name), e))
    print("  ⚠ optional source not found, skipping union: {}".format(sname(table_name)))
    return None

def sk(*cols):
    """Surrogate key — pipe-separated string (identical formula to the order-line fact)."""
    return F.concat_ws(
        "|",
        *[F.col(c).cast("string") if isinstance(c, str) else c.cast("string") for c in cols],
    )

def build_uom_cascade():
    """Item TN-conversion (fwd + reciprocal rev union) from F41002 — identical to the order-line fact.
    Keyed by (identifier_short_item, from_uom); conv_factor turns line units into tons."""
    f41002 = load_silver_table(F41002)
    item_fwd = (f41002.filter((F.trim("related_uom") == "TN") & (F.col("conversion_factor") != 0))
                .select(F.col("identifier_short_item").alias("itm"), F.trim("uom").alias("from_uom"),
                        F.col("conversion_factor").cast("double").alias("conv_factor")))
    item_rev = (f41002.filter((F.trim("uom") == "TN") & (F.col("conversion_factor") != 0))
                .select(F.col("identifier_short_item").alias("itm"), F.trim("related_uom").alias("from_uom"),
                        (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_factor")))
    return item_fwd.unionByName(item_rev)

# ----------------------------------------------------------------------------
# 2) FACT BUILDER
# ----------------------------------------------------------------------------

# Order-line context (one row per sales_order_line_key). These carry the line's money/tons/classification
# the buckets need; NULL adjustment rows still get them (they are line-level).
LINE_COLS = [
    "sales_order_line_key",          # sk(company_key_order_no, order_type, order_number, line_number)
    "company_key_order_no",          # SDKCOO — F4074 join key
    "order_type",                    # SDDCTO — F4074 join key
    "order_number",                  # SDDOCO — F4074 join key
    "line_number",                   # SDLNID — F4074 join key
    "extended_price",                # SDAEXP — Product Price / Freight / Car Charges / Deferred base
    "transaction_quantity",          # SDUORG — ordered units (× conversion = ordered tons)
    "conversion_to_tons_rate",       # F41002 item TN-conversion (F41002-only)
    "second_item_number",            # SDLITM
    "third_item_number",             # SDAITM — freight/car line classification by LEFT(,3)
    "line_type",                     # SDLNTY — F/FT freight-line test
    "gl_class",                      # SDGLC
    "uom",                           # SDUOM
    "deferred_entries_flag",         # F49211 UDDEFF — Deferred Revenue driver
]

# F4074 adjustment detail (NULL on lines with no adjustment).
#   ALAST=price_adjustment_type · ALAPRP1=adj_print_code · ALUPRC=adj_unit_price · ALUOM=adj_uom ·
#   ALBSDVAL=adj_based_on_value · ALGLC=adj_gl_class · ALFVTR=adj_factor_value
ADJ_COLS = ["price_adjustment_type", "adj_print_code", "adj_unit_price", "adj_uom",
            "adj_based_on_value", "adj_gl_class", "adj_factor_value"]

# Derived on the fact:
#   ordered_tons  = transaction_quantity × conversion_to_tons_rate  (line-level; repeats across adj rows)
#   adj_amount    = adj_unit_price × ordered_tons                   (per-adjustment bucket money; 0 on base)
#   is_line_primary = 'Y' on exactly ONE row per line               (line-level buckets count once)
DERIVED_COLS = ["ordered_tons", "adj_amount", "is_line_primary"]

FACT_BUSINESS_COLS = LINE_COLS + ADJ_COLS + DERIVED_COLS

def _build_line_context():
    """Rebuild the order-line context from Silver — byte-identical derivations to the order-line fact:
    F4211 ∪ F42119, F41002 TN-conversion, F49211 deferred flag, and the same sales_order_line_key."""
    f4211 = load_silver_table(F4211)
    # UNION F42119 history (optional) — same SD* schema; F42119 snake-names SDLITM/SDAITM as
    # identifier_2nd_item/identifier_3rd_item, so rename before the union or those cols NULL for history rows.
    _hist = _load_optional(F42119)
    if _hist is not None:
        if "identifier_2nd_item" in _hist.columns and "identifier_second_item" not in _hist.columns:
            _hist = _hist.withColumnRenamed("identifier_2nd_item", "identifier_second_item")
        if "identifier_3rd_item" in _hist.columns and "identifier_third_item" not in _hist.columns:
            _hist = _hist.withColumnRenamed("identifier_3rd_item", "identifier_third_item")
        f4211 = f4211.unionByName(_hist, allowMissingColumns=True)

    conv_item = build_uom_cascade()
    f49211    = load_silver_table(F49211)
    # deferred flag — 1:1 with the line, collapse defensively
    tag = (f49211.groupBy("company_key_order_no", "document_order_invoice_e", "order_type", "line_number")
              .agg(F.first("deferred_entries_flag", ignorenulls=True).alias("deferred_entries_flag")))

    # TN passes through as 1.0, else the item-specific F41002 factor; unresolved stays NULL.
    conv_rate = F.coalesce(F.when(F.trim(F.col("sd.uom_as_input")) == "TN", F.lit(1.0)),
                           F.col("ci.conv_factor"))

    line = (f4211.alias("sd")
            .join(conv_item.alias("ci"),
                  (F.col("ci.itm") == F.col("sd.identifier_short_item")) &
                  (F.col("ci.from_uom") == F.trim(F.col("sd.uom_as_input"))), "left")
            .join(tag.alias("tag"),
                  (F.col("tag.company_key_order_no") == F.col("sd.company_key_order_no")) &
                  (F.col("tag.document_order_invoice_e") == F.col("sd.document_order_invoice_e")) &
                  (F.col("tag.order_type") == F.col("sd.order_type")) &
                  (F.col("tag.line_number") == F.col("sd.line_number")), "left")
            .select(
                F.col("sd.company_key_order_no").alias("company_key_order_no"),   # SDKCOO
                F.col("sd.order_type").alias("order_type"),                       # SDDCTO
                F.col("sd.document_order_invoice_e").alias("order_number"),       # SDDOCO
                F.col("sd.line_number").alias("line_number"),                     # SDLNID
                F.col("sd.amount_extended_price").alias("extended_price"),        # SDAEXP
                F.col("sd.units_transaction_qty").alias("transaction_quantity"),  # SDUORG
                conv_rate.alias("conversion_to_tons_rate"),
                F.col("sd.identifier_second_item").alias("second_item_number"),   # SDLITM
                F.col("sd.identifier_third_item").alias("third_item_number"),     # SDAITM
                F.col("sd.line_type").alias("line_type"),                         # SDLNTY
                F.col("sd.gl_class").alias("gl_class"),                           # SDGLC
                F.col("sd.uom_as_input").alias("uom"),                            # SDUOM
                F.col("tag.deferred_entries_flag").alias("deferred_entries_flag"))# F49211 UDDEFF
            .withColumn("sales_order_line_key",
                        sk("company_key_order_no", "order_type", "order_number", "line_number"))
            # one row per order line (collapses any F4211/F42119 overlap) — same as the order-line fact
            .dropDuplicates(["sales_order_line_key"]))
    return line

def build_fact():
    line = _build_line_context()

    # F4074 adjustments — every row (no whitelist; ALAST is page-filtered downstream). Trim/cast the codes
    # and amounts so they align with the (kcoo, dcto, doco, lnid) line join keys.
    adj = (load_silver_table(F4074).select(
                F.trim(F.col("company_key_order_no")).alias("al_kcoo"),
                F.col("document_order_invoice_e").alias("al_doco"),
                F.trim(F.col("order_type")).alias("al_dcto"),
                F.col("line_number").alias("al_lnid"),
                F.trim(F.col("price_adjustment_type")).alias("price_adjustment_type"),     # ALAST
                F.trim(F.col("pricing_report_code_01")).alias("adj_print_code"),           # ALAPRP1
                F.col("amt_price_per_unit_02").cast("double").alias("adj_unit_price"),      # ALUPRC
                F.trim(F.col("uom_as_input")).alias("adj_uom"),                            # ALUOM
                F.col("based_on_value").cast("double").alias("adj_based_on_value"),         # ALBSDVAL
                F.col("gl_class").alias("adj_gl_class"),                                    # ALGLC
                F.col("factor_value").cast("double").alias("adj_factor_value")))           # ALFVTR

    # order-line  LEFT JOIN  adjustment  (kcoo, dcto, doco, lnid) — a line with N adjustments -> N rows;
    # a line with none -> ONE row, adjustment cols NULL.
    j = (line.alias("ln").join(adj.alias("aj"),
             (F.trim(F.col("ln.company_key_order_no")) == F.col("aj.al_kcoo")) &
             (F.col("ln.order_number")                 == F.col("aj.al_doco")) &
             (F.trim(F.col("ln.order_type"))           == F.col("aj.al_dcto")) &
             (F.col("ln.line_number")                  == F.col("aj.al_lnid")), "left"))

    df = j.select("ln.*", *[F.col("aj." + c).alias(c) for c in ADJ_COLS])

    # ordered tons (line-level) + per-adjustment bucket money
    df = (df.withColumn("ordered_tons",
                        F.col("transaction_quantity") * F.coalesce(F.col("conversion_to_tons_rate"), F.lit(0.0)))
            .withColumn("adj_amount",
                        F.coalesce(F.col("adj_unit_price"), F.lit(0.0)) * F.col("ordered_tons")))

    # is_line_primary — exactly ONE row per line carries the line-level amount (avoids fan-out double count)
    _w = Window.partitionBy("sales_order_line_key").orderBy(
             F.col("price_adjustment_type").asc_nulls_first())
    df = (df.withColumn("_rn", F.row_number().over(_w))
            .withColumn("is_line_primary", F.when(F.col("_rn") == 1, F.lit("Y")).otherwise(F.lit("N")))
            .drop("_rn"))

    # per-(line × adjustment) surrogate key — stable within a line via a deterministic adjustment sequence
    _ws = Window.partitionBy("sales_order_line_key").orderBy(
              F.col("price_adjustment_type").asc_nulls_first(),
              F.col("adj_unit_price").asc_nulls_first(),
              F.col("adj_gl_class").asc_nulls_first())
    df = (df.withColumn("_seq", F.row_number().over(_ws))
            .withColumn("price_adjustment_key", sk("sales_order_line_key", "_seq"))
            .drop("_seq"))

    return df.select("price_adjustment_key", *FACT_BUSINESS_COLS)

# ----------------------------------------------------------------------------
# 3) FACT SOURCES
# ----------------------------------------------------------------------------

# Silver-only. F42119 is optional (unioned via _load_optional when present).
FACT_SOURCES = [
    {"silver": F4211,  "role": "spine"},
    {"silver": F42119, "role": "union",  "optional": True},
    {"silver": F41002, "role": "static"},
    {"silver": F49211, "role": "static"},
    {"silver": F4074,  "role": "adjustment-detail"},
]

# ----------------------------------------------------------------------------
# 4) RUN
# ----------------------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

# preflight — confirm every declared Silver source exists (F42119 optional).
for _s in FACT_SOURCES:
    _ok = spark.catalog.tableExists(sname(_s["silver"]))
    _tag = "OK" if _ok else ("OPTIONAL-missing (skipped)" if _s.get("optional") else "MISSING")
    print("  source {:<44s} {}".format(_s["silver"], _tag))

_run_start = time.time()
if MANUAL_OVERWRITE or not spark.catalog.tableExists(gname(FACT)):
    print("== FULL LOAD ==")
    new = build_fact()
    (new.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(gname(FACT)))
    _rows   = new.count()
    _status = "built"
    print("  {} rows={}".format(gname(FACT), _rows))
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
