# ============================================================================
# ESO1 Gold — fact_price_adjustment  (F4074 price-adjustment ledger, per-adjustment grain)
# ----------------------------------------------------------------------------
# WHY THIS FACT EXISTS
#   The consolidated `fact_sales_order_freight` collapses F4074 to ONE deterministic
#   row per order line (row_number pick). That is correct for the freight report, but it
#   CANNOT reproduce the SOP-family reports that show/aggregate the price adjustment at
#   its own grain — one row per (order line x F4074 adjustment). This fact is that grain.
#
#   Reports served (all join F4074 with an ALAST whitelist):
#     Tier 1 — DISPLAY + GROUP BY the adjustment (strictly need this grain):
#       SOP0025 Monthly Sales Report - Detail
#       SOP0006 Shipped NOT Invoiced Order Inquiry
#       SOP0007 Invoiced Orders
#       SOP0008 Pioneer Natural Resources Sales
#       SOP000x - Sales Orders at Next Status 577
#       SOP000x - Sales Orders at Next Status 580 - SO CO & ST
#       SOP000x - Sales Orders at Next Status 620
#     Tier 2 — F4074 whitelist used only as a row FILTER (the single-pick freight fact
#       cannot answer "does this line have ANY whitelisted adjustment" reliably):
#       Baseline Report (Finance) / DE Orders / BP Freight and Fuel (Combined)
#
# GRAIN
#   One row per actual F4074 record joined to its F4211(u F42119) sales line, via a
#   LEFT join — so a line with NO adjustment still gets ONE row (adjustment columns NULL),
#   reproducing Hubble's `ALAST IS NULL OR ALAST IN (whitelist)` LEFT-join behaviour.
#   A line with N adjustments -> N rows. `adjustment_seq` disambiguates true duplicates.
#
# NO BUSINESS FILTERS (Gold rule)
#   The Hubble ALAST whitelist is NOT applied here — every adjustment is carried and
#   `price_adjustment_type` is the Power BI page filter (each report applies its own
#   whitelist; the sets differ across reports). No company / status / line-type filter.
#
# RELATIONSHIP (wire in the semantic model, not here)
#   fact_price_adjustment[sales_order_line_key] -> fact_sales_order_freight[sales_order_line_key]
#   (many-to-one; the freight fact is unique on that key). Line display columns and
#   line-level page filters come from the freight fact via that relationship; this fact
#   carries only the adjustment columns, the keys, and the line MEASURES (physically, so a
#   SUM at adjustment grain fans out exactly like Hubble's per-adjustment SUM(SDAEXP) etc.).
#
#   `sales_order_line_key` is computed with the SAME sk() over the SAME four columns
#   (company_key_order_no, order_type, order_number, line_number) as the freight fact, so
#   the keys match byte-for-byte.
# ============================================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------

MANUAL_OVERWRITE = True        # True: drop+rebuild; flip to False after a healthy run

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

# ── Silver sources ────────────────────────────────────────────────────────────
F4211   = "f4211_sales_order_detail_file"
F4074   = "f4074_price_adjustment_ledger_file"
F41002  = "f41002_item_units_of_measure_conversion_factors"
F41003  = "f41003_unit_of_measure_standard_conversion"   # standard (item-agnostic) UoM conversion — Tier-2 tons fallback
# page-level / history union (optional — same 268-col SD* schema as F4211)
F42119  = "f42119_sales_order_history_file"

# ── Gold target BUILT here ─────────────────────────────────────────────────────
FACT = "fact_price_adjustment"

print(f"ESO1 Gold price-adjustment fact (batch build) — target {gname(FACT)}")

# HELPERS
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def load_silver_table(table_name):
    df = spark.read.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

def _load_optional(table_name):
    """Load a Silver table only if it exists; else warn and return None (used for F42119)."""
    try:
        if spark.catalog.tableExists(sname(table_name)):
            return load_silver_table(table_name)
    except Exception as e:
        print("  ⚠ optional source check failed for {}: {}".format(sname(table_name), e))
    print("  ⚠ optional source not found, skipping union: {}".format(sname(table_name)))
    return None

def sk(*cols):
    """Surrogate key — pipe-separated string from one or more column names."""
    # identical to the freight fact's sk() so sales_order_line_key matches byte-for-byte
    return F.concat_ws(
        "|",
        *[
            F.col(c).cast("string") if isinstance(c, str) else c.cast("string")
            for c in cols
        ],
    )

def tableExists(fqn):
    try:
        return spark.catalog.tableExists(fqn)
    except Exception:
        return False


# ----------------------------------------------------------------------------
# 2) FACT BUILDER
# ----------------------------------------------------------------------------

# UoM -> TN conversion, faithful to Hubble's SOP620 RC19 / "Shipped w/o Confirmation" cascade:
#   Tier 1  item-specific F41002 (UMMCU=blank, direct uom<->TN, fwd + reciprocal rev)
#   Tier 2  standard F41003 (item-agnostic, direct uom<->TN, fwd + reciprocal rev)  ← was missing (F1)
#   Tier 0  TN passthrough = 1.0     (applied in build_fact)
# For a direct uom<->TN row Hubble's via-primary ratio UMCNV1(uom)/UMCNV1(TN) reduces to UMCONV, so UMCONV
# (conversion_factor) is the correct column. UMMCU=blank matches Hubble's branch-independent pick (F3);
# dedup to one row per key prevents the LEFT-join fan-out that double-counts at adjustment grain (F4).
def build_uom_cascades():
    f41002 = load_silver_table(F41002)
    # UMMCU = blank -> the branch-independent (item default) conversion (Hubble filters UMMCU=' ').
    f41002 = f41002.filter((F.trim(F.col("cost_center")) == "") | F.col("cost_center").isNull())
    item_fwd = (f41002.filter((F.trim("related_uom") == "TN") & (F.col("conversion_factor") != 0))
                .select(F.col("identifier_short_item").alias("itm"), F.trim("uom").alias("from_uom"),
                        F.col("conversion_factor").cast("double").alias("conv_factor")))
    item_rev = (f41002.filter((F.trim("uom") == "TN") & (F.col("conversion_factor") != 0))
                .select(F.col("identifier_short_item").alias("itm"), F.trim("related_uom").alias("from_uom"),
                        (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_factor")))
    return item_fwd.unionByName(item_rev).dropDuplicates(["itm", "from_uom"])

def build_std_uom_cascade():
    # Tier 2 — F41003 standard (item-agnostic) UoM -> TN, coalesced AFTER the F41002 item factor, reproducing
    # Hubble's F41002 -> F41003 tiering. Same direct fwd + reciprocal rev shape; deduped to one row per uom.
    f41003 = load_silver_table(F41003)
    std_fwd = (f41003.filter((F.trim("related_uom") == "TN") & (F.col("conversion_factor") != 0))
               .select(F.trim("uom").alias("from_uom"),
                       F.col("conversion_factor").cast("double").alias("std_factor")))
    std_rev = (f41003.filter((F.trim("uom") == "TN") & (F.col("conversion_factor") != 0))
               .select(F.trim("related_uom").alias("from_uom"),
                       (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("std_factor")))
    return std_fwd.unionByName(std_rev).dropDuplicates(["from_uom"])


# Stored columns, in report order.
FACT_BUSINESS_COLS = [
    # ── line identity (also available on the freight fact; kept here for display/debug) ──
    "company", "company_key_order_no", "order_type", "order_number", "line_number",
    # ── the price adjustment (F4074) — one row per adjustment record ──
    "price_adjustment_type",      # ALAST   (the whitelist filter column; e.g. "PP06", "FRTHIDE")
    "adj_unit_price",             # ALUPRC  (amt_price_per_unit_02) — per-TON adjustment RATE
    "adj_extended_amount",        # extended adjustment $ = adj_unit_price (per-ton) * transaction_quantity_tons (ORDERED tons)
    "adj_uom",                    # ALUOM   (F4074 uom_as_input)
    "adj_based_on_value",         # ALBSDVAL
    "adj_gl_class",               # ALGLC
    "adj_factor_value",           # ALFVTR
    "adjustment_seq",             # 1..N per line (disambiguates duplicate F4074 rows)
    # ── line measures (carried so a SUM at adjustment grain fans out like Hubble) ──
    "quantity_shipped",           # SDSOQS
    "extended_price",             # SDAEXP
    "extended_cost",              # SDECST  (SOP000x-580 ReportColumn17)
    "transaction_quantity",       # SDUORG
    "primary_quantity_ordered",   # SDPQOR
    "quantity_shipped_tons",      # SDSOQS * conv  (ReportColumn10)
    "transaction_quantity_tons",  # SDUORG * conv  (ReportColumn11 — ordered tons)
    # ── conversion / currency ──
    "conversion_to_tons_rate",    # F41002 item factor (TN passthrough = 1.0); NULL if unresolved
    "missing_conversion_flag",    # 'Y' when conversion_to_tons_rate is NULL
    "currency_code",              # SDBCRC (domestic currency display)
]


def build_fact():
    # ── spine: F4211 (u F42119 history when present) ──
    f4211 = load_silver_table(F4211)
    _hist = _load_optional(F42119)
    if _hist is not None:
        f4211 = f4211.unionByName(_hist, allowMissingColumns=True)
    sd = f4211

    # ── F4074 price-adjustment ledger — NOT collapsed (this is the per-adjustment grain) ──
    #    whitelist is NOT applied (Gold rule) — price_adjustment_type is the PBI page filter.
    adj = (load_silver_table(F4074).select(
                F.col("company_key_order_no").alias("adj_kcoo"),          # ALKCOO
                F.col("document_order_invoice_e").alias("adj_doco"),      # ALDOCO
                F.col("order_type").alias("adj_dcto"),                    # ALDCTO
                F.col("line_number").alias("adj_lnid"),                   # ALLNID
                F.trim(F.col("price_adjustment_type")).alias("price_adjustment_type"),  # ALAST
                F.col("amt_price_per_unit_02").cast("double").alias("adj_unit_price"),  # ALUPRC
                F.trim(F.col("uom_as_input")).alias("adj_uom"),           # ALUOM
                F.col("based_on_value").alias("adj_based_on_value"),      # ALBSDVAL
                F.col("gl_class").alias("adj_gl_class"),                  # ALGLC
                F.col("factor_value").alias("adj_factor_value")))         # ALFVTR

    # ── item + standard UoM -> TN conversion (line-level, constant per line) ──
    ci = build_uom_cascades()      # Tier 1 — item-specific F41002
    cs = build_std_uom_cascade()   # Tier 2 — standard F41003

    j = (sd.alias("sd")
         # LEFT: a line with no F4074 adjustment survives with a single NULL-adjustment row
         .join(adj.alias("aj"),
               (F.col("aj.adj_kcoo") == F.col("sd.company_key_order_no")) &
               (F.col("aj.adj_doco") == F.col("sd.document_order_invoice_e")) &
               (F.col("aj.adj_dcto") == F.col("sd.order_type")) &
               (F.col("aj.adj_lnid") == F.col("sd.line_number")), "left")
         .join(ci.alias("ci"),
               (F.col("ci.itm") == F.col("sd.identifier_short_item")) &
               (F.col("ci.from_uom") == F.trim(F.col("sd.uom_as_input"))), "left")
         .join(cs.alias("cs"),                                       # F41003 std fallback (1:1 on uom, no fan-out)
               (F.col("cs.from_uom") == F.trim(F.col("sd.uom_as_input"))), "left"))

    # UoM -> TN rate: TN passthrough (1.0) -> F41002 item factor -> F41003 standard factor; NULL if all unresolved.
    conv_rate = F.coalesce(
        F.when(F.trim(F.col("sd.uom_as_input")) == "TN", F.lit(1.0)),
        F.col("ci.conv_factor"),
        F.col("cs.std_factor"))

    sel = j.select(
        # ── line identity ──
        F.col("sd.company").alias("company"),
        F.col("sd.company_key_order_no").alias("company_key_order_no"),
        F.col("sd.order_type").alias("order_type"),
        F.col("sd.document_order_invoice_e").alias("order_number"),
        F.col("sd.line_number").alias("line_number"),
        # ── adjustment (F4074) ──
        F.col("aj.price_adjustment_type").alias("price_adjustment_type"),
        F.col("aj.adj_unit_price").alias("adj_unit_price"),
        F.col("aj.adj_uom").alias("adj_uom"),
        F.col("aj.adj_based_on_value").alias("adj_based_on_value"),
        F.col("aj.adj_gl_class").alias("adj_gl_class"),
        F.col("aj.adj_factor_value").alias("adj_factor_value"),
        # ── line measures ──
        F.col("sd.units_quantity_shipped").alias("quantity_shipped"),      # SDSOQS
        F.col("sd.amount_extended_price").alias("extended_price"),         # SDAEXP
        F.col("sd.amount_extended_cost").alias("extended_cost"),           # SDECST
        F.col("sd.units_transaction_qty").alias("transaction_quantity"),   # SDUORG
        F.col("sd.units_primary_qty_order").alias("primary_quantity_ordered"),  # SDPQOR
        # ── conversion / currency ──
        conv_rate.alias("conversion_to_tons_rate"),
        F.col("sd.currency_code_base").alias("currency_code"),             # SDBCRC
    )

    # tons: NULL conversion -> 0 tons (matches Hubble's `THEN 0`)
    sel = (sel
           .withColumn("quantity_shipped_tons",
                       F.col("quantity_shipped") * F.coalesce(F.col("conversion_to_tons_rate"), F.lit(0.0)))
           .withColumn("transaction_quantity_tons",
                       F.col("transaction_quantity") * F.coalesce(F.col("conversion_to_tons_rate"), F.lit(0.0)))
           .withColumn("missing_conversion_flag",
                       F.when(F.col("conversion_to_tons_rate").isNull(), F.lit("Y")).otherwise(F.lit("N")))
           # extended adjustment $ = per-TON adjustment rate (ALUPRC) * ORDERED TONS (transaction_quantity_tons =
           # SDUORG * conv). Confirmed vs Hubble: Product Price (A03) matched only after switching the multiplier
           # from quantity_shipped (input UOM) to ordered tons — ALUPRC is priced per ton, so ALUPRC * quantity_shipped
           # overstated it by ~1/conv (the per-item UOM->TN factor, ~3.35x here, varying by item). Pre-materialized so
           # the bucket measures stay a scalar SUM. NULL rate or 0 tons -> 0 (transaction_quantity_tons COALESCEs
           # missing conv to 0 tons; note the F41003 fallback gap still zeroes lines with no F41002 TN row).
           .withColumn("adj_extended_amount",
                       F.col("adj_unit_price") * F.col("transaction_quantity_tons")))

    # ── keys ──
    #  sales_order_line_key: SAME sk + SAME four columns as the freight fact (relationship key)
    sel = sel.withColumn("sales_order_line_key",
                         sk("company_key_order_no", "order_type", "order_number", "line_number"))
    #  adjustment_seq: 1..N per line so identical duplicate F4074 rows stay distinct (Hubble keeps both).
    #  Deterministic order over the adjustment columns; the NULL-adjustment row sorts last -> seq 1.
    _aw = (Window.partitionBy("sales_order_line_key")
           .orderBy(F.col("price_adjustment_type").asc_nulls_last(),
                    F.col("adj_unit_price").asc_nulls_last(),
                    F.col("adj_uom").asc_nulls_last(),
                    F.col("adj_based_on_value").asc_nulls_last(),
                    F.col("adj_gl_class").asc_nulls_last(),
                    F.col("adj_factor_value").asc_nulls_last()))
    sel = sel.withColumn("adjustment_seq", F.row_number().over(_aw))
    sel = sel.withColumn("price_adjustment_key", sk("sales_order_line_key", "adjustment_seq"))

    df = sel.dropDuplicates(["price_adjustment_key"])          # unique by construction; defensive
    return df.select("price_adjustment_key", "sales_order_line_key", *FACT_BUSINESS_COLS)


# ----------------------------------------------------------------------------
# 3) FACT SOURCES  (read-only preflight)
# ----------------------------------------------------------------------------
FACT_SOURCES = [
    {"silver": F4211,  "note": "sales-order line spine (measures + keys + item/uom for conversion)"},
    {"silver": F4074,  "note": "price-adjustment ledger — the per-adjustment grain (ALAST/ALUPRC/ALUOM/ALBSDVAL/ALGLC/ALFVTR)"},
    {"silver": F41002, "note": "item UoM -> TN conversion factors (Tier 1)"},
    {"silver": F41003, "note": "standard UoM -> TN conversion factors (Tier 2 fallback)"},
    {"silver": F42119, "note": "sales-order history (optional union; guarded by _load_optional)"},
]


# ----------------------------------------------------------------------------
# 4) RUN
# ----------------------------------------------------------------------------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_LH}.{GOLD_SCHEMA}")

# preflight: confirm required sources exist (F42119 is optional)
for s in FACT_SOURCES:
    t = s["silver"]
    if t == F42119:
        continue
    if not tableExists(sname(t)):
        raise RuntimeError(f"Required Silver source missing: {sname(t)} — {s['note']}")

target = gname(FACT)
if MANUAL_OVERWRITE or not tableExists(target):
    out = build_fact()
    (out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target))
    n = spark.table(target).count()
    print(f"  ✓ wrote {target}: {n} rows / {len(out.columns)} columns")
    payload = {"table": target, "rows": n, "columns": len(out.columns), "mode": "overwrite"}
else:
    print(f"  • {target} exists and MANUAL_OVERWRITE=False — skip (no-op)")
    payload = {"table": target, "mode": "skip"}

try:
    import json
    notebookutils.notebook.exit(json.dumps(payload))
except Exception:
    print(payload)
