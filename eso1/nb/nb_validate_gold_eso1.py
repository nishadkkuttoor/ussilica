#!/usr/bin/env python
# coding: utf-8

# ## nb_validate_gold_eso1
#
# Validation suite for the Extended Sales Order 1 Gold layer. Covers: REUSED
# dimension health, record-count reconciliation, data completeness, duplicate
# detection, key integrity, fact↔dimension RI, and report reconciliation
# (design §8). Writes one row per check to lh_jde_gold.rpt.eso1_validation_log.
#
# SELF-CONTAINED — no %run; declares its own constants inline. Independent nb/ notebook
# (alongside nb_maintenance_gold_eso1 / nb_semantic_model_eso1); none depends on another
# resolving by name. Run AFTER the Gold build notebooks have seeded the tables.

# In[1]:


from datetime import datetime
from pyspark.sql import functions as F, Row

# ── Self-contained constants + helper (no %run nb_eso1_transforms) ────────────
GOLD_SCHEMA = "lh_jde_gold.rpt"
RPT_SCHEMA  = "lh_jde_gold.rpt"
T_FACT     = f"{GOLD_SCHEMA}.fact_sales_order_freight"
T_DIM_ITEM = f"{GOLD_SCHEMA}.dim_item"
R_DIM_AB      = f"{RPT_SCHEMA}.dim_address_book"
R_DIM_PLANT   = f"{RPT_SCHEMA}.dim_plant"
R_DIM_SHIP_TO = f"{RPT_SCHEMA}.dim_address_ship_to"
R_DIM_SOLD_TO = f"{RPT_SCHEMA}.dim_address_sold_to"
R_DIM_CARRIER = f"{RPT_SCHEMA}.dim_address_carrier"
F4211_TBL = "jde.f4211_sales_order_detail_file"
F0101_TBL = "jde.f0101_address_book_master"
COMPANIES = ["00640", "00645"]
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]
def load_silver_table(spark, table_name):
    df = spark.read.table(table_name)
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

run_dt = datetime.now()
results = []   # (check, expected, actual, passed)

def check(name, expected, actual, passed):
    results.append((name, str(expected), str(actual), bool(passed)))
    print(f"[{'PASS' if passed else 'FAIL'}] {name:42s} expected={expected}  actual={actual}")

fact = spark.read.table(T_FACT)
active = fact.filter(F.col("is_deleted") == False)
LOG_TBL = f"{GOLD_SCHEMA}.eso1_validation_log"

def _short(t): return t.split(".")[-1]


# In[2]:


# ── 0. REUSED DIMENSION HEALTH — verify the conformed dims this fact REUSES ────
# ESO1 does NOT build dim_address_book / dim_plant (or the role views) — it relates
# to them on natural keys. Confirm they exist, are non-empty, unique-PK, supply the
# display labels the report needs, and COVER the fact's keys, BEFORE trusting the
# fact↔dim RI in §4/5. Base Delta dims are HARD; role views are best-effort (they
# may be SQL-endpoint-only -> Direct Lake binds them, Spark may not see them).
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

# 0a — base dims: exist + non-empty + unique PK (HARD)
for tbl, pk in [(R_DIM_AB, "address_number"), (R_DIM_PLANT, "plant_code")]:
    if not _exists(tbl):
        check(f"reused_exists_{_short(tbl)}", "exists", "MISSING", False)
        continue
    d = spark.read.table(tbl); n = d.count()
    check(f"reused_nonempty_{_short(tbl)}", "> 0", f"{n:,}", n > 0)
    dups = d.groupBy(pk).count().filter("count > 1").count()
    check(f"reused_pk_unique_{_short(tbl)}", 0, dups, dups == 0)

# 0b — role views: best-effort. If Spark-visible -> non-empty + subset of base.
base_ids = (spark.read.table(R_DIM_AB).select(F.col("address_number").alias("k"))
            if _exists(R_DIM_AB) else None)
for v in [R_DIM_SHIP_TO, R_DIM_SOLD_TO, R_DIM_CARRIER]:
    if not _exists(v):
        check(f"reused_roleview_{_short(v)}", "spark or sql-endpoint", "not spark-visible (OK if SQL-endpoint)", True)
        continue
    vn = spark.read.table(v).count()
    check(f"reused_roleview_nonempty_{_short(v)}", "> 0", f"{vn:,}", vn > 0)
    if base_ids is not None:
        orphans = (spark.read.table(v).select(F.col("address_number").alias("k"))
                   .where(F.col("k").isNotNull()).join(base_ids, "k", "left_anti").count())
        check(f"reused_roleview_consistent_{_short(v)}", 0, orphans, orphans == 0)

# 0c — attribute usability (informational): the reused dims supply the report's
# display labels; a populated key attribute keeps report labels from rendering blank.
def pct_populated(tbl, col):
    d = spark.read.table(tbl); tot = d.count()
    if tot == 0 or col not in d.columns:
        return None
    nn = d.filter(F.col(col).isNotNull() & (F.trim(F.col(col).cast("string")) != "")).count()
    return round(100.0 * nn / tot, 1)
for tbl, col, tgt in [(R_DIM_AB, "name_alpha", ">=95%"), (R_DIM_PLANT, "plant_name", ">=95%")]:
    if _exists(tbl):
        p = pct_populated(tbl, col)
        check(f"reused_attr_{col}_pct", f"informational ({tgt})", "n/a" if p is None else f"{p}%", True)

# 0d — coverage (informational): % of the fact's distinct natural-key values that
# resolve in the reused dim (complements the HARD left-anti RI gate in §4/5).
def coverage(fact_col, tbl, dim_col):
    if not _exists(tbl):
        return None
    fk = active.select(F.col(fact_col).alias("k")).where(F.col("k").isNotNull()).distinct()
    tot = fk.count()
    if tot == 0:
        return 100.0
    hit = fk.join(spark.read.table(tbl).select(F.col(dim_col).alias("k")), "k", "left_semi").count()
    return round(100.0 * hit / tot, 1)
for fc, tbl, dc in [("ship_to", R_DIM_AB, "address_number"),
                    ("bill_to", R_DIM_AB, "address_number"),
                    ("carrier_number", R_DIM_AB, "address_number"),
                    ("branch_plant", R_DIM_PLANT, "plant_code")]:
    cov = coverage(fc, tbl, dc)
    check(f"reused_coverage_{fc}", "informational (target 100%)",
          "SKIP" if cov is None else f"{cov}%", True)

# 0e — freshness (informational) — newest last_refreshed_timestamp per reused dim
for tbl in [R_DIM_AB, R_DIM_PLANT]:
    if _exists(tbl):
        d = spark.read.table(tbl)
        if "last_refreshed_timestamp" in d.columns:
            ts = d.agg(F.max("last_refreshed_timestamp")).first()[0]
            check(f"reused_freshness_{_short(tbl)}", "informational", str(ts), True)


# In[3]:


# ── 1. RECORD-COUNT RECONCILIATION — Gold active lines vs Silver filtered ─────
f4211 = load_silver_table(spark, F4211_TBL)
f0101 = load_silver_table(spark, F0101_TBL)
gate = (f0101.filter((F.trim("address_type_01").between("A", "P")) |
                     (F.trim("address_type_01").between("R", "ZZZ")))
        .select(F.col("address_number").alias("dq_an8")).distinct())
silver_expected = (f4211
    .filter(F.col("company").isin(*COMPANIES))
    .filter(F.col("line_type") == "S")
    .filter(F.col("status_code_last") != "980")
    .join(gate, f4211["address_number_ship_to"] == gate["dq_an8"], "left_semi")
    .count())
gold_actual = active.count()
# tolerance: gold may DISTINCT-collapse F4074 fan-out -> gold <= silver line count
check("record_count_reconciliation", f"<= {silver_expected:,}", f"{gold_actual:,}", gold_actual <= silver_expected and gold_actual > 0)


# In[4]:


# ── 2. DATA COMPLETENESS — null rate on mandatory fields ──────────────────────
mandatory = ["order_number", "order_type", "line_number", "shipment_number",
             "ship_to", "branch_plant"]
for c in mandatory:
    nulls = active.filter(F.col(c).isNull()).count()
    check(f"completeness_{c}", 0, nulls, nulls == 0)

missing_conv = active.filter(F.col("missing_conversion_flag") == "Y").count()
check("completeness_uom_conversion_present", "informational",
      f"{missing_conv:,} lines missing conversion", True)


# In[5]:


# ── 3. DUPLICATE DETECTION ────────────────────────────────────────────────────
dup_lines = fact.groupBy("sales_order_line_key").count().filter("count > 1").count()
check("duplicate_sales_order_line_key", 0, dup_lines, dup_lines == 0)

# one freight bucket set per shipment (denormalized value must be consistent across the shipment's lines)
inconsistent = (active.filter(F.col("shipment_number").isNotNull())
                .groupBy("shipment_number")
                .agg(F.countDistinct("total_billable").alias("d"))
                .filter("d > 1").count())
check("freight_bucket_consistent_per_shipment", 0, inconsistent, inconsistent == 0)


# In[6]:


# ── 4 & 5. KEY INTEGRITY + FACT↔DIMENSION RI (left-anti must be 0) ────────────
def orphan(fact_key, dim_table, dim_key):
    dim = spark.read.table(dim_table).select(F.col(dim_key).alias("k"))
    return (active.select(F.col(fact_key).alias("k")).where(F.col("k").isNotNull())
            .join(dim, "k", "left_anti").select("k").distinct().count())

# Natural-key RI against the REUSED dims (rpt.dim_address_book, rpt.dim_plant) +
# new dim_item. No date-key RI — ESO1 has no date dimension (dates are raw fact columns).
# Skips a check if a reused dim is not present.
for fk, dt, dk in [
    ("ship_to",                          R_DIM_AB,    "address_number"),
    ("bill_to",                          R_DIM_AB,    "address_number"),
    ("carrier_number",                   R_DIM_AB,    "address_number"),
    ("item_number_short",                T_DIM_ITEM,  "item_number_short"),
    ("branch_plant",                     R_DIM_PLANT, "plant_code"),
]:
    if not spark.catalog.tableExists(dt):
        check(f"key_integrity_{fk}", 0, f"SKIP (missing {dt})", True)
        continue
    o = orphan(fk, dt, dk)
    check(f"key_integrity_{fk}", 0, o, o == 0)


# In[7]:


# ── 6. REPORT RECONCILIATION — deduped freight control totals ─────────────────
# Two independent dedup methods must agree (proves the anchor == SUMX dedup logic).
anchor = (active.filter(F.col("is_primary_shipment_line") == "Y")
          .agg(F.round(F.sum("total_billable"), 2).alias("b"),
               F.round(F.sum("total_payable"), 2).alias("p"),
               F.round(F.sum("total_variance"), 2).alias("v")).first())
sumx = (active.filter(F.col("shipment_number").isNotNull())
        .groupBy("shipment_number")
        .agg(F.max("total_billable").alias("b"), F.max("total_payable").alias("p"),
             F.max("total_variance").alias("v"))
        .agg(F.round(F.sum("b"), 2).alias("b"), F.round(F.sum("p"), 2).alias("p"),
             F.round(F.sum("v"), 2).alias("v")).first())
check("recon_total_billable_dedup", anchor["b"], sumx["b"], anchor["b"] == sumx["b"])
check("recon_total_payable_dedup",  anchor["p"], sumx["p"], anchor["p"] == sumx["p"])
check("recon_total_variance_dedup", anchor["v"], sumx["v"], anchor["v"] == sumx["v"])

ship_ct = active.select("shipment_number").distinct().count()
check("recon_freight_shipment_count", "> 0", f"{ship_ct:,}", ship_ct > 0)
# NOTE: tie the above $ to the Hubble "BvP Combined" control totals once ShiftFactor is wired (design §11).


# In[8]:


# ── 7. BUCKET-RECOMPUTE PARITY (migration guard) ──────────────────────────────
# The ALAPRP1 bucket recompute must change ONLY the 6 adj_* columns (+ the new
# price_adjustment_print_code). This compares the latest fact build against the
# immediately-previous Delta version and asserts: (a) identical row count, and
# (b) every OTHER column byte-identical per sales_order_line_key. Auto-SKIPs when
# <2 versions exist, or once the previous version already carries the new column
# (i.e. after the first post-change rebuild) so ongoing rebuilds don't false-fail.
# ⚠ Assumes both builds ran on the SAME Silver snapshot — Silver drift between them
# would surface here as (legitimate) diffs, not a code regression.
_ADJ_COLS = ["adj_non_product", "adj_al_severance_tax", "adj_misc_billing",
             "adj_freight", "adj_car_charges", "adj_freight_hide"]
_NEW_COLS = ["price_adjustment_print_code"]   # added by the ALAPRP1 change (absent pre-change)
_KEY = "sales_order_line_key"
try:
    _hist = spark.sql(f"DESCRIBE HISTORY {T_FACT}")
    _vers = [r["version"] for r in
             _hist.select("version").orderBy(F.col("version").desc()).limit(2).collect()]
except Exception:
    _vers = []

if len(_vers) < 2:
    check("bucket_parity", "compare latest 2 versions", f"SKIP (only {len(_vers)} version(s))", True)
else:
    _v_cur, _v_prev = _vers[0], _vers[1]
    _cur  = spark.read.format("delta").option("versionAsOf", _v_cur).table(T_FACT)
    _prev = spark.read.format("delta").option("versionAsOf", _v_prev).table(T_FACT)
    if not all(c not in _prev.columns for c in _NEW_COLS):
        check("bucket_parity", "migration rebuild",
              f"SKIP (prev v{_v_prev} already has new schema)", True)
    else:
        _n_prev, _n_cur = _prev.count(), _cur.count()
        check("bucket_parity_rowcount", _n_prev, _n_cur, _n_prev == _n_cur)
        _cmp = [c for c in _cur.columns
                if c in _prev.columns and c not in _ADJ_COLS and c not in _NEW_COLS and c != _KEY]
        def _sig(df):
            return df.select(_KEY, F.sha2(F.concat_ws(
                "||", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in _cmp]), 256).alias("_h"))
        _j = _sig(_cur).alias("c").join(_sig(_prev).alias("p"), _KEY, "full_outer")
        _mismatch = _j.filter(F.col("c._h").isNull() | F.col("p._h").isNull()
                              | (F.col("c._h") != F.col("p._h"))).count()
        check(f"bucket_parity_noncols_identical ({len(_cmp)} cols)", 0, _mismatch, _mismatch == 0)


# In[9]:


# ── Persist the validation log + overall gate ─────────────────────────────────
log = spark.createDataFrame(
    [Row(check_name=n, expected=e, actual=a, passed=p,
         run_timestamp=run_dt) for (n, e, a, p) in results])
(log.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(LOG_TBL))

n_fail = sum(1 for r in results if not r[3])
print(f"\n{'='*60}\nVALIDATION SUMMARY : {len(results)-n_fail}/{len(results)} passed, {n_fail} failed")
display(log.orderBy("passed", "check_name"))
if n_fail:
    raise Exception(f"ESO1 Gold validation FAILED ({n_fail} checks) — see {LOG_TBL}")
print("✓ All ESO1 Gold validation checks passed.")
