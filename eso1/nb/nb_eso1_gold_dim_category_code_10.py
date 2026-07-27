#!/usr/bin/env python
# coding: utf-8

# ## nb_eso1_gold_dim_category_code_10
#
# **Gold UDC-dimension processor** for ESO1 — SOP0027 Commission.
# Builds ONE small reference dimension from ONE Silver source (the user-defined-code
# values, F0005):
#   • `lh_jde_gold.rpt.dim_category_code_10` — UDC 01/10 : category_code_10 -> category_code_10_desc
#
# Address-book category codes AC01–AC30 edit against UDC system '01', types '01'–'30'
# (standard JDE), so ABAC10 (`category_code_10` on the sold-to F0101) resolves against 01/10.
# `fact_sales_commission` stores the raw FK code (`category_code_10`); this dim resolves the
# description in the Direct Lake model (fact.category_code_10 -> dim_category_code_10.category_code_10).
# Same F0005 lookup shape ESO4 uses for dim_sic (01/SC) / dim_state (00/S) and ESO7 for dim_status (40/AT).
#
# ── BUILD (BATCH, structure identical to ESO4 nb_eso4_gold_dim_udc.py / ESO5 dim notebooks) ──
#   • read the full F0005 snapshot, run build_dim() ONCE (UDC-filtered Type-1 dim), overwrite the dim.
#   • MANUAL_OVERWRITE = True → drop + rebuild; False → build only if the dim is missing (re-run to refresh).
#   • no CDF / foreachBatch / checkpoints / streams. Result is IDENTICAL to the previous streaming
#     full-load seed (build_dim() is the old transform_dim() unchanged, dead `restrict_keys` scope-filter
#     removed); plain overwrite (no Gold CDF) matches ESO4/ESO5.
#
# Sections:  1) CONFIG   2) DIM BUILDER   3) RUN
# Design: eso1/docs/ESO1_gold_layer_design.md §4.6


# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------

# CONFIG + CONSTANTS  (names per Fabric_Naming_Convention_Guidelines.pdf)
import json, time
from datetime import datetime, timezone
from pyspark.sql import functions as F

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"          # `jde` (2026-07-26, was `jde_cdc` / `cdf`) — same as ESO4/ESO5; sources read as STATIC batch snapshots (no CDF needed)
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

MANUAL_OVERWRITE = True    # ⚠ set back to False after the first successful run

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

F0005     = "f0005_user_defined_code_values"

# UDC selector — address-book category code 10 = UDC 01/10 (standard JDE: AC01–AC30 -> 01/01–01/30).
# Confirm if US Silica remapped AC10's edit UDC; flagged like ESO4's inferred UDCs (design §5).
CAT10_SYS, CAT10_TYPE = "01", "10"

DIM = "dim_category_code_10"

print(f"ESO1 Gold dim_category_code_10 processor (batch build) — target {gname(DIM)}")


# HELPERS  (identical to the ESO4 dim_udc notebook)
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def load_silver_table(table_name):
    df = spark.read.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])


# ----------------------------------------------------------------------------
# 2) DIM BUILDER
# ----------------------------------------------------------------------------

# DIM transform — F0005 UDC 01/10 lookup. Natural PK = the UDC value (DRKY) within its
# system/type, so the transform filters product_code/user_defined_codes first, then keys on
# trim(user_defined_code).
def build_dim():
    f0005 = (load_silver_table(F0005)
             .where((F.trim(F.col("product_code")) == CAT10_SYS) &
                    (F.trim(F.col("user_defined_codes")) == CAT10_TYPE)))
    return (f0005.select(F.trim(F.col("user_defined_code")).alias("category_code_10"),        # DRKY
                         F.trim(F.col("description_001")).alias("category_code_10_desc"))      # DRDL01
            .where(F.col("category_code_10") != "")
            .dropDuplicates(["category_code_10"]))


# ----------------------------------------------------------------------------
# 3) RUN
# ----------------------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

# BATCH BUILD — read the full F0005 snapshot, run build_dim() once (UDC 01/10), overwrite the dim.
#   MANUAL_OVERWRITE = True  -> drop + rebuild from the full Silver snapshot.
#   MANUAL_OVERWRITE = False -> build only if the dim is missing (re-run to refresh).
#   Plain overwrite (no Gold CDF) — same execution pattern as ESO4 / ESO5.
_run_start = time.time()
if MANUAL_OVERWRITE or not spark.catalog.tableExists(gname(DIM)):
    print("== FULL LOAD ==")
    new = build_dim()
    (new.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(gname(DIM)))
    _rows   = new.count()
    _status = "built"
    print("  {} rows={:,}".format(gname(DIM), _rows))
else:
    print("== skip — {} exists and MANUAL_OVERWRITE=False ==".format(gname(DIM)))
    _rows, _status = None, "skipped"

_elapsed = round(time.time() - _run_start, 1)
_exit_payload = {
    "status":       _status,
    "table":        gname(DIM),
    "rows":         _rows,
    "elapsed_sec":  _elapsed,
    "end_time_utc": datetime.now(timezone.utc).isoformat(),
}
print("exit payload:", json.dumps(_exit_payload))
notebookutils.notebook.exit(json.dumps(_exit_payload))
