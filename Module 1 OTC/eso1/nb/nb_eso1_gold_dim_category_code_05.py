#!/usr/bin/env python
# coding: utf-8

# ## nb_eso1_gold_dim_category_code_05
#
# **Gold UDC-dimension processor** for ESO1 — resolves the salesperson description.
# Builds ONE small reference dimension from ONE Silver source (the user-defined-code
# values, F0005):
#   • `lh_jde_gold.rpt.dim_category_code_05` — UDC 01/05 : category_code_05 -> category_code_05_desc
#
# Address-book category codes AC01–AC30 edit against UDC system '01', types '01'–'30'
# (standard JDE), so ABAC05 (`category_code_05`, the sales-rep code on the ship-to/sold-to
# F0101) resolves against 01/05. `fact_sales_order_freight` stores the raw FK code
# (`category_code_05`); this dim resolves the description in the Direct Lake model
# (fact.category_code_05 -> dim_category_code_05.category_code_05).
#
# ── BUILD (BATCH) ──
#   • read the full F0005 snapshot, run build_dim() ONCE (UDC-filtered Type-1 dim), overwrite the dim.
#   • MANUAL_OVERWRITE = True → drop + rebuild; False → build only if the dim is missing (re-run to refresh).
#
# Sections:  1) CONFIG   2) DIM BUILDER   3) RUN


# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------

# CONFIG + CONSTANTS
import json, time
from datetime import datetime, timezone
from pyspark.sql import functions as F

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"          # Silver schema — static batch snapshots
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

MANUAL_OVERWRITE = True    # ⚠ set back to False after the first successful run

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

F0005     = "f0005_user_defined_code_values"

# UDC selector — address-book category code 05 = UDC 01/05 (standard JDE: AC01–AC30 -> 01/01–01/30).
CAT05_SYS, CAT05_TYPE = "01", "05"

DIM = "dim_category_code_05"

print(f"ESO1 Gold dim_category_code_05 processor (batch build) — target {gname(DIM)}")


# HELPERS
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def load_silver_table(table_name):
    df = spark.read.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])


# ----------------------------------------------------------------------------
# 2) DIM BUILDER
# ----------------------------------------------------------------------------

# DIM transform — F0005 UDC 01/05 lookup. Natural PK = the UDC value (DRKY) within its
# system/type, so the transform filters product_code/user_defined_codes first, then keys on
# trim(user_defined_code).
def build_dim():
    f0005 = (load_silver_table(F0005)
             .where((F.trim(F.col("product_code")) == CAT05_SYS) &
                    (F.trim(F.col("user_defined_codes")) == CAT05_TYPE)))
    return (f0005.select(F.trim(F.col("user_defined_code")).alias("category_code_05"),        # DRKY
                         F.trim(F.col("description_001")).alias("category_code_05_desc"))      # DRDL01
            .where(F.col("category_code_05") != "")
            .dropDuplicates(["category_code_05"]))


# ----------------------------------------------------------------------------
# 3) RUN
# ----------------------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

# BATCH BUILD — read the full F0005 snapshot, run build_dim() once (UDC 01/05), overwrite the dim.
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
