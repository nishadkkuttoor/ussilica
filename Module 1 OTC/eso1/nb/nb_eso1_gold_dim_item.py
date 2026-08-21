#!/usr/bin/env python
# coding: utf-8

# ## nb_eso1_gold_dim_item
#
# **Gold `dim_item` processor** for Extended Sales Order 1 (Billable v Payable Freight).
# Builds ONE table — `lh_jde_gold.rpt.dim_item` — from the Silver item master (F4101).
# Runs as an independent job (own table, own overwrite switch), separate from the fact.
#
# ── BUILD (BATCH) ─────────────────────────────
#   • read the full F4101 snapshot, run build_dim_item() ONCE, overwrite the dim.
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

MANUAL_OVERWRITE = True    # True: drop + rebuild from the full snapshot. ⚠ set False after the first successful run

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

# ── Silver sources ────────────────────────────────────────────────────────────
F4101     = "f4101_item_master"

# ── Gold target BUILT here (new, eso1) ─────────────────────────────────────────
DIM         = "dim_item"

print(f"ESO1 Gold dim_item processor (batch build) — target {gname(DIM)}")


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

# DIM transform — dim_item (F4101, natural PK)
def build_dim_item():
    f4101 = load_silver_table(F4101)
    # business columns only — no audit columns
    return (f4101.select(F.col("identifier_short_item").alias("item_number_short"),
                         F.col("description_line_01").alias("item_name"),
                         F.col("segment_04").alias("item_segment_04"),      # IMSEG4
                         F.col("uom_weight"))
            .dropDuplicates(["item_number_short"]))


# ----------------------------------------------------------------------------
# 3) RUN
# ----------------------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

# BATCH BUILD — read the full F4101 snapshot, run build_dim_item() once, overwrite the dim.
_run_start = time.time()
if MANUAL_OVERWRITE or not spark.catalog.tableExists(gname(DIM)):
    print("== FULL LOAD ==")
    new = build_dim_item()
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
