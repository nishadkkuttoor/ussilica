#!/usr/bin/env python
# coding: utf-8

# ## nb_eso1_gold_dim_item
#
# **Gold `dim_item` processor** for Extended Sales Order 1 (Billable v Payable Freight).
# Builds ONE table — `lh_jde_gold.rpt.dim_item` — from the Silver item master (F4101).
# Split out of nb_eso1_gold_streaming so `dim_item` and the fact run as independent jobs
# (own table, own OVERWRITE switch).
#
# ── BUILD (BATCH, structure like ESO4 / ESO5 dim notebooks) ─────────────────────────────
#   • read the full F4101 snapshot, run build_dim_item() ONCE, overwrite the dim.
#   • MANUAL_OVERWRITE = True → drop + rebuild; False → build only if the dim is missing (re-run to refresh).
#   • no CDF / foreachBatch / checkpoints / streams. Result is IDENTICAL to the previous streaming
#     full-load seed (build_dim_item() is the old transform_dim_item() unchanged, dead `restrict_item`
#     scope-filter removed); plain overwrite (no Gold CDF) matches ESO4/ESO5.
#
# Sections:  1) CONFIG   2) DIM BUILDER   3) RUN
# Design: docs/ESO1_gold_layer_design.md


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

# ── refresh / runtime config (BATCH build, like ESO4 / ESO5 dim notebooks) ──
#   MANUAL_OVERWRITE = True  -> full load: drop + rebuild dim_item from the full Silver snapshot.
#   MANUAL_OVERWRITE = False -> build only if the dim is missing (re-run to refresh).
MANUAL_OVERWRITE = True    # ⚠ set back to False after the first successful run

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
                         F.col("uom_weight").alias("uom_weight"))
            .dropDuplicates(["item_number_short"]))


# ----------------------------------------------------------------------------
# 3) RUN
# ----------------------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

# BATCH BUILD — read the full F4101 snapshot, run build_dim_item() once, overwrite the dim.
#   MANUAL_OVERWRITE = True  -> drop + rebuild from the full Silver snapshot.
#   MANUAL_OVERWRITE = False -> build only if the dim is missing (re-run to refresh).
#   Plain overwrite (no Gold CDF) — same execution pattern as ESO4 / ESO5.
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
