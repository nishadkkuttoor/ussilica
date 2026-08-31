# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bed869e4-f15b-4cc1-9368-c7a9b3e08a83",
# META       "default_lakehouse_name": "lh_jde_gold",
# META       "default_lakehouse_workspace_id": "9ea13355-c802-4ca5-883f-e5dbf8ecc720",
# META       "known_lakehouses": [
# META         {
# META           "id": "bed869e4-f15b-4cc1-9368-c7a9b3e08a83"
# META         },
# META         {
# META           "id": "915ea8b7-e01a-4182-b41a-c283df48a086"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

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
F4211  = "f4211_sales_order_detail_file"      # SDDOCO source (grain of distinct order numbers)
F42119 = "f42119_sales_order_history_file"    # optional history union (closed/purged orders)

# ── Gold target BUILT here (new, rpt) ─────────────────────────────────────────
DIM         = "dim_order_number"

# MAK_EXPORT_ORDERS — the hand-picked SDDOCO whitelist for Ottowa/Mak Export Orders (70 export orders).
# ⚠ Snapshot: refresh this list + re-run when the report's order set changes.
MAK_EXPORT_ORDERS = [
    1593550, 1593549, 1581179, 1581165, 1581173, 1581184, 1581161, 1596420,
    1594410, 1596628, 1593269, 1593272, 1595918, 1557959, 1557961, 1577127,
    1593196, 1593200, 1595505, 1593731, 1594581, 1593732, 1594622, 1593733,
    1595678, 1593736, 1593734, 1593735, 1570618, 1571523, 1571525, 1570619,
    1571520, 1570615, 1571522, 1570617, 1590914, 1583718, 1595704, 1595696,
    1594415, 1594414, 1595402, 1596566, 1590559, 1590562, 1590564, 1590569,
    1596416, 1596417, 1596418, 1594405, 1594406, 1595342, 1596583, 1593829,
    1595675, 1597223, 1596647, 1593945, 1595602, 1595603, 1595604, 1596951,
    1561922, 1561921, 1592418, 1582249, 1587441, 1595294,
]

# One flag COLUMN per whitelist-driven report → (mode, order_list). mode: 'include' = 'Included' when the
# order IS in the list (that report's SDDOCO IN (...) rule). Column name encodes the report so authors
# pick the right one. Add a new entry here for each future order-whitelist report.
ORDER_FILTERS = {
    "mak_export_filter": ("include", MAK_EXPORT_ORDERS),   # Mak Export Orders (SDDOCO IN the 70 export orders)
}

print(f"ESO1 Gold dim_order_number processor (batch build) — target {gname(DIM)}")


# HELPERS
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def load_silver_table(table_name):
    df = spark.read.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

def _load_optional(table_name):
    """Load a Silver table only if it exists; else warn and return None (used for F42119 history)."""
    try:
        if spark.catalog.tableExists(sname(table_name)):
            return load_silver_table(table_name)
    except Exception as e:
        print("  ⚠ optional source check failed for {}: {}".format(sname(table_name), e))
    print("  ⚠ optional source not found, skipping union: {}".format(sname(table_name)))
    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# 2) DIM BUILDER
# ----------------------------------------------------------------------------

# DIM transform — distinct order_number (SDDOCO = document_order_invoice_e) + one 'Included'/'Excluded'
# flag column per report. The key matches the freight fact's order_number for the relationship.
def build_dim():
    ordn = load_silver_table(F4211).select(F.col("document_order_invoice_e"))
    hist = _load_optional(F42119)
    if hist is not None:
        ordn = ordn.unionByName(hist.select("document_order_invoice_e"), allowMissingColumns=True)
    df = (ordn.select(F.col("document_order_invoice_e").alias("order_number"))
          .dropDuplicates(["order_number"]))
    for _col, (_mode, _orders) in ORDER_FILTERS.items():
        _in_list = F.col("order_number").isin(_orders)
        # include: 'Included' when in list; exclude: 'Included' when NOT in list
        _flag = (F.when(_in_list, F.lit("Included")).otherwise(F.lit("Excluded")) if _mode == "include"
                 else F.when(_in_list, F.lit("Excluded")).otherwise(F.lit("Included")))
        df = df.withColumn(_col, _flag)
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# 3) RUN
# ----------------------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

# BATCH BUILD — read the full F4211 (∪ F42119) snapshot, run build_dim() once, overwrite the dim.
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
