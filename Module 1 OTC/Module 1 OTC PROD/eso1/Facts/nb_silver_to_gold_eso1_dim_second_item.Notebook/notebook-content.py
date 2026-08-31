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
F4211  = "f4211_sales_order_detail_file"      # SDLITM source (grain of distinct second item numbers)
F42119 = "f42119_sales_order_history_file"    # optional history union (closed/purged lines)

# ── Gold target BUILT here (new, rpt) ─────────────────────────────────────────
DIM         = "dim_second_item"

# GROUND_ITEMS — the 145 "ground" SDLITM codes. Ground variations filter item IN this list; Whole
# Grain variations filter item NOT IN this list (whole grain = the complement). Codes compared trimmed.
GROUND_ITEMS = [
    "06069B00000", "06069P30154", "06069P30160", "06069P84101", "06069P91101", "06069P9B101",
    "06113B00000", "06113P84101", "06113P91101", "06115B00000", "06115P91101", "06119B00000",
    "06119P58002", "06119P58102", "06119P80102", "06119P90101", "06119P91101", "06123B00000",
    "06143B00000", "06143P80102", "06143P84101", "06143P91101", "07063B00000", "07123B00000",
    "07150R00000", "08101F00000", "08111B00000", "08113B00000", "08117P91101", "08119B00000",
    "08119P10170", "08122B00000", "08123B00000", "08123P10101", "08131B00000", "08143B00000",
    "08143P10170", "08144B00000", "15061B00000", "15061F00000", "15063B00000", "15111B00000",
    "15111F00000", "15111P30149", "15111P30163", "15111P30170", "15111P80101", "15111P80102",
    "15111P91101", "15111PC7101", "15114F00000", "15114P84101", "15115B00000", "15115F00000",
    "15115P08101", "15115P30142", "15115P30156", "15115P30163", "15115P30170", "15115P80101",
    "15115P80102", "15115P83101", "15115P91101", "15119B00000", "15119F00000", "15119P30142",
    "15119P30149", "15119P30156", "15119P30170", "15119P78102", "15119P80102", "15119P87101",
    "15119P91101", "15119P93101", "15131B00000", "15131F00000", "15131P30149", "15131P30156",
    "15131P30163", "15131P30170", "15131P80102", "15131P87101", "15131P91101", "15131PC1101",
    "15143B00000", "15143P30142", "15143P30149", "15143P30156", "15143P30170", "15143P80102",
    "15143P84101", "15143P91101", "156745", "17061F00000", "17063B00000", "17112B00000",
    "17112F00000", "17114F00000", "17116F00000", "17117F00000", "17117P91101", "17119B00000",
    "17119F00000", "17119P91101", "17131B00000", "17131F00000", "17142B00000", "17143B00000",
    "17143F00000", "17144B00000", "17144F00000", "34123B00000", "50061B00000", "50061P30160",
    "50061P50130", "50063B00000", "50063P30160", "50063P50130", "50064B00000", "50065B00000",
    "50065P30160", "50065P50130", "50066B00000", "50066P30160", "50066P51130", "50067B00000",
    "50067P30160", "50067P51130", "50067P52130", "50068B00000", "50068P30160", "50068P52130",
    "50069B00000", "50069P30160", "50069P52130", "50069P91101", "574772", "60280",
    "75531", "75572", "75614", "75630", "90061B00000", "93123P10101",
    "97064B00000",
]

# ASTM_ITEMS — the 3 SDLITM codes for the ASTM variation (disjoint from GROUND_ITEMS).
ASTM_ITEMS = ["50081P18150", "50084P20150", "50087P19150"]

# One flag COLUMN per Ottawa variation → (mode, item_list). mode: 'include' = 'Included' when the item
# IS in the list (that variation's SDLITM IN (...) rule); 'exclude' = 'Included' when the item is NOT
# in the list (that variation's NOT SDLITM IN (...) rule). Column name encodes the variation so report
# authors pick the right one. (Note: the 3 Whole Grain variations share identical item-list logic — the
# rail/truck + packaged/bulk differences are SDMOT/SDSRP3 fact page filters, not item lists — but each
# gets its own named column for clarity; likewise the 2 Ground variations.)
VARIATION_FILTERS = {
    "whole_grain_truck_packaged_filter": ("exclude", GROUND_ITEMS),   # Ottowa - Whole Grain Truck - Packaged (NOT IN ground)
    "whole_grain_truck_bulk_filter":     ("exclude", GROUND_ITEMS),   # Ottowa - Whole Grain Truck - Bulk     (NOT IN ground)
    "whole_grain_rail_bulk_filter":      ("exclude", GROUND_ITEMS),   # Ottowa - Whole Grain Rail  - Bulk     (NOT IN ground)
    "ground_packaged_filter":            ("include", GROUND_ITEMS),   # Ottowa - Ground - Packaged            (IN ground)
    "ground_bulk_filter":                ("include", GROUND_ITEMS),   # Ottowa - Ground - Bulk                (IN ground)
    "astm_packaged_filter":              ("include", ASTM_ITEMS),     # Ottowa - ASTM - Packaged              (IN astm)
}

print(f"ESO1 Gold dim_second_item processor (batch build) — target {gname(DIM)}")


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

# DIM transform — distinct second_item_number (SDLITM) + one 'Included'/'Excluded' flag column per
# Ottawa variation. The stored key is trimmed to match the freight fact's second_item_number for the
# relationship; each flag is computed on the trimmed value against that variation's item list.
def build_dim():
    lit = load_silver_table(F4211).select(F.col("identifier_second_item"))
    hist = _load_optional(F42119)
    if hist is not None:
        # F42119 snake-names SDLITM as `identifier_2nd_item` — rename to match before the union.
        if "identifier_2nd_item" in hist.columns and "identifier_second_item" not in hist.columns:
            hist = hist.withColumnRenamed("identifier_2nd_item", "identifier_second_item")
        lit = lit.unionByName(hist.select("identifier_second_item"), allowMissingColumns=True)
    df = (lit.select(F.trim(F.col("identifier_second_item")).alias("second_item_number"))
          .dropDuplicates(["second_item_number"]))
    for _col, (_mode, _items) in VARIATION_FILTERS.items():
        _in_list = F.col("second_item_number").isin(_items)
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
