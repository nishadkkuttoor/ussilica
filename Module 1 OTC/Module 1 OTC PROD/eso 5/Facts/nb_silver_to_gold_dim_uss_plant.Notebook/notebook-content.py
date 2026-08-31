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
from pyspark.sql import functions as F
import json, time
from datetime import datetime, timezone

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

MANUAL_OVERWRITE = True   # True = drop + rebuild from the full Silver snapshot; False = build only if missing

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

F0005     = "f0005_user_defined_code_values"
UDC_SYS, UDC_TYPE = "55", "UP"   # DRSY='55' AND DRRT='UP'

DIM = "dim_uss_plant"

print(f"ESO5 Gold dim_uss_plant processor (batch build) — target {gname(DIM)}")

# HELPERS
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def load_silver_table(table_name):
    df = spark.read.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# 2) DIM BUILDER
# ----------------------------------------------------------------------------

# DIM transform — dim_uss_plant. Natural PK = the UDC value (DRKY) WITHIN its system/type, so the
# transform filters product_code/user_defined_codes FIRST, then keys on trim(user_defined_code) cast
# to the numeric vendor (TO_NUMBER(rtrim(F0005.drky)) = M.sdvend).
#
# ⚠ KEY TYPE = DOUBLE, NOT long. Direct Lake requires the PK and the related FK to have the SAME physical
# type; the vendor is a JDE numeric that lands as Double. Casting DRKY to long yields Int64, which Direct
# Lake rejects for the relationship ("data types ... are incompatible").
DIM_KEY      = "vendor_number"
DIM_KEY_TYPE = "double"          # MUST match the related FK (Double) — see note above

def build_dim_uss_plant():
    f0005 = (load_silver_table(F0005)
             .where((F.trim(F.col("product_code")) == UDC_SYS) &
                    (F.trim(F.col("user_defined_codes")) == UDC_TYPE)))
    sphd = F.trim(F.col("special_handling_code")).cast("double")
    return (f0005.select(
                F.trim(F.col("user_defined_code")).cast(DIM_KEY_TYPE).alias(DIM_KEY),     # DRKY (numeric)
                F.when((sphd > 1) & (sphd < 9000), F.lit("Y")).otherwise(F.lit("N")).alias("uss_plant_sand"),
                F.when(sphd > 9000, F.lit("TRANSLOAD"))
                 .when((sphd > 1) & (sphd < 9000), F.lit("PLANT"))
                 .otherwise(F.lit("3RDPARTY")).alias("shipped_from"),
                F.trim(F.col("special_handling_code")).alias("lofa_mcu"))                 # LOFAPLANTMCU (raw DRSPHD)
            .where(F.col(DIM_KEY).isNotNull())
            .dropDuplicates([DIM_KEY]))

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

# BATCH BUILD — read the full F0005 snapshot, run build_dim_uss_plant() once, overwrite the dim.
_run_start = time.time()
if MANUAL_OVERWRITE or not spark.catalog.tableExists(gname(DIM)):
    print("== FULL LOAD ==")
    new = build_dim_uss_plant()
    (new.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(gname(DIM)))
    _rows   = new.count()
    _status = "built"
    print("  {} rows={}".format(gname(DIM), _rows))
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
