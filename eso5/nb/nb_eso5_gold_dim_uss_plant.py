#!/usr/bin/env python
# coding: utf-8

# ## nb_eso5_gold_dim_uss_plant
#
# **Gold `dim_uss_plant` processor** — builds ONE table, `lh_jde_gold.rpt.dim_uss_plant`, from the Silver
# user-defined-code values (F0005), UDC system **55 / type UP**.
#
# Keyed by `vendor_number` (F0005 DRKY, numeric). Carries the three USS/plant flags derived from DRSPHD:
#   uss_plant_sand  = USSSAND        : 1 < DRSPHD < 9000 -> 'Y' else 'N'
#   shipped_from    = PLANTTRANSLOAD : DRSPHD > 9000 -> 'TRANSLOAD'; 1 < x < 9000 -> 'PLANT'; else '3RDPARTY'
#   lofa_mcu        = LOFAPLANTMCU   : raw DRSPHD (special-handling code)
#
# BATCH build — one full F0005 snapshot -> UDC-filtered Type-1 dim:
#   • read the full F0005 snapshot, run build_dim_uss_plant() ONCE, overwrite the dim.
#   • MANUAL_OVERWRITE = True -> drop + rebuild; False -> build only if the dim is missing (re-run to refresh).
#   • no CDF / checkpoints / streams.
# UDC 55/UP filter that pins every mapping:
#     select F0005.drsphd from prodctl.F0005
#     where  F0005.drsy = '55' and F0005.drrt = 'UP'
#            and TO_Number(rtrim(F0005.drky, ' ')) = M.sdvend
#     DRSY  = '55'                        -> product_code          (UDC system)
#     DRRT  = 'UP'                        -> user_defined_codes    (UDC type)
#     DRKY  = numeric vendor number       -> user_defined_code     -> the dim PK `vendor_number`
#                                            (trim + numeric cast, mirroring TO_Number(rtrim(...)))
#     DRSPHD= the plant/business-unit MCU -> special_handling_code -> `lofa_mcu`

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
