#!/usr/bin/env python
# coding: utf-8

# Fabric notebook · nb_eso4_gold_dim_udc
# ============================================================================
# ESO4 Gold UDC dimensions (Sales Tax reconciliation). Built from ONE Silver
# source — the user-defined-code values (F0005):
#   • dim_sic   — UDC 01/SC : sic_code   -> sic_description   (docx §6 col 24/25)
#   • dim_state — UDC 00/S  : state_code -> state_name        (jurisdiction name)
# The fact stores the raw FK codes (sic_code, jurisdiction); these dims resolve the
# descriptions in the model (fact.sic_code -> dim_sic, fact.jurisdiction -> dim_state).
# Gold schema: `rpt`, lakehouse: `lh_jde_gold`.
#
# ARCHITECTURE
# ─────────────────────────────────────────────────────────────────────────────
# Batch build. Reads the full F0005 snapshot, runs the two builders once (split by
# UDC system/type), and overwrite-writes each Gold dim. No streaming / CDF.
#
# SOFT DELETES
# ─────────────────────────────────────────────────────────────────────────────
# load_silver_table() strips is_delete = 1 rows (and the audit columns) before use.
#
# FIRST RUN / OVERWRITE
# ─────────────────────────────────────────────────────────────────────────────
# MANUAL_OVERWRITE = True  → drop + rebuild both dims from the full Silver snapshot.
# MANUAL_OVERWRITE = False → build only if a dim is missing; else leave them untouched.
# Design: eso4/docs/ESO4_gold_layer_design.md
# ============================================================================

from pyspark.sql import functions as F
import json, time
from datetime import datetime, timezone

# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------
SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

MANUAL_OVERWRITE = True   # True = drop + rebuild; False = build only if missing

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

F0005 = "f0005_user_defined_code_values"

# UDC selectors (INFERRED system/type — confirm; flagged in design §5, like ESO7 dim_status 40/AT).
SIC_SYS,   SIC_TYPE   = "01", "SC"   # SIC codes  -> dim_sic
STATE_SYS, STATE_TYPE = "00", "S"    # State/Province codes -> dim_state

DIM_SIC   = "dim_sic"
DIM_STATE = "dim_state"

_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def load_silver_table(t):
    """Read a Silver table and strip soft-deleted rows (is_delete = 1) + the audit columns."""
    df = spark.read.table(sname(t))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

# ----------------------------------------------------------------------------
# 2) DIM BUILDERS  (F0005 UDC lookups; natural PK per dim = DRKY WITHIN its system/type)
# ----------------------------------------------------------------------------
def _udc_dim(sys_code, type_code, key_alias, desc_alias):
    f0005 = (load_silver_table(F0005)
             .where((F.trim(F.col("product_code")) == sys_code) &
                    (F.trim(F.col("user_defined_codes")) == type_code)))
    return (f0005.select(F.trim(F.col("user_defined_code")).alias(key_alias),   # DRKY
                         F.trim(F.col("description_001")).alias(desc_alias))     # DRDL01
            .where(F.col(key_alias) != "")
            .dropDuplicates([key_alias]))

def build_dim_sic():
    return _udc_dim(SIC_SYS, SIC_TYPE, "sic_code", "sic_description")

def build_dim_state():
    return _udc_dim(STATE_SYS, STATE_TYPE, "state_code", "state_name")

# ----------------------------------------------------------------------------
# 3) RUN
# ----------------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

_run_start = time.time()
if (MANUAL_OVERWRITE
        or not spark.catalog.tableExists(gname(DIM_SIC))
        or not spark.catalog.tableExists(gname(DIM_STATE))):
    print("== FULL LOAD ==")
    sic   = build_dim_sic()
    state = build_dim_state()
    (sic.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(gname(DIM_SIC)))
    (state.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(gname(DIM_STATE)))
    _rows   = {DIM_SIC: sic.count(), DIM_STATE: state.count()}
    _status = "built"
    print("  {} + {} rows={}".format(DIM_SIC, DIM_STATE, _rows))
else:
    print("== skip — {} + {} exist and MANUAL_OVERWRITE=False ==".format(gname(DIM_SIC), gname(DIM_STATE)))
    _rows, _status = None, "skipped"

_elapsed = round(time.time() - _run_start, 1)
_exit_payload = {
    "status":       _status,
    "tables":       [gname(DIM_SIC), gname(DIM_STATE)],
    "rows":         _rows,
    "elapsed_sec":  _elapsed,
    "end_time_utc": datetime.now(timezone.utc).isoformat(),
}
print("exit payload:", json.dumps(_exit_payload))
notebookutils.notebook.exit(json.dumps(_exit_payload))
