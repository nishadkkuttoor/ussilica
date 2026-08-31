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
# META     },
# META     "environment": {
# META       "environmentId": "e8fc6e8d-6c62-a450-4c29-1771fea37e17",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

#!/usr/bin/env python
# coding: utf-8

# ## nb_silver_to_gold_dim_uss_plant
#
# null

# In[1]:


# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------

# CONFIG + CONSTANTS
from pyspark.sql import functions as F
import re, time

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

F0005     = "f0005_user_defined_code_values"
UDC_SYS, UDC_TYPE = "55", "UP"   # DRSY='55' AND DRRT='UP'

DIM = "dim_uss_plant"

# ── CDF state ─────────────────────────────────────────────────────────────────
# 2026-08-14 — was a plain overwrite on every run, driven by MANUAL_OVERWRITE.
# Now a Change Data Feed stream reads F0005 and the dim is rebuilt ONLY when
# that stream delivers a change to UDC 55/UP. MANUAL_OVERWRITE is gone: the
# checkpoint IS the state, and deleting it is the only way to force a full load —
#     mssparkutils.fs.rm("Files/checkpoints/dimensions/dim_uss_plant", True)
# A full load also happens by itself when the gold table is missing.
#
# ANY change to 55/UP rebuilds the whole dim rather than merging row by row.
# The UDC list is a few hundred rows, so a rebuild costs one short scan and
# cannot leave a stale row or a missed delete behind.
#
# NOTE: nb_silver_to_gold_dim_lookups.py also streams F0005, for the nine plain
# UDC lookup dims. That is fine — the two notebooks keep separate checkpoints
# and write different gold tables. This dim stays separate because it is not a
# plain code→description lookup: it derives uss_plant_sand and shipped_from
# from special_handling_code.
CKPT_ROOT  = "Files/checkpoints/dimensions/dim_uss_plant"
QUERY_NAME = "dim_uss_plant__f0005"

# Delta may compact its log between runs, removing the snapshot entry that CDF
# streaming uses to verify column mapping. Without this flag the SECOND run
# fails with DELTA_STREAMING_CHECK_COLUMN_MAPPING_NO_SNAPSHOT. Safe here
# because bronze→silver never renames a column.
spark.conf.set(
    "spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled",
    "true",
)

print(f"ESO5 Gold dim_uss_plant processor (CDF build) — target {gname(DIM)}")

# HELPERS
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def load_silver_table(table_name):
    df = spark.read.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])


def checkpoint_exists(path):
    try:
        return bool(mssparkutils.fs.ls(path))
    except Exception:
        return False


def remove_checkpoint(path):
    try:
        mssparkutils.fs.rm(path, True)
        print("  removed {}".format(path))
    except Exception as e:
        print("  could not remove {} : {}".format(path, e))


def current_version(src):
    return spark.sql("DESCRIBE HISTORY {}".format(sname(src))).select(F.max("version")).first()[0]


# In[2]:


# ----------------------------------------------------------------------------
# 2) DIM BUILDER
# ----------------------------------------------------------------------------

# DIM transform — dim_uss_plant. Natural PK = the UDC value (DRKY) WITHIN its system/type, so the
# transform filters product_code/user_defined_codes FIRST, then keys on trim(user_defined_code) cast
# to the numeric vendor (TO_NUMBER(rtrim(F0005.drky)) = M.sdvend).
#
# KEY TYPE = DOUBLE, NOT long. Direct Lake requires the PK and the related FK to have the SAME physical
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


# Result of this run, printed at the bottom.
#   built      the dim was rebuilt (first load, or 55/UP changed)
#   no_change  nothing new in 55/UP — the existing dim is already current
_result = {"status": "no_change", "rows": None}


def write_gold():
    """Overwrite the gold dim from the current F0005 snapshot."""
    new = build_dim_uss_plant()
    (new.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .option("delta.enableChangeDataFeed", "true")
       .saveAsTable(gname(DIM)))
    _result["rows"]   = spark.read.table(gname(DIM)).count()
    _result["status"] = "built"
    print("  {} rows={}".format(gname(DIM), _result["rows"]))


# In[3]:


# ----------------------------------------------------------------------------
# 3) STREAM HANDLER
#
# F0005 carries EVERY UDC type, so the batch is filtered to 55/UP with the same
# predicate build_dim_uss_plant() uses. Without that, a change to any unrelated
# code list — and there are many — would rebuild this dim for nothing.
#
# Rebuild at most once per run: AvailableNow can split a backlog into several
# batches, and rebuilding on each would repeat identical work, because the
# rebuild reads the CURRENT silver snapshot — which already contains every row
# those later batches carry.
# ----------------------------------------------------------------------------
CHANGE_TYPES = ["insert", "update_postimage", "delete"]


def handle_f0005(batch_df, batch_id):
    changes = batch_df.filter(
        F.col("_change_type").isin(CHANGE_TYPES)
        & (F.trim(F.col("product_code")) == UDC_SYS)
        & (F.trim(F.col("user_defined_codes")) == UDC_TYPE)
    )
    if INIT_VERSION >= 0:
        changes = changes.filter(F.col("_commit_version") > INIT_VERSION)

    if changes.isEmpty():
        return

    if _result["status"] == "built":
        print("  [F0005 {}/{}] batch {} — already rebuilt this run, skipping".format(
            UDC_SYS, UDC_TYPE, batch_id))
        return

    print("  [F0005 {}/{}] changes in batch {} — rebuilding".format(UDC_SYS, UDC_TYPE, batch_id))
    write_gold()


# ----------------------------------------------------------------------------
# 4) STREAM DRAIN
#
# Three outcomes, and telling them apart is the whole job:
#
#   no new data    KEEP the checkpoint — clearing it would make the next run
#                  think it had never run and full-load everything.
#   unreadable     the silver table was DROP+CREATE'd, or its log was compacted
#                  past our stored offset. Clear the checkpoint, full-load next run.
#   anything else  re-raise. An unknown failure must not be swallowed.
#
# "non-existent version N" is raised for two OPPOSITE reasons, so the message
# text alone cannot be trusted — compare N against the table instead:
#   N ahead of the table → nothing new was written (delta only writes its own
#       internal checkpoint every 10 commits, so a quiet table lands here
#       instead of on DELTA_INVALID_CDC_RANGE). The checkpoint is VALID.
#   N behind the table   → the log really was compacted. The checkpoint is dead.
# ----------------------------------------------------------------------------
NO_NEW_DATA = "DELTA_INVALID_CDC_RANGE"
UNREADABLE_OFFSET = (
    "DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE",
    "non-existent version",
)
VERSION_IN_MSG = re.compile(r"non-existent version[^0-9]*([0-9]+)")


def asks_for_future_version(msg, src):
    match = VERSION_IN_MSG.search(msg)
    if match is None:
        return False
    try:
        return int(match.group(1)) > current_version(src)
    except Exception:
        return False


def drain_stream(src, checkpoint, handler, query_name, init_version):
    # startingVersion is set ONLY when starting fresh — NEVER on a resume.
    #
    # Fabric does NOT ignore startingVersion when a checkpoint exists, whatever
    # the docs say. Passing it on a resume makes delta use it as the batch START
    # while the checkpoint supplies the END, so the range comes out backwards:
    #
    #   [DELTA_INVALID_CDC_RANGE] CDC range from start 4377 to end 4369 was invalid.
    #
    # The stream then dies before reading anything, and the handler below reads
    # that failure as "no new data" — so the dim silently stops updating while
    # every run still reports success. This is the bug found in
    # nb_otc_facts_v3.py on 2026-08-13, where it had suppressed every
    # incremental read since the notebook was written.
    reader = spark.readStream.format("delta").option("readChangeFeed", "true")
    if not checkpoint_exists(checkpoint):
        # First start after a full load. init_version is the silver version
        # captured just before the rebuild, so the stream picks up only what
        # lands after the snapshot we already wrote.
        reader = reader.option("startingVersion", init_version)

    query = (
        reader.table(sname(src))
        .writeStream
        .foreachBatch(handler)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .queryName(query_name)
        .start()
    )

    try:
        query.awaitTermination()
        print("  {} drained".format(query_name))
    except Exception as e:
        msg = str(e)
        if NO_NEW_DATA in msg or asks_for_future_version(msg, src):
            # Print the raw message too. This branch is a JUDGEMENT about an
            # exception, and hiding the evidence makes a wrong judgement invisible.
            print("  {} — no new data, checkpoint kept".format(query_name))
            print("      reason: {}".format(msg[:300].replace("\n", " ")))
        elif any(m in msg for m in UNREADABLE_OFFSET):
            print("  {} — offset unreadable, clearing checkpoint".format(query_name))
            remove_checkpoint(checkpoint)
        else:
            raise


# In[4]:


# ----------------------------------------------------------------------------
# 5) RUN
# ----------------------------------------------------------------------------

_run_start = time.time()

# Full load when there is nothing to resume from: no checkpoint, or the gold
# table does not exist yet.
if not checkpoint_exists(CKPT_ROOT) or not spark.catalog.tableExists(gname(DIM)):
    print("== FULL LOAD ==")
    remove_checkpoint(CKPT_ROOT)
    # Capture the silver version BEFORE building, so the stream starts from the
    # snapshot we are about to write and does not replay it.
    INIT_VERSION = current_version(F0005)
    write_gold()          # sets _result — the full load IS this run's rebuild
    print("  init version {}".format(INIT_VERSION))
else:
    # -1 means "no floor" — the checkpoint already knows where the stream
    # stopped, so the handler needs no version filter.
    INIT_VERSION = -1
    print("== RESUME ==")

drain_stream(F0005, CKPT_ROOT, handle_f0005, QUERY_NAME, INIT_VERSION)

if _result["status"] == "no_change":
    print("== no change in {} {}/{} — {} ".format(
        F0005, UDC_SYS, UDC_TYPE, gname(DIM)))

print("== run complete in {}s — status={} rows={} ==".format(
    round(time.time() - _run_start, 1), _result["status"], _result["rows"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
