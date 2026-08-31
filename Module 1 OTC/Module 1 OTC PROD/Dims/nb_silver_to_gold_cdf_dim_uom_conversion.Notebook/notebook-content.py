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
# META           "id": "915ea8b7-e01a-4182-b41a-c283df48a086"
# META         },
# META         {
# META           "id": "bed869e4-f15b-4cc1-9368-c7a9b3e08a83"
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

# ## nb_silver_to_gold_dim_uom_conversion
#
# New notebook

# In[2]:


#!/usr/bin/env python
# coding: utf-8

# Fabric notebook · nb_silver_to_gold_dim_uom_conversion
# ============================================================================
# Builds and keeps live: lh_jde_gold.rpt.dim_uom_conversion
# Source:     lh_jde_silver.jde.f41003_unit_of_measure_standard_conversion
# Checkpoint: Files/checkpoints/dimensions/dim_uom_conversion
#
# dim_uom_conversion stores standard UOM→TN conversion factors (Tier B
# fallback in the Total Tons DAX measure via RELATED).
#
# UOM conversion tiers:
#   Tier 0 — uom_as_input = 'TN'                    → factor = 1.0  (ETL)
#   Tier A — F41002 item-specific lookup             → stored on fact (ETL)
#   Tier B — this dim (F41003 standard factors)      → DAX RELATED()
#   Fallback — literal 1.0                           → DAX fallback
#
# Bidirectional: both fwd (related_uom=TN) and rev (uom=TN) rows are folded
# into a single from_uom key.  Duplicate from_uom values are dropped.
#
# The key is from_uom ALONE — this table is the generic, item-independent
# fallback.  The per-item factors live in dim_uom_conversion_item (F41002),
# keyed on item_uom_key.
#
# ── FORCING A FULL LOAD ──────────────────────────────────────────────────────
#     mssparkutils.fs.rm("Files/checkpoints/dimensions/dim_uom_conversion", True)
# The checkpoint IS the state.  There is no MANUAL_OVERWRITE flag and no
# _init_versions.json file — both are gone.  A full load also happens by itself
# when the gold table is missing, which is how a rebuilt dim comes back.
#
# ── HOW IT RUNS ──────────────────────────────────────────────────────────────
# Trigger.AvailableNow — drains all pending CDF changes, then exits.  Schedule
# it from a pipeline alongside the other dim notebooks.
#
# ANY change rebuilds the whole dim from the silver snapshot, instead of
# merging row by row.  Two reasons, and the second one is the important one:
#
#   1. F41003 is a JDE setup table — a few hundred rows, changed a handful of
#      times a year.  A rebuild costs one short scan.
#   2. Because of the fwd/rev fold, ONE gold row can be produced by EITHER a
#      fwd source row or a rev one.  The old MERGE had to collect delete keys
#      from four different frames — fwd deletes, rev deletes, and soft deletes
#      read twice — and any key it missed would leave a wrong factor in gold
#      forever, silently, since nothing downstream can tell a stale factor from
#      a correct one.  A rebuild cannot be wrong.
#
# That replaces the whole upsert/delete MERGE block, which needed a second copy
# of the fwd/rev logic to do the same job on a table this small.
# ============================================================================

import re

from pyspark.sql import functions as F

# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------
SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

F41003   = "f41003_unit_of_measure_standard_conversion"
DIM_NAME = "dim_uom_conversion"

CKPT_ROOT  = "Files/checkpoints/dimensions/dim_uom_conversion"
QUERY_NAME = "dim__f41003"

# Delta may compact its log between runs, removing the snapshot entry that CDF
# streaming uses to verify column mapping.  Without this flag the SECOND run
# fails with DELTA_STREAMING_CHECK_COLUMN_MAPPING_NO_SNAPSHOT.  Safe here
# because bronze→silver never renames a column.
spark.conf.set(
    "spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled",
    "true",
)

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

def drop_deleted(df):
    if "is_delete" in df.columns:
        return df.filter(F.col("is_delete").isNull() | (F.col("is_delete") != 1))
    return df

# ----------------------------------------------------------------------------
# 2) BUILDER
# ----------------------------------------------------------------------------
def build_dim_uom_conversion():
    _raw = drop_deleted(spark.read.table(sname(F41003)))
    fwd = (_raw.filter((F.trim(F.col("related_uom")) == "TN") & (F.col("conversion_factor") != 0))
               .select(F.trim(F.col("uom")).alias("from_uom"),
                       F.col("conversion_factor").cast("double").alias("std_factor")))
    rev = (_raw.filter((F.trim(F.col("uom")) == "TN") & (F.col("conversion_factor") != 0))
               .select(F.trim(F.col("related_uom")).alias("from_uom"),
                       (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("std_factor")))
    return fwd.unionByName(rev).dropDuplicates(["from_uom"])

def write_gold():
    """Overwrite the gold dim from the current silver snapshot."""
    fq = gname(DIM_NAME)
    (build_dim_uom_conversion().write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").option("delta.enableChangeDataFeed", "true")
       .saveAsTable(fq))
    print("  built {} rows={}".format(DIM_NAME, spark.read.table(fq).count()))

# ----------------------------------------------------------------------------
# 3) HELPERS
# ----------------------------------------------------------------------------
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

# ----------------------------------------------------------------------------
# 4) STREAM HANDLER
#
# Rebuild at most once per run: AvailableNow can split a backlog into several
# batches, and rebuilding on each would repeat identical work, because the
# rebuild reads the CURRENT silver snapshot — which already contains every row
# those later batches carry.
# ----------------------------------------------------------------------------
CHANGE_TYPES = ["insert", "update_postimage", "delete"]

_already_rebuilt = {"done": False}

def handle_f41003(batch_df, batch_id):
    changes = batch_df.filter(F.col("_change_type").isin(CHANGE_TYPES))
    if INIT_VERSION >= 0:
        changes = changes.filter(F.col("_commit_version") > INIT_VERSION)

    if changes.isEmpty():
        return

    if _already_rebuilt["done"]:
        print("  [F41003] batch {} — already rebuilt this run, skipping".format(batch_id))
        return

    print("  [F41003] changes in batch {} — rebuilding".format(batch_id))
    write_gold()
    _already_rebuilt["done"] = True

# ----------------------------------------------------------------------------
# 5) STREAM DRAIN
#
# Three outcomes, and telling them apart is the whole job:
#
#   no new data    normal for a setup table like F41003.  KEEP the checkpoint —
#                  clearing it would make the next run think it had never run
#                  and full-load everything.
#   unreadable     the silver table was DROP+CREATE'd, or its log was compacted
#                  past our stored offset.  Clear the checkpoint, full-load next run.
#   anything else  re-raise.  An unknown failure must not be swallowed.
#
# "non-existent version N" is raised for two OPPOSITE reasons, so the message
# text alone cannot be trusted — compare N against the table instead:
#   N ahead of the table → nothing new was written (delta only writes its own
#       internal checkpoint every 10 commits, so a near-static table lands here
#       instead of on DELTA_INVALID_CDC_RANGE).  The checkpoint is VALID.
#   N behind the table   → the log really was compacted.  The checkpoint is dead.
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
    # the docs say.  Passing it on a resume makes delta use it as the batch START
    # while the checkpoint supplies the END, so the range comes out backwards:
    #
    #   [DELTA_INVALID_CDC_RANGE] CDC range from start 4377 to end 4369 was invalid.
    #
    # The stream then dies before reading anything, and the handler below reads
    # that failure as "no new data" — so the dim silently stops updating while
    # every run still reports success.  This is the bug found in
    # nb_otc_facts_v3.py on 2026-08-13, where it had suppressed every
    # incremental read since the notebook was written.
    reader = spark.readStream.format("delta").option("readChangeFeed", "true")
    if not checkpoint_exists(checkpoint):
        # First start after a full load.  init_version is the silver version
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
            # Print the raw message too.  This branch is a JUDGEMENT about an
            # exception, and hiding the evidence makes a wrong judgement invisible.
            print("  {} — no new data, checkpoint kept".format(query_name))
            print("      reason: {}".format(msg[:300].replace("\n", " ")))
        elif any(m in msg for m in UNREADABLE_OFFSET):
            print("  {} — offset unreadable, clearing checkpoint".format(query_name))
            remove_checkpoint(checkpoint)
        else:
            raise

# ----------------------------------------------------------------------------
# 6) RUN
# ----------------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

for _q in list(spark.streams.active):
    if _q.name == QUERY_NAME:
        _q.stop()
        print("Stopped leftover stream: {}".format(QUERY_NAME))

# Full load when there is nothing to resume from: no checkpoint, or the gold
# table does not exist yet.
if not checkpoint_exists(CKPT_ROOT) or not spark.catalog.tableExists(gname(DIM_NAME)):
    print("== FULL LOAD ==")
    if checkpoint_exists(CKPT_ROOT):
        remove_checkpoint(CKPT_ROOT)
    # Capture the silver version BEFORE building, so the stream starts from the
    # snapshot we are about to write and does not replay it.
    INIT_VERSION = current_version(F41003)
    write_gold()
    _already_rebuilt["done"] = True   # the full load IS this run's rebuild
    print("  init version {}".format(INIT_VERSION))
else:
    # -1 means "no floor" — the checkpoint already knows where the stream
    # stopped, so the handler needs no version filter.
    INIT_VERSION = -1
    print("== RESUME ==")

drain_stream(F41003, CKPT_ROOT, handle_f41003, QUERY_NAME, INIT_VERSION)

print("== batch run complete ==")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
