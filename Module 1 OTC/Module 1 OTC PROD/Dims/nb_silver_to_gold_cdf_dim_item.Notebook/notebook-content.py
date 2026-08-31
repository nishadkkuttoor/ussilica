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

# ## nb_silver_to_gold_dim_item
#
# New notebook

# In[1]:


# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------

# CONFIG + CONSTANTS
import re, time
from pyspark.sql import functions as F

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"          # Silver schema — static batch snapshots
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

# ── Silver sources ────────────────────────────────────────────────────────────
F4101     = "f4101_item_master"

# ── Gold target BUILT  ─────────────────────────────────────────
DIM         = "dim_item"

CKPT_ROOT  = "Files/checkpoints/dimensions/dim_item"
QUERY_NAME = "dim_item__f4101"

spark.conf.set(
    "spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled",
    "true",
)


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


# ----------------------------------------------------------------------------
# 2) DIM BUILDER
# ----------------------------------------------------------------------------

# DIM transform — dim_item (F4101, natural PK)
def build_dim_item():
    f4101 = load_silver_table(F4101)
    # business columns only — no audit columns
    return (f4101.select(F.col("identifier_short_item").alias("item_number_short"),
                         F.col("description_line_01").alias("item_name"),
                         F.col("segment_04").alias("item_segment_04"),      # IMSEG4 — moved off the freight fact
                         F.col("uom_weight"))
            .dropDuplicates(["item_number_short"]))


# Result of this run, printed at the bottom.
#   built      the dim was rebuilt (first load, or F4101 changed)
#   no_change  F4101 had nothing new — the existing dim is already current
_result = {"status": "no_change", "rows": None}


def write_gold():
    """Overwrite the gold dim from the current F4101 snapshot."""
    new = build_dim_item()
    (new.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .option("delta.enableChangeDataFeed", "true")
       .saveAsTable(gname(DIM)))
    _result["rows"]   = spark.read.table(gname(DIM)).count()
    _result["status"] = "built"
    print("  {} rows={:,}".format(gname(DIM), _result["rows"]))


# In[3]:


# ----------------------------------------------------------------------------
# 3) STREAM HANDLER
#
# Rebuild at most once per run: AvailableNow can split a backlog into several
# batches, and rebuilding on each would repeat identical work, because the
# rebuild reads the CURRENT silver snapshot — which already contains every row
# those later batches carry.
# ----------------------------------------------------------------------------
CHANGE_TYPES = ["insert", "update_postimage", "delete"]


def handle_f4101(batch_df, batch_id):
    changes = batch_df.filter(F.col("_change_type").isin(CHANGE_TYPES))
    if INIT_VERSION >= 0:
        changes = changes.filter(F.col("_commit_version") > INIT_VERSION)

    if changes.isEmpty():
        return

    if _result["status"] == "built":
        print("  [F4101] batch {} — already rebuilt this run, skipping".format(batch_id))
        return

    print("  [F4101] changes in batch {} — rebuilding".format(batch_id))
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
    # every run still reports success.
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

spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

for _q in list(spark.streams.active):
    if _q.name == QUERY_NAME:
        _q.stop()
        print("Stopped leftover stream: {}".format(QUERY_NAME))

_run_start = time.time()

# Full load when there is nothing to resume from: no checkpoint, or the gold
# table does not exist yet.
if not checkpoint_exists(CKPT_ROOT) or not spark.catalog.tableExists(gname(DIM)):
    print("== FULL LOAD ==")
    remove_checkpoint(CKPT_ROOT)
    # Capture the silver version BEFORE building, so the stream starts from the
    # snapshot we are about to write and does not replay it.
    INIT_VERSION = current_version(F4101)
    write_gold()          # sets _result — the full load IS this run's rebuild
    print("  init version {}".format(INIT_VERSION))
else:
    # -1 means "no floor" — the checkpoint already knows where the stream
    # stopped, so the handler needs no version filter.
    INIT_VERSION = -1
    print("== RESUME ==")

drain_stream(F4101, CKPT_ROOT, handle_f4101, QUERY_NAME, INIT_VERSION)

if _result["status"] == "no_change":
    print("== no change in {} — {} left as is ==".format(F4101, gname(DIM)))

print("== run complete in {}s — status={} rows={} ==".format(
    round(time.time() - _run_start, 1), _result["status"], _result["rows"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
