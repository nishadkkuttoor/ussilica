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

# ## nb_eso1_gold_dim_company
#
# null

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

# ── Silver source ─────────────────────────────────────────────────────────────
F0010 = "f0010_company_constants"    # company constants — one row per company (CCCO)

# ── Gold target BUILT here (new, rpt) ─────────────────────────────────────────
DIM   = "dim_company"

# ── CDF state ─────────────────────────────────────────────────────────────────
# 2026-08-14 — was a plain overwrite on every run, driven by MANUAL_OVERWRITE.
# Now a Change Data Feed stream reads F0010 and the dim is rebuilt ONLY when
# that stream delivers a change. MANUAL_OVERWRITE is gone: the checkpoint IS
# the state, and deleting it is the only way to force a full load —
#     mssparkutils.fs.rm("Files/checkpoints/dimensions/dim_company", True)
# A full load also happens by itself when the gold table is missing.
#
# ANY change rebuilds the whole dim rather than merging row by row. F0010 is a
# JDE setup table — one row per company, a few dozen rows — so a rebuild costs
# one short scan and cannot leave a stale row or a missed delete behind.
CKPT_ROOT  = "Files/checkpoints/dimensions/dim_company"
QUERY_NAME = "dim__f0010"

# Delta may compact its log between runs, removing the snapshot entry that CDF
# streaming uses to verify column mapping. Without this flag the SECOND run
# fails with DELTA_STREAMING_CHECK_COLUMN_MAPPING_NO_SNAPSHOT. Safe here
# because bronze→silver never renames a column.
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


# In[2]:


# ----------------------------------------------------------------------------
# 2) DIM BUILDER
# ----------------------------------------------------------------------------

# DIM transform — one row per company (CCCO). The key matches the freight fact's
# company_key_order_no (SDKCOO = CCCO) for the relationship.
#   company                = CCCO     (key)
#   company_name           = CCNAME
#   currency_code          = CCCRCD   (company domestic currency = report DomesticCurrency)
#   period_number_current  = CCPNC    (company current fiscal period)
#   fiscal_year_current    = CCDFF    (company current fiscal year)
def build_dim():
    cc = load_silver_table(F0010)
    df = (cc.select(
              F.trim(F.col("company")).alias("company"),
              F.col("name").alias("company_name"),
              F.col("currency_code_from").alias("currency_code"),
              F.col("period_number_current").cast("int").alias("period_number_current"),
              F.col("financial_reporting_year").cast("int").alias("fiscal_year_current"))
          .dropDuplicates(["company"]))
    return df


# Result of this run, reported in the exit payload at the bottom.
#   built      the dim was rebuilt (first load, or F0010 changed)
#   no_change  F0010 had nothing new — the existing dim is already current
_result = {"status": "no_change", "rows": None}


def write_gold():
    """Overwrite the gold dim from the current F0010 snapshot."""
    new = build_dim()
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


def handle_f0010(batch_df, batch_id):
    changes = batch_df.filter(F.col("_change_type").isin(CHANGE_TYPES))
    if INIT_VERSION >= 0:
        changes = changes.filter(F.col("_commit_version") > INIT_VERSION)

    if changes.isEmpty():
        return

    if _result["status"] == "built":
        print("  [F0010] batch {} — already rebuilt this run, skipping".format(batch_id))
        return

    print("  [F0010] changes in batch {} — rebuilding".format(batch_id))
    write_gold()


# ----------------------------------------------------------------------------
# 4) STREAM DRAIN
#
# Three outcomes, and telling them apart is the whole job:
#
#   no new data    normal for a setup table like F0010. KEEP the checkpoint —
#                  clearing it would make the next run think it had never run
#                  and full-load everything.
#   unreadable     the silver table was DROP+CREATE'd, or its log was compacted
#                  past our stored offset. Clear the checkpoint, full-load next run.
#   anything else  re-raise. An unknown failure must not be swallowed.
#
# "non-existent version N" is raised for two OPPOSITE reasons, so the message
# text alone cannot be trusted — compare N against the table instead:
#   N ahead of the table → nothing new was written (delta only writes its own
#       internal checkpoint every 10 commits, so a near-static table lands here
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
    INIT_VERSION = current_version(F0010)
    write_gold()          # sets _result — the full load IS this run's rebuild
    print("  init version {}".format(INIT_VERSION))
else:
    # -1 means "no floor" — the checkpoint already knows where the stream
    # stopped, so the handler needs no version filter.
    INIT_VERSION = -1
    print("== RESUME ==")

drain_stream(F0010, CKPT_ROOT, handle_f0010, QUERY_NAME, INIT_VERSION)

if _result["status"] == "no_change":
    print("== no change in {} — {} ".format(F0010, gname(DIM)))

print("== run complete in {}s — status={} rows={} ==".format(
    round(time.time() - _run_start, 1), _result["status"], _result["rows"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
