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

# ## nb_silver_to_gold_cdf_dim_plant
#
# null

# In[1]:


# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------
#
# Output table : lh_jde_gold.rpt.dim_plant
# Grain        : one row per plant_code (trimmed F0006.MCMCU)
# Role         : Canonical Gold dim for all plant / branch attribute lookups.
#                Consumed by every Project 1 + Project 2 report that displays
#                or filters by plant.
#
# Power BI relationship pattern:
#   fact.site_number / fact.branch_plant  <->  dim_plant.plant_code
#
# SOURCE — F0006 (Business Unit Master), silver layer
#   MCMCU    -> cost_center                (the plant code)
#   MCDL01   -> description_001            (long plant name, e.g. "CLARK DIRECT")
#   MCDC     -> descrip_compressed         (short plant name)
#   MCSTYL   -> cost_center_type           (BP filter — see note in the builder)
#   MCRP02   -> category_code_cost_ct_002  (plant-category code used in the
#                                           PL/SQL plant-name resolution chain)
#   MCRMCU1  -> related_business_unit      (used by dim_item_cost_cascade)
#   MCCO     -> company                    (cross-ref to company master)
#
# ── CDF state ────────────────────────────────────────────────────────────────
# 2026-08-17 — converted from nb_dim_plant.py, which overwrote the dim on every
# run. Now a Change Data Feed stream reads F0006 and the dim is rebuilt ONLY
# when that stream delivers a change. The checkpoint IS the state, and deleting
# it is the only way to force a full load —
#     mssparkutils.fs.rm("Files/checkpoints/dimensions/dim_plant", True)
# A full load also happens by itself when the gold table is missing.
#
# ANY change to F0006 rebuilds the whole dim rather than merging row by row.
# F0006 is a small table (a few thousand business units), so a rebuild costs one
# short scan and cannot leave a stale row or a missed delete behind.
#
# NOTE: nb_otc_facts_v3.py also reads F0006, as a PASSIVE ref for
# dim_invoice_reconciliation and fact_extended_sales_order_5. That is fine —
# this notebook keeps its own checkpoint and writes a different gold table.
# ----------------------------------------------------------------------------

from pyspark.sql import functions as F
from datetime import datetime
import re, time

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

F0006 = "f0006_business_unit_master"

DIM        = "dim_plant"
CKPT_ROOT  = "Files/checkpoints/dimensions/dim_plant"
QUERY_NAME = "dim_plant__f0006"

# Delta may compact its log between runs, removing the snapshot entry that CDF
# streaming uses to verify column mapping. Without this flag the SECOND run
# fails with DELTA_STREAMING_CHECK_COLUMN_MAPPING_NO_SNAPSHOT. Safe here
# because bronze->silver never renames a column.
spark.conf.set(
    "spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled",
    "true",
)

print(f"Gold dim_plant processor (CDF build) — target {gname(DIM)}")

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
#
# Natural PK = plant_code (trimmed MCMCU).
#
# FILTER: cost_center_type = 'BP' (Branch Plant) is DELIBERATELY NOT applied.
# It was removed on 23/July/2026 so the same table could serve the
# modernization project, which needs the non-BP cost centers too. Left here,
# commented, because the reason it is absent matters more than the line itself.
# ----------------------------------------------------------------------------
DIM_KEY = "plant_code"


def build_dim_plant(run_dt):
    df_f0006 = load_silver_table(F0006)

    df_dim = (
        df_f0006
        # filter removed for re-using the same table for modernizatin project (23/July/2026)
        # .filter(F.trim(F.col("cost_center_type")) == "BP")
        .select(
            F.trim(F.col("cost_center")).alias("plant_code"),
            F.col("description_001").alias("plant_name"),
            F.col("descrip_compressed").alias("plant_name_compressed"),
            F.col("category_code_cost_ct_002").alias("plant_category_code_02"),
            F.trim(F.col("related_business_unit")).alias("related_business_unit"),
            F.col("company"),
            # new columns 'category_code_cost_ct_020' and 'state' added for
            # re-using the same table for modernizatin project (23/July/2026)
            F.col("category_code_cost_ct_020"),
            F.col("state"),
        )
        # For treating multiple branch plants of jackson as one site in reports, we are adding new column parent_plant_code
        # Based on tal's request 27/may/2026
        # parent_plant_code added on 10/june/2026
        .withColumn("parent_plant_code", F.when(
                F.col("plant_code").isin("722", "723", "724"),
                F.lit("721")
            ).otherwise(F.col("plant_code")))
        .withColumn("last_refreshed_timestamp", F.lit(run_dt).cast("timestamp"))
    )

    # Reorder so last_refreshed_timestamp is leftmost
    return df_dim.select(
        "last_refreshed_timestamp",
        *[c for c in df_dim.columns if c != "last_refreshed_timestamp"]
    )


# Result of this run, printed at the bottom.
#   built      the dim was rebuilt (first load, or F0006 changed)
#   no_change  nothing new in F0006 — the existing dim is already current
_result = {"status": "no_change", "rows": None}


def write_gold():
    """Overwrite the gold dim from the current F0006 snapshot."""
    # Captured per REBUILD, not per run. The batch notebook stamped every row
    # with the notebook's start time even when nothing had changed; here the
    # column means "when this data was last actually rebuilt", which is the
    # only reading that stays true once runs become no-ops.
    run_dt = datetime.now()

    new = build_dim_plant(run_dt)
    (new.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .option("delta.enableChangeDataFeed", "true")
       .saveAsTable(gname(DIM)))

    spark.sql("OPTIMIZE {}".format(gname(DIM)))

    _result["rows"]   = spark.read.table(gname(DIM)).count()
    _result["status"] = "built"
    print("  {} rows={} last_refreshed_timestamp={}".format(gname(DIM), _result["rows"], run_dt))


# In[3]:


# ----------------------------------------------------------------------------
# 3) STREAM HANDLER
#
# Unlike dim_uss_plant there is NO sub-filter here. That dim reads F0005, which
# carries every UDC type, so it must narrow the batch to 55/UP or an unrelated
# code list would rebuild it for nothing. F0006 is business units only — every
# row in it is a candidate plant_code, so any change is genuinely ours.
#
# Rebuild at most once per run: AvailableNow can split a backlog into several
# batches, and rebuilding on each would repeat identical work, because the
# rebuild reads the CURRENT silver snapshot — which already contains every row
# those later batches carry.
# ----------------------------------------------------------------------------
CHANGE_TYPES = ["insert", "update_postimage", "delete"]


def handle_f0006(batch_df, batch_id):
    changes = batch_df.filter(F.col("_change_type").isin(CHANGE_TYPES))
    if INIT_VERSION >= 0:
        changes = changes.filter(F.col("_commit_version") > INIT_VERSION)

    if changes.isEmpty():
        return

    if _result["status"] == "built":
        print("  [F0006] batch {} — already rebuilt this run, skipping".format(batch_id))
        return

    print("  [F0006] changes in batch {} — rebuilding".format(batch_id))
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
#   N ahead of the table -> nothing new was written (delta only writes its own
#       internal checkpoint every 10 commits, so a quiet table lands here
#       instead of on DELTA_INVALID_CDC_RANGE). The checkpoint is VALID.
#   N behind the table   -> the log really was compacted. The checkpoint is dead.
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
    INIT_VERSION = current_version(F0006)
    write_gold()          # sets _result — the full load IS this run's rebuild
    print("  init version {}".format(INIT_VERSION))
else:
    # -1 means "no floor" — the checkpoint already knows where the stream
    # stopped, so the handler needs no version filter.
    INIT_VERSION = -1
    print("== RESUME ==")

drain_stream(F0006, CKPT_ROOT, handle_f0006, QUERY_NAME, INIT_VERSION)

if _result["status"] == "no_change":
    print("== no change in {} — {} left as is ==".format(F0006, gname(DIM)))

print("== run complete in {}s — status={} rows={} ==".format(
    round(time.time() - _run_start, 1), _result["status"], _result["rows"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
