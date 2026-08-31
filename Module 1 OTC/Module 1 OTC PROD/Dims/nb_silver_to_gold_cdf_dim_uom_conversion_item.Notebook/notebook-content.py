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

# ## nb_silver_to_gold_dim_uom_conversion_item
#
# New notebook

# In[4]:


#!/usr/bin/env python
# coding: utf-8

# ## nb_silver_to_gold_dim_uom_conversion_item
#
# Gold layer — F41002 (UOM Conversion) — conformed ITEM-SPECIFIC conversion-
# factor dimension. This is Tier A of the Total Tons UOM cascade:
#   Tier 0  uom_as_input = 'TN'                      → factor = 1.0  (DAX)
#   Tier A  this dim (F41002 item-specific factors)  → DAX RELATED()
#   Tier B  dim_uom_conversion (F41003 standard/generic factors) → DAX RELATED()
#   Fallback  literal 1.0                            → DAX fallback
#
# Deliberately a SEPARATE table from dim_uom_conversion (F41003, Tier B) —
# different source table, different grain (per-item vs generic-per-UOM),
# and dim_uom_conversion is already live/streaming shared infrastructure
# that should not be reshaped for this addition.
#
# Bidirectional: both fwd (related_uom='TN') and rev (uom='TN', factor
# inverted) rows are folded into a single (identifier_short_item, from_uom)
# key — mirrors the same fwd/rev pattern already proven in dim_uom_conversion
# and in the retiring fact_extended_sales_order_7's own Tier A join.
#
# Key is 2 columns (identifier_short_item, from_uom) — NOT cost_center —
# matching the already-proven, already-live Tier A join logic in the
# retiring notebook (build_fact_eso7), confirmed to ignore cost_center.
# If a future data check finds conversion_factor genuinely varies by
# cost_center for the same item+uom, revisit this and fold cost_center
# into the key (3-column version) instead.
#
# fact_sales_order_detail relates to this table via a plain computed
# column (item_uom_key = concat_ws("|", identifier_short_item, uom_as_input))
# — no join to F41002 happens in the fact's own notebook at all.
#
# ─────────────────────────────────────────────────────────────────────────────
# 2026-08-14 — CDF ENABLED (was: plain overwrite on every run)
#
# The build logic below is unchanged. What changed is WHEN it runs:
#
#   before   every run rebuilt the table, whether or not F41002 had changed
#   now      a Change Data Feed stream reads F41002 and the table is rebuilt
#            ONLY when that stream actually delivers a change
#
# Why REBUILD on change instead of MERGE row by row: because of the fwd/rev
# fold, one Gold row can come from EITHER a fwd source row or a rev one. A
# MERGE would have to collect both candidate keys from both the before- and
# after-image of every change, and any key it missed would leave a wrong
# factor in Gold forever — silently, because nothing downstream can tell a
# stale factor from a correct one. F41002 is a small setup table that changes
# rarely, so a rebuild costs one short scan and cannot be wrong.
#
# FORCING A FULL LOAD — delete the checkpoint, that is the only switch:
#   mssparkutils.fs.rm("Files/checkpoints/dimensions/dim_uom_conversion_item", True)
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — Imports & constants
# ─────────────────────────────────────────────────────────────────────────────
import re
from pyspark.sql import functions as F
from datetime import datetime

F41002_TBL = "lh_jde_silver.jde.f41002_item_units_of_measure_conversion_factors"
GOLD_TABLE = "lh_jde_gold.rpt.dim_uom_conversion_item"

CHECKPOINT = "Files/checkpoints/dimensions/dim_uom_conversion_item"
QUERY_NAME = "dim_uom_conversion_item__f41002"

# Delta may compact its log between runs, removing the snapshot entry that CDF
# streaming uses to verify column mapping. Without this flag the SECOND run
# fails with DELTA_STREAMING_CHECK_COLUMN_MAPPING_NO_SNAPSHOT. Safe here
# because bronze→silver never renames a column.
spark.conf.set(
    "spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled",
    "true",
)

print(f"Run timestamp : {datetime.now()}")


# In[5]:


# ─────────────────────────────────────────────────────────────────────────────
# CELL 2 — Helper: load silver, drop soft-deleted rows + pipeline metadata
# ─────────────────────────────────────────────────────────────────────────────
_EXCLUDE_COLS = ["is_delete", "deleted_date_time"]

def load_silver(table_name):
    df = spark.read.table(table_name)
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _EXCLUDE_COLS])


# In[6]:


# ─────────────────────────────────────────────────────────────────────────────
# CELL 3 — Helper: checkpoint + version utilities
#
# The checkpoint IS the state of this notebook. There is no override flag and
# no state file — delete the checkpoint folder and the next run full-loads.
# ─────────────────────────────────────────────────────────────────────────────
def checkpoint_exists(path):
    try:
        return bool(mssparkutils.fs.ls(path))
    except Exception:
        return False


def remove_checkpoint(path):
    try:
        mssparkutils.fs.rm(path, True)
        print(f"  removed {path}")
    except Exception as e:
        print(f"  could not remove {path} : {e}")


def current_version(table_name):
    return (
        spark.sql(f"DESCRIBE HISTORY {table_name}")
        .select(F.max("version"))
        .first()[0]
    )


# In[7]:


# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Build bidirectional (fwd + rev) conversion factors, then key
#
# Silver column mapping (confirmed via F41002 table metadata):
#   UMITM  - identifier_short_item  (matches F4211.SDITM, NOT SDLITM)
#   UMUM   - uom
#   UMRUM  - related_uom
#   UMCONV - conversion_factor      (implied_decimals already applied in
#                                    silver — do NOT divide again)
#
# fwd: related_uom = 'TN'  - from_uom = uom,          conv_factor = conversion_factor
# rev: uom = 'TN'          - from_uom = related_uom,  conv_factor = 1 / conversion_factor
#
# conversion_factor = 0 excluded from both directions — a zero factor is
# not usable (and would divide-by-zero on the reverse side).
#
# SELF-CONVERSION rows (uom = related_uom) excluded from both directions.
# A unit of measure converts to itself at 1.0 by definition, so such a row
# carries no information — but F41002 holds at least one with a factor of
# 2000 (item 54481, TN→TN, found 2026-08-14). Left in, it is the one row that
# BOTH legs match: fwd reads it as 2000 and rev reads the same row as 1/2000,
# so the key (item, 'TN') gets two contradictory factors and the fold picks one
# at random. Tier 0 of the cascade already answers uom_as_input = 'TN' with
# 1.0, so nothing downstream needs these rows.
#
# Wrapped in a function so the same logic serves both the first full load and
# every later rebuild triggered by the CDF stream. The logic itself is
# unchanged from the original notebook.
# ─────────────────────────────────────────────────────────────────────────────
def build_dim_uom_conversion_item():
    df_f41002 = load_silver(F41002_TBL).filter(
        F.trim(F.col("uom")) != F.trim(F.col("related_uom"))
    )
    print(f"  F41002 silver rows : {df_f41002.count():,}")

    _fwd = (
        df_f41002
        .filter((F.trim(F.col("related_uom")) == "TN") & (F.col("conversion_factor") != 0))
        .select(
            F.col("identifier_short_item"),
            F.trim(F.col("uom")).alias("from_uom"),
            F.col("conversion_factor").cast("double").alias("conv_factor"),
        )
    )

    _rev = (
        df_f41002
        .filter((F.trim(F.col("uom")) == "TN") & (F.col("conversion_factor") != 0))
        .select(
            F.col("identifier_short_item"),
            F.trim(F.col("related_uom")).alias("from_uom"),
            (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_factor"),
        )
    )

    df_candidates = _fwd.unionByName(_rev)
    check_key_uniqueness(df_candidates)

    df_dim = df_candidates.dropDuplicates(["identifier_short_item", "from_uom"])

    # item_uom_key — 2-column surrogate matching the join column carried on
    # fact_sales_order_detail (identical column order: identifier_short_item, from_uom).
    df_dim = df_dim.withColumn(
        "item_uom_key",
        F.concat_ws("|", "identifier_short_item", "from_uom")
    )
    return df_dim


# In[8]:


# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — Validate: (identifier_short_item, from_uom) is unique
#
# This is the chosen 2-column key (matches the already-proven Tier A join
# in the retiring notebook, which ignores cost_center). Any duplicate here
# means either a silver-layer defect or that conversion_factor genuinely
# varies by cost_center for some item+uom pair — investigate before
# proceeding; do not silently pick one row.
#
# TWO CHANGES from the original check:
#
#   1. It now runs BEFORE the fold, not after. Run after dropDuplicates() on
#      the same two columns, the check could never fail — dropDuplicates had
#      already made the key unique, so the assert was always passing on a
#      frame it could not fault. It now looks at the raw fwd+rev candidates.
#   2. It counts only keys whose factors genuinely DISAGREE, and it WARNS
#      instead of raising. The same key appearing twice with the same factor
#      is harmless. Raising is what changed: this function now runs inside a
#      streaming batch, and an exception there kills the query and leaves the
#      checkpoint mid-flight. A loud print is visible without that cost.
# ─────────────────────────────────────────────────────────────────────────────
KEY_COLS = ["identifier_short_item", "from_uom"]

def check_key_uniqueness(df_candidates):
    df_conflicts = (
        df_candidates
        .groupBy(*KEY_COLS)
        .agg(F.countDistinct("conv_factor").alias("distinct_factors"))
        .filter(F.col("distinct_factors") > 1)
    )
    conflict_count = df_conflicts.count()

    if conflict_count == 0:
        print("  (identifier_short_item, from_uom) uniqueness verified.")
        return

    print(f"  WARNING — {conflict_count} item+uom pair(s) have MORE THAN ONE factor.")
    print("    conversion_factor may genuinely vary by cost_center. Revisit the")
    print("    2-column vs 3-column key decision (see header note). Sample:")
    df_conflicts.show(10, truncate=False)


# In[9]:


# ─────────────────────────────────────────────────────────────────────────────
# CELL 6 — Write Gold dimension table
# ─────────────────────────────────────────────────────────────────────────────
def write_gold():
    df_dim = build_dim_uom_conversion_item()

    df_dim.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("delta.enableChangeDataFeed", "true") \
        .saveAsTable(GOLD_TABLE)

    spark.sql(f"OPTIMIZE {GOLD_TABLE}")
    print(f"  {GOLD_TABLE}  - {spark.read.table(GOLD_TABLE).count():,} rows")


# In[10]:


# ─────────────────────────────────────────────────────────────────────────────
# CELL 7 — CDF handler: rebuild when F41002 actually changed
#
# The stream answers one question — did F41002 change since the last run? If
# it did, rebuild; if it did not, the handler never fires and Gold is left
# alone. That is the whole point of the conversion.
#
# Rebuild at most once per run: AvailableNow can split a backlog into several
# batches, and rebuilding on each would repeat identical work, because the
# rebuild reads the CURRENT silver snapshot — which already contains every row
# those later batches carry.
# ─────────────────────────────────────────────────────────────────────────────
CHANGE_TYPES = ["insert", "update_postimage", "delete"]

_already_rebuilt = {"done": False}

def handle_f41002(batch_df, batch_id):
    df_changes = batch_df.filter(F.col("_change_type").isin(CHANGE_TYPES))
    if INIT_VERSION >= 0:
        df_changes = df_changes.filter(F.col("_commit_version") > INIT_VERSION)

    if df_changes.isEmpty():
        return

    if _already_rebuilt["done"]:
        print(f"  [F41002] batch {batch_id} — already rebuilt this run, skipping")
        return

    print(f"  [F41002] changes in batch {batch_id} — rebuilding")
    write_gold()
    _already_rebuilt["done"] = True


# In[11]:


# ─────────────────────────────────────────────────────────────────────────────
# CELL 8 — Stream helper: drain one AvailableNow CDF stream
#
# Three outcomes, and telling them apart is the whole job:
#
#   no new data      normal for a setup table like F41002. KEEP the checkpoint —
#                    clearing it would make the next run think it had never run
#                    and full-load everything.
#   unreadable       the silver table was DROP+CREATE'd, or its log was
#                    compacted past our stored offset. Clear the checkpoint and
#                    full-load next run.
#   anything else    re-raise. An unknown failure must not be swallowed.
#
# "non-existent version N" is raised for two OPPOSITE reasons, so the message
# text alone cannot be trusted — compare N against the table instead:
#   N ahead of the table → nothing new was written (delta only writes its own
#       internal checkpoint every 10 commits, so a near-static table lands here
#       instead of on DELTA_INVALID_CDC_RANGE). Checkpoint is VALID.
#   N behind the table   → the log really was compacted. Checkpoint is dead.
# ─────────────────────────────────────────────────────────────────────────────
NO_NEW_DATA = "DELTA_INVALID_CDC_RANGE"
UNREADABLE_OFFSET = (
    "DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE",
    "non-existent version",
)
VERSION_IN_MSG = re.compile(r"non-existent version[^0-9]*([0-9]+)")


def asks_for_future_version(msg, table_name):
    match = VERSION_IN_MSG.search(msg)
    if match is None:
        return False
    try:
        return int(match.group(1)) > current_version(table_name)
    except Exception:
        return False


def drain_stream(table_name, checkpoint, handler, query_name, init_version):
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
        reader.table(table_name)
        .writeStream
        .foreachBatch(handler)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .queryName(query_name)
        .start()
    )

    try:
        query.awaitTermination()
        print(f"  {query_name} drained")
    except Exception as e:
        msg = str(e)
        if NO_NEW_DATA in msg or asks_for_future_version(msg, table_name):
            # Print the raw message too. This branch is a JUDGEMENT about an
            # exception, and hiding the evidence makes a wrong judgement invisible.
            print(f"  {query_name} — no new data, checkpoint kept")
            print(f"      reason: {msg[:300]}")
        elif any(m in msg for m in UNREADABLE_OFFSET):
            print(f"  {query_name} — offset unreadable, clearing checkpoint")
            remove_checkpoint(checkpoint)
        else:
            raise


# In[12]:


# ─────────────────────────────────────────────────────────────────────────────
# CELL 9 — Run
# ─────────────────────────────────────────────────────────────────────────────
for _q in list(spark.streams.active):
    if _q.name == QUERY_NAME:
        _q.stop()
        print(f"Stopped leftover stream: {_q.name}")

# Full load when there is nothing to resume from: no checkpoint, or the gold
# table does not exist yet.
if not checkpoint_exists(CHECKPOINT) or not spark.catalog.tableExists(GOLD_TABLE):
    print("== FULL LOAD ==")
    if checkpoint_exists(CHECKPOINT):
        remove_checkpoint(CHECKPOINT)
    # Capture the silver version BEFORE building, so the stream starts from the
    # snapshot we are about to write and does not replay it.
    INIT_VERSION = current_version(F41002_TBL)
    write_gold()
    _already_rebuilt["done"] = True      # the full load IS this run's rebuild
    print(f"  init version {INIT_VERSION}")
else:
    # -1 means "no floor" — the checkpoint already knows where the stream
    # stopped, so the handler needs no version filter.
    INIT_VERSION = -1
    print("== RESUME ==")

drain_stream(F41002_TBL, CHECKPOINT, handle_f41002, QUERY_NAME, INIT_VERSION)

print("== run complete ==")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
