#!/usr/bin/env python
# coding: utf-8

# ## nb_silver_to_gold_cdf_dim_lookups
# 
# New notebook

# In[1]:


#!/usr/bin/env python
# coding: utf-8

# ## nb_silver_to_gold_dim_lookups
#
# Builds and keeps current every UDC-based lookup dimension from F0004 + F0005.

# In[1]:


# ============================================================================
# nb_silver_to_gold_dim_lookups
#
# Gold UDC lookup dimensions, built from Silver F0004 (UDC type definitions)
# and F0005 (UDC code values), kept current by Change Data Feed.
#
# ── ADDING A DIMENSION ───────────────────────────────────────────────────────
# Add one entry to UDC_DIMS and run.  Its Gold table won't exist yet, which
# triggers a full load of every dim.  Nothing else to change.
#
# ── FORCING A FULL LOAD ──────────────────────────────────────────────────────
# Delete the checkpoint root:
#     mssparkutils.fs.rm("Files/checkpoints/dimensions/dim_lookups", True)
# The next run rebuilds every dim from the Silver snapshot and restarts both
# streams from that point.  There is no override flag and no state file — the
# checkpoint IS the state.  It is all-or-nothing by design: a full load rebuilds
# every dim in UDC_DIMS, never a subset.
#
# ── HOW IT RUNS ──────────────────────────────────────────────────────────────
# Trigger.AvailableNow: drains all pending CDF changes, then exits.  Schedule it
# from a pipeline.  The two streams are drained SERIALLY, F0004 first — an
# F0004 change rebuilds a whole dim, and letting that race an F0005 MERGE into
# the same table would corrupt it.
#
#   F0004 CDF → a UDC type definition changed → rebuild the affected dim(s)
#   F0005 CDF → code values added / changed / deleted → MERGE into the dim
#
# Soft deletes (is_delete = 1 on an update_postimage) and hard deletes
# (_change_type = 'delete') both remove the row.
# ============================================================================

import re

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Delta may compact log files between runs, removing the snapshot entry that
# CDF streaming uses to verify column mapping.  This flag lets the stream resume
# without that snapshot.  Safe here because Bronze→Silver never renames columns.
# Same setting nb_otc_facts_v3.py uses, for the same reason.
spark.conf.set(
    "spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled",
    "true",
)

# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------
SILVER_LH, SILVER_SCHEMA = "lh_jde_silver", "jde"
GOLD_LH, GOLD_SCHEMA = "lh_jde_gold", "rpt"

F0004 = "f0004_user_defined_code_types"
F0005 = "f0005_user_defined_code_values"

CKPT_ROOT = "Files/checkpoints/dimensions/dim_lookups"
CKPT_F0005 = CKPT_ROOT + "/f0005"
CKPT_F0004 = CKPT_ROOT + "/f0004"

QUERY_F0005 = "dim_lookups__f0005"
QUERY_F0004 = "dim_lookups__f0004"


# ----------------------------------------------------------------------------
# 2) DIMENSION REGISTRY
# ----------------------------------------------------------------------------
#   product_code / udc_type  the JDE system + category codes (F0004 DTSY/DTRT)
#   table                    Gold table name, without schema
#   key_col / desc_col       Gold column names
#   key_type                 "string" or "long" — must match the related fact
#                            column, or the Power BI relationship won't form
UDC_DIMS = [
    {"product_code": "00", "udc_type": "TM", "table": "dim_mode_of_transport",
     "key_col": "modeoftransport_code", "desc_col": "modeoftransport_description",
     "key_type": "string"},
    {"product_code": "40", "udc_type": "AT", "table": "dim_status_code_next",
     "key_col": "statuscodenext_code", "desc_col": "statuscodenext_description",
     "key_type": "string"},
    {"product_code": "40", "udc_type": "AT", "table": "dim_status_code_last",
     "key_col": "statuscodelast_code", "desc_col": "statuscodelast_description",
     "key_type": "string"},
    {"product_code": "01", "udc_type": "SC", "table": "dim_sic",
     "key_col": "standardindustrycode_code", "desc_col": "standardindustrycode_description",
     "key_type": "string"},
    {"product_code": "00", "udc_type": "S", "table": "dim_state",
     "key_col": "state_code", "desc_col": "state_description",
     "key_type": "string"},
    {"product_code": "01", "udc_type": "10", "table": "dim_category_code_10",
     "key_col": "reportcodeaddbook010_code", "desc_col": "reportcodeaddbook010_description",
     "key_type": "string"},
    {"product_code": "01", "udc_type": "05", "table": "dim_category_code_05",
     "key_col": "reportcodeaddbook005_code", "desc_col": "reportcodeaddbook005_description",
     "key_type": "string"},
    {"product_code": "42", "udc_type": "FR", "table": "dim_freight_handling_code",
     "key_col": "freighthandlingcode_code", "desc_col": "freighthandlingcode_description",
     "key_type": "string"},
    {"product_code": "42", "udc_type": "HC", "table": "dim_hold_orders_code",
     "key_col": "holdorderscode_code", "desc_col": "holdorderscode_description",
     "key_type": "string"},
]

# dim_status_code_next and dim_status_code_last are both UDC 40/AT — the same
# code list role-played twice, so the fact can relate to each independently.


# ----------------------------------------------------------------------------
# 3) HELPERS
# ----------------------------------------------------------------------------
def sname(t):
    return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)


def gname(t):
    return "{}.{}.{}".format(GOLD_LH, GOLD_SCHEMA, t)


def read_silver(table):
    """Silver read with soft-deleted rows removed."""
    df = spark.read.table(sname(table))
    if "is_delete" in df.columns:
        df = df.where(F.col("is_delete").isNull() | (F.col("is_delete") != 1))
    return df


def key_expr(cfg, col="user_defined_code"):
    e = F.trim(F.col(col))
    return e.cast("long") if cfg["key_type"] == "long" else e


def path_exists(path):
    try:
        return bool(mssparkutils.fs.ls(path))
    except Exception:
        return False


def remove_path(path):
    try:
        mssparkutils.fs.rm(path, True)
        print("  removed {}".format(path))
    except Exception as e:
        print("  could not remove {}: {}".format(path, e))


def current_version(table):
    return (
        spark.sql("DESCRIBE HISTORY {}".format(sname(table)))
        .select(F.max("version"))
        .first()[0]
    )


# ----------------------------------------------------------------------------
# 4) BUILD — full rebuild of one dim from the Silver snapshot
# ----------------------------------------------------------------------------
def build_dim(cfg):
    """All F0005 code values for this UDC category.

    F0004 acts purely as a gate: the category must be registered there, which is
    what the original LEFT JOIN plus not-null key filter amounted to."""
    registered = (
        read_silver(F0004)
        .select(
            F.trim(F.col("product_code")).alias("g_pc"),
            F.trim(F.col("user_defined_codes")).alias("g_udc"),
        )
        .distinct()
    )
    return (
        read_silver(F0005)
        .where(
            (F.trim(F.col("product_code")) == cfg["product_code"])
            & (F.trim(F.col("user_defined_codes")) == cfg["udc_type"])
        )
        .join(
            registered,
            (F.trim(F.col("product_code")) == F.col("g_pc"))
            & (F.trim(F.col("user_defined_codes")) == F.col("g_udc")),
            "left_semi",
        )
        .select(
            key_expr(cfg).alias(cfg["key_col"]),
            F.trim(F.col("description_001")).alias(cfg["desc_col"]),
        )
        .where(F.col(cfg["key_col"]).isNotNull())
        .dropDuplicates([cfg["key_col"]])
    )


def rebuild(cfg):
    """Overwrite the Gold table from the Silver snapshot."""
    fq = gname(cfg["table"])
    (
        build_dim(cfg)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true")
        .saveAsTable(fq)
    )
    print("  built {:<28s} rows={}".format(cfg["table"], spark.read.table(fq).count()))


# ----------------------------------------------------------------------------
# 5) CDF HANDLERS
# ----------------------------------------------------------------------------
CHANGE_TYPES = ["insert", "update_postimage", "delete"]


def touched_categories(df):
    """The (product_code, udc_type) pairs present in a CDF batch."""
    rows = (
        df.select(
            F.trim(F.col("product_code")).alias("pc"),
            F.trim(F.col("user_defined_codes")).alias("udc"),
        )
        .distinct()
        .collect()
    )
    return {(r["pc"], r["udc"]) for r in rows}


def final_state(batch, cfg):
    """One row per code touched in this batch, carrying its FINAL state.

    A code can be updated several times inside one trigger window; MERGE needs at
    most one source row per target row, so keep the highest _commit_version.
    _gone marks a hard delete or a soft delete."""
    gone = F.col("_change_type") == "delete"
    if "is_delete" in batch.columns:
        gone = gone | F.coalesce(F.col("is_delete") == 1, F.lit(False))

    rows = (
        batch.where(
            (F.trim(F.col("product_code")) == cfg["product_code"])
            & (F.trim(F.col("user_defined_codes")) == cfg["udc_type"])
        )
        .select(
            key_expr(cfg).alias(cfg["key_col"]),
            F.trim(F.col("description_001")).alias(cfg["desc_col"]),
            gone.alias("_gone"),
            F.col("_commit_version"),
        )
        .where(F.col(cfg["key_col"]).isNotNull())
    )
    latest = Window.partitionBy(cfg["key_col"]).orderBy(F.col("_commit_version").desc())
    return (
        rows.withColumn("_rn", F.row_number().over(latest))
        .where(F.col("_rn") == 1)
        .drop("_rn", "_commit_version")
    )


def merge_dim(cfg, changes):
    """Apply one batch of changes: delete what's gone, upsert what isn't."""
    k, d = cfg["key_col"], cfg["desc_col"]
    (
        DeltaTable.forName(spark, gname(cfg["table"]))
        .alias("t")
        .merge(changes.alias("s"), "t.{k} = s.{k}".format(k=k))
        .whenMatchedDelete(condition="s._gone = true")
        .whenMatchedUpdate(condition="s._gone = false", set={d: "s." + d})
        .whenNotMatchedInsert(
            condition="s._gone = false", values={k: "s." + k, d: "s." + d}
        )
        .execute()
    )


def handle_f0005(batch_df, batch_id):
    """Code values changed — MERGE into every dim built on that category."""
    batch = batch_df.where(F.col("_change_type").isin(CHANGE_TYPES))
    if INIT["f0005"] >= 0:
        batch = batch.where(F.col("_commit_version") > INIT["f0005"])
    batch = batch.cache()
    try:
        if batch.isEmpty():
            return
        touched = touched_categories(batch)
        for cfg in UDC_DIMS:
            if (cfg["product_code"], cfg["udc_type"]) in touched:
                merge_dim(cfg, final_state(batch, cfg))
                print("  [F0005] merged {} (batch {})".format(cfg["table"], batch_id))
    finally:
        batch.unpersist()


def handle_f0004(batch_df, batch_id):
    """A UDC type definition changed — rebuild the affected dim(s) in full.
    F0004 is JDE-admin-only and changes very rarely, so a rebuild is cheap."""
    batch = batch_df.where(F.col("_change_type").isin(CHANGE_TYPES))
    if INIT["f0004"] >= 0:
        batch = batch.where(F.col("_commit_version") > INIT["f0004"])
    if batch.isEmpty():
        return
    touched = touched_categories(batch)
    for cfg in UDC_DIMS:
        if (cfg["product_code"], cfg["udc_type"]) in touched:
            print("  [F0004] definition changed (batch {}) —".format(batch_id), end=" ")
            rebuild(cfg)


# ----------------------------------------------------------------------------
# 6) RUN
# ----------------------------------------------------------------------------
NO_NEW_DATA = "DELTA_INVALID_CDC_RANGE"
UNREADABLE_OFFSET = (
    "DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE",  # table was DROP+CREATE'd
    "non-existent version",  # Delta log compacted past the stored offset
)

# "non-existent version N" is raised for two OPPOSITE reasons, so the text alone
# can't be trusted — compare N against the table instead:
#
#   N ahead of the table  → nothing new has been written.  Same meaning as
#       DELTA_INVALID_CDC_RANGE; Delta only writes its internal checkpoint every
#       10 commits, so a near-static table takes this code path instead.  F0004 is
#       JDE-admin-only and changes maybe twice a year, so it lands here on almost
#       every run.  The checkpoint is VALID — keep it.
#   N behind the table    → the log really was compacted past the stored offset.
#       The checkpoint is unusable — clear it and full-load next run.
VERSION_IN_MSG = re.compile(r"non-existent version[^0-9]*([0-9]+)")


def asks_for_future_version(msg, table):
    m = VERSION_IN_MSG.search(msg)
    if m is None:
        return False
    try:
        return int(m.group(1)) > current_version(table)
    except Exception:
        return False


def drain(table, checkpoint, handler, query_name, init_ver):
    """Start one AvailableNow CDF stream and block until it finishes.

    DELTA_INVALID_CDC_RANGE means 'no new data since the checkpoint' — normal for
    a scheduled run, and the checkpoint MUST be kept.  Clearing it would make the
    next run think it had never run and full-load everything."""
    # ⚠ startingVersion is set ONLY when starting fresh — NEVER on a resume.
    #
    # Fabric does NOT ignore startingVersion when a checkpoint exists, whatever the
    # docs say.  Passing "latest" on a resume makes Delta use it as the batch START
    # while the checkpoint supplies the END, so the range comes out backwards:
    #
    #   [DELTA_INVALID_CDC_RANGE] CDC range from start 4377 to end 4369 was invalid.
    #
    # The stream then dies before reading anything, and the handler below reads that
    # failure as "no new data" — so the dims silently stop updating while every run
    # still reports success.  This is the same bug found in nb_otc_facts_v3.py on
    # 2026-08-13, where it had been suppressing every incremental read since day one.
    reader = spark.readStream.format("delta").option("readChangeFeed", "true")
    if not path_exists(checkpoint):
        # No checkpoint: this is the first start after a full load, and init_ver is
        # the Silver version captured just before the rebuild.
        reader = reader.option("startingVersion", init_ver if init_ver >= 0 else "latest")

    q = (
        reader.table(sname(table))
        .writeStream.foreachBatch(handler)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .queryName(query_name)
        .start()
    )
    try:
        q.awaitTermination()
        print("  {} drained".format(query_name))
    except Exception as e:
        msg = str(e)
        if NO_NEW_DATA in msg or asks_for_future_version(msg, table):
            # Print the raw message too — this branch is a JUDGEMENT about an
            # exception, and hiding the evidence makes a wrong judgement invisible.
            print("  {} — no new data, checkpoint kept".format(query_name))
            print("      reason: {}".format(msg[:300].replace("\n", " ")))
        elif any(m in msg for m in UNREADABLE_OFFSET):
            print("  {} — offset unreadable, clearing checkpoint".format(query_name))
            remove_path(checkpoint)
        else:
            raise


spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

for _q in list(spark.streams.active):
    if _q.name in (QUERY_F0005, QUERY_F0004):
        _q.stop()
        print("Stopped leftover stream: {}".format(_q.name))

# Full load when there is no state to resume from: either checkpoint missing, or
# a registered dim has no Gold table yet (which is how a NEW dim gets built).
_missing_tables = [
    c["table"] for c in UDC_DIMS if not spark.catalog.tableExists(gname(c["table"]))
]
_needs_full_load = (
    not path_exists(CKPT_F0005) or not path_exists(CKPT_F0004) or bool(_missing_tables)
)

if _needs_full_load:
    print("== FULL LOAD ==")
    if _missing_tables:
        print("  missing Gold tables: {}".format(_missing_tables))
    remove_path(CKPT_ROOT)
    # Capture the Silver versions BEFORE building, so the streams pick up only
    # changes that land after this snapshot.  These are used once, by the drains
    # at the bottom of this same run — there is nothing to persist, because a run
    # that dies before those drains leaves no checkpoint and simply full-loads again.
    INIT = {"f0005": current_version(F0005), "f0004": current_version(F0004)}
    for _cfg in UDC_DIMS:
        rebuild(_cfg)
    print("  init versions {}".format(INIT))
else:
    # -1 means "no floor": the checkpoint already knows where each stream stopped,
    # so startingVersion is ignored and the handlers need no version filter.
    INIT = {"f0005": -1, "f0004": -1}
    print("== RESUME ==")

# F0004 FIRST: it rebuilds whole tables, and racing that against an F0005 MERGE
# into the same table would corrupt it.  Serial drain, never concurrent.
drain(F0004, CKPT_F0004, handle_f0004, QUERY_F0004, INIT["f0004"])
drain(F0005, CKPT_F0005, handle_f0005, QUERY_F0005, INIT["f0005"])

print("== run complete ==")

