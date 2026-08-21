#!/usr/bin/env python
# coding: utf-8

# ## nb_eso1_gold_dim_item
# 
# New notebook

# In[1]:


# ## nb_eso1_gold_dim_item
#
# **Gold `dim_item` processor** for Extended Sales Order 1 (Billable v Payable Freight).
# Builds and continuously refreshes ONE table — `lh_jde_gold.eso1.dim_item` — from the
# Silver item master (F4101) via a single Change Data Feed stream. Split out of
# nb_eso1_gold_streaming so `dim_item` and the fact run as independent jobs (own table,
# own checkpoint root, own OVERWRITE switch).
#
# Flow: constants → transform → seed-if-missing → start CDF stream (foreachBatch MERGE)
#       → refresh every 30 seconds.
#
# Streaming model mirrors nb_silver_to_gold_eso7_v2 (CDF incremental):
#   • continuous trigger(processingTime="30 seconds") + awaitAnyTermination
#   • make_dim_item_handler(init_ver): skip the seed rows (_commit_version <= init_ver),
#     then act only on real change rows (_change_type insert/update_postimage/delete)
#     and CDC-write to Gold (MERGE upsert + MERGE delete)
#   • checkpoint namespaced per ENV; startingVersion knob
# Design: docs/ESO1_gold_layer_design.md


# In[1]:


# =============================================================================
# CONFIG + CONSTANTS  (names per Fabric_Naming_Convention_Guidelines.pdf)
# =============================================================================
from pyspark.sql import functions as F
from delta.tables import DeltaTable

GOLD_SCHEMA = "lh_jde_gold.rpt"

# ── refresh / runtime config (CDF concept adopted from nb_silver_to_gold_eso7_v2) ──
ENV             = "dev"                              # checkpoint namespacing — envs never collide
TRIGGER         = {"processingTime": "30 seconds"}   # ← continuous; refresh every 30 s
CKPT            = f"Files/checkpoints/eso1_dim_{ENV}"  # OWN root — independent of the fact notebook
# ── manual reprocess switch (== ESO7 v2 MANUAL_OVERWRITE) ─────────────────────
#   OVERWRITE = True  -> full load: drop + rebuild dim_item from the full Silver snapshot,
#                        snapshot the source's Delta version as init_ver, clear checkpoints.
#   OVERWRITE = False -> resume: keep the table + checkpoints, the stream catches up from
#                        where it left off (init_ver = -1, no version filtering).
OVERWRITE       = False    # ⚠ ONE-OFF full reprocess (cdf schema) — set back to False after this run

# ── Silver sources ────────────────────────────────────────────────────────────
SRC_SCHEMA    = "cdf"     # Silver Change Data Feed schema; CDF must be enabled on F4101.
SRC_LAKEHOUSE = "lh_jde_silver"
F4101_TBL     = "f4101_item_master"

# ── Gold target BUILT here (new, eso1) ─────────────────────────────────────────
T_DIM_ITEM  = f"{GOLD_SCHEMA}.dim_item"

print(f"ESO1 Gold dim_item processor — trigger {TRIGGER}  target {T_DIM_ITEM}")


# In[2]:


# In[2]:


# =============================================================================
# HELPERS
# =============================================================================
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def sname(table_name):
    """Fully-qualified Silver source name (== ESO7 v2 sname)."""
    return f"{SRC_LAKEHOUSE}.{SRC_SCHEMA}.{table_name}"

def load_silver_table(table_name):
    df = spark.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

def current_version(silver_table):
    """Latest committed Delta version of a Silver source (for init_ver seed-skip)."""
    return spark.sql(f"DESCRIBE HISTORY {sname(silver_table)}").select(F.max("version")).first()[0]


# In[3]:


# In[3]:


# =============================================================================
# DIM transform — dim_item (F4101, natural PK)
# =============================================================================
def transform_dim_item(restrict_item=None):
    f4101 = load_silver_table(F4101_TBL)
    if restrict_item is not None:
        f4101 = f4101.join(restrict_item.alias("r"),
                           f4101["identifier_short_item"] == F.col("r.identifier_short_item"), "left_semi")
    # business columns only — no audit columns (CDC handled by upsert_dim_item)
    return (f4101.select(F.col("identifier_short_item").alias("item_number_short"),
                         F.col("description_line_01").alias("item_name"),
                         F.col("uom_weight").alias("uom_weight"))
            .dropDuplicates(["item_number_short"]))


# In[4]:


# In[4]:


# =============================================================================
# CDC WRITE HELPERS — NO audit columns (CDF concept from nb_silver_to_gold_eso7_v2)
#   Gold table stores business columns only — no record_hash / is_deleted /
#   source_commit_timestamp / gold_updated_timestamp.
#   • dim : MERGE upsert (whenMatchedUpdateAll / whenNotMatchedInsertAll) + MERGE delete.
# =============================================================================
def _write_new_table(df, target, cdf=True):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if cdf:
        w = w.option("delta.enableChangeDataFeed", "true")   # Gold CDF on for downstream
    w.saveAsTable(target)

def upsert_dim_item(items_change, items_delete):
    """CDC for dim_item: MERGE upsert changed items + MERGE delete removed items
    (no audit columns). `items_*` carry `identifier_short_item`. Returns upserted rows."""
    n = 0
    if not items_change.rdd.isEmpty():
        src = transform_dim_item(restrict_item=items_change)
        (DeltaTable.forName(spark, T_DIM_ITEM).alias("t")
            .merge(src.alias("s"), "t.item_number_short = s.item_number_short")
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        n = src.count()
    if not items_delete.rdd.isEmpty():
        d = items_delete.select(F.col("identifier_short_item").alias("item_number_short")).distinct()
        (DeltaTable.forName(spark, T_DIM_ITEM).alias("t")
            .merge(d.alias("s"), "t.item_number_short = s.item_number_short")
            .whenMatchedDelete().execute())
    return n


# In[5]:


# In[5]:


# =============================================================================
# FULL LOAD vs RESUME  (streaming approach from nb_silver_to_gold_eso7_v2)
#   1) Stop any of our streams left alive from a previous run in this session
#      (stopping a cell does NOT stop Spark streaming queries).
#   2) FULL LOAD when OVERWRITE, or dim_item is missing, or the checkpoints are gone:
#      drop + rebuild dim_item from the full Silver snapshot, snapshot the source's
#      current Delta version as init_ver, and clear the checkpoints. The stream then
#      starts at init_ver and skips _commit_version <= init_ver (the seed rows).
#   3) Otherwise RESUME (init_ver = -1; the checkpoint drives the offset).
# =============================================================================
# the per-stream checkpoint dir (name must match the queryName() in the start cell)
_CKPT_PATHS = [f"{CKPT}/dim__{F4101_TBL}"]

def _checkpoints_exist():
    """True iff EVERY per-stream checkpoint has a COMMITTED offset (its offsets/ dir is
    non-empty). Checking offsets/ — not merely that the checkpoint dir exists and is
    non-empty — matters because a checkpoint dir can hold metadata/ + sources/ yet never
    have committed a batch (e.g. the query died on its first read). The old dir-only check
    let such an INCOMPLETE checkpoint fool the gate into RESUME; Delta then found no usable
    offset and cold-started the CDF reader at startingVersion=0 (version 0 predates CDF
    enablement) -> DELTA_MISSING_CHANGE_DATA. Requiring a committed offset treats an
    incomplete checkpoint as ABSENT, forcing a FULL LOAD that re-establishes init_ver at a
    CDF-valid version. Also per-stream (not just the root, as ESO7 does) so a rename or a
    stale root can't route the run into RESUME either."""
    for p in _CKPT_PATHS:
        try:
            if not mssparkutils.fs.ls(f"{p}/offsets"):
                return False
        except Exception:
            return False
    return True

# ── (1) stop leftover streams from a previous run in this Spark session ──────────
_STREAM_NAMES = {"dim__" + F4101_TBL}
_stopped = []
for _q in list(spark.streams.active):
    if _q.name in _STREAM_NAMES:
        _q.stop()
        _stopped.append(_q.name)
if _stopped:
    print(f"Stopped leftover streams: {_stopped}")

# ── (2/3) full-load gate — also full-load when the checkpoints are missing ───────
_FULL_LOAD = OVERWRITE or not spark.catalog.tableExists(T_DIM_ITEM) or not _checkpoints_exist()

if _FULL_LOAD:
    print("== FULL LOAD ==")
    spark.sql(f"DROP TABLE IF EXISTS {T_DIM_ITEM}")
    _write_new_table(transform_dim_item(), T_DIM_ITEM)
    print(f"  ✓ seeded {T_DIM_ITEM}")
    # snapshot the source's current Delta version — the stream starts here and the handler
    # skips anything at or below it (that data is the seed, already in Gold).
    _init_ver = {F4101_TBL: current_version(F4101_TBL)}
    print(f"  init versions: {_init_ver}")
    try:
        mssparkutils.fs.rm(CKPT, True)
        print("  checkpoints cleared")
    except Exception as e:
        print(f"  checkpoint clear skipped: {e}")
    print("✓ full load complete")
else:
    print("== RESUME from checkpoint ==")
    _init_ver = {}   # .get(src, -1) -> -1; no version filtering, checkpoint drives


# In[6]:


# In[6]:


# =============================================================================
# STREAM BATCH HANDLER  (structure from nb_silver_to_gold_eso7_v2)
#   init_ver = the source's Delta version at full-load time. Any batch row with
#   _commit_version <= init_ver is seed data (already in Gold via the full load), so
#   the handler skips it. On resume (init_ver = -1) nothing is filtered — the
#   checkpoint drives the offset. The handler then acts only on the real change rows
#   (_change_type insert / update_postimage / delete) and CDC-writes to Gold.
# =============================================================================
def make_dim_item_handler(init_ver):
    def handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        if init_ver >= 0:
            batch_df = batch_df.filter(F.col("_commit_version") > init_ver)
        if batch_df.rdd.isEmpty():
            return
        up   = (batch_df.filter(F.col("_change_type").isin("insert", "update_postimage"))
                .select("identifier_short_item").where(F.col("identifier_short_item").isNotNull()).distinct())
        dele = (batch_df.filter(F.col("_change_type") == "delete")
                .select("identifier_short_item").where(F.col("identifier_short_item").isNotNull()).distinct())
        n = upsert_dim_item(up, dele)   # MERGE upsert + MERGE delete
        print(f"[{F4101_TBL[:12]}] dim_item batch={batch_id} upserts={n}")
    return handler


# In[7]:


# In[7]:


# =============================================================================
# START STREAM — Silver Change Data Feed -> foreachBatch -> CDC write, every 30 s.
# The stream starts at init_ver (full load) — the seed-time version, which EXISTS and
# (with CDF enabled) carries change data. The first batch is that seed version's own
# changes, which the handler discards via `_commit_version <= init_ver`; everything
# after is processed.
#   • startingVersion must be an EXISTING version: init_ver + 1 is beyond the latest at
#     seed time and Delta rejects it ("Cannot time travel to version N").
#   • init_ver must have CDF recorded (delta.enableChangeDataFeed=true at or before it),
#     otherwise Delta raises DELTA_MISSING_CHANGE_DATA for that version.
# On resume (init_ver = -1) the checkpoint's committed offset drives; startingVersion is a
# fallback set to the source's CURRENT version (never 0 — version 0 predates CDF enablement).
# REQUIRES delta.enableChangeDataFeed = true on the source (F4101).
# =============================================================================
def _start_ver(iv, tbl):
    """The version the stream starts at. Full load: init_ver (the seed-time version — it
    exists and carries CDF); the handler skips _commit_version <= init_ver. Resume (iv < 0):
    startingVersion is ignored once the checkpoint has a committed offset, but if Delta ever
    has to cold-start (an offset-less checkpoint slipped through) it must NOT read from 0 —
    version 0 predates CDF enablement and raises DELTA_MISSING_CHANGE_DATA. So fall back to
    the source's CURRENT version, which always exists and is >= the version CDF was enabled
    at, hence always a valid CDF start."""
    return iv if iv >= 0 else current_version(tbl)

iv_item = _init_ver.get(F4101_TBL, -1)
_sv_item = _start_ver(iv_item, F4101_TBL)
(spark.readStream.format("delta")
     .option("readChangeFeed",  "true")
     .option("startingVersion", _sv_item)
     .table(sname(F4101_TBL))
 .writeStream
     .foreachBatch(make_dim_item_handler(iv_item))
     .option("checkpointLocation", f"{CKPT}/dim__{F4101_TBL}")
     .trigger(**TRIGGER)
     .queryName("dim__" + F4101_TBL)
     .start())
print(f"  dim__{F4101_TBL}  startingVersion={_sv_item}  init_ver={iv_item}")

print(f"== started 1 stream — continuous, trigger {TRIGGER}. Target {T_DIM_ITEM}. ==")
spark.streams.awaitAnyTermination()

