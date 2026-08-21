#!/usr/bin/env python
# coding: utf-8

# ## nb_eso4_gold_dim_business_unit
# 
# null

# In[1]:


# ## nb_eso4_gold_dim_business_unit
#
# **Gold `dim_business_unit` processor** for Extended Sales Order 4 (Sales Tax reconciliation).
# Builds and continuously refreshes ONE table — `lh_jde_gold.eso4.dim_business_unit` — from the
# Silver business-unit master (F0006) via a single Change Data Feed stream. Supplies the report's
# Plant / Plant Name / Business Stream (MCRP20) attributes; the fact's `plant` FK joins here.
#
# Streaming model is identical to ESO1's nb/nb_eso1_gold_dim_item.py (CDF incremental):
#   • continuous trigger(processingTime="30 seconds") + awaitAnyTermination
#   • make_dim_bu_handler(init_ver): skip the seed rows (_commit_version <= init_ver), then act
#     only on real change rows (_change_type insert/update_postimage/delete) and CDC-write to
#     Gold (MERGE upsert + MERGE delete)
#   • per-stream checkpoint gate (offsets/ committed) + startingVersion=init_ver knob
# Design: eso4/docs/ESO4_gold_layer_design.md


# In[1]:


# =============================================================================
# CONFIG + CONSTANTS  (names per Fabric_Naming_Convention_Guidelines.pdf)
# =============================================================================
from pyspark.sql import functions as F
from delta.tables import DeltaTable

GOLD_SCHEMA = "lh_jde_gold.rpt"

ENV             = "dev"
TRIGGER         = {"processingTime": "30 seconds"}
CKPT            = f"Files/checkpoints/eso4_dim_bu_{ENV}"   # OWN root — independent of other notebooks
OVERWRITE       = True    # ⚠ ONE-OFF full reprocess — set back to False after a healthy run

SRC_SCHEMA    = "jde_cdc"     # CDF must be enabled on F0006.
SRC_LAKEHOUSE = "lh_jde_silver"
F0006_TBL     = "f0006_business_unit_master"

T_DIM_BU  = f"{GOLD_SCHEMA}.dim_business_unit"

print(f"ESO4 Gold dim_business_unit processor — trigger {TRIGGER}  target {T_DIM_BU}")


# In[2]:


# In[2]:


# =============================================================================
# HELPERS  (identical to ESO1 dim notebook)
# =============================================================================
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def sname(table_name):
    return f"{SRC_LAKEHOUSE}.{SRC_SCHEMA}.{table_name}"

def load_silver_table(table_name):
    df = spark.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

def current_version(silver_table):
    return spark.sql(f"DESCRIBE HISTORY {sname(silver_table)}").select(F.max("version")).first()[0]


# In[3]:


# In[3]:


# =============================================================================
# DIM transform — dim_business_unit (F0006, natural PK = cost_center / MCMCU)
# =============================================================================
def transform_dim_bu(restrict_bu=None):
    f0006 = load_silver_table(F0006_TBL)
    if restrict_bu is not None:
        f0006 = f0006.join(restrict_bu.alias("r"),
                           f0006["cost_center"] == F.col("r.cost_center"), "left_semi")
    return (f0006.select(
                F.trim(F.col("cost_center")).alias("business_unit"),                    # MCMCU
                F.col("description_001").alias("plant_name"),                           # MCDL01
                F.trim(F.col("category_code_cost_ct_020")).alias("business_stream_code"),  # MCRP20
                F.col("company").alias("company"),                                      # MCCO
                F.col("state").alias("state"))                                          # MCADDS
            .dropDuplicates(["business_unit"]))


# In[4]:


# In[4]:


# =============================================================================
# CDC WRITE HELPERS — NO audit columns (CDF concept from nb_silver_to_gold_eso7_v2)
#   • dim : MERGE upsert (whenMatchedUpdateAll / whenNotMatchedInsertAll) + MERGE delete.
# =============================================================================
def _write_new_table(df, target, cdf=True):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if cdf:
        w = w.option("delta.enableChangeDataFeed", "true")
    w.saveAsTable(target)

def upsert_dim_bu(bu_change, bu_delete):
    """CDC for dim_business_unit: MERGE upsert changed BUs + MERGE delete removed BUs.
    `bu_*` carry `cost_center`. Returns upserted rows."""
    n = 0
    if not bu_change.rdd.isEmpty():
        src = transform_dim_bu(restrict_bu=bu_change)
        (DeltaTable.forName(spark, T_DIM_BU).alias("t")
            .merge(src.alias("s"), "t.business_unit = s.business_unit")
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        n = src.count()
    if not bu_delete.rdd.isEmpty():
        d = bu_delete.select(F.trim(F.col("cost_center")).alias("business_unit")).distinct()
        (DeltaTable.forName(spark, T_DIM_BU).alias("t")
            .merge(d.alias("s"), "t.business_unit = s.business_unit")
            .whenMatchedDelete().execute())
    return n


# In[5]:


# In[5]:


# =============================================================================
# FULL LOAD vs RESUME  (streaming approach from nb_silver_to_gold_eso7_v2 / ESO1)
# =============================================================================
_CKPT_PATHS = [f"{CKPT}/dim__{F0006_TBL}"]

def _checkpoints_exist():
    """True iff EVERY per-stream checkpoint has a COMMITTED offset (offsets/ non-empty), so an
    incomplete checkpoint forces a FULL LOAD rather than cold-starting the CDF reader at
    startingVersion=0 (v0 predates CDF enablement -> DELTA_MISSING_CHANGE_DATA)."""
    for p in _CKPT_PATHS:
        try:
            if not mssparkutils.fs.ls(f"{p}/offsets"):
                return False
        except Exception:
            return False
    return True

_STREAM_NAMES = {"dim__" + F0006_TBL}
_stopped = []
for _q in list(spark.streams.active):
    if _q.name in _STREAM_NAMES:
        _q.stop()
        _stopped.append(_q.name)
if _stopped:
    print(f"Stopped leftover streams: {_stopped}")

_FULL_LOAD = OVERWRITE or not spark.catalog.tableExists(T_DIM_BU) or not _checkpoints_exist()

if _FULL_LOAD:
    print("== FULL LOAD ==")
    spark.sql(f"DROP TABLE IF EXISTS {T_DIM_BU}")
    _write_new_table(transform_dim_bu(), T_DIM_BU)
    print(f"  ✓ seeded {T_DIM_BU}")
    _init_ver = {F0006_TBL: current_version(F0006_TBL)}
    print(f"  init versions: {_init_ver}")
    try:
        mssparkutils.fs.rm(CKPT, True)
        print("  checkpoints cleared")
    except Exception as e:
        print(f"  checkpoint clear skipped: {e}")
    print("✓ full load complete")
else:
    print("== RESUME from checkpoint ==")
    _init_ver = {}


# In[6]:


# In[6]:


# =============================================================================
# STREAM BATCH HANDLER  (structure from nb_silver_to_gold_eso7_v2 / ESO1)
# =============================================================================
def make_dim_bu_handler(init_ver):
    def handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        if init_ver >= 0:
            batch_df = batch_df.filter(F.col("_commit_version") > init_ver)
        if batch_df.rdd.isEmpty():
            return
        up   = (batch_df.filter(F.col("_change_type").isin("insert", "update_postimage"))
                .select("cost_center").where(F.col("cost_center").isNotNull()).distinct())
        dele = (batch_df.filter(F.col("_change_type") == "delete")
                .select("cost_center").where(F.col("cost_center").isNotNull()).distinct())
        n = upsert_dim_bu(up, dele)
        print(f"[{F0006_TBL[:12]}] dim_business_unit batch={batch_id} upserts={n}")
    return handler


# In[7]:


# In[7]:


# =============================================================================
# START STREAM — Silver Change Data Feed -> foreachBatch -> CDC write, every 30 s.
# REQUIRES delta.enableChangeDataFeed = true on the source (F0006).
# =============================================================================
def _start_ver(iv, tbl):
    """Full load: init_ver (exists, carries CDF; handler skips <= it). Resume (iv < 0): fall
    back to the source's CURRENT version, never 0 (v0 predates CDF)."""
    return iv if iv >= 0 else current_version(tbl)

iv_bu = _init_ver.get(F0006_TBL, -1)
_sv_bu = _start_ver(iv_bu, F0006_TBL)
(spark.readStream.format("delta")
     .option("readChangeFeed",  "true")
     .option("startingVersion", _sv_bu)
     .table(sname(F0006_TBL))
 .writeStream
     .foreachBatch(make_dim_bu_handler(iv_bu))
     .option("checkpointLocation", f"{CKPT}/dim__{F0006_TBL}")
     .trigger(**TRIGGER)
     .queryName("dim__" + F0006_TBL)
     .start())
print(f"  dim__{F0006_TBL}  startingVersion={_sv_bu}  init_ver={iv_bu}")

print(f"== started 1 stream — continuous, trigger {TRIGGER}. Target {T_DIM_BU}. ==")
spark.streams.awaitAnyTermination()

