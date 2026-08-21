#!/usr/bin/env python
# coding: utf-8

# ## nb_eso4_gold_dim_udc
# 
# null

# In[1]:


# In[1]:

from pyspark.sql import functions as F
from delta.tables import DeltaTable

GOLD_SCHEMA = "lh_jde_gold.rpt"

ENV             = "dev"
TRIGGER         = {"processingTime": "30 seconds"}
CKPT            = f"Files/checkpoints/eso4_dim_udc_{ENV}"   # OWN root — independent of other notebooks
OVERWRITE       = True    # ⚠ ONE-OFF full reprocess — set back to False after a healthy run

SRC_SCHEMA    = "jde_cdc"     # CDF must be enabled on F0005.
SRC_LAKEHOUSE = "lh_jde_silver"
F0005_TBL     = "f0005_user_defined_code_values"

# UDC selectors (INFERRED system/type — confirm; flagged in design §5).
SIC_SYS,   SIC_TYPE   = "01", "SC"   # SIC codes  -> dim_sic
STATE_SYS, STATE_TYPE = "00", "S"    # State/Province codes -> dim_state

T_DIM_SIC   = f"{GOLD_SCHEMA}.dim_sic"
T_DIM_STATE = f"{GOLD_SCHEMA}.dim_state"

print(f"ESO4 Gold dim_udc processor — trigger {TRIGGER}  targets {T_DIM_SIC}, {T_DIM_STATE}")


# In[2]:


# In[2]:

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

def _udc_dim(sys_code, type_code, key_alias, desc_alias, restrict_keys=None):
    f0005 = (load_silver_table(F0005_TBL)
             .where((F.trim(F.col("product_code")) == sys_code) &
                    (F.trim(F.col("user_defined_codes")) == type_code)))
    if restrict_keys is not None:
        f0005 = f0005.join(restrict_keys.alias("r"),
                           F.trim(f0005["user_defined_code"]) == F.col(f"r.{key_alias}"), "left_semi")
    return (f0005.select(F.trim(F.col("user_defined_code")).alias(key_alias),   # DRKY
                         F.trim(F.col("description_001")).alias(desc_alias))     # DRDL01
            .where(F.col(key_alias) != "")
            .dropDuplicates([key_alias]))

def transform_dim_sic(restrict_keys=None):
    return _udc_dim(SIC_SYS, SIC_TYPE, "sic_code", "sic_description", restrict_keys)

def transform_dim_state(restrict_keys=None):
    return _udc_dim(STATE_SYS, STATE_TYPE, "state_code", "state_name", restrict_keys)


# In[4]:


# In[4]:

def _write_new_table(df, target, cdf=True):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if cdf:
        w = w.option("delta.enableChangeDataFeed", "true")
    w.saveAsTable(target)

def _upsert_dim(target, key_col, transform, change_keys, delete_keys):
    """MERGE upsert changed codes + MERGE delete removed codes. `*_keys` carry `key_col`."""
    n = 0
    if not change_keys.rdd.isEmpty():
        src = transform(restrict_keys=change_keys)
        (DeltaTable.forName(spark, target).alias("t")
            .merge(src.alias("s"), f"t.{key_col} = s.{key_col}")
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        n = src.count()
    if not delete_keys.rdd.isEmpty():
        (DeltaTable.forName(spark, target).alias("t")
            .merge(delete_keys.alias("s"), f"t.{key_col} = s.{key_col}")
            .whenMatchedDelete().execute())
    return n


# In[5]:


# In[5]:

_CKPT_PATHS = [f"{CKPT}/dim__{F0005_TBL}"]

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

_STREAM_NAMES = {"dim__" + F0005_TBL}
_stopped = []
for _q in list(spark.streams.active):
    if _q.name in _STREAM_NAMES:
        _q.stop()
        _stopped.append(_q.name)
if _stopped:
    print(f"Stopped leftover streams: {_stopped}")

_FULL_LOAD = (OVERWRITE
              or not spark.catalog.tableExists(T_DIM_SIC)
              or not spark.catalog.tableExists(T_DIM_STATE)
              or not _checkpoints_exist())

if _FULL_LOAD:
    print("== FULL LOAD ==")
    spark.sql(f"DROP TABLE IF EXISTS {T_DIM_SIC}")
    spark.sql(f"DROP TABLE IF EXISTS {T_DIM_STATE}")
    _write_new_table(transform_dim_sic(),   T_DIM_SIC)
    _write_new_table(transform_dim_state(), T_DIM_STATE)
    print(f"  ✓ seeded {T_DIM_SIC} + {T_DIM_STATE}")
    _init_ver = {F0005_TBL: current_version(F0005_TBL)}
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

def make_dim_udc_handler(init_ver):
    def handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        if init_ver >= 0:
            batch_df = batch_df.filter(F.col("_commit_version") > init_ver)
        if batch_df.rdd.isEmpty():
            return

        def _keys(sys_code, type_code, key_alias):
            sel = (batch_df.where((F.trim(F.col("product_code")) == sys_code) &
                                  (F.trim(F.col("user_defined_codes")) == type_code)))
            up = (sel.filter(F.col("_change_type").isin("insert", "update_postimage"))
                     .select(F.trim(F.col("user_defined_code")).alias(key_alias))
                     .where(F.col(key_alias) != "").distinct())
            dele = (sel.filter(F.col("_change_type") == "delete")
                       .select(F.trim(F.col("user_defined_code")).alias(key_alias))
                       .where(F.col(key_alias) != "").distinct())
            return up, dele

        sic_up, sic_del = _keys(SIC_SYS, SIC_TYPE, "sic_code")
        n_sic = _upsert_dim(T_DIM_SIC, "sic_code", transform_dim_sic, sic_up, sic_del)

        st_up, st_del = _keys(STATE_SYS, STATE_TYPE, "state_code")
        n_state = _upsert_dim(T_DIM_STATE, "state_code", transform_dim_state, st_up, st_del)

        print(f"[{F0005_TBL[:12]}] dim_udc batch={batch_id} sic_upserts={n_sic} state_upserts={n_state}")
    return handler


# In[7]:


# In[7]:

def _start_ver(iv, tbl):
    """Full load: init_ver (exists, carries CDF; handler skips <= it). Resume (iv < 0): fall
    back to the source's CURRENT version, never 0 (v0 predates CDF)."""
    return iv if iv >= 0 else current_version(tbl)

iv_udc = _init_ver.get(F0005_TBL, -1)
_sv_udc = _start_ver(iv_udc, F0005_TBL)
(spark.readStream.format("delta")
     .option("readChangeFeed",  "true")
     .option("startingVersion", _sv_udc)
     .table(sname(F0005_TBL))
 .writeStream
     .foreachBatch(make_dim_udc_handler(iv_udc))
     .option("checkpointLocation", f"{CKPT}/dim__{F0005_TBL}")
     .trigger(**TRIGGER)
     .queryName("dim__" + F0005_TBL)
     .start())
print(f"  dim__{F0005_TBL}  startingVersion={_sv_udc}  init_ver={iv_udc}")

print(f"== started 1 stream — continuous, trigger {TRIGGER}. Targets {T_DIM_SIC}, {T_DIM_STATE}. ==")
spark.streams.awaitAnyTermination()

