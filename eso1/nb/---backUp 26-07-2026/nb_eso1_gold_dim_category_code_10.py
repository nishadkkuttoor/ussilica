#!/usr/bin/env python
# coding: utf-8

# ## nb_eso1_gold_dim_category_code_10
#
# **Gold UDC-dimension processor** for ESO1 — SOP0027 Commission.
# Builds and continuously refreshes ONE small reference dimension from ONE Silver source
# (the user-defined-code values, F0005) via a single Change Data Feed stream:
#   • `lh_jde_gold.rpt.dim_category_code_10` — UDC 01/10 : category_code_10 -> category_code_10_desc
#
# Address-book category codes AC01–AC30 edit against UDC system '01', types '01'–'30'
# (standard JDE), so ABAC10 (`category_code_10` on the sold-to F0101) resolves against 01/10.
# `fact_sales_commission` stores the raw FK code (`category_code_10`); this dim resolves the
# description in the Direct Lake model (fact.category_code_10 -> dim_category_code_10.category_code_10).
# Same F0005 lookup shape ESO4 uses for dim_sic (01/SC) / dim_state (00/S) and ESO7 for dim_status (40/AT).
#
# Streaming model is identical to nb_eso4_gold_dim_udc.py (CDF incremental):
#   • continuous trigger(processingTime="30 seconds") + awaitAnyTermination
#   • make_dim_handler(init_ver): skip the seed rows (_commit_version <= init_ver), then act
#     only on real change rows (_change_type insert/update_postimage/delete), filter to UDC
#     01/10, and CDC-write the dim (MERGE upsert + MERGE delete)
#   • per-stream checkpoint gate (offsets/ committed) + startingVersion=init_ver knob
# Design: eso1/docs/ESO1_gold_layer_design.md §4.6


# In[1]:


# =============================================================================
# CONFIG + CONSTANTS  (names per Fabric_Naming_Convention_Guidelines.pdf)
# =============================================================================
from pyspark.sql import functions as F
from delta.tables import DeltaTable

GOLD_SCHEMA = "lh_jde_gold.rpt"

ENV       = "dev"
TRIGGER   = {"processingTime": "30 seconds"}
CKPT      = f"Files/checkpoints/eso1_dim_cat10_{ENV}"   # OWN root — independent of other notebooks
OVERWRITE = True    # ⚠ ONE-OFF full reprocess — set back to False after a healthy run

SILVER_SCHEMA    = "jde"     # CDF must be enabled on F0005.
SRC_LAKEHOUSE = "lh_jde_silver"
F0005_TBL     = "f0005_user_defined_code_values"

# UDC selector — address-book category code 10 = UDC 01/10 (standard JDE: AC01–AC30 -> 01/01–01/30).
# Confirm if US Silica remapped AC10's edit UDC; flagged like ESO4's inferred UDCs (design §5).
CAT10_SYS, CAT10_TYPE = "01", "10"

T_DIM = f"{GOLD_SCHEMA}.dim_category_code_10"

print(f"ESO1 Gold dim_category_code_10 processor — trigger {TRIGGER}  target {T_DIM}")


# In[2]:


# =============================================================================
# HELPERS  (identical to the ESO4 dim_udc notebook)
# =============================================================================
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def sname(table_name):
    return f"{SRC_LAKEHOUSE}.{SILVER_SCHEMA}.{table_name}"

def load_silver_table(table_name):
    df = spark.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

def current_version(silver_table):
    return spark.sql(f"DESCRIBE HISTORY {sname(silver_table)}").select(F.max("version")).first()[0]


# In[3]:


# =============================================================================
# DIM transform — F0005 UDC 01/10 lookup. Natural PK = the UDC value (DRKY) within its
# system/type, so the transform filters product_code/user_defined_codes first, then keys on
# trim(user_defined_code). `restrict_keys` (carrying category_code_10) narrows the CDC recompute.
# =============================================================================
def transform_dim(restrict_keys=None):
    f0005 = (load_silver_table(F0005_TBL)
             .where((F.trim(F.col("product_code")) == CAT10_SYS) &
                    (F.trim(F.col("user_defined_codes")) == CAT10_TYPE)))
    if restrict_keys is not None:
        f0005 = f0005.join(restrict_keys.alias("r"),
                           F.trim(f0005["user_defined_code"]) == F.col("r.category_code_10"), "left_semi")
    return (f0005.select(F.trim(F.col("user_defined_code")).alias("category_code_10"),        # DRKY
                         F.trim(F.col("description_001")).alias("category_code_10_desc"))      # DRDL01
            .where(F.col("category_code_10") != "")
            .dropDuplicates(["category_code_10"]))


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


# =============================================================================
# FULL LOAD vs RESUME  (streaming approach from nb_eso4_gold_dim_udc / ESO1)
# =============================================================================
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
              or not spark.catalog.tableExists(T_DIM)
              or not _checkpoints_exist())

if _FULL_LOAD:
    print("== FULL LOAD ==")
    spark.sql(f"DROP TABLE IF EXISTS {T_DIM}")
    _write_new_table(transform_dim(), T_DIM)
    print(f"  ✓ seeded {T_DIM}")
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


# =============================================================================
# STREAM BATCH HANDLER  (structure from the ESO4 dim_udc notebook)
#   One F0005 stream feeds the dim: keep only UDC 01/10 change rows, upsert / delete.
# =============================================================================
def make_dim_handler(init_ver):
    def handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        if init_ver >= 0:
            batch_df = batch_df.filter(F.col("_commit_version") > init_ver)
        if batch_df.rdd.isEmpty():
            return

        sel = batch_df.where((F.trim(F.col("product_code")) == CAT10_SYS) &
                             (F.trim(F.col("user_defined_codes")) == CAT10_TYPE))
        up = (sel.filter(F.col("_change_type").isin("insert", "update_postimage"))
                 .select(F.trim(F.col("user_defined_code")).alias("category_code_10"))
                 .where(F.col("category_code_10") != "").distinct())
        dele = (sel.filter(F.col("_change_type") == "delete")
                   .select(F.trim(F.col("user_defined_code")).alias("category_code_10"))
                   .where(F.col("category_code_10") != "").distinct())

        n = _upsert_dim(T_DIM, "category_code_10", transform_dim, up, dele)
        print(f"[{F0005_TBL[:12]}] dim_category_code_10 batch={batch_id} upserts={n}")
    return handler


# In[7]:


# =============================================================================
# START STREAM — Silver Change Data Feed -> foreachBatch -> CDC write, every 30 s.
# REQUIRES delta.enableChangeDataFeed = true on the source (F0005).
# =============================================================================
def _start_ver(iv, tbl):
    """Full load: init_ver (exists, carries CDF; handler skips <= it). Resume (iv < 0): fall
    back to the source's CURRENT version, never 0 (v0 predates CDF)."""
    return iv if iv >= 0 else current_version(tbl)

iv_dim = _init_ver.get(F0005_TBL, -1)
_sv_dim = _start_ver(iv_dim, F0005_TBL)
(spark.readStream.format("delta")
     .option("readChangeFeed",  "true")
     .option("startingVersion", _sv_dim)
     .table(sname(F0005_TBL))
 .writeStream
     .foreachBatch(make_dim_handler(iv_dim))
     .option("checkpointLocation", f"{CKPT}/dim__{F0005_TBL}")
     .trigger(**TRIGGER)
     .queryName("dim__" + F0005_TBL)
     .start())
print(f"  dim__{F0005_TBL}  startingVersion={_sv_dim}  init_ver={iv_dim}")

print(f"== started 1 stream — continuous, trigger {TRIGGER}. Target {T_DIM}. ==")
spark.streams.awaitAnyTermination()
