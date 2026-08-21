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
# Source:     lh_jde_silver.cdf.f41003_unit_of_measure_standard_conversion
# Checkpoint: Files/checkpoints/dim_uom_conversion
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
# UPSERT/DELETE pattern:
#   · Inserts and updates → MERGE whenMatchedUpdate / whenNotMatchedInsert
#   · Hard deletes (_change_type = 'delete') → MERGE whenMatchedDelete
#   · Soft deletes (is_delete = 1 via update_postimage) → same delete path
#   · Soft deletes checked on the pre-filter batch to catch rows where
#     conv_factor = 0 (excluded from fwd/rev filters).
#
# MODE:
#   · Currently runs as AvailableNow batch (processes all pending CDF changes
#     then exits).  Schedule via SJD twice a day.
#   · To switch to continuous streaming: uncomment TRIGGER and the
#     processingTime trigger line; comment out the availableNow trigger line;
#     uncomment awaitAnyTermination and comment out awaitTermination.
#
# FIRST RUN:     set MANUAL_OVERWRITE = True  → full load + clear checkpoint
# EVERY RESTART: set MANUAL_OVERWRITE = False → resumes from checkpoint
# ============================================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------
SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"
CKPT_ROOT     = "Files/checkpoints/dim_uom_conversion"
# TRIGGER = "30 seconds"   # streaming mode — uncomment to re-enable

MANUAL_OVERWRITE = True

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

F41003 = "f41003_unit_of_measure_standard_conversion"

def drop_deleted(df):
    if "is_delete" in df.columns:
        return df.filter(F.col("is_delete").isNull() | (F.col("is_delete") != 1))
    return df

def soft_del(df):
    if "is_delete" in df.columns:
        return df.filter((F.col("_change_type") == "update_postimage") & (F.col("is_delete") == 1))
    return df.filter(F.lit(False))

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

# ----------------------------------------------------------------------------
# 3) HELPERS
# ----------------------------------------------------------------------------
def current_version(src):
    return spark.sql("DESCRIBE HISTORY {}".format(sname(src))).select(F.max("version")).first()[0]

_INIT_VER_FILE = "Files/checkpoints/dim_uom_conversion/_init_versions.json"


def _save_init_ver(iv):
    import json
    try:
        mssparkutils.fs.put(_INIT_VER_FILE, json.dumps({"f41003": iv}), True)
    except Exception as e:
        print("  [warn] could not save init_ver: {}".format(e))


def _load_init_ver():
    import json
    try:
        return json.loads(mssparkutils.fs.head(_INIT_VER_FILE, 4096)).get("f41003", -1)
    except Exception:
        return -1

def seed_dim(name, df, force=False):
    fq = gname(name)
    if spark.catalog.tableExists(fq) and not force:
        print("  SKIP  {} (exists)".format(name)); return
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").option("delta.enableChangeDataFeed", "true")
       .saveAsTable(fq))
    print("  SEED  {} rows={}".format(name, df.count()))

# ----------------------------------------------------------------------------
# 4) STREAM HANDLER
# ----------------------------------------------------------------------------
def make_dim_uom_conversion_handler(init_ver):
    def handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        if init_ver >= 0:
            batch_df = batch_df.filter(F.col("_commit_version") > init_ver)
        if batch_df.rdd.isEmpty():
            return
        changed = batch_df.filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
        fwd = changed.filter((F.trim(F.col("related_uom")) == "TN") & (F.col("conversion_factor") != 0))
        rev = changed.filter((F.trim(F.col("uom")) == "TN") & (F.col("conversion_factor") != 0))
        upserts = (drop_deleted(fwd.filter(F.col("_change_type").isin("insert", "update_postimage")))
                      .select(F.trim(F.col("uom")).alias("from_uom"),
                              F.col("conversion_factor").cast("double").alias("std_factor"))
                   .unionByName(
                   drop_deleted(rev.filter(F.col("_change_type").isin("insert", "update_postimage")))
                      .select(F.trim(F.col("related_uom")).alias("from_uom"),
                              (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("std_factor")))
                   .dropDuplicates(["from_uom"]))
        # soft-deleted rows on changed (before fwd/rev filter) to avoid missing rows with conv_factor=0
        _sd = soft_del(changed)
        deletes = (fwd.filter(F.col("_change_type") == "delete")
                      .select(F.trim(F.col("uom")).alias("from_uom"))
                   .unionByName(
                   rev.filter(F.col("_change_type") == "delete")
                      .select(F.trim(F.col("related_uom")).alias("from_uom")))
                   .unionByName(
                   _sd.filter(F.trim(F.col("related_uom")) == "TN")
                      .select(F.trim(F.col("uom")).alias("from_uom")))
                   .unionByName(
                   _sd.filter(F.trim(F.col("uom")) == "TN")
                      .select(F.trim(F.col("related_uom")).alias("from_uom")))
                   .distinct())
        dt = DeltaTable.forName(spark, gname("dim_uom_conversion"))
        if not upserts.rdd.isEmpty():
            (dt.alias("t").merge(upserts.alias("s"), "t.from_uom = s.from_uom")
               .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        if not deletes.rdd.isEmpty():
            (dt.alias("t").merge(deletes.alias("s"), "t.from_uom = s.from_uom")
               .whenMatchedDelete().execute())
        print("[F41003] dim_uom_conversion updated batch={}".format(batch_id))
    return handler

# ----------------------------------------------------------------------------
# 5) RUN
# ----------------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

for _q in list(spark.streams.active):
    if _q.name == "dim__f41003":
        _q.stop()
        print("Stopped leftover stream: dim__f41003")

def _checkpoints_exist():
    try:
        return bool(mssparkutils.fs.ls(CKPT_ROOT))
    except Exception:
        return False

if MANUAL_OVERWRITE or not spark.catalog.tableExists(gname("dim_uom_conversion")) or not _checkpoints_exist():
    print("== FULL LOAD ==")
    seed_dim("dim_uom_conversion", build_dim_uom_conversion(), force=True)
    iv = current_version(F41003)
    print("  init_ver={}".format(iv))
    _save_init_ver(iv)
    if _checkpoints_exist():
        try:
            mssparkutils.fs.rm(CKPT_ROOT, True)
            print("  checkpoints cleared")
        except Exception as e:
            print("  checkpoint clear failed: {}".format(e))
    else:
        print("  no existing checkpoint to clear (first run)")
else:
    print("== resuming from checkpoint ==")
    seed_dim("dim_uom_conversion", build_dim_uom_conversion())
    iv = _load_init_ver()

_sv = iv if iv >= 0 else "latest"
query = (
    spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", _sv)
    .table(sname(F41003))
    .writeStream
    .foreachBatch(make_dim_uom_conversion_handler(iv))
    .option("checkpointLocation", CKPT_ROOT)
    # .trigger(processingTime=TRIGGER)   # streaming: uncomment + comment availableNow to re-enable
    .trigger(availableNow=True)
    .queryName("dim__f41003")
    .start()
)
print("  dim__f41003  startingVersion={}  init_ver={}".format(_sv, iv))
# spark.streams.awaitAnyTermination()   # streaming: uncomment to re-enable
query.awaitTermination()
print("== batch run complete ==")

