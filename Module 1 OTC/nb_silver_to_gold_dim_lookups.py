#!/usr/bin/env python
# coding: utf-8

# ## nb_silver_to_gold_dim_lookups
# 
# New notebook

# In[1]:


#!/usr/bin/env python
# coding: utf-8

# Fabric notebook · nb_silver_to_gold_dim_f0005
# ============================================================================
# Builds and keeps live all UDC-based dimension tables from F0004 + F0005.
#
# ── HOW TO ADD A NEW DIMENSION ───────────────────────────────────────────────
# Add ONE entry to UDC_DIMS below.  No other code changes needed.
# Add the new table name to MANUAL_OVERWRITE (set True on first run, then False).
# ─────────────────────────────────────────────────────────────────────────────
#
# Currently registered dims:
#   lh_jde_gold.rpt.dim_mode_of_transport   (F0004 00/TM)
#   lh_jde_gold.rpt.dim_status_code_next    (F0004 40/AT)
#
# Source streams:
#   F0005 CDF → individual code values added / changed / deleted
#   F0004 CDF → UDC type definition changed → full rebuild of affected dim
#
# Checkpoint roots:
#   Files/checkpoints/dim_f0005   (F0005 stream)
#   Files/checkpoints/dim_f0004   (F0004 stream)
#
# UPSERT/DELETE pattern:
#   · Inserts and updates → MERGE whenMatchedUpdate / whenNotMatchedInsert
#   · Hard deletes (_change_type = 'delete') → MERGE whenMatchedDelete
#   · Soft deletes (is_delete = 1 via update_postimage) → same delete path
#
# MODE:
#   · Currently runs as AvailableNow batch (processes all pending CDF changes
#     then exits).  Schedule via SJD twice a day.
#   · To switch to continuous streaming: uncomment TRIGGER and the
#     processingTime trigger lines; comment out the availableNow trigger lines;
#     uncomment awaitAnyTermination and comment out awaitTermination calls.
#
# MANUAL_OVERWRITE (per dim):
#   True  → wipe that Gold table + reseed from Silver
#   False → resume from checkpoint (default)
#   Checkpoint is cleared only when ALL dims are being overwritten.
#   Set all True on first run or after a schema change; set back to False after.
# ============================================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------
SILVER_LH = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH = "lh_jde_gold"
GOLD_SCHEMA = "rpt"
CKPT_F0005 = "Files/checkpoints/dim_f0005"
CKPT_F0004 = "Files/checkpoints/dim_f0004"
# TRIGGER = "30 seconds"   # streaming mode — uncomment to re-enable

F0004 = "f0004_user_defined_code_types"
F0005 = "f0005_user_defined_code_values"

# ── UDC Dimension Registry ───────────────────────────────────────────────────
# Add one dict per dimension.  All build / stream / MERGE logic is generic.
#
#   product_code  F0004.product_code  (dtsy) — the JDE system code
#   udc_type      F0004.user_defined_codes (dtrt) — the UDC category code
#   table         Gold table name (without schema prefix)
#   key_col       Gold column name for the code value
#   desc_col      Gold column name for the description
#   key_type      "string" or "long" — must match the related fact column type
# ─────────────────────────────────────────────────────────────────────────────
UDC_DIMS = [
    {
        "product_code": "00",
        "udc_type": "TM",
        "table": "dim_mode_of_transport",
        "key_col": "modeoftransport_code",
        "desc_col": "modeoftransport_description",
        "key_type": "string",
    },
    {
        "product_code": "40",
        "udc_type": "AT",
        "table": "dim_status_code_next",
        "key_col": "statuscodenext_code",
        "desc_col": "statuscodenext_description",
        "key_type": "long",
    },
    {
        "product_code": "01",
        "udc_type": "SC",
        "table": "dim_sic",
        "key_col": "standardindustrycode_code",
        "desc_col": "standardindustrycode_description",
        "key_type": "string",
    },
    {
        "product_code": "00",
        "udc_type": "S",
        "table": "dim_state",
        "key_col": "state_code",
        "desc_col": "state_description",
        "key_type": "string",
    },
    {
        "product_code": "01",
        "udc_type": "10",
        "table": "dim_category_code_10",
        "key_col": "reportcodeaddbook010_code",
        "desc_col": "reportcodeaddbook010_description",
        "key_type": "string",
    },
    {
        "product_code": "01",
        "udc_type": "05",
        "table": "dim_category_code_05",
        "key_col": "reportcodeaddbook005_code",
        "desc_col": "reportcodeaddbook005_description",
        "key_type": "string",
    },
    {
        "product_code": "42",
        "udc_type": "FR",
        "table": "dim_freight_handling_code",
        "key_col": "freighthandlingcode_code",
        "desc_col": "freighthandlingcode_description",
        "key_type": "string",
    },
]

MANUAL_OVERWRITE = {
    "dim_mode_of_transport": False,
    "dim_status_code_next": False,
    "dim_sic": False,
    "dim_state": False,
    "dim_category_code_10": False,
    "dim_category_code_05": False,
    "dim_freight_handling_code": False,
}


def sname(t):
    return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)


def gname(t):
    return "{}.{}.{}".format(GOLD_LH, GOLD_SCHEMA, t)


def drop_deleted(df):
    if "is_delete" in df.columns:
        return df.filter(F.col("is_delete").isNull() | (F.col("is_delete") != 1))
    return df


def soft_del(df):
    if "is_delete" in df.columns:
        return df.filter(
            (F.col("_change_type") == "update_postimage") & (F.col("is_delete") == 1)
        )
    return df.filter(F.lit(False))


# ----------------------------------------------------------------------------
# 2) GENERIC BUILDER
# ----------------------------------------------------------------------------
def _udc_base():
    """F0004 LEFT JOIN F0005 — shared base for all UDC dimension builds."""
    f4 = drop_deleted(spark.read.table(sname(F0004))).alias("f4")
    f5 = drop_deleted(spark.read.table(sname(F0005))).alias("f5")
    return f4.join(
        f5,
        (F.trim(F.col("f4.product_code")) == F.trim(F.col("f5.product_code")))
        & (
            F.trim(F.col("f4.user_defined_codes"))
            == F.trim(F.col("f5.user_defined_codes"))
        ),
        "left",
    )


def build_udc_dim(cfg):
    """Build a full Gold DataFrame for one UDC dim from the registry config."""
    key_expr = F.trim(F.col("f5.user_defined_code"))
    if cfg["key_type"] == "long":
        key_expr = key_expr.cast("long")
    return (
        _udc_base()
        .filter(
            (F.trim(F.col("f4.product_code")) == cfg["product_code"])
            & (F.trim(F.col("f4.user_defined_codes")) == cfg["udc_type"])
        )
        .select(
            key_expr.alias(cfg["key_col"]),
            F.trim(F.col("f5.description_001")).alias(cfg["desc_col"]),
        )
        .where(F.col(cfg["key_col"]).isNotNull())
        .dropDuplicates([cfg["key_col"]])
    )


# ----------------------------------------------------------------------------
# 3) HELPERS
# ----------------------------------------------------------------------------
def current_version(src):
    return (
        spark.sql("DESCRIBE HISTORY {}".format(sname(src)))
        .select(F.max("version"))
        .first()[0]
    )


_INIT_VER_FILE = "Files/checkpoints/dim_f0005/_init_versions.json"


def _save_init_versions(iv_f0005, iv_f0004):
    import json
    try:
        mssparkutils.fs.put(_INIT_VER_FILE, json.dumps({"f0005": iv_f0005, "f0004": iv_f0004}), True)
    except Exception as e:
        print("  [warn] could not save init versions: {}".format(e))


def _load_init_versions():
    import json
    try:
        return json.loads(mssparkutils.fs.head(_INIT_VER_FILE, 4096))
    except Exception:
        return {}


def seed_dim(name, df, force=False):
    fq = gname(name)
    if spark.catalog.tableExists(fq) and not force:
        print("  SKIP  {} (exists)".format(name))
        return
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true")
        .saveAsTable(fq)
    )
    print("  SEED  {} rows={}".format(name, spark.read.table(fq).count()))


def _ckpt_exists(path):
    try:
        return bool(mssparkutils.fs.ls(path))
    except Exception:
        return False


def _clear_ckpt(path):
    try:
        mssparkutils.fs.rm(path, True)
        print("  checkpoint cleared: {}".format(path))
    except Exception as e:
        print("  checkpoint clear failed ({}): {}".format(path, e))


# ----------------------------------------------------------------------------
# 4) F0005 STREAM HANDLER — code value changes
#    Loops UDC_DIMS; each dim slice is processed independently.
# ----------------------------------------------------------------------------
def make_f0005_handler(init_ver):
    def handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        if init_ver >= 0:
            batch_df = batch_df.filter(F.col("_commit_version") > init_ver)
        if batch_df.rdd.isEmpty():
            return

        for cfg in UDC_DIMS:
            rel = batch_df.filter(
                (F.trim(F.col("product_code")) == cfg["product_code"])
                & (F.trim(F.col("user_defined_codes")) == cfg["udc_type"])
            )
            if rel.rdd.isEmpty():
                continue

            key_expr = F.trim(F.col("user_defined_code"))
            if cfg["key_type"] == "long":
                key_expr = key_expr.cast("long")

            upserts = (
                drop_deleted(
                    rel.filter(F.col("_change_type").isin("insert", "update_postimage"))
                )
                .select(
                    key_expr.alias(cfg["key_col"]),
                    F.trim(F.col("description_001")).alias(cfg["desc_col"]),
                )
                .where(F.col(cfg["key_col"]).isNotNull())
                .dropDuplicates([cfg["key_col"]])
            )
            _hard_del = rel.filter(F.col("_change_type") == "delete").select(
                key_expr.alias(cfg["key_col"])
            )
            _soft_del = soft_del(rel).select(key_expr.alias(cfg["key_col"]))
            deletes = (
                _hard_del.unionByName(_soft_del)
                .where(F.col(cfg["key_col"]).isNotNull())
                .distinct()
            )

            merge_cond = "t.{c} = s.{c}".format(c=cfg["key_col"])
            dt = DeltaTable.forName(spark, gname(cfg["table"]))
            if not upserts.rdd.isEmpty():
                (
                    dt.alias("t")
                    .merge(upserts.alias("s"), merge_cond)
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            if not deletes.rdd.isEmpty():
                (
                    dt.alias("t")
                    .merge(deletes.alias("s"), merge_cond)
                    .whenMatchedDelete()
                    .execute()
                )
            print("[F0005] {} updated batch={}".format(cfg["table"], batch_id))

    return handler


# ----------------------------------------------------------------------------
# 5) F0004 STREAM HANDLER — UDC type definition changes
#    When a F0004 row changes, find which registered dims are affected and
#    do a full rebuild of each.  F0004 changes very rarely (JDE admin only)
#    so a full rebuild per affected dim is acceptable.
# ----------------------------------------------------------------------------
def make_f0004_handler():
    def handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        changed = (
            batch_df.filter(
                F.col("_change_type").isin("insert", "update_postimage", "delete")
            )
            .select(
                F.trim(F.col("product_code")).alias("pc"),
                F.trim(F.col("user_defined_codes")).alias("udc"),
            )
            .distinct()
        )
        changed_pairs = {(r["pc"], r["udc"]) for r in changed.collect()}
        if not changed_pairs:
            return

        for cfg in UDC_DIMS:
            if (cfg["product_code"], cfg["udc_type"]) in changed_pairs:
                print(
                    "[F0004] {} affected — rebuilding batch={}".format(
                        cfg["table"], batch_id
                    )
                )
                seed_dim(cfg["table"], build_udc_dim(cfg), force=True)

    return handler


# ----------------------------------------------------------------------------
# 6) RUN
# ----------------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

for _q in list(spark.streams.active):
    if _q.name in ("dim__mot", "dim__f0005", "dim__f0004"):
        _q.stop()
        print("Stopped leftover stream: {}".format(_q.name))

# ── Per-dim overwrite flags ──────────────────────────────────────────────────
# A dim needs a full load if: explicitly requested, Gold table missing, or
# the shared F0005 checkpoint is missing (first run).
_f0005_ckpt_ok = _ckpt_exists(CKPT_F0005)

_overwrite = {}
for _cfg in UDC_DIMS:
    _tbl = _cfg["table"]
    _overwrite[_tbl] = (
        MANUAL_OVERWRITE.get(_tbl, False)
        or not spark.catalog.tableExists(gname(_tbl))
        or not _f0005_ckpt_ok
    )

_any_overwrite = any(_overwrite.values())
_all_overwrite = all(_overwrite.values())

if _any_overwrite:
    _flags = {t: v for t, v in _overwrite.items()}
    print("== FULL LOAD {} ==".format(_flags))
    for _cfg in UDC_DIMS:
        seed_dim(_cfg["table"], build_udc_dim(_cfg), force=_overwrite[_cfg["table"]])

    if _all_overwrite:
        # All dims rebuilt — safe to reset both checkpoints and record init_ver.
        iv_f0005 = current_version(F0005)
        iv_f0004 = current_version(F0004)
        print("  init_ver  f0005={} f0004={}".format(iv_f0005, iv_f0004))
        if _f0005_ckpt_ok:
            _clear_ckpt(CKPT_F0005)
        else:
            print("  no F0005 checkpoint to clear (first run)")
        if _ckpt_exists(CKPT_F0004):
            _clear_ckpt(CKPT_F0004)
        else:
            print("  no F0004 checkpoint to clear (first run)")
        _save_init_versions(iv_f0005, iv_f0004)
    else:
        # Partial reload — preserve checkpoints for dims that are resuming.
        # The freshly-seeded dim(s) will re-process a small number of F0005
        # records from the checkpoint position via MERGE — idempotent, safe.
        print(
            "  partial reload — checkpoints preserved; streams resume from checkpoint"
        )
        _persisted = _load_init_versions()
        iv_f0005 = _persisted.get("f0005", -1)
        iv_f0004 = _persisted.get("f0004", -1)
else:
    print("== resuming from checkpoint ==")
    for _cfg in UDC_DIMS:
        seed_dim(_cfg["table"], build_udc_dim(_cfg))
    _persisted = _load_init_versions()
    iv_f0005 = _persisted.get("f0005", -1)
    iv_f0004 = _persisted.get("f0004", -1)

# ── Launch F0005 stream ──────────────────────────────────────────────────────
# Always pass startingVersion — same pattern as the module 1 streaming notebook.
# When a valid checkpoint exists Spark ignores this and resumes from checkpoint.
# When the checkpoint is incomplete/missing, uses the persisted init_ver (never
# version 0), so the CDF version-0 error can never occur.
_sv_f0005 = iv_f0005 if iv_f0005 >= 0 else "latest"
_q_f0005 = (
    spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", _sv_f0005)
    .table(sname(F0005))
    .writeStream.foreachBatch(make_f0005_handler(iv_f0005))
    .option("checkpointLocation", CKPT_F0005)
    # .trigger(processingTime=TRIGGER)   # streaming: uncomment to re-enable
    .trigger(availableNow=True)
    .queryName("dim__f0005")
    .start()
)
print("  dim__f0005  startingVersion={}  init_ver={}".format(_sv_f0005, iv_f0005))

# ── Launch F0004 stream ──────────────────────────────────────────────────────
_sv_f0004 = iv_f0004 if iv_f0004 >= 0 else "latest"
_q_f0004 = (
    spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", _sv_f0004)
    .table(sname(F0004))
    .writeStream.foreachBatch(make_f0004_handler())
    .option("checkpointLocation", CKPT_F0004)
    # .trigger(processingTime=TRIGGER)   # streaming: uncomment to re-enable
    .trigger(availableNow=True)
    .queryName("dim__f0004")
    .start()
)
print("  dim__f0004  startingVersion={}  init_ver={}".format(_sv_f0004, iv_f0004))

# ── Await completion ─────────────────────────────────────────────────────────
# Both streams run concurrently; await each in sequence (AvailableNow exits on its own).
# For continuous streaming mode: replace both awaitTermination calls with
# spark.streams.awaitAnyTermination()
_q_f0005.awaitTermination()
_q_f0004.awaitTermination()

print("== batch run complete ==")

