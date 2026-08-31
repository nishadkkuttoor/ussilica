#!/usr/bin/env python
# coding: utf-8

# ## nb_otc_facts_v3
# 
# New notebook

# In[1]:

# Fabric notebook · nb_otc_facts_v3
# ============================================================================
# Module 1 — Order-to-Cash (OTC)  ·  All facts, single always-on SJD notebook
# Gold schema  : lh_jde_gold.rpt
# Silver schema: jde
# ============================================================================
#
# ─── ARCHITECTURE ────────────────────────────────────────────────────────────
# DELETE + APPEND (v3): Each batch collects the scope PKs, DELETEs those rows
# from Gold (ZORDER makes the DELETE targeted — no full table scan), then
# APPENDs the rebuilt rows.  Eliminates the whenNotMatchedBySourceDelete scan
# that caused 600–700 s MERGEs against the 12.5 M-row ESO1 Gold table in v2.
#
# CDF-DIRECT: Silver tables are streamed via readChangeFeed.
#
#   Spine (any event) → spine_unified_merge  (DELETE+APPEND, facts with refs)
#   Spine (any event) → recompute_no_ref     (DELETE+APPEND, facts no refs)
#   Spine group agg   → spine_group_merge    (DELETE+APPEND, ESO4 pattern)
#   Ref   any event   → ref_direct_merge     (targeted MERGE, no full scan)
#
# ─── CHECKPOINT / VERSION LOGIC ──────────────────────────────────────────────
# On every SJD start:
#   Full load  : Gold table absent OR any checkpoint missing → full load,
#                snapshot versions captured, old checkpoints cleared.
#   Resume     : checkpoints present → Spark resumes from its own checkpoint.
# Within-run FAILED retry: monitoring loop restarts the stream; Spark resumes
#   from the checkpoint it built during the current run.
# ============================================================================

import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

try:
    from notebookutils import mssparkutils
except ImportError:
    pass  # already injected inside a Fabric notebook

# ============================================================================
# 1) SHARED CONFIG
# ============================================================================
SILVER_LH = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH = "lh_jde_gold"
GOLD_SCHEMA = "rpt"

# ── Checkpoint root ───────────────────────────────────────────────────────────
# ABSOLUTE, and pinned by GUID to the Gold lakehouse.  A relative "Files/..."
# path resolves against whatever lakehouse is attached as the notebook's/SJD's
# DEFAULT — so if that attachment ever changes, _checkpoints_exist() finds
# nothing, _needs_full_load flips true, and all 17 facts silently full-load with
# no error to explain why.  GUIDs (not names) so a workspace or lakehouse rename
# cannot break it either.
#
# This is the SAME physical folder the relative path already resolved to, so
# every existing checkpoint is found and every stream resumes normally.
# Verify after any change to this constant — must print True:
#     print(_checkpoints_exist(_ckpt_path("fact_extended_sales_order_3", "f4211")))
CKPT_ROOT = (
    "abfss://9ea13355-c802-4ca5-883f-e5dbf8ecc720@onelake.dfs.fabric.microsoft.com/"
    "bed869e4-f15b-4cc1-9368-c7a9b3e08a83/Files/checkpoints/streaming_tables"
)
# ── RUN MODE ──────────────────────────────────────────────────────────────────
#   "available_now" : each stream drains its CDF backlog, then TERMINATES.  The
#                     notebook exits and the cluster releases.  Schedule from a
#                     Pipeline.  Streams drain ONE AT A TIME — with AvailableNow
#                     they would otherwise all start at once, a higher concurrent
#                     memory peak than always-on ever reaches at steady state.
#   "streaming"     : always-on SJD.  Streams run forever on TRIGGER and the
#                     monitoring loop restarts them on failure.
# Full-load logic, generic_recompute and the handlers are IDENTICAL in both modes.
RUN_MODE = "available_now"          # "available_now" | "streaming"

TRIGGER = "120 seconds"             # streaming mode only — ignored by available_now

# ── AvailableNow concurrency ─────────────────────────────────────────────────
# How many FACTS drain at once.  1 = the original serial drain, exactly.
#
# Parallelism is across facts only; the streams WITHIN a fact stay serial, so
# two streams can never write the same Gold table at the same time and the
# per-fact lock is never contended.
#
# ⚠ MEMORY IS THE LIMIT, NOT CPU.  make_per_fact_handler acquires the per-fact
# semaphore BEFORE it caches its sub_batch, so each fact holds at most one
# cached batch — meaning PARALLEL_FACTS directly bounds how many cached batches
# sit in executor memory at once.  Fabric runs Gluten/Velox, whose off-heap
# memory is invisible to the GC and unbudgeted by memoryOverhead, so the
# symptom of overshooting is a bare exit 137 with LOW GC% — no message names
# the cause.  Raise this one step at a time.
#
# _HEAVY_FACTS are capped separately at HEAVY_PARALLEL regardless of
# PARALLEL_FACTS; the light facts fill the remaining worker slots.  TWO distinct
# costs put a fact on this list — do not read it as one thing:
#
#   (a) READ SIDE.  The build_fn issues its OWN spark.read.table() against a
#       large Silver table and bounds it with a join on the batch line keys.
#       The join prunes rows, but the read is planned against the whole table,
#       and generic_recompute caches a narrowed copy of every ref in a
#       PER-THREAD cache — so N facts in flight hold N independent ref sets.
#       That multiplier is what parallelism actually creates.
#
#   (b) WRITE SIDE.  generic_recompute's DELETE rewrites files in proportion to
#       GOLD table size, not batch size — measured 2026-08-16: ESO2 rewrote
#       12.6M rows to delete 2,527.  Cheap builds can still be expensive here.
#
# On a FULL LOAD the distinction disappears: every fact receives the entire
# spine table, so the cap matters most exactly when rebuilding.
PARALLEL_FACTS = 9       # available_now only — set to 1 to revert
HEAVY_PARALLEL = 8         # max heavy facts running concurrently

_HEAVY_FACTS = {
    "fact_sales_order_freight",        
    "fact_sales_commission", 
    "fact_extended_sales_order_5",   
    "dim_invoice_reconciliation",  
    "fact_extended_sales_order_2",   
    "fact_sales_order_detail",      
    "fact_extended_sales_order_7_v2",   
    "fact_sales_order_price_adjustment",
}

if RUN_MODE not in ("available_now", "streaming"):
    raise ValueError(
        "RUN_MODE must be 'available_now' or 'streaming', got {!r}".format(RUN_MODE)
    )

# Delta may compact log files between SJD runs, removing the snapshot entry that
# CDF streaming uses to verify column mapping.  This flag lets the stream resume
# without that snapshot.  Safe here because Bronze→Silver never renames columns.
spark.conf.set(
    "spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled",
    "true",
)

# Default shuffle partitions (200) causes large per-executor partitions during
# the build/join phase for the 12.5 M-row ESO1 fact.  800 keeps each partition
# smaller and reduces peak heap pressure when executor count is 5.
spark.conf.set("spark.sql.shuffle.partitions", "800")


def sname(t):
    return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)


def gname(t):
    return "{}.{}.{}".format(GOLD_LH, GOLD_SCHEMA, t)


# ============================================================================
# 2) SHARED HELPERS
# ============================================================================
def drop_deleted(df):
    if "is_delete" in df.columns:
        return df.filter(F.col("is_delete").isNull() | (F.col("is_delete") != 1))
    return df


def sk(*cols):
    return F.concat_ws(
        "|",
        *[
            F.col(c).cast("string") if isinstance(c, str) else c.cast("string")
            for c in cols
        ],
    )

def _jde_ts(date_col, time_col):
    """Combine a JDE update date + time-of-day into a timestamp.

    JDE packs time as HHMMSS in a numeric field with leading zeros stripped:
    221101 -> 22:11:01, 91101 -> 09:11:01, 0 -> midnight.  Pad to 6 before parsing.
    NULL date -> NULL timestamp; NULL time -> midnight on that date.
    """
    hhmmss = F.lpad(
        F.coalesce(time_col.cast("bigint"), F.lit(0)).cast("string"), 6, "0"
    )
    return F.when(
        date_col.isNotNull(),
        F.to_timestamp(
            F.concat(F.date_format(date_col, "yyyyMMdd"), hhmmss), "yyyyMMddHHmmss"
        ),
    )

# ============================================================================
# 3) THREAD-LOCAL REF STORAGE
# ============================================================================
_thread_refs = threading.local()


def get_ref(tbl):
    batch_refs = getattr(_thread_refs, "cache", None)
    if batch_refs and tbl in batch_refs:
        return batch_refs[tbl]
    return drop_deleted(spark.read.table(sname(tbl)))


# ============================================================================
# 4) STREAM STATE
# ============================================================================
_FACT_LOCKS = {}
_FACT_BATCH_SEMS = {}
_STREAM_FAIL_COUNTS = {}


# ============================================================================
# 5) GENERIC ENGINE
# ============================================================================
def current_version(src):
    return (
        spark.sql("DESCRIBE HISTORY {}".format(sname(src)))
        .select(F.max("version"))
        .first()[0]
    )


def generic_recompute(affected_keys_df, gold_fact, fc, spine_df=None):
    """Generic DELETE+APPEND for any fact grain (1:1 or fan-out).

    affected_keys_df : cached DataFrame[fact_key STRING, *sl_keys]
                       sl_keys = Silver-side LINE_KEY column names.
                       fact_key = MD5(sl_keys values joined by "||").

    spine_df : optional — the caller's already-prepared live spine rows.
               Spine path passes this so we skip the Silver re-read (the
               CDF batch already has the complete updated row).
               Ref path leaves it None — we must re-read Silver to find
               which spine lines the changed ref rows affect.

    Step 1 — DELETE: MERGE Gold ON fact_key with whenMatchedDelete.
    Step 2 — APPEND: write freshly built rows.  Empty when all affected
             lines are deleted — DELETE alone removes them from Gold.
    """
    sl_keys = fc.get("spine_line_keys") or fc["line_keys"]
    line_keys = fc["line_keys"]

    _spine_cols = fc.get("spine_columns")
    if spine_df is not None:
        # Spine path: batch already has the complete updated rows — no Silver
        # re-read needed.  INNER join still applied so that delete-only line
        # keys (present in affected_keys but absent from spine_df because we
        # excluded _change_type='delete') produce no rows → build_fn returns
        # nothing → DELETE removes the Gold row, nothing is appended.
        _raw_spine = spine_df.join(
            affected_keys_df.select(*sl_keys).distinct(), on=sl_keys, how="inner"
        )
    else:
        # Ref path: batch has ref data only — must re-read Silver spine to
        # find which spine lines are affected by the changed ref rows.
        _spine_src = drop_deleted(spark.read.table(sname(fc["spine"])))
        # A COARSE-grain fact whose grain spans MORE THAN ONE spine table has to
        # re-read all of them.  dim_order_number is the case: an order purged out
        # of F4211 lives on in F42119, and reading only F4211 would find nothing
        # for it — so its Gold row would be DELETED instead of rebuilt, exactly
        # when the history table was supposed to keep it.
        # Only fires for rebuild_from_silver facts, so no existing fact changes.
        if fc.get("rebuild_from_silver"):
            for _extra_spine in fc.get("extra_spines") or []:
                _spine_src = _spine_src.unionByName(
                    drop_deleted(spark.read.table(sname(_extra_spine))),
                    allowMissingColumns=True,
                )
        _raw_spine = _spine_src.join(
            affected_keys_df.select(*sl_keys).distinct(), on=sl_keys, how="inner"
        )
    filtered_spine = (
        _raw_spine.select(*_spine_cols) if _spine_cols else _raw_spine
    ).cache()
    filtered_spine.count()
    # spine_df provided by caller (spine path: lazy batch DataFrame — unpersist is no-op;
    # ref path: cached Silver read — release it now that filtered_spine is in memory).
    if spine_df is not None:
        spine_df.unpersist()

    _thread_refs.cache = {}
    try:
        for ref_tbl in fc.get("ref_tables", set()) | fc.get("passive_refs", set()):
            join_def = fc["ref_spine_join_keys"][ref_tbl]
            if join_def and isinstance(join_def[0], tuple):
                spine_join_cols = [p[0] for p in join_def]
                ref_join_cols = [p[1] for p in join_def]
                sk_df = (
                    filtered_spine.select(*spine_join_cols)
                    .distinct()
                    .toDF(*ref_join_cols)
                )
                join_on = ref_join_cols
            else:
                join_on = join_def
                sk_df = filtered_spine.select(*join_on).distinct()
            _thread_refs.cache[ref_tbl] = drop_deleted(
                spark.read.table(sname(ref_tbl))
            ).join(sk_df, on=join_on, how="left_semi")

        # fact_key on Gold is MD5 of line_keys values — same underlying values as
        # sl_keys, so the hash matches the one computed in the handler from sl_keys.
        new_rows = (
            fc["build_fn"](filtered_spine)
            .withColumn(
                "fact_key",
                F.md5(F.concat_ws("||", *[F.col(k).cast("string") for k in line_keys])),
            )
            .withColumn("last_update_date_time_utc", F.current_timestamp())
            .cache()
        )
        new_count = new_rows.count()

        (
            DeltaTable.forName(spark, gname(gold_fact))
            .alias("t")
            .merge(
                affected_keys_df.select("fact_key").distinct().alias("s"),
                "t.fact_key = s.fact_key",
            )
            .whenMatchedDelete()
            .execute()
        )

        if new_count > 0:
            new_rows.write.format("delta").mode("append").saveAsTable(gname(gold_fact))

        new_rows.unpersist()
        return new_count

    finally:
        _thread_refs.cache = None
        filtered_spine.unpersist()


def make_per_fact_handler(fc, silver_tbl, is_ref, init_ver, stream_name):
    """
    foreachBatch handler for one (fact × Silver table) stream.

    Two paths — spine and ref — both converge on generic_recompute.
    No .collect(), no SQL string, no cardinality-violation risk.

    Spine path: LINE_KEYS are already in the CDF batch → extract directly.
    Ref path  : join changed ref keys back to the spine to find affected LINE_KEYS.

    All change types (insert, update_postimage, delete) are included so that
    soft-deletes on any source table propagate correctly to Gold.
    """
    fact_name = fc["fact"]
    is_spine = not is_ref
    sl_keys = fc.get("spine_line_keys") or fc["line_keys"]

    def handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        try:
            sub_batch = batch_df
            if init_ver >= 0:
                sub_batch = sub_batch.filter(F.col("_commit_version") > init_ver)
            if sub_batch.rdd.isEmpty():
                return

            _batch_sem = _FACT_BATCH_SEMS.setdefault(fc["fact"], threading.Semaphore(1))
            _batch_sem.acquire()
            try:
                sub_batch = sub_batch.cache()
                sub_batch.count()

                t0 = time.time()
                ops_count = 0
                _fact_lock = _FACT_LOCKS.setdefault(fc["fact"], threading.Lock())

                if is_spine:
                    # LINE_KEYS are directly in the CDF batch (main spine or extra_spine).
                    changed_keys = (
                        sub_batch.filter(
                            F.col("_change_type").isin(
                                "insert", "update_postimage", "delete"
                            )
                        )
                        .select(*sl_keys)
                        .distinct()
                    )

                    # Live rows to pass directly to build_fn — avoids Silver re-read.
                    # Only postimage rows (delete rows have no Gold output).
                    # Dedup to latest version per line key in case a line was updated
                    # more than once inside the same trigger window.
                    # Soft-deletes (is_delete=1 update_postimage) removed by drop_deleted.
                    _dedup_w = Window.partitionBy(*sl_keys).orderBy(
                        F.col("_commit_version").desc()
                    )
                    live_spine = drop_deleted(
                        sub_batch.filter(
                            F.col("_change_type").isin("insert", "update_postimage")
                        )
                        .withColumn("_rn", F.row_number().over(_dedup_w))
                        .filter(F.col("_rn") == 1)
                        .drop(
                            "_rn",
                            "_change_type",
                            "_commit_version",
                            "_commit_timestamp",
                        )
                    )
                else:
                    # Ref stream: map changed ref keys back to spine LINE_KEYS.
                    join_def = fc["ref_spine_join_keys"][silver_tbl]
                    if join_def and isinstance(join_def[0], tuple):
                        ref_cols = [p[1] for p in join_def]
                        spine_cols = [p[0] for p in join_def]
                    else:
                        ref_cols = spine_cols = join_def

                    changed_ref_keys = (
                        sub_batch.filter(
                            F.col("_change_type").isin(
                                "insert", "update_postimage", "delete"
                            )
                        )
                        .select(*ref_cols)
                        .distinct()
                        .toDF(*spine_cols)
                    )

                    # Cache the Silver spine read here — pass it to generic_recompute
                    # so it reuses this DataFrame instead of re-reading Silver again.
                    live_spine = (
                        drop_deleted(spark.read.table(sname(fc["spine"])))
                        .join(changed_ref_keys, on=spine_cols, how="inner")
                        .cache()
                    )
                    live_spine.count()

                    changed_keys = live_spine.select(*sl_keys).distinct()

                # fact_key = MD5 of sl_keys values — same values as Gold line_keys,
                # so the hash matches what generic_recompute writes onto Gold rows.
                affected_keys = changed_keys.withColumn(
                    "fact_key",
                    F.md5(
                        F.concat_ws("||", *[F.col(k).cast("string") for k in sl_keys])
                    ),
                ).cache()
                affected_count = affected_keys.count()

                # ⚠ COARSE-GRAIN FACTS must NOT be rebuilt from the batch's live rows.
                #
                # live_spine holds insert/update_postimage only, with soft-deletes
                # dropped.  changed_keys (spine path) is built from the UNFILTERED
                # batch, so a deleted line still puts its group key into the DELETE
                # set.  For a line-grain fact that is exactly right: the line is gone,
                # so the Gold row must go.
                #
                # For a fact aggregated to a COARSER grain it silently destroys data.
                # Delete one line of a 50-line consolidated invoice and, unless a
                # surviving line of that same invoice happens to be in the same batch,
                # the invoice is DELETED from Gold and never rebuilt from the other 49.
                #
                # spine_df=None routes it through generic_recompute's Silver re-read,
                # which joins the FULL spine table to affected_keys — so every
                # surviving member of each affected group comes back.  See Rule 4c.
                _spine_arg = live_spine
                if is_spine and fc.get("rebuild_from_silver"):
                    _spine_arg = None

                if affected_count > 0:
                    try:
                        with _fact_lock:
                            ops_count += generic_recompute(
                                affected_keys,
                                fc["fact"],
                                fc,
                                spine_df=_spine_arg,
                            )
                    finally:
                        affected_keys.unpersist()
                else:
                    affected_keys.unpersist()

            finally:
                sub_batch.unpersist()
                _batch_sem.release()

            print(
                "[{}|{}] batch={} ops={:,} {:.1f}s".format(
                    fact_name, silver_tbl[:14], batch_id, ops_count, time.time() - t0
                )
            )
            _STREAM_FAIL_COUNTS[stream_name] = 0

        except Exception as e:
            count = _STREAM_FAIL_COUNTS.get(stream_name, 0) + 1
            _STREAM_FAIL_COUNTS[stream_name] = count
            print(
                "[{}|{}] {} batch={} fail#{}: {}: {}".format(
                    fact_name,
                    silver_tbl[:14],
                    "DEAD" if count >= 2 else "FAILED",
                    batch_id,
                    count,
                    type(e).__name__,
                    e,
                )
            )
            raise

    return handler


# ============================================================================
# 6) SILVER TABLE CONSTANTS
# ============================================================================
F4201 = "f4201_sales_order_header_file"
F4211 = "f4211_sales_order_detail_file"
F4941 = "f4941_shipment_routing_steps"
F5642B01 = "f5642b01_custom_sales_order_entry_screen_header"
F5542035 = "f5542035_order_re_date_audit_history_table"
F4215 = "f4215_shipment_header"
F0101 = "f0101_address_book_master"
F42119 = "f42119_sales_order_history_file"
F41002 = "f41002_item_units_of_measure_conversion_factors"
F4981 = "f4981_freight_audit_history"
F5642B11 = "f5642b11_custom_sales_order_entry_screen_detail"
F5549002 = "f5549002_mxp_bol_interface_detail"
F03012 = "f03012_customer_master_by_line_of_business"
F49211 = "f49211_sales_order_detail_file_tag_file"
F4106 = "f4106_item_base_price_file"
F4104 = "f4104_item_cross_reference_file"
# additional tables used for eso2
F0010 = "f0010_company_constants"
# additional tables used for eso4
F0006 = "f0006_business_unit_master"
F0116 = "f0116_address_by_date"
F03B11 = "f03b11_customer_ledger"
# additional table used for the commission fact
F42005 = "f42005_sales_commission_file"
# additional tables used for eso5
F4311 = "f4311_purchase_order_detail_file"
F554201T = "f554201t_sand_box_sales_order_qc_information"
F0911 = "f0911_account_ledger"
F43121 = "f43121_purchase_order_receiver_file"
F0005 = "f0005_user_defined_code_values"
# additional tables used for the price-adjustment fact
F4074 = "f4074_price_adjustment_ledger_file"
F4101 = "f4101_item_master"
F41003 = "f41003_unit_of_measure_standard_conversion"

# ============================================================================
# 7) FACT AND DIM DEFINITIONS
# ============================================================================

# ════════════════════════════════════════════════════════════════════════════
# [DIM]  dim_order  ·  spine: F4201
# Grain  : one row per (company, order_number, order_type)
# Gold PK: company, order_number, order_type
# ════════════════════════════════════════════════════════════════════════════
DIM_ORDER_GOLD_LINE_KEYS = ["company", "order_number", "order_type"]
DIM_ORDER_SILVER_LINE_KEYS = [
    "company_key_order_no",
    "document_order_invoice_e",
    "order_type",
]


def build_dim_order(f4201_df):
    f4201_df = drop_deleted(f4201_df)
    df = f4201_df.select(
        F.col("company_key_order_no").alias("company"),
        F.col("document_order_invoice_e").alias("order_number"),
        F.col("order_type").alias("order_type"),
        F.col("address_number").alias("address_number"),
        F.col("address_number_ship_to").alias("address_number_ship_to"),
        F.col("mode_of_transport").alias("mode_of_transport"),
        F.col("date_requested_julian").alias("date_requested"),
        F.col("date_transaction_julian").alias("date_transaction"),
        F.col("ordered_by").alias("ordered_by"),
        F.col("order_taken_by").alias("order_taken_by"),
        F.col("date_updated").alias("date_updated"),
        F.col("hold_orders_code").alias("hold_orders_code"),
        F.col("date_original_promisde").alias("date_original_promisde"),
        _jde_ts(F.col("date_updated"), F.col("time_of_day")).alias(
            "jde_updated_ts"
        ),  # SHUPMJ + SHTDAY
    ).distinct()
    df = df.withColumn(
        "order_key", F.concat_ws("|", "order_number", "order_type", "company")
    )
    return df


# ════════════════════════════════════════════════════════════════════════════
# [DIM]  dim_shipment  ·  spine: F4215
# Grain  : one row per shipment_number
# Gold PK: shipment_number
# ════════════════════════════════════════════════════════════════════════════
DIM_SHIPMENT_LINE_KEYS = ["shipment_number"]


def build_dim_shipment(f4215_df):
    f4215_df = drop_deleted(f4215_df)
    return f4215_df.select(
        F.col("shipment_number").alias("shipment_number"),
        F.col("shipment_status").alias("shipment_status"),
        F.col("mode_of_transport").alias("mode_of_transport"),
        F.col("carrier_01").alias("carrier_01"),
        F.col("carrier_02").alias("carrier_02"),
        F.col("carrier_03").alias("carrier_03"),
        F.col("origin_address_number").alias("origin_address_number"),
        F.col("address_number").alias("address_number"),
        F.col("address_number_ship_to").alias("address_number_ship_to"),
        F.col("cost_center").alias("cost_center"),
        F.col("city").alias("city"),
        F.col("state").alias("state"),
        F.col("country").alias("country"),
        F.col("origin_city").alias("origin_city"),
        F.col("origin_state").alias("origin_state"),
        F.col("origin_country").alias("origin_country"),
        F.col("number_of_routing_steps").alias("number_of_routing_steps"),
        F.col("shipment_weight").alias("shipment_weight"),
        F.col("date_requested_julian").alias("date_requested"),
        F.col("date_release_julian").alias("date_release"),
        F.col("date_updated").alias("date_updated"),
        _jde_ts(F.col("date_updated"), F.col("time_of_day")).alias(
            "jde_updated_ts"
        ),  # XHUPMJ + XHTDAY
    ).distinct()


# ════════════════════════════════════════════════════════════════════════════
# [FACT]  fact_shipment_routing  ·  spine: F4941
# Grain  : one row per (shipment_number, routing_step_number)
# Gold PK: shipment_number, routing_step_number
# ════════════════════════════════════════════════════════════════════════════
FACT_SHIPMENT_ROUTING_LINE_KEYS = ["shipment_number", "routing_step_number"]


def build_fact_shipment_routing(f4941_df):
    f4941_df = drop_deleted(f4941_df)
    return f4941_df.select(
        F.col("shipment_number").alias("shipment_number"),
        F.col("routing_step_number").alias("routing_step_number"),
        F.col("mode_of_transport").alias("mode_of_transport"),
        F.col("origin_address_number").alias("load_port"),
        F.col("address_number_deconsolida").alias("address_number_deconsolida"),
        F.col("date_release_julian").alias("date_release_julian"),
        _jde_ts(F.col("date_updated"), F.col("time_of_day")).alias(
            "jde_updated_ts"
        ),  # RSUPMJ + RSTDAY
    ).distinct()


# ════════════════════════════════════════════════════════════════════════════
# [FACT]  fact_sales_order_detail  ·  spine: F4211
# Grain  : one row per (company, order_number, order_type, line_number)
# Gold PK: company, order_number, order_type, line_number
# ════════════════════════════════════════════════════════════════════════════
FACT_SHIPMENT_ORDER_DETAIL_GOLD_LINE_KEYS = [
    "company",
    "order_number",
    "order_type",
    "line_number",
]
FACT_SHIPMENT_ORDER_DETAIL_SILVER_LINE_KEYS = [
    "company_key_order_no",
    "document_order_invoice_e",
    "order_type",
    "line_number",
]


def _uom_cascades():
    # item-specific F41002 (blank cost-center) + standard F41003, fwd + inverse, direct to TN.
    # Ambiguous keys (>1 distinct factor) collapse to NULL so the join can never fan the grain out.
    f2 = get_ref(F41002).filter(F.trim(F.col("cost_center")) == "")
    i_fwd = (f2.filter((F.trim("related_uom") == "TN") & (F.col("conversion_factor") != 0))
               .select(F.col("identifier_short_item").alias("itm"), F.trim("uom").alias("from_uom"),
                       F.col("conversion_factor").cast("double").alias("f")))
    i_rev = (f2.filter((F.trim("uom") == "TN") & (F.col("conversion_factor") != 0))
               .select(F.col("identifier_short_item").alias("itm"), F.trim("related_uom").alias("from_uom"),
                       (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("f")))
    item = (i_fwd.unionByName(i_rev).dropDuplicates(["itm", "from_uom", "f"])
              .groupBy("itm", "from_uom").agg(F.count("f").alias("n"), F.min("f").alias("f"))
              .withColumn("conv_factor", F.when(F.col("n") > 1, F.lit(None).cast("double")).otherwise(F.col("f")))
              .select("itm", "from_uom", "conv_factor"))
    f3 = get_ref(F41003)
    s_fwd = (f3.filter((F.trim("related_uom") == "TN") & (F.col("conversion_factor") != 0))
               .select(F.trim("uom").alias("from_uom"), F.col("conversion_factor").cast("double").alias("f")))
    s_rev = (f3.filter((F.trim("uom") == "TN") & (F.col("conversion_factor") != 0))
               .select(F.trim("related_uom").alias("from_uom"), (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("f")))
    std = (s_fwd.unionByName(s_rev).dropDuplicates(["from_uom", "f"])
             .groupBy("from_uom").agg(F.count("f").alias("n"), F.min("f").alias("f"))
             .withColumn("conv_std", F.when(F.col("n") > 1, F.lit(None).cast("double")).otherwise(F.col("f")))
             .select("from_uom", "conv_std"))
    return item, std


def build_fact_shipment_order_detail(f4211_df):
    df = drop_deleted(f4211_df)

    # Group by all original columns
    group_cols = [
        F.col("company_key_order_no").alias("company"),
        F.col("document_order_invoice_e").alias("order_number"),
        F.col("order_type").alias("order_type"),
        F.col("line_number").alias("line_number"),
        F.col("line_type").alias("line_type"),
        F.col("transaction_originator").alias("order_originator"),
        F.col("shipment_number").alias("shipment_number"),
        F.col("cost_center").alias("cost_center"),
        F.col("address_number").alias("address_number"),
        F.col("address_number_ship_to").alias("address_number_ship_to"),
        F.col("status_code_last").alias("status_code_last"),
        F.col("status_code_next").alias("status_code_next"),
        F.col("date_promised_ship_julian").alias("date_promised_ship_julian"),
        F.col("date_requested_julian").alias("date_requested_julian"),
        F.col("scheduled_pick_date").alias("scheduled_pick_date"),
        F.col("date_release_julian").alias("date_release_julian"),
        F.col("identifier_second_item").alias("identifier_second_item"),
        F.col("identifier_short_item").alias("identifier_short_item"),
        F.col("uom_as_input").alias("uom_as_input"),
        F.col("freight_handling_code").alias("freight_handling_code"),
        F.col("carrier").alias("carrier"),
        F.col("mode_of_transport").alias("mode_of_transport"),
        F.col("container_id").alias("container_id"),
        F.col("reference_01").alias("reference_01"),
        F.col("units_transaction_qty").alias("units_transaction_qty"),
        F.col("uom_pricing").alias("uom_pricing"),                 # SDUOM4 — Source UoM (Price)
        F.col("amt_price_per_unit_02").alias("unit_price"),        # SDUPRC — Source Price
    ]

    df = df.withColumn(
        "jde_updated_ts", _jde_ts(F.col("date_updated"), F.col("time_of_day"))    # SDUPMJ + SDTDAY
    )

    df = df.groupBy(group_cols).agg(
        F.first("address_number_parent", ignorenulls=True).alias("address_number_parent"),
        F.max("jde_updated_ts").alias("jde_updated_ts"),
    )

    # df = df.withColumn("company_num", F.col("company").cast("decimal(10,5)"))

    df = df.withColumn(
        "order_key", F.concat_ws("|", "order_number", "order_type", "company")
    )
    df = df.withColumn(
        "line_shipment_key",
        F.concat_ws(
            "|",
            "company",
            "order_number",
            "order_type",
            "line_number",
            "shipment_number",
        ),
    )
    df = df.withColumn(
        "shipment_order_key",
        F.concat_ws("|", "shipment_number", "order_number", "order_type", "company"),
    )
    # For Extended sales order 7 tables
    df = df.withColumn(
        "uom_key",
        F.concat_ws("|", "cost_center", "identifier_short_item", "uom_as_input"),
    )
    # For creating relationship with dim_order_activity table
    df = df.withColumn(
        "order_activity_key",
        F.concat_ws("|", "order_type", "line_type", "status_code_last"),
    )
    # 2-column surrogate key for F41002
    df = df.withColumn(
        "item_uom_key", F.concat_ws("|", "identifier_short_item", "uom_as_input")
    )

    # ── standard UoM conversion (baked): SDUOM->TN volume factor/tons/flag + SDUOM4->TN price fields ──
    _keep = df.columns
    _vi, _vs = _uom_cascades()
    df = (df
        .join(_vi.alias("civ"), (F.col("civ.itm") == F.col("identifier_short_item"))
                                & (F.col("civ.from_uom") == F.trim(F.col("uom_as_input"))), "left")
        .join(_vs.alias("csv"), (F.col("csv.from_uom") == F.trim(F.col("uom_as_input"))), "left")
        .join(_vi.alias("cip"), (F.col("cip.itm") == F.col("identifier_short_item"))
                                & (F.col("cip.from_uom") == F.trim(F.col("uom_pricing"))), "left")
        .join(_vs.alias("csp"), (F.col("csp.from_uom") == F.trim(F.col("uom_pricing"))), "left"))
    _vol_raw = F.coalesce(F.when(F.trim(F.col("uom_as_input")) == "TN", F.lit(1.0)),
                          F.col("civ.conv_factor"), F.col("csv.conv_std"))
    _prc_raw = F.coalesce(F.when(F.trim(F.col("uom_pricing")) == "TN", F.lit(1.0)),
                          F.col("cip.conv_factor"), F.col("csp.conv_std"))
    _prc = F.coalesce(_prc_raw, F.lit(1.0))
    df = (df
        .withColumn("conversion_to_tons_rate", F.coalesce(_vol_raw, F.lit(1.0)))
        .withColumn("quantity_shipped_tons", F.col("units_transaction_qty").cast("double") * F.coalesce(_vol_raw, F.lit(1.0)))
        .withColumn("missing_conversion_flag", F.when(_vol_raw.isNull(), F.lit("Y")).otherwise(F.lit("N")))
        .withColumn("price_conversion_factor", _prc)
        .withColumn("converted_price_per_ton",
            F.when((F.col("status_code_last").cast("int") != F.lit(980))
                   & (F.col("status_code_next").cast("int") == F.lit(999)),
                   F.col("unit_price").cast("double") / F.when(_prc != 0, _prc)))
        .withColumn("price_missing_conversion_flag", F.when(_prc_raw.isNull(), F.lit("Y")).otherwise(F.lit("N")))
        .select(*_keep, "conversion_to_tons_rate", "quantity_shipped_tons", "missing_conversion_flag",
                "price_conversion_factor", "converted_price_per_ton", "price_missing_conversion_flag"))
    return df


# ════════════════════════════════════════════════════════════════════════════
# [FACT]  fact_extended_sales_order_8  ·  spine: F5542035
#         refs : F4201 (customer_number, ship_to_number, customer_po)
#                F5642B01 (export_booking_number, export_load_date)
# Grain  : one row per (order_number, run_date) — MAX(math_numeric_01) dedup
# Gold PK: order_number, run_date
# ════════════════════════════════════════════════════════════════════════════
FACT_ESO8_GOLD_LINE_KEYS = ["order_number", "run_date"]
FACT_ESO8_SILVER_LINE_KEYS = ["document_order_invoice_e", "date_updated"]


def build_fact_eso8(f5542035_df):
    window_max_asmath01 = Window.partitionBy(
        "document_order_invoice_e",
        "date_updated",
    )

    f5542035_df = drop_deleted(f5542035_df)

    df_f5542035_filtered = (
        f5542035_df.withColumn(
            "max_asmath01",
            F.max("math_numeric_01").over(window_max_asmath01),
        )
        .filter(F.col("math_numeric_01") == F.col("max_asmath01"))
        .drop("max_asmath01")
    )

    df_f4201_slim = get_ref(F4201).select(
        F.col("document_order_invoice_e").alias("hdr_doco"),
        F.col("order_type").alias("hdr_dcto"),
        F.col("company_key_order_no").alias("hdr_kcoo"),
        F.col("address_number").alias("an8"),
        F.col("address_number_ship_to").alias("shan"),
        F.col("reference_01").alias("vr01"),
    )

    df_f5642b01_slim = get_ref(F5642B01).select(
        F.col("document_order_invoice_e").alias("bk_doco"),
        F.col("shipment_number").alias("bk_shpn"),
        F.col("order_type").alias("bk_dcto"),
        F.col("company_key_order_no").alias("bk_kcoo"),
        F.col("date_latest_pickup").alias("load_date"),
        F.col("booking_no").alias("booking_no"),
    )

    df_joined = (
        df_f5542035_filtered.alias("m")
        .join(
            df_f4201_slim.alias("hdr"),
            (F.col("m.document_order_invoice_e") == F.col("hdr.hdr_doco"))
            & (F.col("m.order_type") == F.col("hdr.hdr_dcto"))
            & (F.col("m.company_key_order_no") == F.col("hdr.hdr_kcoo")),
            "left",
        )
        .join(
            df_f5642b01_slim.alias("bk"),
            (F.col("m.document_order_invoice_e") == F.col("bk.bk_doco"))
            & (F.col("m.shipment_number") == F.col("bk.bk_shpn"))
            & (F.col("m.order_type") == F.col("bk.bk_dcto"))
            & (F.col("m.company_key_order_no") == F.col("bk.bk_kcoo")),
            "left",
        )
    )

    df_fact = df_joined.select(
        F.col("m.description_001").alias("process"),
        F.col("m.cost_center").alias("plant"),
        F.col("m.document_order_invoice_e").alias("order_number"),
        F.col("m.order_type").alias("order_type"),
        F.col("m.company_key_order_no").alias("company"),
        F.col("m.everest_event_point_02").alias("hook_order"),
        F.col("hdr.an8").alias("customer_number"),
        F.col("hdr.shan").alias("ship_to_number"),
        F.col("hdr.vr01").alias("customer_po"),
        F.col("m.date_release_julian").alias("old_sch_pick_date"),
        F.col("m.scheduled_pick_date").alias("new_sch_pick_date"),
        F.col("m.edi_successfully_process").alias("status_flag"),
        F.col("m.message_text").alias("error_message"),
        F.col("bk.booking_no").alias("export_booking_number"),
        F.col("bk.load_date").alias("export_load_date"),
        F.col("m.program_id").alias("program_id"),
        F.col("m.user_id").alias("user"),
        F.col("m.date_updated").alias("run_date"),
        F.col("m.time_last_updated").alias("run_time"),
        F.col("m.work_station_id").alias("work_station"),
        F.col("m.shipment_number").alias("shipment_number"),
    ).distinct()

    
    df_fact = df_fact.withColumn(
        "jde_updated_ts", _jde_ts(F.col("run_date"), F.col("run_time"))  # ASUPMJ + ASTDAY
    )

    grain_cols = [
        "order_number",
        "order_type",
        "company",
        "shipment_number",
        "run_date",
    ]
    df_fact = df_fact.withColumn("shipment_key", F.concat_ws("|", *grain_cols))
    df_fact = df_fact.withColumn(
        "order_key", F.concat_ws("|", "order_number", "order_type", "company")
    )
    return df_fact


# ════════════════════════════════════════════════════════════════════════════
# [FACT]  fact_sales_order_freight  ·  spine: F4211  extra spine: F42119
# Grain  : one row per (company_key_order_no, order_type, order_number, line_number)
# Gold PK: company_key_order_no, order_type, order_number, line_number
# Notes  : F0101 columns (ABAC05/14, ABURAT, ABAN83) moved to semantic model
#          via dim_address_ship_to / dim_address_sold_to relationships.
#          F42119 (Sales Order History) streamed as a second spine — same
#          line-key schema as F4211; build_fn unions both filtered to batch keys.
# ════════════════════════════════════════════════════════════════════════════
FACT_ESO1_GOLD_LINE_KEYS = [
    "company_key_order_no",
    "order_type",
    "order_number",
    "line_number",
]
FACT_ESO1_SILVER_LINE_KEYS = [
    "company_key_order_no",
    "order_type",
    "document_order_invoice_e",
    "line_number",
]

_ESO1_LINE_COLS = [
    "company_key_order_no",
    "order_type",
    "document_order_invoice_e",
    "line_number",
]
_ESO1_DATE_LO = "2000-01-01"
_ESO1_YEARS_AHEAD = 25
SHIFT_FACTOR = 1.0  # neutral default; change here if business rules evolve
_ESO1_RAW_DATES = [
    "order_date",
    "requested_date",
    "scheduled_pick_date",
    "promised_ship_date",
    "actual_ship_date",
    "gl_date",
    "invoice_date",
    "cancel_date",
    "line_price_effective_date",
    "header_price_effective_date",
    "date_earliest_pickup",
    "date_earliest_delivery",
    "date_latest_delivery",
    "release_date",
    "date_requested_ship",
]


def _eso1_clean_date(col):
    hi = F.make_date(
        F.year(F.current_date()) + F.lit(_ESO1_YEARS_AHEAD), F.lit(12), F.lit(31)
    )
    return F.when(col.between(F.lit(_ESO1_DATE_LO).cast("date"), hi), col)


def _eso1_date_key(col):
    return F.when(col.isNotNull(), F.date_format(col, "yyyyMMdd").cast("int"))


def build_fact_eso1(spine_df):
    # ── 1. line keys from whatever spine triggered (F4211 or F42119) ─────────
    line_keys_df = spine_df.select(*_ESO1_LINE_COLS).distinct()

    # ── 2. filtered reads of both sources — inner join on line_keys_df bounds
    #      each read to only the rows matching this batch. No full table scan.
    #      When F4211 fires: f4211_rows = changed lines, f42119_rows ≈ empty.
    #      When F42119 fires: f4211_rows ≈ empty (purged), f42119_rows = changed history. ──
    f4211_rows = drop_deleted(spark.read.table(sname(F4211))).join(
        line_keys_df, on=_ESO1_LINE_COLS, how="inner"
    )

    f42119_rows = drop_deleted(spark.read.table(sname(F42119))).join(
        line_keys_df, on=_ESO1_LINE_COLS, how="inner"
    )
    if (
        "identifier_2nd_item" in f42119_rows.columns
        and "identifier_second_item" not in f42119_rows.columns
    ):
        f42119_rows = f42119_rows.withColumnRenamed(
            "identifier_2nd_item", "identifier_second_item"
        )
    if (
        "identifier_3rd_item" in f42119_rows.columns
        and "identifier_third_item" not in f42119_rows.columns
    ):
        f42119_rows = f42119_rows.withColumnRenamed(
            "identifier_3rd_item", "identifier_third_item"
        )

    sd = f4211_rows.unionByName(f42119_rows, allowMissingColumns=True).dropDuplicates(
        _ESO1_LINE_COLS
    )

    # ── 3. ref tables (pre-filtered by engine cache) ─────────────────────────
    f4201 = get_ref(F4201)
    f41002 = get_ref(F41002)
    f4981 = get_ref(F4981)
    f4941 = get_ref(F4941)
    f5642b01 = get_ref(F5642B01)
    f5642b11 = get_ref(F5642B11)
    f5549002 = get_ref(F5549002)
    f03012 = get_ref(F03012)
    f49211 = get_ref(F49211)
    f4106 = get_ref(F4106)

    # ── 4. pre-aggregations ───────────────────────────────────────────────────
    # UoM → TN item-specific conversion (fwd + reciprocal)
    item_fwd = f41002.filter(
        (F.trim("related_uom") == "TN") & (F.col("conversion_factor") != 0)
    ).select(
        F.col("identifier_short_item").alias("itm"),
        F.trim("uom").alias("from_uom"),
        F.col("conversion_factor").cast("double").alias("conv_factor"),
    )
    item_rev = f41002.filter(
        (F.trim("uom") == "TN") & (F.col("conversion_factor") != 0)
    ).select(
        F.col("identifier_short_item").alias("itm"),
        F.trim("related_uom").alias("from_uom"),
        (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_factor"),
    )
    conv_item = item_fwd.unionByName(item_rev)

    # UoM structure — one row per (item, input-uom) from the TN-gate row
    uom_str = (
        f41002.filter(F.trim(F.col("related_uom")) == "TN")
        .select(
            F.col("identifier_short_item").alias("us_itm"),
            F.trim(F.col("uom")).alias("us_uom"),
            F.col("uom_structure").alias("uom_structure"),
        )
        .dropDuplicates(["us_itm", "us_uom"])
    )

    # F4981 freight buckets → shipment grain
    _bp = F.trim("billable_payable")
    _cgc = F.trim("charge_code_01")
    _amt = F.col("net_amount")
    _inv = F.trim(F.col("vendor_invoice_number")) != "NULL"
    freight = (
        f4981.groupBy("shipment_number")
        .agg(
           F.round(F.sum(F.when((_bp == "B") & (_cgc == "BFR")            & _inv,  _amt).otherwise(0.0)), 2).alias("billable_freight_actual"),
                F.round(F.sum(F.when((_bp == "B") & (_cgc.isin("FSC", "FSB")) & _inv,  _amt).otherwise(0.0)), 2).alias("billable_fuel"),
                F.round(F.sum(F.when((_bp == "P") & (_cgc == "PFR")            & _inv,  _amt).otherwise(0.0)), 2).alias("payable_freight"),
                F.round(F.sum(F.when((_bp == "P") & (_cgc == "FSC")            & _inv,  _amt).otherwise(0.0)), 2).alias("payable_fuel"),
            F.round(F.sum(_amt), 2).alias("total_freight"),
            F.first(F.trim("city"), ignorenulls=True).alias("freight_city"),
            F.first(F.trim("state"), ignorenulls=True).alias("freight_state"),
            F.first(F.trim("zip_code_postal"), ignorenulls=True).alias("freight_zip"),
            F.first(F.trim("freight_handling_code"), ignorenulls=True).alias(
                "freight_audit_handling_code"
            ),
        )
        .withColumn(
            "total_payable",
            F.round(F.col("payable_freight") + F.col("payable_fuel"), 2),
        )
        .withColumn("shift_factor_applied", F.lit(SHIFT_FACTOR).cast("double"))
    )

    # F4941 routing → shipment grain
    route = f4941.groupBy("shipment_number").agg(
        F.first("route_number", ignorenulls=True).alias("route_number"),
        F.coalesce(
            F.max(F.when(F.trim(F.col("mode_of_transport")) == "OCE", F.lit("Y"))),
            F.lit("N"),
        ).alias("is_ocean_route"),
        F.round(F.sum(F.when(F.trim(F.col("mode_of_transport")) == "OCE",
                F.col("number_of_containers").cast("double")).otherwise(0.0)), 0).alias("route_container_count")
    )

    # F5549002 BOL weigh-ticket weights → one row per line
    wt = f5549002.groupBy(
        "company_key_order_no", "document_order_invoice_e", "order_type", "line_number"
    ).agg(
        F.first("gross_weight", ignorenulls=True).alias("gross_weight"),
        F.first("catch_weight", ignorenulls=True).alias("catch_weight"),
        F.first("maximum_weight", ignorenulls=True).alias("max_weight"),
    )

    # F03012 sold-to LOB → one row per address
    lob = f03012.groupBy("address_number").agg(
        F.first("report_code_add_book_005", ignorenulls=True).alias(
            "sold_to_lob_category_05"
        )
    )

    # F49211 SO-line tag → one row per line
    tag = f49211.groupBy(
        "company_key_order_no", "document_order_invoice_e", "order_type", "line_number"
    ).agg(
        F.first("deferred_entries_flag", ignorenulls=True).alias(
            "deferred_entries_flag"
        )
    )

    # F5642B11 booking detail → 5-key grain
    b11d = f5642b11.groupBy(
        "company_key_order_no",
        "document_order_invoice_e",
        "order_type",
        "line_number",
        "shipment_number",
    ).agg(
        F.first("seal_no", ignorenulls=True).alias("seal_no"),
        F.first("production_code", ignorenulls=True).alias("production_code"),
        F.first("production_ship_notes", ignorenulls=True).alias(
            "production_ship_notes"
        ),
    )

    # F5642B01 booking header → 4-key grain
    # destination_port validated against F0101 address book (filtered read via dest_ports)
    dest_ports = f5642b01.select(
        F.col("destination_port").alias("address_number")
    ).dropDuplicates()
    dest_ab = (
        drop_deleted(spark.read.table(sname(F0101)))
        .join(dest_ports, on="address_number", how="left_semi")
        .select(F.col("address_number").alias("_dest_an8"))
        .dropDuplicates()
    )
    b01_valid = f5642b01.join(
        dest_ab, F.col("destination_port") == F.col("_dest_an8"), "left_semi"
    )
    b01d = b01_valid.groupBy(
        "company_key_order_no",
        "document_order_invoice_e",
        "order_type",
        "shipment_number",
    ).agg(
        F.first("booking_no", ignorenulls=True).alias("booking_no"),
        F.first("bookingstatus", ignorenulls=True).alias("booking_status"), 
        F.first("destination_port", ignorenulls=True).alias("destination_port"),
        F.first("no_of_container", ignorenulls=True).alias("no_of_container"),
        F.first("ocean_del_terms", ignorenulls=True).alias("ocean_del_terms"),
        F.first("vessel_name", ignorenulls=True).alias("vessel_name"),
        F.first("date_earliest_pickup", ignorenulls=True).alias("date_earliest_pickup"),
        F.first("date_earliest_delivery", ignorenulls=True).alias(
            "date_earliest_delivery"
        ),
        F.first("date_latest_delivery", ignorenulls=True).alias("date_latest_delivery"),
        F.first("voyage_no", ignorenulls=True).alias("voyage_number"),
        F.first("loading_port", ignorenulls=True).alias("loading_port"),
        F.first("ocean_carrier", ignorenulls=True).alias("ocean_carrier"),
        F.first("reference_01", ignorenulls=True).alias("booking_reference_1"),
        F.first("reference_02", ignorenulls=True).alias("booking_reference_2"),
        F.first("reference_03", ignorenulls=True).alias("booking_reference_3"),
        F.first("date_latest_pickup", ignorenulls=True).alias("date_latest_pickup"),
        F.first("order_reference", ignorenulls=True).alias("order_reference"),
        F.first("routing_notes", ignorenulls=True).alias("routing_notes"),
        F.first("equipment_type", ignorenulls=True).alias("equipment_type"),
        F.first("inland_delterms", ignorenulls=True).alias("inland_delterms"),
        F.first("incoterms", ignorenulls=True).alias("incoterms"),
        F.first("date_requested_ship", ignorenulls=True).alias("date_requested_ship"),
    )

    # ── 5. GL date column — pick whichever Silver name is present ────────────
    _gl_candidates = [
        "dt_for_gl_and_vouch_01",
        "date_for_g_l_julian",
        "date_g_l_julian",
        "date_general_ledger_julian",
        "general_ledger_date",
        "date_for_g_land_voucher_julian",
        "gl_date",
    ]
    _gl_name = next((c for c in _gl_candidates if c in sd.columns), None)
    gl_expr = F.col("sd.{}".format(_gl_name)) if _gl_name else F.lit(None).cast("date")

    # ── 6. main join ──────────────────────────────────────────────────────────
    conv_rate = F.coalesce(
        F.when(F.trim(F.col("sd.uom_as_input")) == "TN", F.lit(1.0)),
        F.col("ci.conv_factor"),
    )

    # ── price-side UoM conversion, added 2026-08-24 (D:\ussilica\Module 1 developer
    # nbs\ESO1\nb_silver_to_gold_eso1_fact_sales_order_freight.py) ──────────────────
    # Reuses _price_adj_conv_lookups (:4082, same helper fact_sales_order_price_adjustment
    # already calls) rather than reimplementing it — the dev's own comment says this is
    # "the same construction". Named conv_item_full/conv_std here so conv_item_full
    # doesn't collide with THIS function's own `conv_item` (:984, the single-leg
    # cascade already used for "ci"/conversion_to_tons_rate) — the dev notebook can use
    # one name for both because it only builds one of them per file; this one builds both.
    conv_item_full, conv_std = _price_adj_conv_lookups()

    # item master IMUOM1 — one row per item, the pivot both fuller-cascade legs resolve
    # against (fallback target-UoM when neither F41002 nor F41003 has a direct row).
    item_im = (
        get_ref(F4101)
        .select(
            F.col("identifier_short_item").alias("im_itm"),
            F.trim(F.col("uom_primary")).alias("im_uom_primary"),
        )
        .dropDuplicates(["im_itm"])
    )

    # existence-only cascade (drives the CORRECTED missing_conversion_flag below) — item
    # leg (cif/cit) then standard leg (csf/cst), both pivoted through im_uom_primary.
    # Same shape as fact_sales_order_price_adjustment's from_factor/to_factor (:4199).
    from_factor = F.coalesce(
        F.col("cif.conv_item"),
        F.col("csf.conv_std"),
        F.when(F.trim(F.col("sd.uom_as_input")) == F.col("im.im_uom_primary"), F.lit(1.0)),
    )
    to_factor = F.coalesce(
        F.col("cit.conv_item"),
        F.col("cst.conv_std"),
        F.when(F.lit("TN") == F.col("im.im_uom_primary"), F.lit(1.0)),
    )
    conv_factor = F.when(
        from_factor.isNull() | to_factor.isNull() | (to_factor == 0), F.lit(0.0)
    ).otherwise(from_factor / to_factor)

    # price factor on the PRICING UoM (SDUOM4, not SDUOM): TN->1.0, else item F41002 (cip,
    # reusing THIS function's own single-leg conv_item — same table as "ci", different join
    # predicate), else standard F41003 (csp), else 1.0. price_factor_raw (pre-1.0-default)
    # drives price_missing_conversion_flag; price_factor (post-default) is what's stored.
    price_factor_raw = F.coalesce(
        F.when(F.trim(F.col("sd.uom_pricing")) == "TN", F.lit(1.0)),
        F.col("cip.conv_factor"),
        F.col("csp.conv_std"),
    )
    price_factor = F.coalesce(price_factor_raw, F.lit(1.0))
    _pf_nz = F.when(price_factor != 0, price_factor)  # NULL only when factor is 0 — guards the divide
    # delivered lines only: Last Status (SDLTTR) <> 980 AND Next Status (SDNXTR) = 999
    _delivered = (F.trim(F.col("sd.status_code_last")).cast("int") != F.lit(980)) & (
        F.trim(F.col("sd.status_code_next")).cast("int") == F.lit(999)
    )
    # price per ton = (SDUPRC, de-scaled in Silver) / price factor; NULL on non-delivered lines
    converted_price_per_ton = F.when(
        _delivered, F.col("sd.amt_price_per_unit_02") / _pf_nz
    )

    j = (
        sd.alias("sd")
        .join(
            f4201.alias("sh"),
            (F.col("sd.company_key_order_no") == F.col("sh.company_key_order_no"))
            & (
                F.col("sd.document_order_invoice_e")
                == F.col("sh.document_order_invoice_e")
            )
            & (F.col("sd.order_type") == F.col("sh.order_type")),
            "inner",
        )
        .join(
            b11d.alias("b11"),
            (F.col("sd.company_key_order_no") == F.col("b11.company_key_order_no"))
            & (
                F.col("sd.document_order_invoice_e")
                == F.col("b11.document_order_invoice_e")
            )
            & (F.col("sd.order_type") == F.col("b11.order_type"))
            & (F.col("sd.line_number") == F.col("b11.line_number"))
            & (F.col("sd.shipment_number") == F.col("b11.shipment_number")),
            "left",
        )
        .join(
            b01d.alias("b01"),
            (F.col("sd.company_key_order_no") == F.col("b01.company_key_order_no"))
            & (
                F.col("sd.document_order_invoice_e")
                == F.col("b01.document_order_invoice_e")
            )
            & (F.col("sd.order_type") == F.col("b01.order_type"))
            & (F.col("sd.shipment_number") == F.col("b01.shipment_number")),
            "left",
        )
        .join(
            uom_str.alias("us"),
            (F.col("us.us_itm") == F.col("sd.identifier_short_item"))
            & (F.col("us.us_uom") == F.trim(F.col("sd.uom_as_input"))),
            "left",
        )
        .join(
            conv_item.alias("ci"),
            (F.col("ci.itm") == F.col("sd.identifier_short_item"))
            & (F.col("ci.from_uom") == F.trim(F.col("sd.uom_as_input"))),
            "left",
        )
        # ── added 2026-08-24, price conversion (see the block above the main join) ──
        .join(
            item_im.alias("im"),
            F.col("im.im_itm") == F.col("sd.identifier_short_item"),
            "left",
        )
        .join(
            conv_item_full.alias("cif"),
            (F.col("cif.itm") == F.col("sd.identifier_short_item"))
            & (F.col("cif.from_uom") == F.trim(F.col("sd.uom_as_input"))),
            "left",
        )
        .join(
            conv_item_full.alias("cit"),
            (F.col("cit.itm") == F.col("sd.identifier_short_item"))
            & (F.col("cit.from_uom") == F.lit("TN")),
            "left",
        )
        .join(
            conv_std.alias("csf"),
            (F.col("csf.from_uom") == F.trim(F.col("sd.uom_as_input")))
            & (F.col("csf.to_uom") == F.col("im.im_uom_primary")),
            "left",
        )
        .join(
            conv_std.alias("cst"),
            (F.col("cst.from_uom") == F.lit("TN"))
            & (F.col("cst.to_uom") == F.col("im.im_uom_primary")),
            "left",
        )
        .join(
            conv_item.alias("cip"),
            (F.col("cip.itm") == F.col("sd.identifier_short_item"))
            & (F.col("cip.from_uom") == F.trim(F.col("sd.uom_pricing"))),
            "left",
        )
        .join(
            conv_std.alias("csp"),
            (F.col("csp.from_uom") == F.trim(F.col("sd.uom_pricing")))
            & (F.col("csp.to_uom") == F.lit("TN")),
            "left",
        )
        .join(
            route.alias("rt"),
            F.col("sd.shipment_number") == F.col("rt.shipment_number"),
            "left",
        )
        .join(
            freight.alias("fr"),
            F.col("sd.shipment_number") == F.col("fr.shipment_number"),
            "left",
        )
        .join(
            wt.alias("wt"),
            (F.col("wt.company_key_order_no") == F.col("sd.company_key_order_no"))
            & (
                F.col("wt.document_order_invoice_e")
                == F.col("sd.document_order_invoice_e")
            )
            & (F.col("wt.order_type") == F.col("sd.order_type"))
            & (F.col("wt.line_number") == F.col("sd.line_number")),
            "left",
        )
        .join(
            lob.alias("lob"),
            F.col("lob.address_number") == F.col("sd.address_number"),
            "left",
        )
        .join(
            tag.alias("tag"),
            (F.col("tag.company_key_order_no") == F.col("sd.company_key_order_no"))
            & (
                F.col("tag.document_order_invoice_e")
                == F.col("sd.document_order_invoice_e")
            )
            & (F.col("tag.order_type") == F.col("sd.order_type"))
            & (F.col("tag.line_number") == F.col("sd.line_number")),
            "left",
        )
    )

    # ── 7. select ─────────────────────────────────────────────────────────────
    sel = j.select(
        F.col("sd.company_key_order_no").alias("company_key_order_no"),
        F.col("sd.order_type").alias("order_type"),
        F.col("sd.document_type").alias("document_type"),
        F.col("sd.document_order_invoice_e").alias("order_number"),
        F.col("sd.line_number").alias("line_number"),
        F.col("sd.company").alias("company"),
        F.col("sd.shipment_number").alias("shipment_number"),
        F.col("sd.user_reserved_number").alias("bol_number"),
        F.col("sd.doc_voucher_invoice_e").alias("invoice_number"),
        F.col("sd.original_document_type").alias("original_document_type"),
        F.col("sd.original_po_so_number").alias("original_po_so_number"),
        F.col("sd.original_document_no").alias("original_document_no"),
        F.col("sd.reference_01").alias("reference_01"),
        F.col("sd.user_reserved_reference").alias("user_reserved_reference"),
        F.col("sh.hold_orders_code").alias("hold_orders_code"),
        F.col("sd.status_code_last").alias("status_code_last"),
        F.col("sd.status_code_next").alias("status_code_next"),
        F.trim(F.col("sd.status_code_next")).cast("int").alias("next_status_num"),
        F.trim(F.col("sd.status_code_last")).cast("int").alias("last_status_num"),
        F.col("sd.freight_handling_code").alias("freight_handling_code"),
        F.trim(F.col("sd.mode_of_transport")).alias("mode_of_transport"),
        F.col("rt.route_number").alias("route_number"),
        F.col("sd.container_id").alias("container_id"),
        F.col("sd.transaction_originator").alias("transaction_originator"),
        F.col("sh.delivery_instruct_line_01").alias("delivery_instruct_line_01"),
        F.col("sh.delivery_instruct_line_02").alias("delivery_instruct_line_02"),
        F.col("sd.gl_class").alias("gl_class"),
        F.col("sd.sales_reporting_code_01").alias("sales_reporting_code_01"),
        F.col("sd.sales_reporting_code_03").alias("sales_reporting_code_03"),
        F.col("sd.address_number_ship_to").alias("ship_to"),
        F.col("sd.address_number").alias("bill_to"),
        F.col("sd.carrier").alias("carrier_number"),
        F.col("sd.address_number_parent").alias("address_number_parent"),
        F.col("sd.identifier_short_item").alias("item_number_short"),
        F.trim(F.col("sd.cost_center")).alias("branch_plant"),
        F.col("sd.date_transaction_julian").alias("order_date"),
        F.col("sd.date_requested_julian").alias("requested_date"),
        F.col("sd.scheduled_pick_date").alias("scheduled_pick_date"),
        F.col("sd.date_promised_ship_julian").alias("promised_ship_date"),
        F.col("sd.date_release_julian").alias("release_date"),
        F.col("sd.actual_ship_date").alias("actual_ship_date"),
        gl_expr.alias("gl_date"),
        F.col("sd.date_invoice_julian").alias("invoice_date"),
        F.col("sd.cancel_date").alias("cancel_date"),
        F.col("sd.date_price_effective_date").alias("line_price_effective_date"),
        F.col("sh.date_price_effective_date").alias("header_price_effective_date"),
        F.col("b01.date_earliest_pickup").alias("date_earliest_pickup"),
        F.col("b01.date_earliest_delivery").alias("date_earliest_delivery"),
        F.col("b01.date_latest_delivery").alias("date_latest_delivery"),
        F.col("sd.identifier_second_item").alias("second_item_number"),
        F.col("sd.identifier_third_item").alias("third_item_number"),
        F.col("sd.line_type").alias("line_type"),
        F.col("sd.uom_as_input").alias("uom"),
        F.col("sd.uom_primary").alias("uom_primary"),
        F.col("sd.uom_pricing").alias("uom_pricing"),
        conv_rate.alias("conversion_to_tons_rate"),
        # CHANGED 2026-08-24 — was `conv_rate.isNull()` (the single-leg item-only
        # cascade). The dev's updated notebook checks the fuller item+standard
        # cascade instead (conv_factor == 0, from_factor/to_factor above), which
        # also resolves through the item's own primary UoM as a last resort.
        # OUTPUT-CHANGING on an EXISTING column — flagged to Riju, not silent.
        F.when(conv_factor == 0, F.lit("Y"))
        .otherwise(F.lit("N"))
        .alias("missing_conversion_flag"),
        # ── added 2026-08-24 — price-side conversion (see block above the main join) ──
        price_factor.alias("price_conversion_factor"),
        converted_price_per_ton.alias("converted_price_per_ton"),
        F.when(price_factor_raw.isNull(), F.lit("Y"))
        .otherwise(F.lit("N"))
        .alias("price_missing_conversion_flag"),
        F.concat_ws("|", F.col("sd.identifier_short_item"), F.trim(F.col("sd.uom_as_input"))).alias("item_uom_key"),
        F.col("us.uom_structure").alias("uom_structure"),
        F.col("sd.payment_terms_code_01").alias("payment_terms"),
        F.col("sd.units_quantity_shipped").alias("quantity_shipped"),
        F.col("sd.units_primary_qty_order").alias("primary_quantity_ordered"),
        F.col("sd.units_transaction_qty").alias("transaction_quantity"),
        F.col("sd.amt_price_per_unit_02").alias("price_per_unit"),
        F.col("sd.uom_ent_up").alias("unit_price_primary"),
        F.col("sd.sales_reporting_code_02").alias("major_prod_code"),
        F.col("sd.sales_reporting_code_04").alias("minor_prod_code"),
        F.col("b11.seal_no").alias("seal_no"),
        F.col("b11.production_code").alias("production_code"),
        F.col("b11.production_ship_notes").alias("production_ship_notes"),
        F.col("b01.booking_no").alias("booking_no"),
        F.col("b01.booking_status").alias("booking_status"),
        F.col("b01.destination_port").alias("destination_port"),
        F.col("b01.no_of_container").alias("no_of_container"),
        F.col("b01.ocean_del_terms").alias("ocean_del_terms"),
        F.col("b01.vessel_name").alias("vessel_name"),
        F.col("fr.freight_city").alias("freight_city"),
        F.col("fr.freight_state").alias("freight_state"),
        F.col("fr.freight_zip").alias("freight_zip"),
        F.col("fr.freight_audit_handling_code").alias("freight_audit_handling_code"),  # F4981 FHFRTH
        F.coalesce(F.col("fr.shift_factor_applied"), F.lit(SHIFT_FACTOR)).alias(
            "shift_factor_applied"
        ),
        F.coalesce(F.col("fr.billable_freight_actual"), F.lit(0.0)).alias("billable_freight_actual"),
        F.coalesce(F.col("fr.billable_fuel"), F.lit(0.0)).alias("billable_fuel"),
        F.coalesce(F.col("fr.payable_freight"), F.lit(0.0)).alias("payable_freight"),
        F.coalesce(F.col("fr.payable_fuel"), F.lit(0.0)).alias("payable_fuel"),
        F.coalesce(F.col("fr.total_payable"), F.lit(0.0)).alias("total_payable"),
        F.coalesce(F.col("fr.total_freight"), F.lit(0.0)).alias("total_freight"),
        F.col("sd.amount_extended_price").alias("extended_price"),
        F.col("sd.amount_extended_cost").alias("extended_cost"),
        F.col("sd.currency_code_base").alias("currency_code"),
        F.col("sd.units_quan_backor_held").alias("backorder_qty"),
        F.col("sd.units_quantity_canceled").alias("cancelled_qty"),
        F.col("sd.quantity_shipped_to_date").alias("qty_to_date"),
        F.col("sd.units_open_quantity").alias("open_qty"),
        F.col("sd.description_line_01").alias("line_description_1"),
        F.col("sd.description_line_02").alias("line_description_2"),
        F.col("sd.date_updated").alias("date_updated"),
        F.col("sd.zone_number").alias("zone_number"),
        F.col("sd.hold_orders_code").alias("line_hold"),
        F.col("rt.is_ocean_route").alias("is_ocean_route"),
        F.col("rt.route_container_count").alias("route_container_count"),
        F.col("wt.gross_weight").alias("gross_weight"),
        F.col("wt.catch_weight").alias("catch_weight"),
        F.col("wt.max_weight").alias("max_weight"),
        F.col("sd.pull_signal").alias("pull_signal"),
        F.col("sd.reference_02_vendor").alias("reference_02"),
        F.col("sd.reference_ucis_no").alias("reference_03"),
        F.col("sd.primary_last_vendor_no").alias("vendor_number"),
        F.col("sd.price_adjustment_schedule_n").alias("price_adjustment_schedule"),
        F.col("sd.user_reserved_code").alias("user_reserved_code"),
        F.col("sd.user_reserved_number").alias("user_reserved_number"),  # SDURAB — same source as bol_number, exposed under its own name
        F.col("sd.price_override_code").alias("price_override_code"),
        F.col("sd.user_id").alias("user_id"),
        F.col("sd.lot").alias("lot_number"),
        F.col("sd.serial_number_lot").alias("serial_number"),
        F.col("sd.location").alias("location"),
        F.col("sd.sales_reporting_code_05").alias("sales_reporting_code_05"),
        F.col("lob.sold_to_lob_category_05").alias("sold_to_lob_category_05"),
        F.col("tag.deferred_entries_flag").alias("deferred_entries_flag"),
        F.col("sd.related_po_so_number").alias("related_po_so_number"),
        F.col("sd.time_of_day").alias("time_of_day"),
        F.col("sh.date_original_promisde").alias("original_promised_date"),
        F.col("b01.voyage_number").alias("voyage_number"),
        F.col("b01.loading_port").alias("loading_port"),
        F.col("b01.ocean_carrier").alias("ocean_carrier"),
        F.col("b01.booking_reference_1").alias("booking_reference_1"),
        F.col("b01.booking_reference_2").alias("booking_reference_2"),
        F.col("b01.booking_reference_3").alias("booking_reference_3"),
        F.col("b01.date_latest_pickup").alias("date_latest_pickup"),
        F.col("b01.order_reference").alias("order_reference"),
        F.col("b01.routing_notes").alias("routing_notes"),
        F.col("b01.equipment_type").alias("equipment_type"),
        F.col("b01.inland_delterms").alias("inland_delterms"),
        F.col("b01.incoterms").alias("incoterms"),
        F.col("b01.date_requested_ship").alias("date_requested_ship"),
        F.col("sh.address_number").alias("header_sold_to"),
        F.col("sh.date_transaction_julian").alias("header_order_date"),
        F.col("sh.carrier").alias("header_carrier_number"),
        F.col("sh.payment_terms_code_01").alias("header_payment_terms"),
        F.col("sh.address_number_parent").alias("header_parent"),
    ).distinct()

    # ── 8. clean sentinel dates, derive buckets + SKs ────────────────────────
    df = sel
    for _dc in _ESO1_RAW_DATES:
        if _dc in df.columns:
            df = df.withColumn(_dc, _eso1_clean_date(F.col(_dc)))

    df = df.withColumn(
        "jde_updated_ts", _jde_ts(F.col("date_updated"), F.col("time_of_day"))  # SDUPMJ + SDTDAY
    )

    _wk = F.weekofyear(F.col("actual_ship_date"))
    _wyr = F.year(F.col("actual_ship_date"))
    _wmth = F.month(F.col("actual_ship_date"))
    _isoyr = (
        F.when((_wmth == 1) & (_wk > 50), _wyr - 1)
        .when((_wmth == 12) & (_wk == 1), _wyr + 1)
        .otherwise(_wyr)
    )

    df = (
        df.withColumn(
            "quantity_shipped_tons",
            F.col("quantity_shipped") * F.col("conversion_to_tons_rate"),
        )
        .withColumn(
            "price_quantity_shipped",
            F.col("price_per_unit") * F.col("quantity_shipped"),
        )
        .withColumn(
            "ship_year_week",
            F.when(
                F.col("actual_ship_date").isNotNull(),
                F.concat(_isoyr, F.lit("-W"), F.lpad(_wk.cast("string"), 2, "0")),
            ),
        )
        .withColumn("order_date_key", _eso1_date_key(F.col("order_date")))
        .withColumn("requested_date_key", _eso1_date_key(F.col("requested_date")))
        .withColumn(
            "scheduled_pick_date_key", _eso1_date_key(F.col("scheduled_pick_date"))
        )
        .withColumn(
            "promised_ship_date_key", _eso1_date_key(F.col("promised_ship_date"))
        )
        .withColumn("ship_date_key", _eso1_date_key(F.col("actual_ship_date")))
        .withColumn("gl_date_key", _eso1_date_key(F.col("gl_date")))
        .withColumn("invoice_date_key", _eso1_date_key(F.col("invoice_date")))
        .withColumn("cancel_date_key", _eso1_date_key(F.col("cancel_date")))
        .withColumn(
            "line_price_effective_date_key",
            _eso1_date_key(F.col("line_price_effective_date")),
        )
        .withColumn(
            "header_price_effective_date_key",
            _eso1_date_key(F.col("header_price_effective_date")),
        )
        .withColumn(
            "earliest_pickup_date_key", _eso1_date_key(F.col("date_earliest_pickup"))
        )
        .withColumn(
            "latest_delivery_date_key", _eso1_date_key(F.col("date_latest_delivery"))
        )
    )

    # is_primary_shipment_line: first line per shipment ordered by (order_number, line_number)
    _w_shp = Window.partitionBy("shipment_number").orderBy(
        "order_number", "line_number"
    )
    df = (
        df.withColumn("_rn", F.row_number().over(_w_shp))
        .withColumn(
            "is_primary_shipment_line",
            F.when(
                (F.col("shipment_number").isNotNull()) & (F.col("_rn") == 1), F.lit("Y")
            ).otherwise(F.lit("N")),
        )
        .drop("_rn")
    )

    # surrogate key (stored for PBI relationships; MERGE key is the 4 natural columns above)
    df = df.withColumn(
        "sales_order_line_key",
        sk("company_key_order_no", "order_type", "order_number", "line_number"),
    )
    df = df.withColumn(
        "order_scope_key", sk("company_key_order_no", "order_type", "order_number")
    )

    df = df.dropDuplicates(["sales_order_line_key"])

    # ── 9. has_effective_price (F4106 date-range left_semi) ──────────────────
    f4106_bp = f4106.filter(F.col("amt_price_per_unit_02") != 0).select(
        F.trim(F.col("identifier_2nd_item")).alias("bp_item2"),
        F.trim(F.col("cost_center")).alias("bp_plant"),
        F.col("address_number").alias("bp_an8"),
        F.col("date_effective_julian_01").alias("bp_eff"),
        F.col("date_expired_julian_01").alias("bp_exp"),
    )
    matched = (
        df.select(
            "sales_order_line_key",
            "second_item_number",
            "branch_plant",
            "ship_to",
            "actual_ship_date",
        )
        .join(
            f4106_bp,
            (F.trim(F.col("second_item_number")) == F.col("bp_item2"))
            & (F.col("branch_plant") == F.col("bp_plant"))
            & (F.col("ship_to") == F.col("bp_an8"))
            & (F.col("bp_eff") <= F.col("actual_ship_date"))
            & (F.col("bp_exp") >= F.col("actual_ship_date")),
            "left_semi",
        )
        .select("sales_order_line_key")
        .distinct()
        .withColumn("has_effective_price", F.lit("Y"))
    )
    df = df.join(matched, "sales_order_line_key", "left").withColumn(
        "has_effective_price", F.coalesce(F.col("has_effective_price"), F.lit("N"))
    )

    df = df.withColumn(
        "pricing_issue_remark",
        F.when(
            (F.col("price_per_unit") == 0) & (F.col("next_status_num") < 620),
            F.lit("Unit Price Zero"),
        )
        .when(
            (F.col("price_per_unit") != 0)
            & F.col("actual_ship_date").isNotNull()
            & (F.col("has_effective_price") == "N"),
            F.lit("No effective price"),
        )
        .otherwise(F.lit(None).cast("string")),
    )

    return df


# ════════════════════════════════════════════════════════════════════════════
# [FACT]  fact_custom_sales_order_2  ·  spine: F4211  ref: F4104
# Grain  : one row per (company_key_order_no, line_number)
# Report : Sales Orders where the Item has NO cross-reference in F4104
# Logic  : F4211 LEFT JOIN F4104 (type='C', description<>' '), KEEP NULL side
# PBI filters (NOT applied here): order_type IN ('SE','SZ','S1'),
#   line_type='S', last_status<>980, next_status<>999
# ════════════════════════════════════════════════════════════════════════════
FACT_CSO2_LINE_KEYS = [
    "company_key_order_no",
    "order_number",
    "order_type",
    "line_number",
]
FACT_CSO2_SILVER_LINE_KEYS = [
    "company_key_order_no",
    "document_order_invoice_e",
    "order_type",
    "line_number",
]


def build_fact_cso2(spine_df):
    df_f4211 = drop_deleted(spine_df)

    df_f4211_prep = df_f4211.select(
        F.col("company_key_order_no"),
        F.col("line_number"),
        F.col("document_order_invoice_e"),
        F.col("order_type"),
        F.col("cost_center"),
        F.col("status_code_next"),
        F.col("status_code_last"),
        F.col("doc_voucher_invoice_e"),
        F.col("identifier_second_item"),
        F.col("address_number"),
        F.col("address_number_ship_to"),
        F.col("line_type"),
        F.col("date_updated"),  # SDUPMJ 
        F.col("time_of_day"),  # SDTDAY 
    ).distinct()

    df_f4104_slim = (
        get_ref(F4104)
        .filter(F.trim(F.col("type_cross_refer_type_c")) == "C")
        .filter(F.col("description_line_01").isNotNull())
        .filter(F.col("description_line_01") != " ")
        .select(F.col("identifier_2nd_item"))
        .distinct()
    )

    df_joined = df_f4211_prep.alias("sd").join(
        df_f4104_slim.alias("xr"),
        F.col("sd.identifier_second_item") == F.col("xr.identifier_2nd_item"),
        "left",
    )

    df_fact = (
        df_joined.filter(F.col("xr.identifier_2nd_item").isNull())
        .select(
            F.col("sd.company_key_order_no"),
            F.col("sd.line_number"),
            F.col("sd.document_order_invoice_e").alias("order_number"),
            F.col("sd.order_type").alias("order_type"),
            F.col("sd.address_number").alias("customer"),
            F.col("sd.doc_voucher_invoice_e").alias("invoice_number"),
            F.col("sd.identifier_second_item").alias("second_item_number"),
            F.col("sd.status_code_last").alias("last_status_code"),
            F.col("sd.status_code_next").alias("next_status_code"),
            F.col("sd.cost_center").alias("plant"),
            F.col("sd.address_number_ship_to").alias("ship_to"),
            F.col("sd.line_type").alias("line_type"),
            _jde_ts(F.col("sd.date_updated"), F.col("sd.time_of_day")).alias(
                "jde_updated_ts"
            ),  # SDUPMJ + SDTDAY
        )
        # GROUP BY (not DISTINCT) — same collapse as before over the same columns,
        # with jde_updated_ts aggregated instead of grouped.  Grouping on it would
        # split a Gold row into one row per update stamp.
        .groupBy(
            "company_key_order_no",
            "line_number",
            "order_number",
            "order_type",
            "customer",
            "invoice_number",
            "second_item_number",
            "last_status_code",
            "next_status_code",
            "plant",
            "ship_to",
            "line_type",
        )
        .agg(F.max("jde_updated_ts").alias("jde_updated_ts"))
    )

    return df_fact


# ════════════════════════════════════════════════════════════════════════════
# [FACT]  fact_extended_sales_order_6  ·  spine: F4211  ref: F4941 , F5642B01
#
# Builds Gold-shaped rows from an F4211 spine DataFrame:
# * F4211 spine (only the universal soft-delete filter applied)
# * INNER JOIN F4941 on `shipment_number`
# * LEFT JOIN F5642B01 on the 4-part key (shipment + company + order_type +
#   document_order_invoice_e)
# * Column projection with type casts (see Step 5 comments)
# * SELECT DISTINCT to collapse exact-duplicate rows
# * `add_fact_key()` prepends the MD5 surrogate as the first column
#
# **Grain**: one row per (F4211 line × F4941 routing step). A multi-leg
# shipment produces multiple fact rows for the same line — one per leg. The
# `fact_key` surrogate hashes the 4-part line natural key only, so multi-leg
# rows for one line share the same `fact_key`.

FACT_ESO6_LINE_KEYS = [
    "company_key_order_no",
    "document_order_invoice_e",
    "order_type",
    "line_number",
]


def build_fact_extended_sales_order_6(spine_df):
    df_f4211 = drop_deleted(spine_df)

    df_f4941 = get_ref(F4941)
    df_f5642b01 = get_ref(F5642B01)

    j = (
        df_f4211.alias("f")
        .join(
            df_f4941.alias("r"),
            F.col("f.shipment_number") == F.col("r.shipment_number"),
            "inner",
        )
        .join(
            df_f5642b01.alias("s"),
            (F.col("f.shipment_number") == F.col("s.shipment_number"))
            & (F.col("f.company_key_order_no") == F.col("s.company_key_order_no"))
            & (F.col("f.order_type") == F.col("s.order_type"))
            & (
                F.col("f.document_order_invoice_e")
                == F.col("s.document_order_invoice_e")
            ),
            "left",
        )
    )

    proj = j.select(
        # Natural key (4-part composite) — retained for business queries + audit.
        # fact_key surrogate is added by add_fact_key() below as the single-column PK.
        F.col("f.company_key_order_no"),  # STRING — leading zeros preserved
        F.col(
            "f.document_order_invoice_e"
        ),  # JDE SDDOCO — order number, always integer
        F.col("f.order_type"),  # STRING — FK to dim_order_type.order_type_code
        F.col(
            "f.line_number"
        ),  # JDE SDLNID — line number, can be fractional (e.g. 1.5) but bounded
        F.col("f.shipment_number"),  # JDE SDSHPN — shipment number, always integer
        # F4941 attributes (routing-step level — multiple rows possible per line)
        F.col("r.mode_of_transport"),  # STRING — FK to dim_mode_of_transport.mot_code
        F.col("r.carrier")
        .cast("bigint")
        .alias("carrier"),  # JDE RSCARS — carrier number, always integer
        F.col("r.route_number")
        .cast("bigint")
        .alias("route_number"),  # JDE RSRTN — route number, always integer
        F.col("r.number_of_containers")
        .cast("int")
        .alias("number_of_containers"),  # JDE RSNCTR — container count, small integer
        # F5642B01 attributes (header-level; nullable via LEFT JOIN)
        F.col(
            "s.booking_no"
        ),  # STRING — may be NULL or blank when no booking header exists
        F.col("s.vessel_name"),  # STRING
        F.col("s.reference_01"),  # STRING
        # F4211 attributes retained on the fact for PBI slicers / audit
        F.col("f.line_type"),  # STRING
        F.col("f.status_code_next"),  # STRING — FK to dim_status.status_code (active)
        F.col("f.status_code_last"),  # STRING — FK to dim_status.status_code (inactive)
        F.col("f.cost_center"),  # STRING
        # Measure(s) from spine — cast to a display-friendly decimal.
        # JDE SDUORG is stored as decimal(38,18) which renders as noisy trailing
        # zeros in PBI ("924.000000000000000000"). decimal(18,4) preserves
        # enough precision for any real JDE quantity while displaying cleanly.
        F.col("f.units_transaction_qty")
        .cast("decimal(18,4)")
        .alias("units_transaction_qty"),
        # Audit — SDUPMJ + SDTDAY
        _jde_ts(F.col("f.date_updated"), F.col("f.time_of_day")).alias("jde_updated_ts"),
    ).groupBy(
        # GROUP BY over exactly the columns the previous DISTINCT covered, so the
        # row count is unchanged.  jde_updated_ts is aggregated instead — grouping
        # on it would split a (line × routing step) row per update stamp.
        "company_key_order_no",
        "document_order_invoice_e",
        "order_type",
        "line_number",
        "shipment_number",
        "mode_of_transport",
        "carrier",
        "route_number",
        "number_of_containers",
        "booking_no",
        "vessel_name",
        "reference_01",
        "line_type",
        "status_code_next",
        "status_code_last",
        "cost_center",
        "units_transaction_qty",
    ).agg(
        F.max("jde_updated_ts").alias("jde_updated_ts")
    )

    df_fact = proj.withColumn(
        "order_key",
        F.concat_ws(
            "|", "document_order_invoice_e", "order_type", "company_key_order_no"
        ),
    )

    return df_fact


# ════════════════════════════════════════════════════════════════════════════
# [FACT]  fact_extended_sales_order_2  ·  spine: F4211  ref: F4201,F0010,F0101,F5549002,F41002
#

FACT_ESO2_GOLD_LINE_KEYS = [
    "key_company_order",
    "order_number",
    "order_type",
    "line_number",
]
FACT_ESO2_SILVER_LINE_KEYS = [
    "company_key_order_no",
    "document_order_invoice_e",
    "order_type",
    "line_number",
]


def build_fact_extended_sales_order_2(spine_df):
    df_f4211 = drop_deleted(spine_df)

    df_f4201 = get_ref(F4201)
    df_f0010 = get_ref(F0010)
    df_f0101 = get_ref(F0101)
    df_f5549002 = get_ref(F5549002)
    df_f41002 = get_ref(F41002)
    df_f41003 = get_ref(F41003)  # standard UoM cascade fallback for price

    # Prep: F0101 DQ gate (INNER join — ABAT1 filtered subquery)
    #
    # Hubble exact logic:
    #   INNER JOIN (
    #       SELECT ABALPH, ABAN8 FROM F0101
    #       WHERE  ABAT1 BETWEEN 'A  ' AND 'P  '
    #           OR ABAT1 BETWEEN 'R  ' AND 'ZZZ'
    #   ) F0101 ON F4211.SDSHAN = F0101.ABAN8
    #
    # NOTE: ship_to_name (ABALPH) & standard_industry_code ABSIC is NOT stored on the fact table.
    #       It will be resolved in Power BI via:
    #           fact[address_number_ship_to] → dim_address_book[address_number] → dim[name_alpha]/dim[standard_industry_code]
    #       The DQ gate (ABAT1 filter) is still applied here to ensure only
    #       valid address book records drive the INNER JOIN row filter.
    #
    # Silver column mapping:
    #   ABAN8  → address_number    (join key)
    #   ABAT1  → address_type_01   (DQ gate filter)
    #   ABSIC  → standard_industry_code (SIC Code — Page Filter column on fact)
    # NOTE: Creating this to avoid ambiguity in column names while joining two tables as snake case of two columns of different tables can be same.
    # ─────────────────────────────────────────────────────────────────────────────
    df_f0101_dq = df_f0101.filter(
        (F.trim(F.col("address_type_01")).between("A", "P"))
        | (F.trim(F.col("address_type_01")).between("R", "ZZZ"))
    ).select(
        F.col("address_number").alias("dq_aban8"),  # ABAN8  — join key
    )

    # F4201 (Sales Order Header — LEFT join)
    #
    # Hubble:
    #   LEFT JOIN F4201 ON SDKCOO=SHKCOO AND SDDOCO=SHDOCO AND SDDCTO=SHDCTO
    #
    # Silver column mapping:
    #   SHKCOO → company_key_order_no     (join key 1)
    #   SHDOCO → document_order_invoice_e (join key 2)
    #   SHDCTO → order_type               (join key 3)
    #   SHDEL1 → delivery_instruct_line_01 (Delivery Instructions — Visual col)
    # NOTE: Creating this to avoid ambiguity in column names while joining two tables as snake case of two columns of different tables can be same.
    # ─────────────────────────────────────────────────────────────────────────────
    df_f4201_slim = df_f4201.select(
        F.col("company_key_order_no").alias("hdr_kcoo"),  # SHKCOO
        F.col("document_order_invoice_e").alias("hdr_doco"),  # SHDOCO
        F.col("order_type").alias("hdr_dcto"),  # SHDCTO
        F.col("delivery_instruct_line_01").alias("delivery_instructions"),  # SHDEL1
    )

    # Prep: F0010 (Company Constants — INNER join)
    #
    # Hubble: INNER JOIN F0010 ON F4211.SDKCOO = F0010.CCCO
    #
    # Silver column mapping:
    #   CCCO   → company                              (join key + Page Filter)
    #   CCPNC  → period_number_current                (MTD month integer 1-12)
    #   CCARFJ → date_ar_fiscal_year_begins_julian    (DateType — MTD year anchor)
    # NOTE: Creating this to avoid ambiguity in column names while joining two tables as snake case of two columns of different tables can be same.
    # ─────────────────────────────────────────────────────────────────────────────
    df_f0010_slim = df_f0010.select(
        F.col("company").alias("ccco"),  # CCCO
        F.col("period_number_current").alias("ccpnc"),  # CCPNC
        F.col("date_ar_fiscal_year_begins_julian").alias("ccarfj"),  # CCARFJ
    )

    # ─────────────────────────────────────────────────────────────────────────────
    #  Prep: F5549002 (Scale Ticket — LEFT join)
    #
    # Hubble:
    #   LEFT JOIN F5549002 ON SDKCOO=MIKCOO AND SDDOCO=MIDOCO
    #                      AND SDDCTO=MIDCTO AND SDLNID=MILNID
    #
    # Silver column mapping:
    #   MIKCOO → company_key_order_no      (join key 1)
    #   MIDOCO → document_order_invoice_e  (join key 2)
    #   MIDCTO → order_type                (join key 3)
    #   MILNID → line_number               (join key 4 — silver already /1000)
    #   MIGRWT → gross_weight              (silver already /10000)
    #   MICTWT → catch_weight              (silver already /10000)
    #   MIMXWT → maximum_weight            (silver already /100)
    # NOTE: Creating this to avoid ambiguity in column names while joining two tables as snake case of two columns of different tables can be same.
    # ─────────────────────────────────────────────────────────────────────────────
    df_scale = df_f5549002.select(
        F.col("company_key_order_no").alias("scale_company_key_order_no"),
        F.col("document_order_invoice_e").alias("scale_order_number"),
        F.col("order_type").alias("scale_order_type"),
        F.col("line_number").alias("scale_line_number"),
        F.col("gross_weight").alias("scale_gross_weight"),
        F.col("catch_weight").alias("scale_tare_weight"),
        F.col("maximum_weight").alias("scale_net_weight"),
    )

    # ─────────────────────────────────────────────────────────────────────────────
    # CELL 8 — Prep: F41002 UoM → TN conversion
    #
    # Forward : UMRUM = 'TN' → factor converts from_uom → TN directly
    # Reverse : UMUM  = 'TN' → factor is stored inverted; flip it
    # Result  : quantity_loaded = units_transaction_qty * conv_factor
    # ─────────────────────────────────────────────────────────────────────────────

    # Forward: source UoM → TN (UMRUM = 'TN')
    df_conv_fwd = df_f41002.filter(
        (F.trim(F.col("related_uom")) == "TN") & (F.col("conversion_factor") != 0)
    ).select(
        F.col("identifier_short_item").alias("conv_itm"),
        F.trim(F.col("uom")).alias("conv_from_uom"),
        F.col("conversion_factor").cast("double").alias("conv_factor"),
    )

    # Reverse: TN stored as source (UMUM = 'TN') → invert factor
    df_conv_rev = df_f41002.filter(
        (F.trim(F.col("uom")) == "TN") & (F.col("conversion_factor") != 0)
    ).select(
        F.col("identifier_short_item").alias("conv_itm"),
        F.trim(F.col("related_uom")).alias("conv_from_uom"),
        (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_factor"),
    )

    # Union forward + reverse, deduplicate
    df_conv = df_conv_fwd.unionByName(df_conv_rev).dropDuplicates(
        ["conv_itm", "conv_from_uom"]
    )

    # F41003 standard UoM -> TN cascade (fwd related_uom='TN', reverse uom='TN' inverted), keyed by from_uom.
    # Standard-tier fallback for the price cascade (SDUOM4 -> TN).
    df_conv_std = (
        df_f41003
        .filter((F.trim(F.col("related_uom")) == "TN") & (F.col("conversion_factor") != 0))
        .select(F.trim(F.col("uom")).alias("std_from_uom"),
                F.col("conversion_factor").cast("double").alias("std_factor"))
        .unionByName(
            df_f41003
            .filter((F.trim(F.col("uom")) == "TN") & (F.col("conversion_factor") != 0))
            .select(F.trim(F.col("related_uom")).alias("std_from_uom"),
                    (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("std_factor")))
        .dropDuplicates(["std_from_uom"])
    )

    # ─────────────────────────────────────────────────────────────────────────────
    # CELL 9 — Joins (exact MySQL/Hubble join order and types)
    #
    # JOIN 1: F0010    INNER  ON SDKCOO = CCCO
    # JOIN 2: F0101    INNER  ON SDSHAN = ABAN8  (ABAT1 DQ gate applied in prep)
    # JOIN 3: F5549002 LEFT   ON 4-key join
    # JOIN 4: F4201    LEFT   ON SDKCOO=SHKCOO AND SDDOCO=SHDOCO AND SDDCTO=SHDCTO
    # JOIN 5: F41002   LEFT   ON SDITM=UMITM AND SDUOM=UMUM
    #
    # NOTE: ship_to_name (ABALPH) is resolved via dim in Power BI — NOT stored
    #       on the fact. The F0101 join here is purely for the ABAT1 DQ gate
    # ─────────────────────────────────────────────────────────────────────────────
    df_joined = (
        df_f4211.alias("sd")
        # ── JOIN 1: F0010 — INNER ─────────────────────────────────────────────────
        .join(
            df_f0010_slim.alias("co"),
            F.col("sd.company_key_order_no") == F.col("co.ccco"),
            "inner",
        )
        # ── JOIN 2: F0101 — INNER (ABAT1 DQ gate) ─────────────────────
        .join(
            df_f0101_dq.alias("ab"),
            F.col("sd.address_number_ship_to") == F.col("ab.dq_aban8"),
            "inner",
        )
        # ── JOIN 3: F5549002 — LEFT ────────────────────────
        .join(
            df_scale.alias("scale"),
            (
                F.col("sd.company_key_order_no")
                == F.col("scale.scale_company_key_order_no")
            )
            & (
                F.col("sd.document_order_invoice_e")
                == F.col("scale.scale_order_number")
            )
            & (F.col("sd.order_type") == F.col("scale.scale_order_type"))
            & (F.col("sd.line_number") == F.col("scale.scale_line_number")),
            "left",
        )
        # ── JOIN 4: F4201 — LEFT ──────────────────────────
        .join(
            df_f4201_slim.alias("hdr"),
            (F.col("sd.company_key_order_no") == F.col("hdr.hdr_kcoo"))
            & (F.col("sd.document_order_invoice_e") == F.col("hdr.hdr_doco"))
            & (F.col("sd.order_type") == F.col("hdr.hdr_dcto")),
            "left",
        )
        # ── JOIN 5: F41002 — LEFT (UoM → TN conversion) ───────────────────────────
        .join(
            df_conv.alias("ci"),
            (F.col("sd.identifier_short_item") == F.col("ci.conv_itm"))
            & (F.trim(F.col("sd.uom_as_input")) == F.col("ci.conv_from_uom")),
            "left",
        )
        # ── JOIN 6/7: price cascade on the pricing UoM SDUOM4 — item F41002 then standard F41003 ──
        .join(
            df_conv.alias("cip"),
            (F.col("sd.identifier_short_item") == F.col("cip.conv_itm"))
            & (F.trim(F.col("sd.uom_pricing")) == F.col("cip.conv_from_uom")),
            "left",
        )
        .join(
            df_conv_std.alias("csp"),
            (F.trim(F.col("sd.uom_pricing")) == F.col("csp.std_from_uom")),
            "left",
        )
    )

    # ─────────────────────────────────────────────────────────────────────────────
    # CELL 11 — Derived columns
    #
    # quantity_loaded (TN):
    #   quantity_loaded = units_transaction_qty (SDUORG) * conv_factor_to_tn
    #
    #   COALESCE priority:
    #     1. If SDUOM already 'TN' → conv_factor = 1.0
    #     2. F41002 has a factor   → use it
    #     3. No factor found       → store raw (factor = 1.0)
    # ─────────────────────────────────────────────────────────────────────────────
    df_derived = (
        df_joined
        # Resolve UoM → TN conversion factor
        .withColumn(
            "conv_factor_to_tn",
            F.coalesce(
                F.when(F.trim(F.col("sd.uom_as_input")) == "TN", F.lit(1.0)),
                F.col("ci.conv_factor"),
                F.lit(1.0),
            ),
        )
        # Quantity Loaded in TN
        .withColumn(
            "quantity_loaded",
            F.round(
                F.col("sd.units_transaction_qty").cast("double")
                * F.col("conv_factor_to_tn"),
                2
            )
        )
        # ── standard UoM conversion columns ──
        # volume: persist the resolved SDUOM->TN factor; flag rows that fell to the 1.0 default
        .withColumn("conversion_to_tons_rate", F.col("conv_factor_to_tn"))
        .withColumn("missing_conversion_flag",
            F.when((F.trim(F.col("sd.uom_as_input")) != "TN") & F.col("ci.conv_factor").isNull(),
                   F.lit("Y")).otherwise(F.lit("N")))
        # price on SDUOM4: TN->1.0, else item F41002, else standard F41003; _raw (no 1.0 default) drives the flag
        # NOTE: materialise uom_pricing/unit_price first, then reference the BARE columns downstream — a
        # withColumn whose name equals the source short-name shadows the sd-qualified column, so a later
        # F.col("sd.uom_pricing") would fail to resolve.
        .withColumn("uom_pricing", F.col("sd.uom_pricing"))
        .withColumn("unit_price", F.col("sd.amt_price_per_unit_02"))
        .withColumn("_price_factor_raw",
            F.coalesce(F.when(F.trim(F.col("uom_pricing")) == "TN", F.lit(1.0)),
                       F.col("cip.conv_factor"), F.col("csp.std_factor")))
        .withColumn("price_conversion_factor", F.coalesce(F.col("_price_factor_raw"), F.lit(1.0)))
        .withColumn("converted_price_per_ton",
            F.when((F.col("sd.status_code_last").cast("int") != F.lit(980))
                   & (F.col("sd.status_code_next").cast("int") == F.lit(999)),
                   F.col("unit_price").cast("double")
                   / F.when(F.col("price_conversion_factor") != 0, F.col("price_conversion_factor"))))
        .withColumn("price_missing_conversion_flag",
            F.when(F.col("_price_factor_raw").isNull(), F.lit("Y")).otherwise(F.lit("N")))
    )
    df_inner = (
        df_derived.select(
            # ── Grain keys ────────────────────────────────────────────────────────
            F.col("sd.company_key_order_no").alias("key_company_order"),  # SDKCOO
            F.col("sd.document_order_invoice_e").alias("order_number"),  # SDDOCO
            F.col("sd.order_type").alias("order_type"),  # SDDCTO
            F.col("sd.line_number").alias("line_number"),  # SDLNID
            # ── Visual columns ────────────────────────────────────────────────────
            F.col("sd.actual_ship_date").alias(
                "actual_ship_date"
            ),  # SDADDJ — Ship Date
            # ship_to_name (ABALPH) → resolved via dim_address_book in Power BI
            F.col("sd.address_number_ship_to").alias(
                "ship_to"
            ),  # SDSHAN — Ship To (FK→dim)
            F.col("sd.reference_01").alias(
                "customer_po_number"
            ),  # SDVR01 — Customer PO Number
            F.col("sd.description_line_01").alias(
                "item_description"
            ),  # SDDSC1 — Item Description
            F.col("sd.user_reserved_number").alias("bol_number"),  # SDURAB — BOL Number
            F.col("sd.pull_signal").alias("alt_bol_number"),  # SDPSIG — ALT BOL Number
            F.col("sd.doc_voucher_invoice_e").alias(
                "invoice_number"
            ),  # SDDOC  — Invoice Number
            F.col("sd.container_id").alias("vehicle_number"),  # SDCNID — Vehicle Number
            F.col("sd.reference_02_vendor").alias("well_name"),  # SDVR02 — Well Name
            F.col("sd.uom_as_input").alias(
                "transactional_uom"
            ),  # SDUOM  — Transactional UOM
            F.col("scale.scale_gross_weight").alias(
                "gross_weight"
            ),  # MIGRWT — Gross Weight
            F.col("scale.scale_tare_weight").alias(
                "tare_weight"
            ),  # MICTWT — Tare Weight
            F.col("scale.scale_net_weight").alias("net_weight"),  # MIMXWT — Net Weight
            F.col("quantity_loaded"),  # Derived — Quantity Loaded (TN)
            # ── standard UoM conversion (volume factor/flag + price side) ──
            F.col("conversion_to_tons_rate"),
            F.col("missing_conversion_flag"),
            F.col("uom_pricing"),  # SDUOM4 — Source UoM (Price)
            F.col("unit_price"),  # SDUPRC — Source Price
            F.col("price_conversion_factor"),
            F.col("converted_price_per_ton"),
            F.col("price_missing_conversion_flag"),
            F.col("hdr.delivery_instructions").alias(
                "delivery_instructions"
            ),  # SHDEL1 — Delivery Instructions
            # ── Measure columns (SUMmed in outer GROUP BY) ────────────────────────
            F.col("sd.units_transaction_qty").alias("quantity_shipped_uom"),  # SDUORG
            F.col("sd.units_primary_qty_order").alias(
                "quantity_ordered_primary"
            ),  # SDPQOR
            # ── F0010 MTD reference columns ───────────────────────────────────────
            F.col("co.ccco").alias("company"),  # CCCO   — Company
            F.col("co.ccpnc").alias("current_period_number"),  # CCPNC
            F.col("co.ccarfj").alias("fiscal_year_begin_date"),  # CCARFJ
            # ── Scale ticket line reference ───────────────────────────────────────
            F.col("scale.scale_line_number").alias(
                "scale_line_number"
            ),  # MILNID — Line Number
            # ── Page Filter columns ───────────────────────────────────────────────
            F.col("sd.status_code_last").alias("last_status"),  # SDLTTR — Last Status
            F.col("sd.status_code_next").alias("next_status"),  # SDNXTR — Next Status
            F.col("sd.original_order_type").alias(
                "original_order_type"
            ),  # SDOCTO — Order Type
            F.col("sd.document_order_invoice_e").alias(
                "load_number"
            ),  # SDDOCO — Load #
            F.col("sd.line_type").alias("line_type"),  # SDLNTY — Line Type
            F.col("sd.identifier_second_item").alias("item_number"),  # SDLITM — Item #
            F.col("sd.identifier_short_item").alias("identifier_short_item"),  # SDITM
            F.col("sd.cost_center").alias("plant"),  # SDMCU  — Plant
            F.col("sd.date_requested_julian").alias(
                "requested_date"
            ),  # SDDRQJ — Requested Date
            F.col("sd.date_invoice_julian").alias(
                "invoice_date"
            ),  # SDIVD  — Invoice Date
            F.col("sd.dt_for_gl_and_vouch_01").alias("gl_date"),  # SDDGL  — GL Date
            F.col("sd.address_number_parent").alias(
                "parent_number"
            ),  # SDPA8  — Parent #
            # ── Audit ─────────────────────────────────────────────────────────────
            _jde_ts(F.col("sd.date_updated"), F.col("sd.time_of_day")).alias(
                "jde_updated_ts"
            ),  # SDUPMJ + SDTDAY
        )
        # DISTINCT — exact Hubble inner subquery behaviour
        .distinct()
    )

    # ─────────────────────────────────────────────────────────────────────────────
    # CELL 13 — Outer GROUP BY + SUM  (Hubble outer query)
    #
    # GROUP BY all non-measure columns.
    # SUM the measure columns: quantity_shipped_uom, quantity_ordered_primary.
    # quantity_loaded is deterministic per grain row → included in GROUP BY.
    #
    # NOTE: ship_to_name excluded from GROUP BY (not on fact — resolved via dim).
    # ─────────────────────────────────────────────────────────────────────────────
    GROUP_BY_COLS = [
        # ── Grain keys ────────────────────────────────────────────────────────────
        "key_company_order",
        "order_number",
        "order_type",
        "line_number",
        # ── Visual columns ────────────────────────────────────────────────────────
        "actual_ship_date",
        # ship_to_name intentionally excluded — resolved via dim in Power BI
        "ship_to",
        "customer_po_number",
        "item_description",
        "bol_number",
        "alt_bol_number",
        "invoice_number",
        "vehicle_number",
        "well_name",
        "transactional_uom",
        "gross_weight",
        "tare_weight",
        "net_weight",
        "quantity_loaded",
        # ── standard UoM conversion (per-line attributes) ──
        "conversion_to_tons_rate",
        "missing_conversion_flag",
        "uom_pricing",
        "unit_price",
        "price_conversion_factor",
        "converted_price_per_ton",
        "price_missing_conversion_flag",
        "delivery_instructions",
        # ── F0010 MTD reference ───────────────────────────────────────────────────
        "company",
        "current_period_number",
        "fiscal_year_begin_date",
        # ── Scale reference ───────────────────────────────────────────────────────
        "scale_line_number",
        # ── Page Filter columns ───────────────────────────────────────────────────
        "last_status",
        "next_status",
        "original_order_type",
        "load_number",
        "line_type",
        "item_number",
        "identifier_short_item",
        "plant",
        "requested_date",
        "invoice_date",
        "gl_date",
        "parent_number",
    ]

    df_fact = df_inner.groupBy(GROUP_BY_COLS).agg(
        F.sum("quantity_shipped_uom").alias("quantity_shipped_uom"),  # SUM(SDUORG)
        F.sum("quantity_ordered_primary").alias(
            "quantity_ordered_primary"
        ),  # SUM(SDPQOR)
        F.max("jde_updated_ts").alias("jde_updated_ts"),  # SDUPMJ + SDTDAY
    )

    return df_fact


# ════════════════════════════════════════════════════════════════════════════
# [FACT]  fact_extended_sales_order_3  ·  spine: F4215  ref: F4211
#

FACT_ESO3_LINE_KEYS = ["shipment_number"]


def build_fact_extended_sales_order_3(spine_df):
    df_f4215 = drop_deleted(spine_df)
    df_f4211 = get_ref(F4211)

    # Select required columns from F4215 (Shipment Header)
    #
    #   #   Heading                Bronze Col   Silver snake_case_field
    #   1   Shipment Number        XHSHPN       shipment_number          ← JOIN key
    #   2   Shipment Status        XHSSTS       shipment_status          ← PBI filter
    #   4   Business Unit          XHMCU        cost_center
    #   8   Freight Handling Code  XHFRTH       freight_handling_code
    #   9   Mode of Transport      XHMOT        mode_of_transport
    #   10  State                  XHADDS       state
    #   12  URRF                   XHURRF       user_reserved_reference
    #   14  Update Date            XHUPMJ       date_updated
    #   15  User ID                XHUSER       user_id
    df_f4215_sel = df_f4215.select(
        F.col("shipment_number"),  # XHSHPN — JOIN key
        F.col("shipment_status"),  # XHSSTS — Power BI page level filter
        F.col("cost_center"),  # XHMCU
        F.col("freight_handling_code"),  # XHFRTH
        F.col("mode_of_transport"),  # XHMOT
        F.col("state"),  # XHADDS
        F.col("user_reserved_reference"),  # XHURRF
        F.col("date_updated"),  # XHUPMJ
        F.col("time_of_day"),  # XHTDAY
        F.col("user_id"),  # XHUSER
    )

    # Select required columns from F4211 (Sales Order Detail)
    #   #   Heading          Bronze Col   Silver snake_case_field
    #   3   Company          SDCO         company
    #   5   Order Number     SDDOCO       document_order_invoice_e
    #   7   Order Type       SDDCTO       order_type
    #   11  Related Order #  SDRORN       related_po_so_number
    #   13  Actual Ship Date SDADDJ       actual_ship_date
    #   —   SUM target       SDUORG       units_transaction_qty
    #   —   JOIN key         SDSHPN       shipment_number
    df_f4211_sel = df_f4211.select(
        F.col("shipment_number"),  # SDSHPN — JOIN key
        F.col("company"),  # SDCO
        F.col("document_order_invoice_e"),  # SDDOCO
        F.col("order_type"),  # SDDCTO
        F.col("related_po_so_number"),  # SDRORN
        F.col("actual_ship_date"),  # SDADDJ
        F.col("units_transaction_qty"),  # SDUORG — SUM target
    )

    # ─────────────────────────────────────────────────────────────────────────────
    #  INNER JOIN F4215 → F4211
    #  Bronze : F4215.XHSHPN = F4211.SDSHPN
    #  Silver : F4215.shipment_number = F4211.shipment_number
    # ─────────────────────────────────────────────────────────────────────────────
    df_joined = df_f4215_sel.alias("F4215").join(
        df_f4211_sel.alias("F4211"),
        F.col("F4215.shipment_number") == F.col("F4211.shipment_number"),
        how="inner",
    )

    # ─────────────────────────────────────────────────────────────────────────────
    # Project all output columns post-join
    #           Final column names = silver snake_case_field (no bronze aliases)
    #           Ambiguous shipment_number resolved via table alias
    # ─────────────────────────────────────────────────────────────────────────────
    df_projected = df_joined.select(
        # ── From F4215 ───────────────────────────────────────────────────────────
        F.col("F4215.shipment_number"),  # XHSHPN  | Shipment Number
        F.col("F4215.shipment_status"),  # XHSSTS  | Shipment Status
        F.col("F4215.cost_center"),  # XHMCU   | Business Unit
        F.col("F4215.freight_handling_code"),  # XHFRTH  | Freight Handling Code
        F.col("F4215.mode_of_transport"),  # XHMOT   | Mode of Transport
        F.col("F4215.state"),  # XHADDS  | State
        F.col("F4215.user_reserved_reference"),  # XHURRF  | URRF
        F.col("F4215.date_updated"),  # XHUPMJ  | Update Date
        _jde_ts(
            F.col("F4215.date_updated"), F.col("F4215.time_of_day")
        ).alias("jde_updated_ts"),
        F.col("F4215.user_id"),  # XHUSER  | User ID
        # ── From F4211 ───────────────────────────────────────────────────────────
        F.col("F4211.company"),  # SDCO   | Company
        F.col("F4211.document_order_invoice_e"),  # SDDOCO | Order Number
        F.col("F4211.order_type"),  # SDDCTO | Order Type
        F.col("F4211.related_po_so_number"),  # SDRORN | Related Order #
        F.col("F4211.actual_ship_date"),  # SDADDJ | Actual Ship Date
        F.col("F4211.units_transaction_qty"),  # SDUORG | SUM target
    )

    # ─────────────────────────────────────────────────────────────────────────────
    #  GROUP BY + SUM aggregation
    #           All group-by keys use silver snake_case_field names directly
    #           Mirrors SQL GROUP BY exactly
    # ─────────────────────────────────────────────────────────────────────────────
    GROUP_BY_COLS = [
        "shipment_number",  # XHSHPN / SDSHPN
        "shipment_status",  # XHSSTS
        "company",  # SDCO
        "cost_center",  # XHMCU
        "document_order_invoice_e",  # SDDOCO
        "order_type",  # SDDCTO
        "freight_handling_code",  # XHFRTH
        "mode_of_transport",  # XHMOT
        "state",  # XHADDS
        "related_po_so_number",  # SDRORN
        "user_reserved_reference",  # XHURRF
        "actual_ship_date",  # SDADDJ
        "date_updated",  # XHUPMJ
        "user_id",  # XHUSER
    ]

    df_aggregated = df_projected.groupBy(GROUP_BY_COLS).agg(
        F.sum("units_transaction_qty").alias("units_transaction_qty_sum"),  # SUM(SDUORG)
        F.max("jde_updated_ts").alias("jde_updated_ts"),  # XHUPMJ + XHTDAY
    )

    return df_aggregated


# ════════════════════════════════════════════════════════════════════════════
# [FACT]  dim_invoice_reconciliation  ·  spine: F4211  ref: F0006,F0101,F0116
#
# Grain is INVOICE, not F4211 line: one row per (document_company,
# invoice_number, document_type).  Gold names differ from Silver, so both key
# lists are required — fact_key is hashed from Silver keys in the handler and
# from Gold keys in generic_recompute, and the two must resolve to the same
# values.
DIM_INVOICE_RECONCILIATION_GOLD_LINE_KEYS = [
    "document_company",
    "invoice_number",
    "document_type",
]
DIM_INVOICE_RECONCILIATION_SILVER_LINE_KEYS = [
    "company_key",
    "doc_voucher_invoice_e",
    "document_type",
]

def build_dim_invoice_reconciliation(spine_df):
    # ── Coarser-grain rebuild ────────────────────────────────────────────────
    # This dim aggregates F4211 (line grain) up to invoice grain.  generic_recompute
    # hands us only the CHANGED spine rows, but the groupBy below derives
    # MIN()/first() representatives that are only correct when computed over the
    # COMPLETE set of lines on each affected invoice — a consolidated invoice can
    # carry hundreds of orders, and rebuilding from one changed line would pick the
    # wrong representative.
    # So: take the invoice keys from the batch, then re-read F4211 for every line on
    # those invoices.  Same pattern build_fact_eso1 uses for F4211/F42119.
    # spine_df ALREADY carries every line of each affected invoice, so no re-read
    # is needed here:
    #   full load  — the whole F4211 table is passed in.
    #   streaming  — "rebuild_from_silver": True makes generic_recompute join the
    #                full F4211 to the affected invoice keys before handing it over,
    #                which is also what brings back invoices whose only change in the
    #                batch was a deleted line.
    # spine_columns lists exactly the 12 columns aggregated below, so nothing is
    # missing.  Re-reading F4211 a second time here would double the scan cost.
    df_f4211 = drop_deleted(spine_df)

    df_f0006 = get_ref(F0006)
    df_f0101 = get_ref(F0101)
    df_f0116 = get_ref(F0116)

    # ---------------------------------------------------------------------------
    # LOOKUP tables — address (F0101 x F0116) and business unit (F0006)
    #
    # Address lookup: F0101 INNER F0116 gated to ABAT1 search-type band, then
    # row_number over date_beginning_effective DESC picks the latest-effective
    # address per ABAN8. Deterministic across runs.
    # ---------------------------------------------------------------------------
    _adr_w = Window.partitionBy(F.col("ad.address_number")).orderBy(
        F.col("ad.date_beginning_effective").desc_nulls_last(),
        F.col("ad.date_updated").desc_nulls_last())

    _abat1 = F.rpad(F.rtrim(F.col("ab.address_type_01")), 3, " ")
    _abat1_band = (((_abat1 >= F.lit("A  ")) & (_abat1 <= F.lit("P  "))) |
                ((_abat1 >= F.lit("R  ")) & (_abat1 <= F.lit("ZZZ"))))

    address = (df_f0101.alias("ab")
            .join(df_f0116.alias("ad"), F.col("ab.address_number") == F.col("ad.address_number"), "inner")
            .where(_abat1_band)
            .withColumn("_arn", F.row_number().over(_adr_w))
            .where(F.col("_arn") == 1)
            .select(F.col("ab.address_number").alias("addr_key"),
                    F.trim(F.col("ab.standard_industry_code")).alias("sic_code"),   # ABSIC
                    F.trim(F.col("ad.state")).alias("jurisdiction"),                 # ALADDS
                    F.trim(F.col("ad.county_address")).alias("county")))              # ALCOUN

    # Business unit collapse: one row per MCU providing MCRP20 for the business_stream calc
    bunit = (df_f0006.groupBy(F.col("cost_center").alias("bu_key"))
            .agg(F.first(F.trim(F.col("category_code_cost_ct_020")), ignorenulls=True)
                .alias("business_stream_code")))


    # ---------------------------------------------------------------------------
    # Pre-aggregate F4211 to invoice grain
    #
    # Consolidated invoices carry many order_numbers (SDDOCO) sharing one invoice.
    # MIN(SDDOCO) picks a deterministic representative — matches Hubble's
    # Reconciliation Version arbitrary pick (verified against the 4 SBX invoices
    # where 371 / 249 / 121 / 2 orders each collapse to 1 row).
    #
    # first(ignorenulls=True) on ship_to / sold_to / parent / plant is safe because
    # every order under a single invoice ships to the same address (verified in
    # Silver during the ESO4 v2 investigation).
    # ---------------------------------------------------------------------------
    sd_invoice = (
        df_f4211.filter(F.col("doc_voucher_invoice_e") > 0)   # skip uninvoiced F4211 lines
        .groupBy(
            F.col("doc_voucher_invoice_e").alias("_inv_no"),      # SDDOC
            F.col("document_type").alias("_inv_doc_type"),        # SDDCT
            F.col("company_key").alias("_inv_co_key"),            # SDKCO
        ).agg(
            F.min("document_order_invoice_e").alias("order_number"),  # SDDOCO representative
            F.min("order_type").alias("order_type"),                  # SDDCTO representative
            F.min("line_number").alias("line_number"),                # SDLNID representative
            F.first("cost_center",             ignorenulls=True).alias("cost_center"),
            F.first("address_number",          ignorenulls=True).alias("sold_to"),
            F.first("address_number_ship_to",  ignorenulls=True).alias("ship_to"),
            F.first("address_number_parent",   ignorenulls=True).alias("parent_number"),
            F.first("tax_explanation_code_01", ignorenulls=True).alias("tax_explanation_code"),
            F.max("dt_for_gl_and_vouch_01").alias("gl_date"),
            # Latest line update across every line on the invoice — SDUPMJ + SDTDAY
            F.max(_jde_ts(F.col("date_updated"), F.col("time_of_day"))).alias("jde_updated_ts"),
        )
    )


    # ---------------------------------------------------------------------------
    # Enrich with F0006 (business_stream_code) and F0101/F0116 (SIC + address)
    # ---------------------------------------------------------------------------
    j = (sd_invoice.alias("sd")
        .join(bunit.alias("bu"),   F.col("sd.cost_center") == F.col("bu.bu_key"),      "left")
        .join(address.alias("ad"), F.col("sd.ship_to")     == F.col("ad.addr_key"),    "left"))

    # Business Stream — ABSIC (ship-to F0101) x MCRP20 (F0006 business unit)
    _absic  = F.trim(F.col("ad.sic_code"))
    _mcrp20 = F.trim(F.col("bu.business_stream_code"))
    business_stream = (F.when((_absic == "F") & (_mcrp20 == "ENG"), F.lit("O&G"))
                        .when((_absic != "F") & (_mcrp20 == "ENG"), F.lit("ISP"))
                        .when((_absic != "F") & (_mcrp20 == "SHR"), F.lit("ISP"))
                        .when((_absic == "F") & (_mcrp20 == "SHR"), F.lit("O&G"))
                        .when(~_mcrp20.isin("ENG", "SHR"), F.lit("ISP")))

    # Avalara Code — RTRIM(LTRIM(NVL(SDDOC,-999999999))) || RTRIM(LTRIM(NVL(SDDCT,''))) || RTRIM(LTRIM(NVL(SDKCO,'')))
    avalara_code = F.concat(
        F.coalesce(F.trim(F.col("sd._inv_no").cast("long").cast("string")), F.lit("-999999999")),
        F.coalesce(F.trim(F.col("sd._inv_doc_type")),                       F.lit("")),
        F.coalesce(F.trim(F.col("sd._inv_co_key").cast("string")),          F.lit("")))



    # ---------------------------------------------------------------------------
    # Final projection — one row per invoice with all attributes
    # ---------------------------------------------------------------------------
    df_dim = (j.select(
        F.col("sd._inv_co_key").alias("document_company"),
        F.col("sd._inv_no").alias("invoice_number"),
        F.col("sd._inv_doc_type").alias("document_type"),
        F.col("sd.order_number"),
        F.col("sd.order_type"),
        F.col("sd.line_number"),
        F.trim(F.col("sd.cost_center")).alias("plant"),
        F.col("sd.sold_to"),
        F.col("sd.ship_to"),
        F.col("sd.parent_number"),
        F.col("sd.tax_explanation_code"),
        avalara_code.alias("avalara_code"),
        F.trim(F.col("bu.business_stream_code")).alias("plant_invoice"),
        business_stream.alias("business_stream"),
        F.col("ad.sic_code"),
        F.col("ad.jurisdiction"),
        F.col("ad.county"),
        F.col("sd.gl_date"),
        F.col("sd.jde_updated_ts"),
    )
    .withColumn("invoice_scope_key",
                sk("document_company", "invoice_number", "document_type"))
    .withColumn("order_scope_key",
                F.when(
                    F.col("order_number").isNotNull()
                    & F.col("order_type").isNotNull()
                    & F.col("document_company").isNotNull(),
                    sk("document_company", "order_number", "order_type")
                ))
    )

    return df_dim


# fact_customer_ledger MOVED OUT 2026-08-19 -> Dimension notebooks\
#   nb_silver_to_gold_cdf_fact_customer_ledger.py
#
# It is a slow table and was dragging the shared module drain.  The build,
# the line keys and the column list all moved there unchanged, and so did the
# DELETE+APPEND semantics — only the schedule differs.  That notebook RESUMES
# FROM THIS MODULE'S CHECKPOINT, so do not delete
#   Files/checkpoints/module1/fact_customer_ledger/
# and do not re-add the fact here: two writers on one Gold table would collide.
#
# F03B11 is left in the Silver constants above but is no longer read here.
# ════════════════════════════════════════════════════════════════════════════
# [FACT]  fact_sales_commission  ·  spine: F4211  extra spine: F42119
#         refs: F42005 (active), F4201 + F0101 + F4074 + F41002 (passive)

FACT_COMMISSION_GOLD_LINE_KEYS = [
    "company_key_order_no", "order_type", "order_number", "line_number",
]
FACT_COMMISSION_SILVER_LINE_KEYS = [
    "company_key_order_no", "order_type", "document_order_invoice_e", "line_number",
]

_COMM_LINE_COLS = FACT_COMMISSION_SILVER_LINE_KEYS

_COMM_KEEP = [
    "company_key_order_no", "order_type", "document_order_invoice_e", "line_number",
    "company", "cost_center", "address_number_ship_to", "identifier_short_item",
    "identifier_second_item", "line_type", "uom_primary", "uom_pricing",
    "sales_reporting_code_05", "doc_voucher_invoice_e",
    "status_code_next", "status_code_last",
    "amount_extended_price", "amount_extended_cost",
    "units_quantity_shipped", "units_primary_qty_order",
    "actual_ship_date", "dt_for_gl_and_vouch_01",
    # ── added line-level (F4211 driver) display/filter attributes ──
    "sales_reporting_code_03", "sales_reporting_code_04", "freight_handling_code",
    "shipment_number", "uom_as_input", "mode_of_transport", "payment_terms_code_01",
    "gl_class", "date_transaction_julian", "date_invoice_julian",
    "date_updated", "time_of_day",          # SDUPMJ + SDTDAY -> jde_updated_ts
]

_COMM_RAW_DATES = ["commission_paid_date", "gl_date", "actual_ship_date",
                   "date_transaction_julian", "date_invoice_julian"]
COMMISSION_SHIFT_FACTOR = 1.0


def build_fact_sales_commission(spine_df):
    # Line keys from whichever spine fired (F4211 or F42119), then re-read BOTH
    # sources for those lines.  Re-reading here rather than trusting filtered_spine
    # keeps the F42119 column fix-up inside build_fn — F42119 names SDLITM as
    # identifier_2nd_item, so a spine_columns select on an F42119 batch would throw.
    line_keys_df = drop_deleted(spine_df).select(*_COMM_LINE_COLS).distinct()

    f4211_rows = drop_deleted(spark.read.table(sname(F4211))).join(
        line_keys_df, on=_COMM_LINE_COLS, how="inner")
    f42119_rows = drop_deleted(spark.read.table(sname(F42119))).join(
        line_keys_df, on=_COMM_LINE_COLS, how="inner")
    if ("identifier_2nd_item" in f42119_rows.columns
            and "identifier_second_item" not in f42119_rows.columns):
        f42119_rows = f42119_rows.withColumnRenamed(
            "identifier_2nd_item", "identifier_second_item")

    ln = f4211_rows.unionByName(f42119_rows, allowMissingColumns=True)
    ln = ln.select(*[c for c in _COMM_KEEP if c in ln.columns]) \
           .dropDuplicates(_COMM_LINE_COLS)

    sc = get_ref(F42005)
    sh = get_ref(F4201)
    cc = get_ref(F0101)

    # F4074 → per-LINE price_adjustment_type. F4074 is line×adjustment, so it is aggregated to ONE row per
    # line before the LEFT join → it CANNOT fan the line×commission grain. ⚠ A line can carry several ALAST
    # codes; this keeps the alphabetically-first (min) = the line's "primary" adjustment (same convention as
    # the price-adjustment fact). Change to a concat/whitelist pick if the report needs a different code.
    padj_line = (get_ref(F4074)
                 .groupBy("company_key_order_no", "order_type", "document_order_invoice_e", "line_number")
                 .agg(F.min(F.trim(F.col("price_adjustment_type"))).alias("price_adjustment_type")))
    # F41002 → item-level UOM structure (UMUSTR, blank cost-center); ONE row per item so the LEFT join
    # can't fan the grain (UMUSTR is an item attribute — constant across the item's UOM rows).
    uom_str = (get_ref(F41002).filter(F.trim(F.col("cost_center")) == "")
               .groupBy("identifier_short_item")
               .agg(F.max(F.trim(F.col("uom_structure"))).alias("uom_structure"))
               .withColumnRenamed("identifier_short_item", "us_itm"))

    # SDUOM->TN volume cascade, added 2026-08-24 (D:\ussilica\Module 1 developer nbs\ESO1\
    # nb_silver_to_gold_eso1_fact_sales_commission.py) — item-specific (F41002) then standard
    # (F41003), each resolved DIRECTLY to TN, no IMUOM1 pivot. This is its OWN small cascade,
    # not a call into _price_adj_conv_lookups (:4188) — that one is a different, more general
    # shape (keyed generically, pivoted through the item's primary UoM afterward). The dev's
    # own two notebooks (freight, commission) duplicate this exact construction rather than
    # share it, so this mirrors that duplication instead of introducing a shared helper.
    # F41003 is an unregistered, direct read — same precedent as _price_adj_conv_lookups
    # (:4223): no column on it the spine can narrow on, and it's a small standard-conversion
    # table, so a full read is correct.
    _vi_f41002 = get_ref(F41002)
    vol_item_fwd = (
        _vi_f41002.filter((F.trim("related_uom") == "TN") & (F.col("conversion_factor") != 0))
        .select(
            F.col("identifier_short_item").alias("itm"),
            F.trim("uom").alias("from_uom"),
            F.col("conversion_factor").cast("double").alias("conv_factor"),
        )
    )
    vol_item_rev = (
        _vi_f41002.filter((F.trim("uom") == "TN") & (F.col("conversion_factor") != 0))
        .select(
            F.col("identifier_short_item").alias("itm"),
            F.trim("related_uom").alias("from_uom"),
            (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_factor"),
        )
    )
    vol_item = vol_item_fwd.unionByName(vol_item_rev)

    _vi_f41003 = drop_deleted(spark.read.table(sname(F41003)))
    vol_std_fwd = _vi_f41003.select(
        F.trim("uom").alias("from_uom"),
        F.trim("related_uom").alias("to_uom"),
        F.col("conversion_factor").cast("double").alias("conv_std"),
    )
    vol_std_rev = (
        _vi_f41003.filter(F.col("conversion_factor").cast("double") != 0)
        .select(
            F.trim("related_uom").alias("from_uom"),
            F.trim("uom").alias("to_uom"),
            (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_std"),
        )
    )
    vol_std = vol_std_fwd.unionByName(vol_std_rev)

    j = (ln.alias("ln")
         .join(sc.alias("sc"),
               (F.col("ln.company_key_order_no")     == F.col("sc.company_key_order_no")) &
               (F.col("ln.order_type")               == F.col("sc.order_type")) &
               (F.col("ln.document_order_invoice_e") == F.col("sc.document_order_invoice_e")) &
               (F.col("ln.line_number")              == F.col("sc.line_number")), "left")
         .join(sh.alias("sh"),
               (F.col("ln.company_key_order_no")     == F.col("sh.company_key_order_no")) &
               (F.col("ln.document_order_invoice_e") == F.col("sh.document_order_invoice_e")) &
               (F.col("ln.order_type")               == F.col("sh.order_type")), "left")
         .join(cc.alias("cc"),
               F.col("cc.address_number") == F.col("sh.address_number"), "left")
         .join(padj_line.alias("pa"),                                                     # F4074 per-line primary adjustment (1 row/line — no fan)
               (F.col("pa.company_key_order_no") == F.col("ln.company_key_order_no")) &
               (F.col("pa.order_type") == F.col("ln.order_type")) &
               (F.col("pa.document_order_invoice_e") == F.col("ln.document_order_invoice_e")) &
               (F.col("pa.line_number") == F.col("ln.line_number")), "left")
         .join(uom_str.alias("us"), F.col("us.us_itm") == F.col("ln.identifier_short_item"), "left")  # F41002 UMUSTR (1 row/item — no fan)
         # SDUOM->TN cascade, added 2026-08-24: item-specific (F41002) then standard (F41003)
         .join(
             vol_item.alias("ci"),
             (F.col("ci.itm") == F.col("ln.identifier_short_item"))
             & (F.col("ci.from_uom") == F.trim(F.col("ln.uom_as_input"))),
             "left",
         )
         .join(
             vol_std.alias("cs"),
             (F.col("cs.from_uom") == F.trim(F.col("ln.uom_as_input")))
             & (F.col("cs.to_uom") == F.lit("TN")),
             "left",
         ))

    # Conversion Factor (Volume): SDUOM->TN — TN->1.0, else item F41002, else standard F41003,
    # else NULL (drives the flag). Added 2026-08-24.
    vol_factor_raw = F.coalesce(
        F.when(F.trim(F.col("ln.uom_as_input")) == "TN", F.lit(1.0)),
        F.col("ci.conv_factor"),
        F.col("cs.conv_std"),
    )
    vol_factor = F.coalesce(vol_factor_raw, F.lit(1.0))

    sel = j.select(
        F.col("ln.company").alias("company"),
        F.col("ln.company_key_order_no").alias("company_key_order_no"),
        F.col("ln.order_type").alias("order_type"),
        F.col("ln.document_order_invoice_e").alias("order_number"),
        F.col("ln.line_number").alias("line_number"),
        F.col("sc.commission_line_number").alias("commission_line_number"),
        F.col("sc.salesperson").alias("salesperson"),
        F.col("sc.commission_code_type").alias("commission_code_type"),
        F.col("ln.address_number_ship_to").alias("ship_to"),
        F.col("sh.address_number").alias("sold_to"),
        F.col("sh.address_number_parent").alias("address_number_parent"),
        F.trim(F.coalesce(F.col("ln.cost_center"), F.col("sc.cost_center"))).alias("branch_plant"),
        F.coalesce(F.col("ln.identifier_short_item"),
                   F.col("sc.identifier_short_item")).alias("item_number_short"),
        F.col("sc.date_commission_paid").alias("commission_paid_date"),
        F.col("ln.dt_for_gl_and_vouch_01").alias("gl_date"),
        F.col("ln.actual_ship_date").alias("actual_ship_date"),
        F.col("sc.percent_commission").alias("percent_commission"),
        F.col("sc.amount_commission").alias("amount_commission"),
        F.col("sc.amt_related_commission").alias("amount_related_commission"),
        F.col("sc.percent_related_commiss").alias("percent_related_commission"),
        F.col("sc.flat_commission_amount").alias("flat_commission_amount"),
        F.col("sc.amount_per_unit").alias("amount_per_unit"),
        F.col("sc.amount_sales_total_line").alias("amount_sales_total_line"),
        F.col("sc.amount_total_line_cost").alias("amount_sales_line_total_cost"),
        F.col("sc.amount_line_gross_margin").alias("amount_line_gross_margin"),
        F.col("sc.amt_line_eligible_margin").alias("amount_line_eligible_margin"),
        F.col("ln.amount_extended_price").alias("extended_price"),
        F.col("ln.amount_extended_cost").alias("extended_cost"),
        F.col("ln.units_quantity_shipped").alias("quantity_shipped"),
        F.col("ln.units_primary_qty_order").alias("primary_quantity_ordered"),
        F.col("ln.doc_voucher_invoice_e").alias("invoice_number"),
        F.col("ln.identifier_second_item").alias("second_item_number"),
        F.col("ln.line_type").alias("line_type"),
        F.col("ln.uom_primary").alias("uom_primary"),
        F.col("ln.uom_pricing").alias("uom_pricing"),
        F.col("ln.sales_reporting_code_05").alias("sales_reporting_code_05"),
        F.col("ln.status_code_next").alias("status_code_next"),
        F.col("ln.status_code_last").alias("status_code_last"),
        F.col("cc.report_code_add_book_010").alias("category_code_10"),
        F.col("cc.address_type_01").alias("sold_to_search_type"),
        # ── added line-level (F4211 driver) display/filter attributes — repeat across the commission fan ──
        F.col("ln.sales_reporting_code_03").alias("sales_reporting_code_03"),        # SDSRP3
        F.col("ln.sales_reporting_code_04").alias("sales_reporting_code_04"),        # SDSRP4
        F.col("ln.freight_handling_code").alias("freight_handling_code"),            # SDFRTH
        F.col("ln.shipment_number").alias("shipment_number"),                        # SDSHPN
        F.col("ln.uom_as_input").alias("uom_as_input"),                              # SDUOM
        F.trim(F.col("ln.mode_of_transport")).alias("mode_of_transport"),            # SDMOT
        F.col("ln.payment_terms_code_01").alias("payment_terms_code_01"),            # SDPTC
        F.col("ln.gl_class").alias("gl_class"),                                      # SDGLC
        F.col("ln.date_transaction_julian").alias("date_transaction_julian"),        # SDTRDJ (order date)
        F.col("ln.date_invoice_julian").alias("date_invoice_julian"),                # SDIVD (invoice date)
        # ── added lookups (fan-safe: aggregated to line/item grain) ──
        F.col("pa.price_adjustment_type").alias("price_adjustment_type"),            # F4074 ALAST (per-line primary)
        F.col("us.uom_structure").alias("uom_structure"),                            # F41002 UMUSTR (item-level)
        # volume conversion (SDUOM->TN cascade), added 2026-08-24
        vol_factor.alias("conversion_to_tons_rate"),
        F.when(vol_factor_raw.isNull(), F.lit("Y")).otherwise(F.lit("N")).alias("missing_conversion_flag"),
        (F.col("ln.units_quantity_shipped").cast("double") * vol_factor).alias("quantity_shipped_tons"),
        F.lit(COMMISSION_SHIFT_FACTOR).cast("double").alias("shift_factor_applied"),
        _jde_ts(F.col("ln.date_updated"), F.col("ln.time_of_day")).alias(
            "jde_updated_ts"
        ),  # SDUPMJ + SDTDAY
    ).distinct()

    # Sentinel-date cleanup — reuse the ESO1 helpers (identical window: 2000-01-01
    # to Dec 31 of current year + 25).
    for _dc in _COMM_RAW_DATES:
        if _dc in sel.columns:
            sel = sel.withColumn(_dc, _eso1_clean_date(F.col(_dc)))

    # 'Y' on exactly one row per sales line (lowest commission_line_number; the sole
    # row when there is no commission).  Partition = the line keys, which is the same
    # key set the engine filters on, so every partition here is complete.
    _cw = (Window.partitionBy("company_key_order_no", "order_type",
                              "order_number", "line_number")
           .orderBy(F.col("commission_line_number").asc_nulls_last()))

    df = (sel
          .withColumn("_crn", F.row_number().over(_cw))
          .withColumn("is_primary_commission_line",
                      F.when(F.col("_crn") == 1, F.lit("Y")).otherwise(F.lit("N")))
          .drop("_crn")
          .withColumn("commission_paid_date_key", _eso1_date_key(F.col("commission_paid_date")))
          .withColumn("gl_date_key",              _eso1_date_key(F.col("gl_date")))
          .withColumn("ship_date_key",            _eso1_date_key(F.col("actual_ship_date"))))

    df = (df
          .withColumn("sales_commission_key",
                      sk("company_key_order_no", "order_type", "order_number", "line_number",
                         F.coalesce(F.col("commission_line_number").cast("string"),
                                    F.lit("__NOCOMM__"))))
          .withColumn("order_scope_key",
                      sk("company_key_order_no", "order_type", "order_number")))

    return df.dropDuplicates(["sales_commission_key"])

# ════════════════════════════════════════════════════════════════════════════
# [FACT]  fact_extended_sales_order_5  ·  spines: F4211 + F4311  (2 streams)
#         passive refs: F554201T, F43121, F4201
#         read directly inside build_fn: F0911, F0101, F0005
#
# ONE Gold table serving FIVE SBX Load/PO reports; `row_class` is the row
# discriminator (LINE | HOLADD | TEXT | PO_HOLADD | PO_OTHER).  Ported from
# nb_silver_to_gold_extended_sales_order_5 — the two leg builders,
# _load_aggregates, and every CASE-instead-of-WHERE decision are the
# developer's and are UNCHANGED.  Names carry an _eso5_ / _ESO5_ prefix only so
# they cannot collide when Modules 2-5 land in the same notebook.
#
# GRAIN IS THE LOAD, NOT THE LINE — one load fans out to many Gold rows.
#   Every per-load value (bol / sand_ticket / sbx_weight / load_last_status /
#   the leg-B FRT back-fill) is computed across the WHOLE load, and leg B is
#   back-filled FROM the load.  One changed line must therefore rebuild every
#   row of its load: at line grain the DELETE would miss the siblings and the
#   back-fill would silently go stale.
#
# ⚠ order_type is deliberately NOT a key column.  _eso5_po_lines() forces
#   l_dcto = 'SX' so the UNION lines up, while the F4311 row behind it carries
#   PDDCTO = 'OX' — so document_type on a Gold row does not identify anything
#   and cannot be hashed.  Leaving it out means an SO and an SX order that share
#   a doco share a fact_key; that is OVER-scoping, which is safe (build_fn
#   filters on (kcoo, doco) with no order-type predicate, so it regenerates
#   both).  Under-scoping is the bug; over-scoping is only extra work.
#
# ⚠ fact_key needs RAW key values.  Gold trims company and casts load_number to
#   long, and build_fn casts DOCO to long for the cross-table joins — so Silver
#   "1184310.000000000000000000" becomes Gold "1184310" and the two MD5s
#   diverge (the ESO6 cast bug arriving by a different route).  load_company_raw
#   / load_number_raw carry the untouched Silver values through both legs for
#   the sole purpose of feeding fact_key.  They are not for reporting.
# ════════════════════════════════════════════════════════════════════════════

FACT_ESO5_GOLD_LINE_KEYS = ["load_company_raw", "load_number_raw"]
FACT_ESO5_SILVER_LINE_KEYS = ["company_key_order_no", "document_order_invoice_e"]

# ── scaling ─────────────────────────────────────────────────────────────────
# RAW JDE integers carry implied decimals (qty /1000, rate ABURAT*0.01, etc.) —
# Silver HAS ALREADY DECODED them, so those divisors drop out here.  Only the
# BUSINESS factor survives: qty(COM) = units / 2000 (tons).
_ESO5_RATE_FACTOR = 1.0  # ABURAT already decoded
_ESO5_TONS_DIVISOR = 2000.0  # COM lines: units → tons

# TRUE ⇒ NO row-selecting predicate anywhere: leg B takes the WHOLE of F4311,
# not just the `order_type='OX' AND item='HOLADD'` lines.  Those two conditions
# become the `row_class` calculation instead ('PO_HOLADD' vs 'PO_OTHER'), and
# item_number (PDLITM) + po_order_type (PDDCTO) are on every PO row so the
# consumer can filter to OX/HOLADD itself.
_ESO5_PO_LEG_UNFILTERED = True

_ESO5_UDC_SYS, _ESO5_UDC_TYPE = "55", "UP"  # DRSY='55' AND DRRT='UP'

# Display columns STORED on the fact — the GROUP BY grain.
_ESO5_CORE_COLS = [
    "load_number",  # SDDOCO
    "document_type",  # SDDCTO
    "company",  # SDKCOO
    "district",  # SDMCU
    "sold_to",  # SDAN8
    "ship_to",  # SDSHAN
    "carrier",  # SDCARS
    "customer_po",  # SDVR01
    "sand_po_number",  # F554201T QCDS50
    "uss_customer_po",  # SOPONO
    "item_number",  # SDLITM
    "item_description",  # SDDSC1
    "order_date",  # SDTRDJ
    "gl_date",  # SDDGL
    "loading_facility",  # LOFA=SDVEND
    "uss_match",  # MATCHFLAG
    "uss_so_order_no",  # SOORDERNO
    "uss_so_weight",  # SOWEIGHT
    "sbx_weight",  # SXWEIGHT
    "so_alt_bol_no",  # SOALTBOLNO
    "sand_ticket",  # SANDTKT
    "bol",  # BOL
    "uom",  # SDUOM
    "quantity",  # QTY (derived)
    "unit_price",  # SDUPRC
    "total_amount",  # SDAEXP
    "last_status",  # SDLTTR
    "next_status",  # SDNXTR
    "invoice_number",  # SDDOC
    "ox_last_status",  # F4311 OXLTTR
    "ox_next_status",  # F4311 OXNXTR
    "ox_amount",  # F4311 OXAMT
    "carrier_po_gl_post_flag",  # F0911 GLPOST
    "carrier_po_gl_post_flag_desc",  # GLPOST as "code - description", e.g. "P - Posted"
    "po_receipt_gl_date",  # F43121 GLDGJ
    "line_id",  # SDLNID
]
# STATUS — every status field stored so the consumer can do all status
# filtering.  NO status predicate filters a row.
_ESO5_STATUS_COLS = [
    "next_status_num",  # numeric copy of next_status for a `next_status_num < 581`
    # page filter.  Direct Lake forbids calculated columns, so
    # the numeric form must be physical.
    "po_holadd_superseded",  # 'Y' ⇔ the load has a LIVE (last_status<>'980') SX
    # HOLADD sales line.  WAS a `NOT EXISTS(...)` row filter.
    "po_order_type",  # PDDCTO on a PO row (document_type is forced to 'SX')
    "load_max_last_status",  # MXLTTR — per-load MAX(SDLTTR)
    "load_min_last_status",  # MILTTR — per-load MIN(SDLTTR)
    "ox_amount_gross",  # the F4311 OX money with NO item/status condition
]
_ESO5_VARIATION_COLS = [
    "row_class",  # LINE | HOLADD | TEXT | PO_HOLADD | PO_OTHER
    "line_type",  # SDLNTY  ('TL' = text lines)
    "product_category",  # SDPRP1  ('COM' sand, 'FRT' freight)
    "sales_report_code_01",  # SDSRP1
    "units_ordered",  # SDUORG — RAW decoded units
    "item_weight",  # SDITWT
    "load_last_status",  # per-load SDLTTR CASE
    "leg_1",  # QCLGL1
    "leg_2",  # QCLGL2
    "leg_3",  # QCLGL3
    "qc_string_3",  # QCFSTR3
    "header_district",  # SHMCU
    "header_sold_to",  # SHAN8
    "header_ship_to",  # SHSHAN
    "header_carrier",  # SHCARS
    "header_customer_po",  # SHVR01
    "header_order_date",  # SHTRDJ
    # ── standard UoM conversion (per-line attributes), added 2026-08-24 ──
    "conversion_to_tons_rate",
    "quantity_shipped_tons",
    "missing_conversion_flag",
    "uom_pricing",
    "price_conversion_factor",
    "converted_price_per_ton",
    "price_missing_conversion_flag",
]
_ESO5_GROUP_BY_COLS = _ESO5_CORE_COLS + _ESO5_STATUS_COLS + _ESO5_VARIATION_COLS
# SUMmed measure.  Semi-additive (constant per LOFA).
_ESO5_MEASURE_COLS = ["lofa_rate"]
_ESO5_BUSINESS_COLS = _ESO5_GROUP_BY_COLS + _ESO5_MEASURE_COLS


def _eso5_pad30(c):
    """rpad(rtrim(rtrim(x,' '),'.'), 30, ' ') — trim trailing spaces then dots."""
    return F.rpad(F.regexp_replace(F.rtrim(c), r"\.+$", ""), 30, " ")


def _eso5_pad25(c):
    """rpad(rtrim(x), 25, ' ')."""
    return F.rpad(F.rtrim(c), 25, " ")


def _eso5_load_scope_expr(kcoo_col, dcto_col, doco_col):
    """The per-load scope key, STORED on the fact.  The trim + long cast
    normalise the key ("00750||SX ||1184310.0" -> "00750||SX||1184310") so it is
    stable.  Vestigial under the batch build; under v3 it is close to the real
    load key, but fact_key is what the engine deletes on — not this."""
    return sk(F.trim(F.col(kcoo_col)), F.trim(F.col(dcto_col)), F.col(doco_col).cast("long"))


def _eso5_plant_mcu():
    """Vendor → plant MCU (DRSPHD) from Silver F0005 UDC 55/UP — the ONLY
    F0005-derived value the fact needs at build time (the SO-match):
      vendor_number = TO_NUMBER(rtrim(DRKY)) ; lofa_mcu = trim(DRSPHD).
    NO F0005 column is stored on the fact.

    Left un-narrowed on purpose: `DRSY='55' AND DRRT='UP'` already reduces the
    UDC table to one small code list (hundreds of rows), so a semi-join on the
    batch's vendors would add a shuffle and save nothing."""
    f0005 = drop_deleted(spark.read.table(sname(F0005))).where(
        (F.trim(F.col("product_code")) == _ESO5_UDC_SYS)  # DRSY='55'
        & (F.trim(F.col("user_defined_codes")) == _ESO5_UDC_TYPE)  # DRRT='UP'
    )
    # u_vend is DOUBLE; the m2f join casts both sides to long for an exact
    # integer compare.  dropDuplicates keeps one row per vendor.
    return (
        f0005.select(
            F.trim(F.col("user_defined_code")).cast("double").alias("u_vend"),  # DRKY
            F.trim(F.col("special_handling_code")).alias("u_mcu"),  # DRSPHD
        )
        .where(F.col("u_vend").isNotNull())
        .dropDuplicates(["u_vend"])
    )


# ── the two row legs ────────────────────────────────────────────────────────
# Both legs are projected into ONE common intermediate schema so the whole
# downstream join chain is written once and applied to both.  Leg-B's PO-side OX
# values ride along as _pox_* and win in the final select.
_ESO5_LEG_COLS = [
    "l_kcoo", "l_doco", "l_dcto", "l_mcu", "l_an8", "l_shan", "l_cars", "l_vr01",
    "l_litm", "l_itm", "l_dsc1", "l_uom", "l_uom4", "l_uorg", "l_itwt", "l_prp1", "l_srp1", "l_lnty",
    "l_trdj", "l_vend", "l_uprc", "l_aexp", "l_dgl", "l_lttr", "l_nxtr", "l_doc",
    "l_lnid", "row_class", "po_holadd_superseded", "l_po_dcto",
    "_pox_lttr", "_pox_nxtr", "_pox_amt", "l_upd_ts",
    # v3 only — untouched Silver key values, carried solely to feed fact_key.
    "l_raw_kcoo", "l_raw_doco",
]


def _eso5_f4211_lines(f4211):
    """Leg A — the F4211 sales-order lines.  NO FILTER OF ANY KIND beyond the
    v3 load scope: every value-selecting predicate is carried as a COLUMN:
        SDDCTO='SX'      -> the `document_type` column
        SDKCOO='00750'   -> the `company` column
        SDLNTY<>'TL'     -> row_class 'TEXT'
        SDLITM<>'HOLADD' -> row_class 'HOLADD'"""
    sd = f4211
    row_class = (
        F.when(F.trim(F.col("line_type")) == "TL", F.lit("TEXT"))
        .when(F.trim(F.col("identifier_second_item")) == "HOLADD", F.lit("HOLADD"))
        .otherwise(F.lit("LINE"))
    )
    return sd.select(
        F.trim(F.col("company_key_order_no")).alias("l_kcoo"),
        F.col("document_order_invoice_e").alias("l_doco"),
        F.trim(F.col("order_type")).alias("l_dcto"),
        F.trim(F.col("cost_center")).alias("l_mcu"),
        F.col("address_number").alias("l_an8"),
        F.col("address_number_ship_to").alias("l_shan"),
        F.col("carrier").alias("l_cars"),
        F.col("reference_01").alias("l_vr01"),
        F.trim(F.col("identifier_second_item")).alias("l_litm"),
        F.col("identifier_short_item").alias("l_itm"),  # SDITM (F41002 join key)
        F.col("description_line_01").alias("l_dsc1"),
        F.col("uom_as_input").alias("l_uom"),
        F.col("uom_pricing").alias("l_uom4"),  # SDUOM4 (pricing UoM)
        F.col("units_transaction_qty").alias("l_uorg"),  # SDUORG
        F.col("amount_unit_weight").cast("double").alias("l_itwt"),  # SDITWT
        F.trim(F.col("purchasing_report_code_01")).alias("l_prp1"),  # SDPRP1
        F.trim(F.col("sales_reporting_code_01")).alias("l_srp1"),  # SDSRP1
        F.trim(F.col("line_type")).alias("l_lnty"),  # SDLNTY
        F.col("date_transaction_julian").alias("l_trdj"),
        F.col("primary_last_vendor_no").alias("l_vend"),
        F.col("amt_price_per_unit_02").alias("l_uprc"),
        F.col("amount_extended_price").alias("l_aexp"),
        F.col("dt_for_gl_and_vouch_01").alias("l_dgl"),
        F.trim(F.col("status_code_last")).alias("l_lttr"),
        F.trim(F.col("status_code_next")).alias("l_nxtr"),
        F.col("doc_voucher_invoice_e").alias("l_doc"),
        F.col("line_number").alias("l_lnid"),
        row_class.alias("row_class"),
        F.lit("N").alias("po_holadd_superseded"),  # only ever 'Y' on a PO row
        F.lit(None).cast("string").alias("l_po_dcto"),  # PDDCTO — PO rows only
        F.lit(None).cast("string").alias("_pox_lttr"),
        F.lit(None).cast("string").alias("_pox_nxtr"),
        F.lit(None).cast("double").alias("_pox_amt"),
        F.col("_raw_kcoo").alias("l_raw_kcoo"),  # v3 fact_key
        F.col("_raw_doco").alias("l_raw_doco"),  # v3 fact_key
        _jde_ts(F.col("date_updated"), F.col("time_of_day")).alias(
            "l_upd_ts"
        ),  # SDUPMJ + SDTDAY
    )


def _eso5_po_lines(f4311, la):
    """Leg B — the F4311 purchase-order lines.  With _ESO5_PO_LEG_UNFILTERED
    (the default) this leg takes the whole scoped table and `row_class`
    classifies each row, so the fact holds NO row-selecting predicate:
        row_class = 'PO_HOLADD'  ⇔  PDDCTO='OX' AND PDLITM='HOLADD'
                    'PO_OTHER'   ⇔  every other purchase-order line
    Sales-side attributes are back-filled from the load's FRT sales line, and
    UPRC/EXTAMT are forced to 0 — the money is on OXAMT.

    ⚠ The `NOT EXISTS(live SX HOLADD, SDLTTR<>'980')` is GONE — a status test
    that decided whether a row EXISTS.  Every HOLADD PO line is a row now, and
    the test is a FIELD: po_holadd_superseded = 'Y'.  The orphan set is exactly
    `po_holadd_superseded = 'N'`.

    ⚠ `document_type` is forced to 'SX' on every row here so the UNION lines up
    with the sales load.  The PO's OWN document type is kept in po_order_type
    (PDDCTO) — and this is why order_type is not a v3 key column."""
    po = (
        f4311
        if _ESO5_PO_LEG_UNFILTERED
        else f4311.where(
            (F.trim(F.col("order_type")) == "OX")
            & (F.trim(F.col("identifier_2nd_item")) == "HOLADD")
        )
    )

    # Back-fill + the ex-NOT-EXISTS flag come from the load's SX aggregates (the
    # FRT subqueries correlate on PDKCOO/PDDOCO with SDDCTO='SX' — hence the
    # literal 'SX' in the join key).
    #
    j = po.alias("pd").join(
        la,
        (F.trim(F.col("pd.company_key_order_no")) == F.col("_la_kcoo"))
        & (F.col("pd.document_order_invoice_e") == F.col("_la_doco"))
        & (F.col("_la_dcto") == F.lit("SX")),
        "left",
    )
    return j.select(
        F.trim(F.col("pd.company_key_order_no")).alias("l_kcoo"),
        F.col("pd.document_order_invoice_e").alias("l_doco"),
        F.lit("SX").alias("l_dcto"),  # the query hard-codes 'SX'
        F.trim(F.col("pd.cost_center")).alias("l_mcu"),  # PDMCU
        F.col("_fr_an8").alias("l_an8"),  # from the FRT line
        F.col("pd.address_number_ship_to").cast("double").alias("l_shan"),  # PDSHAN
        F.col("pd.address_number").alias("l_cars"),  # PDAN8 IS the carrier
        F.col("_fr_vr01").alias("l_vr01"),
        F.trim(F.col("pd.identifier_2nd_item")).alias("l_litm"),
        F.col("pd.identifier_short_item").alias("l_itm"),  # PDITM (F41002 join key)
        F.col("pd.description_line_01").alias("l_dsc1"),  # PDDSC1
        F.col("pd.uom_as_input").alias("l_uom"),  # PDUOM
        F.lit(None).cast("string").alias("l_uom4"),  # PO rows carry no pricing UoM
        F.col("pd.units_transaction_qty").cast("double").alias("l_uorg"),  # PDUORG
        F.lit(None).cast("double").alias("l_itwt"),
        F.lit(None).cast("string").alias("l_prp1"),
        F.lit(None).cast("string").alias("l_srp1"),
        F.lit(None).cast("string").alias("l_lnty"),
        F.col("pd.date_transaction_julian").cast("date").alias("l_trdj"),  # PDTRDJ
        F.col("_fr_vend").alias("l_vend"),  # LOFA from the FRT line
        F.lit(0.0).alias("l_uprc"),  # UPRC = 0
        F.lit(0.0).alias("l_aexp"),  # EXTAMT = 0
        F.col("_fr_dgl").alias("l_dgl"),
        F.col("_fr_lttr").alias("l_lttr"),
        F.col("_fr_nxtr").alias("l_nxtr"),
        F.col("_fr_doc").alias("l_doc"),
        F.col("pd.line_number").cast("double").alias("l_lnid"),  # PDLNID
        # `PDDCTO='OX' AND PDLITM='HOLADD'` — no longer a filter: it CLASSIFIES
        # the row into 'PO_HOLADD' vs 'PO_OTHER'.
        F.when(
            (F.trim(F.col("pd.order_type")) == "OX")
            & (F.trim(F.col("pd.identifier_2nd_item")) == "HOLADD"),
            F.lit("PO_HOLADD"),
        )
        .otherwise(F.lit("PO_OTHER"))
        .alias("row_class"),
        F.coalesce(F.col("_live_holadd"), F.lit("N")).alias("po_holadd_superseded"),
        F.trim(F.col("pd.order_type")).alias("l_po_dcto"),  # PDDCTO
        F.trim(F.col("pd.status_code_last")).alias("_pox_lttr"),  # OXLTTR
        F.trim(F.col("pd.status_code_next")).alias("_pox_nxtr"),  # OXNXTR
        F.col("pd.amount_extended_price").cast("double").alias("_pox_amt"),  # OXAMT
        # v3 fact_key — this PO row's OWN raw Silver values.  F4311 is a second
        # spine, so an F4311 change hashes exactly these and deletes exactly
        # these rows.  For a load that exists in both tables the F4211 stream
        # produces the identical hash, so either change rebuilds the whole load.
        # VERIFIED 2026-08-12: all 2,148,798 shared loads stringify identically
        # on both sides ("00750" / "1492199.000000000000000000").  If Silver ever
        # re-types DOCO on one table and not the other, that breaks and the PO
        # rows start duplicating — re-run that check before any Silver retype.
        F.col("pd._raw_kcoo").alias("l_raw_kcoo"),
        F.col("pd._raw_doco").alias("l_raw_doco"),
        # NULL until F4311's Silver update-date / time-of-day column names are
        # confirmed — same treatment leg B already gives l_itwt / l_prp1 / l_srp1
        # / l_lnty, which F4311 also does not carry.
        F.lit(None).cast("timestamp").alias("l_upd_ts"),
    )


def _eso5_load_aggregates(f4211):
    """EVERY per-load value computed with a correlated subquery, in ONE pass.

    ⚠ THE POINT OF THIS FUNCTION: not one of the source predicates
    (SDLITM='BOL' / 'SANDTKTNBR' / 'HOLADD' / 'FRT', SDPRP1='COM',
    SDLTTR<>'980', SDDCTO='SX') appears in a WHERE.  Every one is a CASE inside
    an aggregate, so it decides whether a line CONTRIBUTES TO A VALUE — never
    whether a row SURVIVES.

    ⚠ v3: `f4211` MUST be the COMPLETE rows of every affected load, never the
    CDF batch.  A partial group silently produces the wrong bol / sand_ticket /
    sbx_weight / FRT back-fill."""
    item = F.trim(F.col("identifier_second_item"))
    lttr = F.trim(F.col("status_code_last"))
    return (
        f4211.groupBy(
            F.trim(F.col("company_key_order_no")).alias("_la_kcoo"),
            F.col("document_order_invoice_e").alias("_la_doco"),
            F.trim(F.col("order_type")).alias("_la_dcto"),
        )
        .agg(
            # BOL / SANDTKT  (were `WHERE SDLITM = 'BOL' / 'SANDTKTNBR'`)
            F.max(F.when(item == "BOL", F.col("description_line_01"))).alias("bol"),
            F.max(F.when(item == "SANDTKTNBR", F.col("description_line_01"))).alias("sand_ticket"),
            # the load's SANDTKTNBR line (its vendor drives the 55/UP plant match)
            F.max(F.when(item == "SANDTKTNBR", F.col("primary_last_vendor_no"))).alias("_st_vend"),
            # SXWEIGHT  (was `WHERE SDDCTO='SX' AND SDPRP1='COM' AND SDLTTR<>'980'`)
            F.sum(
                F.when(
                    (F.trim(F.col("purchasing_report_code_01")) == "COM") & (lttr != "980"),
                    F.col("units_transaction_qty"),
                )
            ).alias("sbx_weight"),
            # `NOT EXISTS(live SX HOLADD, SDLTTR<>'980')` → a FIELD, not a filter
            F.max(F.when((item == "HOLADD") & (lttr != "980"), F.lit("Y"))).alias("_live_holadd"),
            # leg B back-fills its sales-side attributes from the load's FRT line
            F.max(F.when(item == "FRT", F.col("reference_01"))).alias("_fr_vr01"),
            F.max(F.when(item == "FRT", F.col("address_number"))).alias("_fr_an8"),
            F.max(F.when(item == "FRT", F.col("primary_last_vendor_no"))).alias("_fr_vend"),
            F.max(F.when(item == "FRT", F.col("dt_for_gl_and_vouch_01"))).alias("_fr_dgl"),
            F.max(F.when(item == "FRT", lttr)).alias("_fr_lttr"),
            F.max(F.when(item == "FRT", F.trim(F.col("status_code_next")))).alias("_fr_nxtr"),
            F.max(F.when(item == "FRT", F.col("doc_voucher_invoice_e"))).alias("_fr_doc"),
            # per-load SDLTTR CASE + its two inputs (all three stored on the fact)
            F.max(lttr).alias("load_max_last_status"),  # MXLTTR
            F.min(lttr).alias("load_min_last_status"),  # MILTTR
            F.max(
                F.when(
                    (item != "HOLADD") & (F.col("amount_extended_price") != 0), lttr
                )
            ).alias("_alt"),
        )
        .withColumn(
            "load_last_status",
            F.when(
                (F.col("load_max_last_status") == "980")
                & (F.col("load_min_last_status") == "980"),
                F.col("load_max_last_status"),
            ).otherwise(F.col("_alt")),
        )
        .drop("_alt")
    )


def build_fact_extended_sales_order_5(spine_df):
    # ── v3 SCOPE: the loads this batch touched ──────────────────────────────
    # F4211 is the only spine, so spine_df is always F4211 rows.  Both legs are
    # keyed off it — see the ⚠ note on the block header.
    _LK = FACT_ESO5_SILVER_LINE_KEYS

    # generic_recompute narrows an incremental batch to spine_columns (exactly
    # the two load keys); the FULL LOAD path hands over the whole spine table
    # untouched.  On a full load every load is in scope, so the semi-joins below
    # would be no-ops that still cost three shuffles over 12.6M rows — skip them.
    _full_load = len(spine_df.columns) > len(_LK)
    loads = None if _full_load else spine_df.select(*_LK).distinct()

    def _scope(df):
        return df if loads is None else df.join(loads, _LK, "left_semi")

    # Rule 4c — re-read the COMPLETE rows of every affected load.  spine_df holds
    # only the CHANGED rows, and _eso5_load_aggregates must see whole loads.
    f4211 = _scope(drop_deleted(spark.read.table(sname(F4211))))

    # F4311 is the second SPINE, so the engine does not narrow it — scope it
    # here.  TWO scopes are needed and they are NOT interchangeable:
    #   leg B -> (kcoo, doco).  Must match the DELETE scope exactly.  Scoping it
    #            on doco alone would drag in PO rows from other companies that
    #            this batch never deleted, and they would append every run.
    #   ox    -> doco ONLY.  The `ox` aggregate groups by (item, doco) and the
    #            join that consumes it matches on doco alone — it reaches across
    #            companies on purpose, because _is_ox already pins kcoo='00750'.
    #            Narrowing by kcoo would silently blank ox_amount /
    #            ox_last_status / ox_next_status for a load carried elsewhere.
    _f4311_all = get_ref(F4311)  # pinned to the snapshot version on a full load
    f4311 = _scope(_f4311_all)
    _f4311_ox = (
        _f4311_all
        if loads is None
        else _f4311_all.join(
            loads.select("document_order_invoice_e").distinct(),
            ["document_order_invoice_e"],
            "left_semi",
        )
    )

    # Passive refs — the engine narrows these on the two load keys.
    qc = get_ref(F554201T)
    f43121 = get_ref(F43121)
    f4201 = get_ref(F4201)
    # No load key on these two, so the engine cannot narrow them — read direct
    # and narrow each one at source instead (see the WHERE pushdowns below).
    f0911 = drop_deleted(spark.read.table(sname(F0911)))
    ab = drop_deleted(spark.read.table(sname(F0101)))

    # ── keep the RAW key values before any cast (see the ⚠ note above) ───────
    # BOTH spines carry their own: each leg's fact_key comes from the table that
    # produced the row, and the two agree for every shared load (verified).
    f4211 = f4211.withColumn("_raw_kcoo", F.col("company_key_order_no")).withColumn(
        "_raw_doco", F.col("document_order_invoice_e")
    )
    f4311 = f4311.withColumn("_raw_kcoo", F.col("company_key_order_no")).withColumn(
        "_raw_doco", F.col("document_order_invoice_e")
    )

    # ── normalize the order/load key type ACROSS tables ──────────────────────
    # SDDOCO / PDDOCO / PRDOCO are the SAME JDE data item (DOCO), but Silver can
    # land them with DIFFERENT physical types per table (e.g. F4311 as string,
    # F4211/F43121 as decimal).  A cross-table equality JOIN on
    # document_order_invoice_e then SILENTLY MISSES — invisible for leg A, but on
    # the F4311 rows it wipes EVERY sales-side back-fill.  Cast the key to ONE
    # canonical type wherever it is carried so all doco joins are long==long.
    # DOCO is an integer order number (0 implied decimals), so this is lossless.
    _DOCO = "document_order_invoice_e"
    f4211 = f4211.withColumn(_DOCO, F.col(_DOCO).cast("long"))
    f4311 = f4311.withColumn(_DOCO, F.col(_DOCO).cast("long"))
    _f4311_ox = _f4311_ox.withColumn(_DOCO, F.col(_DOCO).cast("long"))
    f43121 = f43121.withColumn(_DOCO, F.col(_DOCO).cast("long"))
    qc = qc.withColumn(_DOCO, F.col(_DOCO).cast("long"))
    f4201 = f4201.withColumn(_DOCO, F.col(_DOCO).cast("long"))

    # ── ALL the per-load F4211 subqueries, in one filter-free pass ───────────
    la = _eso5_load_aggregates(f4211)

    # ── the row population: the scoped F4211 lines UNION the scoped F4311 leg ─
    base = (
        _eso5_f4211_lines(f4211)
        .select(*_ESO5_LEG_COLS)
        .unionByName(_eso5_po_lines(f4311, la).select(*_ESO5_LEG_COLS))
    )

    # ── F554201T — QCDS50 (Sand PO Number) + QCLGL1/2/3 + QCFSTR3, per
    #    (kcoo, doco, dcto).  The legs are Silver `descriptn_01/02/03` and
    #    QCFSTR3 is `future_use_string_03` — the JDE→snake_case map is NOT
    #    literal, so do not guess the names from the alias. ──
    def _qcf(src, alias):
        return F.first(F.col(src), ignorenulls=True).alias(alias)

    qcv = qc.groupBy(
        F.trim(F.col("company_key_order_no")).alias("qc_kcoo"),
        F.col("document_order_invoice_e").alias("qc_doco"),
        F.trim(F.col("order_type")).alias("qc_dcto"),
    ).agg(
        F.first(F.trim(F.col("description_50_characters")), ignorenulls=True).alias("sand_po_number"),
        _qcf("descriptn_01", "leg_1"),  # QCLGL1
        _qcf("descriptn_02", "leg_2"),  # QCLGL2
        _qcf("descriptn_03", "leg_3"),  # QCLGL3
        _qcf("future_use_string_03", "qc_string_3"),  # QCFSTR3
    )

    # ── OX status + OX amount from F4311, by (item, load).  `PDDCTO='OX'` and
    #    `PDKCOO='00750'` were WHERE constants; they are CASE conditions now. ──
    _is_ox = (F.trim(F.col("order_type")) == "OX") & (
        F.trim(F.col("company_key_order_no")) == "00750"
    )
    ox = _f4311_ox.groupBy(
        F.trim(F.col("identifier_2nd_item")).alias("_ox_item"),
        F.col("document_order_invoice_e").alias("_ox_doco"),
    ).agg(
        F.max(F.when(_is_ox, F.col("status_code_next"))).alias("_ox_nxtr"),
        F.max(F.when(_is_ox, F.col("status_code_last"))).alias("_ox_lttr"),
        F.sum(F.when(_is_ox, F.col("amount_extended_price"))).alias("_ox_amt"),
    )

    # ── PO Receipt GL Date — F43121, by the line keys.  `PRMATC='1'` and
    #    `PRDGL>1` stay CASE conditions: the value is the MAX over the matching
    #    receipts, no row is dropped.
    #    v3: `PRDCT='OV'` IS pushed into a WHERE.  Both consumers of f43121
    #    (recv and recv_docs) require OV, and a group that loses every row
    #    yields NULL through the LEFT join below — exactly what the all-NULL
    #    CASE produced.  Same answer, but Delta can skip files and the join
    #    input shrinks by orders of magnitude. ──
    f43121 = f43121.where(F.trim(F.col("document_type")) == "OV")
    _rc_ok = (F.trim(F.col("match_type")) == "1") & (
        F.col("dt_for_gl_and_vouch_01").isNotNull()
    )
    recv = f43121.groupBy(
        F.col("address_number").alias("_rc_pran8"),
        F.trim(F.col("company_key_order_no")).alias("_rc_kcoo"),
        F.col("document_order_invoice_e").alias("_rc_doco"),
        F.trim(F.col("identifier_2nd_item")).alias("_rc_item"),
    ).agg(
        F.max(F.when(_rc_ok, F.col("dt_for_gl_and_vouch_01"))).alias("po_receipt_gl_date")
    )

    # ── Carrier PO GL Post flag — F0911 doc ∈ F43121 PRDOC, linked by the line
    #    keys.  v3: `GLDCT='OV'` and `GLKCO='00750'` are pushed into a WHERE on
    #    F0911 for the same reason as above — F0911 is the General Ledger and is
    #    the largest table this fact touches, so narrowing it before the join is
    #    the single biggest saving in the build. ──
    recv_docs = f43121.select(
        F.col("doc_voucher_invoice_e").alias("_gd_prdoc"),
        F.col("address_number").alias("_gd_pran8"),
        F.trim(F.col("company_key_order_no")).alias("_gd_kcoo"),
        F.col("document_order_invoice_e").alias("_gd_doco"),
        F.trim(F.col("identifier_2nd_item")).alias("_gd_item"),
    )
    glpost_docs = f0911.where(
        (F.trim(F.col("document_type")) == "OV") & (F.trim(F.col("company_key")) == "00750")
    ).select(
        F.col("doc_voucher_invoice_e").alias("_gl_doc"),
        F.col("gl_posted_code").alias("_gl_post"),
    )
    glpost = (
        recv_docs.join(glpost_docs, F.col("_gd_prdoc") == F.col("_gl_doc"), "inner")
        .groupBy("_gd_pran8", "_gd_kcoo", "_gd_doco", "_gd_item")
        .agg(F.max(F.col("_gl_post")).alias("carrier_po_gl_post_flag"))
    )

    # ── the load's SANDTKTNBR line (from `la`) INNER F554201T on
    #    (kcoo, doco, dcto).  The item condition is already the CASE inside `la`,
    #    and the order type + company are the join keys. ──
    plant = _eso5_plant_mcu()  # F0005 55/UP DRSPHD (vendor → plant MCU)
    m2f = (
        la.alias("la")
        .join(
            qcv.alias("qc"),
            (F.col("la._la_kcoo") == F.col("qc.qc_kcoo"))
            & (F.col("la._la_doco") == F.col("qc.qc_doco"))
            & (F.col("la._la_dcto") == F.col("qc.qc_dcto")),
            "inner",
        )
        .join(plant, F.col("la._st_vend").cast("long") == F.col("u_vend").cast("long"), "left")
        .select(
            F.col("la._la_kcoo").alias("us_kcoo"),
            F.col("la._la_doco").alias("us_doco"),
            F.col("la._la_dcto").alias("us_dcto"),
            F.col("la.sand_ticket").alias("us_sddsc1"),  # M.SDDSC1
            F.col("qc.sand_po_number").alias("us_qcds50"),  # F554201T.QCDS50
            F.col("u_mcu").alias("lofa_mcu"),  # SO-match input only
        )
    )

    # SO orders — matched to the sand load by padded pull_signal + reference_01.
    #
    # ⚠ v3: this is the ONE population that CANNOT be narrowed by load key — the
    # SO leg lives on rows an SX scope removes, and it is matched by padded
    # STRING, not by key.  `L.SDDCTO='SO' AND L.SDCO='00400'` are therefore
    # pushed into a WHERE here: they are already part of the join condition, so a
    # row failing them can never match and dropping it early cannot change the
    # result.  Without this the fact scans the whole of F4211 on every batch.
    so = (
        drop_deleted(spark.read.table(sname(F4211)))
        .where((F.trim(F.col("order_type")) == "SO") & (F.trim(F.col("company")) == "00400"))
        .select(
            F.col("pull_signal").alias("so_psig"),
            F.col("reference_01").alias("so_vr01"),
            F.col("document_order_invoice_e").cast("long").alias("so_doco"),
            F.col("cost_center").alias("so_mcu"),
            F.trim(F.col("line_type")).alias("so_lnty"),
            F.col("units_secondary_qty_or").alias("so_sqor"),
        )
    )
    matched = m2f.alias("u").join(
        so.alias("s"),
        (F.col("s.so_psig") == _eso5_pad30(F.col("u.us_sddsc1")))
        & (F.col("s.so_vr01") == _eso5_pad25(F.col("u.us_qcds50"))),
        "left",
    )
    sbxusssand = matched.groupBy("us_kcoo", "us_doco", "us_dcto").agg(
        F.max(F.when(F.col("s.so_doco").isNotNull(), F.lit("Y"))).alias("uss_match"),
        F.first(F.col("s.so_vr01"), ignorenulls=True).alias("uss_customer_po"),  # SOPONO
        F.first(F.col("s.so_psig"), ignorenulls=True).alias("so_alt_bol_no"),  # SOALTBOLNO
        # SOORDERNO — SO doco whose district matches the vendor's plant MCU
        F.first(
            F.when(F.trim(F.col("s.so_mcu")) == F.col("lofa_mcu"), F.col("s.so_doco")),
            ignorenulls=True,
        ).alias("uss_so_order_no"),
        # SOWEIGHT — sum secondary qty over matched SO 'S' lines
        F.sum(F.when(F.col("s.so_lnty") == "S", F.col("s.so_sqor"))).alias("uss_so_weight"),
    )

    # F0101 loading-facility lookup (LOFA = ABAN8): read here ONLY for ABURAT.
    #
    # The `ABAT1 BETWEEN 'A '..'P ' OR 'R '..'ZZZ'` band says WHICH address-book
    # rows COUNT AS a rate source.  That predicate is NOT deleted and is NOT a
    # WHERE: it is a CASE INSIDE THE AGGREGATE.  Deleting it would not make the
    # rate "unfiltered", it would make it WRONG — a 'Q'-band facility would start
    # reporting a rate that should be blank.  As a CASE it drops nothing.
    _at = F.rpad(F.rtrim(F.col("address_type_01")), 3, " ")
    _is_rate_source = ((_at >= F.lit("A  ")) & (_at <= F.lit("P  "))) | (
        (_at >= F.lit("R  ")) & (_at <= F.lit("ZZZ"))
    )
    lofa = ab.groupBy(F.col("address_number").alias("ab_an8")).agg(
        F.first(F.when(_is_rate_source, F.col("user_reserved_amount")), ignorenulls=True).alias("_aburat")
    )

    # F4201 sales-order HEADER — SHMCU/SHAN8/SHSHAN/SHCARS/SHVR01/SHTRDJ are
    # header values and need not equal the line's own.
    hdr = f4201.select(
        F.trim(F.col("company_key_order_no")).alias("h_kcoo"),  # SHKCOO
        F.col("document_order_invoice_e").alias("h_doco"),  # SHDOCO
        F.trim(F.col("order_type")).alias("h_dcto"),  # SHDCTO
        F.trim(F.col("cost_center")).alias("header_district"),  # SHMCU
        F.col("address_number").cast("double").alias("header_sold_to"),  # SHAN8
        F.col("address_number_ship_to").cast("double").alias("header_ship_to"),  # SHSHAN
        F.col("carrier").cast("double").alias("header_carrier"),  # SHCARS
        F.col("reference_01").alias("header_customer_po"),  # SHVR01
        F.col("date_transaction_julian").cast("date").alias("header_order_date"),  # SHTRDJ
    ).dropDuplicates(["h_kcoo", "h_doco", "h_dcto"])

    # ── derivations on the common line ──────────────────────────────────────
    # QTY = DECODE(SDPRP1,'COM',(SDUORG/1000)/2000, SDUORG/1000) — Silver is
    # decoded, so only the COM→tons factor survives.  PO rows have no SDPRP1, so
    # they take the plain-units branch.
    qty = F.when(
        F.col("l_prp1") == "COM", F.col("l_uorg") / F.lit(_ESO5_TONS_DIVISOR)
    ).otherwise(F.col("l_uorg"))
    gl_date = F.coalesce(F.col("l_dgl"), F.to_date(F.lit("1900-01-01")))
    # OXAMT — charged to the FRT line, OR to a live HOLADD line (SDLTTR<>'980'),
    # OR a PO_HOLADD row carries its own PDAEXP.  This is a CASE (a calculation),
    # not a filter.  But it BAKES IN a status rule, so `ox_amount_gross` exposes
    # the same money with NO item/status condition.
    _is_po_row = F.col("row_class").isin("PO_HOLADD", "PO_OTHER")
    ox_amount = (
        F.when(_is_po_row, F.coalesce(F.col("_pox_amt"), F.lit(0.0)))
        .when(F.col("l_litm") == "FRT", F.coalesce(F.col("_ox_amt"), F.lit(0.0)))
        .when(
            (F.col("l_litm") == "HOLADD") & (F.col("l_lttr") != "980"),
            F.coalesce(F.col("_ox_amt"), F.lit(0.0)),
        )
        .otherwise(F.lit(0.0))
    )
    ox_amount_gross = F.coalesce(F.col("_pox_amt"), F.col("_ox_amt"), F.lit(0.0))

    uom_item, uom_std = _uom_cascades()  # SDUOM/SDUOM4 -> TN item (F41002) + standard (F41003) cascades

    # ── assemble: base LEFT (per-load aggregates, OX, F43121, F0911, F554201T,
    #    SO-match, F0101, F4201) ──
    j = (
        base.alias("sd")
        .join(
            la,
            (F.col("sd.l_kcoo") == F.col("_la_kcoo"))
            & (F.col("sd.l_doco") == F.col("_la_doco"))
            & (F.col("sd.l_dcto") == F.col("_la_dcto")),
            "left",
        )
        .join(
            ox,
            (F.col("sd.l_litm") == F.col("_ox_item"))
            & (F.col("sd.l_doco") == F.col("_ox_doco")),
            "left",
        )
        .join(
            recv,
            (F.col("sd.l_cars") == F.col("_rc_pran8"))
            & (F.col("sd.l_kcoo") == F.col("_rc_kcoo"))
            & (F.col("sd.l_doco") == F.col("_rc_doco"))
            & (F.col("sd.l_litm") == F.col("_rc_item")),
            "left",
        )
        .join(
            glpost,
            (F.col("sd.l_cars") == F.col("_gd_pran8"))
            & (F.col("sd.l_kcoo") == F.col("_gd_kcoo"))
            & (F.col("sd.l_doco") == F.col("_gd_doco"))
            & (F.col("sd.l_litm") == F.col("_gd_item")),
            "left",
        )
        .join(
            qcv,
            (F.col("sd.l_kcoo") == F.col("qc_kcoo"))
            & (F.col("sd.l_doco") == F.col("qc_doco"))
            & (F.col("sd.l_dcto") == F.col("qc_dcto")),
            "left",
        )
        .join(
            sbxusssand,
            (F.col("sd.l_kcoo") == F.col("us_kcoo"))
            & (F.col("sd.l_doco") == F.col("us_doco"))
            & (F.col("sd.l_dcto") == F.col("us_dcto")),
            "left",
        )
        .join(lofa, F.col("sd.l_vend") == F.col("ab_an8"), "left")  # LOFA=ABAN8
        .join(
            hdr,
            (F.col("sd.l_kcoo") == F.col("h_kcoo"))
            & (F.col("sd.l_doco") == F.col("h_doco"))
            & (F.col("sd.l_dcto") == F.col("h_dcto")),
            "left",
        )
        # UOM->TN cascades: volume on SDUOM, price on SDUOM4 (item F41002 + standard F41003; deduped)
        .join(uom_item.alias("civ"), (F.col("civ.itm") == F.col("sd.l_itm"))
                                     & (F.col("civ.from_uom") == F.trim(F.col("sd.l_uom"))), "left")
        .join(uom_std.alias("csv"), (F.col("csv.from_uom") == F.trim(F.col("sd.l_uom"))), "left")
        .join(uom_item.alias("cip"), (F.col("cip.itm") == F.col("sd.l_itm"))
                                     & (F.col("cip.from_uom") == F.trim(F.col("sd.l_uom4"))), "left")
        .join(uom_std.alias("csp"), (F.col("csp.from_uom") == F.trim(F.col("sd.l_uom4"))), "left")
    )

    # ── standard UoM conversion (volume on SDUOM, price on SDUOM4) ──
    vol_factor_raw = F.coalesce(F.when(F.trim(F.col("sd.l_uom")) == "TN", F.lit(1.0)),
                                F.col("civ.conv_factor"), F.col("csv.conv_std"))
    vol_factor = F.coalesce(vol_factor_raw, F.lit(1.0))
    qty_tons = F.col("sd.l_uorg").cast("double") * vol_factor
    price_factor_raw = F.coalesce(F.when(F.trim(F.col("sd.l_uom4")) == "TN", F.lit(1.0)),
                                  F.col("cip.conv_factor"), F.col("csp.conv_std"))
    price_factor = F.coalesce(price_factor_raw, F.lit(1.0))
    _pf_nz = F.when(price_factor != 0, price_factor)
    _delivered = (F.col("sd.l_lttr").cast("int") != F.lit(980)) & (F.col("sd.l_nxtr").cast("int") == F.lit(999))
    converted_ppt = F.when(_is_po_row, F.lit(None).cast("double")) \
                     .when(_delivered, F.col("sd.l_uprc").cast("double") / _pf_nz)
    price_factor_out = F.when(_is_po_row, F.lit(None).cast("double")).otherwise(price_factor)
    price_flag_out = F.when(_is_po_row, F.lit(None).cast("string")) \
                      .otherwise(F.when(price_factor_raw.isNull(), F.lit("Y")).otherwise(F.lit("N")))

    sel = j.select(
        # ── v3 key columns — raw Silver values, for fact_key only ──
        F.col("sd.l_raw_kcoo").alias("load_company_raw"),
        F.col("sd.l_raw_doco").alias("load_number_raw"),
        # ── degenerate identifiers ──
        F.col("sd.l_doco").cast("long").alias("load_number"),  # SDDOCO (int64)
        F.col("sd.l_dcto").alias("document_type"),  # SDDCTO
        F.col("sd.l_kcoo").alias("company"),  # SDKCOO
        F.col("sd.l_mcu").alias("district"),  # SDMCU
        # ── address FK codes ──
        F.col("sd.l_an8").alias("sold_to"),  # SDAN8
        F.col("sd.l_shan").alias("ship_to"),  # SDSHAN
        F.col("sd.l_cars").alias("carrier"),  # SDCARS
        F.col("sd.l_vend").alias("loading_facility"),  # LOFA=SDVEND
        # ── degenerate attributes ──
        F.col("sd.l_vr01").alias("customer_po"),  # SDVR01
        F.col("sand_po_number"),  # F554201T QCDS50
        F.col("uss_customer_po"),  # SOPONO
        F.col("sd.l_litm").alias("item_number"),  # SDLITM
        F.col("sd.l_dsc1").alias("item_description"),  # SDDSC1
        F.col("sd.l_trdj").alias("order_date"),  # SDTRDJ
        gl_date.alias("gl_date"),  # SDDGL (null→1900-01-01)
        F.col("uss_match"),
        F.col("uss_so_order_no"),
        F.col("uss_so_weight"),
        F.col("sbx_weight"),
        F.col("so_alt_bol_no"),
        F.col("sand_ticket"),
        F.col("bol"),
        F.col("sd.l_uom").alias("uom"),  # SDUOM
        qty.alias("quantity"),  # QTY (derived)
        F.col("sd.l_uprc").alias("unit_price"),  # SDUPRC (0 on PO rows)
        F.col("sd.l_aexp").alias("total_amount"),  # SDAEXP (0 on PO rows)
        F.col("sd.l_lttr").alias("last_status"),  # SDLTTR
        F.col("sd.l_nxtr").alias("next_status"),  # SDNXTR
        F.col("sd.l_nxtr").cast("int").alias("next_status_num"),  # SDNXTR as int
        F.col("sd.l_doc").cast("long").alias("invoice_number"),  # SDDOC (int64)
        # OX status — an orphan PO row reports ITS OWN PDLTTR/PDNXTR
        F.coalesce(F.col("sd._pox_lttr"), F.col("_ox_lttr")).alias("ox_last_status"),
        F.coalesce(F.col("sd._pox_nxtr"), F.col("_ox_nxtr")).alias("ox_next_status"),
        ox_amount.alias("ox_amount"),  # OXAMT (three-way rule above)
        ox_amount_gross.alias("ox_amount_gross"),  # same money, no conditions
        F.col("carrier_po_gl_post_flag"),  # F0911 (raw code, kept for filtering)
        # decoded G/L Posted Code as a single "code - description" display value:
        # P/D -> "P - Posted", blank (voucher exists, unposted) -> "Unposted",
        # NULL (no matching OV voucher) -> blank, any other code -> the code.
        F.when(F.col("carrier_po_gl_post_flag").isNull(), F.lit(None).cast("string"))
        .when(
            F.trim(F.col("carrier_po_gl_post_flag")).isin("P", "D"),
            F.concat(F.trim(F.col("carrier_po_gl_post_flag")), F.lit(" - Posted")),
        )
        .when(F.trim(F.col("carrier_po_gl_post_flag")) == "", F.lit("Unposted"))
        .otherwise(F.trim(F.col("carrier_po_gl_post_flag")))
        .alias("carrier_po_gl_post_flag_desc"),
        F.col("po_receipt_gl_date"),  # F43121
        # SDLNID — display the RAW JDE line number: 1.00 -> 1000.  Silver decoded
        # the 3 implied decimals; this puts them back.  It is LOSSLESS *because*
        # of the decimals: a fractional kit line 1.010 becomes 1010, so nothing
        # is truncated.  round() first — 1.01 * 1000 is 1009.9999999999999 in
        # binary floating point and a bare cast would floor it to 1009.
        F.round(F.col("sd.l_lnid") * F.lit(1000), 0).cast("long").alias("line_id"),
        # ── variation columns ──
        F.col("sd.row_class"),
        F.col("sd.l_lnty").alias("line_type"),  # SDLNTY
        F.col("sd.l_prp1").alias("product_category"),  # SDPRP1
        F.col("sd.l_srp1").alias("sales_report_code_01"),  # SDSRP1
        F.col("sd.l_uorg").alias("units_ordered"),  # SDUORG (raw decoded)
        F.col("sd.l_itwt").alias("item_weight"),  # SDITWT
        F.col("sd.po_holadd_superseded"),  # ex-NOT EXISTS (status → field)
        F.col("sd.l_po_dcto").alias("po_order_type"),  # PDDCTO (PO rows only)
        F.col("load_last_status"),  # per-load SDLTTR CASE
        F.col("load_max_last_status"),
        F.col("load_min_last_status"),  # MXLTTR / MILTTR
        F.col("leg_1"),
        F.col("leg_2"),
        F.col("leg_3"),
        F.col("qc_string_3"),
        F.col("header_district"),
        F.col("header_sold_to"),
        F.col("header_ship_to"),
        F.col("header_carrier"),
        F.col("header_customer_po"),
        F.col("header_order_date"),
        # ── standard UoM conversion, added 2026-08-24 ──
        vol_factor.alias("conversion_to_tons_rate"),
        qty_tons.alias("quantity_shipped_tons"),
        F.when(vol_factor_raw.isNull(), F.lit("Y")).otherwise(F.lit("N")).alias("missing_conversion_flag"),
        F.col("sd.l_uom4").alias("uom_pricing"),
        price_factor_out.alias("price_conversion_factor"),
        converted_ppt.alias("converted_price_per_ton"),
        price_flag_out.alias("price_missing_conversion_flag"),
        # ── measure ──
        (F.col("_aburat") * F.lit(_ESO5_RATE_FACTOR)).alias("lofa_rate"),  # ABURAT
        # ── audit ──
        F.col("sd.l_upd_ts").alias("jde_updated_ts"),
    ).distinct()

    # ── GROUP BY (outer): one row per display tuple; SUM the single measure ──
    # v3: the raw key columns are functionally determined by the group (company
    # is trim(raw_kcoo), load_number is long(raw_doco)), so they ride in the
    # aggregate as max() rather than widening the shuffle key.
    # The batch build's trailing dropDuplicates(FACT_GROUP_BY_COLS) is dropped —
    # .distinct() has already made the tuple unique and the groupBy cannot
    # re-introduce a duplicate, so it was a third full shuffle for nothing.
    agg = sel.groupBy(*_ESO5_GROUP_BY_COLS).agg(
        F.sum("lofa_rate").alias("lofa_rate"),
        F.max("load_company_raw").alias("load_company_raw"),
        F.max("load_number_raw").alias("load_number_raw"),
        # rides in the aggregate for the same reason the raw keys do — keeping it
        # out of _ESO5_GROUP_BY_COLS leaves the grain and load_line_key unchanged.
        F.max("jde_updated_ts").alias("jde_updated_ts"),
    )

    df = agg.withColumn(
        "load_scope_key",
        _eso5_load_scope_expr("company", "document_type", "load_number"),
    ).withColumn("load_line_key", sk(*_ESO5_GROUP_BY_COLS))
    return df.select(
        "load_line_key",
        "load_scope_key",
        *FACT_ESO5_GOLD_LINE_KEYS,
        *_ESO5_BUSINESS_COLS,
        "jde_updated_ts",
    )


# ════════════════════════════════════════════════════════════════════════════
# [DIM]  dim_order_number  ·  spine: F4211  extra spine: F42119
# Grain  : one row per order number (SDDOCO), across open orders and history
# Gold PK: order_number
#
# Was its own notebook (nb_silver_to_gold_dim_order_number.py), which rebuilt
# the dim with a full DISTINCT over F4211 ∪ F42119 on every run.  Folded in here
# because this notebook already streams both tables for the facts: the changed
# order numbers arrive for free, so the dim costs a filtered read instead of a
# full scan of the two largest tables in the system.
#
# MAK_EXPORT_ORDERS is a hand-maintained list, and editing it does NOT change the
# value in the dim table , the new number keeps its old flag until F4211 (or F42119) 
# happens to change for that specific order
# force a full load:
#     mssparkutils.fs.rm(CKPT_ROOT + "/dim_order_number", True)
# ════════════════════════════════════════════════════════════════════════════
DIM_ORDER_NUMBER_GOLD_LINE_KEYS = ["order_number"]
DIM_ORDER_NUMBER_SILVER_LINE_KEYS = ["document_order_invoice_e"]
_ORDER_NUMBER_SPINE_COLS = ["document_order_invoice_e"]

# MAK_EXPORT_ORDERS — the hand-picked SDDOCO whitelist for Ottowa/Mak Export Orders (70 export orders).
# ⚠ Snapshot: refresh this list + re-run when the report's order set changes.
MAK_EXPORT_ORDERS = [
    1593550, 1593549, 1581179, 1581165, 1581173, 1581184, 1581161, 1596420,
    1594410, 1596628, 1593269, 1593272, 1595918, 1557959, 1557961, 1577127,
    1593196, 1593200, 1595505, 1593731, 1594581, 1593732, 1594622, 1593733,
    1595678, 1593736, 1593734, 1593735, 1570618, 1571523, 1571525, 1570619,
    1571520, 1570615, 1571522, 1570617, 1590914, 1583718, 1595704, 1595696,
    1594415, 1594414, 1595402, 1596566, 1590559, 1590562, 1590564, 1590569,
    1596416, 1596417, 1596418, 1594405, 1594406, 1595342, 1596583, 1593829,
    1595675, 1597223, 1596647, 1593945, 1595602, 1595603, 1595604, 1596951,
    1561922, 1561921, 1592418, 1582249, 1587441, 1595294,
]

# One flag COLUMN per whitelist-driven report → (mode, order_list). mode: 'include' = 'Included' when the
# order IS in the list (that report's SDDOCO IN (...) rule). Column name encodes the report so authors
# pick the right one. Add a new entry here for each future order-whitelist report.
ORDER_FILTERS = {
    "mak_export_filter": ("include", MAK_EXPORT_ORDERS),   # Mak Export Orders (SDDOCO IN the 70 export orders)
}


def build_dim_order_number(spine_df):
    orders = drop_deleted(spine_df).select(
        F.col("document_order_invoice_e").alias("order_number")
    )

    # FULL LOAD hands over the whole F4211 table; the incremental path hands over
    # only _ORDER_NUMBER_SPINE_COLS.  Same column-count test build_fact_eso5 uses.
    #
    # The history table is added HERE only on a full load.  Incrementally it must
    # not be: generic_recompute already unions F42119 into the re-read (see the
    # rebuild_from_silver branch there), and pulling in all of F42119 on every
    # batch would append history orders that carry no matching fact_key in
    # affected_keys — so the next batch's DELETE would not remove them and the
    # dim would grow duplicates.
    if len(spine_df.columns) > len(_ORDER_NUMBER_SPINE_COLS):
        orders = orders.unionByName(
            get_ref(F42119).select(
                F.col("document_order_invoice_e").alias("order_number")
            )
        )

    df = (
        orders.where(F.col("order_number").isNotNull())
        .dropDuplicates(["order_number"])
    )

    # One 'Included'/'Excluded' column per whitelist report, unchanged from the
    # original notebook.
    for _col, (_mode, _orders) in ORDER_FILTERS.items():
        _in_list = F.col("order_number").isin(_orders)
        # include: 'Included' when in list; exclude: 'Included' when NOT in list
        _flag = (F.when(_in_list, F.lit("Included")).otherwise(F.lit("Excluded")) if _mode == "include"
                 else F.when(_in_list, F.lit("Excluded")).otherwise(F.lit("Included")))
        df = df.withColumn(_col, _flag)
    return df


# ════════════════════════════════════════════════════════════════════════════
# [DIM]  dim_second_item  ·  spine: F4211  extra spine: F42119
# Grain  : one row per second item number (SDLITM), across open orders + history
# Gold PK: second_item_number
#
# Was its own notebook (nb_silver_to_gold_dim_second_item.py).  That one was
# GATED: it rebuilt the whole dim only when a batch carried an item code the dim
# did not already have, because a full rebuild on every F4211 change would have
# been pure waste.  Folded in here that gate is not needed and not kept — the
# engine already does the equivalent, and better: it recomputes only the item
# codes the batch actually touched, so any change flows through immediately
# instead of waiting for a brand-new code to appear.
#
# ⚠ second_item_number IS NOT TRIMMED, and that is a DELIBERATE change from the
#   standalone notebook, which wrote TRIM(SDLITM).  Two reasons, both hard:
#
#   1. It is the line key.  generic_recompute hashes fact_key from this Gold
#      column while the handler hashes it from Silver's identifier_second_item.
#      Trimming here would change the string on ONE side only, so the DELETE
#      would match nothing and every batch would APPEND another copy of the row
#      — the dim would grow duplicates forever, silently.  (Rule 1.)
#   2. Every fact in this notebook writes SDLITM raw — ESO1 (:1277), CSO2
#      (:1580), commission (:2561), ESO6 (:4104).  The PBI relationship
#      fact[second_item_number] -> dim[second_item_number] only matches if both
#      sides carry the same string, so raw is the side that agrees with the facts.
#
#   The variation flags are still computed on the TRIMMED value, exactly as the
#   original did, so the item lists below need no padding.
#
# ⚠ GROUND_ITEMS / ASTM_ITEMS are hand-maintained lists.  Editing one does NOT
#   change F4211, so nothing triggers a recompute and every existing row keeps
#   its old flag until that item's lines happen to change.  Same caveat as
#   MAK_EXPORT_ORDERS above.  After editing either list, force a full load:
#     mssparkutils.fs.rm(CKPT_ROOT + "/dim_second_item", True)
# ════════════════════════════════════════════════════════════════════════════
DIM_SECOND_ITEM_GOLD_LINE_KEYS = ["second_item_number"]
DIM_SECOND_ITEM_SILVER_LINE_KEYS = ["identifier_second_item"]
_SECOND_ITEM_SPINE_COLS = ["identifier_second_item"]

# GROUND_ITEMS — the 145 "ground" SDLITM codes. Ground variations filter item IN this list; Whole
# Grain variations filter item NOT IN this list (whole grain = the complement). Codes compared trimmed.
GROUND_ITEMS = [
    "06069B00000", "06069P30154", "06069P30160", "06069P84101", "06069P91101", "06069P9B101",
    "06113B00000", "06113P84101", "06113P91101", "06115B00000", "06115P91101", "06119B00000",
    "06119P58002", "06119P58102", "06119P80102", "06119P90101", "06119P91101", "06123B00000",
    "06143B00000", "06143P80102", "06143P84101", "06143P91101", "07063B00000", "07123B00000",
    "07150R00000", "08101F00000", "08111B00000", "08113B00000", "08117P91101", "08119B00000",
    "08119P10170", "08122B00000", "08123B00000", "08123P10101", "08131B00000", "08143B00000",
    "08143P10170", "08144B00000", "15061B00000", "15061F00000", "15063B00000", "15111B00000",
    "15111F00000", "15111P30149", "15111P30163", "15111P30170", "15111P80101", "15111P80102",
    "15111P91101", "15111PC7101", "15114F00000", "15114P84101", "15115B00000", "15115F00000",
    "15115P08101", "15115P30142", "15115P30156", "15115P30163", "15115P30170", "15115P80101",
    "15115P80102", "15115P83101", "15115P91101", "15119B00000", "15119F00000", "15119P30142",
    "15119P30149", "15119P30156", "15119P30170", "15119P78102", "15119P80102", "15119P87101",
    "15119P91101", "15119P93101", "15131B00000", "15131F00000", "15131P30149", "15131P30156",
    "15131P30163", "15131P30170", "15131P80102", "15131P87101", "15131P91101", "15131PC1101",
    "15143B00000", "15143P30142", "15143P30149", "15143P30156", "15143P30170", "15143P80102",
    "15143P84101", "15143P91101", "156745", "17061F00000", "17063B00000", "17112B00000",
    "17112F00000", "17114F00000", "17116F00000", "17117F00000", "17117P91101", "17119B00000",
    "17119F00000", "17119P91101", "17131B00000", "17131F00000", "17142B00000", "17143B00000",
    "17143F00000", "17144B00000", "17144F00000", "34123B00000", "50061B00000", "50061P30160",
    "50061P50130", "50063B00000", "50063P30160", "50063P50130", "50064B00000", "50065B00000",
    "50065P30160", "50065P50130", "50066B00000", "50066P30160", "50066P51130", "50067B00000",
    "50067P30160", "50067P51130", "50067P52130", "50068B00000", "50068P30160", "50068P52130",
    "50069B00000", "50069P30160", "50069P52130", "50069P91101", "574772", "60280",
    "75531", "75572", "75614", "75630", "90061B00000", "93123P10101",
    "97064B00000",
]

# ASTM_ITEMS — the 3 SDLITM codes for the ASTM variation (disjoint from GROUND_ITEMS).
ASTM_ITEMS = ["50081P18150", "50084P20150", "50087P19150"]

# One flag COLUMN per Ottawa variation → (mode, item_list). mode: 'include' = 'Included' when the item
# IS in the list (that variation's SDLITM IN (...) rule); 'exclude' = 'Included' when the item is NOT
# in the list (that variation's NOT SDLITM IN (...) rule). Column name encodes the variation so report
# authors pick the right one. (Note: the 3 Whole Grain variations share identical item-list logic — the
# rail/truck + packaged/bulk differences are SDMOT/SDSRP3 fact page filters, not item lists — but each
# gets its own named column for clarity; likewise the 2 Ground variations.)
ITEM_FILTERS = {
    "whole_grain_truck_packaged_filter": ("exclude", GROUND_ITEMS),   # Ottowa - Whole Grain Truck - Packaged (NOT IN ground)
    "whole_grain_truck_bulk_filter":     ("exclude", GROUND_ITEMS),   # Ottowa - Whole Grain Truck - Bulk     (NOT IN ground)
    "whole_grain_rail_bulk_filter":      ("exclude", GROUND_ITEMS),   # Ottowa - Whole Grain Rail  - Bulk     (NOT IN ground)
    "ground_packaged_filter":            ("include", GROUND_ITEMS),   # Ottowa - Ground - Packaged            (IN ground)
    "ground_bulk_filter":                ("include", GROUND_ITEMS),   # Ottowa - Ground - Bulk                (IN ground)
    "astm_packaged_filter":              ("include", ASTM_ITEMS),     # Ottowa - ASTM - Packaged              (IN astm)
}


def build_dim_second_item(spine_df):
    items = drop_deleted(spine_df).select(
        F.col("identifier_second_item").alias("second_item_number")
    )

    # FULL LOAD hands over the whole F4211 table; the incremental path hands over
    # only _SECOND_ITEM_SPINE_COLS.  Same column-count test build_dim_order_number
    # and build_fact_eso5 use.
    #
    # ⚠ This is why _SECOND_ITEM_SPINE_COLS must stay at ONE column.  Adding a
    # column to it would make len(spine_df.columns) equal on both paths and every
    # incremental batch would read as a full load.
    #
    # The history table is added HERE only on a full load.  Incrementally it must
    # not be: generic_recompute already unions F42119 into the re-read (see the
    # rebuild_from_silver branch there), and pulling in all of F42119 on every
    # batch would append history item codes that carry no matching fact_key in
    # affected_keys — so the next batch's DELETE would not remove them and the
    # dim would grow duplicates.
    if len(spine_df.columns) > len(_SECOND_ITEM_SPINE_COLS):
        hist = get_ref(F42119)
        # F42119 snake-names SDLITM as `identifier_2nd_item` — rename before the
        # union.  Same guard build_fact_eso1 (:938) and build_fact_sales_commission
        # (:2483) already apply to this table.
        if (
            "identifier_2nd_item" in hist.columns
            and "identifier_second_item" not in hist.columns
        ):
            hist = hist.withColumnRenamed(
                "identifier_2nd_item", "identifier_second_item"
            )
        items = items.unionByName(
            hist.select(F.col("identifier_second_item").alias("second_item_number"))
        )

    df = (
        items.where(F.col("second_item_number").isNotNull())
        .dropDuplicates(["second_item_number"])
    )

    # One 'Included'/'Excluded' column per Ottawa variation, unchanged from the
    # original notebook.  The comparison is on the TRIMMED value even though the
    # stored key is raw — see the ⚠ on the block header.
    for _col, (_mode, _items) in ITEM_FILTERS.items():
        _in_list = F.trim(F.col("second_item_number")).isin(_items)
        # include: 'Included' when in list; exclude: 'Included' when NOT in list
        _flag = (F.when(_in_list, F.lit("Included")).otherwise(F.lit("Excluded")) if _mode == "include"
                 else F.when(_in_list, F.lit("Excluded")).otherwise(F.lit("Included")))
        df = df.withColumn(_col, _flag)
    return df


# ════════════════════════════════════════════════════════════════════════════
# [FACT]  fact_extended_sales_order_7_v2  ·  spine: F4211
#         ref: F5642B01, F5642B11, F4201, F4941
#
# Ported from nb_silver_to_gold_fact_extended_sales_order_7(1) (batch build).
# Flat ESO7 fact that pre-joins:
#   F4211    (Sales Order Detail)            → order line columns
#   F5642B01 (Custom SO Entry Screen Header) → booking/export columns
#   F5642B11 (Custom SO Entry Screen Detail) → line detail columns
#   F4201    (Sales Order Header)            → hold_code, order_placed_date_julian,
#                                               date_original_promisde (F4201.SHOPDJ)
#   F4941    (Shipment Routing Steps)        → port addresses + is_missing_freight
#
# UOM conversion (Total Tons) is NOT pre-computed here.  It is computed in the PBI
# semantic model via DAX SUMX + RELATED through dim_uom_conversion_item (Tier A,
# via item_uom_key) and dim_uom_conversion (Tier B, via uom_as_input).
#
# is_missing_freight is computed directly from F4941 and stored as a boolean on
# every fact row.  fact_shipment_routing_v2 is not needed for ESO7.
#
# NO BUSINESS FILTERS ARE APPLIED HERE — per the Gold-layer design rule, order
# type / status / routing gates belong in PBI.
#
# **Grain**: one row per F4211 line.  Every join is LEFT and non-fanning; the
# batch notebook asserted uniqueness on (company, order_number, order_type,
# line_number) — that assert is NOT ported (see the note in the registry entry).
# ════════════════════════════════════════════════════════════════════════════
FACT_ESO7_V2_GOLD_LINE_KEYS = [
    "company",
    "order_number",
    "order_type",
    "line_number",
]
FACT_ESO7_V2_SILVER_LINE_KEYS = [
    "company_key_order_no",
    "document_order_invoice_e",
    "order_type",
    "line_number",
]


def build_fact_extended_sales_order_7_v2(spine_df):
    # ── F4211 (all order types — no filter applied here) ─────────────────────
    # groupBy, not select+distinct: the grouping keys ARE the dedup keys, so
    # address_number_parent and jde_updated_ts can sit on the fact WITHOUT
    # taking part in the dedup.  Had they been projected into a .distinct(),
    # a line carrying two parent values (or two update stamps) would split into
    # two Gold rows and break the one-row-per-line grain.
    df_f4211 = drop_deleted(spine_df).groupBy(
            F.col("company_key_order_no").alias("company"),
            F.col("document_order_invoice_e").alias("order_number"),
            F.col("order_type").alias("order_type"),
            F.col("line_number").alias("line_number"),
            F.col("line_type").alias("line_type"),
            F.col("order_suffix").alias("order_suffix"),
            F.col("transaction_originator").alias("order_originator"),
            F.col("shipment_number").alias("shipment_number"),
            F.col("cost_center").alias("cost_center"),
            F.col("address_number").alias("address_number"),
            F.col("address_number_ship_to").alias("address_number_ship_to"),
            F.col("status_code_last").alias("status_code_last"),
            F.col("status_code_next").alias("status_code_next"),
            F.col("date_promised_ship_julian").alias("date_promised_ship_julian"),
            F.col("date_requested_julian").alias("date_requested_julian"),
            F.col("scheduled_pick_date").alias("scheduled_pick_date"),
            F.col("date_release_julian").alias("date_release_julian"),
            F.col("identifier_second_item").alias("identifier_second_item"),
            F.col("identifier_short_item").alias("identifier_short_item"),
            F.col("uom_as_input").alias("uom_as_input"),
            F.col("freight_handling_code").alias("freight_handling_code"),
            F.col("carrier").alias("carrier"),
            F.col("mode_of_transport").alias("mode_of_transport"),
            F.col("container_id").alias("container_id"),
            F.col("reference_01").alias("line_reference_01"),
            F.col("units_transaction_qty").alias("units_transaction_qty"),
            F.col("uom_pricing").alias("uom_pricing"),                 # SDUOM4 — Source UoM (Price)
            F.col("amt_price_per_unit_02").alias("unit_price"),        # SDUPRC — Source Price
    ).agg(
        F.max(_jde_ts(F.col("date_updated"), F.col("time_of_day"))).alias(
            "jde_updated_ts"
        ),
        F.first("address_number_parent", ignorenulls=True).alias(
            "address_number_parent"
        ),
    )

    # ── F5642B01 (booking header) ────────────────────────────────────────────
    df_f5642b01 = (
        get_ref(F5642B01)
        .select(
            F.col("shipment_number").alias("b01_shipment_number"),
            F.col("document_order_invoice_e").alias("b01_order_number"),
            F.col("order_type").alias("b01_order_type"),
            F.col("company_key_order_no").alias("b01_company"),
            F.col("booking_no").alias("booking_no"),
            F.col("bookingstatus").alias("bookingstatus"),
            F.col("routing_notes").alias("routing_notes"),
            F.col("date_requested_ship").alias("date_requested_ship"),
            F.col("destination_port").alias("destination_port_address"),
            F.col("date_latest_delivery").alias("date_latest_delivery"),
            F.col("vessel_name").alias("vessel_name"),
            F.col("voyage_no").alias("voyage_no"),
            F.col("date_loaded").alias("date_loaded"),
            F.col("date_release_julian").alias("booking_date_release_julian"),
            F.col("date_01").alias("date_01"),
            F.col("loading_port").alias("loading_port_address"),
            F.col("ocean_carrier").alias("ocean_carrier_address"),
            F.col("ocean_del_terms").alias("ocean_del_terms"),
            F.col("no_of_container").alias("no_of_container"),
            F.col("reference_01").alias("booking_reference_01"),
            F.col("reference_02").alias("booking_reference_02"),
            F.col("inland_delterms").alias("inland_delterms"),
            F.col("incoterms").alias("incoterms"),
            F.col("equipment_type").alias("equipment_type"),
            F.col("date_latest_pickup").alias("date_latest_pickup"),
        )
        .distinct()
    )

    # ── F5642B11 (line detail) ───────────────────────────────────────────────
    df_f5642b11 = (
        get_ref(F5642B11)
        .select(
            F.col("company_key_order_no").alias("b11_company"),
            F.col("document_order_invoice_e").alias("b11_order_number"),
            F.col("order_type").alias("b11_order_type"),
            F.col("line_number").alias("b11_line_number"),
            F.col("shipment_number").alias("b11_shipment_number"),
            F.col("seal_no").alias("seal_no"),
            F.col("production_code").alias("production_code"),
            F.col("production_ship_notes").alias("production_ship_notes"),
        )
        .distinct()
    )

    # ── F4201 (Sales Order Header) ───────────────────────────────────────────
    # date_original_promisde = F4201.SHOPDJ (original promised date at header level)
    df_f4201 = (
        get_ref(F4201)
        .select(
            F.col("company_key_order_no").alias("h_company"),
            F.col("document_order_invoice_e").alias("h_order_number"),
            F.col("order_type").alias("h_order_type"),
            F.col("order_suffix").alias("h_order_suffix"),
            F.col("hold_orders_code").alias("hold_code"),
            F.col("date_transaction_julian").alias("order_placed_date_julian"),
            F.col("date_original_promisde").alias("date_original_promisde"),
        )
        .distinct()
    )

    # ── F4941 OCE routing: port addresses + is_missing_freight flag ───────────
    # Two datasets built from F4941 OCE rows:
    #
    # 1. df_f4941_ports — ALL OCE rows per shipment (any completeness).
    #    Provides routing_load_port_address (F4941.RSORGN) and
    #    routing_dest_port_address (F4941.RSANCC).
    #    For missing freight rows these will be NULL → blank in PBI,
    #    matching Hubble's INNER JOIN behaviour on F0101.
    #
    # 2. df_f4941_oce — only rows where BOTH port addresses are non-null on the
    #    SAME row. Used exclusively to derive is_missing_freight.
    #    Matches Hubble: (F4941.RSORGN * F4941.RSANCC) IS NULL
    _df_f4941_silver = get_ref(F4941).filter(F.col("mode_of_transport") == "OCE")

    # Port addresses for all OCE shipments (NULL for missing freight rows)
    df_f4941_ports = _df_f4941_silver.select(
        F.col("shipment_number").alias("port_shipment"),
        F.col("origin_address_number").alias("routing_load_port_address"),
        F.col("address_number_deconsolida").alias("routing_dest_port_address"),
    ).distinct()

    # Completeness flag: shipments with a fully-populated OCE leg
    df_f4941_oce = (
        _df_f4941_silver.filter(
            F.col("origin_address_number").isNotNull()
            & F.col("address_number_deconsolida").isNotNull()
        )
        .select(F.col("shipment_number").alias("oce_shipment"))
        .distinct()
        .withColumn("has_complete_oce", F.lit(True))
    )

    # ── Join all sources into the flat ESO7 fact ──────────────────────────────
    # Join order:
    #   F4211  LEFT JOIN  F5642B01       ON (shipment×order×type×company)
    #          LEFT JOIN  F5642B11       ON (company×order×type×line×shipment)
    #          LEFT JOIN  F4201          ON (company×order×type×suffix)
    #          LEFT JOIN  df_f4941_ports ON shipment_number
    #          LEFT JOIN  df_f4941_oce   ON shipment_number → is_missing_freight
    #
    # Every ref column is prefix-renamed above, so the spine side stays
    # unambiguous as a bare F.col() — no alias() wrapper needed.
    df_fact = (
        df_f4211
        # ── booking header (F5642B01) ───────────────────────────────────────
        .join(
            df_f5642b01,
            (F.col("shipment_number") == F.col("b01_shipment_number"))
            & (F.col("order_number") == F.col("b01_order_number"))
            & (F.col("order_type") == F.col("b01_order_type"))
            & (F.col("company") == F.col("b01_company")),
            how="left",
        )
        .drop("b01_shipment_number", "b01_order_number", "b01_order_type", "b01_company")
        # ── line detail (F5642B11) ─────────────────────────────────────────
        .join(
            df_f5642b11,
            (F.col("company") == F.col("b11_company"))
            & (F.col("order_number") == F.col("b11_order_number"))
            & (F.col("order_type") == F.col("b11_order_type"))
            & (F.col("line_number") == F.col("b11_line_number"))
            & (F.col("shipment_number") == F.col("b11_shipment_number")),
            how="left",
        )
        .drop(
            "b11_company",
            "b11_order_number",
            "b11_order_type",
            "b11_line_number",
            "b11_shipment_number",
        )
        # ── order header (F4201) ──────────────────────────────────────────
        .join(
            df_f4201,
            (F.col("company") == F.col("h_company"))
            & (F.col("order_number") == F.col("h_order_number"))
            & (F.col("order_type") == F.col("h_order_type"))
            & (F.col("order_suffix") == F.col("h_order_suffix")),
            how="left",
        )
        .drop("h_company", "h_order_number", "h_order_type", "h_order_suffix")
        .drop("order_suffix")
        # ── F4941 routing port addresses ───────────────────────────────────
        .join(
            df_f4941_ports,
            F.col("shipment_number") == F.col("port_shipment"),
            how="left",
        )
        .drop("port_shipment")
        # ── missing freight flag (F4941) ───────────────────────────────────
        .join(
            df_f4941_oce, F.col("shipment_number") == F.col("oce_shipment"), how="left"
        )
        .drop("oce_shipment")
        .withColumn("is_missing_freight", F.col("has_complete_oce").isNull())
        .drop("has_complete_oce")
    )

    # ── Surrogate keys ───────────────────────────────────────────────────────
    # order_key          → dim_order.order_key
    # order_activity_key → dim_order_activity.order_activity_key
    # item_uom_key       → dim_uom_conversion_item.item_uom_key
    #                      Required for the Total Tons DAX RELATED lookup (Tier A).
    #                      Must match nb_silver_to_gold_dim_uom_conversion_item:
    #                      concat_ws('|', identifier_short_item, from_uom)
    df_fact = (
        df_fact.withColumn(
            "order_key", F.concat_ws("|", "order_number", "order_type", "company")
        )
        .withColumn(
            "order_activity_key",
            F.concat_ws("|", "order_type", "line_type", "status_code_last"),
        )
        .withColumn(
            "item_uom_key",
            F.concat_ws("|", "identifier_short_item", "uom_as_input"),
        )
    )

    # ── standard UoM conversion (baked): SDUOM->TN volume factor/tons/flag + SDUOM4->TN price fields ──
    # Additive columns; the existing Total Tons DAX (item_uom_key / uom_as_input) is untouched.
    # Deduped cascade → LEFT joins never fan the line grain out.
    _keep_cols = df_fact.columns
    _vi, _vs = _uom_cascades()
    df_fact = (
        df_fact
        .join(_vi.alias("civ"), (F.col("civ.itm") == F.col("identifier_short_item"))
                                & (F.col("civ.from_uom") == F.trim(F.col("uom_as_input"))), "left")
        .join(_vs.alias("csv"), (F.col("csv.from_uom") == F.trim(F.col("uom_as_input"))), "left")
        .join(_vi.alias("cip"), (F.col("cip.itm") == F.col("identifier_short_item"))
                                & (F.col("cip.from_uom") == F.trim(F.col("uom_pricing"))), "left")
        .join(_vs.alias("csp"), (F.col("csp.from_uom") == F.trim(F.col("uom_pricing"))), "left")
    )
    _vol_raw = F.coalesce(F.when(F.trim(F.col("uom_as_input")) == "TN", F.lit(1.0)),
                          F.col("civ.conv_factor"), F.col("csv.conv_std"))
    _prc_raw = F.coalesce(F.when(F.trim(F.col("uom_pricing")) == "TN", F.lit(1.0)),
                          F.col("cip.conv_factor"), F.col("csp.conv_std"))
    _prc_fac = F.coalesce(_prc_raw, F.lit(1.0))
    df_fact = (
        df_fact
        .withColumn("conversion_to_tons_rate", F.coalesce(_vol_raw, F.lit(1.0)))
        .withColumn("quantity_shipped_tons", F.col("units_transaction_qty").cast("double") * F.coalesce(_vol_raw, F.lit(1.0)))
        .withColumn("missing_conversion_flag", F.when(_vol_raw.isNull(), F.lit("Y")).otherwise(F.lit("N")))
        .withColumn("price_conversion_factor", _prc_fac)
        .withColumn("converted_price_per_ton",
            F.when((F.col("status_code_last").cast("int") != F.lit(980))
                   & (F.col("status_code_next").cast("int") == F.lit(999)),
                   F.col("unit_price").cast("double") / F.when(_prc_fac != 0, _prc_fac)))
        .withColumn("price_missing_conversion_flag", F.when(_prc_raw.isNull(), F.lit("Y")).otherwise(F.lit("N")))
        # keep only the fact's own columns + the 6 derived (drop the cascade join helpers)
        .select(*_keep_cols, "conversion_to_tons_rate", "quantity_shipped_tons", "missing_conversion_flag",
                "price_conversion_factor", "converted_price_per_ton", "price_missing_conversion_flag")
    )

    return df_fact


# ════════════════════════════════════════════════════════════════════════════
# [FACT]  fact_sales_order_price_adjustment  ·  spine: F4211
#         ref: F4074 (fanout), F4201  ·  passive: F4101, F41002, F49211
#         unregistered direct read: F41003
#
# Ported from nb_eso1_gold_fact_sales_order_price_adjustment (batch build).
# Build logic is unchanged; only the v3 plumbing differs:
#   * load_silver_table(F4211) -> the spine_df handed in by the engine
#   * load_silver_table(<ref>) -> get_ref(<ref>), engine-narrowed slices
#   * the notebook's own sk / clean_date / date_key -> v3's shared helpers
#     (identical formulas — v3 sk() is concat_ws("|",…) and _eso1_clean_date
#     uses the same 2000-01-01 / +25y window, so keys and dates are unchanged)
#   * MANUAL_OVERWRITE + the run cell + notebook.exit -> engine full-load logic
#
# **Grain**: one row per (F4211 line × whitelisted F4074 adjustment). A line
# with no whitelisted adjustment keeps ONE base row via the LEFT join. The
# engine's `fact_key` hashes the 4-part LINE natural key only, so every fanned
# row of a line shares one fact_key — DELETE removes the whole fan and the
# rebuild re-emits it complete. Same shape as fact_extended_sales_order_6.
#
# `is_primary_line_row` = 'Y' on exactly one row per line; line-grain measures
# must filter on it or they inflate N× across the fan.
# ════════════════════════════════════════════════════════════════════════════
FACT_PRICE_ADJ_GOLD_LINE_KEYS = [
    "company_key_order_no",
    "order_type",
    "order_number",
    "line_number",
]
FACT_PRICE_ADJ_SILVER_LINE_KEYS = [
    "company_key_order_no",
    "order_type",
    "document_order_invoice_e",
    "line_number",
]

# ── Price-adjustment whitelist (ALAST) — GRAIN-shaping ──
# Only these adjustment types fan a line out; every other F4074 row is treated as "no adjustment"
# (the line keeps its single base row via the LEFT join).
_ADJ_WHITELIST_CORE = [
    "A03", "CASLB", "FRTHIDE", "FRTTAXN", "FRTTAXY",
    "PP06", "PP07", "PP08", "PP13", "PP15", "PP17", "PP26", "PP37",
    "PP50", "PP51", "PP56", "PP57", "PP97", "PP99", "PPSLB",
    "COLPALN", "COLPALT", "ALST",
]
# ENERGY toggle. True → ENERGY adjustments fan out too. A line that carries ENERGY alongside another
# whitelisted adjustment is unaffected in its aggregates (the ENERGY row has is_primary_line_row=N and
# 0 in every bucket column). A line whose ONLY whitelisted adjustment is ENERGY exists as an ENERGY row
# instead of a base row — set False to keep such a line as a base row.
INCLUDE_ENERGY = True
ADJ_WHITELIST = _ADJ_WHITELIST_CORE + (["ENERGY"] if INCLUDE_ENERGY else [])

_PRICE_ADJ_RAW_DATES = [
    "order_date",
    "requested_date",
    "actual_ship_date",
    "invoice_date",
    "gl_date",
]

# ── line-grain columns carried on the fact (repeat across the adjustment fan) ──
_PRICE_ADJ_LINE_COLS = [
    # ── order / line identifiers ──
    "company", "company_key_order_no", "order_type", "order_number", "document_number", "line_number",
    "shipment_number",
    # ── status / handling / transport ──
    "hold_orders_code", "status_code_last", "status_code_next", "next_status_num", "last_status_num",
    "freight_handling_code", "cars", "mode_of_transport", "container_id", "customer_po_number",
    "gl_class", "line_type", "payment_terms_code_01",
    "sales_reporting_code_01", "sales_reporting_code_02", "sales_reporting_code_03",
    "sales_reporting_code_04", "sales_reporting_code_05",
    # ── address / dimension FKs ──
    "ship_to", "sold_to", "address_number_parent", "branch_plant",
    "item_number_short", "second_item_number", "third_item_number", "item_segment_4",
    # ── raw event dates + int keys ──
    "order_date", "requested_date", "actual_ship_date", "invoice_date", "gl_date",
    "order_date_key", "requested_date_key", "ship_date_key", "invoice_date_key", "gl_date_key",
    # ── uom / tons ──
    "uom", "uom_pricing", "uom_primary", "uom_structure", "conversion_to_tons_rate", "missing_conversion_flag",
    "ordered_tons", "shipped_tons",
    # added 2026-08-24 — price-side conversion (see _price_adj_line_df; formula
    # differs from fact_sales_order_freight's, ported as written in each dev file)
    "price_conversion_factor", "converted_price_per_ton", "price_missing_conversion_flag",
    # ── line measures ──
    "quantity_shipped", "primary_quantity_ordered", "transaction_quantity",
    "extended_price", "extended_cost", "unit_price", "currency_code_base",
    # ── misc display / filter ──
    "location", "lot_number", "user_reserved_code", "user_reserved_number", "user_reserved_reference",
    "price_override_code", "price_adjustment_schedule", "transaction_originator", "user_id",
    "date_updated", "time_of_day", "zone_number", "deferred_entries_flag",
    # ── audit ──
    "jde_updated_ts",                                                    # SDUPMJ + SDTDAY
]

# ── adjustment-grain columns (NULL on base rows) ──
_PRICE_ADJ_ADJ_COLS = ["price_adjustment_type", "adj_print_code", "adj_unit_price", "adj_uom",
                       "adj_based_on_value", "adj_gl_class", "adj_factor_value"]

# is_primary_line_row: 'Y' on exactly ONE row per line. Line-grain values repeat across the
# adjustment fan, so line-level measures must sum with this flag (CALCULATE(..., ="Y")) to avoid
# N× inflation; adjustment-level buckets ignore it and iterate all rows.
# is_product_line + freight_hide_amount: precomputed classification so the semantic-model
# measures are trivial fast aggregates instead of per-row FILTER(fact) scans.
_PRICE_ADJ_BUSINESS_COLS = _PRICE_ADJ_LINE_COLS + _PRICE_ADJ_ADJ_COLS + [
    "is_primary_line_row", "is_product_line",
    # precomputed per-row bucket amounts (measures = plain SUM(column))
    "product_ordered_tons", "product_ext_price", "freight_hide_amount",
    "non_product_amount", "al_severance_amount", "misc_billing_amount",
    "freight_amount", "car_charges_amount", "dryer_freight_amount"]


def _price_adj_conv_lookups():
    """Two bidirectional UOM→factor lookups, each collapsed with the ambiguity rule
    (more than one distinct factor for a key → NULL, i.e. unusable). Factors are decoded
    in Silver, so the SQL's 10000000 identity is 1.0 here and the from/to ratio is unit-free.

    conv_item  keyed (item, from_uom):
        fwd  from_uom = UMUM       factor = UMCNV1 (conversion_factor_sec)
        rev  from_uom = UMRUM      factor = UMCNV1 / UMCONV  (UMCONV<>0)
        blank cost-center (UMMCU) rows only — matches the item-specific join.
    conv_std   keyed (from_uom, to_uom):
        fwd  from_uom = UCUM  to_uom = UCRUM   factor = UCCONV (conversion_factor)
        rev  from_uom = UCRUM to_uom = UCUM    factor = 1 / UCCONV  (UCCONV<>0)
    """
    # ── F41002 item-specific (blank cost-center only) ──
    f41002 = get_ref(F41002).filter(F.trim(F.col("cost_center")) == "")
    i_fwd = f41002.select(
        F.col("identifier_short_item").alias("itm"),
        F.trim(F.col("uom")).alias("from_uom"),
        F.round(F.col("conversion_factor_sec").cast("double"), 9).alias("f"))
    i_rev = (f41002.filter(F.col("conversion_factor").cast("double") != 0)
             .select(
                 F.col("identifier_short_item").alias("itm"),
                 F.trim(F.col("related_uom")).alias("from_uom"),
                 F.round(F.col("conversion_factor_sec").cast("double")
                         / F.col("conversion_factor").cast("double"), 9).alias("f")))
    conv_item = (i_fwd.unionByName(i_rev)
                 .dropDuplicates(["itm", "from_uom", "f"])          # SQL UNION dedups identical (key,value)
                 .groupBy("itm", "from_uom")
                 .agg(F.count("f").alias("n"), F.min("f").alias("f"))
                 .withColumn("conv_item", F.when(F.col("n") > 1, F.lit(None).cast("double"))
                                           .otherwise(F.col("f")))  # >1 distinct factor → unusable
                 .select("itm", "from_uom", "conv_item"))

    # ── F41003 standard ──
    # Unregistered ref, read directly — same reason as ESO1's F0101 (:1087). F41003 is keyed by
    # (UCUM, UCRUM) and has NO column the spine can narrow on, and conv_std groups GLOBALLY by
    # (from_uom, to_uom). Registering it would force the engine's left_semi against a spine column
    # that does not exist on it. Small standard-conversion table, so a full read is correct.
    f41003 = drop_deleted(spark.read.table(sname(F41003)))
    s_fwd = f41003.select(
        F.trim(F.col("uom")).alias("from_uom"),
        F.trim(F.col("related_uom")).alias("to_uom"),
        F.round(F.col("conversion_factor").cast("double"), 9).alias("f"))
    s_rev = (f41003.filter(F.col("conversion_factor").cast("double") != 0)
             .select(
                 F.trim(F.col("related_uom")).alias("from_uom"),
                 F.trim(F.col("uom")).alias("to_uom"),
                 F.round(F.lit(1.0) / F.col("conversion_factor").cast("double"), 9).alias("f")))
    conv_std = (s_fwd.unionByName(s_rev)
                .dropDuplicates(["from_uom", "to_uom", "f"])
                .groupBy("from_uom", "to_uom")
                .agg(F.count("f").alias("n"), F.min("f").alias("f"))
                .withColumn("conv_std", F.when(F.col("n") > 1, F.lit(None).cast("double"))
                                         .otherwise(F.col("f")))
                .select("from_uom", "to_uom", "conv_std"))

    return conv_item, conv_std


def _price_adj_line_df(spine_df):
    """One row per F4211 sales-order line, with the item master (IMUOM1), header hold code,
    and the computed ordered/shipped tons. No business WHERE — every line is carried."""
    sd = drop_deleted(spine_df)
    im = get_ref(F4101)
    sh = get_ref(F4201)
    tg = get_ref(F49211)
    conv_item, conv_std = _price_adj_conv_lookups()

    # SO-line tag → deferred-entries flag (UDDEFF); one row per line so the LEFT join can't fan the grain
    tag = (tg.groupBy("company_key_order_no", "order_type", "document_order_invoice_e", "line_number")
             .agg(F.first("deferred_entries_flag", ignorenulls=True).alias("deferred_entries_flag")))

    # item master → primary UOM (IMUOM1), item segment (IMSEG4) — one row per short item
    item = (im.select(
                F.col("identifier_short_item").alias("im_itm"),
                F.trim(F.col("uom_primary")).alias("uom_primary"),
                F.col("segment_04").alias("item_segment_4"))
            .dropDuplicates(["im_itm"]))

    # item-level UOM structure (F41002 UMUSTR, blank cost-center) → one row per item so this LEFT
    # join can't fan the line grain. UMUSTR is an item attribute (the UOM template the item uses —
    # constant across the item's UOM rows); F.max collapses the rare multi-value case deterministically.
    struct = (get_ref(F41002).filter(F.trim(F.col("cost_center")) == "")
              .groupBy("identifier_short_item")
              .agg(F.max(F.trim(F.col("uom_structure"))).alias("uom_structure")))

    # order header → hold code (one row per order)
    hdr = (sh.select(
               F.col("company_key_order_no").alias("h_kcoo"),
               F.col("order_type").alias("h_dcto"),
               F.col("document_order_invoice_e").alias("h_doco"),
               F.col("hold_orders_code").alias("hold_orders_code"))
           .dropDuplicates(["h_kcoo", "h_dcto", "h_doco"]))

    j = (sd.alias("sd")
         .join(item.alias("im"), F.col("im.im_itm") == F.col("sd.identifier_short_item"), "left")
         .join(hdr.alias("sh"),
               (F.col("sh.h_kcoo") == F.col("sd.company_key_order_no")) &
               (F.col("sh.h_dcto") == F.col("sd.order_type")) &
               (F.col("sh.h_doco") == F.col("sd.document_order_invoice_e")), "left")
         .join(tag.alias("tg"),
               (F.col("tg.company_key_order_no") == F.col("sd.company_key_order_no")) &
               (F.col("tg.order_type") == F.col("sd.order_type")) &
               (F.col("tg.document_order_invoice_e") == F.col("sd.document_order_invoice_e")) &
               (F.col("tg.line_number") == F.col("sd.line_number")), "left")
         .join(struct.alias("us"),
               F.col("us.identifier_short_item") == F.col("sd.identifier_short_item"), "left")
         # tons cascade lookups: item-specific (from SDUOM / to TN) then standard (from SDUOM / to TN)
         .join(conv_item.alias("cif"),
               (F.col("cif.itm") == F.col("sd.identifier_short_item")) &
               (F.col("cif.from_uom") == F.trim(F.col("sd.uom_as_input"))), "left")
         .join(conv_item.alias("cit"),
               (F.col("cit.itm") == F.col("sd.identifier_short_item")) &
               (F.col("cit.from_uom") == F.lit("TN")), "left")
         .join(conv_std.alias("csf"),
               (F.col("csf.from_uom") == F.trim(F.col("sd.uom_as_input"))) &
               (F.col("csf.to_uom") == F.trim(F.col("im.uom_primary"))), "left")
         .join(conv_std.alias("cst"),
               (F.col("cst.from_uom") == F.lit("TN")) &
               (F.col("cst.to_uom") == F.trim(F.col("im.uom_primary"))), "left")
         # PRICE cascade lookups on the pricing UoM (SDUOM4), added 2026-08-24: item-specific
         # + standard, from SDUOM4 to IMUOM1 — reuses conv_item/conv_std, no new lookup table.
         .join(conv_item.alias("cifp"),
               (F.col("cifp.itm") == F.col("sd.identifier_short_item")) &
               (F.col("cifp.from_uom") == F.trim(F.col("sd.uom_pricing"))), "left")
         .join(conv_std.alias("csfp"),
               (F.col("csfp.from_uom") == F.trim(F.col("sd.uom_pricing"))) &
               (F.col("csfp.to_uom") == F.trim(F.col("im.uom_primary"))), "left"))

    # from-factor: item-specific → standard → (SDUOM = IMUOM1 ? 1.0)
    from_factor = F.coalesce(
        F.col("cif.conv_item"), F.col("csf.conv_std"),
        F.when(F.trim(F.col("sd.uom_as_input")) == F.trim(F.col("im.uom_primary")), F.lit(1.0)))
    # to-factor: item-specific(TN) → standard(TN) → ('TN' = IMUOM1 ? 1.0)
    to_factor = F.coalesce(
        F.col("cit.conv_item"), F.col("cst.conv_std"),
        F.when(F.lit("TN") == F.trim(F.col("im.uom_primary")), F.lit(1.0)))
    # zero/null-divisor guard → factor 0 (zero-on-fail cascade)
    factor = F.when(from_factor.isNull() | to_factor.isNull() | (to_factor == 0), F.lit(0.0)) \
              .otherwise(from_factor / to_factor)

    # price factor on the pricing UoM SDUOM4, added 2026-08-24 — same two-hop method as the
    # volume factor above: from-leg SDUOM4->IMUOM1 (item F41002 then standard F41003), to-leg
    # TN->IMUOM1 (shared to_factor, NOT recomputed). This is a DIFFERENT formula from
    # fact_sales_order_freight's price cascade (which goes SDUOM4->TN directly, no IMUOM1
    # pivot) — ported exactly as each dev notebook wrote it, not reconciled between the two.
    from_factor_price = F.coalesce(
        F.col("cifp.conv_item"), F.col("csfp.conv_std"),
        F.when(F.trim(F.col("sd.uom_pricing")) == F.trim(F.col("im.uom_primary")), F.lit(1.0)))
    price_factor_raw = F.when(
        from_factor_price.isNull() | to_factor.isNull() | (to_factor == 0), F.lit(None).cast("double")
    ).otherwise(from_factor_price / to_factor)
    price_factor = F.coalesce(price_factor_raw, F.lit(1.0))
    _pf_nz = F.when(price_factor != 0, price_factor)
    # delivered lines only: Last Status (SDLTTR) <> 980 AND Next Status (SDNXTR) = 999
    _delivered = (F.trim(F.col("sd.status_code_last")).cast("int") != F.lit(980)) & (
        F.trim(F.col("sd.status_code_next")).cast("int") == F.lit(999)
    )
    converted_price_per_ton = F.when(_delivered, F.col("sd.amt_price_per_unit_02") / _pf_nz)

    sel = j.select(
        # ── order / line identifiers ──
        F.col("sd.company").alias("company"),
        F.col("sd.company_key_order_no").alias("company_key_order_no"),   # SDKCOO
        F.col("sd.order_type").alias("order_type"),                       # SDDCTO
        F.col("sd.document_order_invoice_e").alias("order_number"),       # SDDOCO
        F.col("sd.doc_voucher_invoice_e").alias("document_number"),       # SDDOC
        F.col("sd.line_number").alias("line_number"),                     # SDLNID
        F.col("sd.shipment_number").alias("shipment_number"),             # SDSHPN
        # ── status / handling / transport ──
        F.col("sh.hold_orders_code").alias("hold_orders_code"),           # SHHOLD (header)
        F.col("sd.status_code_last").alias("status_code_last"),           # SDLTTR
        F.col("sd.status_code_next").alias("status_code_next"),           # SDNXTR
        F.trim(F.col("sd.status_code_next")).cast("int").alias("next_status_num"),
        F.trim(F.col("sd.status_code_last")).cast("int").alias("last_status_num"),
        F.col("sd.freight_handling_code").alias("freight_handling_code"), # SDFRTH
        F.col("sd.carrier").alias("cars"),                                # SDCARS
        F.trim(F.col("sd.mode_of_transport")).alias("mode_of_transport"), # SDMOT
        F.col("sd.container_id").alias("container_id"),                   # SDCNID (Vehicle No.)
        F.col("sd.reference_01").alias("customer_po_number"),             # SDVR01 (Customer PO Number)
        F.col("sd.gl_class").alias("gl_class"),                           # SDGLC
        F.col("sd.line_type").alias("line_type"),                        # SDLNTY
        F.col("sd.payment_terms_code_01").alias("payment_terms_code_01"), # SDPTC (payment terms)
        F.col("sd.sales_reporting_code_01").alias("sales_reporting_code_01"),
        F.col("sd.sales_reporting_code_02").alias("sales_reporting_code_02"),
        F.col("sd.sales_reporting_code_03").alias("sales_reporting_code_03"),
        F.col("sd.sales_reporting_code_04").alias("sales_reporting_code_04"),
        F.col("sd.sales_reporting_code_05").alias("sales_reporting_code_05"),
        # ── address / dimension FKs ──
        F.col("sd.address_number_ship_to").alias("ship_to"),              # SDSHAN
        F.col("sd.address_number").alias("sold_to"),                      # SDAN8
        F.col("sd.address_number_parent").alias("address_number_parent"), # SDPA8
        F.trim(F.col("sd.cost_center")).alias("branch_plant"),            # SDMCU
        F.col("sd.identifier_short_item").alias("item_number_short"),     # SDITM
        F.col("sd.identifier_second_item").alias("second_item_number"),   # SDLITM
        F.col("sd.identifier_third_item").alias("third_item_number"),     # SDAITM (freight/car-charge line prefix)
        F.col("im.item_segment_4").alias("item_segment_4"),               # IMSEG4
        # ── raw event dates ──
        F.col("sd.date_transaction_julian").alias("order_date"),          # SDTRDJ
        F.col("sd.date_requested_julian").alias("requested_date"),        # SDDRQJ
        F.col("sd.actual_ship_date").alias("actual_ship_date"),           # SDADDJ
        F.col("sd.date_invoice_julian").alias("invoice_date"),            # SDIVD
        F.col("sd.dt_for_gl_and_vouch_01").alias("gl_date"),              # SDDGL
        # ── uom / tons ──
        F.col("sd.uom_as_input").alias("uom"),                            # SDUOM
        F.col("sd.uom_pricing").alias("uom_pricing"),                     # SDUOM4 (pricing UoM — added 2026-08-24)
        F.col("im.uom_primary").alias("uom_primary"),                     # IMUOM1
        F.col("us.uom_structure").alias("uom_structure"),                 # UMUSTR (F41002 item UOM structure)
        factor.alias("conversion_to_tons_rate"),
        F.when(factor == 0, F.lit("Y")).otherwise(F.lit("N")).alias("missing_conversion_flag"),
        (F.col("sd.units_transaction_qty").cast("double") * factor).alias("ordered_tons"),   # SDUORG × factor
        (F.col("sd.units_quantity_shipped").cast("double") * factor).alias("shipped_tons"),  # SDSOQS × factor
        # ── price-side conversion, added 2026-08-24 (SDUOM4->TN cascade; per-ton on delivered lines) ──
        price_factor.alias("price_conversion_factor"),
        converted_price_per_ton.alias("converted_price_per_ton"),
        F.when(price_factor_raw.isNull(), F.lit("Y")).otherwise(F.lit("N")).alias("price_missing_conversion_flag"),
        # ── line measures ──
        F.col("sd.units_quantity_shipped").alias("quantity_shipped"),         # SDSOQS
        F.col("sd.units_primary_qty_order").alias("primary_quantity_ordered"),# SDPQOR
        F.col("sd.units_transaction_qty").alias("transaction_quantity"),      # SDUORG
        F.col("sd.amount_extended_price").alias("extended_price"),            # SDAEXP
        F.col("sd.amount_extended_cost").alias("extended_cost"),              # SDECST
        F.col("sd.amt_price_per_unit_02").alias("unit_price"),                # SDUPRC
        F.col("sd.currency_code_base").alias("currency_code_base"),           # SDBCRC
        # ── misc display / filter ──
        F.col("sd.location").alias("location"),                           # SDLOCN
        F.col("sd.lot").alias("lot_number"),                             # SDLOTN
        F.col("sd.user_reserved_code").alias("user_reserved_code"),      # SDURCD
        F.col("sd.user_reserved_number").alias("user_reserved_number"),  # SDURAB
        F.col("sd.user_reserved_reference").alias("user_reserved_reference"),  # SDURRF
        F.col("sd.price_override_code").alias("price_override_code"),     # SDPROV
        F.col("sd.price_adjustment_schedule_n").alias("price_adjustment_schedule"),  # SDASN
        F.col("sd.transaction_originator").alias("transaction_originator"),  # SDTORG
        F.col("sd.user_id").alias("user_id"),                            # SDUSER
        F.col("sd.date_updated").alias("date_updated"),                  # SDUPMJ
        F.col("sd.time_of_day").alias("time_of_day"),                    # SDTDAY
        F.col("sd.zone_number").alias("zone_number"),                    # SDZON
        F.col("tg.deferred_entries_flag").alias("deferred_entries_flag"),# F49211 UDDEFF
        # ── Audit ──
        # Line-grain, so it repeats across the adjustment fan like every other
        # line column.  No aggregation sits downstream — the fan-out join and the
        # two windows all preserve rows — so a plain projection is correct here
        # and no F.max() collapse is needed.
        _jde_ts(F.col("sd.date_updated"), F.col("sd.time_of_day")).alias(
            "jde_updated_ts"
        ),  # SDUPMJ + SDTDAY
    )

    # clean sentinel dates, then derive int date keys
    for _dc in _PRICE_ADJ_RAW_DATES:
        sel = sel.withColumn(_dc, _eso1_clean_date(F.col(_dc)))
    sel = (sel
           .withColumn("order_date_key",     _eso1_date_key(F.col("order_date")))
           .withColumn("requested_date_key", _eso1_date_key(F.col("requested_date")))
           .withColumn("ship_date_key",      _eso1_date_key(F.col("actual_ship_date")))
           .withColumn("invoice_date_key",   _eso1_date_key(F.col("invoice_date")))
           .withColumn("gl_date_key",        _eso1_date_key(F.col("gl_date"))))

    # line key + one row per line (defensive — F4211 is already line grain)
    sel = sel.withColumn("sales_order_line_key",
                         sk("company_key_order_no", "order_type", "order_number", "line_number"))
    return sel.dropDuplicates(["sales_order_line_key"])


def _price_adj_adjustments():
    """Whitelisted F4074 adjustments only, keyed to the line. The whitelist is applied
    BEFORE the fan-join so a line whose only adjustments are non-whitelisted keeps a single
    base row (LEFT join → NULL adjustment)."""
    adj = get_ref(F4074)
    adj = adj.filter(F.trim(F.col("price_adjustment_type")).isin(ADJ_WHITELIST))
    adj = adj.select(
        sk("company_key_order_no", "order_type", "document_order_invoice_e", "line_number")  # ALKCOO|ALDCTO|ALDOCO|ALLNID
          .alias("sales_order_line_key"),
        F.trim(F.col("price_adjustment_type")).alias("price_adjustment_type"),  # ALAST
        F.trim(F.col("pricing_report_code_01")).alias("adj_print_code"),        # ALAPRP1
        F.col("amt_price_per_unit_02").cast("double").alias("adj_unit_price"),   # ALUPRC
        F.trim(F.col("uom_as_input")).alias("adj_uom"),                         # ALUOM
        F.col("based_on_value").cast("double").alias("adj_based_on_value"),      # ALBSDVAL
        F.col("gl_class").alias("adj_gl_class"),                                 # ALGLC
        F.col("factor_value").cast("double").alias("adj_factor_value"))          # ALFVTR
    # collapse to the adjustment grain (ALAST, ALUPRC, ALUOM, ALBSDVAL) so duplicate F4074
    # records don't double-count in the adjustment-bucket measures.
    return adj.dropDuplicates(["sales_order_line_key", "price_adjustment_type",
                               "adj_unit_price", "adj_uom", "adj_based_on_value"])


def build_fact_sales_order_price_adjustment(spine_df):
    line = _price_adj_line_df(spine_df)
    adj = _price_adj_adjustments()

    # fan the line out to one row per whitelisted adjustment; lines with none keep one base row
    df = line.join(adj, "sales_order_line_key", "left")

    # per-fanned-row surrogate key — stable within a line by a deterministic adjustment sequence
    _ws = Window.partitionBy("sales_order_line_key").orderBy(
              F.col("price_adjustment_type").asc_nulls_first(),
              F.col("adj_unit_price").asc_nulls_first(),
              F.col("adj_gl_class").asc_nulls_first())
    df = (df.withColumn("_seq", F.row_number().over(_ws))
            .withColumn("price_adjustment_key", sk("sales_order_line_key", "_seq"))
            .withColumn("is_primary_line_row",
                        F.when(F.col("_seq") == 1, F.lit("Y")).otherwise(F.lit("N")))
            .drop("_seq"))

    # ── classification columns (precomputed so the DAX measures are fast aggregates) ──
    # is_product_line: a product line is priced by the standard base-price adjustment (has an A03 F4074
    # row) OR net-priced (user_reserved_code NP/N3), AND is NOT a freight line (F/FT) and NOT a
    # charge/dryer item.
    _charge_items = ["MISC BILLING", "EXPEDITE FEE", "BANKING FEE", "TRANSLOAD CHARGES",
                     "DRYER TAILINGS", "DRYER TAILING #1", "DRYER TAILING #40"]
    _line_w = Window.partitionBy("sales_order_line_key")
    df = df.withColumn("_has_a03",
                       F.max(F.when(F.trim(F.col("price_adjustment_type")) == "A03", F.lit(1)).otherwise(F.lit(0)))
                        .over(_line_w))
    _is_prod = ((((F.col("_has_a03") == 1) | F.trim(F.col("user_reserved_code")).isin("NP", "N3"))
                 & (~F.trim(F.col("line_type")).isin("F", "FT")))
                & (~F.trim(F.col("second_item_number")).isin(_charge_items)))
    df = (df.withColumn("is_product_line", F.when(_is_prod, F.lit("Y")).otherwise(F.lit("N")))
            .drop("_has_a03"))
    # freight_hide_amount: FRTHIDE adjustment priced by its own UOM = adj_unit_price × qty-in-ALUOM
    # (ordered_tons when adj_uom=TN, else the line's native transaction_quantity). Counted ONLY for the
    # ALUOM×SDUOM pairs (TM,BG),(TN,TN),(TN,BG),(BG,BG),(TM,TM); zero on all other rows/pairs.
    _au = F.trim(F.col("adj_uom")); _su = F.trim(F.col("uom"))
    _fh_pair = (((_au == "TM") & (_su == "BG")) | ((_au == "TN") & (_su == "TN"))
                | ((_au == "TN") & (_su == "BG")) | ((_au == "BG") & (_su == "BG"))
                | ((_au == "TM") & (_su == "TM")))
    df = df.withColumn("freight_hide_amount",
                       F.when((F.trim(F.col("price_adjustment_type")) == "FRTHIDE") & _fh_pair,
                              F.col("adj_unit_price").cast("double")
                              * F.when(_au == "TN", F.col("ordered_tons"))
                                 .otherwise(F.col("transaction_quantity").cast("double")))
                        .otherwise(F.lit(0.0)))

    # ── precomputed per-row bucket amounts so every DAX measure is a plain SUM(column) (no per-cell
    #    FILTER(fact) scans). ──
    _prod    = (F.col("is_product_line") == "Y") & (F.col("is_primary_line_row") == "Y")   # one product row per line
    _adjton  = F.col("adj_unit_price").cast("double") * F.col("ordered_tons")               # adjustment $ = ALUPRC × tons
    _pref3   = F.substring(F.trim(F.col("third_item_number")), 1, 3)                        # SDAITM prefix
    _frtline = (F.col("is_primary_line_row") == "Y") & F.trim(F.col("line_type")).isin("F", "FT")  # primary freight row
    df = (df
          # product line values (deduped) — Total Tons / Product Price
          .withColumn("product_ordered_tons", F.when(_prod, F.col("ordered_tons")).otherwise(F.lit(0.0)))
          .withColumn("product_ext_price",    F.when(_prod, F.col("extended_price").cast("double")).otherwise(F.lit(0.0)))
          # adjustment buckets by print code (ALUPRC × tons)
          .withColumn("non_product_amount",   F.when(F.trim(F.col("adj_print_code")) == "NON", _adjton).otherwise(F.lit(0.0)))
          .withColumn("al_severance_amount",  F.when(F.trim(F.col("adj_print_code")) == "ALA", _adjton).otherwise(F.lit(0.0)))
          .withColumn("misc_billing_amount",  F.when((F.trim(F.col("adj_print_code")) == "ACR")
                          | (F.substring(F.trim(F.col("price_adjustment_type")), 1, 2) == "PP"), _adjton).otherwise(F.lit(0.0)))
          # freight-line buckets by SDAITM prefix (extended_price on the primary freight row) + FRTTAX adj
          .withColumn("freight_amount",
                      F.when(_frtline & _pref3.isin("BIL", "FRE", "FUE", "TRA"), F.col("extended_price").cast("double")).otherwise(F.lit(0.0))
                      + F.when(F.trim(F.col("price_adjustment_type")).isin("FRTTAXN", "FRTTAXY"), _adjton).otherwise(F.lit(0.0)))
          .withColumn("car_charges_amount",
                      F.when(_frtline & (_pref3 == "RAI"), F.col("extended_price").cast("double")).otherwise(F.lit(0.0)))
          # dryer-freight bucket: freight line (F/FT) whose SDAITM prefix is DRY/Dry → extended_price
          .withColumn("dryer_freight_amount",
                      F.when(_frtline & _pref3.isin("DRY", "Dry"), F.col("extended_price").cast("double")).otherwise(F.lit(0.0))))

    return df.select("price_adjustment_key", "sales_order_line_key", *_PRICE_ADJ_BUSINESS_COLS)


# ════════════════════════════════════════════════════════════════════════════
# [FACT]  fact_price_adjustment  ·  spine: F4074  (no refs)
# Grain  : one row per stored F4074 adjustment (NOT fanned onto an F4211 line —
#          that is fact_sales_order_price_adjustment, above; this is the raw
#          adjustment ledger, decoded).  Relates to freight/price_adjustment via
#          sales_order_line_key, not a Gold-to-Gold join.
# Ported from D:\ussilica\Module 1 developer nbs\ESO1\
#   nb_silver_to_gold_eso1_fact_price_adjustment.py (added 2026-08-24) — the
#   developer's F4074 billable-freight columns moved OFF fact_sales_order_freight
#   and onto this table as a DAX measure target (see that fact's header note).
#
# price_adjustment_key is a row_number() over a window partitioned by
# sales_order_line_key (same formula fact_sales_order_price_adjustment already
# uses at :4339 — proven pattern, not new).  That means the numbering of every
# row in a line can shift when ANY sibling adjustment on that line is added or
# removed, so a partial CDF batch must NEVER be renumbered on its own — the
# whole line's F4074 rows have to be re-read and rebuilt together every time.
#
# THIS is why rebuild_from_silver=True is required HERE but not on
# fact_sales_order_price_adjustment: there, F4074 is a ref, and the ref path
# already re-reads the full Silver spine unconditionally.  Here F4074 IS the
# spine, so without the flag the spine path would use just the batch's live
# rows and renumber wrong.
#
# line_keys/spine_line_keys reuse FACT_PRICE_ADJ_GOLD_LINE_KEYS /
# FACT_PRICE_ADJ_SILVER_LINE_KEYS (:3992) unchanged — same line grain, same
# names, so no need to declare a second identical pair.  They are the LINE key,
# not price_adjustment_key: row_number() can renumber on an unrelated sibling
# change, so fact_key must be stable across a rebuild, which only the line key
# is.  fact_key therefore repeats across a line's adjustment rows on purpose —
# verify grain uniqueness on price_adjustment_key, not fact_key.
#
# The dev notebook's own select() only emits price_adjustment_key +
# sales_order_line_key + the 7 ADJ_COLS — it never exposes the 4 line-key
# components as their own columns.  They are added back here (mechanical only,
# same values already computed for sales_order_line_key) because the engine's
# fact_key hash needs them as real output columns, exactly like every sibling
# fact in this file already exposes its line key on Gold.
# ════════════════════════════════════════════════════════════════════════════
def build_fact_price_adjustment(spine):
    df = (
        spine
        .withColumn(
            "sales_order_line_key",
            sk("company_key_order_no", "order_type",
               "document_order_invoice_e", "line_number"),
        )
        .withColumn("price_adjustment_type", F.trim(F.col("price_adjustment_type")))
        .withColumn("adj_print_code", F.trim(F.col("pricing_report_code_01")))
        .withColumn("adj_unit_price", F.col("amt_price_per_unit_02").cast("double"))
        .withColumn("adj_uom", F.trim(F.col("uom_as_input")))
        .withColumn("adj_based_on_value", F.col("based_on_value").cast("double"))
        .withColumn("adj_gl_class", F.col("gl_class"))
        .withColumn("adj_factor_value", F.col("factor_value").cast("double"))
    )

    # per-adjustment surrogate key — same window as fact_sales_order_price_adjustment
    _ws = Window.partitionBy("sales_order_line_key").orderBy(
        F.col("price_adjustment_type").asc_nulls_first(),
        F.col("adj_unit_price").asc_nulls_first(),
        F.col("adj_gl_class").asc_nulls_first(),
    )
    df = (
        df.withColumn("_seq", F.row_number().over(_ws))
          .withColumn("price_adjustment_key", sk("sales_order_line_key", "_seq"))
          .drop("_seq")
    )

    return df.select(
        "price_adjustment_key",
        "company_key_order_no",
        "order_type",
        F.col("document_order_invoice_e").alias("order_number"),
        "line_number",
        "sales_order_line_key",
        "price_adjustment_type",
        "adj_print_code",
        "adj_unit_price",
        "adj_uom",
        "adj_based_on_value",
        "adj_gl_class",
        "adj_factor_value",
    )


# ============================================================================
# 8) MODULE REGISTRY
# ============================================================================
# fact_customer_ledger was REMOVED 2026-08-19 — it is a slow table and was
# dragging the shared module drain.  It now lives in its own notebook:
#   D:\ussilica\Dimension notebooks\nb_silver_to_gold_cdf_fact_customer_ledger.py
# Same Gold table name, same build logic, same DELETE+APPEND semantics; it just
# runs on its own schedule.  That notebook RESUMES FROM THIS MODULE'S CHECKPOINT
# (Files/checkpoints/module1/fact_customer_ledger/...), so do NOT delete that
# directory and do NOT re-add the fact here — two writers on one Gold table
# would collide.
MODULE_FACTS = {
    "fact_shipment_routing": {
        "fact": "fact_shipment_routing",
        "line_keys": FACT_SHIPMENT_ROUTING_LINE_KEYS,
        "spine": F4941,
        "spine_columns": [
            "shipment_number",
            "routing_step_number",
            "mode_of_transport",
            "origin_address_number",
            "address_number_deconsolida",
            "date_release_julian",
            "date_updated",  # RSUPMJ ─┐ jde_updated_ts
            "time_of_day",  # RSTDAY ─┘
        ],
        "ref_tables": set(),
        "build_fn": build_fact_shipment_routing,
    },
    "fact_sales_order_detail": {
        "fact": "fact_sales_order_detail",
        "line_keys": FACT_SHIPMENT_ORDER_DETAIL_GOLD_LINE_KEYS,
        "spine": F4211,
        "spine_line_keys": FACT_SHIPMENT_ORDER_DETAIL_SILVER_LINE_KEYS,
        "spine_columns": [
            "company_key_order_no",
            "document_order_invoice_e",
            "order_type",
            "line_number",
            "line_type",
            "transaction_originator",
            "shipment_number",
            "cost_center",
            "address_number",
            "address_number_ship_to",
            "status_code_last",
            "status_code_next",
            "date_promised_ship_julian",
            "date_requested_julian",
            "scheduled_pick_date",
            "date_release_julian",
            "identifier_second_item",
            "identifier_short_item",
            "uom_as_input",
            "freight_handling_code",
            "carrier",
            "mode_of_transport",
            "container_id",
            "reference_01",
            "units_transaction_qty",
            "uom_pricing",             # SDUOM4 (price cascade)
            "amt_price_per_unit_02",   # SDUPRC (Source Price)
            "address_number_parent",
            "date_updated",
            "time_of_day",
        ],
        "ref_tables": set(),
        # F41002/F41003 are read full via get_ref() inside _uom_cascades() (small UoM tables) —
        # NOT registered as refs, so no ref_spine_join_keys narrowing entry is needed.
        "build_fn": build_fact_shipment_order_detail,
    },
    "fact_extended_sales_order_8": {
        "fact": "fact_extended_sales_order_8",
        "line_keys": FACT_ESO8_GOLD_LINE_KEYS,
        "spine_line_keys": FACT_ESO8_SILVER_LINE_KEYS,
        "spine": F5542035,
        "spine_columns": [
            "document_order_invoice_e",
            "date_updated",
            "order_type",
            "company_key_order_no",
            "math_numeric_01",
            "description_001",
            "cost_center",
            "everest_event_point_02",
            "date_release_julian",
            "scheduled_pick_date",
            "edi_successfully_process",
            "message_text",
            "program_id",
            "user_id",
            "time_last_updated",
            "work_station_id",
            "shipment_number",
        ],
        "ref_tables": {F4201, F5642B01},
        "build_fn": build_fact_eso8,
        "ref_spine_join_keys": {
            F4201: ["document_order_invoice_e", "order_type", "company_key_order_no"],
            F5642B01: [
                "document_order_invoice_e",
                "shipment_number",
                "order_type",
                "company_key_order_no",
            ],
        },
    },
    "dim_order": {
        "fact": "dim_order",
        "line_keys": DIM_ORDER_GOLD_LINE_KEYS,
        "spine": F4201,
        "spine_line_keys": DIM_ORDER_SILVER_LINE_KEYS,
        "spine_columns": [
            "company_key_order_no",
            "document_order_invoice_e",
            "order_type",
            "address_number",
            "address_number_ship_to",
            "mode_of_transport",
            "date_requested_julian",
            "date_transaction_julian",
            "ordered_by",
            "order_taken_by",
            "date_updated",
            "hold_orders_code",
            "date_original_promisde",
            "time_of_day",  # SHTDAY — with date_updated -> jde_updated_ts
        ],
        "ref_tables": set(),
        "build_fn": build_dim_order,
    },
    "dim_shipment": {
        "fact": "dim_shipment",
        "line_keys": DIM_SHIPMENT_LINE_KEYS,
        "spine": F4215,
        "spine_columns": [
            "shipment_number",
            "shipment_status",
            "mode_of_transport",
            "carrier_01",
            "carrier_02",
            "carrier_03",
            "origin_address_number",
            "address_number",
            "address_number_ship_to",
            "cost_center",
            "city",
            "state",
            "country",
            "origin_city",
            "origin_state",
            "origin_country",
            "number_of_routing_steps",
            "shipment_weight",
            "date_requested_julian",
            "date_release_julian",
            "date_updated",
            "time_of_day",  # XHTDAY — with date_updated -> jde_updated_ts
        ],
        "ref_tables": set(),
        "build_fn": build_dim_shipment,
    },
    "fact_sales_order_freight": {
        # Limit CDF log versions per trigger for all ESO1 streams.
        # ESO1 is the largest fact (12.5 M Gold rows, 14 ref joins).  Without a
        # limit, the first streaming batch after a full load catches up on every
        # Silver change since the snapshot in one shot — potentially 10 K+ header
        # changes that trigger a spine rebuild of hundreds of thousands of rows.
        # Multiple ref-stream threads each cache their sub_batch while waiting for
        # the fact lock, accumulating executor heap pressure until OOM (exit 137).
        # 10 log-files per trigger caps each batch; normal 60 s triggers are
        # naturally small anyway, so this only affects catch-up bursts.
        # "max_files_per_trigger": 10,
        "fact": "fact_sales_order_freight",
        "line_keys": FACT_ESO1_GOLD_LINE_KEYS,
        "spine_line_keys": FACT_ESO1_SILVER_LINE_KEYS,
        "spine": F4211,
        # build_fact_eso1 only reads _ESO1_LINE_COLS from filtered_spine (for line keys),
        # then re-reads F4211/F42119 directly.  Remaining cols needed by the ref-filter
        # loop in generic_recompute (one per ref_spine_join_keys entry).
        "spine_columns": [
            "company_key_order_no",
            "order_type",
            "document_order_invoice_e",
            "line_number",
            "shipment_number",
            "identifier_short_item",
            "address_number",
            "identifier_second_item",
        ],
        # F42119 (Sales Order History) is a second spine — same line-key schema as F4211.
        # The engine opens a CDF stream on it and routes its events through the same
        # handler as F4211. build_fn unions both sources filtered to batch line keys.
        "extra_spines": [F42119],
        "ref_tables": {F4201, F4981, F5642B01, F5642B11},
        # Passive refs: read fresh on every rebuild but no CDF stream opened.
        # Changes to these tables propagate the next time a fast table (spine or
        # active ref) triggers a rebuild for the same spine lines.
        # F4101 added 2026-08-24 — item master IMUOM1, the pivot for the fuller
        # item+standard UoM cascade (see build_fact_eso1's price-conversion block,
        # same F4101 role already established on fact_sales_order_price_adjustment).
        "passive_refs": {F4941, F41002, F5549002, F03012, F49211, F4106, F4101},
        "build_fn": build_fact_eso1,
        "ref_spine_join_keys": {
            F4201: ["company_key_order_no", "document_order_invoice_e", "order_type"],
            F4941: ["shipment_number"],
            F4981: ["shipment_number"],
            F5642B01: [
                "company_key_order_no",
                "document_order_invoice_e",
                "order_type",
                "shipment_number",
            ],
            F5642B11: [
                "company_key_order_no",
                "document_order_invoice_e",
                "order_type",
                "line_number",
                "shipment_number",
            ],
            F41002: ["identifier_short_item"],
            F5549002: [
                "company_key_order_no",
                "document_order_invoice_e",
                "order_type",
                "line_number",
            ],
            F03012: ["address_number"],
            F49211: [
                "company_key_order_no",
                "document_order_invoice_e",
                "order_type",
                "line_number",
            ],
            # F4106: filter by second_item_number (= SDLITM) → F4106.identifier_2nd_item
            F4106: [("identifier_second_item", "identifier_2nd_item")],
            F4101: ["identifier_short_item"],
        },
    },
    "fact_custom_sales_order_2": {
        "fact": "fact_custom_sales_order_2",
        "line_keys": FACT_CSO2_LINE_KEYS,
        "spine": F4211,
        "spine_line_keys": FACT_CSO2_SILVER_LINE_KEYS,
        "spine_columns": [
            "company_key_order_no",
            "line_number",
            "document_order_invoice_e",
            "order_type",
            "cost_center",
            "status_code_next",
            "status_code_last",
            "doc_voucher_invoice_e",
            "identifier_second_item",
            "address_number",
            "address_number_ship_to",
            "line_type",
            "date_updated",  # SDUPMJ ─┐ jde_updated_ts
            "time_of_day",  # SDTDAY ─┘
        ],
        "ref_tables": set(),
        # F4104 is an anti-join gate (NOT EXISTS) and a slow table — no CDF stream.
        # Changes propagate the next time F4211 (fast spine) fires for the same item.
        # When a cross-reference is added for an item, all F4211 lines with that item
        # must be rebuilt — they may disappear from Gold.  When removed, they may appear.
        "passive_refs": {F4104},
        "build_fn": build_fact_cso2,
        "ref_spine_join_keys": {
            F4104: [("identifier_second_item", "identifier_2nd_item")],
        },
    },
    "fact_extended_sales_order_7_v2": {
        "fact": "fact_extended_sales_order_7_v2",
        "line_keys": FACT_ESO7_V2_GOLD_LINE_KEYS,
        "spine_line_keys": FACT_ESO7_V2_SILVER_LINE_KEYS,
        "spine": F4211,
        "spine_columns": [
            "company_key_order_no",
            "document_order_invoice_e",
            "order_type",
            "line_number",
            "line_type",
            "order_suffix",
            "transaction_originator",
            "shipment_number",
            "cost_center",
            "address_number",
            "address_number_ship_to",
            "status_code_last",
            "status_code_next",
            "date_promised_ship_julian",
            "date_requested_julian",
            "scheduled_pick_date",
            "date_release_julian",
            "identifier_second_item",
            "identifier_short_item",
            "uom_as_input",
            "freight_handling_code",
            "carrier",
            "mode_of_transport",
            "container_id",
            "reference_01",
            "units_transaction_qty",
            "uom_pricing",  # SDUOM4 — Source UoM (Price)
            "amt_price_per_unit_02",  # SDUPRC — Source Price
            "date_updated",
            "time_of_day",
            "address_number_parent",  # SDPA8
        ],
        # All four are active.  Each one changes a value that is the POINT of this
        # report, and every join is LEFT — so without its own stream the column
        # would stay stale until F4211 happened to fire for the same line:
        #   F4941    is_missing_freight flips when a freight rate finally arrives
        #   F5642B01 booking/vessel/port filled in after order entry
        #   F5642B11 seal + production detail, same lifecycle as freight's usage
        #   F4201    hold_code changes independently of the line
        "ref_tables": {F5642B01, F5642B11, F4201, F4941},
        "build_fn": build_fact_extended_sales_order_7_v2,
        "ref_spine_join_keys": {
            F5642B01: [
                "shipment_number",
                "company_key_order_no",
                "order_type",
                "document_order_invoice_e",
            ],
            F5642B11: [
                "company_key_order_no",
                "document_order_invoice_e",
                "order_type",
                "line_number",
                "shipment_number",
            ],
            # Four parts, not the usual three — this fact joins F4201 on
            # order_suffix as well.
            F4201: [
                "company_key_order_no",
                "document_order_invoice_e",
                "order_type",
                "order_suffix",
            ],
            F4941: ["shipment_number"],
        },
    },
    "fact_sales_order_price_adjustment": {
        "fact": "fact_sales_order_price_adjustment",
        "line_keys": FACT_PRICE_ADJ_GOLD_LINE_KEYS,
        "spine_line_keys": FACT_PRICE_ADJ_SILVER_LINE_KEYS,
        "spine": F4211,
        "spine_columns": [
            # ── order / line identifiers ──
            "company",
            "company_key_order_no",
            "order_type",
            "document_order_invoice_e",
            "doc_voucher_invoice_e",
            "line_number",
            "shipment_number",
            # ── status / handling / transport ──
            "status_code_last",
            "status_code_next",
            "freight_handling_code",
            "carrier",
            "mode_of_transport",
            "container_id",
            "reference_01",
            "gl_class",
            "line_type",
            "payment_terms_code_01",
            "sales_reporting_code_01",
            "sales_reporting_code_02",
            "sales_reporting_code_03",
            "sales_reporting_code_04",
            "sales_reporting_code_05",
            # ── address / dimension FKs ──
            "address_number_ship_to",
            "address_number",
            "address_number_parent",
            "cost_center",
            "identifier_short_item",
            "identifier_second_item",
            "identifier_third_item",
            # ── raw event dates ──
            "date_transaction_julian",
            "date_requested_julian",
            "actual_ship_date",
            "date_invoice_julian",
            "dt_for_gl_and_vouch_01",
            # ── uom / tons ──
            "uom_as_input",
            "uom_pricing",  # SDUOM4 — added 2026-08-24 for the price-side conversion
            # ── line measures ──
            "units_transaction_qty",
            "units_quantity_shipped",
            "units_primary_qty_order",
            "amount_extended_price",
            "amount_extended_cost",
            "amt_price_per_unit_02",
            "currency_code_base",
            # ── misc display / filter ──
            "location",
            "lot",
            "user_reserved_code",
            "user_reserved_number",
            "user_reserved_reference",
            "price_override_code",
            "price_adjustment_schedule_n",
            "transaction_originator",
            "user_id",
            "date_updated",
            "time_of_day",
            "zone_number",
        ],
        # F4074 is the FAN-OUT source — adding or removing an adjustment changes the
        # number of Gold rows for a line, so it must fire its own rebuild.  F4201
        # carries the hold code and churns, same call as ESO1 makes.
        "ref_tables": {F4074, F4201},
        # Slow-moving attribute sources.  Changes propagate the next time F4211 or
        # F4074 fires for the same line — same classification ESO1 gives F41002/F49211.
        "passive_refs": {F4101, F41002, F49211},
        "build_fn": build_fact_sales_order_price_adjustment,
        "ref_spine_join_keys": {
            F4074: [
                "company_key_order_no",
                "order_type",
                "document_order_invoice_e",
                "line_number",
            ],
            F4201: [
                "company_key_order_no",
                "order_type",
                "document_order_invoice_e",
            ],
            F4101: ["identifier_short_item"],
            F41002: ["identifier_short_item"],
            F49211: [
                "company_key_order_no",
                "order_type",
                "document_order_invoice_e",
                "line_number",
            ],
        },
        # F41003 is deliberately absent from all three keys above — it has no
        # spine-derived narrowing column.  _price_adj_conv_lookups reads it directly.
    },
    "fact_price_adjustment": {
        "fact": "fact_price_adjustment",
        "line_keys": FACT_PRICE_ADJ_GOLD_LINE_KEYS,
        "spine_line_keys": FACT_PRICE_ADJ_SILVER_LINE_KEYS,
        "spine": F4074,
        "spine_columns": [
            "company_key_order_no", "order_type", "document_order_invoice_e",
            "line_number", "price_adjustment_type", "pricing_report_code_01",
            "amt_price_per_unit_02", "uom_as_input", "based_on_value",
            "gl_class", "factor_value",
        ],
        # See the [FACT] header above build_fact_price_adjustment for why this
        # is required here and NOT on fact_sales_order_price_adjustment.
        "rebuild_from_silver": True,
        "build_fn": build_fact_price_adjustment,
        "ref_spine_join_keys": {},
    },
    "fact_extended_sales_order_6": {
        "fact": "fact_extended_sales_order_6",
        "line_keys": FACT_ESO6_LINE_KEYS,
        "spine": F4211,
        "spine_columns": [
            "company_key_order_no",
            "document_order_invoice_e",
            "order_type",
            "line_number",
            "shipment_number",
            "line_type",
            "status_code_next",
            "status_code_last",
            "cost_center",
            "units_transaction_qty",
            "date_updated",  # SDUPMJ ─┐ jde_updated_ts
            "time_of_day",  # SDTDAY ─┘
        ],
        # F4941 MUST stay an active ref since inner join is used.
        "ref_tables": {F4941, F5642B01},
        "build_fn": build_fact_extended_sales_order_6,
        "ref_spine_join_keys": {
            F4941: ["shipment_number"],
            F5642B01: [
                "shipment_number",
                "company_key_order_no",
                "order_type",
                "document_order_invoice_e",
            ],
        },
    },
    "fact_extended_sales_order_2": {
        "fact": "fact_extended_sales_order_2",
        "line_keys": FACT_ESO2_GOLD_LINE_KEYS,
        "spine_line_keys": FACT_ESO2_SILVER_LINE_KEYS,
        "spine": F4211,
        "spine_columns": [
            "company_key_order_no",
            "document_order_invoice_e",
            "order_type",
            "line_number",
            "address_number_ship_to",
            "identifier_short_item",
            "uom_as_input",
            "units_transaction_qty",
            "uom_pricing",  # SDUOM4 — Source UoM (Price)
            "amt_price_per_unit_02",  # SDUPRC — Source Price
            "units_primary_qty_order",
            "actual_ship_date",
            "reference_01",
            "description_line_01",
            "user_reserved_number",
            "pull_signal",
            "doc_voucher_invoice_e",
            "container_id",
            "reference_02_vendor",
            "status_code_last",
            "status_code_next",
            "original_order_type",
            "line_type",
            "identifier_second_item",
            "cost_center",
            "date_requested_julian",
            "date_invoice_julian",
            "dt_for_gl_and_vouch_01",
            "address_number_parent",
            "date_updated",  # SDUPMJ ─┐ jde_updated_ts
            "time_of_day",  # SDTDAY ─┘
        ],
        "ref_tables": {F4201, F0010, F0101},
        "passive_refs": {F41002, F5549002},
        "build_fn": build_fact_extended_sales_order_2,
        "ref_spine_join_keys": {
            F0010: [("company_key_order_no", "company")],
            F0101: [("address_number_ship_to", "address_number")],
            F5549002: [
                "company_key_order_no",
                "document_order_invoice_e",
                "order_type",
                "line_number",
            ],
            F4201: ["company_key_order_no", "document_order_invoice_e", "order_type"],
            F41002: ["identifier_short_item"],
        },
    },
    "fact_extended_sales_order_3": {
        "fact": "fact_extended_sales_order_3",
        "line_keys": FACT_ESO3_LINE_KEYS,
        "spine": F4215,
        "spine_columns": [
            "shipment_number",
            "shipment_status",
            "cost_center",
            "freight_handling_code",
            "mode_of_transport",
            "state",
            "user_reserved_reference",
            "date_updated",
            "time_of_day",
            "user_id",
        ],
        "ref_tables": {F4211},
        "build_fn": build_fact_extended_sales_order_3,
        "ref_spine_join_keys": {
            F4211: ["shipment_number"],
        },
    },
    "dim_invoice_reconciliation": {
        "fact": "dim_invoice_reconciliation",
        "line_keys": DIM_INVOICE_RECONCILIATION_GOLD_LINE_KEYS,
        "spine_line_keys": DIM_INVOICE_RECONCILIATION_SILVER_LINE_KEYS,
        "spine": F4211,
        # The ONLY coarse-grain fact in this module: its grain is the INVOICE, while
        # the spine is the F4211 LINE.  Deleting one line must rebuild the invoice
        # from the surviving lines, never remove it — so the rebuild has to come from
        # Silver, not from the batch's live rows.  See the note in the handler.
        "rebuild_from_silver": True,
        "spine_columns": [
            "company_key",
            "doc_voucher_invoice_e",
            "document_type",
            "company_key_order_no",
            "document_order_invoice_e",
            "order_type",
            "line_number",
            "cost_center",
            "address_number",
            "address_number_ship_to",
            "address_number_parent",
            "tax_explanation_code_01",
            "dt_for_gl_and_vouch_01",
            "date_updated", 
            "time_of_day",
        ],
        "passive_refs": {F0006, F0101, F0116},
        "build_fn": build_dim_invoice_reconciliation,
        "ref_spine_join_keys": {
            F0006: ["cost_center"],
            F0101: [("address_number_ship_to", "address_number")],
            F0116: [("address_number_ship_to", "address_number")],
        },
    },
    "dim_order_number": {
        "fact": "dim_order_number",
        "line_keys": DIM_ORDER_NUMBER_GOLD_LINE_KEYS,
        "spine_line_keys": DIM_ORDER_NUMBER_SILVER_LINE_KEYS,
        "spine": F4211,
        "extra_spines": [F42119],
        # COARSE grain, like dim_invoice_reconciliation: the grain is the ORDER
        # while the spine is the order LINE.  Deleting one line of a 10-line order
        # must NOT delete the order — so the rebuild comes from Silver, not from
        # the batch's live rows.  See the note in the handler (Rule 4d).
        "rebuild_from_silver": True,
        "spine_columns": _ORDER_NUMBER_SPINE_COLS,
        # No joins: the dim is the order number plus flags computed from a
        # literal list, so there is nothing to register and nothing to go stale.
        "ref_tables": set(),
        "passive_refs": set(),
        "ref_spine_join_keys": {},
        "build_fn": build_dim_order_number,
    },
    "dim_second_item": {
        "fact": "dim_second_item",
        "line_keys": DIM_SECOND_ITEM_GOLD_LINE_KEYS,
        "spine_line_keys": DIM_SECOND_ITEM_SILVER_LINE_KEYS,
        "spine": F4211,
        "extra_spines": [F42119],
        # COARSE grain, like dim_order_number: the grain is the ITEM CODE while
        # the spine is the order LINE.  Deleting one line that carries an item
        # code must NOT delete the code while other lines still use it — so the
        # rebuild comes from Silver, not from the batch's live rows.  This is
        # also what keeps a code alive once its last open-order line is purged
        # into F42119: the re-read unions the history table.  (Rule 4d.)
        "rebuild_from_silver": True,
        "spine_columns": _SECOND_ITEM_SPINE_COLS,
        # No joins: the dim is the item code plus flags computed from literal
        # lists, so there is nothing to register and nothing to go stale.
        "ref_tables": set(),
        "passive_refs": set(),
        "ref_spine_join_keys": {},
        "build_fn": build_dim_second_item,
    },
    "fact_sales_commission": {
        # "max_files_per_trigger": 10,
        "fact": "fact_sales_commission",
        "line_keys": FACT_COMMISSION_GOLD_LINE_KEYS,
        "spine_line_keys": FACT_COMMISSION_SILVER_LINE_KEYS,
        "spine": F4211,
        "extra_spines": [F42119],
        # build_fn re-reads F4211/F42119 itself, so the spine only supplies line keys
        # plus the columns the engine needs for ref narrowing.
        "spine_columns": [
            "company_key_order_no", "order_type", "document_order_invoice_e",
            "line_number", "address_number",
            # narrowing key for F41002 only — build_fn takes the item off its own
            # F4211/F42119 re-read, not from here.
            "identifier_short_item",
        ],
        # F42005 MUST be active: commissions are written AFTER the order completes,
        # so the F4211 line may never change again.  Passive = new commissions never
        # reach Gold.
        "ref_tables": {F42005},
        # F4074 is PASSIVE here but ACTIVE on fact_sales_order_price_adjustment — the
        # difference is what it does.  There it drives the fan-out, so an adjustment
        # appearing changes the ROW COUNT and must fire a rebuild.  Here it is
        # aggregated to one row per line and supplies a single display attribute, so a
        # stale code costs a wrong label, not a wrong row set.  Promote it to
        # ref_tables if the report needs live adjustment codes.
        # F41002 (UMUSTR) is an item attribute that effectively never changes — same
        # call ESO1 makes.
        "passive_refs": {F4201, F0101, F4074, F41002},
        "build_fn": build_fact_sales_commission,
        "ref_spine_join_keys": {
            F42005: ["company_key_order_no", "order_type",
                     "document_order_invoice_e", "line_number"],
            F4201:  ["company_key_order_no", "order_type", "document_order_invoice_e"],
            F0101:  ["address_number"],
            # Narrowed on exactly the 4 columns padj_line groups by, so every group
            # the semi-join keeps is complete (Rule 4b corollary).
            F4074:  ["company_key_order_no", "order_type",
                     "document_order_invoice_e", "line_number"],
            # Narrowed on the column uom_str groups by — complete groups per item.
            F41002: ["identifier_short_item"],
        },
    },
    "fact_extended_sales_order_5": {
        "fact": "fact_extended_sales_order_5",
        "line_keys": FACT_ESO5_GOLD_LINE_KEYS,
        "spine_line_keys": FACT_ESO5_SILVER_LINE_KEYS,
        "spine": F4211,
        # ── TWO SPINES.  This is what makes ESO5 work under v3 at all. ────────
        # Leg B is F4311 rows, and 386,894 of them (14.9%) have NO F4211 row
        # anywhere — 386,878 PO_OTHER plus 16 PO_HOLADD, which report 4 reads.
        # With F4211 as the only spine those rows have no fact_key: the
        # developer's LEFT join hashes them to MD5("") and they append forever,
        # and forcing the join to INNER drops them.  Registering F4311 as a
        # second spine gives every PO row a key from its OWN table, so the
        # developer's LEFT join is preserved EXACTLY and all 2,589,334 PO rows
        # are maintained.
        #
        # This is only sound because both tables stringify the key identically.
        # VERIFIED against Silver 2026-08-12: 2,148,798 of 2,148,798 shared
        # loads matched, "00750" and "1492199.000000000000000000" on both sides.
        # The engine hashes CAST(col AS STRING) on the RAW value, so if Silver
        # ever re-types DOCO on one table and not the other, the two streams
        # produce different keys for the same load and the PO rows start
        # duplicating.  Re-run that check before any Silver retype.
        "extra_spines": [F4311],
        # build_fn re-reads both spines itself (whole loads are required — see
        # _eso5_load_aggregates), so the spine only supplies the load keys.
        # Both F4211 and F4311 carry these two columns, which is what lets one
        # handler serve both streams.
        "spine_columns": FACT_ESO5_SILVER_LINE_KEYS,
        # No live ref streams — two spines is already this fact's CU cost, and
        # every lookup below refreshes the next time its load changes.  Promote
        # F43121 to ref_tables if PO receipts must appear without a load touch.
        "ref_tables": set(),
        # F41002/F41003 read full via get_ref() inside _uom_cascades() (small UoM tables), NOT registered
        # as refs — the spine here carries only load keys (no item col to narrow on) anyway.
        "passive_refs": {F554201T, F43121, F4201},
        "ref_spine_join_keys": {
            F554201T: ["company_key_order_no", "document_order_invoice_e"],
            F43121: ["company_key_order_no", "document_order_invoice_e"],
            F4201: ["company_key_order_no", "document_order_invoice_e"],
        },
        # F0911 / F0101 / F0005 are NOT registered: they key on voucher document,
        # vendor and UDC respectively, so there is no load key for the engine to
        # narrow them on.  build_fn reads them directly and narrows each one at
        # source instead (see the WHERE pushdowns).
        "build_fn": build_fact_extended_sales_order_5,
    },
}

# Auto-derive one stream per (fact × Silver table).
# extra_spines are treated as spine streams (is_ref=False).
MODULE_STREAMS_PER_FACT = {}
for _fc_fact, _fc in MODULE_FACTS.items():
    _streams = {_fc["spine"]: [False]}
    for _extra in _fc.get("extra_spines", []):
        _streams[_extra] = [False]
    for _ref_tbl in _fc.get("ref_tables", set()):
        _streams[_ref_tbl] = [True]
    MODULE_STREAMS_PER_FACT[_fc_fact] = _streams

_TOTAL_STREAMS = sum(len(v) for v in MODULE_STREAMS_PER_FACT.values())


# ============================================================================
# 9) RUN
# ============================================================================


def _ckpt_path(fact_name, silver_tbl):
    return "{}/{}/{}".format(CKPT_ROOT, fact_name, silver_tbl)


def _checkpoints_exist(path):
    try:
        return bool(mssparkutils.fs.ls(path))
    except Exception:
        return False


# ── Stop any leftover streams from a previous cell run ───────────────────────
_all_stream_names = {
    "m1_{}_{}".format(ft, tbl)
    for ft, tbls in MODULE_STREAMS_PER_FACT.items()
    for tbl in tbls
}
for _q in list(spark.streams.active):
    if _q.name in _all_stream_names:
        _q.stop()
        print("Stopped leftover stream: {}".format(_q.name))

# ── Per-fact: full load or resume ─────────────────────────────────────────────
_fact_init_ver = {}

for _fact_key, _fc in MODULE_FACTS.items():
    _fact = _fc["fact"]
    _spine = _fc["spine"]
    _unique_silver = list(MODULE_STREAMS_PER_FACT[_fact_key].keys())

    _fact_ckpts_ok = all(
        _checkpoints_exist(_ckpt_path(_fact, t)) for t in _unique_silver
    )
    _needs_full_load = not spark.catalog.tableExists(gname(_fact)) or not _fact_ckpts_ok

    if _needs_full_load:
        print("== [{}] FULL LOAD ==".format(_fact))

        _iv = {t: current_version(t) for t in _unique_silver}
        _spine_iv = _iv[_spine]

        _thread_refs.cache = {}
        try:
            # Cache ref tables AND extra_spines at their snapshot versions so
            # build_fn can call get_ref() for any of them during the full load.
            for _ref_tbl in list(_fc.get("ref_tables") or []) + list(
                _fc.get("extra_spines") or []
            ):
                _thread_refs.cache[_ref_tbl] = drop_deleted(
                    spark.read.format("delta")
                    .option("versionAsOf", _iv[_ref_tbl])
                    .table(sname(_ref_tbl))
                )
            _fl_line_keys = _fc["line_keys"]
            _new = (
                _fc["build_fn"](
                    spark.read.format("delta")
                    .option("versionAsOf", _spine_iv)
                    .table(sname(_spine))
                )
                .withColumn(
                    "fact_key",
                    F.md5(
                        F.concat_ws(
                            "||", *[F.col(k).cast("string") for k in _fl_line_keys]
                        )
                    ),
                )
                .withColumn("last_update_date_time_utc", F.current_timestamp())
            )
        finally:
            _thread_refs.cache = None

        (
            _new.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("delta.enableChangeDataFeed", "true")
            .saveAsTable(gname(_fact))
        )
        print(
            "  [{}] {} rows={:,}".format(
                _fact, _fact, spark.table(gname(_fact)).count()
            )
        )
        _fact_init_ver[_fact] = _iv
        print("  [{}] init versions: {}".format(_fact, _iv))

        _fact_ckpt_dir = "{}/{}".format(CKPT_ROOT, _fact)
        if _checkpoints_exist(_fact_ckpt_dir):
            try:
                mssparkutils.fs.rm(_fact_ckpt_dir, True)
                print("  [{}] checkpoints cleared".format(_fact))
            except Exception as _e:
                print("  [{}] checkpoint clear failed: {}".format(_fact, _e))
    else:
        print("== [{}] resuming from checkpoint ==".format(_fact))
        _fact_init_ver[_fact] = {}


# ── Launch streams ─────────────────────────────────────────────────────────────
_stream_registry = {}
_active_queries = []


# All three mean "the stored offset is no longer readable" — NOT "data was skipped":
#   DELTA_INVALID_CDC_RANGE                        — no new Silver data since last run;
#       Spark computes next-batch range [N+1, N] → start > end → error.
#   DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE — Silver table was DROP+CREATE'd;
#       the UUID in the checkpoint no longer matches the table.
#   non-existent version N                         — Delta log compacted between runs;
#       the version the checkpoint references no longer exists on disk.
# Fix for all three: delete the checkpoint so startingVersion="latest" takes effect.
_AUTO_HEAL_MARKERS = (
    "DELTA_INVALID_CDC_RANGE",
    "DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE",
    "non-existent version",
)


def _is_auto_heal(msg):
    return any(m in msg for m in _AUTO_HEAL_MARKERS)


# ⚠ The three markers above are NOT equivalent, and treating them as one is what
# caused the full-load / resume alternation under RUN_MODE="available_now":
#
#   DELTA_INVALID_CDC_RANGE  means "NO NEW DATA since the checkpoint".  Under a
#     scheduled drain that is the NORMAL outcome for any slow-moving ref — F4201,
#     F5642B01, F4941 and friends hit it on most runs.  The checkpoint is still
#     perfectly valid and MUST BE KEPT.  Clearing it makes _checkpoints_exist()
#     False, which makes _needs_full_load True, which full-loads the whole fact
#     on the NEXT run (~34 min for ESO1) — and then a quiet ref does it again.
#     That is the loop: resume → quiet ref clears a checkpoint → full load →
#     resume → ...
#
#   The other two mean the stored offset is genuinely UNREADABLE (table was
#     DROP+CREATE'd, or the Delta log was compacted past the stored version).
#     Only these justify deleting the checkpoint.
#
# In STREAMING mode all three must still clear, because an always-on query that
# keeps its checkpoint would restart straight back into the same error forever.
# The monitoring loop therefore keeps using _is_auto_heal() unchanged.
_UNREADABLE_OFFSET_MARKERS = (
    "DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE",
    "non-existent version",
)


def _is_no_new_data(msg):
    return "DELTA_INVALID_CDC_RANGE" in msg


def _is_unreadable_offset(msg):
    return any(m in msg for m in _UNREADABLE_OFFSET_MARKERS)


# ⚠ "non-existent version N" is AMBIGUOUS — it is raised for two opposite causes,
# and telling them apart is the difference between a 5-second no-op and a 34-minute
# full load:
#
#   N is AHEAD of the table   → same meaning as DELTA_INVALID_CDC_RANGE: no new data.
#       Delta only writes its own internal checkpoint every 10 commits, so on a
#       near-static table (F42119 sits at version 3, F42005 at 20) the range planner
#       has no checkpoint to consult, falls through to getSnapshotAt(N), and reports
#       file-not-found instead of the tidy DELTA_INVALID_CDC_RANGE.  Same condition,
#       different code path.  The checkpoint is VALID — keep it.
#
#   N is BEHIND the table     → the Delta log really was compacted/VACUUMed past the
#       stored offset.  The checkpoint is unusable — clear it.
#
# So compare N against the table's current version instead of guessing from the text.
_VERSION_IN_MSG = re.compile(r"non-existent version[^0-9]*([0-9]+)")


def _table_version(silver_tbl):
    """Current Delta version of a Silver table, or None if it can't be read."""
    try:
        return DeltaTable.forName(spark, sname(silver_tbl)).history(1).collect()[0][
            "version"
        ]
    except Exception:
        return None


def _asks_for_future_version(msg, silver_tbl):
    """True when the offset the checkpoint wants simply hasn't been written yet."""
    _m = _VERSION_IN_MSG.search(msg)
    if _m is None:
        return False
    _latest = _table_version(silver_tbl)
    return _latest is not None and int(_m.group(1)) > _latest


def _clear_checkpoint(fact_name, silver_tbl, stream_name):
    try:
        mssparkutils.fs.rm(_ckpt_path(fact_name, silver_tbl), True)
        print("  [{}] auto-heal: checkpoint cleared".format(stream_name))
    except Exception as _ce:
        print("  [{}] checkpoint clear failed: {}".format(stream_name, _ce))


def _start_stream(fc, silver_tbl, is_ref, init_ver):
    """Start one CDF stream for (fc, silver_tbl) and register it.

    Trigger depends on RUN_MODE: availableNow drains the backlog and terminates;
    processingTime runs forever.  Everything else — startingVersion, the checkpoint
    location, maxFilesPerTrigger — is identical in both modes.
    """
    fact_name = fc["fact"]
    stream_name = "m1_{}_{}".format(fact_name, silver_tbl)

    # ⚠ startingVersion is passed ONLY when starting fresh — NEVER on a resume.
    #
    # The old code always passed it, on the documented assumption that Delta ignores
    # it once a checkpoint exists.  THAT ASSUMPTION IS FALSE IN THIS RUNTIME, and it
    # silently broke every incremental read from day one.  Evidence, 2026-08-13:
    #
    #   [DELTA_INVALID_CDC_RANGE] CDC range from start 4377 to end 4369 was invalid.
    #
    # start 4377 = the table's current version (i.e. "latest"), identical across every
    # stream on that table; end 4369 = this stream's own checkpoint position, different
    # per stream.  Delta was being asked to read BACKWARDS from latest to the
    # checkpoint, so every resumed stream failed and processed nothing.  It looked
    # harmless because the old handler read the failure as "no new data".
    #
    # Three cases, and only the first two may set the option:
    #   Full load        — no checkpoint yet; _sv is the snapshot version.
    #   Checkpoint gone  — cleared by the auto-heal; "latest" makes the stream idle
    #                      until new data arrives instead of replaying from v0.
    #   Resume           — a checkpoint exists; it is the ONLY source of position.
    _sv = _fact_init_ver.get(fact_name, {}).get(silver_tbl)
    _ckpt = _ckpt_path(fact_name, silver_tbl)
    _resuming = _checkpoints_exist(_ckpt)

    _reader = spark.readStream.format("delta").option("readChangeFeed", "true")
    if _resuming:
        sv = "checkpoint"
    else:
        sv = _sv if _sv is not None else "latest"
        _reader = _reader.option("startingVersion", sv)
    _mfpt = fc.get("max_files_per_trigger")
    if _mfpt is not None:
        _reader = _reader.option("maxFilesPerTrigger", str(_mfpt))

    _writer = (
        _reader.table(sname(silver_tbl))
        .writeStream.foreachBatch(
            make_per_fact_handler(fc, silver_tbl, is_ref, init_ver, stream_name)
        )
        .option("checkpointLocation", _ckpt)
        .queryName(stream_name)
    )
    if RUN_MODE == "available_now":
        _writer = _writer.trigger(availableNow=True)
    else:
        _writer = _writer.trigger(processingTime=TRIGGER)

    q = _writer.start()
    _stream_registry[stream_name] = (fc, silver_tbl, is_ref, init_ver)
    print("  [{}] started  sv={}".format(stream_name, sv))
    return q


# ── AvailableNow drainer ──────────────────────────────────────────────────────
_failures = []
_failures_lock = threading.Lock()


def _record_failure(stream_name, phase, msg):
    with _failures_lock:
        _failures.append((stream_name, phase, msg))


def _drain_stream(fc, silver_tbl, is_ref, init_ver):
    """Start one AvailableNow stream and block until it drains.

    Records failures instead of raising, so one bad stream doesn't abandon the rest
    of the run.  _run_available_now() raises at the end if anything failed.
    """
    fact_name = fc["fact"]
    stream_name = "m1_{}_{}".format(fact_name, silver_tbl)
    t0 = time.time()

    try:
        q = _start_stream(fc, silver_tbl, is_ref, init_ver)
    except Exception as _se:
        print("  [{}] START FAILED: {}".format(stream_name, _se))
        _record_failure(stream_name, "start", repr(_se)[:400])
        return

    try:
        q.awaitTermination()
        print("  [{}] drained {:.1f}s".format(stream_name, time.time() - t0))
    except Exception as _re:
        _msg = str(_re)
        if _is_no_new_data(_msg) or _asks_for_future_version(_msg, silver_tbl):
            # Nothing arrived in Silver since the last drain.  The checkpoint is
            # valid and is KEPT — clearing it would force a full load next run.
            # The raw message is printed too: this branch is a JUDGEMENT about an
            # exception, and hiding the evidence makes a wrong judgement invisible.
            print("  [{}] no new data — checkpoint kept".format(stream_name))
            print("      reason: {}".format(_msg[:300].replace("\n", " ")))
        elif "non-existent version" in _msg:
            # Marker present but the version could not be compared against the table
            # (unparseable message, or DESCRIBE HISTORY failed).  Do NOT guess: keeping
            # a good checkpoint costs one loud failure, clearing a good one costs a
            # full load on every future run.  Fail loudly and let a human decide.
            print("  [{}] UNRESOLVED OFFSET — checkpoint kept, not cleared".format(
                stream_name))
            print("      if this repeats, delete {} by hand".format(
                _ckpt_path(fact_name, silver_tbl)))
            _record_failure(stream_name, "offset", _msg[:400])
        elif _is_unreadable_offset(_msg):
            # UUID mismatch — the Silver table was DROP+CREATE'd, so the stored offset
            # points at a table that no longer exists.  Clear it; the next run full-loads
            # the fact, which is the correct recovery.  Nothing is lost.
            _clear_checkpoint(fact_name, silver_tbl, stream_name)
        else:
            print("  [{}] FAILED: {}".format(stream_name, _msg[:400]))
            _record_failure(stream_name, "run", _msg[:400])


_heavy_sem = threading.Semaphore(HEAVY_PARALLEL)


def _drain_fact(fact_name):
    """Drain every stream of ONE fact, serially.

    The unit of parallelism is the fact, not the stream: each fact writes its
    own Gold table, so two facts can never conflict in Delta, and keeping a
    fact's own streams serial means its spine and ref streams can never overlap.
    """
    fc = MODULE_FACTS[fact_name]
    tbl_groups = MODULE_STREAMS_PER_FACT[fact_name]
    is_heavy = fact_name in _HEAVY_FACTS

    if is_heavy:
        _heavy_sem.acquire()
    try:
        print("== [{}] {} stream(s){} ==".format(
            fc["fact"], len(tbl_groups), " [heavy]" if is_heavy else ""))
        for silver_tbl, flags in tbl_groups.items():
            iv = _fact_init_ver.get(fact_name, {}).get(silver_tbl, -1)
            _drain_stream(fc, silver_tbl, flags[0], iv)
    finally:
        if is_heavy:
            _heavy_sem.release()


# Longest-first.  A worker pool has no barrier, so the only way to end up with
# one worker running alone at the tail is to start the long jobs late — heavy
# facts therefore go on the queue first, and the light ones backfill.
_ORDERED_FACTS = (
    [f for f in MODULE_STREAMS_PER_FACT if f in _HEAVY_FACTS]
    + [f for f in MODULE_STREAMS_PER_FACT if f not in _HEAVY_FACTS]
)


def _run_available_now():
    """Drain every stream, then return.  Raises if any stream failed.

    PARALLEL_FACTS == 1 keeps the original serial drain; above that, facts are
    drained by a bounded worker pool.
    """
    print("== AvailableNow run: {} facts / {} streams — {} worker(s), heavy cap {} ==".format(
        len(MODULE_FACTS), _TOTAL_STREAMS, PARALLEL_FACTS, HEAVY_PARALLEL))
    _t0 = time.time()

    if PARALLEL_FACTS <= 1:
        for _fact_name in _ORDERED_FACTS:
            _drain_fact(_fact_name)
    else:
        # _drain_stream records failures instead of raising, so no worker can
        # die mid-run and abandon the queue.
        with ThreadPoolExecutor(max_workers=PARALLEL_FACTS) as _pool:
            list(_pool.map(_drain_fact, _ORDERED_FACTS))

    print("== run complete in {:.1f}s — {} failure(s) ==".format(
        time.time() - _t0, len(_failures)))
    for _n, _phase, _m in _failures:
        print("  FAILED [{}] during {}: {}".format(_n, _phase, _m))

    if _failures:
        raise Exception(
            "AvailableNow run finished with {} failed stream(s)".format(len(_failures))
        )


if RUN_MODE == "available_now":
    _run_available_now()

else:
    print("== starting {} streams ({} facts) ==".format(
        _TOTAL_STREAMS, len(MODULE_FACTS)))

    for _fc_fact, _tbl_groups in MODULE_STREAMS_PER_FACT.items():
        _fc = MODULE_FACTS[_fc_fact]
        for _silver_tbl, _flags in _tbl_groups.items():
            _is_ref = _flags[0]
            _iv = _fact_init_ver.get(_fc_fact, {}).get(_silver_tbl, -1)
            try:
                _q = _start_stream(_fc, _silver_tbl, _is_ref, _iv)
                _active_queries.append(_q)
            except Exception as _start_err:
                print(
                    "  ERROR starting m1_{}_{}: {}".format(
                        _fc_fact, _silver_tbl, _start_err
                    )
                )

    print(
        "== {}/{} streams started — entering monitoring loop ==".format(
            len(_active_queries), _TOTAL_STREAMS
        )
    )

# ── Monitoring loop (streaming mode ONLY) ─────────────────────────────────────
# Checks every 30 s.  When a stream dies with an exception:
#   fail#1 → FAILED → restart (same batch retried; Spark's checkpoint not advanced)
#   fail#2 → DEAD   → not restarted (prevents infinite loop on code bugs)
#   success → fail counter reset to 0
#
# Within-run restarts use the Spark checkpoint that built up since SJD start,
# so they resume from exactly the last committed offset — no reprocessing.
#
# available_now mode never reaches here: terminating IS the success condition there,
# so this loop would misread a clean drain as a dead stream.
if RUN_MODE == "streaming":
    while True:
        time.sleep(30)

        still_active = []
        _jvm_dead = False
        for _q in _active_queries:
            try:
                _sname = _q.name
                _is_active = _q.isActive
            except Exception as _jvm_err:
                print(
                    "[monitoring] Spark JVM connection lost ({}) — exiting".format(
                        type(_jvm_err).__name__
                    )
                )
                _jvm_dead = True
                break
            if _is_active:
                _STREAM_FAIL_COUNTS[_sname] = 0
                still_active.append(_q)
                continue

            _exc = _q.exception()

            if _exc is not None:
                _exc_str = str(_exc)
                print("[{}] error: {}".format(_sname, _exc_str[:400]))

                if _is_auto_heal(_exc_str):
                    _reg_h = _stream_registry.get(_sname)
                    if _reg_h:
                        _clear_checkpoint(_reg_h[0]["fact"], _reg_h[1], _sname)
                    _STREAM_FAIL_COUNTS[_sname] = 0
                    _fail = 0
                else:
                    _fail = _STREAM_FAIL_COUNTS.get(_sname, 0) + 1
                    _STREAM_FAIL_COUNTS[_sname] = _fail

                if _fail >= 2:
                    print(
                        "[{}] DEAD ({} consecutive failures) — not restarting".format(
                            _sname, _fail
                        )
                    )
                else:
                    if _fail == 0:
                        print("[{}] restarting (auto-heal)".format(_sname))
                    else:
                        print("[{}] FAILED (fail #{}) — restarting".format(_sname, _fail))
                    _reg = _stream_registry.get(_sname)
                    if _reg:
                        try:
                            _new_q = _start_stream(*_reg)
                            still_active.append(_new_q)
                        except Exception as _re:
                            _STREAM_FAIL_COUNTS[_sname] += 1
                            print("[{}] restart failed: {}".format(_sname, _re))
                    else:
                        print("[{}] no registry entry — cannot restart".format(_sname))
            else:
                print("[{}] stream stopped cleanly".format(_sname))

        if _jvm_dead:
            break
        _active_queries = still_active
        if not _active_queries:
            print("All streams stopped — exiting.")
            break

