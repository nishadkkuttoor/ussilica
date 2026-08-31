# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

#!/usr/bin/env python
# coding: utf-8

# Fabric notebook · nb_otc_facts_eso8
# ============================================================================
# Module 1 — Order-to-Cash (OTC)  ·  ESO8 test streaming notebook
# Gold schema  : lh_jde_gold.rpt
# Silver schema: jde_cdc_1  (test Silver — swap to jde for production)
# Last updated : 2026-07-31
#
# ─── ARCHITECTURE ────────────────────────────────────────────────────────────
# CDF-DIRECT: Silver tables are never re-read for streaming updates.
#
#   Spine INSERT  → filtered ref reads (only the ref rows for affected orders)
#                   → build_fn joins them → MERGE INSERT into Gold
#   Spine UPDATE  → CDF postimage used directly (no Silver read)
#                   → MERGE updates only spine-derived Gold columns
#                   → ref columns (customer_number etc.) left untouched in Gold
#   Spine is_delete=1 → comes as update_postimage; drop_deleted removes it
#                        from source → whenNotMatchedBySourceDelete deletes Gold row
#   Ref   any event  → CDF postimage → targeted Gold column MERGE
#                       → no spine Silver read, no ref Silver read
#
# SINGLE MERGE: replaces old DELETE+APPEND (2 commits → 1 commit).
#   No localCheckpoint needed — no gap between delete and insert.
#
# NO GLOBAL CACHE: ref tables are not cached in Spark executor memory.
#   INSERT path uses thread-local filtered Silver reads (a handful of rows).
#   get_ref() checks thread-local storage first; falls back to live Silver read
#   only on full load (all rows needed there).
#
# COMPOSITE key_in_set: whenNotMatchedBySourceDelete scopes to the EXACT
#   composite key pairs in the batch — not just the first key column.
#   Prevents accidental deletion of sibling rows sharing only the first key.
#
# DEDUP: when the same key appears multiple times in one CDF batch, keep the
#   row with the highest last_update_date_time_utc (source system timestamp).
#
# Gold last_update_date_time_utc = F.current_timestamp() at ETL write time.
#   Use (Gold ts − Silver ts) to monitor pipeline latency.
# ============================================================================

import threading
import time
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
SILVER_SCHEMA = "jde"  # ← change to "jde" for production
GOLD_LH = "lh_jde_gold"
GOLD_SCHEMA = "rpt"
TRIGGER = "60 seconds"


def sname(t):
    return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)


def gname(t):
    return "{}.{}.{}".format(GOLD_LH, GOLD_SCHEMA, t)


# ============================================================================
# 2) SHARED HELPERS
# ============================================================================
def drop_deleted(df):
    """Strip soft-deleted rows (is_delete = 1) before any join or select."""
    if "is_delete" in df.columns:
        return df.filter(F.col("is_delete").isNull() | (F.col("is_delete") != 1))
    return df


def sk(*cols):
    """Surrogate key — pipe-separated string from one or more column names."""
    return F.concat_ws(
        "|",
        *[
            F.col(c).cast("string") if isinstance(c, str) else c.cast("string")
            for c in cols
        ],
    )


# ============================================================================
# 3) THREAD-LOCAL REF STORAGE
#    Used only by the spine INSERT path for facts that have ref tables (eso8).
#    Each Spark streaming thread has its own isolated _thread_refs.cache slot —
#    no cross-stream interference even when multiple facts run concurrently.
#
#    Handler sets filtered refs BEFORE calling build_fn, clears AFTER.
#    get_ref() picks them up transparently; build_fn code is unchanged.
#    On full load (cache is None) get_ref() falls back to a full Silver read.
# ============================================================================
_thread_refs = threading.local()


def get_ref(tbl):
    """
    Return a Silver DataFrame for the reference table tbl.
      Streaming INSERT path : thread-local cache has a filtered Silver df → return it
      Full load path        : cache is None → live full Silver read
    """
    batch_refs = getattr(_thread_refs, "cache", None)
    if batch_refs and tbl in batch_refs:
        return batch_refs[tbl]
    return drop_deleted(spark.read.table(sname(tbl)))


# ============================================================================
# 4) STREAM STATE
# ============================================================================
_FACT_LOCKS = {}  # {gold_fact_name: threading.Lock}  — one lock per Gold table
_STREAM_FAIL_COUNTS = {}  # {stream_name: int}  — consecutive failure count


# ============================================================================
# 5) GENERIC ENGINE
# ============================================================================
def current_version(src):
    """Latest committed Delta version for a Silver table."""
    return (
        spark.sql("DESCRIBE HISTORY {}".format(sname(src)))
        .select(F.max("version"))
        .first()[0]
    )


def build_scope_condition(scope_rows, line_keys):
    """
    Build an exact composite-key WHERE clause for whenNotMatchedBySourceDelete.

    Produces one (k1 = v1 AND k2 = v2 ...) clause per scope row joined by OR,
    so only the exact key combinations in this batch are in scope.
    Using only the first key would over-scope and accidentally delete sibling
    Gold rows that share the first key but have a different second key.
    """
    if not scope_rows:
        return "1=0"

    def fmt(v):
        if v is None:
            return "NULL"
        if isinstance(v, (int, float)):
            return str(v)
        return "'{}'".format(str(v).replace("'", "''"))

    clauses = [
        "({})".format(
            " AND ".join("t.{} = {}".format(k, fmt(row[k])) for k in line_keys)
        )
        for row in scope_rows
    ]
    return " OR ".join(clauses)


# ─────────────────────────────────────────────────────────────────────────────
# recompute_no_ref  — spine-only facts (shipment_routing, dim_order, etc.)
# ─────────────────────────────────────────────────────────────────────────────
def recompute_no_ref(postimage, line_keys, sl_keys, gold_fact, build_fn):
    """
    Single-MERGE update for a fact with NO ref tables.

    All CDF event types (insert, update_postimage, is_delete=1) are handled
    in one pass:
      · is_delete=1 rows are in scope but stripped by build_fn via drop_deleted
        → whenNotMatchedBySourceDelete removes their Gold rows
      · updated rows → whenMatchedUpdateAll
      · new rows     → whenNotMatchedInsertAll

    postimage : CDF rows already deduped by last_update_date_time_utc;
                CDF metadata columns already dropped.
    sl_keys   : Silver primary key column names (may differ from line_keys).
    line_keys : Gold primary key column names.
    """
    # Scope: map Silver names → Gold names (includes is_delete=1 before build_fn)
    scope_rows = (
        postimage.select(*[F.col(s).alias(g) for s, g in zip(sl_keys, line_keys)])
        .distinct()
        .collect()
    )
    if not scope_rows:
        return 0

    key_in_set = build_scope_condition(scope_rows, line_keys)

    # build_fn calls drop_deleted → is_delete=1 rows excluded from new_rows
    new_rows = (
        build_fn(postimage)
        .withColumn("last_update_date_time_utc", F.current_timestamp())
        .cache()
    )
    inserted_count = new_rows.count()

    merge_cond = " AND ".join(["t.{0} = s.{0}".format(k) for k in line_keys])
    (
        DeltaTable.forName(spark, gname(gold_fact))
        .alias("t")
        .merge(new_rows.alias("s"), merge_cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete(condition=key_in_set)
        .execute()
    )
    new_rows.unpersist()
    return inserted_count


# ─────────────────────────────────────────────────────────────────────────────
# spine_update_merge  — UPDATE path for facts with ref tables (eso8)
# ─────────────────────────────────────────────────────────────────────────────
def spine_update_merge(postimage_updates, line_keys, sl_keys, gold_fact, fc):
    """
    Handle spine UPDATE (update_postimage) events for a fact that has ref tables.

    Updates ONLY spine-derived Gold columns. Ref-derived columns
    (customer_number, ship_to_number, etc.) are already correct in Gold —
    they were set at INSERT time or by the last ref-stream direct MERGE.
    No Silver reads of any kind.

    is_delete=1 rows are included in the scope but filtered by drop_deleted
    inside build_fn → whenNotMatchedBySourceDelete removes their Gold rows.
    The MERGE always runs even when update_source is empty (all-delete batch)
    so that the delete clause fires correctly.
    """
    # Scope in Gold column names (includes is_delete=1 before build_fn filter)
    scope_rows = (
        postimage_updates.select(
            *[F.col(s).alias(g) for s, g in zip(sl_keys, line_keys)]
        )
        .distinct()
        .collect()
    )
    if not scope_rows:
        return 0

    key_in_set = build_scope_condition(scope_rows, line_keys)

    # refs_provided=False → build_fn returns spine-derived Gold columns only
    update_source = (
        fc["build_fn"](postimage_updates, refs_provided=False)
        .withColumn("last_update_date_time_utc", F.current_timestamp())
        .cache()
    )
    count = update_source.count()  # materialise before MERGE

    # Explicit update set — only the columns present in update_source (spine-derived).
    # whenMatchedUpdateAll() would fail here because it translates to UPDATE SET *,
    # which requires the source to have EVERY target column including ref-derived ones
    # (customer_number, ship_to_number etc.) that are intentionally absent in the
    # spine-only source.  The explicit dict leaves ref cols in Gold untouched.
    update_set = {c: F.col("s.{}".format(c)) for c in update_source.columns}

    merge_cond = " AND ".join(["t.{0} = s.{0}".format(k) for k in line_keys])
    (
        DeltaTable.forName(spark, gname(gold_fact))
        .alias("t")
        .merge(update_source.alias("s"), merge_cond)
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedBySourceDelete(condition=key_in_set)
        .execute()
    )
    update_source.unpersist()
    return count


# ─────────────────────────────────────────────────────────────────────────────
# spine_insert_merge  — INSERT path for facts with ref tables (eso8)
# ─────────────────────────────────────────────────────────────────────────────
def spine_insert_merge(postimage_inserts, line_keys, gold_fact, fc):
    """
    Handle spine INSERT events for a fact that has ref tables.

    For brand-new fact rows, ref columns (customer_number etc.) do not yet
    exist in Gold, so we must join ref data from Silver.  Instead of reading
    the full ref Silver table, we semi-join each ref table against the spine
    postimage using the exact composite join keys (ref_spine_join_keys).

    Using a left_semi join (not a single-column isin filter) ensures we fetch
    only the ref rows that will actually match the eventual JOIN in build_fn —
    filtering on one column alone could return ref rows with a matching order
    number but a different order_type or company, causing wrong JOIN results.

    Filtered ref dfs are placed in thread-local storage so build_fn's
    get_ref() calls pick them up without any code change to build_fn.
    Thread-local is isolated per streaming thread — no cross-stream conflict.
    """
    if postimage_inserts.rdd.isEmpty():
        return 0

    # Semi-join each ref table against the spine postimage on the exact join keys
    _thread_refs.cache = {}
    for ref_tbl in fc["ref_tables"]:
        join_cols = fc["ref_spine_join_keys"][ref_tbl]
        spine_keys = postimage_inserts.select(*join_cols).distinct()
        _thread_refs.cache[ref_tbl] = drop_deleted(
            spark.read.table(sname(ref_tbl))
        ).join(spine_keys, on=join_cols, how="left_semi")
    try:
        # build_fn calls get_ref() → picks up thread-local filtered dfs
        new_rows = (
            fc["build_fn"](postimage_inserts)
            .withColumn("last_update_date_time_utc", F.current_timestamp())
            .cache()
        )
        count = new_rows.count()
        if count > 0:
            merge_cond = " AND ".join(["t.{0} = s.{0}".format(k) for k in line_keys])
            (
                DeltaTable.forName(spark, gname(gold_fact))
                .alias("t")
                .merge(new_rows.alias("s"), merge_cond)
                .whenNotMatchedInsertAll()
                .execute()
            )
        new_rows.unpersist()
        return count
    finally:
        _thread_refs.cache = None  # always clear — next batch starts fresh


# ─────────────────────────────────────────────────────────────────────────────
# ref_direct_merge  — ref stream handler
# ─────────────────────────────────────────────────────────────────────────────
def ref_direct_merge(postimage_ref, silver_tbl, gold_fact, fc):
    """
    Handle ref table CDF events by directly patching ref-derived Gold columns.

    No Silver reads (neither spine nor ref Silver).
    CDF postimage values are used directly for the update.

    is_delete=1 in ref postimage → ref-derived Gold columns set to NULL
    (LEFT JOIN semantics: the fact row stays in Gold but ref data disappears).

    Only Gold rows whose join-on columns match the ref postimage are touched.
    """
    ref_col_map = fc["ref_col_maps"][silver_tbl]  # {gold_col: silver_col}
    join_pairs = fc["ref_join_conditions"][silver_tbl]  # [(silver_col, gold_col)]

    # t.<gold_col> = r.<silver_col>  for each join condition
    merge_cond = " AND ".join(
        "t.{} = r.{}".format(gold_col, silver_col)
        for silver_col, gold_col in join_pairs
    )

    # For each ref-derived Gold column:
    #   is_delete=1 → NULL (ref row logically deleted, LEFT JOIN → NULL)
    #   otherwise   → CDF postimage value
    update_set = {
        gold_col: (
            F.when(F.col("r.is_delete") == 1, F.lit(None)).otherwise(
                F.col("r.{}".format(silver_col))
            )
        )
        for gold_col, silver_col in ref_col_map.items()
    }
    update_set["last_update_date_time_utc"] = F.current_timestamp()

    (
        DeltaTable.forName(spark, gname(gold_fact))
        .alias("t")
        .merge(postimage_ref.alias("r"), merge_cond)
        .whenMatchedUpdate(set=update_set)
        .execute()
    )


# ─────────────────────────────────────────────────────────────────────────────
# make_per_fact_handler  — routes each stream to the correct write path
# ─────────────────────────────────────────────────────────────────────────────
def make_per_fact_handler(fc, silver_tbl, is_ref, init_ver, stream_name):
    """
    Return the foreachBatch handler for ONE fact from ONE Silver source table.

    Routing logic:
      Spine + no refs → recompute_no_ref   (single MERGE, all event types)
      Spine + has refs:
          update_postimage → spine_update_merge  (no Silver reads, spine cols only)
          insert           → spine_insert_merge  (filtered ref reads, full INSERT)
      Ref stream       → ref_direct_merge    (no Silver reads, targeted col MERGE)

    On exception: increments _STREAM_FAIL_COUNTS and re-raises.
    Spark does not advance the checkpoint on handler exception, so the same
    batch is retried on restart (FAILED state).
    Two consecutive failures → DEAD (not restarted).
    """
    fact_tag = fc["tag"]
    is_spine = not is_ref
    has_refs = bool(fc.get("ref_tables"))

    def handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        try:
            sub_batch = batch_df
            if init_ver >= 0:
                sub_batch = sub_batch.filter(F.col("_commit_version") > init_ver)
            if sub_batch.rdd.isEmpty():
                return

            # Cache the batch so INSERT/UPDATE splits don't re-read CDF data twice
            sub_batch = sub_batch.cache()
            sub_batch.count()

            t0 = time.time()
            ops_count = 0
            _fact_lock = _FACT_LOCKS.setdefault(fc["fact"], threading.Lock())

            try:
                sl_keys = fc.get("spine_line_keys") or fc["line_keys"]

                # Dedup window: when the same line_key appears multiple times
                # in one batch, keep the row with the latest source timestamp.
                dedup_w = Window.partitionBy(*sl_keys).orderBy(
                    F.col("last_update_date_time_utc").desc()
                )

                if is_spine and has_refs:
                    # ── Spine with refs (eso8): split INSERT vs UPDATE ──────────
                    # INSERT must run BEFORE UPDATE.
                    # If the same key has both INSERT and UPDATE in the same batch
                    # (row created and updated within one trigger window):
                    #   INSERT first  → Gold row is created
                    #   UPDATE second → whenMatchedUpdateAll() finds it and applies
                    #                   the more recent data
                    # Reverse order (UPDATE first) would silently lose the UPDATE
                    # because the Gold row doesn't exist yet when whenMatchedUpdate runs.
                    inserts_df = sub_batch.filter(F.col("_change_type") == "insert")
                    updates_df = sub_batch.filter(
                        F.col("_change_type") == "update_postimage"
                    )

                    with _fact_lock:
                        # INSERT path — filtered ref reads, full fact row.
                        # Also covers re-activation: if a Gold row was deleted by a
                        # prior is_delete=1 event and Silver is now is_delete=0
                        # (update_postimage), the row is included here as a non-delete
                        # update. whenNotMatchedInsertAll fires only when the Gold row
                        # is absent, so existing rows are never double-written.
                        non_delete_updates = updates_df.filter(
                            F.col("is_delete").isNull() | (F.col("is_delete") != 1)
                        )
                        insert_candidates = inserts_df.unionByName(non_delete_updates)
                        if not insert_candidates.rdd.isEmpty():
                            latest_inserts = (
                                insert_candidates.withColumn(
                                    "rn", F.row_number().over(dedup_w)
                                )
                                .filter("rn = 1")
                                .drop(
                                    "rn",
                                    "_change_type",
                                    "_commit_version",
                                    "_commit_timestamp",
                                )
                            )
                            ops_count += spine_insert_merge(
                                latest_inserts, fc["line_keys"], fc["fact"], fc
                            )

                        # UPDATE path — spine cols only for existing rows + is_delete=1
                        # deletion. For re-activations the whenMatchedUpdate is a benign
                        # no-op on the just-inserted row (same spine data, ref cols left).
                        if not updates_df.rdd.isEmpty():
                            latest_updates = (
                                updates_df.withColumn(
                                    "rn", F.row_number().over(dedup_w)
                                )
                                .filter("rn = 1")
                                .drop(
                                    "rn",
                                    "_change_type",
                                    "_commit_version",
                                    "_commit_timestamp",
                                )
                            )
                            ops_count += spine_update_merge(
                                latest_updates, fc["line_keys"], sl_keys, fc["fact"], fc
                            )

                elif is_spine:
                    # ── Spine without refs — single unified MERGE ───────────────
                    latest = (
                        sub_batch.filter(
                            F.col("_change_type").isin("update_postimage", "insert")
                        )
                        .withColumn("rn", F.row_number().over(dedup_w))
                        .filter("rn = 1")
                        .drop(
                            "rn", "_change_type", "_commit_version", "_commit_timestamp"
                        )
                    )
                    with _fact_lock:
                        ops_count = recompute_no_ref(
                            latest, fc["line_keys"], sl_keys, fc["fact"], fc["build_fn"]
                        )

                else:
                    # ── Ref stream — direct targeted Gold MERGE ─────────────────
                    # Ref Silver PK = first element of each ref_join_conditions pair
                    ref_silver_keys = [
                        s for s, _ in fc["ref_join_conditions"][silver_tbl]
                    ]
                    ref_dedup_w = Window.partitionBy(*ref_silver_keys).orderBy(
                        F.col("last_update_date_time_utc").desc()
                    )
                    latest_ref = (
                        sub_batch.filter(
                            F.col("_change_type").isin("update_postimage", "insert")
                        )
                        .withColumn("rn", F.row_number().over(ref_dedup_w))
                        .filter("rn = 1")
                        .drop(
                            "rn", "_change_type", "_commit_version", "_commit_timestamp"
                        )
                    )
                    with _fact_lock:
                        ref_direct_merge(latest_ref, silver_tbl, fc["fact"], fc)

            finally:
                sub_batch.unpersist()

            print(
                "[{}|{}] batch={} ops={:,} {:.1f}s".format(
                    fact_tag, silver_tbl[:14], batch_id, ops_count, time.time() - t0
                )
            )
            _STREAM_FAIL_COUNTS[stream_name] = 0

        except Exception as e:
            count = _STREAM_FAIL_COUNTS.get(stream_name, 0) + 1
            _STREAM_FAIL_COUNTS[stream_name] = count
            print(
                "[{}|{}] {} batch={} fail#{}: {}: {}".format(
                    fact_tag,
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


# ============================================================================
# 7) FACT AND DIM DEFINITIONS
# ============================================================================

# ════════════════════════════════════════════════════════════════════════════
# [DIM]  dim_order  ·  spine: F4201
# Grain  : one row per (company, order_number, order_type)
# Gold PK: company, order_number, order_type
# ════════════════════════════════════════════════════════════════════════════
DIM_ORDER_GOLD = "dim_order"
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
DIM_SHIPMENT_GOLD = "dim_shipment"
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
    ).distinct()


# ════════════════════════════════════════════════════════════════════════════
# [FACT]  fact_shipment_routing  ·  spine: F4941
# Grain  : one row per (shipment_number, routing_step_number)
# Gold PK: shipment_number, routing_step_number
# ════════════════════════════════════════════════════════════════════════════
FACT_SHIPMENT_ROUTING_GOLD = "fact_shipment_routing"
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
    ).distinct()


# ════════════════════════════════════════════════════════════════════════════
# [FACT]  fact_sales_order_detail  ·  spine: F4211
# Grain  : one row per (company, order_number, order_type, line_number)
# Gold PK: company, order_number, order_type, line_number
# ════════════════════════════════════════════════════════════════════════════
FACT_SHIPMENT_ORDER_DETAIL_GOLD = "fact_sales_order_detail"
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


def build_fact_shipment_order_detail(f4211_df):
    df = drop_deleted(f4211_df)
    df = df.select(
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
    ).distinct()
    df = df.withColumn(
        "order_key", F.concat_ws("|", "order_number", "order_type", "company")
    )
    df = df.withColumn(
        "line_key",
        F.concat_ws("|", "company", "order_number", "order_type", "line_number"),
    )
    df = df.withColumn(
        "uom_key",
        F.concat_ws("|", "cost_center", "identifier_short_item", "uom_as_input"),
    )
    return df


# ════════════════════════════════════════════════════════════════════════════
# [FACT]  fact_extended_sales_order_8  ·  spine: F5542035
#         refs : F4201 (customer_number, ship_to_number, customer_po)
#                F5642B01 (export_booking_number, export_load_date)
# Grain  : one row per (order_number, run_date)  — MAX(math_numeric_01) dedup
# Gold PK: order_number, run_date
# ════════════════════════════════════════════════════════════════════════════
FACT_ESO8_GOLD = "fact_extended_sales_order_8"
FACT_ESO8_GOLD_LINE_KEYS = ["order_number", "run_date"]
FACT_ESO8_SILVER_LINE_KEYS = ["document_order_invoice_e", "date_updated"]


def build_fact_eso8(f5542035_df, refs_provided=True):
    """
    Build fact_extended_sales_order_8 rows from a F5542035 spine DataFrame.

    refs_provided=True  (INSERT path / full load):
        Joins F4201 and F5642B01 via get_ref() to populate all Gold columns.
        On the INSERT streaming path, get_ref() returns filtered Silver reads
        (only the rows for the affected orders) placed by spine_insert_merge
        into thread-local storage.  On full load, get_ref() reads full Silver.

    refs_provided=False (UPDATE path):
        Skips ref joins entirely. Returns only spine-derived Gold columns so
        the MERGE can update spine cols without touching ref cols already in Gold.
    """
    # Fact-specific dedup: F5542035 can have multiple rows per (order, date)
    # when the same order is re-processed intra-day.  Keep the MAX(math_numeric_01).
    window_max_asmath01 = Window.partitionBy(
        "document_order_invoice_e",  # ASDOCO — original silver name
        "date_updated",  # ASUPMJ — original silver name
    )

    f5542035_df = drop_deleted(f5542035_df)

    df_f5542035_filtered = (
        f5542035_df.withColumn(
            "max_asmath01",
            F.max("math_numeric_01").over(window_max_asmath01),  # ASMATH01
        )
        .filter(F.col("math_numeric_01") == F.col("max_asmath01"))
        .drop("max_asmath01")
    )

    if refs_provided:
        # ── Full select: spine + LEFT JOIN F4201 + LEFT JOIN F5642B01 ─────────

        df_f4201_slim = get_ref(F4201).select(
            F.col("document_order_invoice_e").alias("hdr_doco"),  # SHDOCO
            F.col("order_type").alias("hdr_dcto"),  # SHDCTO
            F.col("company_key_order_no").alias("hdr_kcoo"),  # SHKCOO
            F.col("address_number").alias("an8"),  # SHAN8  → PBI dim_address_book FK
            F.col("address_number_ship_to").alias(
                "shan"
            ),  # SHSHAN → PBI dim_address_book FK
            F.col("reference_01").alias("vr01"),  # SHVR01
        )

        df_f5642b01_slim = get_ref(F5642B01).select(
            F.col("document_order_invoice_e").alias("bk_doco"),  # BADOCO
            F.col("shipment_number").alias("bk_shpn"),  # BASHPN
            F.col("order_type").alias("bk_dcto"),  # BADCTO
            F.col("company_key_order_no").alias("bk_kcoo"),  # BAKCOO
            F.col("date_latest_pickup").alias("load_date"),  # BADLPU — DateType
            F.col("booking_no").alias("booking_no"),  # BA55BKNO
        )

        df_joined = (
            df_f5542035_filtered.alias("m")
            # ── JOIN 1: F4201 — LEFT (AN8, SHAN, VR01) ───────────────────────────────
            .join(
                df_f4201_slim.alias("hdr"),
                (F.col("m.document_order_invoice_e") == F.col("hdr.hdr_doco"))
                & (F.col("m.order_type") == F.col("hdr.hdr_dcto"))
                & (F.col("m.company_key_order_no") == F.col("hdr.hdr_kcoo")),
                "left",
            )
            # ── JOIN 4: F5642B01 — LEFT (LOADDATE, BOOKINGNO) ───────────────────────
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
            # ── Col 1  : ASDL01 — Process ─────────────────────────────────────────
            F.col("m.description_001").alias("process"),
            # ── Col 2  : ASMCU — Plant ────────────────────────────────────────────
            F.col("m.cost_center").alias("plant"),
            # ── Col 3  : ASDOCO — Order Number ───────────────────────────────────
            F.col("m.document_order_invoice_e").alias("order_number"),
            # ── Col 4  : ASDCTO — Order Type ─────────────────────────────────────
            F.col("m.order_type").alias("order_type"),
            # ── Col 5  : ASKCOO — Company ─────────────────────────────────────────
            F.col("m.company_key_order_no").alias("company"),
            # ── Col 6  : ASEV02 — Hook Order (Y/N) ───────────────────────────────
            F.col("m.everest_event_point_02").alias("hook_order"),
            # ── Col 7  : AN8 — Customer Number (FK → dim_address_book) ───────────
            F.col("hdr.an8").alias("customer_number"),
            # ── Col 8  : SHAN — Ship To Number (FK → dim_address_book) ───────────
            F.col("hdr.shan").alias("ship_to_number"),
            # ── Col 9  : VR01 — Customer PO ──────────────────────────────────────
            F.col("hdr.vr01").alias("customer_po"),
            # ── Col 10 : OLDPICK — Old Sch Pick Date ─────────────────────────────
            F.col("m.date_release_julian").alias("old_sch_pick_date"),
            # ── Col 11 : NEWPICK — New Sch Pick Date ─────────────────────────────
            F.col("m.scheduled_pick_date").alias("new_sch_pick_date"),
            # ── Col 12 : ASEDSP — Status Flag ────────────────────────────────────
            F.col("m.edi_successfully_process").alias("status_flag"),
            # ── Col 13 : ASMGTX — Error Message ──────────────────────────────────
            F.col("m.message_text").alias("error_message"),
            # ── Col 14 : BOOKINGNO — Export Booking Number ────────────────────────
            F.col("bk.booking_no").alias("export_booking_number"),
            # ── Col 15 : LOADDATE — Export Load Date ──────────────────────────────
            F.col("bk.load_date").alias("export_load_date"),
            # ── Col 16 : ASPID — Program ID ──────────────────────────────────────
            F.col("m.program_id").alias("program_id"),
            # ── Col 17 : ASUSER — User ────────────────────────────────────────────
            F.col("m.user_id").alias("user"),
            # ── Col 18 : UPMJ — Run Date ──────────────────────────────────────────
            F.col("m.date_updated").alias("run_date"),
            # ── Col 19 : ASUPMT — Run Time ───────────────────────────────────────
            F.col("m.time_last_updated").alias("run_time"),
            # ── Col 20 : ASJOBN — Work Station ───────────────────────────────────
            F.col("m.work_station_id").alias("work_station"),
            # ── Col 21 : ASSHPN — Shipment Number ───────────────────────────────────
            F.col("m.shipment_number").alias("shipment_number"),
        ).distinct()

    else:
        # ── Spine-only select (UPDATE path): ref-derived cols intentionally omitted ─
        df_fact = df_f5542035_filtered.select(
            F.col("description_001").alias("process"),
            F.col("cost_center").alias("plant"),
            F.col("document_order_invoice_e").alias("order_number"),
            F.col("order_type").alias("order_type"),
            F.col("company_key_order_no").alias("company"),
            F.col("everest_event_point_02").alias("hook_order"),
            F.col("date_release_julian").alias("old_sch_pick_date"),
            F.col("scheduled_pick_date").alias("new_sch_pick_date"),
            F.col("edi_successfully_process").alias("status_flag"),
            F.col("message_text").alias("error_message"),
            F.col("program_id").alias("program_id"),
            F.col("user_id").alias("user"),
            F.col("date_updated").alias("run_date"),
            F.col("time_last_updated").alias("run_time"),
            F.col("work_station_id").alias("work_station"),
            F.col("shipment_number").alias("shipment_number"),
        ).distinct()

    # Computed surrogate keys — same in both paths
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


# ============================================================================
# 8) MODULE REGISTRY
# ============================================================================
MODULE_FACTS = {
    # ── FACTS ─────────────────────────────────────────────────────────────────
    "shipment_routing": {
        "tag": "shipment_routing",
        "fact": FACT_SHIPMENT_ROUTING_GOLD,
        "line_keys": FACT_SHIPMENT_ROUTING_LINE_KEYS,
        "spine": F4941,
        "ref_tables": set(),
        "build_fn": build_fact_shipment_routing,
    },
    "shipment_order_detail": {
        "tag": "shipment_order_detail",
        "fact": FACT_SHIPMENT_ORDER_DETAIL_GOLD,
        "line_keys": FACT_SHIPMENT_ORDER_DETAIL_GOLD_LINE_KEYS,
        "spine": F4211,
        "spine_line_keys": FACT_SHIPMENT_ORDER_DETAIL_SILVER_LINE_KEYS,
        "ref_tables": set(),
        "build_fn": build_fact_shipment_order_detail,
    },
    "eso8": {
        "tag": "eso8",
        "fact": FACT_ESO8_GOLD,
        "line_keys": FACT_ESO8_GOLD_LINE_KEYS,
        "spine_line_keys": FACT_ESO8_SILVER_LINE_KEYS,
        "spine": F5542035,
        "ref_tables": {F4201, F5642B01},
        "build_fn": build_fact_eso8,
        # ref_spine_join_keys: columns shared between spine postimage and each ref table,
        # used for the left_semi join that filters ref Silver to only affected rows on INSERT.
        # Must match all columns in the eventual JOIN inside build_fn.
        "ref_spine_join_keys": {
            F4201: ["document_order_invoice_e", "order_type", "company_key_order_no"],
            F5642B01: [
                "document_order_invoice_e",
                "shipment_number",
                "order_type",
                "company_key_order_no",
            ],
        },
        # ref_col_maps: which Gold columns each ref table is responsible for
        #   key = Gold column name, value = Silver column name in the ref table
        "ref_col_maps": {
            F4201: {
                "customer_number": "address_number",
                "ship_to_number": "address_number_ship_to",
                "customer_po": "reference_01",
            },
            F5642B01: {
                "export_booking_number": "booking_no",
                "export_load_date": "date_latest_pickup",
            },
        },
        # ref_join_conditions: ON clause for the ref direct MERGE
        #   list of (silver_col_in_ref, gold_col_in_fact)
        "ref_join_conditions": {
            F4201: [
                ("document_order_invoice_e", "order_number"),
                ("order_type", "order_type"),
                ("company_key_order_no", "company"),
            ],
            F5642B01: [
                ("document_order_invoice_e", "order_number"),
                ("shipment_number", "shipment_number"),
                ("order_type", "order_type"),
                ("company_key_order_no", "company"),
            ],
        },
    },
    # ── DIMS ──────────────────────────────────────────────────────────────────
    "dim_order": {
        "tag": "dim_order",
        "fact": DIM_ORDER_GOLD,
        "line_keys": DIM_ORDER_GOLD_LINE_KEYS,
        "spine": F4201,
        "spine_line_keys": DIM_ORDER_SILVER_LINE_KEYS,
        "ref_tables": set(),
        "build_fn": build_dim_order,
    },
    "dim_shipment": {
        "tag": "dim_shipment",
        "fact": DIM_SHIPMENT_GOLD,
        "line_keys": DIM_SHIPMENT_LINE_KEYS,
        "spine": F4215,
        "ref_tables": set(),
        "build_fn": build_dim_shipment,
    },
}

# ── Per-fact, per-Silver stream registry (auto-derived) ──────────────────────
# Derived entirely from spine, ref_tables, and ref_join_conditions.
# No separate FACT_SOURCES_* constants needed.
MODULE_STREAMS_PER_FACT = {}
for _fc_tag, _fc in MODULE_FACTS.items():
    _streams = {}
    # One spine stream — always present
    _streams[_fc["spine"]] = [False]
    # One ref stream per ref table — only for facts with refs
    for _ref_tbl in _fc.get("ref_tables", set()):
        _streams[_ref_tbl] = [True]
    MODULE_STREAMS_PER_FACT[_fc_tag] = _streams

_TOTAL_STREAMS = sum(len(v) for v in MODULE_STREAMS_PER_FACT.values())


# ============================================================================
# 9) RUN
# ============================================================================
def _ckpt_path(fact_tag, silver_tbl):
    return "Files/checkpoints/module1/{}/{}".format(fact_tag, silver_tbl)


def _checkpoints_exist(path):
    try:
        return bool(mssparkutils.fs.ls(path))
    except Exception:
        return False


# ── Stop leftover streams from a previous run ─────────────────────────────────
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
_fact_init_ver = {}  # {fact_tag: {silver_tbl: init_version}}

for _fact_key, _fc in MODULE_FACTS.items():
    _tag = _fc["tag"]
    _fact = _fc["fact"]
    _spine = _fc["spine"]
    _unique_silver = list(MODULE_STREAMS_PER_FACT[_fact_key].keys())

    _fact_ckpts_ok = all(
        _checkpoints_exist(_ckpt_path(_tag, t)) for t in _unique_silver
    )
    _needs_full_load = not spark.catalog.tableExists(gname(_fact)) or not _fact_ckpts_ok

    if _needs_full_load:
        print("== [{}] FULL LOAD ==".format(_tag))

        # Capture Silver versions BEFORE reading so snapshot and CDF filter stay in sync
        _iv = {t: current_version(t) for t in _unique_silver}
        _spine_iv = _iv[_spine]

        # Full load: get_ref() falls back to live full Silver reads (thread-local is None)
        _new = _fc["build_fn"](
            spark.read.format("delta")
            .option("versionAsOf", _spine_iv)
            .table(sname(_spine))
        ).withColumn("last_update_date_time_utc", F.current_timestamp())

        (
            _new.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("delta.enableChangeDataFeed", "true")
            .saveAsTable(gname(_fact))
        )
        print(
            "  [{}] {} rows={:,}".format(_tag, _fact, spark.table(gname(_fact)).count())
        )
        _fact_init_ver[_tag] = _iv
        print("  [{}] init versions: {}".format(_tag, _iv))

        # Clear only this fact's checkpoints — other facts are never disturbed
        _fact_ckpt_dir = "Files/checkpoints/module1/{}".format(_tag)
        if _checkpoints_exist(_fact_ckpt_dir):
            try:
                mssparkutils.fs.rm(_fact_ckpt_dir, True)
                print("  [{}] checkpoints cleared".format(_tag))
            except Exception as _e:
                print("  [{}] checkpoint clear failed: {}".format(_tag, _e))
    else:
        print("== [{}] resuming from checkpoint ==".format(_tag))
        _fact_init_ver[_tag] = {}


# ── Launch streams ─────────────────────────────────────────────────────────────
_stream_registry = {}
_active_queries = []


def _start_stream(fc, silver_tbl, is_ref, init_ver):
    fact_tag = fc["tag"]
    stream_name = "m1_{}_{}".format(fact_tag, silver_tbl)
    _sv = _fact_init_ver.get(fact_tag, {}).get(silver_tbl)
    sv = _sv if _sv is not None else "latest"

    q = (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", sv)
        .table(sname(silver_tbl))
        .writeStream.foreachBatch(
            make_per_fact_handler(fc, silver_tbl, is_ref, init_ver, stream_name)
        )
        .option("checkpointLocation", _ckpt_path(fact_tag, silver_tbl))
        .trigger(processingTime=TRIGGER)
        .queryName(stream_name)
        .start()
    )
    _stream_registry[stream_name] = (fc, silver_tbl, is_ref, init_ver)
    print(
        "  [{}] started  sv={}  ckpt={}".format(
            stream_name, sv, _ckpt_path(fact_tag, silver_tbl)
        )
    )
    return q


print("== starting {} streams ({} facts) ==".format(_TOTAL_STREAMS, len(MODULE_FACTS)))

for _fc_tag, _tbl_groups in MODULE_STREAMS_PER_FACT.items():
    _fc = MODULE_FACTS[_fc_tag]
    for _silver_tbl, _flags in _tbl_groups.items():
        _is_ref = _flags[0]
        _iv = _fact_init_ver.get(_fc_tag, {}).get(_silver_tbl, -1)
        try:
            _q = _start_stream(_fc, _silver_tbl, _is_ref, _iv)
            _active_queries.append(_q)
        except Exception as _start_err:
            print(
                "  ERROR starting m1_{}_{}: {}".format(_fc_tag, _silver_tbl, _start_err)
            )

print(
    "== {}/{} streams started — entering monitoring loop ==".format(
        len(_active_queries), _TOTAL_STREAMS
    )
)

# ── Monitoring loop (every 30 s) ──────────────────────────────────────────────
# FAILED/DEAD lifecycle:
#   First failure  → FAILED → checkpoint not advanced → same batch retried on restart
#   Second failure → DEAD   → not restarted (prevents infinite loop on code bugs)
#   Success        → fail counter reset to 0
while True:
    time.sleep(30)

    still_active = []
    for _q in _active_queries:
        if _q.isActive:
            still_active.append(_q)
            continue

        _sname = _q.name
        _exc = _q.exception()

        if _exc is not None:
            _fail = _STREAM_FAIL_COUNTS.get(_sname, 0)
            if _fail >= 2:
                print(
                    "[{}] DEAD ({} consecutive failures) — not restarting".format(
                        _sname, _fail
                    )
                )
            else:
                print(
                    "[{}] FAILED (fail #{}) — restarting for batch retry".format(
                        _sname, _fail
                    )
                )
                _reg = _stream_registry.get(_sname)
                if _reg:
                    try:
                        _new_q = _start_stream(*_reg)
                        still_active.append(_new_q)
                    except Exception as _re:
                        _STREAM_FAIL_COUNTS[_sname] = _fail + 1
                        print("[{}] restart failed: {}".format(_sname, _re))
                else:
                    print("[{}] no registry entry — cannot restart".format(_sname))
        else:
            print("[{}] stream stopped cleanly".format(_sname))

    _active_queries = still_active
    if not _active_queries:
        print("All streams stopped — exiting.")
        break

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
