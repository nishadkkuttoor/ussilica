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

# Fabric notebook · nb_module1_otc_facts
# ============================================================================
# Module 1 — Order-to-Cash (OTC)  ·  Single Notebook, All Facts
# Gold schema:  lh_jde_gold.rpt
# Last updated: 2026-07-16
#
# ─── PURPOSE ─────────────────────────────────────────────────────────────────
# This notebook streams every OTC fact table from Silver to Gold.
# One notebook replaces one-notebook-per-fact so all OTC facts share a single
# Spark session, a single reference cache, and a single CU budget.
#
# ─── ADDING A NEW FACT (5-step checklist) ────────────────────────────────────
# a. Copy the ESO7 fact section as a template; rename all ESO7_* identifiers
# b. Define the Silver table constants, FACT name, LINE_KEYS, FACT_SOURCES
# c. Write build_fact_<name>(spine_df) using get_ref() for every reference table
# d. Register the fact in MODULE_FACTS at the bottom of the fact sections
# e. Nothing else changes — the generic engine and RUN section pick it up
#
# ─── DESIGN ──────────────────────────────────────────────────────────────────
# SHARED INFRASTRUCTURE
#   All facts share one _FACT_LOCKS dict, one _REF_CACHE, and the same generic
#   engine functions (affected_lines, recompute_fact, make_fact_handler).
#   A table referenced by multiple facts is cached exactly once.
#
# SMART REFERENCE CACHE
#   Tables  ≤ CACHE_ROW_LIMIT → Spark .cache() into executor memory
#   Tables  >  CACHE_ROW_LIMIT → read live each batch; Fabric's Delta file
#                                 cache (SSD-backed) handles repeated reads
#   Cache invalidation fires automatically when a reference table's own CDF
#   stream detects a change.  Large tables skip invalidation — reading live
#   already produces fresh data without any in-memory stale copy.
#
# RECOMPUTE PATTERN: DELETE + APPEND
#   On each relevant batch, the affected LINE_KEYS are deleted from Gold and
#   recomputed from scratch.  UPDATE is not used because an INNER join gate
#   (e.g. F4941 routing) can eliminate a row entirely and the row count per
#   LINE_KEY can change on a multi-table join.
#
# SOFT DELETES
#   Silver marks deleted rows with is_delete = 1.  drop_deleted() strips them
#   before any join or cache so they never appear in Gold.
#
# CONCURRENCY
#   _FACT_LOCKS (per-fact dict of threading.Lock) serialises Gold writes and
#   cache reloads per Gold table.  Subscribers sharing a Silver stream run
#   concurrently via ThreadPoolExecutor and acquire their own fact-specific lock,
#   so different Gold tables can be written in parallel while DELETE+APPEND pairs
#   for the same table remain atomic.
#
# FIRST RUN:      set the relevant tag to True in MANUAL_OVERWRITE → full reload that fact
# EVERY RESTART:  leave all tags False → all facts resume from their checkpoints
# NEW FACT ADDED: leave its tag False — the engine auto-detects the missing Gold
#                 table and runs a full load automatically without touching other facts
# TO OVERWRITE ONE FACT ONLY: set that tag to True; all others stay False
# ============================================================================

import concurrent.futures
import threading
import time
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ============================================================================
# 1) SHARED CONFIG
# ============================================================================
SILVER_LH = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH = "lh_jde_gold"
GOLD_SCHEMA = "rpt"
TRIGGER = "30 seconds"  # continuous streaming

# Per-fact overwrite control.  Set a tag to True to force a full reload of that
# fact only (wipes its Gold table and checkpoints).  All other facts resume from
# their existing checkpoints and are not disturbed.
# Tags must match the keys in MODULE_FACTS ("eso7", "eso4", "eso1", ...).
# A tag that is missing from this dict defaults to False (no overwrite).
MANUAL_OVERWRITE = {
    "eso7": True,
    "eso7_ship": True,
}

# Tables with row count above this are too large for Spark executor memory.
# They are read live each batch instead; Fabric's SSD-backed Delta file
# cache keeps repeated reads fast without touching executor memory.
CACHE_ROW_LIMIT = 5_000_000


def sname(t):
    return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)


def gname(t):
    return "{}.{}.{}".format(GOLD_LH, GOLD_SCHEMA, t)


# ============================================================================
# 2) SHARED HELPERS
# ============================================================================
def drop_deleted(df):
    """Strip soft-deleted rows (is_delete = 1) before any join or cache load."""
    if "is_delete" in df.columns:
        return df.filter(F.col("is_delete").isNull() | (F.col("is_delete") != 1))
    return df


# def sk(*cols):
#     return F.sha2(F.concat_ws("||", *[F.col(c).cast("string") if isinstance(c, str)
#                                        else c.cast("string") for c in cols]), 256)


def sk(*cols):
    """Surrogate key — pipe-separated string from one or more column names."""
    return F.concat_ws(
        "|",
        *[
            F.col(c).cast("string") if isinstance(c, str) else c.cast("string")
            for c in cols
        ],
    )


_INIT_VER_FILE = "Files/checkpoints/m1/_init_versions.json"


def _save_init_versions(ver_dict):
    """Persist init versions to disk so they survive session restarts."""
    import json

    flat = {}
    for tag, tbl_map in ver_dict.items():
        for tbl, ver in tbl_map.items():
            flat["{}/{}".format(tag, tbl)] = ver
    try:
        mssparkutils.fs.put(_INIT_VER_FILE, json.dumps(flat), True)
    except Exception as e:
        print("  [warn] could not save init versions: {}".format(e))


def _load_init_versions():
    """Load persisted init versions from disk. Returns {} if file does not exist."""
    import json

    try:
        content = mssparkutils.fs.head(_INIT_VER_FILE, 65536)
        flat = json.loads(content)
        result = {}
        for key, ver in flat.items():
            tag, tbl = key.split("/", 1)
            if tag not in result:
                result[tag] = {}
            result[tag][tbl] = ver
        return result
    except Exception:
        return {}


# ============================================================================
# 3) SHARED REFERENCE CACHE
#    One cache dict shared across all facts in this module.
#    Keys  : Silver table names (strings)
#    Values: cached Spark DataFrame  — for tables at or below CACHE_ROW_LIMIT
#            None (sentinel)         — for tables above CACHE_ROW_LIMIT
# ============================================================================
_FACT_LOCKS = {}  # keyed by Gold fact table name; one lock per fact, populated lazily
_REF_CACHE = {}  # populated by init_module_cache() before streams start


def ensure_cached(tbl):
    """
    Cache tbl in Spark executor memory if its row count is at or below
    CACHE_ROW_LIMIT.  If it exceeds the limit, register None as a sentinel
    so get_ref() knows to read live on every batch.
    Idempotent — calling it for an already-cached table is a no-op.
    """
    if tbl in _REF_CACHE:
        return
    df = drop_deleted(spark.read.table(sname(tbl)))
    count = df.count()
    if count <= CACHE_ROW_LIMIT:
        df = df.cache()
        df.count()  # materialise into executor memory now, not on first batch
        _REF_CACHE[tbl] = df
        print("  cached    {} ({:,} rows)".format(tbl, count))
    else:
        _REF_CACHE[tbl] = None  # sentinel: too large — use Delta file cache (SSD)
        print(
            "  skip cache {} ({:,} rows > {:,}) — reads live via Delta file cache".format(
                tbl, count, CACHE_ROW_LIMIT
            )
        )


def get_ref(tbl):
    """
    Return the reference DataFrame for tbl.
      · Cached entry exists  → return from Spark executor memory (fastest)
      · None sentinel        → read live from Silver; Fabric's Delta file
                               cache (SSD) handles repeated file reads
    All build_fact_*() functions must call get_ref() instead of
    spark.read.table() so the cache layer is always respected.
    """
    entry = _REF_CACHE.get(tbl)
    if entry is not None:
        return entry
    return drop_deleted(spark.read.table(sname(tbl)))


def reload_ref_cache(tbl):
    """
    Invalidate one reference table's in-memory cache and replace it with
    a fresh Silver read.  Called inside _FACT_LOCK when that table's CDF
    stream fires so the immediately following recompute_fact() sees the
    updated reference data.

    Large tables (None sentinel): no-op.  They already read live every
    batch so there is no stale in-memory copy to replace.
    """
    if tbl not in _REF_CACHE:
        ensure_cached(tbl)
        return
    if _REF_CACHE[tbl] is None:
        print("  [skip reload] {} — large table, reads live each batch".format(tbl))
        return
    # small table: release the stale copy and load fresh
    _REF_CACHE[tbl].unpersist()
    df = drop_deleted(spark.read.table(sname(tbl)))
    count = df.count()
    if count <= CACHE_ROW_LIMIT:
        df = df.cache()
        df.count()
        _REF_CACHE[tbl] = df
        print("  [cache reload] {} ({:,} rows)".format(tbl, count))
    else:
        # table has grown past the limit since session start — switch to live reads
        _REF_CACHE[tbl] = None
        print(
            "  [cache reload] {} ({:,} rows) — exceeded limit, switching to live reads".format(
                tbl, count
            )
        )


def init_module_cache(all_ref_tables):
    """
    Cache every unique reference table needed by all facts in this module.
    Called once before any streams start.
    Each Silver table is cached only once even if multiple facts reference it.
    """
    print("== initialising module reference cache ==")
    for tbl in all_ref_tables:
        ensure_cached(tbl)
    print("== module cache ready ==")


# ============================================================================
# 4) GENERIC ENGINE
#    These functions are fact-agnostic.  They receive the fact's configuration
#    as parameters so the same code services every fact registered in
#    MODULE_FACTS without modification.
# ============================================================================
def current_version(src):
    """Return the latest committed Delta version for a Silver table."""
    return (
        spark.sql("DESCRIBE HISTORY {}".format(sname(src)))
        .select(F.max("version"))
        .first()[0]
    )


def affected_lines(gold_fact, change_keys, join_pairs, line_keys):
    """
    Join the changed keys from one CDF batch against the Gold fact table
    to find which LINE_KEY combinations need to be recomputed.
    Returns a distinct DataFrame of LINE_KEY tuples in Gold column names.

    gold_fact   : Gold fact table name (e.g. ESO7_FACT)
    change_keys : distinct changed key values from the batch
    join_pairs  : list of (source_col, gold_col) tuples — right side is Gold column name
    line_keys   : Gold fact primary key column names
    """
    f = spark.read.table(gname(gold_fact)).alias("f")
    c = change_keys.alias("c")
    cond = None
    for scol, fcol in join_pairs:
        eq = F.col("c.{}".format(scol)) == F.col("f.{}".format(fcol))
        cond = eq if cond is None else (cond & eq)
    return (
        f.join(c, cond, "inner")
        .select(*[F.col("f.{}".format(k)) for k in line_keys])
        .distinct()
    )


def recompute_fact(
    lines, spine_tbl, line_keys, gold_fact, build_fn, spine_line_keys=None
):
    """
    Delete the existing Gold rows for the affected LINE_KEYS then append
    freshly computed rows produced by build_fn.

    If build_fn returns an empty DataFrame (spine row deleted or an INNER
    join gate no longer matches), the DELETE removes the rows and nothing is
    appended — effectively hard-deleting those LINE_KEYS from Gold.

    lines          : DataFrame of affected LINE_KEYS in Gold column names
                     (returned by affected_lines, which now reads Gold).
    spine_line_keys: optional list of Silver column names when the Silver spine
                     uses different names from Gold (e.g. ESO4: company_key vs
                     document_company).  Used only to join lines back to the
                     Silver spine so build_fn gets the right spine rows.
                     When None, Gold and Silver names are assumed identical.

    Must be called inside the corresponding _FACT_LOCKS[gold_fact] lock.
    """
    # Rename lines from Gold → Silver names for the spine join when they differ
    sl_keys = spine_line_keys or line_keys
    lines_for_spine = lines
    if spine_line_keys:
        for g, s in zip(line_keys, spine_line_keys):
            if g != s:
                lines_for_spine = lines_for_spine.withColumnRenamed(g, s)
    base = spark.read.table(sname(spine_tbl)).join(lines_for_spine, sl_keys, "inner")
    new_rows = build_fn(base).withColumn(
        "last_update_date_time_utc", F.current_timestamp()
    )
    # Materialise new_rows BEFORE the DELETE.  If new_rows were lazy and evaluated
    # after the DELETE, a Spark cache eviction of `lines` (e.g. caused by
    # reload_ref_cache loading a large ref table) would force the lines plan to
    # re-read Gold — which now has the affected rows deleted — returning 0 rows
    # and permanently losing data.  Caching first breaks that dependency.
    new_rows = new_rows.cache()
    inserted_count = new_rows.count()
    dt = DeltaTable.forName(spark, gname(gold_fact))
    line_cond = " AND ".join(["t.{0} = s.{0}".format(k) for k in line_keys])
    dt.alias("t").merge(lines.alias("s"), line_cond).whenMatchedDelete().execute()
    new_rows.write.format("delta").mode("append").saveAsTable(gname(gold_fact))
    new_rows.unpersist()
    return inserted_count


def make_fact_handler(silver_tbl, subscribers):
    """
    Return the foreachBatch handler for one Silver CDF stream that serves
    one or more facts.

    silver_tbl : Silver table name this stream reads from.
    subscribers: list of dicts, one per fact subscribed to this stream:
        {
          "fact_cfg":   fc,         # entry from MODULE_FACTS
          "join_pairs": [...],      # (source_col, spine_col) pairs for affected_lines
          "is_ref":     bool,       # True when this table is a ref table for this fact
          "init_ver":   int,        # Delta version at last full load; -1 = resume
        }

    The raw batch_df is cached once and shared across all subscribers so the
    CDF log is read from disk only once per micro-batch regardless of how many
    facts subscribe to this Silver table.  Each subscriber applies its own
    _commit_version filter independently so that facts with different full-load
    times skip the correct version ranges without affecting each other.

    When there are multiple subscribers (e.g. ESO7 and ESO4 both share F4211),
    they run concurrently via ThreadPoolExecutor.  Each subscriber acquires
    _FACT_LOCKS[gold_fact] — a per-fact lock — so subscribers writing to
    different Gold tables proceed in parallel while DELETE+APPEND pairs for the
    same Gold table remain atomic.
    """

    def _process_subscriber(sub, batch_df, batch_id, batch_reloaded):
        fc = sub["fact_cfg"]
        join_pairs = sub["join_pairs"]
        is_ref = sub["is_ref"]
        iv = sub["init_ver"]

        sub_batch = batch_df
        if iv >= 0:
            sub_batch = sub_batch.filter(F.col("_commit_version") > iv)
        if sub_batch.rdd.isEmpty():
            return

        keys = [p[0] for p in join_pairs]
        changed = (
            sub_batch.filter(
                F.col("_change_type").isin("insert", "update_postimage", "delete")
            )
            .select(*keys)
            .distinct()
        )

        t0 = time.time()
        if is_ref:
            # Reference table: scan Gold to find which LINE_KEYS are downstream of
            # the changed reference rows.  Gold scan is necessary here because the
            # reference table's own keys are different from the fact's LINE_KEYS.
            lines = affected_lines(
                fc["fact"], changed, join_pairs, fc["line_keys"]
            ).cache()
        else:
            # Spine: the CDF batch already contains the LINE_KEYS directly.
            # Using affected_lines here would miss INSERT events (new rows not yet
            # in Gold return 0 results → skipped).  Rename source_col → gold_col
            # so recompute_fact receives Gold column names for the DELETE MERGE;
            # it handles the reverse rename (Gold → Silver) internally via spine_line_keys.
            lines = (
                changed.select(*[F.col(p[0]).alias(p[1]) for p in join_pairs])
                .distinct()
                .cache()
            )

        line_count = lines.count()
        if line_count == 0:
            lines.unpersist()
            print(
                "[{}|{}] batch={} no changes — skipped".format(
                    fc["tag"], silver_tbl[:14], batch_id
                )
            )
            return
        inserted_count = 0
        try:
            _lock = _FACT_LOCKS.setdefault(fc["fact"], threading.Lock())
            with _lock:
                if is_ref and silver_tbl not in batch_reloaded:
                    reload_ref_cache(silver_tbl)
                    batch_reloaded.add(silver_tbl)
                inserted_count = recompute_fact(
                    lines,
                    fc["spine"],
                    fc["line_keys"],
                    fc["fact"],
                    fc["build_fn"],
                    spine_line_keys=fc.get("spine_line_keys"),
                )
        finally:
            lines.unpersist()
            print(
                "[{}|{}] batch={} lines={:,} ins={:,} {:.1f}s".format(
                    fc["tag"],
                    silver_tbl[:14],
                    batch_id,
                    line_count,
                    inserted_count,
                    time.time() - t0,
                )
            )

    def handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        batch_df = batch_df.cache()
        batch_df.count()  # materialise once before any subscriber filters it

        _batch_reloaded = set()  # tracks which ref tables were reloaded this batch
        try:
            n = len(subscribers)
            if n == 1:
                _process_subscriber(subscribers[0], batch_df, batch_id, _batch_reloaded)
            else:
                with concurrent.futures.ThreadPoolExecutor(max_workers=n) as pool:
                    futs = [
                        pool.submit(
                            _process_subscriber,
                            sub,
                            batch_df,
                            batch_id,
                            _batch_reloaded,
                        )
                        for sub in subscribers
                    ]
                    for fut in concurrent.futures.as_completed(futs):
                        fut.result()  # re-raise any subscriber exception
        finally:
            batch_df.unpersist()

    return handler


# ============================================================================
# 5) SILVER TABLE CONSTANTS
#    Module-level — shared across all facts and dims in this notebook.
#    One entry per unique JDE Silver table.  Add new tables here as ESO2,
#    ESO3, ESO4, ESO8 facts are onboarded; never duplicate an existing entry.
# ============================================================================
F4201 = "f4201_sales_order_header_file"
F4211 = "f4211_sales_order_detail_file"
F4941 = "f4941_shipment_routing_steps"
F5642B01 = "f5642b01_custom_sales_order_entry_screen_header"
F5642B11 = "f5642b11_custom_sales_order_entry_screen_detail"
F41002 = "f41002_item_units_of_measure_conversion_factors"
F5542035 = "f5542035_order_re_date_audit_history_table"
F4215 = "f4215_shipment_header"
# additional tables in eso2
F0101 = "f0101_address_book_master"
F0010 = "f0010_company_constants"
F5549002 = "f5549002_mxp_bol_interface_detail"
# additional tables in cso2
F4104 = "f4104_item_cross_reference_file"
# ════════════════════════════════════════════════════════════════════════════


# ════════════════════════════════════════════════════════════════════════════
# [FACT]  ESO7  ·  fact_extended_sales_order_7                spine: F4211
# ────────────────────────────────────────────────────────────────────────────
# Spine   : F4211 (sales order detail)
# Joins   : F4941 INNER (routing gate) · F4201 LEFT (order header)
#            F5642B01 LEFT (shipment header) · F5642B11 LEFT (shipment detail)
#            F41002 LEFT (UOM conversion factors)
# Checkpoint: Files/checkpoints/m1/<silver_table>  (shared, auto-managed by RUN section)
# ════════════════════════════════════════════════════════════════════════════

# ── Gold output ──────────────────────────────────────────────────────────────
ESO7_FACT = "fact_extended_sales_order_7"
ESO7_LINE_KEYS = [
    "company_key_order_no",
    "document_order_invoice_e",
    "order_type",
    "line_number",
]

# ── CDF sources + join pairs ─────────────────────────────────────────────────
# join_pairs: list of (source_col, gold_col) used by affected_lines() to
#             find which ESO7_LINE_KEYS are impacted when this source changes.
#             Right side is always the Gold fact column name (post-alias).
FACT_SOURCES_ESO7 = [
    {"silver": F4211, "join_pairs": [(c, c) for c in ESO7_LINE_KEYS]},
    {"silver": F4941, "join_pairs": [("shipment_number", "shipment_number")]},
    {
        "silver": F4201,
        "join_pairs": [
            ("company_key_order_no", "company_key_order_no"),
            ("document_order_invoice_e", "document_order_invoice_e"),
            ("order_type", "order_type"),
        ],
    },
    {
        "silver": F5642B01,
        "join_pairs": [
            ("shipment_number", "shipment_number"),
            ("company_key_order_no", "company_key_order_no"),
            ("order_type", "order_type"),
            ("document_order_invoice_e", "document_order_invoice_e"),
        ],
    },
    {
        "silver": F5642B11,
        "join_pairs": [
            ("company_key_order_no", "company_key_order_no"),
            ("document_order_invoice_e", "document_order_invoice_e"),
            ("order_type", "order_type"),
            ("line_number", "line_number"),
            ("shipment_number", "shipment_number"),
        ],
    },
    {
        "silver": F41002,
        "join_pairs": [
            ("identifier_short_item", "identifier_short_item"),
            ("uom", "uom_as_input"),
        ],
    },  # fwd: related_uom='TN'
    {
        "silver": F41002,
        "join_pairs": [
            ("identifier_short_item", "identifier_short_item"),
            ("related_uom", "uom_as_input"),
        ],
    },  # rev: uom='TN'
]


# ── Fact builder ─────────────────────────────────────────────────────────────
def build_fact_eso7(f4211_df):
    """
    Build ESO7 Gold fact rows from an F4211 spine DataFrame.
    All reference tables are read via get_ref() — never spark.read.table().

    Join sequence:
      F4941   INNER on shipment_number                    routing gate
      F4201   LEFT  on company / order / order_type       order header
      F5642B01 LEFT on shipment + company / order / type  shipment header
      F5642B11 LEFT on company / order / type / line / shipment  detail
      F41002  LEFT  bidirectional on item + uom           UOM factor

    UOM conversion tiers (applied in build_fact, finalised in DAX):
      Tier 0  uom_as_input = 'TN'      → conversion_factor = 1.0  (ETL)
      Tier A  F41002 item-specific     → conversion_factor stored on fact
      Tier B  F41003 standard fallback → via DAX RELATED(dim_uom_conversion)
      Fallback literal 1.0             → in DAX

    Routing dedup:
      A single order line can match multiple F4941 legs.  Because
      routing_date_release_julian is the only F4941 column in the SELECT and all
      other columns come from F4211 or the LEFT-joined tables, legs sharing the
      same date produce identical rows after the SELECT.  .distinct() collapses
      them to one Gold row per unique (LINE_KEY, routing_date) combination.
    """
    f4211_df = drop_deleted(f4211_df)

    # F41002 bidirectional UOM lookup: forward (uom→TN) + reverse (TN→uom)
    _f41002 = get_ref(F41002)
    _uom_fwd = _f41002.filter(
        (F.trim(F.col("related_uom")) == "TN") & (F.col("conversion_factor") != 0)
    ).select(
        F.col("identifier_short_item"),
        F.trim(F.col("uom")).alias("from_uom"),
        F.col("conversion_factor").cast("double").alias("conv_factor"),
    )
    _uom_rev = _f41002.filter(
        (F.trim(F.col("uom")) == "TN") & (F.col("conversion_factor") != 0)
    ).select(
        F.col("identifier_short_item"),
        F.trim(F.col("related_uom")).alias("from_uom"),
        (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_factor"),
    )
    uom = _uom_fwd.unionByName(_uom_rev)

    j = (
        f4211_df.alias("f")
        .join(
            get_ref(F4941).alias("r"),
            F.col("f.shipment_number") == F.col("r.shipment_number"),
            "left", # arun
        )
        .join(
            get_ref(F4201).alias("h"),
            (F.col("f.company_key_order_no") == F.col("h.company_key_order_no"))
            & (
                F.col("f.document_order_invoice_e")
                == F.col("h.document_order_invoice_e")
            )
            & (F.col("f.order_type") == F.col("h.order_type")),
            "left",
        )
        .join(
            get_ref(F5642B01).alias("s"),
            (F.col("f.shipment_number") == F.col("s.shipment_number"))
            & (F.col("f.company_key_order_no") == F.col("s.company_key_order_no"))
            & (F.col("f.order_type") == F.col("s.order_type"))
            & (
                F.col("f.document_order_invoice_e")
                == F.col("s.document_order_invoice_e")
            ),
            "left",
        )
        .join(
            get_ref(F5642B11).alias("e"),
            (F.col("f.company_key_order_no") == F.col("e.company_key_order_no"))
            & (
                F.col("f.document_order_invoice_e")
                == F.col("e.document_order_invoice_e")
            )
            & (F.col("f.order_type") == F.col("e.order_type"))
            & (F.col("f.line_number") == F.col("e.line_number"))
            & (F.col("f.shipment_number") == F.col("e.shipment_number")),
            "left",
        )
        .join(
            uom.alias("u"),
            (F.col("f.identifier_short_item") == F.col("u.identifier_short_item"))
            & (F.col("f.uom_as_input") == F.col("u.from_uom")),
            "left",
        )
    )

    proj = j.select(
        F.col("f.company_key_order_no"),
        F.col("f.document_order_invoice_e"),
        F.col("f.order_type"),
        F.col("f.line_number"),
        sk(
            F.col("f.shipment_number"),
            F.col("f.company_key_order_no"),
            F.col("f.order_type"),
            F.col("f.document_order_invoice_e"),
        ).alias("shipment_order_key"),
        F.col("f.shipment_number"),
        F.col("f.cost_center"),
        F.col("f.address_number"),
        F.col("f.address_number_ship_to"),
        F.col("f.carrier"),
        F.col("f.status_code_last"),
        sk(
            F.col("f.order_type"), F.col("f.line_type"), F.col("f.status_code_last")
        ).alias("order_activity_key"),
        F.col("f.status_code_next").cast("long").alias("status_code_next"),
        F.col("f.line_type"),
        F.col("f.identifier_second_item"),
        F.col("f.identifier_short_item"),
        F.col("f.uom_as_input"),
        F.col("f.freight_handling_code"),
        F.col("f.mode_of_transport"),
        F.col("f.container_id"),
        F.col("f.reference_01"),
        F.col("f.transaction_originator"),
        F.col("f.date_requested_julian"),
        F.col("f.date_promised_ship_julian"),
        F.col("f.scheduled_pick_date"),
        F.col("f.date_release_julian"),
        F.col("f.units_transaction_qty"),
        F.col("r.date_release_julian").alias("routing_date_release_julian"),
        F.col("h.hold_orders_code"),
        F.col("h.date_original_promisde"),
        F.col("s.routing_notes"),
        F.col("s.date_requested_ship"),
        F.col("s.destination_port"),
        F.col("s.date_latest_delivery"),
        F.col("s.vessel_name"),
        F.col("s.loading_port"),
        F.col("s.ocean_carrier"),
        F.col("s.ocean_del_terms"),
        F.col("s.no_of_container"),
        F.col("s.reference_01").alias("shipment_reference_01"),
        F.col("s.reference_02"),
        F.col("s.inland_delterms"),
        F.col("s.incoterms"),
        F.col("s.date_loaded"),
        F.col("s.date_release_julian").alias("shipment_date_release_julian"),
        F.col("s.date_01"),
        F.col("e.seal_no"),
        F.col("e.production_code"),
        F.col("e.production_ship_notes"),
        F.coalesce(
            F.when(F.trim(F.col("f.uom_as_input")) == "TN", F.lit(1.0)),
            F.col("u.conv_factor"),
        ).alias("conversion_factor"),
    )

    return proj.distinct()


# End of ESO7 fact_extended_sales_order_7
# ════════════════════════════════════════════════════════════════════════════


# ════════════════════════════════════════════════════════════════════════════
# [FACT]  ESO7-SHIP  ·  fact_extended_sales_order_7_shipment  spine: F5642B01
# ────────────────────────────────────────────────────────────────────────────
# Spine   : F5642B01 (shipment header)
# Joins   : F4941 OCE leg LEFT on shipment_number (routing_origin / routing_carrier)
# Grain   : one row per (shipment_number + company + order_type + order_no)
# Key     : shipment_order_key = SHA-256(shipment_number || company || order_type || order_no)
#            — same formula used on fact_extended_sales_order_7 so PBI can relate them
# Triggers: F5642B01 CDF (spine changed) + F4941 CDF (OCE routing updated)
#
# PBI use:
#   Core ESO7 report  : routing_date_release_julian IS NOT NULL  (filter on fact)
#   Missing Freight   : ISBLANK([routing_origin]) OR ISBLANK([routing_carrier])
#                       (filter on fact_extended_sales_order_7_shipment — propagates via relationship)
# ════════════════════════════════════════════════════════════════════════════

ESO7_SHIP_FACT = "fact_extended_sales_order_7_shipment"
ESO7_SHIP_KEYS = [
    "shipment_number",
    "company_key_order_no",
    "order_type",
    "document_order_invoice_e",
]
ESO7_SHIP_SK = "shipment_order_key"

FACT_SOURCES_ESO7_SHIP = [
    {"silver": F5642B01, "join_pairs": [(c, c) for c in ESO7_SHIP_KEYS]},
    {"silver": F4941, "join_pairs": [("shipment_number", "shipment_number")]},
]


def build_fact_eso7_ship(b01_df):
    """
    Build fact_extended_sales_order_7_shipment rows from an F5642B01 spine DataFrame.
    F4941 OCE leg is LEFT-joined to supply routing_origin and routing_carrier.
    NULL routing_origin / routing_carrier = the Missing Freight signal in PBI.

    Uses get_ref(F4941) so the OCE join always sees the latest cached
    routing data.  When F4941 changes, reload_ref_cache fires before this
    function is called (F4941 is in ref_tables for eso7_ship).

    Columns selected here are ONLY those not already available on
    fact_extended_sales_order_7 via its F5642B01 LEFT join.  The 16 shared
    shipment-header columns (routing_notes, destination_port, vessel_name, etc.)
    are dropped — PBI accesses them from fact_eso7 through the shipment_order_key
    relationship.  Only the 4 booking/equipment columns unique to this sub-fact
    and the 2 OCE routing columns are kept.
    """
    b01_df = drop_deleted(b01_df)
    f4941_oce = get_ref(F4941).filter(F.trim(F.col("mode_of_transport")) == "OCE")

    return (
        b01_df.alias("s")
        .join(
            f4941_oce.alias("oce"),
            F.col("s.shipment_number") == F.col("oce.shipment_number"),
            "left",
        )
        .select(
            # surrogate key — relates to fact_extended_sales_order_7
            sk(
                F.col("s.shipment_number"),
                F.col("s.company_key_order_no"),
                F.col("s.order_type"),
                F.col("s.document_order_invoice_e"),
            ).alias(ESO7_SHIP_SK),
            # natural key columns
            F.col("s.shipment_number"),
            F.col("s.company_key_order_no"),
            F.col("s.order_type"),
            F.col("s.document_order_invoice_e"),
            # booking / equipment columns — not on fact_eso7
            F.col("s.equipment_type"),
            F.col("s.booking_no"),
            F.col("s.bookingstatus").alias("booking_status"),
            F.col("s.voyage_no"),
            # OCE routing leg — the Missing Freight signal; NULL = no OCE entry
            F.col("oce.origin_address_number").alias("routing_origin"),
            F.col("oce.address_number_deconsolida").alias("routing_carrier"),
        )
        .dropDuplicates([ESO7_SHIP_SK])
    )


# End of ESO7 fact_extended_sales_order_7_shipment
# ══════════════════════════════════════════════════════════════════════════════


# ============================================================================
# MODULE REGISTRY
# ─────────────────────────────────────────────────────────────────────────────
# Register every active [FACT] and [DIM] here.  The RUN section iterates this
# dict automatically — no other changes needed when a new table is added.
#
# Keys per entry:
#   tag              short label used in stream names and log output
#   fact             Gold table name  (facts use fact_* prefix; dims use dim_* prefix)
#   line_keys        primary key columns in Gold — used by affected_lines (reads Gold)
#                    and as DELETE MERGE condition in recompute_fact
#   spine            Silver spine table — read by recompute_fact to feed build_fn
#   spine_line_keys  optional; Silver column names when they differ from line_keys
#                    (e.g. dim_order: company_key_order_no → company). Omit when names match.
#   ref_tables       Silver tables that are reference tables for this entry
#                    (all FACT_SOURCES / DIM_SOURCES entries except the spine)
#   build_fn         build_fact_*() or build_dim_*() function for this entry
#   sources          FACT_SOURCES_* or DIM_SOURCES_* list for this entry
#                    join_pairs right side = Gold column names (since affected_lines reads Gold)
# ============================================================================
MODULE_FACTS = {
    # ── FACTS ─────────────────────────────────────────────────────────────────
    "eso7": {
        "tag": "eso7",
        "fact": ESO7_FACT,
        "line_keys": ESO7_LINE_KEYS,
        "spine": F4211,
        "ref_tables": {
            F4941,
            F4201,
            F5642B01,
            F5642B11,
            F41002,
        },
        "build_fn": build_fact_eso7,
        "sources": FACT_SOURCES_ESO7,
    },
    "eso7_ship": {
        "tag": "eso7_ship",
        "fact": ESO7_SHIP_FACT,
        "line_keys": ESO7_SHIP_KEYS,
        "spine": F5642B01,
        "ref_tables": {F4941},
        "build_fn": build_fact_eso7_ship,
        "sources": FACT_SOURCES_ESO7_SHIP,
    },
}

# ── Shared stream registry (auto-derived from MODULE_FACTS) ──────────────────
# Maps each unique Silver table to the list of facts that subscribe to it.
# One Spark Structured Streaming query is launched per entry in this dict —
# if two facts share a Silver table (e.g. both use F4211 as spine), they share
# one CDF stream and the batch_df is materialised only once per micro-batch.
#
# Checkpoint paths move to Files/checkpoints/m1/<silver_table> — one path per
# unique Silver table, shared by all subscribers.
# NOTE: the first run after adding this dict will full-reload all active facts
# because the new checkpoint paths do not exist yet.  Subsequent runs resume.
MODULE_STREAMS = {}
for _fc in MODULE_FACTS.values():
    for _src in _fc["sources"]:
        _tbl = _src["silver"]
        if _tbl not in MODULE_STREAMS:
            MODULE_STREAMS[_tbl] = []
        MODULE_STREAMS[_tbl].append(
            {
                "fact_cfg": _fc,
                "join_pairs": _src["join_pairs"],
                "is_ref": _tbl in _fc["ref_tables"],
            }
        )


# ============================================================================
# RUN
# ============================================================================
spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

# ── stop leftover streams from a previous run in this session ────────────────
_all_stream_names = {"m1__{}".format(tbl) for tbl in MODULE_STREAMS}
_stopped = []
for _q in list(spark.streams.active):
    if _q.name in _all_stream_names:
        _q.stop()
        _stopped.append(_q.name)
if _stopped:
    print("Stopped leftover streams: {}".format(_stopped))

# ── initialise module reference cache (all unique ref tables, cached once) ───
_all_ref_tables = list(
    {tbl for fc in MODULE_FACTS.values() for tbl in fc["ref_tables"]}
)
init_module_cache(_all_ref_tables)


# ── checkpoint existence helper ───────────────────────────────────────────────
def _checkpoints_exist(ckpt_path):
    try:
        return bool(mssparkutils.fs.ls(ckpt_path))
    except Exception:
        return False


# ── per-fact: full load or resume; collect init versions ─────────────────────
# _fact_init_ver[tag][silver_tbl] = Delta version at full load time.
# Used by make_fact_handler to skip CDF commits already captured in the full load.
# Persisted to _INIT_VER_FILE so they survive session restarts — this is the
# correct startingVersion fallback when a checkpoint dir exists but has no saved
# offset (session stopped before the first 30-second trigger fired).
_persisted_init_ver = _load_init_versions()
_fact_init_ver = {}

for _fact_key, _fc in MODULE_FACTS.items():
    _tag = _fc["tag"]
    _fact = _fc["fact"]
    _spine = _fc["spine"]
    _build_fn = _fc["build_fn"]
    _sources = _fc["sources"]

    # Deduplicate Silver tables — FACT_SOURCES may list the same Silver table
    # more than once (e.g. two F41002 entries for bidirectional UOM).  Without
    # deduplication, current_version() and _checkpoints_exist() would be called
    # twice for the same table, and checkpoint clearing could run twice.
    _unique_silver = list(dict.fromkeys(s["silver"] for s in _sources))

    # Shared checkpoints live at Files/checkpoints/m1/<silver_tbl>.
    # A fact needs a full load if ANY of its source checkpoints are missing.
    _fact_ckpts_ok = all(
        _checkpoints_exist("Files/checkpoints/m1/{}".format(tbl))
        for tbl in _unique_silver
    )
    _needs_full_load = (
        MANUAL_OVERWRITE.get(_tag, False)
        or not spark.catalog.tableExists(gname(_fact))
        or not _fact_ckpts_ok
    )

    if _needs_full_load:
        print("== [{}] FULL LOAD ==".format(_tag))
        _new = _build_fn(spark.read.table(sname(_spine))).withColumn(
            "last_update_date_time_utc", F.current_timestamp()
        )
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

        _iv = {tbl: current_version(tbl) for tbl in _unique_silver}
        _fact_init_ver[_tag] = _iv
        print("  [{}] init versions: {}".format(_tag, _iv))

        # Clear checkpoint only for Silver tables where ALL subscribers need a
        # full load.  If another fact shares the table and is resuming, its
        # checkpoint must be preserved so it does not lose its CDF position.
        for _tbl in _unique_silver:
            _all_subs_reload = all(
                MANUAL_OVERWRITE.get(sub["fact_cfg"]["tag"], False)
                or not spark.catalog.tableExists(gname(sub["fact_cfg"]["fact"]))
                for sub in MODULE_STREAMS[_tbl]
            )
            if _all_subs_reload:
                _ckpt_path = "Files/checkpoints/m1/{}".format(_tbl)
                if _checkpoints_exist(_ckpt_path):
                    try:
                        mssparkutils.fs.rm(_ckpt_path, True)
                        print("  [{}] checkpoint cleared: {}".format(_tag, _tbl))
                    except Exception as _e:
                        print(
                            "  [{}] checkpoint clear failed ({}): {}".format(
                                _tag, _tbl, _e
                            )
                        )
                else:
                    print(
                        "  [{}] checkpoint already absent (cleared by earlier fact): {}".format(
                            _tag, _tbl
                        )
                    )
    else:
        print("== [{}] resuming from checkpoint ==".format(_tag))
        _fact_init_ver[_tag] = _persisted_init_ver.get(_tag, {})

_save_init_versions(_fact_init_ver)

# ── launch one stream per unique Silver table 
# Each stream serves all facts that subscribe to that Silver table.
# The batch_df is materialised once and shared across subscribers.
print(
    "== starting {} streams ({} unique Silver tables) ==".format(
        len(MODULE_STREAMS), len(MODULE_STREAMS)
    )
)

for _tbl, _subs in MODULE_STREAMS.items():
    # Attach per-subscriber init_ver resolved from each fact's full-load record.
    _subs_with_iv = []
    for _sub in _subs:
        _sub_tag = _sub["fact_cfg"]["tag"]
        _iv = _fact_init_ver.get(_sub_tag, {}).get(_tbl, -1)
        _subs_with_iv.append({**_sub, "init_ver": _iv})

    # startingVersion = min init_ver across subscribers.
    # When checkpoint has a valid offset, Spark ignores this and resumes from it.
    # When checkpoint has no offset (session stopped before first trigger fired),
    # this prevents startingVersion=latest from skipping intermediate Silver changes.
    _all_ivs = [
        _fact_init_ver.get(s["fact_cfg"]["tag"], {}).get(_tbl) for s in _subs_with_iv
    ]
    _valid_ivs = [v for v in _all_ivs if v is not None]
    _sv = min(_valid_ivs) if _valid_ivs else "latest"

    _stream_name = "m1__{}".format(_tbl)
    (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", _sv)
        .table(sname(_tbl))
        .writeStream.foreachBatch(make_fact_handler(_tbl, _subs_with_iv))
        .option("checkpointLocation", "Files/checkpoints/m1/{}".format(_tbl))
        .trigger(processingTime=TRIGGER)  # Continous Mode
        # .trigger(availableNow=True)  # One Time Update
        .queryName(_stream_name)
        .start()
    )

    _sub_summary = {s["fact_cfg"]["tag"]: s["init_ver"] for s in _subs_with_iv}
    print("  {} → {}".format(_stream_name, _sub_summary))

print(
    "== all module 1 streams running ({} streams) — awaiting termination ==".format(
        len(MODULE_STREAMS)
    )
)

# AvailableNow mode →  notebook waits for ALL streams to finish:
# for _q in spark.streams.active:
#     _q.awaitTermination()

# Continuous mode  → notebook stays alive until any stream errors.
spark.streams.awaitAnyTermination()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
