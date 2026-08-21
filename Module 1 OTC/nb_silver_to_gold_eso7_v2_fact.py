#!/usr/bin/env python
# coding: utf-8

# ## nb_silver_to_gold_eso7_v2_fact
# 
# New notebook

# In[3]:


#!/usr/bin/env python
# coding: utf-8

# Fabric notebook · nb_silver_to_gold_eso7_v2_fact
# ============================================================================
# ESO7 v2 Gold fact table.  Dims are built by separate dim notebooks.
# Gold schema: `rpt`, lakehouse: `lh_jde_gold`.
#
# ARCHITECTURE
# ─────────────────────────────────────────────────────────────────────────────
# Six Spark Structured Streaming queries run concurrently, one per Silver CDF
# source.  Every 30 seconds each query reads the CDF log of its source table
# and, if anything changed, recomputes the affected Gold fact rows.
#
# Source table roles and join types:
#
#   Source      Role in join      Join type   Delete behaviour in Gold
#   ──────────  ────────────────  ──────────  ────────────────────────────────
#   F4211       Spine             (spine)     Gold row removed
#   F4941       Routing step      INNER       Gold row removed
#   F4201       Order header      LEFT        Gold row stays, columns → NULL
#   F5642B01    Shipment header   LEFT        Gold row stays, columns → NULL
#   F5642B11    Shipment detail   LEFT        Gold row stays, columns → NULL
#   F41002      UOM factors       LEFT        Gold row stays, columns → NULL
#
# CONCURRENCY AND LOCKING
# ─────────────────────────────────────────────────────────────────────────────
# _FACT_LOCK (threading.Lock) serialises all Gold fact writes.  Without it,
# two concurrent foreachBatch callbacks could interleave their DELETE+APPEND
# operations and corrupt the fact table.
#
# SOFT DELETES
# ─────────────────────────────────────────────────────────────────────────────
# Silver uses soft deletes (is_delete = 1 set on the row; the row itself
# stays in the table).  drop_deleted() strips these rows before any join so
# they never appear in the Gold output.  A soft-deleted F4211 spine row
# produces an empty build_fact() result, which causes recompute_fact() to
# remove the corresponding Gold row (hard delete via MERGE DELETE).
#
# RECOMPUTE PATTERN: DELETE + APPEND (not UPDATE)
# ─────────────────────────────────────────────────────────────────────────────
# recompute_fact() first deletes all Gold rows matching the affected LINE_KEYS,
# then appends the freshly computed rows.  UPDATE is not used because:
#   · The number of Gold rows per LINE_KEY can change (DISTINCT of a multi-join)
#   · If the F4941 INNER join no longer matches, the row must disappear
#   · DELETE+APPEND handles both cases with a single uniform pattern
#
# FIRST RUN:     set MANUAL_OVERWRITE = True  → full load + clear checkpoints
# EVERY RESTART: set MANUAL_OVERWRITE = False → streams resume from checkpoint
# ============================================================================

import json
import threading
import time
from datetime import datetime, timezone
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------
SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "cdf"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "eso7"
CKPT_ROOT     = "Files/checkpoints/eso7_v2"
# TRIGGER = "30 seconds"   # streaming mode — uncomment to revert (also see trigger + await below)

MANUAL_OVERWRITE = False   # True = full reload; set back to False after first run

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

F4211    = "f4211_sales_order_detail_file"
F4201    = "f4201_sales_order_header_file"
F4941    = "f4941_shipment_routing_steps"
F5642B01 = "f5642b01_custom_sales_order_entry_screen_header"
F5642B11 = "f5642b11_custom_sales_order_entry_screen_detail"
F41002   = "f41002_item_units_of_measure_conversion_factors"

FACT      = "fact_extended_sales_order_7"
LINE_KEYS = ["company_key_order_no", "document_order_invoice_e", "order_type", "line_number"]

_FACT_LOCK = threading.Lock()   # serialises Gold writes across concurrent streams

def drop_deleted(df):
    """Remove soft-deleted rows (is_delete = 1) before joining or caching."""
    if "is_delete" in df.columns:
        return df.filter(F.col("is_delete").isNull() | (F.col("is_delete") != 1))
    return df

# ----------------------------------------------------------------------------
# 2) FACT BUILDER
# ----------------------------------------------------------------------------
def build_fact(f4211_df):
    """
    Join the F4211 spine DataFrame against all reference tables to produce the
    Gold fact rows.  Reference tables are read directly from Silver on every call.

    UOM conversion (F41002):
      Tier 0 — uom_as_input = 'TN'  → conversion_factor = 1.0  (no join needed)
      Tier A — F41002 item-specific bidirectional lookup → conversion_factor stored
      Tier B — DAX RELATED(dim_uom_conversion[std_factor]) when Tier A is NULL
      Fallback — literal 1.0 in DAX

    Routing dedup (Window filter):
      A single order line can match multiple F4941 routing steps (multiple legs).
      The Window keeps only rows where routing_date_release_julian is non-NULL,
      unless ALL legs have NULL routing dates — in which case one NULL row is kept
      to avoid silently dropping the order line from Gold.

    Returns a DISTINCT DataFrame; caller handles the MERGE into the Gold table.
    """
    f4211_df = drop_deleted(f4211_df)
    f4201    = drop_deleted(spark.read.table(sname(F4201)))
    b01      = drop_deleted(spark.read.table(sname(F5642B01)))
    b11      = drop_deleted(spark.read.table(sname(F5642B11)))
    _f41002  = drop_deleted(spark.read.table(sname(F41002)))
    _uom_fwd = (_f41002
                .filter((F.trim(F.col("related_uom")) == "TN") & (F.col("conversion_factor") != 0))
                .select(F.col("identifier_short_item"),
                        F.trim(F.col("uom")).alias("from_uom"),
                        F.col("conversion_factor").cast("double").alias("conv_factor")))
    _uom_rev = (_f41002
                .filter((F.trim(F.col("uom")) == "TN") & (F.col("conversion_factor") != 0))
                .select(F.col("identifier_short_item"),
                        F.trim(F.col("related_uom")).alias("from_uom"),
                        (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_factor")))
    uom   = _uom_fwd.unionByName(_uom_rev)
    f4941 = drop_deleted(spark.read.table(sname(F4941)))

    j = (f4211_df.alias("f")
         .join(f4941.alias("r"), F.col("f.shipment_number") == F.col("r.shipment_number"), "inner")
         .join(f4201.alias("h"),
               (F.col("f.company_key_order_no")     == F.col("h.company_key_order_no")) &
               (F.col("f.document_order_invoice_e") == F.col("h.document_order_invoice_e")) &
               (F.col("f.order_type")               == F.col("h.order_type")), "left")
         .join(b01.alias("s"),
               (F.col("f.shipment_number")          == F.col("s.shipment_number")) &
               (F.col("f.company_key_order_no")     == F.col("s.company_key_order_no")) &
               (F.col("f.order_type")               == F.col("s.order_type")) &
               (F.col("f.document_order_invoice_e") == F.col("s.document_order_invoice_e")), "left")
         .join(b11.alias("e"),
               (F.col("f.company_key_order_no")     == F.col("e.company_key_order_no")) &
               (F.col("f.document_order_invoice_e") == F.col("e.document_order_invoice_e")) &
               (F.col("f.order_type")               == F.col("e.order_type")) &
               (F.col("f.line_number")              == F.col("e.line_number")) &
               (F.col("f.shipment_number")          == F.col("e.shipment_number")), "left")
         .join(uom.alias("u"),
               (F.col("f.identifier_short_item")    == F.col("u.identifier_short_item")) &
               (F.col("f.uom_as_input")             == F.col("u.from_uom")), "left"))

    proj = j.select(
        F.col("f.company_key_order_no"),
        F.col("f.document_order_invoice_e"),
        F.col("f.order_type"),
        F.col("f.line_number"),
        F.col("f.shipment_number"),
        F.col("f.cost_center"),
        F.col("f.address_number"),
        F.col("f.address_number_ship_to"),
        F.col("f.carrier"),
        F.col("f.status_code_last"),
        F.col("f.status_code_next").cast("long").alias("status_code_next"),
        F.col("f.line_type"),
        F.col("f.identifier_second_item"),
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
            F.col("u.conv_factor")        # F41002 item-specific; F41003 fallback via DAX RELATED
        ).alias("conversion_factor"),
    )

    w = Window.partitionBy(*LINE_KEYS)
    proj = (proj
        .withColumn("_has_nonnull_routing",
                    F.max(F.col("routing_date_release_julian").isNotNull().cast("int")).over(w))
        .filter(F.col("routing_date_release_julian").isNotNull() | (F.col("_has_nonnull_routing") == 0))
        .drop("_has_nonnull_routing"))
    return proj.distinct()

# ----------------------------------------------------------------------------
# 3) FACT SOURCES
# ----------------------------------------------------------------------------
# Each entry defines one CDF stream and the join keys used to find which
# F4211 order lines are affected when that source table changes.
# join_pairs: list of (source_col, f4211_col) tuples.
FACT_SOURCES = [
    {"silver": F4211,    "join_pairs": [(c, c) for c in LINE_KEYS]},
    {"silver": F4201,    "join_pairs": [("company_key_order_no",     "company_key_order_no"),
                                        ("document_order_invoice_e", "document_order_invoice_e"),
                                        ("order_type",               "order_type")]},
    {"silver": F5642B01, "join_pairs": [("shipment_number",          "shipment_number"),
                                        ("company_key_order_no",     "company_key_order_no"),
                                        ("order_type",               "order_type"),
                                        ("document_order_invoice_e", "document_order_invoice_e")]},
    {"silver": F5642B11, "join_pairs": [("company_key_order_no",     "company_key_order_no"),
                                        ("document_order_invoice_e", "document_order_invoice_e"),
                                        ("order_type",               "order_type"),
                                        ("line_number",              "line_number"),
                                        ("shipment_number",          "shipment_number")]},
    {"silver": F41002,   "join_pairs": [("identifier_short_item",    "identifier_short_item"),
                                        ("uom",                      "uom_as_input")]},
    {"silver": F4941,    "join_pairs": [("shipment_number",          "shipment_number")]},
]

def current_version(src):
    """Return the latest committed Delta version for a Silver table."""
    return spark.sql("DESCRIBE HISTORY {}".format(sname(src))).select(F.max("version")).first()[0]

def affected_lines(change_keys, join_pairs):
    """
    Given a DataFrame of changed keys from one source table's CDF batch, join
    against F4211 to find which LINE_KEY combinations in Gold are affected.

    Returns a distinct DataFrame of LINE_KEY tuples.  This is the set that
    recompute_fact() will DELETE from Gold and then recompute from scratch.
    """
    f, c = spark.read.table(sname(F4211)).alias("f"), change_keys.alias("c")
    cond = None
    for scol, fcol in join_pairs:
        eq = F.col("c.{}".format(scol)) == F.col("f.{}".format(fcol))
        cond = eq if cond is None else (cond & eq)
    return f.join(c, cond, "inner").select(*[F.col("f.{}".format(k)) for k in LINE_KEYS]).distinct()

# ----------------------------------------------------------------------------
# 4) RECOMPUTE (called only while holding _FACT_LOCK)
# ----------------------------------------------------------------------------
def recompute_fact(lines):
    """
    Recompute Gold fact rows for the given set of LINE_KEYS.

    Step 1 — DELETE: remove existing Gold rows for these LINE_KEYS via MERGE.
    Step 2 — APPEND: insert the freshly computed rows from build_fact().

    If build_fact() returns an empty DataFrame (e.g. the F4211 spine row was
    soft-deleted, or the F4941 INNER join no longer matches), Step 2 appends
    nothing — effectively hard-deleting those Gold rows.

    Must be called inside _FACT_LOCK to prevent concurrent DELETE+APPEND pairs
    from interleaving across threads.
    """
    base      = spark.read.table(sname(F4211)).join(lines, LINE_KEYS, "inner")
    new       = build_fact(base)
    dt        = DeltaTable.forName(spark, gname(FACT))
    line_cond = " AND ".join(["t.{0} = s.{0}".format(k) for k in LINE_KEYS])
    dt.alias("t").merge(lines.alias("s"), line_cond).whenMatchedDelete().execute()
    new.write.format("delta").mode("append").saveAsTable(gname(FACT))

# ----------------------------------------------------------------------------
# 5) STREAM BATCH HANDLER
# ----------------------------------------------------------------------------
def make_fact_handler(cfg, init_ver):
    """
    Build and return the foreachBatch handler for one Silver CDF source.

    For the F4211 spine stream:
      · Extract the changed LINE_KEYS from the batch.
      · Acquire _FACT_LOCK, then call recompute_fact().

    init_ver: the Delta version of this source at the time of the last full
      load.  Batches that contain only versions <= init_ver are skipped because
      those rows were already captured in the full load.  Set to -1 when
      resuming from checkpoint (no filtering needed).

    lines is cached with .cache() + .count() to materialise it once, preventing
    Spark from re-evaluating the affected_lines join multiple times.  The
    try/finally guarantees unpersist() even if recompute_fact() throws.
    """
    src    = cfg["silver"]
    keys   = [p[0] for p in cfg["join_pairs"]]

    def handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        if init_ver >= 0:
            batch_df = batch_df.filter(F.col("_commit_version") > init_ver)
        if batch_df.rdd.isEmpty():
            return
        changed = (batch_df
            .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
            .select(*keys).distinct())
        t0         = time.time()
        lines      = affected_lines(changed, cfg["join_pairs"]).cache()
        line_count = lines.count()
        if line_count == 0:
            lines.unpersist()
            return
        try:
            with _FACT_LOCK:
                recompute_fact(lines)
        finally:
            lines.unpersist()
            print("[{}] batch={} lines={} {:.1f}s".format(src[:12], batch_id, line_count, time.time() - t0))
    return handler

# ── is_spine guard — DISABLED (confirmed 2026-07-03: merge keys are never updated) ──
# Background: if a LINE_KEY column were ever changed in JDE, Delta CDF would emit an
# update_preimage event with the OLD identity.  The handler above processes only
# postimage events, so the old Gold row would be orphaned.  The is_spine guard was the
# fix: for F4211, extract LINE_KEYS from update_preimage + delete events and explicitly
# delete those old identities before recompute_fact inserts the new ones.
#
# WHY DISABLED: the team confirmed (2026-07-03) that JDE merge key columns are natural
# business keys that identify an order line for its entire lifetime and are never updated.
#
# To re-enable: see the full guard block in nb_silver_to_gold_eso7_v2.py (archive).

# ----------------------------------------------------------------------------
# 6) RUN
# ----------------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

# Stop any leftover streams from a previous run in the same session.
_our_names = {"fact__" + cfg["silver"] for cfg in FACT_SOURCES}
_stopped = []
for _q in list(spark.streams.active):
    if _q.name in _our_names:
        _q.stop()
        _stopped.append(_q.name)
if _stopped:
    print("Stopped leftover streams: {}".format(_stopped))

def _checkpoints_exist():
    try:
        return bool(mssparkutils.fs.ls(CKPT_ROOT))
    except Exception:
        return False

if MANUAL_OVERWRITE or not spark.catalog.tableExists(gname(FACT)) or not _checkpoints_exist():
    print("== FULL LOAD ==")
    new = build_fact(spark.read.table(sname(F4211)))
    (new.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").option("delta.enableChangeDataFeed", "true")
       .saveAsTable(gname(FACT)))
    print("  {} rows={}".format(FACT, new.count()))

    _all_src  = [cfg["silver"] for cfg in FACT_SOURCES]
    _init_ver = {src: current_version(src) for src in _all_src}
    print("  init versions: {}".format(_init_ver))

    try:
        mssparkutils.fs.rm(CKPT_ROOT, True)
        print("  checkpoints cleared")
    except Exception as e:
        print("  checkpoint clear skipped: {}".format(e))
else:
    print("== resuming from checkpoint ==")
    _init_ver = {}

print("== starting {} streams (availableNow — processes all pending data then stops) ==".format(len(FACT_SOURCES)))

_run_start = time.time()
_queries = []
for cfg in FACT_SOURCES:
    src = cfg["silver"]
    iv  = _init_ver.get(src, -1)
    # sv  = iv if iv >= 0 else "latest"
    # Always pass "latest" as startingVersion.
    # · Full load path  (_iv >= 0): full load captured the complete Silver
    #   state; "latest" tells Spark to only pick up new commits from here.
    #   Using init_ver instead caused the checkpoint to record
    #   "next = init_ver+1" which breaks resume when no new data has
    #   arrived yet (the next version does not exist).
    # · Resume path (_iv = -1): Spark ignores startingVersion entirely
    #   when a checkpoint exists — the checkpoint determines the position.
    #   Any valid value works; "latest" is the safest default.
    sv  = "latest"
    _q = (spark.readStream.format("delta")
        .option("readChangeFeed",  "true")
        .option("startingVersion", sv)
        .table(sname(src))
        .writeStream
        .foreachBatch(make_fact_handler(cfg, iv))
        .option("checkpointLocation", "{}/fact__{}".format(CKPT_ROOT, src))
        # .trigger(processingTime=TRIGGER)   # streaming mode — uncomment to revert (also restore TRIGGER above + awaitAnyTermination below)
        .trigger(availableNow=True)          # batch mode: processes all pending CDF data then stops
        .queryName("fact__" + src)
        .start())
    _queries.append(_q)
    print("  fact__{}  startingVersion={}  init_ver={}".format(src, sv, iv))

print("== all streams running — awaiting completion ==")
# spark.streams.awaitAnyTermination()   # streaming mode — uncomment to revert
for _q in _queries:                     # batch mode: wait for every stream to finish
    _q.awaitTermination()

_elapsed = round(time.time() - _run_start, 1)
print("== all streams complete  elapsed={:.1f}s ==".format(_elapsed))

# Collect any stream-level exceptions so the pipeline can detect failures.
_stream_errors = [
    "{}: {}".format(q.name, q.exception())
    for q in _queries if q.exception() is not None
]
_status = "with_errors" if _stream_errors else "ok"

# Return a structured JSON payload to the pipeline. Downstream activities
# read this via `@activity('nb_eso7_fact').output.result.exitValue` and can
# parse it with `@json(...)`.
# notebookutils.notebook.exit() terminates the notebook — keep it LAST.
_exit_payload = {
    "status":          _status,
    "streams_count":   len(_queries),
    "streams_errors":  _stream_errors,
    "elapsed_sec":     _elapsed,
    "end_time_utc":    datetime.now(timezone.utc).isoformat(),
}
print("exit payload:", json.dumps(_exit_payload))
notebookutils.notebook.exit(json.dumps(_exit_payload))

