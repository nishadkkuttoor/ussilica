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
# META           "id": "bed869e4-f15b-4cc1-9368-c7a9b3e08a83"
# META         },
# META         {
# META           "id": "915ea8b7-e01a-4182-b41a-c283df48a086"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_silver_to_gold_eso6_fact
# 
# **Output**: `lh_jde_gold.rpt.fact_extended_sales_order_6` (streaming,
# continuously refreshed).
# 
# ## Purpose
# 
# Builds and maintains the Gold-layer fact table for the Extended Sales Order 6
# report family. The fact holds the complete, clean dataset of sales-order
# lines joined to shipment routing and custom booking headers, so it can be
# reused across the current ESO6 report and any future report at the same
# grain without requiring changes or duplicate tables.
# 
# ## Design Rule compliance
# 
# Per the Gold Layer Design Rule, the fact contains ONLY universal exclusions
# (soft-delete filtering). No business-specific filters (Company, Order Type,
# Line Type, Status, Carrier, Mode of Transport, Booking presence, etc.) are
# applied at this layer. All business filters live in the Power BI semantic
# model or report page-level filters.
# 
# ## Grain
# 
# One row per (F4211 sales-order line × F4941 shipment routing step), with
# F5642B01 custom booking header attributes attached via LEFT JOIN (columns
# are NULL if the order has no custom booking header). A single order line
# with N routing steps produces N rows.
# 
# The `fact_key` surrogate hashes the 4-part line natural key only, so
# multiple fact rows for the same line share the same `fact_key`.
# 
# ## Consumers
# 
# Power BI semantic model `sm_extended_sales_order_6`, which drives two
# reports:
# * `Extended Sales Order 6` (core)
# * `Orders with a Dummy Route - Scheduler` (variation with identical data,
#   different audience layout)


# MARKDOWN ********************

# ## Cell 1 — Imports
# 
# `threading.Lock` serialises Gold writes and cache reloads across the three
# concurrent stream batch handlers. Without it, two batches could interleave
# `DELETE + APPEND` operations on the fact table and corrupt it.
# 
# `DeltaTable` is used inside `recompute_fact()` for the MERGE-based delete
# step. Every fact recompute is DELETE-then-APPEND under the lock.

# CELL ********************

import threading
import time
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 2 — Configuration
# 
# All environment-dependent identifiers live in this cell so they're easy to
# change per environment (dev → prod).
# 
# **Silver source**: `lh_jde_silver.jde_cdc` (CDF-enabled Delta tables). Every
# source table has:
# * `delta.enableChangeDataFeed = true` — required. The streaming pattern
#   reads via `readChangeFeed`.
# * `is_delete` column for soft-delete tracking. Deleted rows are retained
#   with the flag set to 1; the `drop_deleted()` helper below filters them
#   out at read time. Not all Silver tables have this column — the helper
#   handles both cases defensively.
# 
# **Gold target**: `lh_jde_gold.rpt` schema. The fact table itself has CDF
# enabled on write (see the full-load block in Cell 10) so downstream Gold
# tables or reports can chain via `readChangeFeed` if ever needed.
# 
# **Checkpoints**: `Files/checkpoints/eso6_fact/` — one sub-directory per
# stream. Do NOT delete these unless doing a full re-seed, otherwise streams
# will re-process history.
# 
# **Trigger**: 30 seconds matches the Bronze ingest cadence. Every 30 seconds
# each stream polls its Silver source's CDF log for new versions.
# 
# **`MANUAL_OVERWRITE`**: `True` = full seed + checkpoint clear (use on first
# run or intentional re-seed). `False` = normal restart from checkpoint.


# CELL ********************

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"
CKPT_ROOT     = "Files/checkpoints/eso6_fact"
TRIGGER       = "30 seconds"

# First-run seed control. Set to True on initial deploy or intentional re-seed;
# flip back to False for the SJD's regular restarts.
MANUAL_OVERWRITE = False

# Silver table names — all lowercase snake_case per Silver layer convention.
F4211    = "f4211_sales_order_detail_file"                            # spine — one row per order line
F4941    = "f4941_shipment_routing_steps"                             # child — one row per shipment routing step (multi-row per shipment for multi-leg routes)
F5642B01 = "f5642b01_custom_sales_order_entry_screen_header"          # header — one row per sales-order-header (4-part key excluding line number)

# Gold fact table + composite natural key columns from F4211.
FACT      = "fact_extended_sales_order_6"
LINE_KEYS = ["company_key_order_no", "document_order_invoice_e", "order_type", "line_number"]

# Silver tables whose CDF streams reload their reference cache and trigger a
# fact recompute. F4211 is NOT in this list — it's the spine, read fresh from
# Silver at each recompute, not cached.
_REF_TABLES = [F4941, F5642B01]

# Two module-level singletons shared across all stream batch handlers.
#   _FACT_LOCK  — threading.Lock so parallel batches (from the 3 concurrent
#                 streams) cannot interleave writes and corrupt the fact.
#   _REF_CACHE  — dict of {table_name: cached_DataFrame} populated by
#                 init_ref_cache(). Reloaded under _FACT_LOCK when the
#                 corresponding Silver source changes.
_FACT_LOCK = threading.Lock()
_REF_CACHE = {}

# Convenience helpers to build fully-qualified Silver / Gold table names.
def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 3 — Soft-delete filter helper
# 
# Silver tables use soft-delete semantics: a deleted row stays in the table
# with `is_delete = 1` set on it. This is how CDF captures "delete then
# re-insert" style behaviour cleanly.
# 
# This is a universal data-quality exclusion — the kind explicitly allowed by
# the Gold Layer Design Rule ("void, cancelled, test records, or invalid
# keys"). It is NOT a business filter.
# 
# Any code that JOINs or CACHES Silver data must call `drop_deleted()` first.
# Without it, soft-deleted rows would appear in Gold as if they still existed.
# 
# The `is_delete` column may not exist on every Silver table (e.g., a
# reference lookup that never had deletes). The helper defensively checks for
# the column and only filters if present. Tables without the column are
# returned unchanged.

# CELL ********************

def drop_deleted(df):
    """Filter out soft-deleted rows (is_delete = 1) before any join or cache.

    This is the ONLY row-level filter applied in the Gold layer. It's a
    universal data-quality exclusion, not a business filter — per the Gold
    Layer Design Rule.

    Safe on tables that don't have an is_delete column — returns df unchanged.
    """
    if "is_delete" in df.columns:
        # is_delete IS NULL means the row was never soft-deleted (older schema
        # or never touched by a delete op); is_delete != 1 means it's active.
        return df.filter(F.col("is_delete").isNull() | (F.col("is_delete") != 1))
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 4 — Reference table cache management
# 
# `F4941` and `F5642B01` are read once at session start via `init_ref_cache()`
# and stored in Spark memory (`.cache()` + `.count()` to force materialisation
# immediately). Caching means each 30-second batch doesn't have to re-scan
# multi-million-row Silver tables when nothing has changed.
# 
# **Cache invalidation**: when a reference table's own CDF stream fires
# (meaning that table just changed), the batch handler calls
# `reload_ref_cache()` — under the lock, BEFORE running `recompute_fact()` —
# so the very next fact build uses the fresh data.
# 
# **Memory footprint**: F4941 (~3M rows) + F5642B01 (~60K rows) fit
# comfortably in executor memory. If either grows dramatically, consider
# switching to Delta caching or pre-filtering the cache.

# CELL ********************

def init_ref_cache():
    """Read every reference table from Silver once and cache in Spark memory.

    Called once at session start (both on full-load path and resume path).
    Uses `.count()` to force materialisation — without this, the first
    batch would trigger the Silver scan, giving no cache benefit.
    """
    print("== initialising reference cache ==")
    for tbl in _REF_TABLES:
        # drop_deleted BEFORE caching so soft-deleted rows are never held in
        # memory — they cannot participate in any join.
        df    = drop_deleted(spark.read.table(sname(tbl))).cache()
        count = df.count()
        _REF_CACHE[tbl] = df
        print("  cached {} ({:,} rows)".format(tbl, count))
    print("== reference cache ready ==")


def reload_ref_cache(tbl):
    """Invalidate one reference table's cache entry, replace with fresh Silver read.

    Called INSIDE _FACT_LOCK by the corresponding stream's batch handler when
    that table's CDF stream detects a change. Because the caller holds the
    lock, build_fact() cannot run concurrently with this reload — so it
    always sees a consistent, up-to-date cache.
    """
    old = _REF_CACHE.get(tbl)
    if old is not None:
        # Free the old cache before re-caching to avoid memory doubling.
        old.unpersist()
    df    = drop_deleted(spark.read.table(sname(tbl))).cache()
    count = df.count()
    _REF_CACHE[tbl] = df
    print("  [cache reload] {} ({:,} rows)".format(tbl, count))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 5 — Fact builder (`build_fact`)
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
# **Business filters live in the Power BI report layer.** The following
# report-specific filters are NOT applied here — they are applied as page-
# level / semantic-model filters in Power BI:
# 
# | Column                                      | ESO6 report filter |
# |---------------------------------------------|--------------------|
# | `company_key_order_no`                      | IN ('00640', '00645') |
# | `order_type`                                | = 'SE'             |
# | `line_type`                                 | = 'S'              |
# | `status_code_next`                          | NOT IN ('980', '999') |
# | `mode_of_transport`                         | = 'OCE'            |
# | `carrier`                                   | = 20002565         |
# | `booking_no`                                | != '' (not blank)  |
# 
# **Grain**: one row per (F4211 line × F4941 routing step). A multi-leg
# shipment produces multiple fact rows for the same line — one per leg. The
# `fact_key` surrogate hashes the 4-part line natural key only, so multi-leg
# rows for one line share the same `fact_key`.
# 
# **Fan-out awareness**: Power BI measures that SUM line-level values (e.g.,
# `units_transaction_qty` from F4211) can double-count if a line has multiple
# routing steps. The report layer must use `SUMX(DISTINCT(...))` patterns or
# aggregate to line grain before summing.


# CELL ********************

def add_fact_key(df):
    """Add fact_key = MD5 hex of the 4 natural-key columns, placed FIRST in the schema.

    Purpose: give Power BI a single-column surrogate for relationships and
    MERGE operations. Provides a single-column key without losing the
    natural-key columns themselves (which stay on the fact for business
    queries and lineage).

    Multiple fact rows for the same order line (e.g., different F4941 routing
    steps) share the same fact_key — that's intentional and matches the
    business meaning that "one order line" is the natural entity, regardless
    of how many rows it produces in Gold.

    All inputs are cast to STRING and joined with a '||' separator so the hash
    is stable across data-type differences and future column additions.
    """
    keyed = df.withColumn(
        "fact_key",
        F.md5(F.concat_ws("||",
            F.col("company_key_order_no").cast("string"),
            F.col("document_order_invoice_e").cast("string"),
            F.col("order_type").cast("string"),
            F.col("line_number").cast("string"),
        ))
    )
    # Reorder columns so fact_key is first (Kimball convention: PK first).
    ordered_cols = ["fact_key"] + [c for c in keyed.columns if c != "fact_key"]
    return keyed.select(*ordered_cols)


def build_fact(f4211_df):
    """Join the F4211 spine to F4941 (INNER) and F5642B01 (LEFT), then project.

    NO business filters are applied here. Only the universal soft-delete
    exclusion is applied inside drop_deleted() when the caller reads Silver.

    Called both from the full-load path (with `spark.read.table(F4211)`) and
    from the incremental path (with a filtered subset of F4211 rows produced
    by affected_lines()).

    Returns a DataFrame of Gold-shaped rows with fact_key as the first column.
    """
    # Step 1 — apply only the universal soft-delete exclusion to the spine.
    # No business filters at this layer.
    f4211 = drop_deleted(f4211_df)

    # Step 2 — F4941 is already soft-delete-filtered when init_ref_cache() ran.
    # No pre-filter on carrier / mode of transport / route number.
    f4941 = _REF_CACHE[F4941]

    # Step 3 — F5642B01 is already soft-delete-filtered when init_ref_cache() ran.
    # No pre-filter on booking-not-blank; rows with blank/null bookings will
    # appear in the fact with NULL booking columns.
    f5642b01 = _REF_CACHE[F5642B01]

    # Step 4 — Join spine → F4941 (INNER on shipment_number) → F5642B01 (LEFT).
    #
    # INNER on F4941: F4211 lines without a matching shipment routing step are
    # excluded from the fact. This is a data-quality gate — a sales-order line
    # without any routing step is incomplete and cannot participate in any
    # shipment-oriented analysis.
    #
    # LEFT on F5642B01: an order line with no custom booking header row is
    # kept, with NULL booking / vessel / reference columns. Standard header-
    # to-line denormalization; no fan-out because F5642B01 grain is 4-part
    # header, not line-level.
    j = (f4211.alias("f")
        .join(f4941.alias("r"),
              F.col("f.shipment_number") == F.col("r.shipment_number"),
              "inner")
        .join(f5642b01.alias("s"),
              (F.col("f.shipment_number")          == F.col("s.shipment_number")) &
              (F.col("f.company_key_order_no")     == F.col("s.company_key_order_no")) &
              (F.col("f.order_type")               == F.col("s.order_type")) &
              (F.col("f.document_order_invoice_e") == F.col("s.document_order_invoice_e")),
              "left")
    )

    # Step 5 — project only the columns Gold needs, casting numeric-looking
    # ID columns from Silver's decimal(38,18) into their logical integer types.
    #
    # Silver inherits decimal(38,18) from the Oracle→CDF ingestion. In Power BI
    # this causes floating-point precision noise: the value 20002565 is
    # rendered/compared as 20002565.000000004, which breaks basic filter
    # equality (e.g., "carrier = 20002565" matches zero rows even when the row
    # is there). Casting to bigint here yields clean integer values that PBI
    # can filter, group, and display without noise. Precision is not lost —
    # these columns are logical JDE keys/counts, always whole numbers.
    #
    # Descriptions (order_type description, mode-of-transport display, status
    # description) are NOT joined here; they live in the dim tables and are
    # resolved via Power BI relationships.
    proj = j.select(
        # Natural key (4-part composite) — retained for business queries + audit.
        # fact_key surrogate is added by add_fact_key() below as the single-column PK.
        F.col("f.company_key_order_no"),                                            # STRING — leading zeros preserved
        F.col("f.document_order_invoice_e").cast("bigint")
             .alias("document_order_invoice_e"),                                    # JDE SDDOCO — order number, always integer
        F.col("f.order_type"),                                                      # STRING — FK to dim_order_type.order_type_code
        F.col("f.line_number").cast("decimal(10,3)")
             .alias("line_number"),                                                 # JDE SDLNID — line number, can be fractional (e.g. 1.5) but bounded
        F.col("f.shipment_number").cast("bigint")
             .alias("shipment_number"),                                             # JDE SDSHPN — shipment number, always integer

        # F4941 attributes (routing-step level — multiple rows possible per line)
        F.col("r.mode_of_transport"),                                               # STRING — FK to dim_mode_of_transport.mot_code
        F.col("r.carrier").cast("bigint").alias("carrier"),                         # JDE RSCARS — carrier number, always integer
        F.col("r.route_number").cast("bigint").alias("route_number"),               # JDE RSRTN — route number, always integer
        F.col("r.number_of_containers").cast("int").alias("number_of_containers"),  # JDE RSNCTR — container count, small integer

        # F5642B01 attributes (header-level; nullable via LEFT JOIN)
        F.col("s.booking_no"),                 # STRING — may be NULL or blank when no booking header exists
        F.col("s.vessel_name"),                # STRING
        F.col("s.reference_01"),               # STRING

        # F4211 attributes retained on the fact for PBI slicers / audit
        F.col("f.line_type"),                  # STRING
        F.col("f.status_code_next"),           # STRING — FK to dim_status.status_code (active)
        F.col("f.status_code_last"),           # STRING — FK to dim_status.status_code (inactive)
        F.col("f.cost_center"),                # STRING

        # Measure(s) from spine — cast to a display-friendly decimal.
        # JDE SDUORG is stored as decimal(38,18) which renders as noisy trailing
        # zeros in PBI ("924.000000000000000000"). decimal(18,4) preserves
        # enough precision for any real JDE quantity while displaying cleanly.
        F.col("f.units_transaction_qty").cast("decimal(18,4)")
             .alias("units_transaction_qty"),
    ).distinct()   # DISTINCT collapses exact-duplicate rows only

    # Step 6 — add fact_key surrogate as the first column
    return add_fact_key(proj)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 6 — Fact source definitions
# 
# `FACT_SOURCES` declares one entry per streaming CDF source. Each entry says:
# * `silver`: which Silver table this stream watches
# * `join_pairs`: how to map that source's columns back to F4211's LINE_KEYs
# 
# When a source's CDF batch arrives, the batch handler extracts the changed
# rows' column values (per `join_pairs[i][0]`) and joins back to F4211 (per
# `join_pairs[i][1]`) to find the LINE_KEYs of every fact row that needs
# recomputing.
# 
# **Why we don't just recompute the entire fact table**: it would waste
# compute. This design only touches the affected fact rows.

# CELL ********************

FACT_SOURCES = [
    # F4211 spine: changed keys ARE the LINE_KEYs (1-to-1)
    {"silver": F4211,    "join_pairs": [(c, c) for c in LINE_KEYS]},

    # F4941: a change on a shipment could affect all F4211 lines with that shipment
    {"silver": F4941,    "join_pairs": [("shipment_number",          "shipment_number")]},

    # F5642B01: 4-part key mapping — matches the fact's LEFT join above
    {"silver": F5642B01, "join_pairs": [("shipment_number",          "shipment_number"),
                                        ("company_key_order_no",     "company_key_order_no"),
                                        ("order_type",               "order_type"),
                                        ("document_order_invoice_e", "document_order_invoice_e")]},
]


def current_version(src):
    """Latest committed Delta version for a Silver table.
    Used at first-run time to record where the streams should start from."""
    return spark.sql("DESCRIBE HISTORY {}".format(sname(src))) \
                .select(F.max("version")).first()[0]


def find_stream_start_version(src, after_ver):
    """Return the smallest data-op version > after_ver on `src`, or None if none exists.

    Delta CDF has a bootstrap constraint: `readChangeFeed` requires the specified
    `startingVersion` to actually have CDF data recorded. That's TRUE only for:
      1. Data-modifying operations (WRITE, MERGE, UPDATE, DELETE, STREAMING UPDATE), AND
      2. Versions committed AFTER `delta.enableChangeDataFeed = true` was set.

    A `SET TBLPROPERTIES` commit that turns CDF on does NOT itself have CDF data
    (metadata-only) — pointing `startingVersion` at it fails with
    DELTA_MISSING_CHANGE_DATA. Pointing at a nonexistent future version fails
    with "Cannot time travel Delta table to version N".

    This helper walks history and returns the smallest data-op version > `after_ver`.
    Since it's called AFTER the full seed captured state through `after_ver`, any
    subsequent data-op is a valid stream start.

    Returns:
        int version number, or None if no data-op version > after_ver exists yet
        (source table hasn't been modified since we seeded).
    """
    hist = spark.sql("DESCRIBE HISTORY {}".format(sname(src))).collect()
    data_ops = {"WRITE", "MERGE", "UPDATE", "DELETE", "STREAMING UPDATE"}
    candidates = [int(r["version"]) for r in hist
                  if (r["operation"] or "") in data_ops
                  and int(r["version"]) > after_ver]
    return min(candidates) if candidates else None


def affected_lines(change_keys, join_pairs):
    """Find which fact rows need recompute given a set of changed source-side keys.

    Joins the changed keys against F4211 (the spine, read fresh from Silver each
    time — NOT from cache, because we want the very latest spine state at batch
    time).

    Returns a DataFrame with columns [fact_key, LINE_KEYS...] — the fact_key is
    added via add_fact_key() so callers can MERGE-delete Gold rows using the
    single-column surrogate.
    """
    f, c = spark.read.table(sname(F4211)).alias("f"), change_keys.alias("c")
    cond = None
    for scol, fcol in join_pairs:
        eq = F.col("c.{}".format(scol)) == F.col("f.{}".format(fcol))
        cond = eq if cond is None else (cond & eq)
    keys_df = (f.join(c, cond, "inner")
                .select(*[F.col("f.{}".format(k)) for k in LINE_KEYS])
                .distinct())
    return add_fact_key(keys_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 7 — Recompute (DELETE + APPEND under lock)
# 
# Given a set of `LINE_KEYs` that need rebuilding, this function:
# 1. Reads the spine rows for those keys (from Silver, live).
# 2. Runs `build_fact()` to compute the new Gold rows.
# 3. `DeltaTable.merge().whenMatchedDelete()` — removes the old Gold rows for
#    these keys, using fact_key as a single-column predicate.
# 4. Appends the newly computed rows.
# 
# **Why DELETE+APPEND instead of MERGE UPDATE**:
# * A single LINE_KEY may produce 0, 1, or many Gold rows (multiple routing
#   steps per line) — the row count can shift between refreshes.
# * If the F4941 INNER join no longer matches (e.g., the routing step got
#   deleted), the LINE_KEY should PRODUCE NO ROWS — and DELETE+APPEND with
#   empty APPEND handles this uniformly.
# * MERGE UPDATE would leave orphan rows in these cases.
# 
# **Concurrency**: this function MUST be called inside `_FACT_LOCK`. The batch
# handler wrapper enforces that.

# CELL ********************

def recompute_fact(lines):
    """Recompute Gold fact rows for the given set of affected lines.

    `lines` is a DataFrame with columns [fact_key, LINE_KEYS...] produced by
    affected_lines(). The merge uses fact_key (single-column surrogate) — one
    clean equality predicate.

    Step 1 — DELETE existing Gold rows whose fact_key appears in `lines`.
    Step 2 — APPEND freshly computed rows from build_fact() (they already
             carry fact_key via add_fact_key inside build_fact).

    If build_fact() returns empty (all affected spine rows got soft-deleted,
    or F4941 no longer matches), Step 2 appends nothing — the DELETE alone
    effectively removes those rows from Gold.

    IMPORTANT: must be called inside _FACT_LOCK. The stream batch handler
    wrapper ensures this.
    """
    # Fetch the affected spine rows fresh from Silver, joined on the natural
    # key (not fact_key — F4211 spine doesn't have fact_key, it's a Gold
    # surrogate).
    base = spark.read.table(sname(F4211)).join(lines.select(*LINE_KEYS), LINE_KEYS, "inner")
    new  = build_fact(base)

    # Single-column MERGE DELETE on fact_key.
    dt = DeltaTable.forName(spark, gname(FACT))
    (dt.alias("t")
       .merge(lines.select("fact_key").alias("s"), "t.fact_key = s.fact_key")
       .whenMatchedDelete()
       .execute())

    # Append the fresh rows (may be empty → effectively a hard delete for that line).
    new.write.format("delta").mode("append").saveAsTable(gname(FACT))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 8 — Batch handler factory
# 
# Spark's `foreachBatch` calls a handler function with `(batch_df, batch_id)`
# on every micro-batch. One handler is needed PER stream, generated from a
# factory that closes over the source config.
# 
# Handler steps per batch:
# 1. Skip if empty (nothing to do).
# 2. If just after a full load, skip any versions the full load already
#    covered (`_commit_version <= init_ver`).
# 3. Filter CDF events to insert / update_postimage / delete (skip
#    update_preimage — those are the pre-state we don't need).
# 4. Extract the changed keys from the batch.
# 5. Join back to F4211 to find LINE_KEYs to rebuild (this is
#    `affected_lines()`).
# 6. Under `_FACT_LOCK`:
#    a. If this handler watches a reference table, reload that table's cache
#       first.
#    b. Call `recompute_fact()`.
# 
# **Caching `lines`**: `.cache() + .count()` forces materialisation before
# taking the lock. Without this, Spark would re-evaluate the affected_lines
# join inside the critical section, holding the lock longer than necessary.
# 
# **try/finally on unpersist**: guarantees release of the cached DataFrame's
# memory even if `recompute_fact()` throws.


# CELL ********************

def make_fact_handler(cfg, init_ver):
    """Return a foreachBatch handler for one Silver CDF source.

    cfg      — one entry from FACT_SOURCES
    init_ver — Delta version at last full-load time (or -1 if resuming from
               checkpoint). Batches with _commit_version <= init_ver are
               skipped because those changes were already captured in the
               full seed.
    """
    src    = cfg["silver"]
    keys   = [p[0] for p in cfg["join_pairs"]]   # source-side columns to extract
    is_ref = src in _REF_TABLES                  # True → this handler reloads cache first

    def handler(batch_df, batch_id):
        # Step 1: skip empty batches.
        if batch_df.rdd.isEmpty():
            return
        # Step 2: on first-run streams, skip versions already covered by the full load.
        if init_ver >= 0:
            batch_df = batch_df.filter(F.col("_commit_version") > init_ver)
        if batch_df.rdd.isEmpty():
            return

        # Step 3-4: filter to meaningful CDF events, extract distinct changed keys.
        changed = (batch_df
            .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
            .select(*keys).distinct())

        # Step 5: find affected LINE_KEYs (join changed keys back to spine).
        t0         = time.time()
        lines      = affected_lines(changed, cfg["join_pairs"]).cache()
        line_count = lines.count()
        if line_count == 0:
            # Nothing in F4211 matches these changed keys → no fact rows to touch.
            lines.unpersist()
            return

        # Step 6: acquire lock, optionally reload cache, then recompute.
        try:
            with _FACT_LOCK:
                if is_ref:
                    reload_ref_cache(src)
                recompute_fact(lines)
        finally:
            lines.unpersist()
            print("[{}] batch={} lines={} {:.1f}s".format(
                src[:12], batch_id, line_count, time.time() - t0))
    return handler

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 9 — Schema bootstrap + leftover stream cleanup
# 
# Two safety operations before the main run:
# 1. **Create Gold schema if missing** — first time this notebook runs in a
#    fresh lakehouse, the `rpt` schema may not exist yet.
# 2. **Stop leftover streams from previous notebook runs in the same
#    session** — Fabric notebooks can hold Spark session state across runs.
#    If a previous run started streams and errored out, they'll still be
#    active. This block cleanly stops them so the current run can start
#    fresh.

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

# Stop any leftover streams from a previous run in the same session.
_our_names = {"fact_eso6__" + cfg["silver"] for cfg in FACT_SOURCES}
_stopped = []
for _q in list(spark.streams.active):
    if _q.name in _our_names:
        _q.stop()
        _stopped.append(_q.name)
if _stopped:
    print("Stopped leftover streams: {}".format(_stopped))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 10 — Full load OR resume from checkpoint
# 
# Decides whether this run should do a **full seed** or **resume from
# checkpoint**:
# 
# * **Full seed** triggers when:
#   * `MANUAL_OVERWRITE = True` (operator forces it), OR
#   * Gold fact table doesn't exist yet, OR
#   * Checkpoint directory doesn't exist.
# 
# * **Resume** happens on any other run — the streams pick up from where they
#   left off.
# 
# **Full seed flow**:
# 1. Initialise reference cache (before `build_fact()`, so the initial rows
#    are correctly resolved).
# 2. Read ALL of F4211 as the initial spine, run `build_fact()` on it → get
#    initial Gold rows.
# 3. `mode("overwrite")` + `overwriteSchema` + enable CDF on the new table.
# 4. Record the current Delta version of each source table → streams start
#    from these versions.
# 5. Clear checkpoints so streams don't try to replay history that's already
#    in the fact.
# 
# **Resume flow**:
# 1. Just initialise the reference cache — the first batch will fill in any
#    missed changes from the CDF log.
# 2. `_init_ver = {}` → filter logic in the handler treats "no init_ver
#    known" as "process everything from the checkpoint".


# CELL ********************

def _checkpoints_exist():
    """Whether the checkpoint root directory has any content."""
    try:
        return bool(mssparkutils.fs.ls(CKPT_ROOT))
    except Exception:
        return False


if MANUAL_OVERWRITE or not spark.catalog.tableExists(gname(FACT)) or not _checkpoints_exist():
    print("== FULL LOAD ==")
    # Cache reference tables FIRST so the initial build_fact() call uses them.
    init_ref_cache()

    # Compute initial Gold rows from full spine + cached refs.
    new = build_fact(spark.read.table(sname(F4211)))
    (new.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .option("delta.enableChangeDataFeed", "true")
       .saveAsTable(gname(FACT)))
    print("  {} rows={}".format(FACT, new.count()))

    # Record start-versions for each stream so we skip already-covered CDF events.
    _all_src  = [cfg["silver"] for cfg in FACT_SOURCES]
    _init_ver = {src: current_version(src) for src in _all_src}
    print("  init versions: {}".format(_init_ver))

    # Only try to clear checkpoints if the path actually exists (avoids the
    # noisy PathNotFoundException stack trace on the true first run).
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
    # Cache references BEFORE streams start so the first batch is fast.
    init_ref_cache()
    # No init_ver known → handlers process all CDF events in the batch.
    _init_ver = {}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 11 — Start streams and wait
# 
# For each `FACT_SOURCE`:
# 1. Open a `readStream` on the Silver Delta table with `readChangeFeed = true`.
# 2. Set `startingVersion` to the recorded init_ver (full load path) or
#    `"latest"` (resume path).
# 3. Attach the batch handler generated in Cell 8.
# 4. Set the checkpoint location under `CKPT_ROOT/<stream_name>/`.
# 5. Configure the 30-second processing-time trigger.
# 6. Start the stream.
# 
# **`awaitAnyTermination()`** blocks the notebook indefinitely. In Fabric,
# this notebook should be deployed as a **Spark Job Definition** so it stays
# alive continuously. A watchdog pipeline should monitor it and restart if
# it dies.

# CELL ********************

print("== starting {} streams (trigger={}) ==".format(len(FACT_SOURCES), TRIGGER))

# Determine a safe startingVersion per source. See find_stream_start_version()
# docstring for the CDF bootstrap constraints.
#   On seed path (iv >= 0): find smallest data-op version > iv. If ANY source
#     lacks a valid next version, streams can't start — skip and let the next
#     run retry.
#   On resume path (iv == -1 for that source): checkpoint drives the reader,
#     startingVersion is just a placeholder.
sv_per_src = {}
missing_srcs = []
for cfg in FACT_SOURCES:
    src = cfg["silver"]
    iv  = _init_ver.get(src, -1)
    if iv >= 0:
        sv = find_stream_start_version(src, iv)
        if sv is None:
            missing_srcs.append((src, iv))
        sv_per_src[src] = sv
    else:
        sv_per_src[src] = "latest"

should_start_streams = True
if missing_srcs:
    # At least one source hasn't been modified since CDF was enabled → can't
    # stream. Seed already populated the Gold fact table with current state;
    # the notebook exits cleanly and the next scheduled run retries.
    should_start_streams = False
    for src, iv in missing_srcs:
        print("  No data-op version > {} yet on {} — nothing to stream.".format(iv, src))
    print("== at least one source needs a post-seed data change before streaming can start ==")
    print("== fact table populated with current state; re-run when Silver sources change ==")

if should_start_streams:
    for cfg in FACT_SOURCES:
        src = cfg["silver"]
        iv  = _init_ver.get(src, -1)
        sv  = sv_per_src[src]

        (spark.readStream.format("delta")
            .option("readChangeFeed",  "true")
            .option("startingVersion", sv)
            .table(sname(src))
            .writeStream
            .foreachBatch(make_fact_handler(cfg, iv))
            .option("checkpointLocation", "{}/fact_eso6__{}".format(CKPT_ROOT, src))
            .trigger(processingTime=TRIGGER)
            .queryName("fact_eso6__" + src)
            .start())

        print("  fact_eso6__{}  startingVersion={}  init_ver={}".format(src, sv, iv))

    print("== all streams running -- awaiting termination ==")
    spark.streams.awaitAnyTermination()
else:
    print("== batch run complete (seed only, no streams) ==")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
