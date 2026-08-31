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

# ## nb_silver_to_gold_cdf_dim_address_book
#
# null

# In[1]:


# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------
#
# Output table : lh_jde_gold.rpt.dim_address_book
# Grain        : one row per address_number (ABAN8) — current state
# Role         : Canonical Gold dimension for ALL address-book name lookups
#                across Project 1 reports (and Project 2 in future).
#
# Powers role-playing dimensions in the Power BI semantic model via SQL views
# (see cell 6 — 6 view-creation statements). ONE physical table; SIX logical
# tables in the model. No data duplication.
#
# SOURCES & WHY
#   F0101 (Address Book Master) — the NAME and identity attributes
#     • Keyed on ABAN8 (one row per address number)
#     • Brings ABALPH (name), ABAT1 (search type), ABSIC (SIC code)
#     • One row per ABAN8 — no fan-out risk
#
#   F0116 (Address by Date) — the POSTAL attributes (city/state/zip)
#     • Keyed on (ALAN8, ALEFTB) — multiple effective-dated rows per address
#     • FAN-OUT RISK: joining only on ALAN8 would multiply rows by N
#       effective-dated F0116 rows. The latest effective row is picked BEFORE
#       the join — same pattern as customer_order_line v2's F0116 fix.
#
# WHAT'S DELIBERATELY EXCLUDED
#   • No ABAT1 filter at the dim level. v7's customer_order_line applies
#     ABAT1 BETWEEN 'A'..'P' OR 'R'..'ZZZ' for the SHIP-TO role specifically.
#     That filter is ROLE-SPECIFIC and lives on the FACT, not the dim. The dim
#     holds ALL addresses (carriers, ports, employees, sold-tos) so every role
#     lookup can resolve a name.
#   • No deduplication beyond F0116 latest-effective. F0101's PK is ABAN8
#     (single column) — guaranteed unique.
#
# ── CDF state ────────────────────────────────────────────────────────────────
# 2026-08-17 — converted from nb_dim_address_book.py, which overwrote the dim on
# every run. Now TWO Change Data Feed streams (one per source) drive it, and the
# dim is rebuilt ONLY when one of them delivers a change. The checkpoints ARE
# the state, and deleting them is the only way to force a full load —
#     mssparkutils.fs.rm("Files/checkpoints/dimensions/dim_address_book", True)
# A full load also happens by itself when the gold table is missing, or when the
# checkpoint for EITHER source is absent.
#
# ANY change to F0101 or F0116 rebuilds the whole dim rather than merging row by
# row. The dim is ~100-300K rows, so a rebuild costs one short scan and cannot
# leave a stale row or a missed delete behind.
#
# Two sources, two checkpoints, ONE rebuild per run — the same shape
# nb_silver_to_gold_dim_second_item.py uses for F4211 + F42119.
#
# NOTE: nb_otc_facts_v3.py also reads F0101 and F0116, as refs. That is fine —
# this notebook keeps its own checkpoints and writes a different gold table.
# ----------------------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import re, time

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

F0101 = "f0101_address_book_master"
F0116 = "f0116_address_by_date"

DIM       = "dim_address_book"
CKPT_ROOT = "Files/checkpoints/dimensions/dim_address_book"
CKPT = {
    F0101: CKPT_ROOT + "/f0101",
    F0116: CKPT_ROOT + "/f0116",
}
QUERY_NAME = {
    F0101: "dim_address_book__f0101",
    F0116: "dim_address_book__f0116",
}

# Delta may compact its log between runs, removing the snapshot entry that CDF
# streaming uses to verify column mapping. Without this flag the SECOND run
# fails with DELTA_STREAMING_CHECK_COLUMN_MAPPING_NO_SNAPSHOT. Safe here
# because bronze->silver never renames a column.
spark.conf.set(
    "spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled",
    "true",
)

# HELPERS
#   - If is_delete exists -> filter to is_delete=0 (only for tables where
#     monthly_delete='Y' in bronze_to_silver_config.csv)
#   - Always drop is_delete + deleted_date_time from the projection
#
# F0101 and F0116 are monthly_delete='N' -> these columns shouldn't exist.
# The helper still handles them defensively in case anything changes.
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]


def load_silver_table(table_name):
    df = spark.read.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])


def checkpoint_exists(path):
    try:
        return bool(mssparkutils.fs.ls(path))
    except Exception:
        return False


def remove_checkpoint(path):
    try:
        mssparkutils.fs.rm(path, True)
        print("  removed {}".format(path))
    except Exception as e:
        print("  could not remove {} : {}".format(path, e))


def current_version(src):
    return spark.sql("DESCRIBE HISTORY {}".format(sname(src))).select(F.max("version")).first()[0]


# In[2]:


# ----------------------------------------------------------------------------
# 2) DIM BUILDER
# ----------------------------------------------------------------------------
DIM_KEY = "address_number"


def build_dim_address_book(run_dt):
    df_f0101 = load_silver_table(F0101)
    df_f0116 = load_silver_table(F0116)

    # ── F0116 FAN-OUT GUARD — pick the LATEST effective row per address_number ──
    # F0116 stores postal addresses on a date-effective basis. A single ALAN8 can
    # have MULTIPLE rows with different date_beginning_effective (ALEFTB) values.
    # Joining without picking ONE row per ALAN8 would MULTIPLY every F0101 row.
    #
    # ROW_NUMBER() over a window partitioned by address_number, ordered by:
    #   1. date_beginning_effective DESC (when the address became current)
    #   2. date_updated DESC             (tie-breaker — most recently maintained)
    # then keep rn = 1 -> exactly ONE F0116 row per address.
    #
    # .desc_nulls_last() pushes NULL dates to the end so they don't win ties.
    f0116_window = (
        Window
        .partitionBy("address_number")
        .orderBy(
            F.col("date_beginning_effective").desc_nulls_last(),
            F.col("date_updated").desc_nulls_last(),
        )
    )

    df_f0116_latest = (
        df_f0116
        .withColumn("_rn", F.row_number().over(f0116_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── MAIN JOIN — F0101 -> F0116 (LATEST effective), LEFT not INNER ──────────
    # F0101 is the master table — every address has a row there. F0116 may have
    # NO row for some addresses (system accounts, carriers with no postal
    # address). LEFT preserves ALL F0101 addresses; missing F0116 -> NULL
    # city/state. INNER would drop ~5-15% of addresses and BREAK the downstream
    # role copies (carrier name lookup would fail).
    #
    # v7 uses INNER because it has already applied an ABAT1 filter (Ship-To
    # role), so most surviving rows do have F0116 records. This dim has no such
    # filter — hence LEFT.
    df_joined = (
        df_f0101.alias("ab")
        .join(
            df_f0116_latest.alias("addr"),
            F.col("ab.address_number") == F.col("addr.address_number"),
            "left"
        )
    )

    # ── FINAL SELECT — only what Power BI needs, business-friendly names ───────
    df_dim = df_joined.select(
        # ── Identity (the join key for every fact role) ──────────────────────────
        F.col("ab.address_number").alias("address_number"),
        # PK — JDE ABAN8. Every fact column that holds an address number
        # (SDAN8, SDSHAN, SDCARS, BA55LODP, BA55OCCR, BA55DSTPT) joins here.

        # ── Name + classification (from F0101) ───────────────────────────────────
        F.trim(F.col("ab.name_alpha")).alias("name_alpha"),
        # ABALPH — the address name. THIS is the primary value Power BI shows
        # when a fact column references this address.

        F.trim(F.col("ab.address_type_01")).alias("address_type_01"),
        # ABAT1 — JDE search-type code. Exposed so the fact join can apply the
        # Ship-To filter; NOT filtered here.

        F.trim(F.col("ab.standard_industry_code")).alias("standard_industry_code"),
        # ABSIC — SIC industry code, resolved via the relationship rather than
        # carried on the fact.

        # ── Postal attributes (from F0116 — latest effective row) ────────────────
        F.trim(F.col("addr.city")).alias("city"),
        F.trim(F.col("addr.state")).alias("state"),
        F.trim(F.col("addr.country")).alias("country"),
        F.trim(F.col("addr.zip_code_postal")).alias("zip_code_postal"),
        F.trim(F.col("addr.address_line_01")).alias("address_line_01"),
        F.trim(F.col("addr.address_line_02")).alias("address_line_02"),
        F.trim(F.col("addr.address_line_03")).alias("address_line_03"),
        F.trim(F.col("addr.address_line_04")).alias("address_line_04"),

        # newly added for OTC
        F.trim(F.col("ab.report_code_add_book_005")).alias("category_code_05"),
        F.trim(F.col("ab.report_code_add_book_014")).alias("category_code_14"),
        F.trim(F.col("ab.user_reserved_amount")).alias("address_rate"),
        F.trim(F.col("ab.report_code_add_book_010")).alias("category_code_10"),
        F.trim(F.col("ab.address_number_third")).alias("related_address_3"),

        # newly added for AP
        F.trim(F.col("ab.report_code_add_book_001")).alias("category_code_01"),

        F.col("addr.date_beginning_effective").alias("address_effective_date"),
        # When the latest postal address became effective. Useful for DQ
        # investigations (e.g., "this address has been current since X").

        # ── DQ flag — was an F0116 record found? ─────────────────────────────────
        F.when(F.col("addr.address_number").isNotNull(), "Y").otherwise("N").alias("has_postal_address"),
        # 'Y' = F0116 row joined successfully (city/state populated)
        # 'N' = F0116 had no matching ALAN8 (city/state will be NULL)
    )

    df_final = df_dim.withColumn(
        "last_refreshed_timestamp", F.lit(run_dt).cast("timestamp")
    )

    # Put last_refreshed_timestamp at the leftmost position for visibility
    return df_final.select(
        "last_refreshed_timestamp",
        *[c for c in df_final.columns if c != "last_refreshed_timestamp"]
    )


# Result of this run, printed at the bottom.
#   built      the dim was rebuilt (first load, or F0101/F0116 changed)
#   no_change  nothing new in either source — the existing dim is already current
_result = {"status": "no_change", "rows": None}


def write_gold():
    """Overwrite the gold dim from the current F0101 + F0116 snapshots."""
    # Captured per REBUILD, not per run. The batch notebook stamped every row
    # with the notebook's start time even when nothing had changed; here the
    # column means "when this data was last actually rebuilt", which is the only
    # reading that stays true once runs become no-ops.
    run_dt = datetime.now()

    new = build_dim_address_book(run_dt)
    (new.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .option("delta.enableChangeDataFeed", "true")
       .saveAsTable(gname(DIM)))

    spark.sql("OPTIMIZE {}".format(gname(DIM)))

    _result["rows"]   = spark.read.table(gname(DIM)).count()
    _result["status"] = "built"
    print("  {} rows={} last_refreshed_timestamp={}".format(gname(DIM), _result["rows"], run_dt))


# In[3]:


# ----------------------------------------------------------------------------
# 3) STREAM HANDLERS
#
# One handler per source, built by the same factory so the two cannot drift.
#
# No sub-filter on either stream. F0101 is address-book rows only and F0116 is
# address-by-date rows only — every row in both is a candidate for this dim, so
# any change is genuinely ours. (Contrast dim_uss_plant, which reads F0005 and
# MUST narrow to 55/UP because that table carries every UDC type.)
#
# Rebuild at most once per run, across BOTH streams: AvailableNow can split a
# backlog into several batches, and the rebuild reads the CURRENT silver
# snapshot — which already contains every row those later batches carry. The
# F0116 stream drains after F0101, so without this guard a run where both
# tables changed would rebuild twice for the same result.
# ----------------------------------------------------------------------------
CHANGE_TYPES = ["insert", "update_postimage", "delete"]


def make_handler(src, init_version):
    def handler(batch_df, batch_id):
        changes = batch_df.filter(F.col("_change_type").isin(CHANGE_TYPES))
        if init_version >= 0:
            changes = changes.filter(F.col("_commit_version") > init_version)

        if changes.isEmpty():
            return

        if _result["status"] == "built":
            print("  [{}] batch {} — already rebuilt this run, skipping".format(src, batch_id))
            return

        print("  [{}] changes in batch {} — rebuilding".format(src, batch_id))
        write_gold()

    return handler


# ----------------------------------------------------------------------------
# 4) STREAM DRAIN
#
# Three outcomes, and telling them apart is the whole job:
#
#   no new data    KEEP the checkpoint — clearing it would make the next run
#                  think it had never run and full-load everything.
#   unreadable     the silver table was DROP+CREATE'd, or its log was compacted
#                  past our stored offset. Clear the checkpoint, full-load next run.
#   anything else  re-raise. An unknown failure must not be swallowed.
#
# "non-existent version N" is raised for two OPPOSITE reasons, so the message
# text alone cannot be trusted — compare N against the table instead:
#   N ahead of the table -> nothing new was written (delta only writes its own
#       internal checkpoint every 10 commits, so a quiet table lands here
#       instead of on DELTA_INVALID_CDC_RANGE). The checkpoint is VALID.
#   N behind the table   -> the log really was compacted. The checkpoint is dead.
# ----------------------------------------------------------------------------
NO_NEW_DATA = "DELTA_INVALID_CDC_RANGE"
UNREADABLE_OFFSET = (
    "DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE",
    "non-existent version",
)
VERSION_IN_MSG = re.compile(r"non-existent version[^0-9]*([0-9]+)")


def asks_for_future_version(msg, src):
    match = VERSION_IN_MSG.search(msg)
    if match is None:
        return False
    try:
        return int(match.group(1)) > current_version(src)
    except Exception:
        return False


def drain_stream(src, init_version):
    checkpoint = CKPT[src]
    query_name = QUERY_NAME[src]

    # startingVersion is set ONLY when starting fresh — NEVER on a resume.
    #
    # Fabric does NOT ignore startingVersion when a checkpoint exists, whatever
    # the docs say. Passing it on a resume makes delta use it as the batch START
    # while the checkpoint supplies the END, so the range comes out backwards:
    #
    #   [DELTA_INVALID_CDC_RANGE] CDC range from start 4377 to end 4369 was invalid.
    #
    # The stream then dies before reading anything, and the handler below reads
    # that failure as "no new data" — so the dim silently stops updating while
    # every run still reports success. This is the bug found in
    # nb_otc_facts_v3.py on 2026-08-13, where it had suppressed every
    # incremental read since the notebook was written.
    reader = spark.readStream.format("delta").option("readChangeFeed", "true")
    if not checkpoint_exists(checkpoint):
        # First start after a full load. init_version is the silver version
        # captured just before the rebuild, so the stream picks up only what
        # lands after the snapshot we already wrote.
        reader = reader.option("startingVersion", init_version)

    query = (
        reader.table(sname(src))
        .writeStream
        .foreachBatch(make_handler(src, init_version))
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .queryName(query_name)
        .start()
    )

    try:
        query.awaitTermination()
        print("  {} drained".format(query_name))
    except Exception as e:
        msg = str(e)
        if NO_NEW_DATA in msg or asks_for_future_version(msg, src):
            # Print the raw message too. This branch is a JUDGEMENT about an
            # exception, and hiding the evidence makes a wrong judgement invisible.
            print("  {} — no new data, checkpoint kept".format(query_name))
            print("      reason: {}".format(msg[:300].replace("\n", " ")))
        elif any(m in msg for m in UNREADABLE_OFFSET):
            print("  {} — offset unreadable, clearing checkpoint".format(query_name))
            remove_checkpoint(checkpoint)
        else:
            raise


# In[4]:


# ----------------------------------------------------------------------------
# 5) RUN
# ----------------------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

SOURCES = [F0101, F0116]

for _q in list(spark.streams.active):
    if _q.name in QUERY_NAME.values():
        _q.stop()
        print("Stopped leftover stream: {}".format(_q.name))

_run_start = time.time()

# Full load when there is nothing to resume from: a checkpoint missing for
# EITHER source, or the gold table does not exist yet. Both checkpoints are
# cleared together — a rebuild reads both snapshots, so resuming one stream
# against a dim built from a newer snapshot of the other would be incoherent.
if not all(checkpoint_exists(CKPT[s]) for s in SOURCES) or not spark.catalog.tableExists(gname(DIM)):
    print("== FULL LOAD ==")
    remove_checkpoint(CKPT_ROOT)
    # Capture the silver versions BEFORE building, so each stream starts from the
    # snapshot we are about to write and does not replay it.
    INIT = {s: current_version(s) for s in SOURCES}
    write_gold()          # sets _result — the full load IS this run's rebuild
    print("  init versions {}".format(INIT))
else:
    # -1 means "no floor" — the checkpoint already knows where each stream
    # stopped, so the handler needs no version filter.
    INIT = {s: -1 for s in SOURCES}
    print("== RESUME ==")

for _src in SOURCES:
    drain_stream(_src, INIT[_src])

if _result["status"] == "no_change":
    print("== no change in {} or {} — {} left as is ==".format(F0101, F0116, gname(DIM)))

print("== run complete in {}s — status={} rows={} ==".format(
    round(time.time() - _run_start, 1), _result["status"], _result["rows"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
