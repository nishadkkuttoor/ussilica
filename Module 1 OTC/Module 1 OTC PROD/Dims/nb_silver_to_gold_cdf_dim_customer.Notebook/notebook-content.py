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

# ## nb_silver_to_gold_cdf_dim_customer
#
# New notebook

# In[1]:


# ----------------------------------------------------------------------------
# 1) CONFIG
#
# Output table : lh_jde_gold.rpt.dim_customer
# Grain        : one row per address_number (AIAN8) — current state
#
# KEY DESIGN DECISION — two name columns:
#   customer_name_abbreviated  = F0111.WWMLNM (line_number_id = 0)
#                                The mailing name — what Hubble displays.
#                                Use this in PBI visuals for Customer Name.
#                                Fixes SOSTMEIER LOGISTICS vs LUHE MINERALS GMBH mismatch.
#
#   name_alpha                 = F0101.ABALPH
#                                The address book alpha name. Different from
#                                mailing name when a freight agent is involved.
#
# WHY F03012 AS BASE (not F0101 directly):
#   F03012 contains only A/R billing customer records.
#   F0101 includes ALL address types (ship-to children, carriers, ports, etc.).
#   Using F03012 as base prevents ship-to child records (e.g. PROFILTRA B.V. - PPG)
#   from appearing as sold-to customer names.
#
# Sources:
#   F03012  Customer Master by Line of Business  → billing/sold-to records only
#   F0101   Address Book Master                  → name_alpha, search_type, category codes
#   F0111   Who's Who (line 0 only)              → customer_name_abbreviated (mailing name)
#   F0004   UDC Header                           → UDC type descriptions
#   F0005   UDC Detail                           → UDC code descriptions
#
# NO BUSINESS FILTERS ARE APPLIED IN THIS NOTEBOOK.
#
# ── CDF state ────────────────────────────────────────────────────────────────
# 2026-08-19 — converted from nb_silver_to_gold_dim_customer.py, which overwrote
# the dim on every run. Now FIVE Change Data Feed streams (one per source) drive
# it, and the dim is rebuilt ONLY when one of them delivers a relevant change.
# The checkpoints ARE the state, and deleting them is the only way to force a
# full load —
#     mssparkutils.fs.rm("Files/checkpoints/dimensions/dim_customer", True)
# A full load also happens by itself when the gold table is missing, or when the
# checkpoint for ANY source is absent.
#
# ANY relevant change rebuilds the whole dim rather than merging row by row. The
# dim is one row per billing customer — small — so a rebuild costs one short scan
# and cannot leave a stale row or a missed delete behind.
#
# Five sources, five checkpoints, ONE rebuild per run — the same shape
# nb_silver_to_gold_cdf_dim_address_book.py uses for F0101 + F0116.
#
# ⚠ WHY THE STREAMS ARE SUB-FILTERED (see SUB_FILTER below)
#   Three of these tables carry far more than this dim consumes, and without a
#   filter every unrelated row would trigger a full rebuild:
#     F0004 / F0005 carry EVERY UDC type; this dim reads only H42 and 01.
#     F0111 carries every contact; this dim reads only line_number_id = 0.
#     F03012 carries every line of business; this dim reads only company '00000'.
#   Same reasoning dim_uss_plant applies when it narrows F0005 to 55/UP.
#   F0101 is NOT filtered: any address number could belong to a customer in
#   F03012, and deciding that needs the join — so every F0101 change is ours.
#
# NOTE: nb_otc_facts_v3.py also reads F0101 / F0005, as refs. That is fine —
# this notebook keeps its own checkpoints and writes a different gold table.
# ----------------------------------------------------------------------------

from pyspark.sql import functions as F
import re, time

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

# ── Silver sources ────────────────────────────────────────────────────────────
F03012 = "f03012_customer_master_by_line_of_business"
F0101  = "f0101_address_book_master"
F0111  = "f0111_address_book_who_is_who"
F0004  = "f0004_user_defined_code_types"
F0005  = "f0005_user_defined_code_values"

# ── Gold target BUILT here ────────────────────────────────────────────────────
DIM       = "dim_customer"

# ── CDF state ─────────────────────────────────────────────────────────────────
# Relative path, matching every other notebook in this folder. It resolves
# against the notebook's DEFAULT lakehouse, so if that attachment ever changes
# the checkpoints are simply not found and the next run full-loads. That is
# cheap here — a full load IS the rebuild this notebook already does — which is
# why this does not use the absolute GUID-pinned form nb_otc_facts_v3.py needs.
CKPT_ROOT = "Files/checkpoints/dimensions/dim_customer"
CKPT = {
    F03012: CKPT_ROOT + "/f03012",
    F0101:  CKPT_ROOT + "/f0101",
    F0111:  CKPT_ROOT + "/f0111",
    F0004:  CKPT_ROOT + "/f0004",
    F0005:  CKPT_ROOT + "/f0005",
}
QUERY_NAME = {
    F03012: "dim_customer__f03012",
    F0101:  "dim_customer__f0101",
    F0111:  "dim_customer__f0111",
    F0004:  "dim_customer__f0004",
    F0005:  "dim_customer__f0005",
}

# Only rows this dim actually consumes should trigger a rebuild. None = no
# filter, every change in that table is relevant. See the ⚠ note above.
UDC_TYPES = ["H42", "01"]
SUB_FILTER = {
    F03012: F.col("company") == "00000",
    F0101:  None,
    F0111:  F.col("line_number_id") == 0,
    F0004:  F.col("product_code").isin(UDC_TYPES),
    F0005:  F.col("product_code").isin(UDC_TYPES),
}

# Delta may compact its log between runs, removing the snapshot entry that CDF
# streaming uses to verify column mapping. Without this flag the SECOND run
# fails with DELTA_STREAMING_CHECK_COLUMN_MAPPING_NO_SNAPSHOT. Safe here
# because bronze→silver never renames a column.
spark.conf.set(
    "spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled",
    "true",
)

print("Gold dim_customer processor (CDF build) — target {}".format(gname(DIM)))


# HELPERS
#   - If is_delete exists -> filter to is_delete=0 (only for tables where
#     monthly_delete='Y' in bronze_to_silver_config.csv)
#   - Always drop is_delete + deleted_date_time from the projection
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
#
# The batch notebook's cells 3-7, unchanged, wrapped in one function so the
# stream handler can call it. Same sources, same filters, same join order, same
# final column list.
# ----------------------------------------------------------------------------
def build_dim_customer():
    # ── UDC lookup tables (F0004 + F0005) ────────────────────────────────────
    #
    # UDC join: F0004 (header) LEFT JOIN F0005 (codes)
    #   ON product_code (DTSY=DRSY) AND user_defined_codes (DTRT=DRRT)
    #
    # F0004.description_001 = the human-readable UDC type name (e.g. 'Territory')
    # F0005.user_defined_code = the code value (e.g. '01')
    # F0005.description_001   = the code description (e.g. 'NORTHEAST')
    df_f0004 = load_silver_table(F0004)
    df_f0005 = load_silver_table(F0005)

    df_udc = (
        df_f0004.alias("hdr")
        .join(
            df_f0005.alias("det"),
            (F.col("hdr.product_code")     == F.col("det.product_code")) &
            (F.col("hdr.user_defined_codes") == F.col("det.user_defined_codes")),
            how="left"
        )
        .select(
            F.col("hdr.product_code").alias("udc_product_code"),
            F.col("hdr.description_001").alias("udc_type_name"),
            F.trim(F.col("det.user_defined_code")).alias("udc_code"),
            F.trim(F.col("det.description_001")).alias("udc_description"),
        )
    )

    # Billing Address Type (H42)
    lu_billing_type = (
        df_udc
        .filter((F.col("udc_product_code") == "H42") & (F.col("udc_type_name") == "Billing Address Type"))
        .select(F.col("udc_code").alias("bat_code"), F.col("udc_description").alias("bat_description"))
    )

    # Territory (01)
    lu_territory = (
        df_udc
        .filter((F.col("udc_product_code") == "01") & (F.col("udc_type_name") == "Territory"))
        .select(F.col("udc_code").alias("terr_code"), F.col("udc_description").alias("terr_description"))
    )

    # ── F03012 (Customer Master) ─────────────────────────────────────────────
    #
    # Base table — only A/R billing customer records exist here.
    # Ship-to child records, carriers, and ports are NOT in F03012.
    # This is why we use F03012 as the base (not F0101 directly).
    #
    # NOTE: The original US Silica query had WHERE AIAN8 = 10162057 — this was
    # a test filter and is intentionally NOT included here.
    df_f03012 = (
        load_silver_table(F03012)
        .filter(F.col("company") == "00000")
        .select(
            F.col("address_number"),                           # AIAN8  — join key
            F.col("company"),                                  # AICO
            F.col("billing_address_type"),                     # AIBADT
            F.col("territory_id"),                             # AITERRID
            F.col("hold_orders_code"),                         # AIHOLD
            F.col("customer_status"),                          # AICUSTS
            F.trim(F.col("report_code_add_book_016")).alias("territory_code"),    # AIAC16
            F.trim(F.col("report_code_add_book_004")).alias("credit_analyst_code"), # AIAC04
            F.trim(F.col("report_code_add_book_005")).alias("sales_rep_code"),     # AIAC05
            F.trim(F.col("report_code_add_book_009")).alias("basin_code"),         # AIAC09
            F.trim(F.col("report_code_add_book_012")).alias("formation_code"),     # AIAC12
        )
        .distinct()
    )

    # ── F0101 (Address Book) ─────────────────────────────────────────────────
    #
    # Provides the alpha name and address type for each address number.
    # Joined to F03012 on address_number (ABAN8 = AIAN8).
    df_f0101 = (
        load_silver_table(F0101)
        .select(
            F.col("address_number").alias("f0101_address_number"),  # ABAN8
            F.trim(F.col("name_alpha")).alias("name_alpha"),        # ABALPH
            F.trim(F.col("address_type_01")).alias("search_type"),  # ABAT1
            F.trim(F.col("descrip_compressed")).alias("name_compressed"),  # ABDC
            F.col("standard_industry_code"),                        # ABSIC
        )
        .distinct()
    )

    # ── F0111 (Who's Who) — line_number_id = 0 only ──────────────────────────
    #
    # F0111 stores multiple name/contact entries per address number.
    # line_number_id = 0 is the PRIMARY mailing name of the company —
    # the name JDE prints on documents and what Hubble displays as Customer Name.
    #
    # WHY THIS FIXES SOSTMEIER LOGISTICS:
    #   For SOSTMEIER orders, F0101.name_alpha = 'LUHE MINERALS GMBH' (JDE company record)
    #   but F0111.name_mailing = 'SOSTMEIER LOGISTICS' (the mailing/agent name).
    #   Hubble resolves customer names via F0111.WWMLNM — not F0101.ABALPH.
    #   Using customer_name_abbreviated in PBI visuals matches Hubble exactly.
    #
    # replace(name_mailing, 'PARENT', '') removes the 'PARENT' suffix that JDE
    # appends to parent account mailing name entries.
    df_f0111 = (
        load_silver_table(F0111)
        .filter(F.col("line_number_id") == 0)          # WWIDLN = 0 → primary mailing name
        .select(
            F.col("address_number").alias("f0111_address_number"),  # WWAN8
            F.trim(
                F.regexp_replace(F.col("name_mailing"), "PARENT", "")
            ).alias("customer_name_abbreviated"),                   # WWMLNM (cleaned)
        )
        .distinct()
    )

    # ── Join all sources into dim_customer ───────────────────────────────────
    #
    # Join order:
    #   F03012  LEFT JOIN  F0101       ON address_number  → adds name_alpha, search_type
    #           LEFT JOIN  F0111       ON address_number  → adds customer_name_abbreviated
    #           LEFT JOIN  lu_billing  ON billing_address_type
    #           LEFT JOIN  lu_territory ON territory_code
    df_dim = (
        df_f03012
        # ── address book (F0101) ─────────────────────────────────────────────
        .join(
            df_f0101,
            df_f03012.address_number == df_f0101.f0101_address_number,
            how="left"
        )
        .drop("f0101_address_number")
        # ── mailing name (F0111 line 0) ──────────────────────────────────────
        .join(
            df_f0111,
            df_f03012.address_number == df_f0111.f0111_address_number,
            how="left"
        )
        .drop("f0111_address_number")
        # ── billing address type description ────────────────────────────────
        .join(
            lu_billing_type,
            df_f03012.billing_address_type == lu_billing_type.bat_code,
            how="left"
        )
        .drop("bat_code")
        # ── territory description ────────────────────────────────────────────
        .join(
            lu_territory,
            F.col("territory_code") == lu_territory.terr_code,
            how="left"
        )
        .drop("terr_code")
        # ── final column order ───────────────────────────────────────────────
        .select(
            "address_number",
            "customer_name_abbreviated",   # F0111.WWMLNM — use this in PBI visuals
            "name_alpha",                  # F0101.ABALPH — alternate/secondary name
            "name_compressed",
            "company",
            "search_type",
            "standard_industry_code",
            "billing_address_type",
            "bat_description",
            "territory_code",
            "terr_description",
            "territory_id",
            "hold_orders_code",
            "customer_status",
            "credit_analyst_code",
            "sales_rep_code",
            "basin_code",
            "formation_code",
        )
    )

    # The base count is returned alongside so the validation below can compare
    # without rebuilding df_f03012 a second time.
    return df_dim, df_f03012


# Result of this run, printed at the bottom.
#   built      the dim was rebuilt (first load, or a source changed)
#   no_change  nothing relevant in any source — the existing dim is already current
_result = {"status": "no_change", "rows": None}


def write_gold():
    """Validate, then overwrite the gold dim from the current silver snapshots."""
    df_dim, df_f03012 = build_dim_customer()

    # The batch notebook evaluated these DataFrames eight separate times across
    # its count() calls. Cache once — the validation below needs three passes and
    # the write needs a fourth.
    df_dim = df_dim.cache()

    # ── Validate ─────────────────────────────────────────────────────────────
    # Both asserts are KEPT from the batch notebook, unlike the one dropped in
    # nb_silver_to_gold_cdf_fact_customer_ledger. The difference is where they
    # sit: this one runs BEFORE an atomic overwrite, so a failure leaves the
    # existing dim untouched and kills the run loudly. There is no half-written
    # state to clean up, and a fan-out here would silently double every customer
    # in every report that relates to this dim.
    f03012_count = df_f03012.count()
    dim_count    = df_dim.count()
    if dim_count != f03012_count:
        df_dim.unpersist()
        raise AssertionError(
            "Row count mismatch — F03012: {:,}, dim_customer: {:,}. "
            "A join caused fan-out.".format(f03012_count, dim_count)
        )

    dup_count = (
        df_dim.groupBy("address_number").count()
        .filter(F.col("count") > 1).count()
    )
    if dup_count != 0:
        df_dim.unpersist()
        raise AssertionError(
            "Primary key violated — {} duplicate address_numbers found.".format(dup_count)
        )

    with_name = df_dim.filter(F.col("customer_name_abbreviated").isNotNull()).count()

    # ── Write ────────────────────────────────────────────────────────────────
    # enableChangeDataFeed is set so anything downstream can stream FROM this
    # dim the same way this notebook streams from silver. The batch version did
    # not set it.
    (df_dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .option("delta.enableChangeDataFeed", "true")
       .saveAsTable(gname(DIM)))

    spark.sql("OPTIMIZE {}".format(gname(DIM)))

    df_dim.unpersist()

    _result["rows"]   = spark.read.table(gname(DIM)).count()
    _result["status"] = "built"
    print("  {} rows={:,}  (matches F03012)".format(gname(DIM), _result["rows"]))
    print("  customer_name_abbreviated coverage : {:,} / {:,} ({}%)".format(
        with_name, dim_count, 100 * with_name // dim_count if dim_count else 0))


# In[3]:


# ----------------------------------------------------------------------------
# 3) STREAM HANDLERS
#
# One handler per source, built by the same factory so the five cannot drift.
#
# Each stream applies its own SUB_FILTER first — see the ⚠ note in the config
# cell. Without it, one unrelated UDC code or one non-primary contact row would
# rebuild the whole dim.
#
# Rebuild at most once per run, across ALL streams: AvailableNow can split a
# backlog into several batches, and the rebuild reads the CURRENT silver
# snapshot — which already contains every row those later batches carry. The
# streams drain one after another, so without this guard a run where three
# tables changed would rebuild three times for the same result.
# ----------------------------------------------------------------------------
CHANGE_TYPES = ["insert", "update_postimage", "delete"]


def make_handler(src, init_version):
    sub_filter = SUB_FILTER[src]

    def handler(batch_df, batch_id):
        changes = batch_df.filter(F.col("_change_type").isin(CHANGE_TYPES))
        if init_version >= 0:
            changes = changes.filter(F.col("_commit_version") > init_version)
        if sub_filter is not None:
            changes = changes.filter(sub_filter)

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
#   N ahead of the table → nothing new was written (delta only writes its own
#       internal checkpoint every 10 commits, so a quiet table lands here
#       instead of on DELTA_INVALID_CDC_RANGE). The checkpoint is VALID.
#   N behind the table   → the log really was compacted. The checkpoint is dead.
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

SOURCES = [F03012, F0101, F0111, F0004, F0005]

for _q in list(spark.streams.active):
    if _q.name in QUERY_NAME.values():
        _q.stop()
        print("Stopped leftover stream: {}".format(_q.name))

_run_start = time.time()

# Full load when there is nothing to resume from: a checkpoint missing for ANY
# source, or the gold table does not exist yet. All checkpoints are cleared
# together — a rebuild reads all five snapshots, so resuming one stream against
# a dim built from a newer snapshot of another would be incoherent.
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
    print("== no relevant change in any source — {} left as is ==".format(gname(DIM)))

print("== run complete in {}s — status={} rows={} ==".format(
    round(time.time() - _run_start, 1), _result["status"], _result["rows"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
