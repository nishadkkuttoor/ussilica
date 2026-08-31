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

# ## nb_silver_to_gold_cdf_fact_customer_ledger
#
# New notebook

# In[1]:


# ----------------------------------------------------------------------------
# 1) CONFIG
#
# fact_customer_ledger, lifted out of nb_otc_facts_v3 on 2026-08-19.
#
# WHY IT MOVED: F03B11 is a slow table and its stream was dragging the shared
# Module 1 drain.  Nothing about the fact itself changed — same Gold table name,
# same build, same keys, same DELETE+APPEND semantics.  Only the schedule is
# now its own.
#
# WHAT IS DELIBERATELY NOT THE DIM PATTERN: the other notebooks in this folder
# rebuild their whole target whenever CDF reports any change.  That is right for
# a four-column dim; it is wrong here, because this is a large AR ledger and a
# full overwrite per run is exactly the cost we are moving away from.  So this
# notebook keeps v3's incremental behaviour instead: each batch DELETEs the
# affected fact_keys from Gold and appends the rebuilt rows.
# ----------------------------------------------------------------------------

# CONFIG + CONSTANTS
import re, time
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"          # Silver schema — static batch snapshots
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

# ── Silver sources ────────────────────────────────────────────────────────────
F03B11    = "f03b11_customer_ledger"

# ── Gold target BUILT here ────────────────────────────────────────────────────
# Name unchanged from v3 — the Power BI semantic model and the
# dim_invoice_reconciliation relationship both point at it.
FACT        = "fact_customer_ledger"

# ── CDF state ─────────────────────────────────────────────────────────────────
# ⚠ THIS IS v3's OWN CHECKPOINT DIRECTORY, ON PURPOSE.
#
# Pointing at it means this notebook picks up exactly where the module left off:
# the Gold table already exists and the offset is still valid, so the first run
# RESUMES and costs one incremental batch instead of a full reload of a large
# ledger table.  The module no longer starts this stream, so there is no second
# writer.
#
# Two rules follow from that, and breaking either one is expensive:
#   1. Do NOT re-add fact_customer_ledger to nb_otc_facts_v3.  Two writers on one
#      Gold table interleave a DELETE from one with an APPEND from the other.
#   2. Do NOT clear Files/checkpoints/module1/fact_customer_ledger/ as part of
#      any module-wide checkpoint reset.  Losing it costs a full load here.
#
# To force a full load deliberately, remove the path below — the checkpoint IS
# the state.  A full load also happens by itself when the Gold table is missing.
#
# ABSOLUTE and pinned by GUID, matching v3.  A relative "Files/..." path resolves
# against whatever lakehouse happens to be the notebook's DEFAULT, so if that
# attachment ever changes the checkpoint is simply not found — and a not-found
# checkpoint means a silent full load, with no error to explain why.
_FILES_ROOT = (
    "abfss://9ea13355-c802-4ca5-883f-e5dbf8ecc720@onelake.dfs.fabric.microsoft.com/"
    "bed869e4-f15b-4cc1-9368-c7a9b3e08a83/Files"
)
CKPT_ROOT  = _FILES_ROOT + "/checkpoints/module1/fact_customer_ledger/" + F03B11
QUERY_NAME = "fact_customer_ledger__f03b11"

# Delta may compact its log between runs, removing the snapshot entry that CDF
# streaming uses to verify column mapping. Without this flag the SECOND run
# fails with DELTA_STREAMING_CHECK_COLUMN_MAPPING_NO_SNAPSHOT. Safe here
# because bronze→silver never renames a column.
spark.conf.set(
    "spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled",
    "true",
)

print(f"ESO4 Gold fact_customer_ledger processor (CDF build) — target {gname(FACT)}")


# HELPERS
def drop_deleted(df):
    if "is_delete" in df.columns:
        return df.filter(F.col("is_delete").isNull() | (F.col("is_delete") != 1))
    return df


def sk(*cols):
    # v3's builder — a PLAIN concat_ws("|"), not the sha2() the ESO4 batch
    # notebooks use.  dim_invoice_reconciliation writes invoice_scope_key with
    # this one, and both sides of that relationship must agree or it matches
    # zero rows.  See the note on SURROGATE KEYS below.
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
# 2) FACT BUILDER
#
# Grain  : one row per F03B11 pay item — JDE's confirmed PK
#          (RPKCO, RPDOC, RPDCT, RPSFX).
# Gold PK: company_key, invoice_number, document_type, document_pay_item
#
# The simplest fact in the module: one table, no joins, no aggregation, 1:1
# projection.  Nothing can fan out and nothing can go stale behind a passive ref
# — which is also why this one was safe to lift out on its own.
#
# ⚠ SURROGATE KEYS USE v3's sk(), NOT the original ESO4 notebook's.
#   The ESO4 batch notebooks define sk() as sha2(concat_ws("||", ...), 256);
#   v3 defines it as a plain concat_ws("|", ...).  dim_invoice_reconciliation —
#   still built by v3 — already writes PLAIN-TEXT invoice_scope_key, so hashing
#   here would give the two tables different values for the same invoice and the
#   Power BI relationship would match ZERO rows.  Both sides must use the same
#   builder.  Moving the fact into this notebook does NOT change that: the
#   partner table is still v3's.
#
# Relationship built in the semantic model (not here):
#   fact_customer_ledger[invoice_scope_key] -> dim_invoice_reconciliation[invoice_scope_key]
# ----------------------------------------------------------------------------
FACT_CUSTOMER_LEDGER_GOLD_LINE_KEYS = [
    "company_key",
    "invoice_number",
    "document_type",
    "document_pay_item",
]
FACT_CUSTOMER_LEDGER_SILVER_LINE_KEYS = [
    "company_key",
    "doc_voucher_invoice_e",
    "document_type",
    "document_pay_item",
]

# Every column read from the spine.  RPLNID note (kept from the original): for a
# consolidated invoice this identifies only ONE of the F4211 lines that rolled
# into the pay item — never use it for line-level tax attribution.
_CUSTOMER_LEDGER_COLS = [
    "company_key",              # RPKCO
    "doc_voucher_invoice_e",    # RPDOC
    "document_type",            # RPDCT
    "document_pay_item",        # RPSFX
    "company_key_original",     # RPOKCO
    "original_document_no",     # RPODOC
    "original_document_type",   # RPODCT
    "line_number",              # RPLNID
    "amount_taxable",           # RPATXA
    "amount_tax_exempt",        # RPATXN
    "amt_tax_02",               # RPSTAM
    "amount_gross",             # RPAG
    "tax_area_01",              # RPTXA1
    "date_service_currency",    # RPDSVJ
    "date_updated",             # RPUPMJ ─┐ jde_updated_ts
    "time_last_updated",        # RPTDAY ─┘
]


def build_fact_customer_ledger(spine_df):
    # ---------------------------------------------------------------------------
    # 2) LOAD F03B11 (universal exclusion only — is_delete)
    #
    # The caller hands the rows in: the whole table on a full load, or just the
    # changed pay items on a streaming batch. Same shape either way, so nothing
    # else in this function has to care which one it got.
    # ---------------------------------------------------------------------------
    df_f03b11 = drop_deleted(spine_df)

    # ---------------------------------------------------------------------------
    # 3) SELECT conformed columns
    #
    # JDE column mapping (RPKCO / RPDOC / RPDCT / RPSFX is F03B11's PK):
    #   RPKCO   -> company_key                (primary key — company for the AR entry)
    #   RPDOC   -> doc_voucher_invoice_e      (primary key — invoice / document number)
    #   RPDCT   -> document_type              (primary key — RI / RM / RC / etc.)
    #   RPSFX   -> document_pay_item          (primary key — pay item suffix e.g. 001, 002)
    #   RPOKCO  -> company_key_original       (originating F4211 company)
    #   RPODOC  -> original_document_no       (originating F4211 order document)
    #   RPODCT  -> original_document_type     (originating F4211 order type)
    #   RPLNID  -> line_number                (originating F4211 line — see NOTE)
    #   RPATXA  -> amount_taxable
    #   RPATXN  -> amount_tax_exempt
    #   RPSTAM  -> amt_tax_02                 (Silver name inherited from JDE metadata)
    #   RPAG    -> amount_gross
    #   RPTXA1  -> tax_area_01
    #   RPDSVJ  -> date_service_currency      (Service / Tax date)
    #
    # NOTE on RPLNID: F03B11's Original Line Number is populated with the F4211
    # line that originated the pay item. For consolidated invoices where many
    # F4211 orders roll into one invoice/pay item, RPLNID identifies only ONE of
    # those F4211 lines — the others do not appear here. Downstream reports that
    # need per-order tax attribution must aggregate at the invoice level, not
    # rely on RPLNID for line-level splits (see the ESO4 v1 -> v2 investigation).
    #
    # NOTE: the four PK columns are RENAMED but never CAST. fact_key is hashed
    # from the Silver names in the handler and from these Gold names in
    # recompute(), and a cast would change the string on one side only — the
    # delete would then match nothing and every update would append a duplicate
    # instead of replacing the row.
    # ---------------------------------------------------------------------------
    df_fact = (
        df_f03b11
        .select(
            # primary key
            F.col("company_key").alias("company_key"),
            F.col("doc_voucher_invoice_e").alias("invoice_number"),
            F.col("document_type").alias("document_type"),
            F.col("document_pay_item").alias("document_pay_item"),
            # originating F4211 order link
            F.col("company_key_original").alias("order_company"),
            F.col("original_document_no").alias("order_number"),
            F.col("original_document_type").alias("order_type"),
            F.col("line_number").alias("originating_line_number"),
            # measures
            F.col("amount_taxable").alias("amount_taxable"),
            F.col("amount_tax_exempt").alias("amount_tax_exempt"),
            F.col("amt_tax_02").alias("amount_tax"),
            F.col("amount_gross").alias("amount_gross"),
            # tax + date attributes
            F.col("tax_area_01").alias("tax_area"),
            F.col("date_service_currency").alias("service_tax_date"),
            # audit
            _jde_ts(F.col("date_updated"), F.col("time_last_updated")).alias(
                "jde_updated_ts"
            ),  # RPUPMJ + RPTDAY
        )
        .distinct()
    )

    # ---------------------------------------------------------------------------
    # 4) SURROGATE KEYS
    #
    # customer_ledger_key: built from the JDE PK. Stable across runs. Used as the
    #                      PBI Direct Lake relationship key when the fact is on
    #                      the many side of a dim relationship.
    #
    # invoice_scope_key:   built from (company_key, invoice_number, document_type).
    #                      Coarser than the PK — collapses multiple pay items on
    #                      one invoice. Used to relate this fact to
    #                      dim_invoice_reconciliation at invoice grain, so tax
    #                      amounts can be sliced by the F4211-derived invoice.
    #
    # order_scope_key:     built from (order_company, order_number, order_type).
    #                      Retained for future reports that link the AR entry
    #                      back to its originating F4211 order. Populated only when
    #                      RPODOC / RPODCT / RPOKCO are non-null (they are null
    #                      for AR-only entries that don't originate from a sales
    #                      order — receipts, manual adjustments, etc.).
    # ---------------------------------------------------------------------------
    df_fact = (
        df_fact
        .withColumn("customer_ledger_key",
                    sk("company_key", "invoice_number", "document_type", "document_pay_item"))
        .withColumn("invoice_scope_key",
                    sk("company_key", "invoice_number", "document_type"))
        .withColumn("order_scope_key",
                    F.when(
                        F.col("order_number").isNotNull()
                          & F.col("order_type").isNotNull()
                          & F.col("order_company").isNotNull(),
                        sk("order_company", "order_number", "order_type")
                    ))
    )

    # The original notebook's PK-uniqueness assert is deliberately NOT carried
    # over: on a stream it would kill the query mid-batch. The .distinct() above
    # is kept, and duplicates would show up as a rising COUNT(*) across runs.
    return df_fact


# Result of this run, printed at the bottom.
#   built      full load — first run, or the checkpoint / Gold table was missing
#   updated    at least one incremental batch changed rows
#   no_change  F03B11 had nothing new — the existing fact is already current
_result = {"status": "no_change", "rows": None, "ops": 0}


def _with_engine_columns(df):
    """fact_key + audit stamp, exactly as v3 writes them.

    fact_key = MD5 of the four Gold line-key values joined by "||".  The handler
    computes the same hash from the SILVER names, which hold the same values —
    that is what lets a DELETE find the row a rebuild is about to replace.
    """
    return (
        df.withColumn(
            "fact_key",
            F.md5(
                F.concat_ws(
                    "||",
                    *[F.col(k).cast("string") for k in FACT_CUSTOMER_LEDGER_GOLD_LINE_KEYS],
                )
            ),
        )
        .withColumn("last_update_date_time_utc", F.current_timestamp())
    )


# In[3]:


# ----------------------------------------------------------------------------
# 3) RECOMPUTE — DELETE + APPEND
#
# v3's generic_recompute, reduced to this fact.  Everything the generic version
# carried for OTHER facts is gone, because this one has none of it: no ref
# tables, no passive refs, no extra spines, no rebuild_from_silver.  What is left
# is the two steps that matter:
#
#   Step 1 — DELETE: MERGE Gold ON fact_key with whenMatchedDelete.
#   Step 2 — APPEND: write the freshly built rows.  Empty when every affected pay
#            item was deleted in Silver — then the DELETE alone is the whole
#            update, which is how a hard delete reaches Gold.
#
# DELETE-then-APPEND rather than a row-wise MERGE update is v3's contract and is
# kept: it is what makes a rebuild correct even when the number of Gold rows for
# a key changes.
# ----------------------------------------------------------------------------
def recompute(affected_keys_df, spine_df):
    sl_keys = FACT_CUSTOMER_LEDGER_SILVER_LINE_KEYS

    # The batch already carries the complete updated rows, so there is no Silver
    # re-read.  The INNER join still matters: a pay item that was DELETED is in
    # affected_keys (built from the unfiltered batch) but NOT in spine_df (which
    # holds insert/update_postimage only) — so it produces no rows, and the
    # DELETE below removes it from Gold with nothing appended in its place.
    filtered_spine = (
        spine_df.join(
            affected_keys_df.select(*sl_keys).distinct(), on=sl_keys, how="inner"
        )
        .select(*_CUSTOMER_LEDGER_COLS)
        .cache()
    )
    filtered_spine.count()

    try:
        new_rows = _with_engine_columns(build_fact_customer_ledger(filtered_spine)).cache()
        new_count = new_rows.count()

        (
            DeltaTable.forName(spark, gname(FACT))
            .alias("t")
            .merge(
                affected_keys_df.select("fact_key").distinct().alias("s"),
                "t.fact_key = s.fact_key",
            )
            .whenMatchedDelete()
            .execute()
        )

        if new_count > 0:
            new_rows.write.format("delta").mode("append").saveAsTable(gname(FACT))

        new_rows.unpersist()
        return new_count

    finally:
        filtered_spine.unpersist()


# ----------------------------------------------------------------------------
# 4) STREAM HANDLER
#
# Single-source fact, so only the SPINE path exists — the line keys are already
# in the CDF batch and never have to be mapped back through a ref table.
#
# All three change types are read.  insert/update_postimage rebuild the row;
# delete contributes its key to the DELETE set only.  A SOFT delete arrives as
# an update_postimage carrying is_delete=1 and is stripped by drop_deleted inside
# the build — same outcome, one row fewer.  Re-activation (is_delete 1 -> 0) is
# also an update_postimage, so it flows back in as a normal rebuild.
#
# No locks here.  v3 needed them because several streams could recompute one fact
# at once; this notebook runs a single query and foreachBatch is serial.
# ----------------------------------------------------------------------------
CHANGE_TYPES = ["insert", "update_postimage", "delete"]


def handle_f03b11(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    sub_batch = batch_df
    if INIT_VERSION >= 0:
        sub_batch = sub_batch.filter(F.col("_commit_version") > INIT_VERSION)
    if sub_batch.rdd.isEmpty():
        return

    sub_batch = sub_batch.cache()
    try:
        t0 = time.time()
        sl_keys = FACT_CUSTOMER_LEDGER_SILVER_LINE_KEYS

        changed_keys = (
            sub_batch.filter(F.col("_change_type").isin(CHANGE_TYPES))
            .select(*sl_keys)
            .distinct()
        )

        # Live rows passed straight to the build — no Silver re-read.
        # Dedup to the latest version per key, in case one pay item was updated
        # more than once inside the same trigger window.
        _dedup_w = Window.partitionBy(*sl_keys).orderBy(F.col("_commit_version").desc())
        live_spine = (
            sub_batch.filter(F.col("_change_type").isin("insert", "update_postimage"))
            .withColumn("_rn", F.row_number().over(_dedup_w))
            .filter(F.col("_rn") == 1)
            .drop("_rn", "_change_type", "_commit_version", "_commit_timestamp")
        )

        # Same hash as _with_engine_columns, computed from the Silver names —
        # the values behind them are identical, so the two agree.
        affected_keys = changed_keys.withColumn(
            "fact_key",
            F.md5(F.concat_ws("||", *[F.col(k).cast("string") for k in sl_keys])),
        ).cache()

        try:
            if affected_keys.count() == 0:
                return
            ops = recompute(affected_keys, live_spine)
        finally:
            affected_keys.unpersist()

        _result["ops"] += ops
        if _result["status"] == "no_change":
            _result["status"] = "updated"
        print("  [F03B11] batch={} ops={:,} {:.1f}s".format(
            batch_id, ops, time.time() - t0))

    finally:
        sub_batch.unpersist()


def write_gold_full():
    """Full load: rebuild the whole fact from the current F03B11 snapshot."""
    new = _with_engine_columns(
        build_fact_customer_ledger(
            spark.read.format("delta").option("versionAsOf", INIT_VERSION).table(sname(F03B11))
        )
    )
    (new.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .option("delta.enableChangeDataFeed", "true")
       .saveAsTable(gname(FACT)))
    _result["rows"]   = spark.read.table(gname(FACT)).count()
    _result["status"] = "built"
    print("  {} rows={:,}".format(gname(FACT), _result["rows"]))


# In[4]:


# ----------------------------------------------------------------------------
# 5) STREAM DRAIN
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


def drain_stream(src, checkpoint, handler, query_name, init_version):
    # startingVersion is set ONLY when starting fresh — NEVER on a resume.
    #
    # Fabric does NOT ignore startingVersion when a checkpoint exists, whatever
    # the docs say. Passing it on a resume makes delta use it as the batch START
    # while the checkpoint supplies the END, so the range comes out backwards:
    #
    #   [DELTA_INVALID_CDC_RANGE] CDC range from start 4377 to end 4369 was invalid.
    #
    # The stream then dies before reading anything, and the handler below reads
    # that failure as "no new data" — so the fact silently stops updating while
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
        .foreachBatch(handler)
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


# In[5]:


# ----------------------------------------------------------------------------
# 6) RUN
# ----------------------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

for _q in list(spark.streams.active):
    if _q.name == QUERY_NAME:
        _q.stop()
        print("Stopped leftover stream: {}".format(QUERY_NAME))

_run_start = time.time()

# Full load when there is nothing to resume from: no checkpoint, or the gold
# table does not exist yet.
#
# On the FIRST run after the move both already exist — the table was built by
# the module and the checkpoint is the module's — so this should print RESUME and
# process one ordinary incremental batch. If it prints FULL LOAD instead, stop
# and find out why before letting it run: it means the checkpoint above was not
# found, and the likely cause is a wrong workspace/lakehouse GUID in _FILES_ROOT.
if not checkpoint_exists(CKPT_ROOT) or not spark.catalog.tableExists(gname(FACT)):
    print("== FULL LOAD ==")
    remove_checkpoint(CKPT_ROOT)
    # Capture the silver version BEFORE building, so the stream starts from the
    # snapshot we are about to write and does not replay it.
    INIT_VERSION = current_version(F03B11)
    write_gold_full()     # sets _result — the full load IS this run's rebuild
    print("  init version {}".format(INIT_VERSION))
else:
    # -1 means "no floor" — the checkpoint already knows where the stream
    # stopped, so the handler needs no version filter.
    INIT_VERSION = -1
    print("== RESUME ==")

drain_stream(F03B11, CKPT_ROOT, handle_f03b11, QUERY_NAME, INIT_VERSION)

if _result["status"] == "no_change":
    print("== no change in {} — {} left as is ==".format(F03B11, gname(FACT)))

print("== run complete in {}s — status={} rows={} ops={:,} ==".format(
    round(time.time() - _run_start, 1), _result["status"], _result["rows"], _result["ops"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
