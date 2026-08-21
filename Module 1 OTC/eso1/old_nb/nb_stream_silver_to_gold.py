#!/usr/bin/env python
# coding: utf-8

# ## nb_stream_silver_to_gold
#
# **PRIMARY runtime** for the Extended Sales Order 1 (Billable v Payable Freight)
# Gold layer. Always-on Structured Streaming, Silver → Gold, CDC via Change Data
# Feed → `foreachBatch` → Delta MERGE/UPSERT (insert / update / soft-delete).
#
# Run order: seed once with `nb_backfill_gold_eso1`, then start this. Design: docs/ESO1_gold_layer_design.md

# In[1]:


# Pull in the shared transform + MERGE library (functions only, no side effects).
get_ipython().run_line_magic('run', 'nb_eso1_transforms')

from datetime import datetime
from pyspark.sql import functions as F

# Silver tables expose a Change Data Feed; we read only changed rows each microbatch.
CKPT     = "Files/_checkpoints/eso1"          # one sub-path per stream (never shared)
TRIGGER  = {"processingTime": "30 seconds"}    # NRT; swap to {"continuous": "1 second"} for lowest latency
print("Streaming controller — targets:", T_FACT)


# In[2]:


# ── PREFLIGHT: REUSED dimensions must exist before starting the streams ───────
# The fact this stream produces relates to REUSED dims on natural keys; they are
# NOT streamed here (their old_nb jobs own them). Hard-fail at startup if a base
# reused dim is missing/empty, so an always-on stream never emits a fact whose FKs
# point at absent dimensions. Role views are best-effort (SQL-endpoint-only is OK).
def _exists(fqn):
    try:
        if spark.catalog.tableExists(fqn):
            return True
    except Exception:
        pass
    try:
        spark.read.table(fqn).limit(1).collect()
        return True
    except Exception:
        return False

_errs = []
for tbl in [R_DIM_AB, R_DIM_PLANT]:
    if not _exists(tbl):
        _errs.append(f"MISSING {tbl}"); print(f"  MISSING : {tbl}"); continue
    n = spark.read.table(tbl).count()
    if n == 0:
        _errs.append(f"EMPTY {tbl}")
    print(f"  OK      : {tbl:38s} rows={n:,}")
for v in [R_DIM_SHIP_TO, R_DIM_SOLD_TO, R_DIM_CARRIER]:
    print(f"  {'OK     ' if _exists(v) else 'no-spark'} : {v}  (reused role view)")
if _errs:
    raise Exception("Reused-dimension preflight FAILED — start aborted: " + "; ".join(_errs)
                    + ". Build/refresh via old_nb (nb_dim_address_book, nb_dim_plant) first.")
print("✓ reused-dimension preflight passed — starting streams")


# In[3]:


# =============================================================================
# foreachBatch — DIMENSION  (only dim_item is built here)
# REUSED dims (dim_address_book + role views, dim_plant in lh_jde_gold.rpt) are
# streamed/refreshed by their OWN existing old_nb jobs — not this notebook.
# =============================================================================
def upsert_dim_item(batch_df, batch_id):
    """F4101 CDF batch -> refresh dim_item for the changed items."""
    run_dt = datetime.now()
    itm = batch_df.select(F.col("identifier_short_item")).distinct().where(F.col("identifier_short_item").isNotNull())
    if itm.rdd.isEmpty():
        return
    cdc_merge(spark, transform_dim_item(spark, run_dt, restrict_item=itm),
              T_DIM_ITEM, "item_number_short", run_dt, soft_delete_scope_col="item_number_short")


# In[4]:


# =============================================================================
# foreachBatch — FACT (driven by F4211 changes)
# =============================================================================
def upsert_fact_from_f4211(batch_df, batch_id):
    """
    F4211 CDF batch -> recompute the changed ORDERS' lines and MERGE the fact.
    Soft-delete is scoped to the changed orders (order_scope_key), so a line
    removed from a changed order is flagged is_deleted=true without touching
    unrelated orders. (branch_plant resolves against the reused rpt.dim_plant.)
    """
    run_dt = datetime.now()
    orders = (batch_df.select(
                F.col("company_key_order_no"),
                F.col("order_type"),
                F.col("document_order_invoice_e").alias("order_number"))
              .distinct())
    if orders.rdd.isEmpty():
        return
    fact = transform_fact(spark, run_dt, restrict_orders=orders)
    cdc_merge(spark, fact, T_FACT, "sales_order_line_key", run_dt,
              cluster_by=["branch_plant", "shipment_number"], soft_delete_scope_col="order_scope_key")


def upsert_fact_from_f4981(batch_df, batch_id):
    """
    F4981 (freight) CDF batch -> the freight $ for some shipments changed, but the
    order lines may be unchanged. Map changed shipments -> their orders (F4211) and
    re-run the SAME fact transform so the denormalized freight buckets refresh.
    Full-row MERGE on sales_order_line_key (idempotent). Concurrency with the
    F4211 writer is handled by Delta optimistic concurrency (design §6).
    """
    run_dt = datetime.now()
    ships = batch_df.select(F.col("shipment_number")).distinct().where(F.col("shipment_number").isNotNull())
    if ships.rdd.isEmpty():
        return
    f4211 = load_silver_table(spark, F4211_TBL).filter(F.col("company").isin(*COMPANIES))
    orders = (f4211.join(ships.alias("s"), f4211["shipment_number"] == F.col("s.shipment_number"), "left_semi")
              .select(F.col("company_key_order_no"), F.col("order_type"),
                      F.col("document_order_invoice_e").alias("order_number"))
              .distinct())
    if orders.rdd.isEmpty():
        return
    fact = transform_fact(spark, run_dt, restrict_orders=orders)
    # freight-only refresh: do NOT soft-delete here (the F4211 stream owns line deletes)
    cdc_merge(spark, fact, T_FACT, "sales_order_line_key", run_dt,
              cluster_by=["branch_plant", "shipment_number"], soft_delete_scope_col=None)


# In[5]:


# =============================================================================
# START STREAMS — one readStream per Silver source on its Change Data Feed
# =============================================================================
def start_cdf_stream(silver_table, ckpt_name, fn):
    return (spark.readStream.format("delta")
            .option("readChangeFeed", "true")
            .option("ignoreDeletes", "false")     # we WANT deletes -> soft-delete downstream
            .table(silver_table)
            .writeStream
            .option("checkpointLocation", f"{CKPT}/{ckpt_name}")
            .trigger(**TRIGGER)
            .foreachBatch(fn)
            .start())

q_item    = start_cdf_stream(F4101_TBL, "dim_item",    upsert_dim_item)
q_fact    = start_cdf_stream(F4211_TBL, "fact_f4211",  upsert_fact_from_f4211)
q_freight = start_cdf_stream(F4981_TBL, "fact_f4981",  upsert_fact_from_f4981)

print("Started 3 streams: dim_item, fact(F4211), fact-freight(F4981). "
      "Reused dims (rpt.dim_address_book/role views, rpt.dim_plant) refresh via their own jobs.")


# In[6]:


# =============================================================================
# HOLD THE SESSION — block until any stream terminates (always-on)
# =============================================================================
# In a scheduled/fallback context, replace the trigger with availableNow and drop
# this awaitAnyTermination so the job drains pending changes and stops.
spark.streams.awaitAnyTermination()
