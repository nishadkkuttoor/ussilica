#!/usr/bin/env python
# coding: utf-8

# ## nb_backfill_gold_eso1
#
# One-time **seed** (and checkpoint-loss **recovery**) full load of the Extended
# Sales Order 1 Gold layer. Builds dim_date + all dims + the consolidated fact via
# the SAME transforms the stream uses, with batch MERGE. Run this BEFORE starting
# `nb_stream_silver_to_gold`. Design: docs/ESO1_gold_layer_design.md

# In[1]:


get_ipython().run_line_magic('run', 'nb_eso1_transforms')

from datetime import datetime
run_dt = datetime.now()
print(f"Backfill run timestamp : {run_dt}")


# In[2]:


# ── PREFLIGHT: REUSED dimensions must exist & be sound before seeding ─────────
# ESO1 relates the fact to REUSED conformed dims on natural keys and does NOT
# build them here. Verify the base dims (rpt.dim_address_book, rpt.dim_plant)
# exist, are non-empty, and have unique PKs; check the role views best-effort
# (they may live only in the SQL endpoint). Hard-fail on a missing/empty base dim
# so we never seed a fact whose FKs point at absent dimensions.
from pyspark.sql import functions as F

def _exists(fqn):
    try:
        if spark.catalog.tableExists(fqn):
            return True
    except Exception:
        pass
    try:                                   # role views may be SQL-endpoint-only
        spark.read.table(fqn).limit(1).collect()
        return True
    except Exception:
        return False

errors = []
# base reused dims — hard requirement: exist + non-empty + unique PK
for tbl, pk in [(R_DIM_AB, "address_number"), (R_DIM_PLANT, "plant_code")]:
    if not _exists(tbl):
        errors.append(f"MISSING base dim {tbl}"); print(f"  MISSING : {tbl}"); continue
    d = spark.read.table(tbl); n = d.count()
    dups = d.groupBy(pk).count().filter("count > 1").count()
    fresh = (d.agg(F.max("last_refreshed_timestamp")).first()[0]
             if "last_refreshed_timestamp" in d.columns else "n/a")
    if n == 0:    errors.append(f"EMPTY base dim {tbl}")
    if dups > 0:  errors.append(f"DUP PK in {tbl} ({dups})")
    print(f"  OK      : {tbl:38s} rows={n:,}  dup_{pk}={dups}  refreshed={fresh}")

# role views — best-effort (warn only; Direct Lake binds them via the SQL endpoint)
for v in [R_DIM_SHIP_TO, R_DIM_SOLD_TO, R_DIM_CARRIER]:
    print(f"  {'OK     ' if _exists(v) else 'no-spark'} : {v}  (reused role view)")

if errors:
    raise Exception("Reused-dimension preflight FAILED — fix before backfill: "
                    + "; ".join(errors)
                    + ". Build/refresh them via their old_nb jobs (nb_dim_address_book, nb_dim_plant).")
print("✓ reused-dimension preflight passed")


# In[3]:


# ── dim_date (static calendar — full overwrite each backfill) ─────────────────
dim_date = build_dim_date(spark, "2015-01-01", "2031-12-31")
(dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(T_DIM_DATE))
print(f"✓ dim_date : {dim_date.count():,} days")


# In[4]:


# ── NEW conformed dim: dim_item (item_number_short PK). dim_date built above. ──
# REUSED dims (rpt.dim_address_book + role views, dim_plant) were verified in the
# preflight (In[2]); they are built/refreshed by their existing old_nb jobs.
cdc_merge(spark, transform_dim_item(spark, run_dt), T_DIM_ITEM, "item_number_short", run_dt)
print("✓ dim_item loaded")


# In[5]:


# ── Consolidated fact — full open universe (no restrict_orders) ───────────────
fact = transform_fact(spark, run_dt)
print(f"Fact rows to load : {fact.count():,} | columns : {len(fact.columns)}")

dups = fact.groupBy("sales_order_line_key").count().filter("count > 1").count()
print(f"Duplicate sales_order_line_key : {dups}  ← must be 0")

cdc_merge(spark, fact, T_FACT, "sales_order_line_key", run_dt,
          cluster_by=["branch_plant", "shipment_number"])
print(f"✓ {T_FACT} seeded")


# In[6]:


# ── Quick smoke read ──────────────────────────────────────────────────────────
f = spark.read.table(T_FACT).filter(F.col("is_deleted") == False)
print(f"Active fact rows  : {f.count():,}")
print(f"Distinct shipments: {f.select('shipment_number').distinct().count():,}")
display(f.agg(
    F.round(F.sum(F.when(F.col("is_primary_shipment_line") == "Y", F.col("total_billable"))), 2).alias("total_billable_dedup"),
    F.round(F.sum(F.when(F.col("is_primary_shipment_line") == "Y", F.col("total_payable"))), 2).alias("total_payable_dedup"),
    F.round(F.sum(F.when(F.col("is_primary_shipment_line") == "Y", F.col("total_variance"))), 2).alias("total_variance_dedup"),
))
print("Backfill complete — now start nb_stream_silver_to_gold.")
