#!/usr/bin/env python
# coding: utf-8

# ## nb_maintenance_gold_eso1
#
# OPTIMIZE / VACUUM maintenance for the Extended Sales Order 1 Gold tables. Run on a
# SEPARATE hourly/nightly schedule to compact small files so Direct Lake stays fast and
# the F64 compute budget is protected. Design: docs/ESO1_gold_layer_design.md §7.
#
# SELF-CONTAINED — no %run; declares its own constants inline. Independent nb/ notebook
# (alongside nb_validate_gold_eso1 / nb_semantic_model_eso1); none depends on another
# resolving by name.

# In[1]:


from pyspark.sql import functions as F

# ── Self-contained constants (no %run nb_eso1_transforms) ─────────────────────
GOLD_SCHEMA = "lh_jde_gold.rpt"
RPT_SCHEMA  = "lh_jde_gold.rpt"
T_FACT      = f"{GOLD_SCHEMA}.fact_sales_order_freight"
T_DIM_ITEM  = f"{GOLD_SCHEMA}.dim_item"   # no dim_date — ESO1 has no date dimension
R_DIM_AB    = f"{RPT_SCHEMA}.dim_address_book"
R_DIM_PLANT = f"{RPT_SCHEMA}.dim_plant"

# Only tables THIS solution owns. Reused dims (rpt.dim_address_book, rpt.dim_plant)
# are optimized by their own maintenance jobs — explicitly OUT of scope here.
TABLES = [T_FACT, T_DIM_ITEM]
REUSED_DIMS = [R_DIM_AB, R_DIM_PLANT]        # NOT maintained here — status only
RETAIN_HOURS = 168   # 7 days time-travel
STALE_DAYS   = 7     # heads-up threshold for reused-dim freshness
print("Maintenance targets:", TABLES)


# In[2]:


# ── REUSED-DIMENSION CHECK (read-only) — confirm scope boundary + freshness ───
# Maintenance must NOT OPTIMIZE/VACUUM the reused dims (they belong to other jobs).
# Report their existence + last refresh so a stale one is visible; never touch them.
for tbl in REUSED_DIMS:
    if not spark.catalog.tableExists(tbl):
        print(f"  MISSING (owned by old_nb) : {tbl}")
        continue
    d = spark.read.table(tbl)
    if "last_refreshed_timestamp" in d.columns:
        info = (d.agg(F.max("last_refreshed_timestamp").alias("ts"))
                 .withColumn("age_days", F.datediff(F.current_timestamp(), F.col("ts"))).first())
        flag = "  ⚠ STALE" if (info["age_days"] is not None and info["age_days"] > STALE_DAYS) else ""
        print(f"  NOT MAINTAINED HERE : {tbl:34s} last_refreshed={info['ts']} ({info['age_days']}d){flag}")
    else:
        print(f"  NOT MAINTAINED HERE : {tbl}  (no last_refreshed_timestamp column)")
print("  → reused dims are maintained by their own old_nb jobs; skipped intentionally.\n")


# In[3]:


# OPTIMIZE = incremental liquid-clustering compaction (no ZORDER args needed).
# V-Order is ON by Fabric default. VACUUM reclaims superseded files + deletion vectors.
for t in TABLES:
    if not spark.catalog.tableExists(t):
        print(f"– skip (missing): {t}")
        continue
    print(f"OPTIMIZE {t} …")
    spark.sql(f"OPTIMIZE {t}")
    print(f"VACUUM {t} RETAIN {RETAIN_HOURS} HOURS …")
    spark.sql(f"VACUUM {t} RETAIN {RETAIN_HOURS} HOURS")
    print(f"✓ {t}")

print("✓ Maintenance complete.")
