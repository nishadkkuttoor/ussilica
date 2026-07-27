#!/usr/bin/env python
# coding: utf-8

# ## nb_maintenance_eso1
#
# New notebook — OPTIMIZE + VACUUM maintenance for the ESO1 near-real-time facts

# In[1]:


# =============================================================================
# WHAT THIS NOTEBOOK DOES
# =============================================================================
# Runs OPTIMIZE (liquid-clustering compaction) + VACUUM on the two ESO1 NRT
# fact tables. Kept SEPARATE from the 5-min load notebooks so the expensive
# compaction does NOT run every cycle. Scheduled nightly via pl_eso1_freight_maint.
#
#   • OPTIMIZE (no ZORDER args) compacts the many small files produced by the
#     frequent 5-min MERGEs and applies the CLUSTER BY clustering. V-Order is on
#     by default in Fabric. Keeps Direct Lake scans fast.
#   • VACUUM reclaims superseded MERGE files + deletion-vector tombstones older
#     than the retention window (168h = 7 days of time-travel preserved).
#
# The reused dimensions (dim_address_book, dim_plant, dim_item_cost_cascade) are
# maintained by their own (existing) project jobs — not touched here.
# =============================================================================

VACUUM_RETAIN_HOURS = 168   # 7 days time-travel; adjust per data-retention policy

TABLES = [
    "lh_jde_gold.otc.fact_sales_order_line",
    "lh_jde_gold.otc.fact_freight_audit",
    "lh_jde_gold.otc.dim_shipment",
]

for t in TABLES:
    if not spark.catalog.tableExists(t):
        print(f"⚠ {t} does not exist yet — skipped.")
        continue
    print(f"OPTIMIZE {t} …")
    spark.sql(f"OPTIMIZE {t}")
    print(f"VACUUM {t} RETAIN {VACUUM_RETAIN_HOURS} HOURS …")
    spark.sql(f"VACUUM {t} RETAIN {VACUUM_RETAIN_HOURS} HOURS")
    print(f"✓ {t}")

print("✓ Maintenance complete for all ESO1 NRT facts")
