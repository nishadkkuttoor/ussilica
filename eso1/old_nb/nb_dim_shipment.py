#!/usr/bin/env python
# coding: utf-8

# ## nb_dim_shipment
#
# New notebook — conformed shipment bridge dimension (near-real-time)

# In[1]:


# =============================================================================
# WHAT THIS NOTEBOOK BUILDS
# =============================================================================
# Output table : lh_jde_gold.otc.dim_shipment
# Grain        : ONE row per shipment_number (the conformed bridge key)
# Refresh      : current-state MERGE — run AFTER the two facts in the 5-min
#                load pipeline (dependsOn fact_sales_order_line + fact_freight_audit).
# Role         : BRIDGE between the two ESO1 facts so the report can slice
#                "Billable v Payable Freight by Sales Order / Shipment" at
#                shipment level. ONE physical bridge → 1:* relationship to BOTH
#                facts on shipment_number. Avoids a (discouraged) direct
#                fact-to-fact relationship.
# =============================================================================
# WHY BUILD IT FROM THE FACTS (not Silver)
# =============================================================================
# The two Gold facts are already filtered (open orders / invoiced freight). If
# we sourced shipment_number from Silver we'd create dim members the facts never
# reference (and risk missing none-the-less). Sourcing the DISTINCT union of
# shipment_number FROM BOTH facts guarantees referential integrity: every
# shipment a fact references has exactly one dim row, no orphans, no extras.
#
#   fact_sales_order_line : MANY lines per shipment  -> first(ignorenulls) attrs
#   fact_freight_audit    : ONE row per shipment     -> route_number + freight attrs
# Overlapping attrs (carrier/ship_to/branch_plant) COALESCE line-fact-first.
# =============================================================================
# REFRESH / WRITE STRATEGY
# =============================================================================
# MERGE on shipment_number: insert new shipments, update changed attrs (skip
# no-ops via record_hash). whenNotMatchedBySource flags is_active='N' for
# shipments no longer referenced by either fact (retained, not deleted, so
# existing relationships never break). Power BI filters is_active='Y'.
# =============================================================================

from datetime import datetime
from pyspark.sql import functions as F
from delta.tables import DeltaTable

run_dt = datetime.now()

SOL_TBL      = "lh_jde_gold.otc.fact_sales_order_line"
FREIGHT_TBL  = "lh_jde_gold.otc.fact_freight_audit"
TARGET_TABLE = "lh_jde_gold.otc.dim_shipment"

print(f"Run timestamp : {run_dt}")
print(f"Target table  : {TARGET_TABLE}")


# In[2]:


# ─── Load the two facts (active rows only) ────────────────────────────────────
# Bridge is built from CURRENT (not soft-deleted) fact rows.
df_sol = (
    spark.read.table(SOL_TBL)
    .filter(F.col("is_deleted") == False)
    .filter(F.col("shipment_number").isNotNull())
)
df_fr = (
    spark.read.table(FREIGHT_TBL)
    .filter(F.col("is_deleted") == False)
    .filter(F.col("shipment_number").isNotNull())
)
print(f"fact_sales_order_line active rows : {df_sol.count():,}")
print(f"fact_freight_audit active rows    : {df_fr.count():,}")


# In[3]:


# ─── Aggregate the order-line fact to shipment grain ──────────────────────────
# Many lines per shipment -> pick a representative (first non-null) per attribute.
df_sol_agg = (
    df_sol
    .groupBy("shipment_number")
    .agg(
        F.first("company",               ignorenulls=True).alias("sol_company"),
        F.first("carrier_number",        ignorenulls=True).alias("sol_carrier_number"),
        F.first("ship_to",               ignorenulls=True).alias("sol_ship_to"),
        F.first("branch_plant",          ignorenulls=True).alias("sol_branch_plant"),
        F.first("mode_of_transport",     ignorenulls=True).alias("mode_of_transport"),
        F.first("freight_handling_code", ignorenulls=True).alias("freight_handling_code"),
        F.first("loading_port",          ignorenulls=True).alias("loading_port"),
        F.first("ocean_carrier",         ignorenulls=True).alias("ocean_carrier"),
        F.first("port_of_destination",   ignorenulls=True).alias("port_of_destination"),
        F.first("vessel_name",           ignorenulls=True).alias("vessel_name"),
        F.first("booking_number",        ignorenulls=True).alias("booking_number"),
        F.first("seal_number",           ignorenulls=True).alias("seal_number"),
        F.first("actual_ship_date",      ignorenulls=True).alias("actual_ship_date"),
        F.first("sail_date",             ignorenulls=True).alias("sail_date"),
    )
    .withColumn("has_order_line", F.lit("Y"))
)

# ─── Aggregate the freight fact to shipment grain (already ~1/shipment) ───────
df_fr_agg = (
    df_fr
    .groupBy("shipment_number")
    .agg(
        F.first("company",        ignorenulls=True).alias("fr_company"),
        F.first("carrier_number", ignorenulls=True).alias("fr_carrier_number"),
        F.first("ship_to",        ignorenulls=True).alias("fr_ship_to"),
        F.first("branch_plant",   ignorenulls=True).alias("fr_branch_plant"),
        F.first("route_number",   ignorenulls=True).alias("route_number"),
        F.first("gl_date",        ignorenulls=True).alias("gl_date"),
    )
    .withColumn("has_freight", F.lit("Y"))
)


# In[4]:


# ─── Full-outer union of the shipment universe + COALESCE shared attrs ────────
df_bridge = (
    df_sol_agg.alias("s")
    .join(df_fr_agg.alias("f"), "shipment_number", "full_outer")
    .select(
        F.col("shipment_number"),
        # Shared attrs — prefer the order-line fact, fall back to freight
        F.coalesce(F.col("s.sol_company"),        F.col("f.fr_company")).alias("company"),
        F.coalesce(F.col("s.sol_carrier_number"), F.col("f.fr_carrier_number")).alias("carrier_number"),
        F.coalesce(F.col("s.sol_ship_to"),        F.col("f.fr_ship_to")).alias("ship_to"),
        F.coalesce(F.col("s.sol_branch_plant"),   F.col("f.fr_branch_plant")).alias("branch_plant"),
        # Order-line-only attrs
        F.col("s.mode_of_transport"),
        F.col("s.freight_handling_code"),
        F.col("s.loading_port"),
        F.col("s.ocean_carrier"),
        F.col("s.port_of_destination"),
        F.col("s.vessel_name"),
        F.col("s.booking_number"),
        F.col("s.seal_number"),
        F.col("s.actual_ship_date"),
        F.col("s.sail_date"),
        # Freight-only attrs
        F.col("f.route_number"),
        F.col("f.gl_date"),
        # DQ presence flags
        F.coalesce(F.col("s.has_order_line"), F.lit("N")).alias("has_order_line"),
        F.coalesce(F.col("f.has_freight"),    F.lit("N")).alias("has_freight"),
    )
)
print(f"Distinct shipments (bridge grain) : {df_bridge.count():,}")
dups = df_bridge.groupBy("shipment_number").count().filter(F.col("count") > 1).count()
print(f"Duplicate shipment_number : {dups}  ← must be 0")


# In[5]:


# ─── Add dim audit + record_hash, then order columns ──────────────────────────
attr_cols = [
    "company", "carrier_number", "ship_to", "branch_plant",
    "mode_of_transport", "freight_handling_code", "loading_port", "ocean_carrier",
    "port_of_destination", "vessel_name", "booking_number", "seal_number",
    "actual_ship_date", "sail_date", "route_number", "gl_date", "has_order_line", "has_freight",
]

df_src = (
    df_bridge
    .withColumn("is_active",                F.lit("Y"))
    .withColumn("last_refreshed_timestamp", F.lit(run_dt).cast("timestamp"))
    .withColumn("record_hash",
        F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in attr_cols]), 256))
    .select(
        "last_refreshed_timestamp", "shipment_number", *attr_cols, "is_active", "record_hash",
    )
)
df_src.printSchema()


# In[6]:


# ─── WRITE — current-state MERGE on shipment_number ───────────────────────────
spark.sql("CREATE SCHEMA IF NOT EXISTS lh_jde_gold.otc")   # schema-enabled lakehouse guard
if not spark.catalog.tableExists(TARGET_TABLE):
    (df_src.write
        .format("delta")
        .option("delta.enableDeletionVectors", "true")
        .clusterBy("shipment_number")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE))
    print(f"✓ Created {TARGET_TABLE} ({df_src.count():,} rows)")
else:
    tgt = DeltaTable.forName(spark, TARGET_TABLE)
    (tgt.alias("t")
        .merge(df_src.alias("s"), "t.shipment_number = s.shipment_number")
        .whenMatchedUpdateAll(condition="t.record_hash <> s.record_hash")
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceUpdate(set={                 # shipment no longer referenced by either fact
            "is_active":                F.lit("N"),
            "last_refreshed_timestamp": F.lit(run_dt).cast("timestamp"),
        })
        .execute())
    print(f"✓ MERGE complete into {TARGET_TABLE}")


# In[7]:


# ─── VALIDATION ───────────────────────────────────────────────────────────────
dim = spark.read.table(TARGET_TABLE)
print(f"Total rows : {dim.count():,} | active : {dim.filter(F.col('is_active')=='Y').count():,}")

dups = dim.groupBy("shipment_number").count().filter(F.col("count") > 1).count()
print(f"Duplicate shipment_number : {dups}  ← must be 0")

print("\nPresence breakdown (has_order_line × has_freight):")
display(dim.groupBy("has_order_line", "has_freight").count().orderBy("has_order_line", "has_freight"))

# Referential integrity — every active fact shipment must exist in the bridge
sol_ship = (spark.read.table(SOL_TBL).filter(F.col("is_deleted") == False)
            .select("shipment_number").distinct())
missing = sol_ship.join(dim, "shipment_number", "left_anti").count()
print(f"\nLine-fact shipments missing from bridge : {missing}  ← must be 0")

print("\nSample 10:")
display(dim.filter(F.col("is_active") == "Y").limit(10))


# In[8]:


# ─────────────────────────────────────────────────────────────────────────────
# POWER BI SEMANTIC MODEL — dim_shipment bridge
# ─────────────────────────────────────────────────────────────────────────────
# (reference notes — nothing executed)
#
# Relationships (One dim_shipment : Many fact, single cross-filter dim → fact):
#   dim_shipment.shipment_number  →  fact_sales_order_line.shipment_number
#   dim_shipment.shipment_number  →  fact_freight_audit.shipment_number
#
#   This single bridge lets a shipment selection filter BOTH facts at once —
#   enabling "Billable v Payable Freight by Sales Order / Shipment" without a
#   direct fact-to-fact relationship.
#
#   • Slice freight $ and order-line tons/revenue by the same shipment, carrier,
#     ship-to, branch_plant, mode, route, etc. from this dim.
#   • Filter is_active = 'Y' in the model so stale shipments drop out.
#   • Hide record_hash / last_refreshed_timestamp from report view.
#   • If you ALSO keep direct dim_address_book / dim_plant relationships on the
#     facts, leave dim_shipment's carrier/ship_to/branch_plant as descriptive
#     (display) attributes to avoid ambiguous filter paths — or relate
#     dim_shipment → dim_address_book if a single shipment-centric model is wanted.
# ─────────────────────────────────────────────────────────────────────────────
print("Power BI relationship notes — see cell comments above.")
