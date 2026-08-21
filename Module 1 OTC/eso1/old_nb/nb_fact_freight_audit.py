#!/usr/bin/env python
# coding: utf-8

# ## nb_fact_freight_audit
#
# New notebook — near-real-time current-state freight fact

# In[1]:


# =============================================================================
# WHAT THIS NOTEBOOK BUILDS
# =============================================================================
# Output table : lh_jde_gold.otc.fact_freight_audit
# Grain        : ONE CURRENT row per shipment_number  (F4981 pre-bucketed)
# Refresh      : scheduled every 5 min — incremental MERGE (current-state, NOT
#                a daily snapshot). No snapshot_date accumulation.
# Role         : the Billable-vs-Payable freight-$ fact for the Extended Sales
#                Order 1 (Billable v Payable Freight) report. Conforms to
#                dim_address_book (carrier/ship-to) and dim_plant; joins to
#                fact_sales_order_line via the degenerate shipment_number.
# =============================================================================
# WHY A SEPARATE FACT (not folded into the order-line fact)
# =============================================================================
# Freight dollars live at SHIPMENT grain; the order line is at LINE grain and
# many lines share a shipment. Joining freight onto each line would multiply
# (fan-out) the freight $. So we keep freight on its own shipment-grain fact and
# relate by shipment_number in Power BI.
# =============================================================================
# SOURCE & BUCKETS (verified against Silver data — ESO1_Silver_Data_Analysis)
# =============================================================================
# F4981 (Freight Audit History) — has is_delete (filtered to 0 by helper).
#   Gate : vendor_invoice_number <> 'NULL'   (literal text "NULL", space-padded)
#   Bucket = SUM(net_amount) by billable_payable x charge_code_01:
#       billable_freight : B  & BFR
#       billable_fuel    : B  & FSC/FSB
#       payable_freight  : P  & PFR
#       payable_fuel     : P  & FSC
#   Derived: total_billable, total_payable, freight_variance, total_variance
#   CM% ratios are computed in DAX (SUM/SUM) — NOT stored per row.
# =============================================================================
# ShiftFactor NOTE
# =============================================================================
# Silver net_amount is ALREADY implied-decimal resolved (e.g. 3500.00). The
# per-company "ShiftFactor" referenced in the Hubble tie-out is a SEPARATE
# business multiplier from a company-constants table NOT among the 11 sources.
# Until that table is wired in we apply shift_factor_applied = 1.0 (identity) —
# we do NOT re-apply 0.01 (that was the raw-Oracle implied-decimal scaling,
# already done in Silver; re-applying would double-scale). TODO: join the real
# per-company factor to tie out exactly to Hubble.
# =============================================================================
# REFRESH / WRITE STRATEGY (current-state MERGE)
# =============================================================================
# Full current-state recompute of the freight buckets (the F4981 universe is
# bounded), then MERGE on shipment_number:
#   • WHEN MATCHED AND record_hash changed  -> UPDATE (skip no-op writes)
#   • WHEN NOT MATCHED                      -> INSERT
#   • WHEN NOT MATCHED BY SOURCE            -> soft-delete (is_deleted = true)
# record_hash makes re-runs cheap (only changed shipments are written).
# Power BI / role views filter is_deleted = false.
# OPTIMIZE / V-Order + liquid clustering (shipment_number) run on a schedule.
# =============================================================================

from datetime import datetime
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ── Refresh timestamp (captured ONCE per run) ─────────────────────────────────
run_dt = datetime.now()
print(f"Run timestamp : {run_dt}")

# ── Table refs ────────────────────────────────────────────────────────────────
F4981_TBL    = "jde.f4981_freight_audit_history"
TARGET_TABLE = "lh_jde_gold.otc.fact_freight_audit"   # new ESO1 fact — otc schema
print(f"Target table  : {TARGET_TABLE}")


# In[2]:


# ── Helper: load silver, drop soft-delete cols (filter is_delete=0 if present) ─
exclude_cols = ["is_delete", "deleted_date_time"]

def load_silver_table(table_name):
    df = spark.read.table(table_name)
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)          # F4981 IS soft-delete enabled
    return df.select(*[c for c in df.columns if c not in exclude_cols])

df_f4981 = load_silver_table(F4981_TBL)
print(f"F4981 rows (is_delete=0) : {df_f4981.count():,}")


# In[3]:


# ─────────────────────────────────────────────────────────────────────────────
# GATE — only invoiced freight rows feed the buckets
# ─────────────────────────────────────────────────────────────────────────────
# vendor_invoice_number holds the literal text "NULL" (space-padded) for
# un-invoiced rows. TRIM then compare as a string (NOT SQL IS NULL).
# ─────────────────────────────────────────────────────────────────────────────
df_fr = df_f4981.filter(F.trim(F.col("vendor_invoice_number")) != "NULL")
print(f"F4981 rows after vendor_invoice gate : {df_fr.count():,}")


# In[4]:


# ─────────────────────────────────────────────────────────────────────────────
# BUCKET + AGGREGATE TO SHIPMENT GRAIN
# ─────────────────────────────────────────────────────────────────────────────
# One row per shipment_number. Measures = conditional SUM(net_amount).
# Header-ish attributes (carrier, ship_to, plant, route, mode, frth, company,
# dates) collapsed via first(ignorenulls=True) — any representative value is
# correct at shipment grain (same approach as made_loads_data).
# ─────────────────────────────────────────────────────────────────────────────
bp   = F.trim(F.col("billable_payable"))
cgc  = F.trim(F.col("charge_code_01"))
amt  = F.col("net_amount")

df_ship = (
    df_fr
    .groupBy(F.col("shipment_number"))
    .agg(
        # ── Billable / Payable freight & fuel buckets ──────────────────────
        F.round(F.sum(F.when((bp == "B") & (cgc == "BFR"), amt).otherwise(0.0)), 2).alias("billable_freight"),
        F.round(F.sum(F.when((bp == "B") & (cgc.isin("FSC", "FSB")), amt).otherwise(0.0)), 2).alias("billable_fuel"),
        F.round(F.sum(F.when((bp == "P") & (cgc == "PFR"), amt).otherwise(0.0)), 2).alias("payable_freight"),
        F.round(F.sum(F.when((bp == "P") & (cgc == "FSC"), amt).otherwise(0.0)), 2).alias("payable_fuel"),

        # ── Conformed dimension keys (representative per shipment) ──────────
        F.first(F.col("company_key_order_no"),          ignorenulls=True).alias("company"),
        F.first(F.col("carrier"),                       ignorenulls=True).alias("carrier_number"),
        F.first(F.col("address_number_ship_to"),        ignorenulls=True).alias("ship_to"),
        F.first(F.trim(F.col("cost_center_origin")),    ignorenulls=True).alias("branch_plant"),   # origin plant
        F.first(F.col("route_number"),                  ignorenulls=True).alias("route_number"),    # F4981 FHRTN (per design §12)
        F.first(F.trim(F.col("mode_of_transport")),     ignorenulls=True).alias("mode_of_transport"),
        F.first(F.trim(F.col("freight_handling_code")), ignorenulls=True).alias("freight_handling_code"),

        # ── Event dates ────────────────────────────────────────────────────
        F.first(F.col("actual_ship_date"),              ignorenulls=True).alias("actual_ship_date"),
        F.first(F.col("date_for_g_land_voucher_julian"),ignorenulls=True).alias("gl_date"),  # FHDGJ (silver already decoded)

        # ── Source change proxy (replace with true CDC commit ts if available)
        F.max(F.col("date_updated")).alias("_src_change_date"),
    )
)

# Derived totals + variance
df_ship = (
    df_ship
    .withColumn("total_billable",  F.round(F.col("billable_freight") + F.col("billable_fuel"), 2))
    .withColumn("total_payable",   F.round(F.col("payable_freight")  + F.col("payable_fuel"), 2))
    .withColumn("freight_variance",F.round(F.col("billable_freight") - F.col("payable_freight"), 2))
    .withColumn("total_variance",  F.round(F.col("total_billable")   - F.col("total_payable"), 2))
)

print(f"Shipments (current-state grain) : {df_ship.count():,}")
dupes = df_ship.groupBy("shipment_number").count().filter(F.col("count") > 1).count()
print(f"Duplicate shipment_number rows  : {dupes}  ← must be 0")


# In[5]:


# ─────────────────────────────────────────────────────────────────────────────
# NRT / AUDIT COLUMNS + record_hash
# ─────────────────────────────────────────────────────────────────────────────
# • shift_factor_applied  — 1.0 placeholder (see ShiftFactor NOTE). TODO: real
#   per-company factor from the company-constants table for exact Hubble tie-out.
# • source_commit_timestamp — data "as of" (proxy = max source change date).
# • gold_updated_timestamp  — this merge run.
# • is_deleted              — current rows are false; MERGE flips absent keys true.
# • record_hash             — over measures + dim keys; lets MERGE skip no-ops.
# ─────────────────────────────────────────────────────────────────────────────
SHIFT_FACTOR = 1.0   # TODO: replace with per-company factor (company-constants table)

hash_cols = [
    "billable_freight", "billable_fuel", "payable_freight", "payable_fuel",
    "total_billable", "total_payable", "freight_variance", "total_variance",
    "company", "carrier_number", "ship_to", "branch_plant", "route_number",
    "mode_of_transport", "freight_handling_code", "actual_ship_date", "gl_date",
]

df_src = (
    df_ship
    .withColumn("shift_factor_applied",     F.lit(SHIFT_FACTOR).cast("double"))
    .withColumn("is_deleted",               F.lit(False))
    .withColumn("source_commit_timestamp",  F.col("_src_change_date").cast("timestamp"))
    .withColumn("gold_updated_timestamp",   F.lit(run_dt).cast("timestamp"))
    .withColumn("record_hash",              F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in hash_cols]), 256))
    .drop("_src_change_date")
)

# Column order: business key first, then dims, measures, audit
ordered_cols = [
    "shipment_number",
    "company", "carrier_number", "ship_to", "branch_plant",
    "route_number", "mode_of_transport", "freight_handling_code",
    "actual_ship_date", "gl_date",
    "billable_freight", "billable_fuel", "total_billable",
    "payable_freight", "payable_fuel", "total_payable",
    "freight_variance", "total_variance", "shift_factor_applied",
    "is_deleted", "source_commit_timestamp", "gold_updated_timestamp", "record_hash",
]
df_src = df_src.select(*ordered_cols)
print(f"Final column count : {len(df_src.columns)}")
df_src.printSchema()


# In[6]:


# ─────────────────────────────────────────────────────────────────────────────
# WRITE — current-state MERGE on shipment_number
# ─────────────────────────────────────────────────────────────────────────────
# First run creates the table (+ liquid clustering on shipment_number).
# Subsequent runs MERGE: update changed, insert new, soft-delete absent.
# ─────────────────────────────────────────────────────────────────────────────
spark.sql("CREATE SCHEMA IF NOT EXISTS lh_jde_gold.otc")   # schema-enabled lakehouse guard
if not spark.catalog.tableExists(TARGET_TABLE):
    (df_src.write
        .format("delta")
        .option("delta.enableDeletionVectors", "true")
        .clusterBy("shipment_number")            # liquid clustering for NRT merges + Direct Lake
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE))
    print(f"✓ Created {TARGET_TABLE} ({df_src.count():,} rows)")
else:
    tgt = DeltaTable.forName(spark, TARGET_TABLE)
    (tgt.alias("t")
        .merge(df_src.alias("s"), "t.shipment_number = s.shipment_number")
        .whenMatchedUpdateAll(condition="t.record_hash <> s.record_hash")   # skip no-ops
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceUpdate(set={                                 # soft-delete dropped shipments
            "is_deleted":             F.lit(True),
            "gold_updated_timestamp": F.lit(run_dt).cast("timestamp"),
        })
        .execute())
    print(f"✓ MERGE complete into {TARGET_TABLE}")

print(f"  run_dt = {run_dt}")


# In[7]:


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────────────────────────────────────────────
fct = spark.read.table(TARGET_TABLE)
active = fct.filter(F.col("is_deleted") == False)

print(f"Total rows        : {fct.count():,}")
print(f"Active (not del)  : {active.count():,}")

# 1 — uniqueness of shipment_number
dups = fct.groupBy("shipment_number").count().filter(F.col("count") > 1).count()
print(f"Duplicate shipment_number : {dups}  ← must be 0")

# 2 — bucket totals (active)
print("\nBucket totals (active):")
display(active.agg(
    F.round(F.sum("billable_freight"), 2).alias("billable_freight"),
    F.round(F.sum("billable_fuel"),    2).alias("billable_fuel"),
    F.round(F.sum("payable_freight"),  2).alias("payable_freight"),
    F.round(F.sum("payable_fuel"),     2).alias("payable_fuel"),
    F.round(F.sum("total_variance"),   2).alias("total_variance"),
))

# 3 — refresh recency
print("\nMax gold_updated_timestamp (should be ~now):")
display(fct.agg(F.max("gold_updated_timestamp").alias("last_merge")))

# 4 — sample (top by total_billable)
print("\nSample 10 (top billable):")
display(active.orderBy(F.col("total_billable").desc_nulls_last()).limit(10))


# In[8]:


# ─────────────────────────────────────────────────────────────────────────────
# OPTIMIZE / VACUUM MAINTENANCE  —  run on a SEPARATE, slower schedule
# ─────────────────────────────────────────────────────────────────────────────
# DO NOT run this in the 5-min load cycle. Clustering compaction + vacuum are
# expensive and would burn the F64 compute budget. Run it from a dedicated
# maintenance job (hourly or nightly) — flip RUN_MAINTENANCE=True there.
#
#   • OPTIMIZE (no ZORDER args) performs the incremental LIQUID-CLUSTERING
#     compaction for the CLUSTER BY (shipment_number) key, and merges the many
#     small files that frequent 5-min MERGEs create. Keeps Direct Lake fast.
#   • V-Order is ON by default in Fabric — no extra step needed.
#   • VACUUM reclaims files older than the retention window (deletion vectors +
#     superseded MERGE files). 168h (7 days) preserves a week of time-travel.
# ─────────────────────────────────────────────────────────────────────────────
RUN_MAINTENANCE = False   # ← set True ONLY in the scheduled maintenance run

if RUN_MAINTENANCE:
    print(f"OPTIMIZE {TARGET_TABLE} (liquid-clustering compaction)…")
    spark.sql(f"OPTIMIZE {TARGET_TABLE}")
    print(f"VACUUM {TARGET_TABLE} RETAIN 168 HOURS…")
    spark.sql(f"VACUUM {TARGET_TABLE} RETAIN 168 HOURS")
    print("✓ Maintenance complete")
else:
    print("RUN_MAINTENANCE=False — skipped (run from the scheduled maintenance job).")


# In[9]:


# ─────────────────────────────────────────────────────────────────────────────
# POWER BI SEMANTIC MODEL — relationships for fact_freight_audit
# ─────────────────────────────────────────────────────────────────────────────
# (reference notes — nothing executed)
#
# Relationships (all Many fact : One dim, single cross-filter dim → fact):
#   fact_freight_audit.carrier_number  →  dim_address_carrier.address_number
#   fact_freight_audit.ship_to         →  dim_address_ship_to.address_number
#   fact_freight_audit.branch_plant    →  dim_plant.plant_code            (origin plant)
#
#   (dim_address_carrier / dim_address_ship_to are role-playing SQL VIEWS over
#    dim_address_book — created once in nb_dim_address_book. ONE physical dim,
#    many logical roles; Direct-Lake-safe.)
#
# CONFORMANCE WITH fact_sales_order_line:
#   • shipment_number is a DEGENERATE column on BOTH facts (the bridge).
#   • Do NOT create a direct fact-to-fact relationship (ambiguous, fan-out).
#     Analyze freight vs order metrics through the SHARED dims
#     (dim_address carrier/ship-to, dim_plant, company).
#   • If shipment-level cross-fact slicing is required, build a small
#     dim_shipment = DISTINCT shipment_number, related 1:* to each fact.
#
# MODEL HYGIENE:
#   • Filter is_deleted = false  (model-level filter or a v_… view) so CDC
#     soft-deleted shipments drop out of every visual.
#   • Measures: Total Billable = SUM(total_billable); Total Payable =
#     SUM(total_payable); Freight CM % = DIVIDE(SUM(freight_variance),
#     SUM(billable_freight)) — ratios computed in DAX, never per row.
#   • Hide keys (shipment_number, carrier_number, ship_to, branch_plant,
#     record_hash) from report view; surface measures + dim attributes.
# ─────────────────────────────────────────────────────────────────────────────
print("Power BI relationship notes — see cell comments above.")
