#!/usr/bin/env python
# coding: utf-8

# ## nb_silver_to_gold_dim_uom_conversion_item
# 
# New notebook

# In[4]:


#!/usr/bin/env python
# coding: utf-8

# ## nb_silver_to_gold_dim_uom_conversion_item
#
# Gold layer — F41002 (UOM Conversion) — conformed ITEM-SPECIFIC conversion-
# factor dimension. This is Tier A of the Total Tons UOM cascade:
#   Tier 0  uom_as_input = 'TN'                      → factor = 1.0  (DAX)
#   Tier A  this dim (F41002 item-specific factors)  → DAX RELATED()
#   Tier B  dim_uom_conversion (F41003 standard/generic factors) → DAX RELATED()
#   Fallback  literal 1.0                            → DAX fallback
#
# Deliberately a SEPARATE table from dim_uom_conversion (F41003, Tier B) —
# different source table, different grain (per-item vs generic-per-UOM),
# and dim_uom_conversion is already live/streaming shared infrastructure
# that should not be reshaped for this addition.
#
# Bidirectional: both fwd (related_uom='TN') and rev (uom='TN', factor
# inverted) rows are folded into a single (identifier_short_item, from_uom)
# key — mirrors the same fwd/rev pattern already proven in dim_uom_conversion
# and in the retiring fact_extended_sales_order_7's own Tier A join.
#
# Key is 2 columns (identifier_short_item, from_uom) — NOT cost_center —
# matching the already-proven, already-live Tier A join logic in the
# retiring notebook (build_fact_eso7), confirmed to ignore cost_center.
# If a future data check finds conversion_factor genuinely varies by
# cost_center for the same item+uom, revisit this and fold cost_center
# into the key (3-column version) instead.
#
# fact_sales_order_detail relates to this table via a plain computed
# column (item_uom_key = concat_ws("|", identifier_short_item, uom_as_input))
# — no join to F41002 happens in the fact's own notebook at all.



# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — Imports & constants
# ─────────────────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from datetime import datetime

F41002_TBL = "lh_jde_silver.jde.f41002_item_units_of_measure_conversion_factors"
GOLD_TABLE = "lh_jde_gold.rpt.dim_uom_conversion_item"

print(f"Run timestamp : {datetime.now()}")


# In[5]:


# ─────────────────────────────────────────────────────────────────────────────
# CELL 2 — Helper: load silver, drop soft-deleted rows + pipeline metadata
# ─────────────────────────────────────────────────────────────────────────────
_EXCLUDE_COLS = ["is_delete", "deleted_date_time"]

def load_silver(table_name):
    df = spark.read.table(table_name)
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _EXCLUDE_COLS])


# In[6]:


# ─────────────────────────────────────────────────────────────────────────────
# CELL 3 — Load F41002 (universal exclusion only — is_delete, handled above)
# ─────────────────────────────────────────────────────────────────────────────
df_f41002 = load_silver(F41002_TBL)
print(f"F41002 silver rows : {df_f41002.count():,}")


# In[7]:


# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Build bidirectional (fwd + rev) conversion factors, then key
#
# Silver column mapping (confirmed via F41002 table metadata):
#   UMITM  → identifier_short_item  (matches F4211.SDITM, NOT SDLITM)
#   UMUM   → uom
#   UMRUM  → related_uom
#   UMCONV → conversion_factor      (implied_decimals already applied in
#                                    silver — do NOT divide again)
#
# fwd: related_uom = 'TN'  → from_uom = uom,          conv_factor = conversion_factor
# rev: uom = 'TN'          → from_uom = related_uom,  conv_factor = 1 / conversion_factor
#
# conversion_factor = 0 excluded from both directions — a zero factor is
# not usable (and would divide-by-zero on the reverse side).
# ─────────────────────────────────────────────────────────────────────────────
_fwd = (
    df_f41002
    .filter((F.trim(F.col("related_uom")) == "TN") & (F.col("conversion_factor") != 0))
    .select(
        F.col("identifier_short_item"),
        F.trim(F.col("uom")).alias("from_uom"),
        F.col("conversion_factor").cast("double").alias("conv_factor"),
    )
)

_rev = (
    df_f41002
    .filter((F.trim(F.col("uom")) == "TN") & (F.col("conversion_factor") != 0))
    .select(
        F.col("identifier_short_item"),
        F.trim(F.col("related_uom")).alias("from_uom"),
        (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_factor"),
    )
)

df_dim = (
    _fwd.unionByName(_rev)
    .dropDuplicates(["identifier_short_item", "from_uom"])
)

# item_uom_key — 2-column surrogate matching the join column carried on
# fact_sales_order_detail (identical column order: identifier_short_item, from_uom).
df_dim = df_dim.withColumn(
    "item_uom_key",
    F.concat_ws("|", "identifier_short_item", "from_uom")
)

print(f"dim_uom_conversion_item rows (pre-write) : {df_dim.count():,}")



# In[8]:


# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — Validate: (identifier_short_item, from_uom) is unique
#
# This is the chosen 2-column key (matches the already-proven Tier A join
# in the retiring notebook, which ignores cost_center). Any duplicate here
# means either a silver-layer defect or that conversion_factor genuinely
# varies by cost_center for some item+uom pair — investigate before
# proceeding; do not silently pick one row.
# ─────────────────────────────────────────────────────────────────────────────
key_cols = ["identifier_short_item", "from_uom"]

dup_check = df_dim.groupBy(*key_cols).count().filter(F.col("count") > 1)
dup_count = dup_check.count()
assert dup_count == 0, (
    f"(identifier_short_item, from_uom) key violated — {dup_count} duplicate "
    f"combinations found after fwd/rev fold. This likely means conversion_factor "
    f"varies by cost_center for at least one item+uom pair — revisit the "
    f"2-column vs 3-column key decision (see header note) before proceeding."
)
print("✓ (identifier_short_item, from_uom) uniqueness verified.")


# In[9]:


# ─────────────────────────────────────────────────────────────────────────────
# CELL 6 — Write Gold dimension table
# ─────────────────────────────────────────────────────────────────────────────
df_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_TABLE)

spark.sql(f"OPTIMIZE {GOLD_TABLE}")
print(f"✓ {GOLD_TABLE}  →  {spark.read.table(GOLD_TABLE).count():,} rows")

