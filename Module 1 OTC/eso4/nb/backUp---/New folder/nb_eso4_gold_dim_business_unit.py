#!/usr/bin/env python
# coding: utf-8

# In[1]:

# ## nb_eso4_gold_dim_business_unit
#
# **Gold `dim_business_unit` processor** for Extended Sales Order 4 (Sales Tax reconciliation).
# Batch build of ONE table — `lh_jde_gold.rpt.dim_business_unit` — from the Silver business-unit
# master (F0006). Supplies the report's Plant / Plant Name / Business Stream (MCRP20) attributes;
# the fact's `plant` FK joins here.
# Design: eso4/docs/ESO4_gold_layer_design.md


# =============================================================================
# CONFIG + CONSTANTS  (names per Fabric_Naming_Convention_Guidelines.pdf)
# =============================================================================
from pyspark.sql import functions as F

GOLD_SCHEMA = "lh_jde_gold.rpt"

# ── manual reprocess switch ──────────────────────────────────────────────────
#   OVERWRITE = True  -> drop + rebuild from the full Silver snapshot.
#   OVERWRITE = False -> build only if the table is missing; otherwise leave it untouched.
OVERWRITE     = True

SRC_SCHEMA    = "jde_cdc"
SRC_LAKEHOUSE = "lh_jde_silver"
F0006_TBL     = "f0006_business_unit_master"

T_DIM_BU  = f"{GOLD_SCHEMA}.dim_business_unit"

print(f"ESO4 Gold dim_business_unit processor — target {T_DIM_BU}  OVERWRITE={OVERWRITE}")


# In[2]:


# =============================================================================
# HELPERS
# =============================================================================
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def sname(table_name):
    return f"{SRC_LAKEHOUSE}.{SRC_SCHEMA}.{table_name}"

def load_silver_table(table_name):
    df = spark.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

def _write_table(df, target):
    """Overwrite-write a Gold table (schema + data replaced)."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(target))


# In[3]:


# =============================================================================
# DIM transform — dim_business_unit (F0006, natural PK = cost_center / MCMCU)
# =============================================================================
def transform_dim_bu():
    f0006 = load_silver_table(F0006_TBL)
    return (f0006.select(
                F.trim(F.col("cost_center")).alias("business_unit"),                    # MCMCU
                F.col("description_001").alias("plant_name"),                           # MCDL01
                # Code + description, ready to display: "771010 - SBX TRANS DISTRICT-WEST TEXAS".
                # Built HERE rather than in a consumer because Direct Lake tables cannot carry DAX
                # calculated columns — the concatenation has to be a physical column. ESO5's fact points
                # its `district` FK at this dim to render exactly this string. Falls back to the bare code
                # when F0006 has no description, so it never renders a dangling " - ".
                F.when(F.trim(F.coalesce(F.col("description_001"), F.lit(""))) == "",
                       F.trim(F.col("cost_center")))
                 .otherwise(F.concat_ws(" - ", F.trim(F.col("cost_center")),
                                        F.trim(F.col("description_001"))))
                 .alias("business_unit_display"),
                F.trim(F.col("category_code_cost_ct_020")).alias("business_stream_code"),  # MCRP20
                F.col("company").alias("company"),                                      # MCCO
                F.col("state").alias("state"))                                          # MCADDS
            .dropDuplicates(["business_unit"]))


# In[4]:


# =============================================================================
# BUILD — batch full load, gated by OVERWRITE (streaming removed; results identical to a full
#   OVERWRITE run of the previous streaming version — same transform, same overwrite write).
# =============================================================================
if OVERWRITE or not spark.catalog.tableExists(T_DIM_BU):
    spark.sql(f"DROP TABLE IF EXISTS {T_DIM_BU}")
    _write_table(transform_dim_bu(), T_DIM_BU)
    print(f"✓ built {T_DIM_BU}")
else:
    print(f"skip — {T_DIM_BU} exists and OVERWRITE=False")
