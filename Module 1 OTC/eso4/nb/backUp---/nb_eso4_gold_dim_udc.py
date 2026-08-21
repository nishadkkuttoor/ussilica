#!/usr/bin/env python
# coding: utf-8

# In[1]:

# ## nb_eso4_gold_dim_udc
#
# **Gold UDC-dimension processor** for Extended Sales Order 4 (Sales Tax reconciliation).
# Batch build of TWO small reference dimensions from ONE Silver source — the user-defined-code
# values (F0005):
#   • `lh_jde_gold.rpt.dim_sic`   — UDC 01/SC : sic_code   -> sic_description   (docx §6 col 24/25)
#   • `lh_jde_gold.rpt.dim_state` — UDC 00/S  : state_code -> state_name        (jurisdiction name)
#
# The fact `fact_sales_tax_reconciliation` stores the raw FK codes (sic_code, jurisdiction); these
# dims resolve the descriptions in the Direct Lake model (fact.sic_code -> dim_sic.sic_code,
# fact.jurisdiction -> dim_state.state_code). Same F0005 lookup shape ESO7 uses for dim_status (40/AT).
# Design: eso4/docs/ESO4_gold_layer_design.md


# =============================================================================
# CONFIG + CONSTANTS  (names per Fabric_Naming_Convention_Guidelines.pdf)
# =============================================================================
from pyspark.sql import functions as F

GOLD_SCHEMA = "lh_jde_gold.rpt"

# ── manual reprocess switch ──────────────────────────────────────────────────
#   OVERWRITE = True  -> drop + rebuild both dims from the full Silver snapshot.
#   OVERWRITE = False -> build only if a dim is missing; otherwise leave them untouched.
OVERWRITE     = True

SRC_SCHEMA    = "jde_cdc"
SRC_LAKEHOUSE = "lh_jde_silver"
F0005_TBL     = "f0005_user_defined_code_values"

# UDC selectors (INFERRED system/type — confirm; flagged in design §5, like ESO7 dim_status 40/AT).
SIC_SYS,   SIC_TYPE   = "01", "SC"   # SIC codes  -> dim_sic
STATE_SYS, STATE_TYPE = "00", "S"    # State/Province codes -> dim_state

T_DIM_SIC   = f"{GOLD_SCHEMA}.dim_sic"
T_DIM_STATE = f"{GOLD_SCHEMA}.dim_state"

print(f"ESO4 Gold dim_udc processor — targets {T_DIM_SIC}, {T_DIM_STATE}  OVERWRITE={OVERWRITE}")


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
# DIM transforms — F0005 UDC lookups. Natural PK per dim = the UDC value (DRKY) WITHIN its
# system/type, so each transform filters product_code/user_defined_codes first, then keys on
# trim(user_defined_code).
# =============================================================================
def _udc_dim(sys_code, type_code, key_alias, desc_alias):
    f0005 = (load_silver_table(F0005_TBL)
             .where((F.trim(F.col("product_code")) == sys_code) &
                    (F.trim(F.col("user_defined_codes")) == type_code)))
    return (f0005.select(F.trim(F.col("user_defined_code")).alias(key_alias),   # DRKY
                         F.trim(F.col("description_001")).alias(desc_alias))     # DRDL01
            .where(F.col(key_alias) != "")
            .dropDuplicates([key_alias]))

def transform_dim_sic():
    return _udc_dim(SIC_SYS, SIC_TYPE, "sic_code", "sic_description")

def transform_dim_state():
    return _udc_dim(STATE_SYS, STATE_TYPE, "state_code", "state_name")


# In[4]:


# =============================================================================
# BUILD — batch full load, gated by OVERWRITE (streaming removed; results identical to a full
#   OVERWRITE run of the previous streaming version — same transforms, same overwrite write).
#   One F0005 source feeds BOTH dims (split by UDC system/type in each transform).
# =============================================================================
if OVERWRITE or not spark.catalog.tableExists(T_DIM_SIC) or not spark.catalog.tableExists(T_DIM_STATE):
    spark.sql(f"DROP TABLE IF EXISTS {T_DIM_SIC}")
    spark.sql(f"DROP TABLE IF EXISTS {T_DIM_STATE}")
    _write_table(transform_dim_sic(),   T_DIM_SIC)
    _write_table(transform_dim_state(), T_DIM_STATE)
    print(f"✓ built {T_DIM_SIC} + {T_DIM_STATE}")
else:
    print(f"skip — {T_DIM_SIC} + {T_DIM_STATE} exist and OVERWRITE=False")
