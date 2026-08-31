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
# META     }
# META   }
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json, time
from datetime import datetime, timezone

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

# ── refresh / runtime config (BATCH build) ──
MANUAL_OVERWRITE = True    # ⚠ set back to False after the first successful run

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

# ── Silver source ──────────────────────────────────────────────────────────────
F4074 = "f4074_price_adjustment_ledger_file"

# ── Gold target BUILT here ─────────────────────────────────────────────────────
FACT = "fact_price_adjustment"

print(f"ESO1 Gold fact_price_adjustment processor (batch build) — target {gname(FACT)}")

# HELPERS
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def load_silver_table(table_name):
    df = spark.read.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

def sk(*cols):
    """Surrogate key — pipe-separated string (identical formula to the order-line fact)."""
    return F.concat_ws(
        "|",
        *[F.col(c).cast("string") if isinstance(c, str) else c.cast("string") for c in cols],
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# 2) FACT BUILDER
# ----------------------------------------------------------------------------

# F4074 adjustment detail (the only stored payload besides the two keys).
#   ALAST=price_adjustment_type · ALAPRP1=adj_print_code · ALUPRC=adj_unit_price · ALUOM=adj_uom ·
#   ALBSDVAL=adj_based_on_value · ALGLC=adj_gl_class · ALFVTR=adj_factor_value
ADJ_COLS = ["price_adjustment_type", "adj_print_code", "adj_unit_price", "adj_uom",
            "adj_based_on_value", "adj_gl_class", "adj_factor_value"]

FACT_BUSINESS_COLS = ["sales_order_line_key"] + ADJ_COLS

def build_fact():
    adj = load_silver_table(F4074)
    df = adj.select(
        # line keys (F4074 ALKCOO/ALDCTO/ALDOCO/ALLNID = the line's SDKCOO/SDDCTO/SDDOCO/SDLNID) — used ONLY
        # to build sales_order_line_key; UNTRIMMED, mirroring the order-line fact's key exactly.
        F.col("company_key_order_no").alias("_kcoo"),      # ALKCOO
        F.col("order_type").alias("_dcto"),                # ALDCTO
        F.col("document_order_invoice_e").alias("_doco"),  # ALDOCO
        F.col("line_number").alias("_lnid"),               # ALLNID
        # F4074 adjustment detail
        F.trim(F.col("price_adjustment_type")).alias("price_adjustment_type"),   # ALAST
        F.trim(F.col("pricing_report_code_01")).alias("adj_print_code"),         # ALAPRP1
        F.col("amt_price_per_unit_02").cast("double").alias("adj_unit_price"),    # ALUPRC
        F.trim(F.col("uom_as_input")).alias("adj_uom"),                          # ALUOM
        F.col("based_on_value").cast("double").alias("adj_based_on_value"),       # ALBSDVAL
        F.col("gl_class").alias("adj_gl_class"),                                  # ALGLC
        F.col("factor_value").cast("double").alias("adj_factor_value"))          # ALFVTR

    # sales_order_line_key — SAME formula as the order-line fact: sk(KCOO | DCTO | DOCO | LNID), untrimmed.
    df = df.withColumn("sales_order_line_key", sk("_kcoo", "_dcto", "_doco", "_lnid"))

    # per-adjustment surrogate key — stable within a line via a deterministic adjustment sequence
    _ws = Window.partitionBy("sales_order_line_key").orderBy(
              F.col("price_adjustment_type").asc_nulls_first(),
              F.col("adj_unit_price").asc_nulls_first(),
              F.col("adj_gl_class").asc_nulls_first())
    df = (df.withColumn("_seq", F.row_number().over(_ws))
            .withColumn("price_adjustment_key", sk("sales_order_line_key", "_seq"))
            .drop("_seq"))

    return df.select("price_adjustment_key", *FACT_BUSINESS_COLS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# 3) FACT SOURCES
# ----------------------------------------------------------------------------

FACT_SOURCES = [
    {"silver": F4074, "role": "adjustment-detail"},
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# 4) RUN
# ----------------------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

# preflight — confirm the Silver source exists before building.
for _s in FACT_SOURCES:
    print("  source {:<44s} {}".format(_s["silver"],
                                       "OK" if spark.catalog.tableExists(sname(_s["silver"])) else "MISSING"))

_run_start = time.time()
if MANUAL_OVERWRITE or not spark.catalog.tableExists(gname(FACT)):
    print("== FULL LOAD ==")
    new = build_fact()
    (new.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(gname(FACT)))
    _rows   = new.count()
    _status = "built"
    print("  {} rows={}".format(gname(FACT), _rows))
else:
    print("== skip — {} exists and MANUAL_OVERWRITE=False ==".format(gname(FACT)))
    _rows, _status = None, "skipped"

_elapsed = round(time.time() - _run_start, 1)
_exit_payload = {
    "status":       _status,
    "table":        gname(FACT),
    "rows":         _rows,
    "elapsed_sec":  _elapsed,
    "end_time_utc": datetime.now(timezone.utc).isoformat(),
}
print("exit payload:", json.dumps(_exit_payload))
notebookutils.notebook.exit(json.dumps(_exit_payload))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
