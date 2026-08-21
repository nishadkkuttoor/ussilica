#!/usr/bin/env python
# coding: utf-8

# ## nb_eso1_gold_dim_company
#
# **Gold `dim_company` processor** for Extended Sales Order 1 (Billable v Payable Freight).
# Builds ONE small dimension — `lh_jde_gold.rpt.dim_company` — the distinct JDE company (F0010
# company constants) plus its current fiscal-period / fiscal-year / domestic-currency constants.
# Relates to the freight fact on `company_key_order_no` (SDKCOO = CCCO; 1 -> many).
#
# Serves the company-level fields the freight fact can't carry (order-line grain):
#   • currency_code           (CCCRCD) — the report "DomesticCurrency" column (Solvay / SM Past Due /
#                                         Days Since Invoice)
#   • period_number_current   (CCPNC)  — company current fiscal period (Leslie's Poolmart Scheduler)
#   • fiscal_year_current     (CCDFF)  — company current fiscal year   (Leslie's Poolmart Scheduler)
# The two Leslie's "period offset" columns are then DAX over the fact + RELATED(dim_company[…]):
#   CCPNC - MONTH(invoice_date)   and   RIGHT(YEAR(invoice_date),2) - fiscal_year_current
#
# ⚠ CCPNC / CCDFF are the company's CURRENT period/year in JDE — they roll as periods close, so this
# dim reflects whatever the constants are at the Gold refresh. Matches Hubble only when both are read
# at the same time; for a fixed reporting period use a report parameter instead.
#
# ── BUILD (BATCH) ─────────────────────────────
#   • read the full F0010 snapshot, run build_dim() ONCE, overwrite the dim.
#   • MANUAL_OVERWRITE = True → drop + rebuild; False → build only if the dim is missing (re-run to refresh).
#
# Sections:  1) CONFIG   2) DIM BUILDER   3) RUN


# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------

# CONFIG + CONSTANTS
import json, time
from datetime import datetime, timezone
from pyspark.sql import functions as F

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"          # Silver schema — static batch snapshots
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

MANUAL_OVERWRITE = True    # True: drop + rebuild from the full snapshot. ⚠ set False after the first successful run

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

# ── Silver source ─────────────────────────────────────────────────────────────
F0010 = "f0010_company_constants"    # company constants — one row per company (CCCO)

# ── Gold target BUILT here (new, rpt) ─────────────────────────────────────────
DIM   = "dim_company"

print(f"ESO1 Gold dim_company processor (batch build) — target {gname(DIM)}")


# HELPERS
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def load_silver_table(table_name):
    df = spark.read.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])


# ----------------------------------------------------------------------------
# 2) DIM BUILDER
# ----------------------------------------------------------------------------

# DIM transform — one row per company (CCCO). The key matches the freight fact's
# company_key_order_no (SDKCOO = CCCO) for the relationship.
#   company                = CCCO     (key)
#   company_name           = CCNAME
#   currency_code          = CCCRCD   (company domestic currency = report DomesticCurrency)
#   period_number_current  = CCPNC    (company current fiscal period)
#   fiscal_year_current    = CCDFF    (company current fiscal year)
def build_dim():
    cc = load_silver_table(F0010)
    df = (cc.select(
              F.trim(F.col("company")).alias("company"),
              F.col("name").alias("company_name"),
              F.col("currency_code_from").alias("currency_code"),
              F.col("period_number_current").cast("int").alias("period_number_current"),
              F.col("financial_reporting_year").cast("int").alias("fiscal_year_current"))
          .dropDuplicates(["company"]))
    return df


# ----------------------------------------------------------------------------
# 3) RUN
# ----------------------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

# BATCH BUILD — read the full F0010 snapshot, run build_dim() once, overwrite the dim.
_run_start = time.time()
if MANUAL_OVERWRITE or not spark.catalog.tableExists(gname(DIM)):
    print("== FULL LOAD ==")
    new = build_dim()
    (new.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(gname(DIM)))
    _rows   = new.count()
    _status = "built"
    print("  {} rows={:,}".format(gname(DIM), _rows))
else:
    print("== skip — {} exists and MANUAL_OVERWRITE=False ==".format(gname(DIM)))
    _rows, _status = None, "skipped"

_elapsed = round(time.time() - _run_start, 1)
_exit_payload = {
    "status":       _status,
    "table":        gname(DIM),
    "rows":         _rows,
    "elapsed_sec":  _elapsed,
    "end_time_utc": datetime.now(timezone.utc).isoformat(),
}
print("exit payload:", json.dumps(_exit_payload))
notebookutils.notebook.exit(json.dumps(_exit_payload))
