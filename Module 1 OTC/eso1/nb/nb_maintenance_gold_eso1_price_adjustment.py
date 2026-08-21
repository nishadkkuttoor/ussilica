#!/usr/bin/env python
# coding: utf-8

# ## nb_maintenance_gold_eso1_price_adjustment
#
# OPTIMIZE / VACUUM maintenance for the ESO1 price-adjustment Gold fact
# `lh_jde_gold.rpt.fact_sales_order_price_adjustment`. Run on a nightly/hourly schedule
# after the fact is rebuilt so Direct Lake stays fast (compact small files, V-Order the
# parquet, skip files via ZORDER on the report's filter columns) and the F64 budget is
# protected.
#
# The price-adjustment semantic model has ONE table of its own — this fact. Its dimensions
# are the REUSED conformed dims (dim_address_*, dim_item, dim_plant, dim_company, …), which
# are maintained by their own jobs and are explicitly OUT of scope here (status only).
#
# SELF-CONTAINED — no %run; declares its own constants inline.

# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------

import json, time
from datetime import datetime, timezone
from pyspark.sql import functions as F

GOLD = "lh_jde_gold.rpt"
FACT = f"{GOLD}.fact_sales_order_price_adjustment"

# Reused dims — maintained by their own jobs; reported for freshness, never touched here.
REUSED_DIMS = [f"{GOLD}.dim_address_ship_to", f"{GOLD}.dim_address_sold_to",
               f"{GOLD}.dim_address_parent", f"{GOLD}.dim_item", f"{GOLD}.dim_plant",
               f"{GOLD}.dim_company", f"{GOLD}.dim_freight_handling_code",
               f"{GOLD}.dim_mode_of_transport", f"{GOLD}.dim_category_code_05"]

# ZORDER on the columns the reports filter / the measures aggregate by — improves file
# skipping so Direct Lake reads fewer rowgroups. Set ZORDER_ENABLED=False for plain
# compaction only.
ZORDER_ENABLED = True
ZORDER_COLS    = ["next_status_num", "order_type", "is_product_line", "is_primary_line_row", "gl_date"]

RETAIN_HOURS = 168   # 7-day time-travel window kept by VACUUM

print(f"Maintenance target: {FACT}")


# ----------------------------------------------------------------------------
# 2) REUSED-DIM SCOPE CHECK (read-only — never OPTIMIZE/VACUUM them here)
# ----------------------------------------------------------------------------

for _t in REUSED_DIMS:
    print(f"  reused (not maintained here) : {_t}  {'OK' if spark.catalog.tableExists(_t) else 'MISSING'}")
print("  → reused dims are maintained by their own jobs; skipped intentionally.\n")


# ----------------------------------------------------------------------------
# 3) OPTIMIZE + VACUUM the fact
# ----------------------------------------------------------------------------

def _detail(t):
    try:
        r = spark.sql(f"DESCRIBE DETAIL {t}").select("numFiles", "sizeInBytes").first()
        return r["numFiles"], r["sizeInBytes"]
    except Exception:
        return None, None

_run_start = time.time()
_status = "skipped"
if not spark.catalog.tableExists(FACT):
    print(f"– skip (missing): {FACT} — build it with nb_eso1_gold_fact_sales_order_price_adjustment first.")
else:
    _pf, _ps = _detail(FACT)
    print(f"before : numFiles={_pf}  sizeMB={round((_ps or 0)/1048576, 1)}")

    if ZORDER_ENABLED:
        _z = ", ".join(ZORDER_COLS)
        print(f"OPTIMIZE {FACT} ZORDER BY ({_z}) …")
        spark.sql(f"OPTIMIZE {FACT} ZORDER BY ({_z})")
    else:
        print(f"OPTIMIZE {FACT} …")
        spark.sql(f"OPTIMIZE {FACT}")

    print(f"VACUUM {FACT} RETAIN {RETAIN_HOURS} HOURS …")
    spark.sql(f"VACUUM {FACT} RETAIN {RETAIN_HOURS} HOURS")

    _nf, _ns = _detail(FACT)
    print(f"after  : numFiles={_nf}  sizeMB={round((_ns or 0)/1048576, 1)}")
    _status = "optimized"
    print(f"✓ {FACT}")


# ----------------------------------------------------------------------------
# 4) EXIT
# ----------------------------------------------------------------------------

_exit_payload = {
    "status":       _status,
    "table":        FACT,
    "zorder":       ZORDER_COLS if ZORDER_ENABLED else None,
    "elapsed_sec":  round(time.time() - _run_start, 1),
    "end_time_utc": datetime.now(timezone.utc).isoformat(),
}
print("✓ Maintenance complete. exit payload:", json.dumps(_exit_payload))
notebookutils.notebook.exit(json.dumps(_exit_payload))
