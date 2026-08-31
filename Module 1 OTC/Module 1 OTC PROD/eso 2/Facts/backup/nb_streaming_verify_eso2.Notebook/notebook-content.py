# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

#!/usr/bin/env python
# coding: utf-8

# ## nb_streaming_verify_eso2
#
# End-to-end test that confirms the CDF streaming pipeline is live for ESO2.
# Flow: UPDATE a known row in the CDF mirror → confirm CDF captured it → confirm gold updated.
#
# PRE-CONDITION: nb_silver_to_gold_eso2_v2_stm must be running BEFORE you start this notebook.
# RUN ORDER: cells 1 → 2 → 3 → wait 30-60s → 4 → 5 → wait 30-60s → 4 again (should revert).
#
# NOTE: The SQL analytics endpoint is READ-ONLY.
#       All DML (UPDATE) must be done via Spark — that is what this notebook does.
# ============================================================================

GOLD_LH     = "lh_jde_gold"
GOLD_SCHEMA = "eso2"
FACT_TABLE  = "fact_extended_sales_order_2"

MIRROR_LH     = "lh_jde_silver"
MIRROR_SCHEMA = "cdf"
F4211_TABLE   = "f4211_sales_order_detail_file"

# A known order that exists in Gold — you will confirm this in Cell 1.
# ESO2 uses ship_to (address_number_ship_to) as the DQ gate, not shipment_number.
# Filter by a plant or company you know has data.
TEST_COMPANY = "00640"     # change to a company that has rows in your Gold fact
TEST_DELTA   = 1000        # added then subtracted — easy to spot in Gold

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 1 — Find a test row in Gold and note its 4-column grain key
# ============================================================================
# ESO2 LINE_KEYS in Gold are aliased:
#   key_company_order  ← company_key_order_no  (SDKCOO)
#   order_number       ← document_order_invoice_e (SDDOCO)
#   order_type         ← order_type            (SDDCTO)
#   line_number        ← line_number           (SDLNID)
#
# Copy the values printed below into CELL 2 and CELL 4.
# ============================================================================

print("=== CELL 1: Pick a test row from Gold ===")
print("Copy the key values below into CELL 2 and CELL 4.")

spark.sql("""
    SELECT
        key_company_order,
        order_number,
        order_type,
        line_number,
        ship_to,
        plant,
        actual_ship_date,
        quantity_shipped_uom,
        quantity_loaded
    FROM {gold_lh}.{gold_schema}.{fact_table}
    WHERE key_company_order = '{company}'
    LIMIT 5
""".format(
    gold_lh     = GOLD_LH,
    gold_schema = GOLD_SCHEMA,
    fact_table  = FACT_TABLE,
    company     = TEST_COMPANY,
)).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 2 — UPDATE the CDF mirror (paste key values from Cell 1)
# ============================================================================
# IMPORTANT: The Silver F4211 table uses the RAW Silver column names, not the
# Gold aliases.  Map as follows:
#   Gold key_company_order  → Silver company_key_order_no
#   Gold order_number       → Silver document_order_invoice_e
#   Gold order_type         → Silver order_type         (same)
#   Gold line_number        → Silver line_number        (same)
#
# >>> PASTE the exact key values from Cell 1 output here <<<
# ============================================================================

COMPANY    = "00640"              # key_company_order from Cell 1
ORDER_NO   = ""                   # order_number from Cell 1  ← fill this in
ORDER_TYPE = ""                   # order_type from Cell 1    ← fill this in
LINE_NO    = None                 # line_number from Cell 1   ← fill this in (numeric)

if not all([COMPANY, ORDER_NO, ORDER_TYPE, LINE_NO is not None]):
    raise ValueError(
        "Fill in COMPANY, ORDER_NO, ORDER_TYPE, LINE_NO from Cell 1 before running Cell 2.")

print("=== CELL 2: UPDATE CDF mirror (Silver F4211) ===")

spark.sql("""
    UPDATE {mirror_lh}.{mirror_schema}.{f4211}
    SET    units_transaction_qty = units_transaction_qty + {delta}
    WHERE  company_key_order_no     = '{company}'
      AND  document_order_invoice_e = '{order_no}'
      AND  order_type               = '{order_type}'
      AND  line_number              = {line_no}
""".format(
    mirror_lh     = MIRROR_LH,
    mirror_schema = MIRROR_SCHEMA,
    f4211         = F4211_TABLE,
    delta         = TEST_DELTA,
    company       = COMPANY,
    order_no      = ORDER_NO,
    order_type    = ORDER_TYPE,
    line_no       = LINE_NO,
))

print("UPDATE done — added {} to units_transaction_qty for order {} line {}.".format(
    TEST_DELTA, ORDER_NO, LINE_NO))
print("Now run Cell 3 to confirm CDF captured the change.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 3 — Confirm CDF captured the change (update_preimage + update_postimage)
# ============================================================================
# You should see exactly 2 rows for your order+line:
#   update_preimage  — the old value of units_transaction_qty
#   update_postimage — the new value (original + TEST_DELTA)
#
# If you see 0 rows, CDF is not enabled on this Silver table.
# Check: ALTER TABLE ... SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
# ============================================================================

print("=== CELL 3: CDF change log (most recent first) ===")
print("You should see 2 rows: update_preimage (old) and update_postimage (new).")

(
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("{}.{}.{}".format(MIRROR_LH, MIRROR_SCHEMA, F4211_TABLE))
    .filter("_change_type IN ('update_preimage', 'update_postimage')")
    .filter(
        (F.col("company_key_order_no")     == COMPANY)
        & (F.col("document_order_invoice_e") == ORDER_NO)
        & (F.col("order_type")               == ORDER_TYPE)
        & (F.col("line_number")              == LINE_NO)
    )
    .orderBy("_commit_timestamp", ascending=False)
    .select(
        "_change_type",
        "_commit_timestamp",
        "_commit_version",
        "company_key_order_no",
        "document_order_invoice_e",
        "order_type",
        "line_number",
        "units_transaction_qty",
    )
    .limit(10)
    .show(truncate=False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 4 — Check Gold (wait 30-60 seconds after Cell 2 before running this)
# ============================================================================
# ESO2 streaming picks up the F4211 CDF event, calls affected_lines() to find
# the Gold LINE_KEYS, then recompute_fact() deletes the old Gold row and appends
# the recomputed row with the updated units_transaction_qty.
#
# If streaming is live, quantity_shipped_uom should be +TEST_DELTA vs original.
# quantity_loaded will also change because it is derived from units_transaction_qty.
#
# NOTE: Gold uses aliased column names — query uses key_company_order, order_number
#       (not company_key_order_no, document_order_invoice_e).
# ============================================================================

print("=== CELL 4: Gold fact check ===")
print("Wait 30-60 seconds after Cell 2 before running this.")
print("quantity_shipped_uom should be +{} vs the original.".format(TEST_DELTA))

spark.sql("""
    SELECT
        key_company_order,
        order_number,
        order_type,
        line_number,
        quantity_shipped_uom,
        quantity_ordered_primary,
        quantity_loaded,
        actual_ship_date,
        plant,
        ship_to
    FROM {gold_lh}.{gold_schema}.{fact_table}
    WHERE key_company_order = '{company}'
      AND order_number      = '{order_no}'
      AND order_type        = '{order_type}'
      AND line_number       = {line_no}
""".format(
    gold_lh     = GOLD_LH,
    gold_schema = GOLD_SCHEMA,
    fact_table  = FACT_TABLE,
    company     = COMPANY,
    order_no    = ORDER_NO,
    order_type  = ORDER_TYPE,
    line_no     = LINE_NO,
)).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 5 — Revert: subtract the test delta back
# ============================================================================

print("=== CELL 5: REVERT ===")

spark.sql("""
    UPDATE {mirror_lh}.{mirror_schema}.{f4211}
    SET    units_transaction_qty = units_transaction_qty - {delta}
    WHERE  company_key_order_no     = '{company}'
      AND  document_order_invoice_e = '{order_no}'
      AND  order_type               = '{order_type}'
      AND  line_number              = {line_no}
""".format(
    mirror_lh     = MIRROR_LH,
    mirror_schema = MIRROR_SCHEMA,
    f4211         = F4211_TABLE,
    delta         = TEST_DELTA,
    company       = COMPANY,
    order_no      = ORDER_NO,
    order_type    = ORDER_TYPE,
    line_no       = LINE_NO,
))

print("Reverted — subtracted {} from units_transaction_qty.".format(TEST_DELTA))
print("Wait 30-60s, then run Cell 4 again to confirm Gold shows the original value.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 6 — Confirm revert in Silver
# ============================================================================
# Reads the current Silver value directly (not via CDF) to confirm the UPDATE
# in Cell 5 landed correctly before the stream picks it up.
# ============================================================================

print("=== CELL 6: Confirm revert in Silver ===")

spark.sql("""
    SELECT
        company_key_order_no,
        document_order_invoice_e,
        order_type,
        line_number,
        units_transaction_qty,
        address_number_ship_to,
        cost_center
    FROM {mirror_lh}.{mirror_schema}.{f4211}
    WHERE company_key_order_no     = '{company}'
      AND document_order_invoice_e = '{order_no}'
      AND order_type               = '{order_type}'
      AND line_number              = {line_no}
""".format(
    mirror_lh     = MIRROR_LH,
    mirror_schema = MIRROR_SCHEMA,
    f4211         = F4211_TABLE,
    company       = COMPANY,
    order_no      = ORDER_NO,
    order_type    = ORDER_TYPE,
    line_no       = LINE_NO,
)).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 7 — Full CDF history for this row (diagnostic)
# ============================================================================
# Shows every CDF event ever recorded for this order line.
# Useful if Cell 4 is not updating — you can see whether CDF captured the
# change and what version it was committed at.
# Compare _commit_version against the init_ver printed by the streaming notebook
# at startup — if your change version <= init_ver the handler skipped it.
# ============================================================================

print("=== CELL 7: Full CDF history for this row ===")

(
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("{}.{}.{}".format(MIRROR_LH, MIRROR_SCHEMA, F4211_TABLE))
    .filter(
        (F.col("company_key_order_no")     == COMPANY)
        & (F.col("document_order_invoice_e") == ORDER_NO)
        & (F.col("order_type")               == ORDER_TYPE)
        & (F.col("line_number")              == LINE_NO)
    )
    .orderBy("_commit_timestamp", ascending=False)
    .select(
        "_change_type",
        "_commit_timestamp",
        "_commit_version",
        "company_key_order_no",
        "document_order_invoice_e",
        "order_type",
        "line_number",
        "units_transaction_qty",
    )
    .show(20, truncate=False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 8 — Check active streams (diagnostic)
# ============================================================================
# Lists all currently running Spark streaming queries in this session.
# You should see 6 streams named fact__f4211..., fact__f4201..., etc.
# If the list is empty the streaming notebook has stopped or was never started.
# ============================================================================

print("=== CELL 8: Active streams ===")

active = spark.streams.active
if not active:
    print("WARNING — no active streams found.")
    print("Make sure nb_silver_to_gold_eso2_v2_stm is running in another session.")
else:
    print("{} active stream(s):".format(len(active)))
    for q in active:
        print("  name={:50s}  status={}".format(q.name, q.status["message"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
