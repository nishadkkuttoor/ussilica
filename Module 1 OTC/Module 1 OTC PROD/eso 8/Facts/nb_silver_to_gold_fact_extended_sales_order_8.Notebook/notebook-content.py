# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "915ea8b7-e01a-4182-b41a-c283df48a086",
# META       "default_lakehouse_name": "lh_jde_silver",
# META       "default_lakehouse_workspace_id": "9ea13355-c802-4ca5-883f-e5dbf8ecc720",
# META       "known_lakehouses": [
# META         {
# META           "id": "915ea8b7-e01a-4182-b41a-c283df48a086"
# META         },
# META         {
# META           "id": "bed869e4-f15b-4cc1-9368-c7a9b3e08a83"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — Imports & constants
# ─────────────────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# ── Silver source tables ──────────────────────────────────────────────────────
F4201_TBL    = "lh_jde_silver.jde.f4201_sales_order_header_file"
F5642B01_TBL = "lh_jde_silver.jde.f5642b01_custom_sales_order_entry_screen_header"
F5542035_TBL = "lh_jde_silver.jde.f5542035_order_re_date_audit_history_table"

# ── Gold output table ─────────────────────────────────────────────────────────
FACT_TABLE = "lh_jde_gold.rpt.fact_extended_sales_order_8"

# ── NOTE: F0101 (Address Book) is NOT loaded here. ───────────────────────────
# BILLTO name  (AN8)      → resolved via Power BI relationship to dim_address_book
# SHIPTO name  (SHAN)     → resolved via Power BI relationship to dim_address_book
# LOADPORT name (loadport)→ resolved via Power BI relationship to dim_address_book

print(f"Run timestamp : {datetime.now()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 2 — Helper: load silver
# ─────────────────────────────────────────────────────────────────────────────
_EXCLUDE_COLS = ["is_delete", "deleted_date_time"]

def load_silver(table_name):
    """Read silver table, drop soft-deleted rows, drop pipeline metadata."""
    df = spark.read.table(table_name)
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _EXCLUDE_COLS])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 3 — Load all silver tables
# ─────────────────────────────────────────────────────────────────────────────
df_f4201    = load_silver(F4201_TBL)
df_f5642b01 = load_silver(F5642B01_TBL)
df_f5542035 = load_silver(F5542035_TBL)

# print("All silver tables loaded.")
# print(f"  F4201    : {df_f4201.count():,}")
# print(f"  F5642B01 : {df_f5642b01.count():,}")
# print(f"  F5542035 : {df_f5542035.count():,}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Prep: F5542035 — Filter MAX ASMATH01 per ASDOCO + ASUPMJ
#
# Replicates original correlated subquery:
#   WHERE M.ASMATH01 = (SELECT MAX(A.ASMATH01)
#                       FROM F5542035 A
#                       WHERE A.ASDOCO = M.ASDOCO
#                       AND   A.ASUPMJ = M.ASUPMJ)
#
# Silver column mapping (from metadata):
#   ASDL01   → description_001              (grain / Page Filter)
#   ASMCU    → cost_center                  (grain / Page Filter)
#   ASDOCO   → document_order_invoice_e     (join key / grain)
#   ASDCTO   → order_type                   (join key / grain)
#   ASKCOO   → company_key_order_no         (join key / grain)
#   ASEV02   → everest_event_point_02       (Visual col)
#   ASRSDJ   → date_release_julian          (OLDPICK — already DateType)
#   ASPDDJ   → scheduled_pick_date          (NEWPICK — already DateType)
#   ASEDSP   → edi_successfully_process     (Page Filter)
#   ASMGTX   → message_text                 (Visual col)
#   ASJOBN   → work_station_id              (Page Filter)
#   ASPID    → program_id                   (Page Filter)
#   ASUSER   → user_id                      (Page Filter)
#   ASUPMJ   → date_updated                 (UPMJ — already DateType)
#   ASUPMT   → time_last_updated            (Page Filter)
#   ASSHPN   → shipment_number              (join key → F4941 / F5642B01)
#   ASMATH01 → math_numeric_01              (used for MAX filter only — dropped after)
# ─────────────────────────────────────────────────────────────────────────────

# Step 2: Apply window using original silver column names ONLY
window_max_asmath01 = Window.partitionBy(
    "document_order_invoice_e",   # ASDOCO — original silver name
    "date_updated"                # ASUPMJ — original silver name
)

# Step 3: withColumn uses original names — safe
df_f5542035_filtered = (
    df_f5542035
    .withColumn(
        "max_asmath01",
        F.max("math_numeric_01").over(window_max_asmath01)    # ASMATH01
    )
    .filter(F.col("math_numeric_01") == F.col("max_asmath01"))
    .drop("max_asmath01")
)

# print(f"F5542035 after MAX ASMATH01 filter : {df_f5542035_filtered.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — Prep: F4201 (Sales Order Header — LEFT join)
#
# Replicates:
#   LEFT JOIN F4201 ON SHDOCO=ASDOCO AND SHDCTO=ASDCTO AND SHKCOO=ASKCOO
#
# Silver column mapping (from metadata):
#   SHDOCO  → document_order_invoice_e   (join key)
#   SHDCTO  → order_type                 (join key)
#   SHKCOO  → company_key_order_no       (join key)
#   SHAN8   → address_number             (AN8  — FK exposed to Power BI dim_address_book)
#   SHSHAN  → address_number_ship_to     (SHAN — FK exposed to Power BI dim_address_book)
#   SHVR01  → reference_01               (VR01 — Customer PO Number)
# ─────────────────────────────────────────────────────────────────────────────
df_f4201_slim = (
    df_f4201
    .select(
        F.col("document_order_invoice_e").alias("hdr_doco"),    # SHDOCO
        F.col("order_type").alias("hdr_dcto"),                  # SHDCTO
        F.col("company_key_order_no").alias("hdr_kcoo"),        # SHKCOO
        F.col("address_number").alias("an8"),                   # SHAN8  → PBI dim_address_book FK
        F.col("address_number_ship_to").alias("shan"),          # SHSHAN → PBI dim_address_book FK
        F.col("reference_01").alias("vr01"),                    # SHVR01
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 8 — Prep: F5642B01 — LOADDATE + BOOKINGNO
#
# Replicates:
#   LEFT JOIN F5642B01
#     ON BADOCO=ASDOCO AND BASHPN=ASSHPN AND BADCTO=ASDCTO AND BAKCOO=ASKCOO
#
# Silver column mapping (from metadata):
#   BADOCO   → document_order_invoice_e   (join key)
#   BASHPN   → shipment_number            (join key)
#   BADCTO   → order_type                 (join key)
#   BAKCOO   → company_key_order_no       (join key)
#   BADLPU   → date_latest_pickup         (LOADDATE — already DateType)
#   BA55BKNO → booking_no                 (BOOKINGNO)
# ─────────────────────────────────────────────────────────────────────────────
df_f5642b01_slim = (
    df_f5642b01
    .select(
        F.col("document_order_invoice_e").alias("bk_doco"),    # BADOCO
        F.col("shipment_number").alias("bk_shpn"),             # BASHPN
        F.col("order_type").alias("bk_dcto"),                  # BADCTO
        F.col("company_key_order_no").alias("bk_kcoo"),        # BAKCOO
        F.col("date_latest_pickup").alias("load_date"),        # BADLPU — DateType
        F.col("booking_no").alias("booking_no"),               # BA55BKNO
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 9 — Joins
#
# BASE  : F5542035  (M — re-date audit history, MAX filtered)
# JOIN 1: F4201     LEFT   ON ASDOCO=SHDOCO AND ASDCTO=SHDCTO AND ASKCOO=SHKCOO
# JOIN 4: F5642B01  LEFT   ON ASDOCO/ASSHPN/ASDCTO/ASKCOO
#
# NOTE: F0101 joins for BILLTO/SHIPTO/LOADPORT names are intentionally omitted.
#       Name resolution is handled via Power BI relationships to dim_address_book
#       using the FK keys: an8 (BILLTO), shan (SHIPTO), load_port (LOADPORT).
# ─────────────────────────────────────────────────────────────────────────────
df_joined = (
    df_f5542035_filtered.alias("m")

    # ── JOIN 1: F4201 — LEFT (AN8, SHAN, VR01) ───────────────────────────────
    .join(
        df_f4201_slim.alias("hdr"),
        (F.col("m.document_order_invoice_e") == F.col("hdr.hdr_doco"))
        & (F.col("m.order_type")              == F.col("hdr.hdr_dcto"))
        & (F.col("m.company_key_order_no")    == F.col("hdr.hdr_kcoo")),
        "left",
    )

    # ── JOIN 4: F5642B01 — LEFT (LOADDATE, BOOKINGNO) ───────────────────────
    .join(
        df_f5642b01_slim.alias("bk"),
        (F.col("m.document_order_invoice_e") == F.col("bk.bk_doco"))
        & (F.col("m.shipment_number")         == F.col("bk.bk_shpn"))
        & (F.col("m.order_type")              == F.col("bk.bk_dcto"))
        & (F.col("m.company_key_order_no")    == F.col("bk.bk_kcoo")),
        "left",
    )
)
print(f"after join : {df_joined.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 10 — SELECT DISTINCT
#
# Column mapping (27 columns):
#
# ┌────┬──────────────────────────────┬────────────┬──────────────────────────────────────────┬──────────────┐
# │ #  │ Fact Column                  │ Bronze Col │ Silver Column                            │ Source Table │
# ├────┼──────────────────────────────┼────────────┼──────────────────────────────────────────┼──────────────┤
# │  1 │ process                      │ ASDL01     │ description_001                          │ F5542035     │
# │  2 │ plant                        │ ASMCU      │ cost_center                              │ F5542035     │
# │  3 │ order_number                 │ ASDOCO     │ document_order_invoice_e                 │ F5542035     │
# │  4 │ order_type                   │ ASDCTO     │ order_type                               │ F5542035     │
# │  5 │ company                      │ ASKCOO     │ company_key_order_no                     │ F5542035     │
# │  6 │ hook_order                   │ ASEV02     │ everest_event_point_02                   │ F5542035     │
# │  7 │ customer_number              │ AN8        │ address_number (via F4201)               │ F4201        │
# │  8 │ ship_to_number               │ SHAN       │ address_number_ship_to (via F4201)       │ F4201        │
# │  9 │ customer_po                  │ VR01       │ reference_01 (via F4201)                 │ F4201        │
# │ 10 │ old_sch_pick_date            │ OLDPICK    │ date_release_julian                      │ F5542035     │
# │ 11 │ new_sch_pick_date            │ NEWPICK    │ scheduled_pick_date                      │ F5542035     │
# │ 12 │ status_flag                  │ ASEDSP     │ edi_successfully_process                 │ F5542035     │
# │ 13 │ error_message                │ ASMGTX     │ message_text                             │ F5542035     │
# │ 14 │ export_booking_number        │ BOOKINGNO  │ booking_no                               │ F5642B01     │
# │ 15 │ export_load_date             │ LOADDATE   │ date_latest_pickup                       │ F5642B01     │
# │ 16 │ program_id                   │ ASPID      │ program_id                               │ F5542035     │
# │ 17 │ user                         │ ASUSER     │ user_id                                  │ F5542035     │
# │ 18 │ run_date                     │ UPMJ       │ date_updated                             │ F5542035     │
# │ 19 │ run_time                     │ ASUPMT     │ time_last_updated                        │ F5542035     │
# │ 20 │ work_station                 │ ASJOBN     │ work_station_id                          │ F5542035     │
# └────┴──────────────────────────────┴────────────┴──────────────────────────────────────────┴──────────────┘
#

df_fact = (
    df_joined
    .select(

        # ── Col 1  : ASDL01 — Process ─────────────────────────────────────────
        F.col("m.description_001").alias("process"),

        # ── Col 2  : ASMCU — Plant ────────────────────────────────────────────
        F.col("m.cost_center").alias("plant"),

        # ── Col 3  : ASDOCO — Order Number ───────────────────────────────────
        F.col("m.document_order_invoice_e").alias("order_number"),

        # ── Col 4  : ASDCTO — Order Type ─────────────────────────────────────
        F.col("m.order_type").alias("order_type"),

        # ── Col 5  : ASKCOO — Company ─────────────────────────────────────────
        F.col("m.company_key_order_no").alias("company"),

        # ── Col 6  : ASEV02 — Hook Order (Y/N) ───────────────────────────────
        F.col("m.everest_event_point_02").alias("hook_order"),

        # ── Col 7  : AN8 — Customer Number (FK → dim_address_book) ───────────
        F.col("hdr.an8").alias("customer_number"),

        # ── Col 8  : SHAN — Ship To Number (FK → dim_address_book) ───────────
        F.col("hdr.shan").alias("ship_to_number"),

        # ── Col 9  : VR01 — Customer PO ──────────────────────────────────────
        F.col("hdr.vr01").alias("customer_po"),

        # ── Col 10 : OLDPICK — Old Sch Pick Date ─────────────────────────────
        F.col("m.date_release_julian").alias("old_sch_pick_date"),

        # ── Col 11 : NEWPICK — New Sch Pick Date ─────────────────────────────
        F.col("m.scheduled_pick_date").alias("new_sch_pick_date"),

        # ── Col 12 : ASEDSP — Status Flag ────────────────────────────────────
        F.col("m.edi_successfully_process").alias("status_flag"),

        # ── Col 13 : ASMGTX — Error Message ──────────────────────────────────
        F.col("m.message_text").alias("error_message"),

        # ── Col 14 : BOOKINGNO — Export Booking Number ────────────────────────
        F.col("bk.booking_no").alias("export_booking_number"),

        # ── Col 15 : LOADDATE — Export Load Date ──────────────────────────────
        F.col("bk.load_date").alias("export_load_date"),

        # ── Col 16 : ASPID — Program ID ──────────────────────────────────────
        F.col("m.program_id").alias("program_id"),

        # ── Col 17 : ASUSER — User ────────────────────────────────────────────
        F.col("m.user_id").alias("user"),

        # ── Col 18 : UPMJ — Run Date ──────────────────────────────────────────
        F.col("m.date_updated").alias("run_date"),

        # ── Col 19 : ASUPMT — Run Time ───────────────────────────────────────
        F.col("m.time_last_updated").alias("run_time"),

        # ── Col 20 : ASJOBN — Work Station ───────────────────────────────────
        F.col("m.work_station_id").alias("work_station"),

        # ── Col 21 : ASSHPN — Shipment Number ───────────────────────────────────
        F.col("m.shipment_number").alias("shipment_number"),
    )
    .distinct()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

grain_cols = ["order_number", "order_type", "company", "shipment_number", "run_date"]

df_fact = df_fact.withColumn(
    "shipment_key",
    F.concat_ws("|", *grain_cols)
)

# order_key — composite surrogate for the Power BI relationship to dim_order.
# Power BI relationships are single-column; (order_number, order_type, company)
# is a 3-column business key, so both this fact and dim_order carry the same
# concatenated surrogate to join on.
df_fact = df_fact.withColumn(
    "order_key",
    F.concat_ws("|", "order_number", "order_type", "company")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 11 — Validate before write
# ─────────────────────────────────────────────────────────────────────────────
dup_check = (
    df_fact.groupBy(*grain_cols).count().filter(F.col("count") > 1)
)
dup_count = dup_check.count()
assert dup_count == 0, (
    f"Fact grain violated — {dup_count} duplicate keys found. "
    f"Direct Lake relationships will FAIL if this table is written with "
    f"duplicate shipment_key values. Investigate before proceeding."
)
print("✓ Grain uniqueness verified — safe for Direct Lake relationship.")

print(f"Fact row count (pre-write) : {df_fact.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 15 — Write fact table
# ─────────────────────────────────────────────────────────────────────────────
df_fact.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(FACT_TABLE)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

spark.sql(f"OPTIMIZE {FACT_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"✓ {FACT_TABLE}  →  {spark.read.table(FACT_TABLE).count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
