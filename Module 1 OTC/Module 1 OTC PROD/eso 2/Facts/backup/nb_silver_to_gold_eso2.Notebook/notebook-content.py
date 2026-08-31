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
from datetime import datetime

# ── Silver source tables ──────────────────────────────────────────────────────
F4211_TBL    = "lh_jde_silver.jde.f4211_sales_order_detail_file"
F4201_TBL    = "lh_jde_silver.jde.f4201_sales_order_header_file"
F0101_TBL    = "lh_jde_silver.jde.f0101_address_book_master"
F0010_TBL    = "lh_jde_silver.jde.f0010_company_constants"
F0006_TBL    = "lh_jde_silver.jde.f0006_business_unit_master"
F5549002_TBL = "lh_jde_silver.jde.f5549002_mxp_bol_interface_detail"
F41002_TBL   = "lh_jde_silver.jde.f41002_item_units_of_measure_conversion_factors"


# ── Gold output table ─────────────────────────────────────────────────────────
FACT_TABLE = "lh_jde_gold.eso2.fact_extended_sales_order_2"

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
df_f4211    = load_silver(F4211_TBL)
df_f4201    = load_silver(F4201_TBL)
df_f0101    = load_silver(F0101_TBL)
df_f0010    = load_silver(F0010_TBL)
df_f0006    = load_silver(F0006_TBL)
df_f5549002 = load_silver(F5549002_TBL)
df_f41002   = load_silver(F41002_TBL)

print("All silver tables loaded.")
print(f"  F4211    : {df_f4211.count():,}")
print(f"  F4201    : {df_f4201.count():,}")
print(f"  F0010    : {df_f0010.count():,}")
print(f"  F0101    : {df_f0101.count():,}")
print(f"  F0006    : {df_f0006.count():,}")
print(f"  F5549002 : {df_f5549002.count():,}")
print(f"  F41002   : {df_f41002.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Prep: F0101 DQ gate (INNER join — ABAT1 filtered subquery)
#
# MySQL/Hubble exact logic:
#   INNER JOIN (
#       SELECT ABALPH, ABAN8 FROM F0101
#       WHERE  ABAT1 BETWEEN 'A  ' AND 'P  '
#           OR ABAT1 BETWEEN 'R  ' AND 'ZZZ'
#   ) F0101 ON F4211.SDSHAN = F0101.ABAN8
#
# NOTE: ship_to_name (ABALPH) is NOT stored on the fact table.
#       It will be resolved in Power BI via:
#           fact[address_number_ship_to] → dim_address_book[address_number] → dim[name_alpha]
#       The DQ gate (ABAT1 filter) is still applied here to ensure only
#       valid address book records drive the INNER JOIN row filter.
#
# Silver column mapping:
#   ABAN8  → address_number    (join key)
#   ABAT1  → address_type_01   (DQ gate filter)
#   ABSIC  → standard_industry_code (SIC Code — Page Filter column on fact)
# ─────────────────────────────────────────────────────────────────────────────
df_f0101_dq = (
    df_f0101
    .filter(
        (F.trim(F.col("address_type_01")).between("A", "P"))
        | (F.trim(F.col("address_type_01")).between("R", "ZZZ"))
    )
    .select(
        F.col("address_number").alias("dq_aban8"),              # ABAN8  — join key
        F.col("standard_industry_code").alias("sic_code"),      # ABSIC  — Page Filter
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — Prep: F4201 (Sales Order Header — LEFT join)
#
# MySQL/Hubble:
#   LEFT JOIN F4201 ON SDKCOO=SHKCOO AND SDDOCO=SHDOCO AND SDDCTO=SHDCTO
#
# Silver column mapping:
#   SHKCOO → company_key_order_no     (join key 1)
#   SHDOCO → document_order_invoice_e (join key 2)
#   SHDCTO → order_type               (join key 3)
#   SHDEL1 → delivery_instruct_line_01 (Delivery Instructions — Visual col)
# ─────────────────────────────────────────────────────────────────────────────
df_f4201_slim = (
    df_f4201
    .select(
        F.col("company_key_order_no").alias("hdr_kcoo"),              # SHKCOO
        F.col("document_order_invoice_e").alias("hdr_doco"),          # SHDOCO
        F.col("order_type").alias("hdr_dcto"),                        # SHDCTO
        F.col("delivery_instruct_line_01").alias("delivery_instructions"),  # SHDEL1
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 6 — Prep: F0010 (Company Constants — INNER join)
#
# MySQL/Hubble: INNER JOIN F0010 ON F4211.SDKCOO = F0010.CCCO
#
# Silver column mapping:
#   CCCO   → company                              (join key + Page Filter)
#   CCPNC  → period_number_current                (MTD month integer 1-12)
#   CCARFJ → date_ar_fiscal_year_begins_julian    (DateType — MTD year anchor)
# ─────────────────────────────────────────────────────────────────────────────
df_f0010_slim = (
    df_f0010
    .select(
        F.col("company").alias("ccco"),                                        # CCCO
        F.col("period_number_current").alias("ccpnc"),                         # CCPNC
        F.col("date_ar_fiscal_year_begins_julian").alias("ccarfj"),             # CCARFJ
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 7 — Prep: F5549002 (Scale Ticket — LEFT join)
#
# MySQL/Hubble:
#   LEFT JOIN F5549002 ON SDKCOO=MIKCOO AND SDDOCO=MIDOCO
#                      AND SDDCTO=MIDCTO AND SDLNID=MILNID
#
# Silver column mapping:
#   MIKCOO → company_key_order_no      (join key 1)
#   MIDOCO → document_order_invoice_e  (join key 2)
#   MIDCTO → order_type                (join key 3)
#   MILNID → line_number               (join key 4 — silver already /1000)
#   MIGRWT → gross_weight              (silver already /10000)
#   MICTWT → catch_weight              (silver already /10000)
#   MIMXWT → maximum_weight            (silver already /100)
# ─────────────────────────────────────────────────────────────────────────────
df_scale = (
    df_f5549002
    .select(
        F.col("company_key_order_no").alias("sc_kcoo"),      # MIKCOO
        F.col("document_order_invoice_e").alias("sc_doco"),  # MIDOCO
        F.col("order_type").alias("sc_dcto"),                # MIDCTO
        F.col("line_number").alias("sc_lnid"),               # MILNID
        F.col("gross_weight").alias("gross_weight"),         # MIGRWT
        F.col("catch_weight").alias("tare_weight"),          # MICTWT
        F.col("maximum_weight").alias("net_weight"),         # MIMXWT
    )
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 8 — Prep: F41002 UoM → TN conversion
#
# Forward : UMRUM = 'TN' → factor converts from_uom → TN directly
# Reverse : UMUM  = 'TN' → factor is stored inverted; flip it
# Result  : quantity_loaded = units_transaction_qty * conv_factor
# ─────────────────────────────────────────────────────────────────────────────

# Forward: source UoM → TN (UMRUM = 'TN')
df_conv_fwd = (
    df_f41002
    .filter(
        (F.trim(F.col("related_uom")) == "TN")
        & (F.col("conversion_factor") != 0)
    )
    .select(
        F.col("identifier_short_item").alias("conv_itm"),
        F.trim(F.col("uom")).alias("conv_from_uom"),
        F.col("conversion_factor").cast("double").alias("conv_factor"),
    )
)

# Reverse: TN stored as source (UMUM = 'TN') → invert factor
df_conv_rev = (
    df_f41002
    .filter(
        (F.trim(F.col("uom")) == "TN")
        & (F.col("conversion_factor") != 0)
    )
    .select(
        F.col("identifier_short_item").alias("conv_itm"),
        F.trim(F.col("related_uom")).alias("conv_from_uom"),
        (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_factor"),
    )
)

# Union forward + reverse, deduplicate
df_conv = (
    df_conv_fwd
    .unionByName(df_conv_rev)
    .dropDuplicates(["conv_itm", "conv_from_uom"])
)

print(f"F41002 conversion rows: {df_conv.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 9 — Joins (exact MySQL/Hubble join order and types)
#
# JOIN 1: F0010    INNER  ON SDKCOO = CCCO
# JOIN 2: F0101    INNER  ON SDSHAN = ABAN8  (ABAT1 DQ gate applied in prep)
# JOIN 3: F4201    LEFT   ON SDKCOO=SHKCOO AND SDDOCO=SHDOCO AND SDDCTO=SHDCTO
# JOIN 4: F5549002 LEFT   ON 4-key join
# JOIN 5: F41002   LEFT   ON SDITM=UMITM AND SDUOM=UMUM
#
# NOTE: ship_to_name (ABALPH) is resolved via dim in Power BI — NOT stored
#       on the fact. The F0101 join here is purely for the ABAT1 DQ gate
#       and to bring in ABSIC (SIC Code) as a Page Filter column.
# ─────────────────────────────────────────────────────────────────────────────
df_joined = (
    df_f4211.alias("sd")

    # ── JOIN 1: F0010 — INNER ─────────────────────────────────────────────────
    .join(
        df_f0010_slim.alias("co"),
        F.col("sd.company_key_order_no") == F.col("co.ccco"),
        "inner",
    )

    # ── JOIN 2: F0101 — INNER (ABAT1 DQ gate + SIC code) ─────────────────────
    .join(
        df_f0101_dq.alias("ab"),
        F.col("sd.address_number_ship_to") == F.col("ab.dq_aban8"),
        "inner",
    )

    # ── JOIN 3: F4201 — LEFT (Delivery Instructions) ──────────────────────────
    .join(
        df_f4201_slim.alias("hdr"),
        (F.col("sd.company_key_order_no")      == F.col("hdr.hdr_kcoo"))
        & (F.col("sd.document_order_invoice_e") == F.col("hdr.hdr_doco"))
        & (F.col("sd.order_type")               == F.col("hdr.hdr_dcto")),
        "left",
    )

    # ── JOIN 4: F5549002 — LEFT (Scale ticket weights) ────────────────────────
    .join(
        df_scale.alias("sc"),
        (F.col("sd.company_key_order_no")      == F.col("sc.sc_kcoo"))
        & (F.col("sd.document_order_invoice_e") == F.col("sc.sc_doco"))
        & (F.col("sd.order_type")               == F.col("sc.sc_dcto"))
        & (F.col("sd.line_number")              == F.col("sc.sc_lnid")),
        "left",
    )

    # ── JOIN 5: F41002 — LEFT (UoM → TN conversion) ───────────────────────────
    .join(
        df_conv.alias("ci"),
        (F.col("sd.identifier_short_item")  == F.col("ci.conv_itm"))
        & (F.trim(F.col("sd.uom_as_input")) == F.col("ci.conv_from_uom")),
        "left",
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 10 — MTD Filters / YTD Filters (exact Hubble WHERE logic)
#
# Silver has converted all Julian dates to DateType — the MySQL SUBSTRING
# arithmetic on Julian integers collapses to native PySpark date functions:
# MySQL Condition 1: (CCPNC - julian_doy_to_month(SDADDJ)) = 0
#   → PySpark: MONTH(actual_ship_date) = period_number_current
#
# MySQL Condition 2: (SUBSTRING(SDADDJ,2,2) - SUBSTRING(CCARFJ,2,2)) = 0
#   → PySpark: YEAR(actual_ship_date) = YEAR(date_ar_fiscal_year_begins_julian)

## This is to be commented out only if latest month data to be stored in final table and not previous history 
## else don't touch this cell.

# ─────────────────────────────────────────────────────────────────────────────
df_filtered = (
    df_joined
#     # Condition 1 — Ship month = current period number
#     .filter(
#         F.month(F.col("sd.actual_ship_date")) == F.col("co.ccpnc")
#     )

#     # Condition 2 — Ship year = fiscal year begin year
#     .filter(
#         F.year(F.col("sd.actual_ship_date")) == F.year(F.col("co.ccarfj"))
#     )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 11 — Derived columns
#
# quantity_loaded (TN):
#   quantity_loaded = units_transaction_qty (SDUORG) * conv_factor_to_tn
#
#   COALESCE priority:
#     1. If SDUOM already 'TN' → conv_factor = 1.0
#     2. F41002 has a factor   → use it
#     3. No factor found       → store raw (factor = 1.0)
# ─────────────────────────────────────────────────────────────────────────────
df_derived = (
    df_filtered

    # Resolve UoM → TN conversion factor
    .withColumn(
        "conv_factor_to_tn",
        F.coalesce(
            F.when(F.trim(F.col("sd.uom_as_input")) == "TN", F.lit(1.0)),
            F.col("ci.conv_factor"),
            F.lit(1.0),
        )
    )

    # Quantity Loaded in TN
    .withColumn(
        "quantity_loaded",
        F.round(
            F.col("sd.units_transaction_qty").cast("double")
            * F.col("conv_factor_to_tn"),
            4
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 12 — Inner SELECT DISTINCT  (Hubble inner subquery)
#
# Column mapping (all confirmed from spec + schema):
#
# ┌─────────────────────────────────┬─────────┬──────────────────────────────────────┬──────────────┬──────────────┐
# │ Fact column                     │ Bronze  │ Silver column                        │ Visual       │ Gold Table   │
# ├─────────────────────────────────┼─────────┼──────────────────────────────────────┼──────────────┼──────────────┤
# │ actual_ship_date                │ SDADDJ  │ actual_ship_date (DateType)          │ Visual       │ Fact         │
# │ ship_to (FK→dim_address_book)   │ SDSHAN  │ address_number_ship_to               │ Visual/Filter│ Fact         │
# │ customer_po_number              │ SDVR01  │ reference_01                         │ Visual/Filter│ Fact         │
# │ item_description                │ SDDSC1  │ description_line_01                  │ Visual       │ Fact         │
# │ bol_number                      │ SDURAB  │ user_reserved_number                 │ Visual       │ Fact         │
# │ alt_bol_number                  │ SDPSIG  │ pull_signal                          │ Visual       │ Fact         │
# │ invoice_number                  │ SDDOC   │ doc_voucher_invoice_e                │ Visual/Filter│ Fact         │
# │ vehicle_number                  │ SDCNID  │ container_id                         │ Visual       │ Fact         │
# │ well_name                       │ SDVR02  │ reference_02_vendor                  │ Visual       │ Fact         │
# │ transactional_uom               │ SDUOM   │ uom_as_input                         │ Visual       │ Fact         │
# │ gross_weight                    │ MIGRWT  │ gross_weight (F5549002)              │ Visual       │ Fact         │
# │ tare_weight                     │ MICTWT  │ catch_weight  (F5549002)             │ Visual       │ Fact         │
# │ net_weight                      │ MIMXWT  │ maximum_weight(F5549002)             │ Visual       │ Fact         │
# │ quantity_loaded                 │ calc    │ derived (TN conversion)              │ Visual       │ Fact         │
# │ delivery_instructions           │ SHDEL1  │ delivery_instruct_line_01 (F4201)    │ Visual       │ Fact         │
# │ quantity_shipped_uom            │ SDUORG  │ units_transaction_qty (measure)      │ -            │ Fact         │
# │ quantity_ordered_primary        │ SDPQOR  │ units_primary_qty_order (measure)    │ -            │ Fact         │
# │ current_period_number           │ CCPNC   │ period_number_current (F0010)        │ -            │ Fact         │
# │ fiscal_year_begin_date          │ CCARFJ  │ date_ar_fiscal_year_begins_julian    │ -            │ Fact         │
# │ scale_line_number               │ MILNID  │ line_number (F5549002)               │ -            │ Fact         │
# │ last_status                     │ SDLTTR  │ status_code_last                     │ Page Filter  │ Fact         │
# │ next_status                     │ SDNXTR  │ status_code_next                     │ Page Filter  │ Fact         │
# │ original_order_type             │ SDOCTO  │ original_order_type                  │ Page Filter  │ Fact         │
# │ sic_code                        │ ABSIC   │ standard_industry_code (F0101)       │ Page Filter  │ Fact         │
# │ load_number                     │ SDDOCO  │ document_order_invoice_e             │ Page Filter  │ Fact         │
# │ company                         │ CCCO    │ company (F0010)                      │ Page Filter  │ Fact         │
# │ line_type                       │ SDLNTY  │ line_type                            │ Page Filter  │ Fact         │
# │ item_number                     │ SDLITM  │ identifier_second_item               │ Page Filter  │ Fact         │
# │ plant                           │ SDMCU   │ cost_center                          │ Page Filter  │ Fact         │
# │ requested_date                  │ SDDRQJ  │ date_requested_julian (DateType)     │ Page Filter  │ Fact         │
# │ invoice_date                    │ SDIVD   │ date_invoice_julian (DateType)       │ Page Filter  │ Fact         │
# │ gl_date                         │ SDDGL   │ dt_for_gl_and_vouch_01 (DateType)    │ Page Filter  │ Fact         │
# │ parent_number                   │ SDPA8   │ address_number_parent                │ Page Filter  │ Fact         │
# │ key_company_order               │ SDKCOO  │ company_key_order_no (grain key)     │ -            │ Fact         │
# │ order_number                    │ SDDOCO  │ document_order_invoice_e (grain key) │ -            │ Fact         │
# │ order_type                      │ SDDCTO  │ order_type (grain key)               │ -            │ Fact         │
# │ line_number                     │ SDLNID  │ line_number (grain key, /1000)       │ -            │ Fact         │
# └─────────────────────────────────┴─────────┴──────────────────────────────────────┴──────────────┴──────────────┘
#
# NOTE: ship_to_name (ABALPH) is intentionally excluded from the fact table.
#       It will be resolved in Power BI via the relationship:
#           fact[ship_to] → dim_address_book[address_number] → dim[name_alpha]
# ─────────────────────────────────────────────────────────────────────────────
df_inner = (
    df_derived
    .select(

        # ── Grain keys ────────────────────────────────────────────────────────
        F.col("sd.company_key_order_no").alias("key_company_order"),        # SDKCOO
        F.col("sd.document_order_invoice_e").alias("order_number"),         # SDDOCO
        F.col("sd.order_type").alias("order_type"),                         # SDDCTO
        F.col("sd.line_number").alias("line_number"),                       # SDLNID

        # ── Visual columns ────────────────────────────────────────────────────
        F.col("sd.actual_ship_date").alias("actual_ship_date"),             # SDADDJ — Ship Date
        # ship_to_name (ABALPH) → resolved via dim_address_book in Power BI
        F.col("sd.address_number_ship_to").alias("ship_to"),                # SDSHAN — Ship To (FK→dim)
        F.col("sd.reference_01").alias("customer_po_number"),               # SDVR01 — Customer PO Number
        F.col("sd.description_line_01").alias("item_description"),          # SDDSC1 — Item Description
        F.col("sd.user_reserved_number").alias("bol_number"),               # SDURAB — BOL Number
        F.col("sd.pull_signal").alias("alt_bol_number"),                    # SDPSIG — ALT BOL Number
        F.col("sd.doc_voucher_invoice_e").alias("invoice_number"),          # SDDOC  — Invoice Number
        F.col("sd.container_id").alias("vehicle_number"),                   # SDCNID — Vehicle Number
        F.col("sd.reference_02_vendor").alias("well_name"),                 # SDVR02 — Well Name
        F.col("sd.uom_as_input").alias("transactional_uom"),                # SDUOM  — Transactional UOM
        F.col("sc.gross_weight").alias("gross_weight"),                     # MIGRWT — Gross Weight
        F.col("sc.tare_weight").alias("tare_weight"),                       # MICTWT — Tare Weight
        F.col("sc.net_weight").alias("net_weight"),                         # MIMXWT — Net Weight
        F.col("quantity_loaded"),                                           # Derived — Quantity Loaded (TN)
        F.col("hdr.delivery_instructions").alias("delivery_instructions"),  # SHDEL1 — Delivery Instructions

        # ── Measure columns (SUMmed in outer GROUP BY) ────────────────────────
        F.col("sd.units_transaction_qty").alias("quantity_shipped_uom"),         # SDUORG
        F.col("sd.units_primary_qty_order").alias("quantity_ordered_primary"),   # SDPQOR

        # ── F0010 MTD reference columns ───────────────────────────────────────
        F.col("co.ccco").alias("company"),                                   # CCCO   — Company
        F.col("co.ccpnc").alias("current_period_number"),                    # CCPNC
        F.col("co.ccarfj").alias("fiscal_year_begin_date"),                  # CCARFJ

        # ── Scale ticket line reference ───────────────────────────────────────
        F.col("sc.sc_lnid").alias("scale_line_number"),                     # MILNID — Line Number

        # ── Page Filter columns ───────────────────────────────────────────────
        F.col("sd.status_code_last").alias("last_status"),                  # SDLTTR — Last Status
        F.col("sd.status_code_next").alias("next_status"),                  # SDNXTR — Next Status
        F.col("sd.original_order_type").alias("original_order_type"),       # SDOCTO — Order Type
        F.col("ab.sic_code").alias("sic_code"),                             # ABSIC  — SIC Code
        F.col("sd.document_order_invoice_e").alias("load_number"),          # SDDOCO — Load #
        F.col("sd.line_type").alias("line_type"),                           # SDLNTY — Line Type
        F.col("sd.identifier_second_item").alias("item_number"),            # SDLITM — Item #
        F.col("sd.cost_center").alias("plant"),                             # SDMCU  — Plant
        F.col("sd.date_requested_julian").alias("requested_date"),          # SDDRQJ — Requested Date
        F.col("sd.date_invoice_julian").alias("invoice_date"),              # SDIVD  — Invoice Date
        F.col("sd.dt_for_gl_and_vouch_01").alias("gl_date"),               # SDDGL  — GL Date
        F.col("sd.address_number_parent").alias("parent_number"),           # SDPA8  — Parent #
    )
    # DISTINCT — exact Hubble inner subquery behaviour
    .distinct()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 13 — Outer GROUP BY + SUM  (Hubble outer query)
#
# GROUP BY all non-measure columns.
# SUM the measure columns: quantity_shipped_uom, quantity_ordered_primary.
# quantity_loaded is deterministic per grain row → included in GROUP BY.
#
# NOTE: ship_to_name excluded from GROUP BY (not on fact — resolved via dim).
# ─────────────────────────────────────────────────────────────────────────────
GROUP_BY_COLS = [

    # ── Grain keys ────────────────────────────────────────────────────────────
    "key_company_order",
    "order_number",
    "order_type",
    "line_number",

    # ── Visual columns ────────────────────────────────────────────────────────
    "actual_ship_date",
    # ship_to_name intentionally excluded — resolved via dim in Power BI
    "ship_to",
    "customer_po_number",
    "item_description",
    "bol_number",
    "alt_bol_number",
    "invoice_number",
    "vehicle_number",
    "well_name",
    "transactional_uom",
    "gross_weight",
    "tare_weight",
    "net_weight",
    "quantity_loaded",
    "delivery_instructions",

    # ── F0010 MTD reference ───────────────────────────────────────────────────
    "company",
    "current_period_number",
    "fiscal_year_begin_date",

    # ── Scale reference ───────────────────────────────────────────────────────
    "scale_line_number",

    # ── Page Filter columns ───────────────────────────────────────────────────
    "last_status",
    "next_status",
    "original_order_type",
    "sic_code",
    "load_number",
    "line_type",
    "item_number",
    "plant",
    "requested_date",
    "invoice_date",
    "gl_date",
    "parent_number",
]

df_fact = (
    df_inner
    .groupBy(GROUP_BY_COLS)
    .agg(
        F.sum("quantity_shipped_uom").alias("quantity_shipped_uom"),          # SUM(SDUORG)
        F.sum("quantity_ordered_primary").alias("quantity_ordered_primary"),  # SUM(SDPQOR)
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 14 — Validate before write
# ─────────────────────────────────────────────────────────────────────────────
print(f"Fact row count (pre-write) : {df_fact.count():,}")
print(f"Fact column count          : {len(df_fact.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Core report Validation
display(
    df_fact
    .filter(F.col("parent_number") == 10100242)
    .filter(F.trim(F.col("plant")).isin("061", "501", "571"))
    .filter(F.col("order_type").isin("SO"))
    #.filter(F.col("vehicle_number").isin("AOKX 482836"))
    .filter(F.col("actual_ship_date").between(
        F.to_date(F.lit("2026-07-01")), 
        F.to_date(F.lit("2026-07-01"))
    ))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Halliburton report Validation
display(
    df_fact
    .filter(F.col("parent_number") == 10043240)
    .filter(F.col("order_type").isin("SO"))
    .filter(F.col("company") == "00400")
    .filter(F.col("actual_ship_date").between(
        F.to_date(F.lit("2026-07-01")),
        F.to_date(F.lit("2026-06-12"))
    ))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Nextier report Validation
df_nextier = (
    df_fact
    .filter(F.col("parent_number") == 10100242)
    .filter(F.trim(F.col("plant")).isin("061", "501", "571"))
    .filter(F.col("order_type").isin("SO"))
    .filter(F.col("actual_ship_date").between(
        F.to_date(F.lit("2026-06-08")),
        F.to_date(F.lit("2026-06-08"))
    ))
)
# ── Display filtered records ──────────────────────────────────────────────────
display(df_nextier)

# ── SUM of quantity_loaded ────────────────────────────────────────────────────
print("\n── Quantity Loaded Summary ──")
df_nextier.agg(
    F.count("*").alias("total_rows"),
    F.sum("quantity_loaded").alias("total_quantity_loaded"),
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Pioneer - WTX report Validation
df_pioneer = (
    df_fact
    .filter(F.col("parent_number") == 10112037)
    .filter(F.col("ship_to").isin("10112039","10115878"))
    .filter(F.trim(F.col("plant")).isin("321","341","161","181"))
    .filter(F.col("order_type").isin("SO"))
    .filter(F.col("actual_ship_date").between(
        F.to_date(F.lit("2026-06-02")),
        F.to_date(F.lit("2026-06-02"))
    ))
)
# ── Display filtered records ──────────────────────────────────────────────────
display(df_pioneer)

# ── SUM of quantity_loaded ────────────────────────────────────────────────────
print("\n── Quantity Loaded Summary ──")
df_pioneer.agg(
    F.count("*").alias("total_rows"),
    F.sum("quantity_loaded").alias("total_quantity_loaded"),
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Ascent Load report Validation
# df_ascent = (
#     df_fact
#     #.filter(F.col("parent_number") == 10145146)
#     .filter(F.col("next_status").cast("int") >= 560)
#     .filter(F.col("order_type").isin("SO","CO"))
#     .filter(F.col("actual_ship_date").between(
#         F.to_date(F.lit("2026-05-10")),
#         F.to_date(F.lit("2026-05-10"))
#     ))
# )
# # ── Display filtered records ──────────────────────────────────────────────────
# display(df_ascent)

# # ── SUM of quantity_loaded ────────────────────────────────────────────────────
# print("\n── Quantity Loaded Summary ──")
# df_ascent.agg(
#     F.count("*").alias("total_rows"),
#     F.sum("quantity_loaded").alias("total_quantity_loaded"),
# ).show(truncate=False)

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

spark.sql(f"OPTIMIZE {FACT_TABLE}")
print(f"✓ {FACT_TABLE}  →  {spark.read.table(FACT_TABLE).count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
