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

# ## nb_silver_to_gold_fact_extended_sales_order_7
#
# Gold layer — ESO7-specific flat fact table that pre-joins:
#   F4211    (Sales Order Detail)                → order line columns
#   F5642B01 (Custom SO Entry Screen Header)     → booking/export columns
#   F5642B11 (Custom SO Entry Screen Detail)     → line detail columns
#   F4201    (Sales Order Header)                → hold_code, order_placed_date_julian,
#                                                   date_original_promisde (F4201.SHOPDJ)
#   F4941    (Shipment Routing Steps)            → is_missing_freight flag
#
# UOM conversion (Total Tons) is NOT pre-computed here.
# It is computed in the PBI semantic model via DAX SUMX + RELATED:
#   Total Tons =
#     SUMX(fact_extended_sales_order_7,
#       [units_transaction_qty] *
#       COALESCE(
#         RELATED(dim_uom_conversion_item[conv_factor]),   -- Tier A
#         RELATED(dim_uom_conversion[std_factor]),          -- Tier B
#         1.0))
#
# Semantic model relationships required for Total Tons:
#   fact_extended_sales_order_7.item_uom_key → dim_uom_conversion_item.item_uom_key (Single)
#   fact_extended_sales_order_7.uom_as_input → dim_uom_conversion.from_uom          (Single)
#
# is_missing_freight is computed directly from F4941 and stored as a boolean column
# on every fact row. fact_shipment_routing_v2 is no longer needed for ESO7.
#
# NO BUSINESS FILTERS ARE APPLIED IN THIS NOTEBOOK.

# ─────────────────────────────────────────────────────────────────────────────
# CELL 1 — Imports & constants
# ─────────────────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from datetime import datetime

F4211_TBL    = "lh_jde_silver.jde.f4211_sales_order_detail_file"
F5642B01_TBL = "lh_jde_silver.jde.f5642b01_custom_sales_order_entry_screen_header"
F5642B11_TBL = "lh_jde_silver.jde.f5642b11_custom_sales_order_entry_screen_detail"
F4201_TBL    = "lh_jde_silver.jde.f4201_sales_order_header_file"
F4941_TBL    = "lh_jde_silver.jde.f4941_shipment_routing_steps"
F41002_TBL   = "lh_jde_silver.jde.f41002_item_units_of_measure_conversion_factors"
F41003_TBL   = "lh_jde_silver.jde.f41003_unit_of_measure_standard_conversion"
GOLD_TABLE   = "lh_jde_gold.rpt.fact_extended_sales_order_7_v2"

print(f"Run timestamp : {datetime.now()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 2 — Helper: load silver, drop soft-deleted rows + pipeline metadata
# ─────────────────────────────────────────────────────────────────────────────
_EXCLUDE_COLS = ["is_delete", "deleted_date_time"]

def load_silver(table_name):
    df = spark.read.table(table_name)
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _EXCLUDE_COLS])


def build_uom_cascades():
    # UoM->TN cascade — item-specific F41002 (item-generic, blank cost-center) then standard F41003,
    # each fwd + inverse, resolved directly to TN. Ambiguous keys (>1 distinct factor) collapse to NULL
    # so the join can never fan the grain out.
    f2 = load_silver(F41002_TBL).filter(F.trim(F.col("cost_center")) == "")
    i_fwd = (f2.filter((F.trim("related_uom") == "TN") & (F.col("conversion_factor") != 0))
             .select(F.col("identifier_short_item").alias("itm"), F.trim("uom").alias("from_uom"),
                     F.col("conversion_factor").cast("double").alias("f")))
    i_rev = (f2.filter((F.trim("uom") == "TN") & (F.col("conversion_factor") != 0))
             .select(F.col("identifier_short_item").alias("itm"), F.trim("related_uom").alias("from_uom"),
                     (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("f")))
    item = (i_fwd.unionByName(i_rev).dropDuplicates(["itm", "from_uom", "f"])
            .groupBy("itm", "from_uom").agg(F.count("f").alias("n"), F.min("f").alias("f"))
            .withColumn("conv_factor", F.when(F.col("n") > 1, F.lit(None).cast("double")).otherwise(F.col("f")))
            .select("itm", "from_uom", "conv_factor"))
    f3 = load_silver(F41003_TBL)
    s_fwd = (f3.filter((F.trim("related_uom") == "TN") & (F.col("conversion_factor") != 0))
             .select(F.trim("uom").alias("from_uom"), F.col("conversion_factor").cast("double").alias("f")))
    s_rev = (f3.filter((F.trim("uom") == "TN") & (F.col("conversion_factor") != 0))
             .select(F.trim("related_uom").alias("from_uom"), (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("f")))
    std = (s_fwd.unionByName(s_rev).dropDuplicates(["from_uom", "f"])
           .groupBy("from_uom").agg(F.count("f").alias("n"), F.min("f").alias("f"))
           .withColumn("conv_std", F.when(F.col("n") > 1, F.lit(None).cast("double")).otherwise(F.col("f")))
           .select("from_uom", "conv_std"))
    return item, std

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 3 — Load F4211 (all order types — no filter applied here)
# ─────────────────────────────────────────────────────────────────────────────
df_f4211 = (
    load_silver(F4211_TBL)
    .select(
        F.col("company_key_order_no").alias("company"),
        F.col("document_order_invoice_e").alias("order_number"),
        F.col("order_type").alias("order_type"),
        F.col("line_number").alias("line_number"),
        F.col("line_type").alias("line_type"),
        F.col("order_suffix").alias("order_suffix"),
        F.col("transaction_originator").alias("order_originator"),
        F.col("shipment_number").alias("shipment_number"),
        F.col("cost_center").alias("cost_center"),
        F.col("address_number").alias("address_number"),
        F.col("address_number_ship_to").alias("address_number_ship_to"),
        F.col("status_code_last").alias("status_code_last"),
        F.col("status_code_next").alias("status_code_next"),
        F.col("date_promised_ship_julian").alias("date_promised_ship_julian"),
        F.col("date_requested_julian").alias("date_requested_julian"),
        F.col("scheduled_pick_date").alias("scheduled_pick_date"),
        F.col("date_release_julian").alias("date_release_julian"),
        F.col("identifier_second_item").alias("identifier_second_item"),
        F.col("identifier_short_item").alias("identifier_short_item"),
        F.col("uom_as_input").alias("uom_as_input"),
        F.col("freight_handling_code").alias("freight_handling_code"),
        F.col("carrier").alias("carrier"),
        F.col("mode_of_transport").alias("mode_of_transport"),
        F.col("container_id").alias("container_id"),
        F.col("reference_01").alias("line_reference_01"),
        F.col("units_transaction_qty").alias("units_transaction_qty"),
        F.col("uom_pricing").alias("uom_pricing"),                  # SDUOM4 — Source UoM (Price)
        F.col("amt_price_per_unit_02").alias("unit_price"),         # SDUPRC — Source Price
    )
    .distinct()
)

print(f"F4211 rows (no filter) : {df_f4211.count():,}")
df_f4211.groupBy("order_type").count().orderBy("order_type").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 4 — Load F5642B01 (booking header)
# ─────────────────────────────────────────────────────────────────────────────
df_f5642b01 = (
    load_silver(F5642B01_TBL)
    .select(
        F.col("shipment_number").alias("b01_shipment_number"),
        F.col("document_order_invoice_e").alias("b01_order_number"),
        F.col("order_type").alias("b01_order_type"),
        F.col("company_key_order_no").alias("b01_company"),
        F.col("booking_no").alias("booking_no"),
        F.col("bookingstatus").alias("bookingstatus"),
        F.col("routing_notes").alias("routing_notes"),
        F.col("date_requested_ship").alias("date_requested_ship"),
        F.col("destination_port").alias("destination_port_address"),
        F.col("date_latest_delivery").alias("date_latest_delivery"),
        F.col("vessel_name").alias("vessel_name"),
        F.col("voyage_no").alias("voyage_no"),
        F.col("date_loaded").alias("date_loaded"),
        F.col("date_release_julian").alias("booking_date_release_julian"),
        F.col("date_01").alias("date_01"),
        F.col("loading_port").alias("loading_port_address"),
        F.col("ocean_carrier").alias("ocean_carrier_address"),
        F.col("ocean_del_terms").alias("ocean_del_terms"),
        F.col("no_of_container").alias("no_of_container"),
        F.col("reference_01").alias("booking_reference_01"),
        F.col("reference_02").alias("booking_reference_02"),
        F.col("inland_delterms").alias("inland_delterms"),
        F.col("incoterms").alias("incoterms"),
        F.col("equipment_type").alias("equipment_type"),
        F.col("date_latest_pickup").alias("date_latest_pickup"),
    )
    .distinct()
)

print(f"F5642B01 rows : {df_f5642b01.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 5 — Load F5642B11 (line detail)
# ─────────────────────────────────────────────────────────────────────────────
df_f5642b11 = (
    load_silver(F5642B11_TBL)
    .select(
        F.col("company_key_order_no").alias("b11_company"),
        F.col("document_order_invoice_e").alias("b11_order_number"),
        F.col("order_type").alias("b11_order_type"),
        F.col("line_number").alias("b11_line_number"),
        F.col("shipment_number").alias("b11_shipment_number"),
        F.col("seal_no").alias("seal_no"),
        F.col("production_code").alias("production_code"),
        F.col("production_ship_notes").alias("production_ship_notes"),
    )
    .distinct()
)

print(f"F5642B11 rows : {df_f5642b11.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 5a — Load F4201 (Sales Order Header)
# date_original_promisde = F4201.SHOPDJ (original promised date at order header level)
# ─────────────────────────────────────────────────────────────────────────────
df_f4201 = (
    load_silver(F4201_TBL)
    .select(
        F.col("company_key_order_no").alias("h_company"),
        F.col("document_order_invoice_e").alias("h_order_number"),
        F.col("order_type").alias("h_order_type"),
        F.col("order_suffix").alias("h_order_suffix"),
        F.col("hold_orders_code").alias("hold_code"),
        F.col("date_transaction_julian").alias("order_placed_date_julian"),
        F.col("date_original_promisde").alias("date_original_promisde"),
    )
    .distinct()
)

print(f"F4201 header rows : {df_f4201.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 5b — Load F4941 OCE routing: port addresses + is_missing_freight flag
#
# Two datasets built from F4941 OCE rows:
#
# 1. df_f4941_ports — ALL OCE rows per shipment (any completeness).
#    Provides routing_load_port_address (F4941.RSORGN) and
#    routing_dest_port_address (F4941.RSANCC).
#    For missing freight rows these will be NULL → blank in PBI,
#    matching Hubble's INNER JOIN behaviour on F0101.
#
# 2. df_f4941_oce — only rows where BOTH port addresses are non-null on the
#    SAME row. Used exclusively to derive is_missing_freight.
#    Matches Hubble: (F4941.RSORGN * F4941.RSANCC) IS NULL
# ─────────────────────────────────────────────────────────────────────────────
_df_f4941_silver = (
    load_silver(F4941_TBL)
    .filter(F.col("mode_of_transport") == "OCE")
)

# Port addresses for all OCE shipments (NULL for missing freight rows)
df_f4941_ports = (
    _df_f4941_silver
    .select(
        F.col("shipment_number").alias("port_shipment"),
        F.col("origin_address_number").alias("routing_load_port_address"),
        F.col("address_number_deconsolida").alias("routing_dest_port_address"),
    )
    .distinct()
)

# Completeness flag: shipments with a fully-populated OCE leg
df_f4941_oce = (
    _df_f4941_silver
    .filter(
        F.col("origin_address_number").isNotNull() &
        F.col("address_number_deconsolida").isNotNull()
    )
    .select(F.col("shipment_number").alias("oce_shipment"))
    .distinct()
    .withColumn("has_complete_oce", F.lit(True))
)

print(f"OCE shipments (all)      : {df_f4941_ports.count():,}")
print(f"OCE shipments (complete) : {df_f4941_oce.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 7 — Join all sources into flat ESO7 fact
#
# Join order:
#   F4211  LEFT JOIN  F5642B01      ON (shipment×order×type×company)
#          LEFT JOIN  F5642B11      ON (company×order×type×line×shipment)
#          LEFT JOIN  F4201         ON (company×order×type×suffix)
#          LEFT JOIN  df_f4941_ports ON shipment_number
#                                   → routing_load_port_address (F4941.RSORGN)
#                                   → routing_dest_port_address (F4941.RSANCC)
#                                   → NULL for missing freight rows (matches Hubble)
#          LEFT JOIN  df_f4941_oce  ON shipment_number
#                                   → derives is_missing_freight boolean
#
# UOM conversion is NOT joined here — handled in PBI via DAX SUMX + RELATED
# through dim_uom_conversion_item (Tier A) and dim_uom_conversion (Tier B).
# ─────────────────────────────────────────────────────────────────────────────
df_fact = (
    df_f4211
    # ── booking header (F5642B01) ───────────────────────────────────────
    .join(
        df_f5642b01,
        (df_f4211.shipment_number == df_f5642b01.b01_shipment_number) &
        (df_f4211.order_number    == df_f5642b01.b01_order_number)    &
        (df_f4211.order_type      == df_f5642b01.b01_order_type)      &
        (df_f4211.company         == df_f5642b01.b01_company),
        how="left"
    )
    .drop("b01_shipment_number", "b01_order_number", "b01_order_type", "b01_company")
    # ── line detail (F5642B11) ─────────────────────────────────────────
    .join(
        df_f5642b11,
        (df_f4211.company         == df_f5642b11.b11_company)         &
        (df_f4211.order_number    == df_f5642b11.b11_order_number)    &
        (df_f4211.order_type      == df_f5642b11.b11_order_type)      &
        (df_f4211.line_number     == df_f5642b11.b11_line_number)     &
        (df_f4211.shipment_number == df_f5642b11.b11_shipment_number),
        how="left"
    )
    .drop("b11_company", "b11_order_number", "b11_order_type", "b11_line_number", "b11_shipment_number")
    # ── order header (F4201) ──────────────────────────────────────────
    .join(
        df_f4201,
        (df_f4211.company       == df_f4201.h_company)       &
        (df_f4211.order_number  == df_f4201.h_order_number)  &
        (df_f4211.order_type    == df_f4201.h_order_type)    &
        (df_f4211.order_suffix  == df_f4201.h_order_suffix),
        how="left"
    )
    .drop("h_company", "h_order_number", "h_order_type", "h_order_suffix")
    .drop("order_suffix")
    # ── F4941 routing port addresses ───────────────────────────────────
    .join(df_f4941_ports, df_f4211.shipment_number == df_f4941_ports.port_shipment, how="left")
    .drop("port_shipment")
    # ── missing freight flag (F4941) ───────────────────────────────────
    .join(df_f4941_oce, df_f4211.shipment_number == df_f4941_oce.oce_shipment, how="left")
    .drop("oce_shipment")
    .withColumn("is_missing_freight", F.col("has_complete_oce").isNull())
    .drop("has_complete_oce")
)

print(f"Flat fact rows (pre-write) : {df_fact.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 8 — Add surrogate keys
#
# order_key          → dim_order.order_key
# order_activity_key → dim_order_activity.order_activity_key
# item_uom_key       → dim_uom_conversion_item.item_uom_key
#                      Required for Total Tons DAX RELATED lookup (Tier A).
#                      Must match the key built in nb_silver_to_gold_dim_uom_conversion_item:
#                      concat_ws('|', identifier_short_item, from_uom)
# ─────────────────────────────────────────────────────────────────────────────
df_fact = (
    df_fact
    .withColumn(
        "order_key",
        F.concat_ws("|", "order_number", "order_type", "company")
    )
    .withColumn(
        "order_activity_key",
        F.concat_ws("|", "order_type", "line_type", "status_code_last")
    )
    .withColumn(
        "item_uom_key",
        F.concat_ws("|", "identifier_short_item", "uom_as_input")
    )
)

print("Surrogate keys added: order_key, order_activity_key, item_uom_key")
print(f"Total columns : {len(df_fact.columns)}")
print("Columns:", df_fact.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 8a — Baked UoM conversion columns
#
# SDUOM->TN volume factor/tons/flag + SDUOM4->TN price fields, resolved through the
# item F41002 then standard F41003 cascade (fwd + inverse, default 1 with a missing flag).
# The existing Total Tons DAX (item_uom_key / uom_as_input relationships) is untouched — these
# are additive columns. Deduped cascade → LEFT joins never fan the line grain out.
# ─────────────────────────────────────────────────────────────────────────────
_keep_cols = df_fact.columns
_vi, _vs = build_uom_cascades()
df_fact = (
    df_fact
    .join(_vi.alias("civ"), (F.col("civ.itm") == F.col("identifier_short_item"))
                            & (F.col("civ.from_uom") == F.trim(F.col("uom_as_input"))), "left")
    .join(_vs.alias("csv"), (F.col("csv.from_uom") == F.trim(F.col("uom_as_input"))), "left")
    .join(_vi.alias("cip"), (F.col("cip.itm") == F.col("identifier_short_item"))
                            & (F.col("cip.from_uom") == F.trim(F.col("uom_pricing"))), "left")
    .join(_vs.alias("csp"), (F.col("csp.from_uom") == F.trim(F.col("uom_pricing"))), "left")
)
_vol_raw = F.coalesce(F.when(F.trim(F.col("uom_as_input")) == "TN", F.lit(1.0)),
                      F.col("civ.conv_factor"), F.col("csv.conv_std"))
_prc_raw = F.coalesce(F.when(F.trim(F.col("uom_pricing")) == "TN", F.lit(1.0)),
                      F.col("cip.conv_factor"), F.col("csp.conv_std"))
_prc_fac = F.coalesce(_prc_raw, F.lit(1.0))
df_fact = (
    df_fact
    .withColumn("conversion_to_tons_rate", F.coalesce(_vol_raw, F.lit(1.0)))
    .withColumn("quantity_shipped_tons", F.col("units_transaction_qty").cast("double") * F.coalesce(_vol_raw, F.lit(1.0)))
    .withColumn("missing_conversion_flag", F.when(_vol_raw.isNull(), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn("price_conversion_factor", _prc_fac)
    .withColumn("converted_price_per_ton",
        F.when((F.col("status_code_last").cast("int") != F.lit(980))
               & (F.col("status_code_next").cast("int") == F.lit(999)),
               F.col("unit_price").cast("double") / F.when(_prc_fac != 0, _prc_fac)))
    .withColumn("price_missing_conversion_flag", F.when(_prc_raw.isNull(), F.lit("Y")).otherwise(F.lit("N")))
    # keep only the fact's own columns + the 6 derived (drop the cascade join helpers)
    .select(*_keep_cols, "conversion_to_tons_rate", "quantity_shipped_tons", "missing_conversion_flag",
            "price_conversion_factor", "converted_price_per_ton", "price_missing_conversion_flag")
)

print(f"UoM conversion columns baked. Total columns : {len(df_fact.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 9 — Validate
# ─────────────────────────────────────────────────────────────────────────────
key_cols = ["company", "order_number", "order_type", "line_number"]

dup_check = df_fact.groupBy(*key_cols).count().filter(F.col("count") > 1)
dup_count = dup_check.count()
assert dup_count == 0, (
    f"Primary key violated — {dup_count} duplicate "
    f"(company, order_number, order_type, line_number) combinations found."
)
print("\u2713 Primary key uniqueness verified — joins did not fan out rows.")

with_booking = df_fact.filter(F.col("booking_no").isNotNull()).count()
total        = df_fact.count()
print(f"Rows with booking detail : {with_booking:,} / {total:,} ({100*with_booking//total}%)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL 10 — Write Gold fact table
# ─────────────────────────────────────────────────────────────────────────────
df_fact.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_TABLE)

spark.sql(f"OPTIMIZE {GOLD_TABLE}")
print(f"\u2713 {GOLD_TABLE}  \u2192  {spark.read.table(GOLD_TABLE).count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
