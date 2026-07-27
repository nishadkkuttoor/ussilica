#!/usr/bin/env python
# coding: utf-8

# ## nb_fact_sales_order_line
#
# New notebook — near-real-time current-state sales-order-line fact

# In[1]:


# =============================================================================
# WHAT THIS NOTEBOOK BUILDS
# =============================================================================
# Output table : lh_jde_gold.otc.fact_sales_order_line
# Grain        : ONE CURRENT row per order line
#                (company_key_order + order_type + order_number + line_number)
# Refresh      : scheduled every 5 min — incremental MERGE (current-state, NOT a
#                daily snapshot). No snapshot_date accumulation.
# Role         : sales-order-line fact for Extended Sales Order 1 (Billable v
#                Payable Freight). Conforms to dim_address_book (role views),
#                dim_plant, dim_item_cost_cascade; joins fact_freight_audit via
#                the degenerate shipment_number.
#
# This REPLACES the previous project's daily-snapshot `customer_order_line`
# (which was built for historical accumulation). Business transformation logic
# is carried over verbatim from customer_order_line_v3; only the load/write
# pattern changes to current-state MERGE.
# =============================================================================
# SOURCES & WHY  (Silver — decoding already done Bronze->Silver)
# =============================================================================
#   F4211   driver — one row per (order, line, shipment)
#   F4201   SO header — hold code, delivery instr, price-eff (INNER: line needs header)
#   F0101   Ship-To ABAT1 DQ gate (INNER, no columns brought — skinny fact)
#   F4101   item description + weight UoM (LEFT)
#   F5642B01/B11  custom port/booking/vessel/seal/production (LEFT; B11 adds shipment_number)
#   F4981   freight-audit city/state/postal (LEFT on shipment)  [freight $ live in fact_freight_audit]
#   F41002/F41003  UoM->TN conversion cascades (item-specific then standard, bidirectional)
#   F4074   price adjustment — ALAST whitelist, then DISTINCT collapse (fan-out guard)
#   dim_item_cost_cascade (Gold) — 8-tier cost/ton in ONE LEFT join
# Names/city/state resolve via dim_address_book SQL views in Power BI (skinny fact).
# =============================================================================
# HARD FILTERS (F4211 driver, per v7 spec)
# =============================================================================
#   company_key_order_no IN ('00640','00645')  · line_type='S'
#   status_code_last <> '980'  · status_code_next < '561'
#   order_type IN ('SE','SZ','S1','ST','SG')
#   + INNER Ship-To gate: F0101.address_type_01 BETWEEN 'A'..'P' OR 'R'..'ZZZ'
# =============================================================================
# REFRESH / WRITE STRATEGY (current-state MERGE)
# =============================================================================
# Full current-state recompute (the open-order universe is small after filters),
# then MERGE on order_line_key:
#   • WHEN MATCHED AND record_hash changed -> UPDATE (skip no-op writes)
#   • WHEN NOT MATCHED                     -> INSERT
#   • WHEN NOT MATCHED BY SOURCE           -> soft-delete (is_deleted=true)
#       i.e. lines that ship/close out of the open-order filter are flagged.
# record_hash keeps re-runs cheap; Power BI / role views filter is_deleted=false.
# Liquid clustering (branch_plant, shipment_number) + deletion vectors; scheduled
# OPTIMIZE/V-Order keep frequent merges + Direct Lake fast.
# NOTE: if Silver exposes a true CDC commit timestamp, narrow the recompute to
# changed orders/shipments since the last watermark (further compute saving).
# =============================================================================

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ── Constants ─────────────────────────────────────────────────────────────────
F4211_TBL    = "jde.f4211_sales_order_detail_file"
F4201_TBL    = "jde.f4201_sales_order_header_file"
F0101_TBL    = "jde.f0101_address_book_master"
F4101_TBL    = "jde.f4101_item_master"
F5642B01_TBL = "jde.f5642b01_custom_sales_order_entry_screen_header"
F5642B11_TBL = "jde.f5642b11_custom_sales_order_entry_screen_detail"
F41002_TBL   = "jde.f41002_item_units_of_measure_conversion_factors"
F41003_TBL   = "jde.f41003_unit_of_measure_standard_conversion"
F4074_TBL    = "jde.f4074_price_adjustment_ledger_file"
F4981_TBL    = "jde.f4981_freight_audit_history"
DIM_COST_TBL = "lh_jde_gold.rpt.dim_item_cost_cascade"   # reused dim — stays in rpt
TARGET_TABLE = "lh_jde_gold.otc.fact_sales_order_line"   # new ESO1 fact — otc schema

# ── Refresh stamp (captured ONCE) ─────────────────────────────────────────────
run_dt = datetime.now()
print(f"Run timestamp : {run_dt}")
print(f"Target table  : {TARGET_TABLE}")


# In[2]:


# ── Helper: load silver, filter is_delete=0 if present, drop soft-delete cols ─
exclude_cols = ["is_delete", "deleted_date_time"]

def load_silver_table(table_name):
    df = spark.read.table(table_name)
    if "is_delete" in df.columns:                      # F4074/F4101/F4981 are soft-delete enabled
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in exclude_cols])

df_f4211  = load_silver_table(F4211_TBL)
df_f4201  = load_silver_table(F4201_TBL)
df_f0101  = load_silver_table(F0101_TBL)
df_f4101  = load_silver_table(F4101_TBL)
df_b01    = load_silver_table(F5642B01_TBL)
df_b11    = load_silver_table(F5642B11_TBL)
df_f41002 = load_silver_table(F41002_TBL)
df_f41003 = load_silver_table(F41003_TBL)
df_f4074  = load_silver_table(F4074_TBL)
df_f4981  = load_silver_table(F4981_TBL)
df_dim_cost = spark.read.table(DIM_COST_TBL)

print(f"F4211: {df_f4211.count():,} | F4201: {df_f4201.count():,} | F4101: {df_f4101.count():,}")


# In[3]:


# ─────────────────────────────────────────────────────────────────────────────
# UoM -> TN conversion cascades (bidirectional; item-specific then standard)
#   Tier A F41002 (item): fwd UMUM=X,UMRUM=TN -> factor ; rev UMUM=TN,UMRUM=X -> 1/factor
#   Tier B F41003 (std) : same shape, no item
#   Tier C literal 1.0 if from_uom already = 'TN'
# JDE stores conversions in either direction -> we cover both.
# ─────────────────────────────────────────────────────────────────────────────
df_conv_item_fwd = (
    df_f41002.filter((F.trim(F.col("related_uom")) == "TN") & (F.col("conversion_factor") != 0))
    .select(F.col("identifier_short_item").alias("itm"),
            F.trim(F.col("uom")).alias("from_uom"),
            F.col("conversion_factor").cast("double").alias("conv_factor")))
df_conv_item_rev = (
    df_f41002.filter((F.trim(F.col("uom")) == "TN") & (F.col("conversion_factor") != 0))
    .select(F.col("identifier_short_item").alias("itm"),
            F.trim(F.col("related_uom")).alias("from_uom"),
            (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_factor")))
df_conv_item = df_conv_item_fwd.unionByName(df_conv_item_rev)

df_conv_std_fwd = (
    df_f41003.filter((F.trim(F.col("related_uom")) == "TN") & (F.col("conversion_factor") != 0))
    .select(F.trim(F.col("uom")).alias("from_uom"),
            F.col("conversion_factor").cast("double").alias("conv_factor")))
df_conv_std_rev = (
    df_f41003.filter((F.trim(F.col("uom")) == "TN") & (F.col("conversion_factor") != 0))
    .select(F.trim(F.col("related_uom")).alias("from_uom"),
            (F.lit(1.0) / F.col("conversion_factor").cast("double")).alias("conv_factor")))
df_conv_std = df_conv_std_fwd.unionByName(df_conv_std_rev)

print(f"conv_item: {df_conv_item.count():,} | conv_std: {df_conv_std.count():,}")


# In[4]:


# ─── F4074 prep — ALAST whitelist BEFORE join (then LEFT join + DISTINCT) ──────
ALAST_WHITELIST = [
    "A03", "FRTHIDE", "FRTTAXN", "FRTTAXY", "PRODEP", "A07B", "TEST", "EPDISC",
    "EPDELFRT", "NAPAREB", "FRTNBP", "ITMOFSET", "OCEANFRT", "OFS", "ES",
]
df_f4074_filtered = df_f4074.filter(F.trim(F.col("price_adjustment_type")).isin(*ALAST_WHITELIST))
print(f"F4074 after ALAST whitelist : {df_f4074_filtered.count():,}")


# In[5]:


# ─── DRIVER FILTER — F4211 open orders (5 hard filters) ───────────────────────
df_sd = (
    df_f4211
    .filter(F.col("company_key_order_no").isin("00640", "00645"))
    .filter(F.col("line_type") == "S")
    .filter(F.col("status_code_last") != "980")
    .filter(F.col("status_code_next") < "561")
    .filter(F.col("order_type").isin("SE", "SZ", "S1", "ST", "SG"))
)
print(f"F4211 open orders after filters : {df_sd.count():,}")

# ─── Ship-To DQ gate (INNER, no columns brought — skinny fact) ────────────────
df_f0101_shipto_gate = (
    df_f0101
    .filter((F.trim(F.col("address_type_01")).between("A", "P")) |
            (F.trim(F.col("address_type_01")).between("R", "ZZZ")))
    .select(F.col("address_number").alias("dq_an8"))
    .distinct()
)


# In[6]:


# ─────────────────────────────────────────────────────────────────────────────
# MAIN JOIN — 2 INNER (header + ship-to gate) + 9 LEFT
# ─────────────────────────────────────────────────────────────────────────────
df_join = (
    df_sd.alias("sd")
    # 1. F4201 header — INNER (every line must have a header)
    .join(df_f4201.alias("sh"),
          (F.col("sd.company_key_order_no")     == F.col("sh.company_key_order_no")) &
          (F.col("sd.document_order_invoice_e") == F.col("sh.document_order_invoice_e")) &
          (F.col("sd.order_type")               == F.col("sh.order_type")), "inner")
    # 2. Ship-To DQ gate — INNER
    .join(df_f0101_shipto_gate.alias("ship_gate"),
          F.col("sd.address_number_ship_to") == F.col("ship_gate.dq_an8"), "inner")
    # 3. F5642B11 custom detail — LEFT (adds shipment_number condition: fan-out guard)
    .join(df_b11.alias("b11"),
          (F.col("sd.company_key_order_no")     == F.col("b11.company_key_order_no")) &
          (F.col("sd.document_order_invoice_e") == F.col("b11.document_order_invoice_e")) &
          (F.col("sd.order_type")               == F.col("b11.order_type")) &
          (F.col("sd.line_number")              == F.col("b11.line_number")) &
          (F.col("sd.shipment_number")          == F.col("b11.shipment_number")), "left")
    # 4. F5642B01 custom header — LEFT (on shipment)
    .join(df_b01.alias("b01"),
          (F.col("sd.company_key_order_no")     == F.col("b01.company_key_order_no")) &
          (F.col("sd.document_order_invoice_e") == F.col("b01.document_order_invoice_e")) &
          (F.col("sd.order_type")               == F.col("b01.order_type")) &
          (F.col("sd.shipment_number")          == F.col("b01.shipment_number")), "left")
    # 5. F4981 freight audit — LEFT (on shipment; city/state/postal only)
    .join(df_f4981.alias("fh"),
          F.col("sd.shipment_number") == F.col("fh.shipment_number"), "left")
    # 6. F4101 item master — LEFT
    .join(df_f4101.alias("im"),
          F.col("sd.identifier_short_item") == F.col("im.identifier_short_item"), "left")
    # 7. UoM cascade — quantity (SDUOM->TN)
    .join(df_conv_item.alias("ci"),
          (F.col("ci.itm") == F.col("sd.identifier_short_item")) &
          (F.col("ci.from_uom") == F.trim(F.col("sd.uom_as_input"))), "left")
    .join(df_conv_std.alias("cs"),
          F.col("cs.from_uom") == F.trim(F.col("sd.uom_as_input")), "left")
    # 8. UoM cascade — pricing (SDUOM4->TN)
    .join(df_conv_item.alias("ci_p"),
          (F.col("ci_p.itm") == F.col("sd.identifier_short_item")) &
          (F.col("ci_p.from_uom") == F.trim(F.col("sd.uom_pricing"))), "left")
    .join(df_conv_std.alias("cs_p"),
          F.col("cs_p.from_uom") == F.trim(F.col("sd.uom_pricing")), "left")
    # 9. F4074 price adjustment — LEFT (whitelisted; DISTINCT next cell)
    .join(df_f4074_filtered.alias("al"),
          (F.col("al.company_key_order_no")     == F.col("sd.company_key_order_no")) &
          (F.col("al.document_order_invoice_e") == F.col("sd.document_order_invoice_e")) &
          (F.col("al.order_type")               == F.col("sd.order_type")) &
          (F.col("al.line_number")              == F.col("sd.line_number")), "left")
)


# In[7]:


# ─── Select needed cols + DISTINCT (collapse F4074 multi-adjustment fan-out) ──
df_with_derived = df_join.select(
    # F4211 identity & status
    F.col("sd.company").alias("sdco"),
    F.col("sd.company_key_order_no").alias("sdkcoo"),
    F.col("sd.cost_center").alias("sdmcu"),
    F.col("sd.address_number").alias("sdan8"),
    F.col("sd.address_number_ship_to").alias("sdshan"),
    F.col("sd.document_order_invoice_e").alias("sddoco"),
    F.col("sd.line_number").alias("sdlnid"),
    F.col("sd.order_type").alias("sddcto"),
    F.col("sd.line_type").alias("sdlnty"),
    F.col("sd.status_code_last").alias("sdlttr"),
    F.col("sd.status_code_next").alias("sdnxtr"),
    F.col("sd.date_updated").alias("sd_date_updated"),          # NRT: source-change proxy
    # Dates (silver already DateType)
    F.col("sd.date_transaction_julian").alias("sdtrdj"),
    F.col("sd.date_requested_julian").alias("sddrqj"),
    F.col("sd.scheduled_pick_date").alias("sdpddj"),
    F.col("sd.date_promised_ship_julian").alias("sdppdj"),
    F.col("sd.actual_ship_date").alias("sdaddj"),
    # Item + UoM + price
    F.col("sd.identifier_second_item").alias("sdlitm"),
    F.col("sd.identifier_short_item").alias("sditm"),
    F.col("sd.uom_as_input").alias("sduom"),
    F.col("sd.uom_primary").alias("sduom1"),
    F.col("sd.amt_price_per_unit_02").alias("sduprc"),
    F.col("sd.uom_pricing").alias("sduom4"),
    F.col("sd.date_price_effective_date").alias("sdpefj"),
    F.col("sh.date_price_effective_date").alias("sh_pefj"),
    # Freight, carrier, transport
    F.col("sd.freight_handling_code").alias("sdfrth"),
    F.col("sd.carrier").alias("sdcars"),
    F.col("sd.container_id").alias("sdcnid"),
    F.col("sd.mode_of_transport").alias("sdmot"),
    # Sales reporting codes
    F.col("sd.sales_reporting_code_01").alias("sdsrp1"),
    F.col("sd.sales_reporting_code_02").alias("sdsrp2"),
    F.col("sd.sales_reporting_code_03").alias("sdsrp3"),
    F.col("sd.sales_reporting_code_04").alias("sdsrp4"),
    # Refs / originator
    F.col("sd.reference_01").alias("sdvr01"),
    F.col("sd.transaction_originator").alias("sdtorg"),
    F.col("sd.address_number_parent").alias("sdpa8"),
    F.col("sd.user_reserved_number").alias("sdurab"),
    F.col("sd.user_reserved_reference").alias("sdurrf"),
    F.col("sd.gl_class").alias("sdglc"),
    # Original-order refs
    F.col("sd.original_document_type").alias("sdodct"),
    F.col("sd.original_po_so_number").alias("sdoorn"),
    F.col("sd.original_document_no").alias("sdodoc"),
    F.col("sd.cancel_date").alias("sdcndj"),
    # Invoice
    F.col("sd.doc_voucher_invoice_e").alias("sddoc"),
    F.col("sd.date_invoice_julian").alias("sdivd"),
    # Shipment + ASN
    F.col("sd.shipment_number").alias("sdshpn"),
    F.col("sd.price_adjustment_schedule_n").alias("sdasn"),
    # Quantities (silver already actual units) + extended price
    F.col("sd.units_transaction_qty").alias("sduorg"),
    F.col("sd.units_quantity_shipped").alias("sdsoqs"),
    F.col("sd.units_primary_qty_order").alias("sdpqor"),
    F.col("sd.quantity_shipped_to_date").alias("sdqtyt"),
    F.col("sd.units_open_quantity").alias("sduopn"),
    F.col("sd.units_quan_backor_held").alias("sdsobk"),
    F.col("sd.amount_extended_price").alias("sdaexp"),
    # F4201 header
    F.col("sh.hold_orders_code").alias("shhold"),
    F.col("sh.delivery_instruct_line_01").alias("shdel1"),
    F.col("sh.delivery_instruct_line_02").alias("shdel2"),
    # F5642B11 + F5642B01 (custom)
    F.col("b11.seal_no").alias("ak55seln"),
    F.col("b11.production_code").alias("ak55pdcd"),
    F.col("b11.production_ship_notes").alias("ak55pdshnt"),
    F.col("b01.booking_no").alias("ba55bkno"),
    F.col("b01.routing_notes").alias("ba55rout"),
    F.col("b01.date_earliest_pickup").alias("badepu"),
    F.col("b01.date_latest_delivery").alias("badldl"),
    F.col("b01.destination_port").alias("ba55dstpt"),
    F.col("b01.no_of_container").alias("ba55ncon"),
    F.col("b01.ocean_del_terms").alias("ba55ocdlt"),
    F.col("b01.vessel_name").alias("ba55vlno"),
    F.col("b01.loading_port").alias("ba55lodp"),
    F.col("b01.date_requested_ship").alias("barqsj"),
    F.col("b01.ocean_carrier").alias("ba55occr"),
    F.col("b01.date_01").alias("ba55dt01"),
    # F4981 freight audit (city/state/postal only)
    F.col("fh.city").alias("fhcty1"),
    F.col("fh.state").alias("fhadds"),
    F.col("fh.zip_code_postal").alias("fhaddz"),
    # F4101
    F.col("im.uom_weight").alias("imuwum"),
    F.col("im.description_line_01").alias("imdsc1"),
    # F4074
    F.col("al.price_adjustment_type").alias("alast"),
    F.col("al.amt_price_per_unit_02").alias("aluprc"),
    # UoM cascade pickers (COALESCE: TN literal -> F41002 -> F41003 -> 1.0)
    F.coalesce(F.when(F.trim(F.col("sd.uom_as_input")) == "TN", F.lit(1.0)),
               F.col("ci.conv_factor"), F.col("cs.conv_factor"), F.lit(1.0)).alias("conv_to_tons_rate"),
    F.when(F.coalesce(F.when(F.trim(F.col("sd.uom_as_input")) == "TN", F.lit(1.0)),
                      F.col("ci.conv_factor"), F.col("cs.conv_factor")).isNull(),
           F.lit("Y")).otherwise(F.lit("N")).alias("missing_conv_flag"),
    F.coalesce(F.when(F.trim(F.col("sd.uom_pricing")) == "TN", F.lit(1.0)),
               F.col("ci_p.conv_factor"), F.col("cs_p.conv_factor"), F.lit(1.0)).alias("conv_factor_pricing_to_TN"),
    F.when(F.coalesce(F.when(F.trim(F.col("sd.uom_pricing")) == "TN", F.lit(1.0)),
                      F.col("ci_p.conv_factor"), F.col("cs_p.conv_factor")).isNull(),
           F.lit("Y")).otherwise(F.lit("N")).alias("missing_price_conv_flag"),
)
df_base_distinct = df_with_derived.distinct()
print(f"After DISTINCT (F4074 fan-out collapsed) : {df_base_distinct.count():,}")


# In[8]:


# ─── Derived: effective_date/year/month + price_per_unit_TN ────────────────────
df_with_eff = (
    df_base_distinct
    .withColumn("date_actual_ship",    F.col("sdaddj"))
    .withColumn("date_scheduled_pick", F.col("sdpddj"))
    .withColumn("effective_date", F.coalesce(F.col("date_actual_ship"), F.col("date_scheduled_pick")))
    .withColumn("effective_year",  F.year("effective_date"))
    .withColumn("effective_month", F.month("effective_date"))
    .withColumn("price_per_unit_TN",
        F.col("sduprc") / F.when(F.col("conv_factor_pricing_to_TN") == 0, None)
                            .otherwise(F.col("conv_factor_pricing_to_TN")))
    .withColumn("sdlttr_int", F.col("sdlttr").cast("int"))
    .withColumn("sdnxtr_int", F.col("sdnxtr").cast("int"))
)


# In[9]:


# ─── 5-tier status hierarchy (Ship Confirmed / Loaded / Assigned / Invoiced / Open) ──
def is_real(col):
    return col.isNotNull() & ~F.trim(col).isin(".", "NA", "N/A")

df_with_status = (
    df_with_eff
    .withColumn("status",
        F.when((F.col("sdnxtr_int") > 560) & (F.col("sdlttr_int") < 999) & (F.col("sdlttr_int") != 980), "Ship Confirmed")
         .when(is_real(F.col("ak55seln")) & (F.col("sdlttr_int") != 999), "Loaded")
         .when(is_real(F.col("sdcnid")) & ~is_real(F.col("ak55seln")) & (F.col("sdlttr_int") != 999), "Assigned")
         .when(F.col("sdlttr_int") == 999, "Invoiced")
         .otherwise("Open"))
    .withColumn("status_sort",
        F.when((F.col("sdnxtr_int") > 560) & (F.col("sdlttr_int") < 999) & (F.col("sdlttr_int") != 980), 2)
         .when(is_real(F.col("ak55seln")) & (F.col("sdlttr_int") != 999), 3)
         .when(is_real(F.col("sdcnid")) & ~is_real(F.col("ak55seln")) & (F.col("sdlttr_int") != 999), 4)
         .when(F.col("sdlttr_int") < 999, 1)
         .otherwise(5))
)


# In[10]:


# ─── Tons-converted metrics + lane price (silver already decimal-resolved) ────
# Qty fields are actual units -> multiply by conv_to_tons_rate (no /1000 here).
# DLV freight discount factor 0.253910373 applied only when freight code = 'DLV'.
df_with_metrics = (
    df_with_status
    .withColumn("units_transaction_qty_unconv",   F.col("sduorg"))
    .withColumn("units_transaction_qty_tons",     F.col("sduorg") * F.col("conv_to_tons_rate"))
    .withColumn("qty_shipped_tons",               F.col("sdsoqs") * F.col("conv_to_tons_rate"))
    .withColumn("units_shipped_to_date_tons",     F.col("sdqtyt") * F.col("conv_to_tons_rate"))
    .withColumn("units_open_tons",                F.col("sduopn") * F.col("conv_to_tons_rate"))
    .withColumn("units_qty_bo_held_tons",         F.col("sdsobk") * F.col("conv_to_tons_rate"))
    .withColumn("units_pri_qty_ordered_tons",     F.col("sdpqor") * F.col("conv_to_tons_rate"))
    .withColumn("volume_unconverted",             F.col("sduorg"))
    .withColumn("revenue_dollars",                F.col("sdaexp"))
    .withColumn("calculated_latest_lane_price",
        F.when(F.trim(F.col("sdfrth")) == "DLV", F.col("sdaexp") * (F.lit(1.0) - F.lit(0.253910373)))
         .otherwise(F.col("sdaexp")))
    .withColumn("business_unit", F.trim(F.col("sdmcu")))
)


# In[11]:


# ─── Join dim_item_cost_cascade (8-tier cost in ONE LEFT join) ────────────────
df_dim_cost_join = df_dim_cost.select(
    F.col("plant_code").alias("dim_plant_code"),
    F.col("item_short").alias("dim_item_short"),
    F.col("year").alias("dim_year"),
    F.col("cost_per_ton"), F.col("cost_method"),
    F.col("cost_source_tier"), F.col("cost_confidence"),
)
df_with_cost = (
    df_with_metrics
    .join(df_dim_cost_join,
          (F.col("business_unit")  == F.col("dim_plant_code")) &
          (F.col("sditm")          == F.col("dim_item_short")) &
          (F.col("effective_year") == F.col("dim_year")), "left")
    .drop("dim_plant_code", "dim_item_short", "dim_year")
)


# In[12]:


# ─── FINAL SELECT — business columns (skinny fact; names via dim_address_book) ─
df_business = df_with_cost.select(
    # Dimension columns
    F.col("sdlnty").alias("line_type"),
    F.col("sddoco").alias("order_number"),
    F.col("sdshpn").alias("shipment_number"),
    F.col("sddcto").alias("order_type"),
    F.col("shhold").alias("hold_orders_code"),
    F.col("sdmcu").alias("branch_plant"),
    F.col("sdan8").alias("bill_to"),
    F.col("sdshan").alias("ship_to"),
    F.col("sdlttr").alias("status_code_last"),
    F.col("sdnxtr").alias("status_code_next"),
    # Dates
    F.col("sdtrdj").alias("order_date"),
    F.col("sddrqj").alias("requested_date"),
    F.col("sdpddj").alias("scheduled_pick_date"),
    F.col("sdppdj").alias("promised_ship_date"),
    F.col("sdaddj").alias("actual_ship_date"),
    F.col("date_actual_ship"), F.col("date_scheduled_pick"),
    F.col("effective_date"),
    F.col("effective_year").alias("fiscal_year"),
    F.col("effective_month").alias("fiscal_month"),
    # Item
    F.col("sdlitm").alias("second_item_number"),
    F.col("sditm").alias("item_number_short"),
    F.col("imdsc1").alias("item_name"),
    # UoM + qty
    F.col("sduom").alias("uom"),
    F.col("sduom1").alias("uom_primary"),
    F.col("conv_to_tons_rate").alias("conversion_to_tons_rate"),
    F.col("missing_conv_flag").alias("missing_conversion_flag"),
    F.col("volume_unconverted"),
    F.col("units_transaction_qty_unconv"),
    F.col("units_transaction_qty_tons"),
    F.col("qty_shipped_tons").alias("quantity_shipped_tons"),
    F.col("units_shipped_to_date_tons"),
    F.col("units_open_tons"),
    F.col("units_qty_bo_held_tons").alias("units_qty_backordered_held_tons"),
    F.col("units_pri_qty_ordered_tons").alias("units_primary_qty_ordered_tons"),
    # Pricing
    F.col("sduprc").alias("price_per_unit"),
    F.col("sduom4").alias("uom_pricing"),
    F.col("sdpefj").alias("price_effective_detail"),
    F.col("sh_pefj").alias("price_effective_header"),
    F.col("conv_factor_pricing_to_TN"),
    F.col("missing_price_conv_flag"),
    F.col("price_per_unit_TN"),
    F.col("revenue_dollars"),
    F.col("calculated_latest_lane_price"),
    # Status
    F.col("status"), F.col("status_sort"),
    # Custom (F5642B01/B11)
    F.col("ak55seln").alias("seal_number"),
    F.col("ak55pdcd").alias("production_code"),
    F.col("ak55pdshnt").alias("production_ship_notes"),
    F.col("ba55bkno").alias("booking_number"),
    F.col("ba55rout").alias("routing_notes"),
    F.col("badepu").alias("sail_date"),
    F.col("badldl").alias("date_latest_delivery"),
    F.col("ba55dstpt").alias("port_of_destination"),
    F.col("ba55ncon").alias("no_of_container"),
    F.col("ba55ocdlt").alias("ocean_del_terms"),
    F.col("ba55vlno").alias("vessel_name"),
    F.col("ba55lodp").alias("loading_port"),
    F.col("ba55occr").alias("ocean_carrier"),
    F.col("barqsj").alias("date_requested_ship"),
    F.col("ba55dt01").alias("appointment_date"),
    # Freight, carrier, transport
    F.col("sdfrth").alias("freight_handling_code"),
    F.col("sdcars").alias("carrier_number"),
    F.col("sdmot").alias("mode_of_transport"),
    F.col("sdcnid").alias("vehicle_number"),
    # F4981 exposed (city/state/postal)
    F.col("fhcty1").alias("shipment_city"),
    F.col("fhadds").alias("shipment_state"),
    F.col("fhaddz").alias("postal_code"),
    # Customer refs
    F.col("sdvr01").alias("customer_po_number"),
    F.col("sdpa8").alias("parent_number"),
    F.col("sdurab").alias("bol_number"),
    # Sales reporting codes
    F.col("sdsrp1").alias("mxp_prod_group"),
    F.col("sdsrp2").alias("major_prod_code"),
    F.col("sdsrp3").alias("pack_code"),
    F.col("sdsrp4").alias("minor_prod_code"),
    # Originator, original-order refs
    F.col("sdtorg").alias("transaction_originator"),
    F.col("sdodct").alias("document_type_original"),
    F.col("sdoorn").alias("original_order_number"),
    F.col("sdodoc").alias("document_original"),
    F.col("sdcndj").alias("date_cancel"),
    F.col("sdurrf").alias("package_size"),
    # Invoice
    F.col("sddoc").alias("invoice_number"),
    F.col("sdivd").alias("invoice_date"),
    # F4074 adjustment
    F.col("alast").alias("adjustment_name"),
    F.col("aluprc").alias("adjustment_price_unit"),
    # F4101 misc
    F.col("imuwum").alias("uom_weight"),
    F.col("sdglc").alias("gl_offset"),
    # Header / instructions
    F.col("shdel1").alias("delivery_instructions_line_1"),
    F.col("shdel2").alias("delivery_instructions_line_2"),
    # Identity / business
    F.col("sdco").alias("company"),
    F.col("sdkcoo").alias("key_company_order"),
    F.col("sdlnid").alias("line_number"),
    F.col("business_unit"),
    # Cost (from dim_item_cost_cascade)
    F.col("cost_per_ton").alias("item_unit_cost"),
    F.col("cost_method"), F.col("cost_source_tier"), F.col("cost_confidence"),
    # ASN
    F.col("sdasn").alias("advanced_ship_notice"),
    # carried for NRT audit (dropped from hash/business below)
    F.col("sd_date_updated"),
)
print(f"Business columns : {len(df_business.columns) - 1}")   # minus sd_date_updated helper


# In[13]:


# ─────────────────────────────────────────────────────────────────────────────
# NRT KEYS + AUDIT COLUMNS
# ─────────────────────────────────────────────────────────────────────────────
# • order_line_key          = sha2(business key)  — MERGE + Power BI relationship key
# • source_commit_timestamp = data "as of" (proxy = F4211 date_updated; replace
#                             with true CDC commit timestamp if Silver exposes it)
# • gold_updated_timestamp  = this merge run
# • is_deleted              = false for current rows (MERGE flips absent keys true)
# • record_hash            = over all business columns -> skip no-op merges
# ─────────────────────────────────────────────────────────────────────────────
business_cols = [c for c in df_business.columns if c != "sd_date_updated"]

df_keyed = (
    df_business
    .withColumn("order_line_key",
        F.sha2(F.concat_ws("||",
            F.col("key_company_order").cast("string"),
            F.col("order_type").cast("string"),
            F.col("order_number").cast("string"),
            F.col("line_number").cast("string")), 256))
    .withColumn("record_hash",
        F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in business_cols]), 256))
    .withColumn("is_deleted",              F.lit(False))
    .withColumn("source_commit_timestamp", F.col("sd_date_updated").cast("timestamp"))
    .withColumn("gold_updated_timestamp",  F.lit(run_dt).cast("timestamp"))
    .drop("sd_date_updated")
)

# order_line_key first, then business cols, then audit
audit_cols = ["is_deleted", "source_commit_timestamp", "gold_updated_timestamp", "record_hash"]
df_src = df_keyed.select("order_line_key", *business_cols, *audit_cols)

# Grain guard — order_line_key must be unique
dups = df_src.groupBy("order_line_key").count().filter(F.col("count") > 1).count()
print(f"Final rows: {df_src.count():,} | columns: {len(df_src.columns)} | dup keys: {dups}  ← must be 0")


# In[14]:


# ─────────────────────────────────────────────────────────────────────────────
# WRITE — current-state MERGE on order_line_key
# ─────────────────────────────────────────────────────────────────────────────
spark.sql("CREATE SCHEMA IF NOT EXISTS lh_jde_gold.otc")   # schema-enabled lakehouse guard
if not spark.catalog.tableExists(TARGET_TABLE):
    (df_src.write
        .format("delta")
        .option("delta.enableDeletionVectors", "true")
        .clusterBy("branch_plant", "shipment_number")     # liquid clustering for NRT + Direct Lake
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE))
    print(f"✓ Created {TARGET_TABLE} ({df_src.count():,} rows)")
else:
    tgt = DeltaTable.forName(spark, TARGET_TABLE)
    (tgt.alias("t")
        .merge(df_src.alias("s"), "t.order_line_key = s.order_line_key")
        .whenMatchedUpdateAll(condition="t.record_hash <> s.record_hash")
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceUpdate(set={                # lines that ship/close out of the open set
            "is_deleted":             F.lit(True),
            "gold_updated_timestamp": F.lit(run_dt).cast("timestamp"),
        })
        .execute())
    print(f"✓ MERGE complete into {TARGET_TABLE}")

print(f"  run_dt = {run_dt}")


# In[15]:


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────────────────────────────────────────────
fct = spark.read.table(TARGET_TABLE)
active = fct.filter(F.col("is_deleted") == False)
print(f"Total rows: {fct.count():,} | active: {active.count():,}")

dups = fct.groupBy("order_line_key").count().filter(F.col("count") > 1).count()
print(f"Duplicate order_line_key : {dups}  ← must be 0")

print("\nStatus distribution (active):")
display(active.groupBy("status").count().orderBy(F.col("count").desc()))

print("\nCost source tier distribution (active):")
display(active.groupBy("cost_source_tier").count().orderBy(F.col("count").desc()))

print("\nMissing conversion (qty side):")
display(active.filter(F.col("missing_conversion_flag") == "Y").groupBy("uom").count().orderBy(F.col("count").desc()))

print("\nMax gold_updated_timestamp (should be ~now):")
display(fct.agg(F.max("gold_updated_timestamp").alias("last_merge")))

print("\nSample 10 (top by revenue):")
display(active.orderBy(F.col("revenue_dollars").desc_nulls_last()).limit(10))


# In[16]:


# ─────────────────────────────────────────────────────────────────────────────
# OPTIMIZE / VACUUM MAINTENANCE  —  run on a SEPARATE, slower schedule
# ─────────────────────────────────────────────────────────────────────────────
# DO NOT run this in the 5-min load cycle. Clustering compaction + vacuum are
# expensive and would burn the F64 compute budget. Run it from a dedicated
# maintenance job (hourly or nightly) — flip RUN_MAINTENANCE=True there.
#
#   • OPTIMIZE (no ZORDER args) performs the incremental LIQUID-CLUSTERING
#     compaction for the CLUSTER BY (branch_plant, shipment_number) keys, and
#     merges the small files that frequent 5-min MERGEs create. Direct Lake stays fast.
#   • V-Order is ON by default in Fabric — no extra step needed.
#   • VACUUM reclaims files older than the retention window (deletion vectors +
#     superseded MERGE files). 168h (7 days) preserves a week of time-travel.
# ─────────────────────────────────────────────────────────────────────────────
RUN_MAINTENANCE = False   # ← set True ONLY in the scheduled maintenance run

if RUN_MAINTENANCE:
    print(f"OPTIMIZE {TARGET_TABLE} (liquid-clustering compaction)…")
    spark.sql(f"OPTIMIZE {TARGET_TABLE}")
    print(f"VACUUM {TARGET_TABLE} RETAIN 168 HOURS…")
    spark.sql(f"VACUUM {TARGET_TABLE} RETAIN 168 HOURS")
    print("✓ Maintenance complete")
else:
    print("RUN_MAINTENANCE=False — skipped (run from the scheduled maintenance job).")


# In[17]:


# ─────────────────────────────────────────────────────────────────────────────
# POWER BI SEMANTIC MODEL — relationships for fact_sales_order_line
# ─────────────────────────────────────────────────────────────────────────────
# (reference notes — nothing executed)
#
# Relationships (all Many fact : One dim, single cross-filter dim → fact):
#   bill_to             →  dim_address_sold_to.address_number
#   ship_to             →  dim_address_ship_to.address_number
#   carrier_number      →  dim_address_carrier.address_number
#   loading_port        →  dim_address_loading_port.address_number
#   ocean_carrier       →  dim_address_ocean_carrier.address_number
#   port_of_destination →  dim_address_destination.address_number
#   branch_plant        →  dim_plant.plant_code
#
#   (the dim_address_* are role-playing SQL VIEWS over dim_address_book, created
#    once in nb_dim_address_book — ONE physical dim, six logical roles. Names /
#    city / state resolve via these views; this fact stays skinny, ID-only.)
#
# COST: already EMBEDDED on the fact (item_unit_cost, cost_method,
#   cost_source_tier, cost_confidence via the dim_item_cost_cascade join) —
#   no relationship needed. Do NOT also relate to dim_item_cost_cascade
#   (would be a redundant composite-key relationship).
#
# CONFORMANCE WITH fact_freight_audit:
#   • shipment_number is a DEGENERATE column on BOTH facts (the bridge).
#   • Do NOT create a direct fact-to-fact relationship. Slice freight vs
#     order-line metrics through the SHARED dims (dim_address carrier/ship-to,
#     dim_plant, company). For shipment-level cross-fact slicing, add a small
#     dim_shipment = DISTINCT shipment_number related 1:* to each fact.
#
# DATES: house pattern has no physical dim_date — date columns are inline
#   (order_date, actual_ship_date, effective_date, fiscal_year/fiscal_month).
#   If a shared calendar is later required, add a dim_date and relate
#   effective_date (mark as date table).
#
# MODEL HYGIENE:
#   • Filter is_deleted = false so CDC soft-deleted (shipped/closed) lines drop
#     out of every visual.
#   • status_sort is the Sort-By-Column for `status`.
#   • Hide keys (order_line_key, record_hash, *_number IDs) from report view.
# ─────────────────────────────────────────────────────────────────────────────
print("Power BI relationship notes — see cell comments above.")
