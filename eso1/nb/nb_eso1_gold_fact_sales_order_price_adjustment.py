#!/usr/bin/env python
# coding: utf-8

# ## nb_eso1_gold_fact_sales_order_price_adjustment
#
# **Gold `fact_sales_order_price_adjustment` processor** — builds ONE table,
# `lh_jde_gold.rpt.fact_sales_order_price_adjustment`, at
# **sales-order-line × price-adjustment** grain, for the price-adjustment
# reporting family.
#
# ── GRAIN ──
# One row per F4211 sales-order line × qualifying F4074 price-adjustment. F4074 is
# LEFT-joined after a whitelist pre-filter (ALAST in ADJ_WHITELIST), so a line with N
# whitelisted adjustments fans out to N rows, and a line with none produces one base
# row (adjustment columns NULL).
#
# ── F4211-ONLY (no F42119 history union) ──
# Driver is the live F4211 sales-order detail only; F42119 (sales-order history) is NOT
# unioned here.
#
# ── LINE MEASURES REPEAT ACROSS THE FAN ──
# The line-grain values (extended_price SDAEXP, extended_cost SDECST, ordered_tons,
# shipped_tons, quantity SDSOQS/SDPQOR) are computed once per line and repeat on every
# adjustment row of that line. Display-once is a measure / row-type concern.
#
# ── ORDERED / SHIPPED TONS CASCADE (self-contained, not the shared UOM dims) ──
# ordered_tons = SDUORG × factor ; shipped_tons = SDSOQS × factor, where factor is the
# SDUOM→TN conversion built here from F41002 (item-specific, blank cost-center) → F41003
# (standard) → SDUOM=IMUOM1 identity, with the ambiguity rule (more than one distinct
# factor for a key → unusable → 0) and the zero/NULL-divisor guard → 0. The item-specific
# leg reads F41002.UMCNV1 (Silver conversion_factor_sec) and its reverse UMCNV1/UMCONV;
# the standard leg reads F41003.UCCONV (Silver conversion_factor) and its reciprocal. Kept
# separate from the shared `dim_uom_conversion*` cascade (which uses UMCONV) so this fact
# resolves tons independently.
#
# ── KEY ──
# `sales_order_line_key` = sk(SDKCOO | SDDCTO | SDDOCO | SDLNID) — the SAME formula the
# other ESO1 facts use, so the shared dimensions relate unchanged.
# `price_adjustment_key` = sk(sales_order_line_key | seq) — unique per fanned row.
#
# ── NO BUSINESS FILTERS ──
# No status / document-type / date / branch / customer WHERE — the fact carries EVERY
# F4211 line (those are page filters). The ONLY grain-shaping rule baked in is the F4074
# ALAST whitelist (it defines the fan-out and the base-row semantics).
#
# ── BUILD (BATCH) ──
#   • read the Silver snapshot of every source, run build_fact() ONCE, overwrite the fact.
#   • MANUAL_OVERWRITE = True -> drop + rebuild; False -> build only if the fact is missing.
#   • static snapshot — no CDF / checkpoints / streams.
#
# Sections:  1) CONFIG   2) FACT BUILDER   3) FACT SOURCES   4) RUN

# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------

import json, time
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

# ── refresh / runtime config (BATCH build) ──
MANUAL_OVERWRITE = True    # ⚠ set back to False after the first successful run

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

# ── Silver sources ──────────────────────────────────────────────────────────────
F4211  = "f4211_sales_order_detail_file"                             # sales-order detail (line grain — driver)
F4074  = "f4074_price_adjustment_ledger_file"                        # price-adjustment ledger (fan-out)
F4101  = "f4101_item_master"                                         # item master (IMUOM1 for the tons cascade)
F4201  = "f4201_sales_order_header_file"                             # order header (hold code)
F41002 = "f41002_item_units_of_measure_conversion_factors"          # item-specific UOM conversion (Tier A)
F41003 = "f41003_unit_of_measure_standard_conversion"               # standard UOM conversion (Tier B)
F49211 = "f49211_sales_order_detail_file_tag_file"                  # SO-line tag file (UDDEFF deferred flag)

# ── Gold target BUILT here ─────────────────────────────────────────────────────
FACT = "fact_sales_order_price_adjustment"

# ── Price-adjustment whitelist (ALAST) — GRAIN-shaping ──
# Only these adjustment types fan a line out; every other F4074 row is treated as "no adjustment"
# (the line keeps its single base row via the LEFT join).
_ADJ_WHITELIST_CORE = [
    "A03", "CASLB", "FRTHIDE", "FRTTAXN", "FRTTAXY",
    "PP06", "PP07", "PP08", "PP13", "PP15", "PP17", "PP26", "PP37",
    "PP50", "PP51", "PP56", "PP57", "PP97", "PP99", "PPSLB",
    "COLPALN", "COLPALT", "ALST",
]
# ENERGY toggle. True → ENERGY adjustments fan out too. A line that carries ENERGY alongside another
# whitelisted adjustment is unaffected in its aggregates (the ENERGY row has is_primary_line_row=N and
# 0 in every bucket column). A line whose ONLY whitelisted adjustment is ENERGY exists as an ENERGY row
# instead of a base row — set False to keep such a line as a base row.
INCLUDE_ENERGY = True
ADJ_WHITELIST = _ADJ_WHITELIST_CORE + (["ENERGY"] if INCLUDE_ENERGY else [])

print(f"ESO1 Gold fact_sales_order_price_adjustment processor (batch build) — target {gname(FACT)}")

# HELPERS
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

def load_silver_table(table_name):
    df = spark.read.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

def sk(*cols):
    """Surrogate key — pipe-separated string (identical formula to the other ESO1 facts)."""
    return F.concat_ws(
        "|",
        *[F.col(c).cast("string") if isinstance(c, str) else c.cast("string") for c in cols],
    )

# ── date hygiene ──────────────────────────────────────────────────────────────
# JDE carries sentinel/corrupt dates; null anything outside a plausible business window
# so the stored raw dates (and derived int keys) stay clean.
VALID_DATE_LO     = "2000-01-01"
VALID_YEARS_AHEAD = 25

_RAW_DATE_COLS = ["order_date", "requested_date", "actual_ship_date", "invoice_date", "gl_date"]

def clean_date(col):
    hi = F.make_date(F.year(F.current_date()) + F.lit(VALID_YEARS_AHEAD), F.lit(12), F.lit(31))
    return F.when(col.between(F.lit(VALID_DATE_LO).cast("date"), hi), col)

def date_key(col):
    return F.when(col.isNotNull(), F.date_format(col, "yyyyMMdd").cast("int"))


# ----------------------------------------------------------------------------
# 2) FACT BUILDER
# ----------------------------------------------------------------------------

def build_conv_lookups():
    """Two bidirectional UOM→factor lookups, each collapsed with the ambiguity rule
    (more than one distinct factor for a key → NULL, i.e. unusable). Factors are decoded
    in Silver, so the SQL's 10000000 identity is 1.0 here and the from/to ratio is unit-free.

    conv_item  keyed (item, from_uom):
        fwd  from_uom = UMUM       factor = UMCNV1 (conversion_factor_sec)
        rev  from_uom = UMRUM      factor = UMCNV1 / UMCONV  (UMCONV<>0)
        blank cost-center (UMMCU) rows only — matches the item-specific join.
    conv_std   keyed (from_uom, to_uom):
        fwd  from_uom = UCUM  to_uom = UCRUM   factor = UCCONV (conversion_factor)
        rev  from_uom = UCRUM to_uom = UCUM    factor = 1 / UCCONV  (UCCONV<>0)
    """
    # ── F41002 item-specific (blank cost-center only) ──
    f41002 = load_silver_table(F41002).filter(F.trim(F.col("cost_center")) == "")
    i_fwd = f41002.select(
        F.col("identifier_short_item").alias("itm"),
        F.trim(F.col("uom")).alias("from_uom"),
        F.round(F.col("conversion_factor_sec").cast("double"), 9).alias("f"))
    i_rev = (f41002.filter(F.col("conversion_factor").cast("double") != 0)
             .select(
                 F.col("identifier_short_item").alias("itm"),
                 F.trim(F.col("related_uom")).alias("from_uom"),
                 F.round(F.col("conversion_factor_sec").cast("double")
                         / F.col("conversion_factor").cast("double"), 9).alias("f")))
    conv_item = (i_fwd.unionByName(i_rev)
                 .dropDuplicates(["itm", "from_uom", "f"])          # SQL UNION dedups identical (key,value)
                 .groupBy("itm", "from_uom")
                 .agg(F.count("f").alias("n"), F.min("f").alias("f"))
                 .withColumn("conv_item", F.when(F.col("n") > 1, F.lit(None).cast("double"))
                                           .otherwise(F.col("f")))  # >1 distinct factor → unusable
                 .select("itm", "from_uom", "conv_item"))

    # ── F41003 standard ──
    f41003 = load_silver_table(F41003)
    s_fwd = f41003.select(
        F.trim(F.col("uom")).alias("from_uom"),
        F.trim(F.col("related_uom")).alias("to_uom"),
        F.round(F.col("conversion_factor").cast("double"), 9).alias("f"))
    s_rev = (f41003.filter(F.col("conversion_factor").cast("double") != 0)
             .select(
                 F.trim(F.col("related_uom")).alias("from_uom"),
                 F.trim(F.col("uom")).alias("to_uom"),
                 F.round(F.lit(1.0) / F.col("conversion_factor").cast("double"), 9).alias("f")))
    conv_std = (s_fwd.unionByName(s_rev)
                .dropDuplicates(["from_uom", "to_uom", "f"])
                .groupBy("from_uom", "to_uom")
                .agg(F.count("f").alias("n"), F.min("f").alias("f"))
                .withColumn("conv_std", F.when(F.col("n") > 1, F.lit(None).cast("double"))
                                         .otherwise(F.col("f")))
                .select("from_uom", "to_uom", "conv_std"))

    return conv_item, conv_std


# ── line-grain columns carried on the fact (repeat across the adjustment fan) ──
LINE_COLS = [
    # ── order / line identifiers ──
    "company", "company_key_order_no", "order_type", "order_number", "document_number", "line_number",
    "shipment_number",
    # ── status / handling / transport ──
    "hold_orders_code", "status_code_last", "status_code_next", "next_status_num", "last_status_num",
    "freight_handling_code", "cars", "mode_of_transport", "container_id", "customer_po_number",
    "gl_class", "line_type", "payment_terms_code_01",
    "sales_reporting_code_01", "sales_reporting_code_02", "sales_reporting_code_03",
    "sales_reporting_code_04", "sales_reporting_code_05",
    # ── address / dimension FKs ──
    "ship_to", "sold_to", "address_number_parent", "branch_plant",
    "item_number_short", "second_item_number", "third_item_number", "item_segment_4",
    # ── raw event dates + int keys ──
    "order_date", "requested_date", "actual_ship_date", "invoice_date", "gl_date",
    "order_date_key", "requested_date_key", "ship_date_key", "invoice_date_key", "gl_date_key",
    # ── uom / tons ──
    "uom", "uom_primary", "uom_structure", "conversion_to_tons_rate", "missing_conversion_flag",
    "ordered_tons", "shipped_tons",
    # ── line measures ──
    "quantity_shipped", "primary_quantity_ordered", "transaction_quantity",
    "extended_price", "extended_cost", "unit_price", "currency_code_base",
    # ── misc display / filter ──
    "location", "lot_number", "user_reserved_code", "user_reserved_number", "user_reserved_reference",
    "price_override_code", "price_adjustment_schedule", "transaction_originator", "user_id",
    "date_updated", "time_of_day", "zone_number", "deferred_entries_flag",
]

# ── adjustment-grain columns (NULL on base rows) ──
ADJ_COLS = ["price_adjustment_type", "adj_print_code", "adj_unit_price", "adj_uom",
            "adj_based_on_value", "adj_gl_class", "adj_factor_value"]

# is_primary_line_row: 'Y' on exactly ONE row per line. Line-grain values repeat across the
# adjustment fan, so line-level measures must sum with this flag (CALCULATE(..., ="Y")) to avoid
# N× inflation; adjustment-level buckets ignore it and iterate all rows.
# is_product_line + freight_hide_amount: precomputed classification so the semantic-model
# measures are trivial fast aggregates instead of per-row FILTER(fact) scans.
FACT_BUSINESS_COLS = LINE_COLS + ADJ_COLS + [
    "is_primary_line_row", "is_product_line",
    # precomputed per-row bucket amounts (measures = plain SUM(column))
    "product_ordered_tons", "product_ext_price", "freight_hide_amount",
    "non_product_amount", "al_severance_amount", "misc_billing_amount",
    "freight_amount", "car_charges_amount", "dryer_freight_amount"]


def build_line_df():
    """One row per F4211 sales-order line, with the item master (IMUOM1), header hold code,
    and the computed ordered/shipped tons. No business WHERE — every line is carried."""
    sd  = load_silver_table(F4211)
    im  = load_silver_table(F4101)
    sh  = load_silver_table(F4201)
    tg  = load_silver_table(F49211)
    conv_item, conv_std = build_conv_lookups()

    # SO-line tag → deferred-entries flag (UDDEFF); one row per line so the LEFT join can't fan the grain
    tag = (tg.groupBy("company_key_order_no", "order_type", "document_order_invoice_e", "line_number")
             .agg(F.first("deferred_entries_flag", ignorenulls=True).alias("deferred_entries_flag")))

    # item master → primary UOM (IMUOM1), item segment (IMSEG4) — one row per short item
    item = (im.select(
                F.col("identifier_short_item").alias("im_itm"),
                F.trim(F.col("uom_primary")).alias("uom_primary"),
                F.col("segment_04").alias("item_segment_4"))
            .dropDuplicates(["im_itm"]))

    # item-level UOM structure (F41002 UMUSTR, blank cost-center) → one row per item so this LEFT
    # join can't fan the line grain. UMUSTR is an item attribute (the UOM template the item uses —
    # constant across the item's UOM rows); F.max collapses the rare multi-value case deterministically.
    struct = (load_silver_table(F41002).filter(F.trim(F.col("cost_center")) == "")
              .groupBy("identifier_short_item")
              .agg(F.max(F.trim(F.col("uom_structure"))).alias("uom_structure")))

    # order header → hold code (one row per order)
    hdr = (sh.select(
               F.col("company_key_order_no").alias("h_kcoo"),
               F.col("order_type").alias("h_dcto"),
               F.col("document_order_invoice_e").alias("h_doco"),
               F.col("hold_orders_code").alias("hold_orders_code"))
           .dropDuplicates(["h_kcoo", "h_dcto", "h_doco"]))

    j = (sd.alias("sd")
         .join(item.alias("im"), F.col("im.im_itm") == F.col("sd.identifier_short_item"), "left")
         .join(hdr.alias("sh"),
               (F.col("sh.h_kcoo") == F.col("sd.company_key_order_no")) &
               (F.col("sh.h_dcto") == F.col("sd.order_type")) &
               (F.col("sh.h_doco") == F.col("sd.document_order_invoice_e")), "left")
         .join(tag.alias("tg"),
               (F.col("tg.company_key_order_no") == F.col("sd.company_key_order_no")) &
               (F.col("tg.order_type") == F.col("sd.order_type")) &
               (F.col("tg.document_order_invoice_e") == F.col("sd.document_order_invoice_e")) &
               (F.col("tg.line_number") == F.col("sd.line_number")), "left")
         .join(struct.alias("us"),
               F.col("us.identifier_short_item") == F.col("sd.identifier_short_item"), "left")
         # tons cascade lookups: item-specific (from SDUOM / to TN) then standard (from SDUOM / to TN)
         .join(conv_item.alias("cif"),
               (F.col("cif.itm") == F.col("sd.identifier_short_item")) &
               (F.col("cif.from_uom") == F.trim(F.col("sd.uom_as_input"))), "left")
         .join(conv_item.alias("cit"),
               (F.col("cit.itm") == F.col("sd.identifier_short_item")) &
               (F.col("cit.from_uom") == F.lit("TN")), "left")
         .join(conv_std.alias("csf"),
               (F.col("csf.from_uom") == F.trim(F.col("sd.uom_as_input"))) &
               (F.col("csf.to_uom") == F.trim(F.col("im.uom_primary"))), "left")
         .join(conv_std.alias("cst"),
               (F.col("cst.from_uom") == F.lit("TN")) &
               (F.col("cst.to_uom") == F.trim(F.col("im.uom_primary"))), "left"))

    # from-factor: item-specific → standard → (SDUOM = IMUOM1 ? 1.0)
    from_factor = F.coalesce(
        F.col("cif.conv_item"), F.col("csf.conv_std"),
        F.when(F.trim(F.col("sd.uom_as_input")) == F.trim(F.col("im.uom_primary")), F.lit(1.0)))
    # to-factor: item-specific(TN) → standard(TN) → ('TN' = IMUOM1 ? 1.0)
    to_factor = F.coalesce(
        F.col("cit.conv_item"), F.col("cst.conv_std"),
        F.when(F.lit("TN") == F.trim(F.col("im.uom_primary")), F.lit(1.0)))
    # zero/null-divisor guard → factor 0 (zero-on-fail cascade)
    factor = F.when(from_factor.isNull() | to_factor.isNull() | (to_factor == 0), F.lit(0.0)) \
              .otherwise(from_factor / to_factor)

    sel = j.select(
        # ── order / line identifiers ──
        F.col("sd.company").alias("company"),
        F.col("sd.company_key_order_no").alias("company_key_order_no"),   # SDKCOO
        F.col("sd.order_type").alias("order_type"),                       # SDDCTO
        F.col("sd.document_order_invoice_e").alias("order_number"),       # SDDOCO
        F.col("sd.doc_voucher_invoice_e").alias("document_number"),       # SDDOC
        F.col("sd.line_number").alias("line_number"),                     # SDLNID
        F.col("sd.shipment_number").alias("shipment_number"),             # SDSHPN
        # ── status / handling / transport ──
        F.col("sh.hold_orders_code").alias("hold_orders_code"),           # SHHOLD (header)
        F.col("sd.status_code_last").alias("status_code_last"),           # SDLTTR
        F.col("sd.status_code_next").alias("status_code_next"),           # SDNXTR
        F.trim(F.col("sd.status_code_next")).cast("int").alias("next_status_num"),
        F.trim(F.col("sd.status_code_last")).cast("int").alias("last_status_num"),
        F.col("sd.freight_handling_code").alias("freight_handling_code"), # SDFRTH
        F.col("sd.carrier").alias("cars"),                                # SDCARS
        F.trim(F.col("sd.mode_of_transport")).alias("mode_of_transport"), # SDMOT
        F.col("sd.container_id").alias("container_id"),                   # SDCNID (Vehicle No.)
        F.col("sd.reference_01").alias("customer_po_number"),             # SDVR01 (Customer PO Number)
        F.col("sd.gl_class").alias("gl_class"),                           # SDGLC
        F.col("sd.line_type").alias("line_type"),                        # SDLNTY
        F.col("sd.payment_terms_code_01").alias("payment_terms_code_01"), # SDPTC (payment terms)
        F.col("sd.sales_reporting_code_01").alias("sales_reporting_code_01"),
        F.col("sd.sales_reporting_code_02").alias("sales_reporting_code_02"),
        F.col("sd.sales_reporting_code_03").alias("sales_reporting_code_03"),
        F.col("sd.sales_reporting_code_04").alias("sales_reporting_code_04"),
        F.col("sd.sales_reporting_code_05").alias("sales_reporting_code_05"),
        # ── address / dimension FKs ──
        F.col("sd.address_number_ship_to").alias("ship_to"),              # SDSHAN
        F.col("sd.address_number").alias("sold_to"),                      # SDAN8
        F.col("sd.address_number_parent").alias("address_number_parent"), # SDPA8
        F.trim(F.col("sd.cost_center")).alias("branch_plant"),            # SDMCU
        F.col("sd.identifier_short_item").alias("item_number_short"),     # SDITM
        F.col("sd.identifier_second_item").alias("second_item_number"),   # SDLITM
        F.col("sd.identifier_third_item").alias("third_item_number"),     # SDAITM (freight/car-charge line prefix)
        F.col("im.item_segment_4").alias("item_segment_4"),               # IMSEG4
        # ── raw event dates ──
        F.col("sd.date_transaction_julian").alias("order_date"),          # SDTRDJ
        F.col("sd.date_requested_julian").alias("requested_date"),        # SDDRQJ
        F.col("sd.actual_ship_date").alias("actual_ship_date"),           # SDADDJ
        F.col("sd.date_invoice_julian").alias("invoice_date"),            # SDIVD
        F.col("sd.dt_for_gl_and_vouch_01").alias("gl_date"),              # SDDGL
        # ── uom / tons ──
        F.col("sd.uom_as_input").alias("uom"),                            # SDUOM
        F.col("im.uom_primary").alias("uom_primary"),                     # IMUOM1
        F.col("us.uom_structure").alias("uom_structure"),                 # UMUSTR (F41002 item UOM structure)
        factor.alias("conversion_to_tons_rate"),
        F.when(factor == 0, F.lit("Y")).otherwise(F.lit("N")).alias("missing_conversion_flag"),
        (F.col("sd.units_transaction_qty").cast("double") * factor).alias("ordered_tons"),   # SDUORG × factor
        (F.col("sd.units_quantity_shipped").cast("double") * factor).alias("shipped_tons"),  # SDSOQS × factor
        # ── line measures ──
        F.col("sd.units_quantity_shipped").alias("quantity_shipped"),         # SDSOQS
        F.col("sd.units_primary_qty_order").alias("primary_quantity_ordered"),# SDPQOR
        F.col("sd.units_transaction_qty").alias("transaction_quantity"),      # SDUORG
        F.col("sd.amount_extended_price").alias("extended_price"),            # SDAEXP
        F.col("sd.amount_extended_cost").alias("extended_cost"),              # SDECST
        F.col("sd.amt_price_per_unit_02").alias("unit_price"),                # SDUPRC
        F.col("sd.currency_code_base").alias("currency_code_base"),           # SDBCRC
        # ── misc display / filter ──
        F.col("sd.location").alias("location"),                           # SDLOCN
        F.col("sd.lot").alias("lot_number"),                             # SDLOTN
        F.col("sd.user_reserved_code").alias("user_reserved_code"),      # SDURCD
        F.col("sd.user_reserved_number").alias("user_reserved_number"),  # SDURAB
        F.col("sd.user_reserved_reference").alias("user_reserved_reference"),  # SDURRF
        F.col("sd.price_override_code").alias("price_override_code"),     # SDPROV
        F.col("sd.price_adjustment_schedule_n").alias("price_adjustment_schedule"),  # SDASN
        F.col("sd.transaction_originator").alias("transaction_originator"),  # SDTORG
        F.col("sd.user_id").alias("user_id"),                            # SDUSER
        F.col("sd.date_updated").alias("date_updated"),                  # SDUPMJ
        F.col("sd.time_of_day").alias("time_of_day"),                    # SDTDAY
        F.col("sd.zone_number").alias("zone_number"),                    # SDZON
        F.col("tg.deferred_entries_flag").alias("deferred_entries_flag"),# F49211 UDDEFF
    )

    # clean sentinel dates, then derive int date keys
    for _dc in _RAW_DATE_COLS:
        sel = sel.withColumn(_dc, clean_date(F.col(_dc)))
    sel = (sel
           .withColumn("order_date_key",     date_key(F.col("order_date")))
           .withColumn("requested_date_key", date_key(F.col("requested_date")))
           .withColumn("ship_date_key",      date_key(F.col("actual_ship_date")))
           .withColumn("invoice_date_key",   date_key(F.col("invoice_date")))
           .withColumn("gl_date_key",        date_key(F.col("gl_date"))))

    # line key + one row per line (defensive — F4211 is already line grain)
    sel = sel.withColumn("sales_order_line_key",
                         sk("company_key_order_no", "order_type", "order_number", "line_number"))
    return sel.dropDuplicates(["sales_order_line_key"])


def build_adjustments():
    """Whitelisted F4074 adjustments only, keyed to the line. The whitelist is applied
    BEFORE the fan-join so a line whose only adjustments are non-whitelisted keeps a single
    base row (LEFT join → NULL adjustment)."""
    adj = load_silver_table(F4074)
    adj = adj.filter(F.trim(F.col("price_adjustment_type")).isin(ADJ_WHITELIST))
    adj = adj.select(
        sk("company_key_order_no", "order_type", "document_order_invoice_e", "line_number")  # ALKCOO|ALDCTO|ALDOCO|ALLNID
          .alias("sales_order_line_key"),
        F.trim(F.col("price_adjustment_type")).alias("price_adjustment_type"),  # ALAST
        F.trim(F.col("pricing_report_code_01")).alias("adj_print_code"),        # ALAPRP1
        F.col("amt_price_per_unit_02").cast("double").alias("adj_unit_price"),   # ALUPRC
        F.trim(F.col("uom_as_input")).alias("adj_uom"),                         # ALUOM
        F.col("based_on_value").cast("double").alias("adj_based_on_value"),      # ALBSDVAL
        F.col("gl_class").alias("adj_gl_class"),                                 # ALGLC
        F.col("factor_value").cast("double").alias("adj_factor_value"))          # ALFVTR
    # collapse to the adjustment grain (ALAST, ALUPRC, ALUOM, ALBSDVAL) so duplicate F4074
    # records don't double-count in the adjustment-bucket measures.
    return adj.dropDuplicates(["sales_order_line_key", "price_adjustment_type",
                               "adj_unit_price", "adj_uom", "adj_based_on_value"])


def build_fact():
    line = build_line_df()
    adj  = build_adjustments()

    # fan the line out to one row per whitelisted adjustment; lines with none keep one base row
    df = line.join(adj, "sales_order_line_key", "left")

    # per-fanned-row surrogate key — stable within a line by a deterministic adjustment sequence
    _ws = Window.partitionBy("sales_order_line_key").orderBy(
              F.col("price_adjustment_type").asc_nulls_first(),
              F.col("adj_unit_price").asc_nulls_first(),
              F.col("adj_gl_class").asc_nulls_first())
    df = (df.withColumn("_seq", F.row_number().over(_ws))
            .withColumn("price_adjustment_key", sk("sales_order_line_key", "_seq"))
            .withColumn("is_primary_line_row",
                        F.when(F.col("_seq") == 1, F.lit("Y")).otherwise(F.lit("N")))
            .drop("_seq"))

    # ── classification columns (precomputed so the DAX measures are fast aggregates) ──
    # is_product_line: a product line is priced by the standard base-price adjustment (has an A03 F4074
    # row) OR net-priced (user_reserved_code NP/N3), AND is NOT a freight line (F/FT) and NOT a
    # charge/dryer item.
    _charge_items = ["MISC BILLING", "EXPEDITE FEE", "BANKING FEE", "TRANSLOAD CHARGES",
                     "DRYER TAILINGS", "DRYER TAILING #1", "DRYER TAILING #40"]
    _line_w = Window.partitionBy("sales_order_line_key")
    df = df.withColumn("_has_a03",
                       F.max(F.when(F.trim(F.col("price_adjustment_type")) == "A03", F.lit(1)).otherwise(F.lit(0)))
                        .over(_line_w))
    _is_prod = ((((F.col("_has_a03") == 1) | F.trim(F.col("user_reserved_code")).isin("NP", "N3"))
                 & (~F.trim(F.col("line_type")).isin("F", "FT")))
                & (~F.trim(F.col("second_item_number")).isin(_charge_items)))
    df = (df.withColumn("is_product_line", F.when(_is_prod, F.lit("Y")).otherwise(F.lit("N")))
            .drop("_has_a03"))
    # freight_hide_amount: FRTHIDE adjustment priced by its own UOM = adj_unit_price × qty-in-ALUOM
    # (ordered_tons when adj_uom=TN, else the line's native transaction_quantity). Counted ONLY for the
    # ALUOM×SDUOM pairs (TM,BG),(TN,TN),(TN,BG),(BG,BG),(TM,TM); zero on all other rows/pairs.
    _au = F.trim(F.col("adj_uom")); _su = F.trim(F.col("uom"))
    _fh_pair = (((_au == "TM") & (_su == "BG")) | ((_au == "TN") & (_su == "TN"))
                | ((_au == "TN") & (_su == "BG")) | ((_au == "BG") & (_su == "BG"))
                | ((_au == "TM") & (_su == "TM")))
    df = df.withColumn("freight_hide_amount",
                       F.when((F.trim(F.col("price_adjustment_type")) == "FRTHIDE") & _fh_pair,
                              F.col("adj_unit_price").cast("double")
                              * F.when(_au == "TN", F.col("ordered_tons"))
                                 .otherwise(F.col("transaction_quantity").cast("double")))
                        .otherwise(F.lit(0.0)))

    # ── precomputed per-row bucket amounts so every DAX measure is a plain SUM(column) (no per-cell
    #    FILTER(fact) scans). ──
    _prod    = (F.col("is_product_line") == "Y") & (F.col("is_primary_line_row") == "Y")   # one product row per line
    _adjton  = F.col("adj_unit_price").cast("double") * F.col("ordered_tons")               # adjustment $ = ALUPRC × tons
    _pref3   = F.substring(F.trim(F.col("third_item_number")), 1, 3)                        # SDAITM prefix
    _frtline = (F.col("is_primary_line_row") == "Y") & F.trim(F.col("line_type")).isin("F", "FT")  # primary freight row
    df = (df
          # product line values (deduped) — Total Tons / Product Price
          .withColumn("product_ordered_tons", F.when(_prod, F.col("ordered_tons")).otherwise(F.lit(0.0)))
          .withColumn("product_ext_price",    F.when(_prod, F.col("extended_price").cast("double")).otherwise(F.lit(0.0)))
          # adjustment buckets by print code (ALUPRC × tons)
          .withColumn("non_product_amount",   F.when(F.trim(F.col("adj_print_code")) == "NON", _adjton).otherwise(F.lit(0.0)))
          .withColumn("al_severance_amount",  F.when(F.trim(F.col("adj_print_code")) == "ALA", _adjton).otherwise(F.lit(0.0)))
          .withColumn("misc_billing_amount",  F.when((F.trim(F.col("adj_print_code")) == "ACR")
                          | (F.substring(F.trim(F.col("price_adjustment_type")), 1, 2) == "PP"), _adjton).otherwise(F.lit(0.0)))
          # freight-line buckets by SDAITM prefix (extended_price on the primary freight row) + FRTTAX adj
          .withColumn("freight_amount",
                      F.when(_frtline & _pref3.isin("BIL", "FRE", "FUE", "TRA"), F.col("extended_price").cast("double")).otherwise(F.lit(0.0))
                      + F.when(F.trim(F.col("price_adjustment_type")).isin("FRTTAXN", "FRTTAXY"), _adjton).otherwise(F.lit(0.0)))
          .withColumn("car_charges_amount",
                      F.when(_frtline & (_pref3 == "RAI"), F.col("extended_price").cast("double")).otherwise(F.lit(0.0)))
          # dryer-freight bucket: freight line (F/FT) whose SDAITM prefix is DRY/Dry → extended_price
          .withColumn("dryer_freight_amount",
                      F.when(_frtline & _pref3.isin("DRY", "Dry"), F.col("extended_price").cast("double")).otherwise(F.lit(0.0))))

    return df.select("price_adjustment_key", "sales_order_line_key", *FACT_BUSINESS_COLS)


# ----------------------------------------------------------------------------
# 3) FACT SOURCES
# ----------------------------------------------------------------------------

# spine = F4211 line driver; fanout = F4074 whitelisted adjustments; static = lookup/attribute source.
FACT_SOURCES = [
    {"silver": F4211,  "role": "spine"},
    {"silver": F4074,  "role": "fanout"},
    {"silver": F4101,  "role": "static"},   # IMUOM1 (tons cascade) / IMSEG4
    {"silver": F4201,  "role": "static"},   # header hold code
    {"silver": F41002, "role": "static"},   # item-specific UOM conversion (Tier A)
    {"silver": F41003, "role": "static"},   # standard UOM conversion (Tier B)
    {"silver": F49211, "role": "static"},   # SO-line tag → deferred-entries flag (UDDEFF)
]

# ----------------------------------------------------------------------------
# 4) RUN
# ----------------------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

for _s in FACT_SOURCES:
    print("  source {:<52s} {}".format(_s["silver"],
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
