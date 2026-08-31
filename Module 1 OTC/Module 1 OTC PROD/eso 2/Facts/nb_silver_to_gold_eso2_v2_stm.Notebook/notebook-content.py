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

#!/usr/bin/env python
# coding: utf-8

# ## nb_silver_to_gold_eso2_v2
#
# Fabric notebook · nb_silver_to_gold_eso2_v2
# ============================================================================
# ESO2 v2 Gold fact table.  Schema: `eso2`, lakehouse: `lh_jde_gold`.
#
# 6 CDF streams (one per fact source), each with its own Spark checkpoint
# under Files/checkpoints/eso2_v2/.  A threading.Lock serialises writes so
# concurrent foreachBatch calls never conflict on the Gold fact table.
#
# SOURCE TABLES  (Silver → lh_jde_silver.cdf.*)
#   Spine   : F4211    — sales order detail (LINE_KEYS live here)
#   Fact    : F4201    — sales order header (delivery instructions)
#   Fact    : F0010    — company constants  (INNER gate + MTD columns)
#   Fact    : F0101    — address book master (INNER gate — ABAT1 DQ filter)
#   Fact    : F5549002 — scale ticket detail (weight columns)
#   Fact    : F41002   — UOM conversion factors (TN conversion, folded)
#
# GOLD OUTPUT  (lh_jde_gold.eso2.*)
#   fact_extended_sales_order_2
#
# LINE_KEYS (composite PK of the fact):
#   key_company_order  (F4211.company_key_order_no  / SDKCOO)
#   order_number       (F4211.document_order_invoice_e / SDDOCO)
#   order_type         (F4211.order_type             / SDDCTO)
#   line_number        (F4211.line_number            / SDLNID)
#
# FIRST RUN:    set MANUAL_OVERWRITE = True  → full load + clear checkpoints
# EVERY RESTART: set MANUAL_OVERWRITE = False → all streams resume from
#                checkpoints, catching up any missed changes automatically.
# ============================================================================

import threading
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------
SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "cdf"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "eso2"
CKPT_ROOT     = "Files/checkpoints/eso2_v2"
TRIGGER       = "30 seconds"

# True  = full reload + clear checkpoints.
#         Use on first run OR after any schema / column change.
# False = resume from Spark checkpoints (normal restarts).
MANUAL_OVERWRITE = True

# ---------------------------------------------------------------------------
# Fully-qualified name helpers
# ---------------------------------------------------------------------------
def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,   t)

# ---------------------------------------------------------------------------
# Silver table name constants
# ---------------------------------------------------------------------------
F4211    = "f4211_sales_order_detail_file"
F4201    = "f4201_sales_order_header_file"
F0101    = "f0101_address_book_master"
F0010    = "f0010_company_constants"
F5549002 = "f5549002_mxp_bol_interface_detail"
F41002   = "f41002_item_units_of_measure_conversion_factors"

# ---------------------------------------------------------------------------
# Gold table name constant
# ---------------------------------------------------------------------------
FACT = "fact_extended_sales_order_2"

# ---------------------------------------------------------------------------
# Composite primary key of the fact table.
# Derived from F4211's merge_key in bronze_to_silver_config (snake_case).
# Every Gold row is uniquely identified by these 4 columns.
# These are the GOLD-side aliased names (not raw Silver column names).
# ---------------------------------------------------------------------------
LINE_KEYS = [
    "key_company_order",   # SDKCOO — company_key_order_no
    "order_number",        # SDDOCO — document_order_invoice_e
    "order_type",          # SDDCTO — order_type
    "line_number",         # SDLNID — line_number
]

# ---------------------------------------------------------------------------
# Threading lock — serialises all Gold fact writes.
# foreachBatch handlers for different source tables run in parallel driver
# threads; this lock makes them take turns so no two streams write to the
# Gold fact table simultaneously.
# ---------------------------------------------------------------------------
_FACT_LOCK = threading.Lock()

# ---------------------------------------------------------------------------
# Soft-delete filter — mirrors load_silver() from the original batch notebook.
# Applied inside build_fact() to every Silver snapshot read.
# ---------------------------------------------------------------------------
def drop_deleted(df):
    """Filter out soft-deleted rows — exact match to batch load_silver() logic.
    Keeps only rows where is_delete = 0.
    Rows where is_delete IS NULL are excluded, matching the batch notebook's
    F.col('is_delete') == 0 behaviour exactly."""
    if "is_delete" in df.columns:
        return df.filter(F.col("is_delete") == 0)
    return df

print("CONFIG loaded — GOLD_SCHEMA={} MANUAL_OVERWRITE={}".format(
    GOLD_SCHEMA, MANUAL_OVERWRITE))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# 2) FACT BUILDER
# ----------------------------------------------------------------------------
# build_fact(f4211_df)
#   Takes a subset of F4211 rows (the spine) and produces the corresponding
#   Gold fact rows by joining all other Silver sources.
#
# All logic, join order, derived columns, GROUP BY and SUM are taken verbatim
# from the original batch notebook — nothing changed except the function wrapper.
#
# JOIN CHAIN (exact Hubble / batch query order):
#   F4211 spine
#     INNER F0010    — company constants (INNER gate: SDKCOO = CCCO)
#     INNER F0101    — address book DQ gate (ABAT1 between A-P or R-ZZZ)
#     LEFT  F5549002 — scale ticket weights (4-col join)
#     LEFT  F4201    — order header (delivery instructions, 3-col join)
#     LEFT  F41002   — UOM->TN conversion (bidirectional union, folded on fact)
#
# Derived columns (exact batch logic preserved):
#   conv_factor_to_tn : COALESCE(TN→1.0, F41002 factor, 1.0)
#   quantity_loaded   : FLOOR(units_transaction_qty * conv_factor_to_tn * 100) / 100
#
# Output: inner DISTINCT → outer GROUP BY + SUM  (mirrors Hubble two-query structure)
# ----------------------------------------------------------------------------

def build_fact(f4211_df):
    """Build the Gold fact DataFrame from a spine subset of F4211."""

    # ── Apply soft-delete filter to spine and all sources ────────────────────
    f4211_df = drop_deleted(f4211_df)
    f4201    = drop_deleted(spark.read.table(sname(F4201)))
    f0101    = drop_deleted(spark.read.table(sname(F0101)))
    f0010    = drop_deleted(spark.read.table(sname(F0010)))
    f5549002 = drop_deleted(spark.read.table(sname(F5549002)))
    _f41002  = drop_deleted(spark.read.table(sname(F41002)))

    # ── F0101 DQ gate subquery — ABAT1 filter (mirrors Hubble INNER subquery) ─
    # INNER JOIN (SELECT ABAN8 FROM F0101
    #             WHERE ABAT1 BETWEEN 'A' AND 'P' OR ABAT1 BETWEEN 'R' AND 'ZZZ')
    # ON F4211.SDSHAN = F0101.ABAN8
    # NOTE: ship_to_name (ABALPH) is NOT stored on the fact — resolved via
    #       dim_address_book in Power BI.  DQ gate filter is still applied.
    f0101_dq = (
        f0101
        .filter(
            (F.trim(F.col("address_type_01")).between("A", "P"))
            | (F.trim(F.col("address_type_01")).between("R", "ZZZ"))
        )
        .select(
            F.col("address_number").alias("dq_aban8"),   # ABAN8 — join key only
        )
    )

    # ── F0010 slim — INNER join source ────────────────────────────────────────
    # INNER JOIN F0010 ON F4211.SDKCOO = F0010.CCCO
    # Aliased to avoid column name ambiguity after join.
    f0010_slim = (
        f0010
        .select(
            F.col("company").alias("ccco"),                               # CCCO
            F.col("period_number_current").alias("ccpnc"),                # CCPNC
            F.col("date_ar_fiscal_year_begins_julian").alias("ccarfj"),   # CCARFJ
        )
    )

    # ── F4201 slim — LEFT join source ─────────────────────────────────────────
    # LEFT JOIN F4201 ON SDKCOO=SHKCOO AND SDDOCO=SHDOCO AND SDDCTO=SHDCTO
    # Aliased to avoid column name ambiguity after join.
    f4201_slim = (
        f4201
        .select(
            F.col("company_key_order_no").alias("hdr_kcoo"),              # SHKCOO
            F.col("document_order_invoice_e").alias("hdr_doco"),          # SHDOCO
            F.col("order_type").alias("hdr_dcto"),                        # SHDCTO
            F.col("delivery_instruct_line_01").alias("delivery_instructions"),  # SHDEL1
        )
    )

    # ── F5549002 slim — LEFT join source ──────────────────────────────────────
    # LEFT JOIN F5549002 ON SDKCOO=MIKCOO AND SDDOCO=MIDOCO
    #                    AND SDDCTO=MIDCTO AND SDLNID=MILNID
    # Aliased to avoid column name ambiguity after join.
    scale = (
        f5549002
        .select(
            F.col("company_key_order_no").alias("scale_company_key_order_no"),
            F.col("document_order_invoice_e").alias("scale_order_number"),
            F.col("order_type").alias("scale_order_type"),
            F.col("line_number").alias("scale_line_number"),
            F.col("gross_weight").alias("scale_gross_weight"),   # MIGRWT
            F.col("catch_weight").alias("scale_tare_weight"),    # MICTWT
            F.col("maximum_weight").alias("scale_net_weight"),   # MIMXWT
        )
    )

    # ── F41002 bidirectional UOM->TN conversion ───────────────────────────────
    # Forward : UMRUM = 'TN' → factor converts from_uom -> TN directly
    # Reverse : UMUM  = 'TN' → factor stored inverted; flip it
    df_conv_fwd = (
        _f41002
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
    df_conv_rev = (
        _f41002
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
    df_conv = (
        df_conv_fwd
        .unionByName(df_conv_rev)
        .dropDuplicates(["conv_itm", "conv_from_uom"])
    )

    # ── Join chain (exact Hubble join order and types) ────────────────────────
    df_joined = (
        f4211_df.alias("sd")

        # JOIN 1: F0010 — INNER (company constants gate)
        .join(
            f0010_slim.alias("co"),
            F.col("sd.company_key_order_no") == F.col("co.ccco"),
            "inner",
        )

        # JOIN 2: F0101 — INNER (ABAT1 DQ gate on ship-to address)
        .join(
            f0101_dq.alias("ab"),
            F.col("sd.address_number_ship_to") == F.col("ab.dq_aban8"),
            "inner",
        )

        # JOIN 3: F5549002 — LEFT (scale ticket weights, 4-col join)
        .join(
            scale.alias("scale"),
            (F.col("sd.company_key_order_no")      == F.col("scale.scale_company_key_order_no"))
            & (F.col("sd.document_order_invoice_e") == F.col("scale.scale_order_number"))
            & (F.col("sd.order_type")               == F.col("scale.scale_order_type"))
            & (F.col("sd.line_number")              == F.col("scale.scale_line_number")),
            "left",
        )

        # JOIN 4: F4201 — LEFT (order header: delivery instructions, 3-col join)
        .join(
            f4201_slim.alias("hdr"),
            (F.col("sd.company_key_order_no")      == F.col("hdr.hdr_kcoo"))
            & (F.col("sd.document_order_invoice_e") == F.col("hdr.hdr_doco"))
            & (F.col("sd.order_type")               == F.col("hdr.hdr_dcto")),
            "left",
        )

        # JOIN 5: F41002 — LEFT (UOM->TN conversion, folded on fact)
        .join(
            df_conv.alias("ci"),
            (F.col("sd.identifier_short_item")  == F.col("ci.conv_itm"))
            & (F.trim(F.col("sd.uom_as_input")) == F.col("ci.conv_from_uom")),
            "left",
        )
    )

    # ── Derived columns (exact batch logic) ───────────────────────────────────
    # conv_factor_to_tn:
    #   1. If SDUOM already 'TN' → 1.0
    #   2. F41002 has a factor   → use it
    #   3. No factor found       → 1.0
    # quantity_loaded: FLOOR(units_transaction_qty * conv_factor_to_tn * 100) / 100
    df_derived = (
        df_joined
        .withColumn(
            "conv_factor_to_tn",
            F.coalesce(
                F.when(F.trim(F.col("sd.uom_as_input")) == "TN", F.lit(1.0)),
                F.col("ci.conv_factor"),
                F.lit(1.0),
            ),
        )
        .withColumn(
            "quantity_loaded",
            F.floor(
                F.col("sd.units_transaction_qty").cast("double")
                * F.col("conv_factor_to_tn")
                * 100
            ) / 100,
        )
    )

    # ── Inner SELECT DISTINCT (Hubble inner subquery) ─────────────────────────
    # Column mapping preserved exactly from the batch notebook.
    # MTD filters (ship month = current period, ship year = fiscal year)
    # are intentionally left commented out — same as the batch notebook —
    # so that full history is stored in the Gold fact table.
    df_inner = (
        df_derived
        .select(

            # ── Grain keys (LINE_KEYS) ────────────────────────────────────────
            F.col("sd.company_key_order_no").alias("key_company_order"),        # SDKCOO
            F.col("sd.document_order_invoice_e").alias("order_number"),         # SDDOCO
            F.col("sd.order_type").alias("order_type"),                         # SDDCTO
            F.col("sd.line_number").alias("line_number"),                       # SDLNID

            # ── Visual columns ────────────────────────────────────────────────
            F.col("sd.actual_ship_date").alias("actual_ship_date"),             # SDADDJ
            # ship_to_name (ABALPH) → resolved via dim_address_book in Power BI
            F.col("sd.address_number_ship_to").alias("ship_to"),                # SDSHAN
            F.col("sd.reference_01").alias("customer_po_number"),               # SDVR01
            F.col("sd.description_line_01").alias("item_description"),          # SDDSC1
            F.col("sd.user_reserved_number").alias("bol_number"),               # SDURAB
            F.col("sd.pull_signal").alias("alt_bol_number"),                    # SDPSIG
            F.col("sd.doc_voucher_invoice_e").alias("invoice_number"),          # SDDOC
            F.col("sd.container_id").alias("vehicle_number"),                   # SDCNID
            F.col("sd.reference_02_vendor").alias("well_name"),                 # SDVR02
            F.col("sd.uom_as_input").alias("transactional_uom"),                # SDUOM
            F.col("scale.scale_gross_weight").alias("gross_weight"),            # MIGRWT
            F.col("scale.scale_tare_weight").alias("tare_weight"),              # MICTWT
            F.col("scale.scale_net_weight").alias("net_weight"),                # MIMXWT
            F.col("quantity_loaded"),                                           # Derived
            F.col("hdr.delivery_instructions").alias("delivery_instructions"),  # SHDEL1

            # ── Measure columns (SUMmed in outer GROUP BY) ────────────────────
            F.col("sd.units_transaction_qty").alias("quantity_shipped_uom"),       # SDUORG
            F.col("sd.units_primary_qty_order").alias("quantity_ordered_primary"), # SDPQOR

            # ── F0010 MTD reference columns ───────────────────────────────────
            F.col("co.ccco").alias("company"),                                  # CCCO
            F.col("co.ccpnc").alias("current_period_number"),                   # CCPNC
            F.col("co.ccarfj").alias("fiscal_year_begin_date"),                 # CCARFJ

            # ── Scale ticket line reference ───────────────────────────────────
            F.col("scale.scale_line_number").alias("scale_line_number"),        # MILNID

            # ── Page Filter columns ───────────────────────────────────────────
            F.col("sd.status_code_last").alias("last_status"),                  # SDLTTR
            F.col("sd.status_code_next").alias("next_status"),                  # SDNXTR
            F.col("sd.original_order_type").alias("original_order_type"),       # SDOCTO
            F.col("sd.document_order_invoice_e").alias("load_number"),          # SDDOCO
            F.col("sd.line_type").alias("line_type"),                           # SDLNTY
            F.col("sd.identifier_second_item").alias("item_number"),            # SDLITM
            F.col("sd.cost_center").alias("plant"),                             # SDMCU
            F.col("sd.date_requested_julian").alias("requested_date"),          # SDDRQJ
            F.col("sd.date_invoice_julian").alias("invoice_date"),              # SDIVD
            F.col("sd.dt_for_gl_and_vouch_01").alias("gl_date"),               # SDDGL
            F.col("sd.address_number_parent").alias("parent_number"),           # SDPA8
        )
        # DISTINCT — exact Hubble inner subquery behaviour
        .distinct()
    )

    # ── Outer GROUP BY + SUM (Hubble outer query) ─────────────────────────────
    # GROUP BY all non-measure columns.
    # SUM quantity_shipped_uom and quantity_ordered_primary.
    # quantity_loaded is deterministic per grain row → included in GROUP BY.
    GROUP_BY_COLS = [
        "key_company_order", "order_number", "order_type", "line_number",
        "actual_ship_date", "ship_to", "customer_po_number", "item_description",
        "bol_number", "alt_bol_number", "invoice_number", "vehicle_number",
        "well_name", "transactional_uom", "gross_weight", "tare_weight",
        "net_weight", "quantity_loaded", "delivery_instructions",
        "company", "current_period_number", "fiscal_year_begin_date",
        "scale_line_number",
        "last_status", "next_status", "original_order_type", "load_number",
        "line_type", "item_number", "plant", "requested_date", "invoice_date",
        "gl_date", "parent_number",
    ]

    return (
        df_inner
        .groupBy(GROUP_BY_COLS)
        .agg(
            F.sum("quantity_shipped_uom").alias("quantity_shipped_uom"),
            F.sum("quantity_ordered_primary").alias("quantity_ordered_primary"),
        )
    )

print("build_fact defined")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# 3) FACT SOURCES
# ----------------------------------------------------------------------------
# FACT_SOURCES drives two things:
#   1. Which Silver tables get a CDF stream.
#   2. How affected_lines() traces changes in that source back to LINE_KEYS
#      in the Gold fact.
#
# CRITICAL RULE — join_pairs mirrors the build_fact JOIN CONDITION for that
# source to F4211, NOT the source table's full merge_key.
#
# Source          join_pairs                              Why
# -----------------------------------------------------------------------
# F4211 (spine)   all 4 LINE_KEYS mapped to Gold aliases  Spine IS F4211
# F4201 (header)  3-col header join (KCOO+DOCO+DCTO)      build_fact LEFT join
# F0010 (company) company_key_order_no -> key_company_order  1-col INNER join
# F0101 (addr)    address_number_ship_to -> ship_to        1-col INNER join
# F5549002 (scale)all 4 LINE_KEYS                          build_fact 4-col join
# F41002 (UOM)    identifier_short_item + uom -> item_number + transactional_uom
#                 cost_centre EXCLUDED (not a join col — would under-scope);
#                 related_uom EXCLUDED (filter only, not a join column)
#
# NOTE: join_pairs source col  = raw Silver column name (as it arrives in CDF)
#       join_pairs fact col    = Gold alias (as stored in LINE_KEYS / Gold fact)
# ----------------------------------------------------------------------------

FACT_SOURCES = [
    # ── Spine — F4211 ─────────────────────────────────────────────────────────
    {
        "silver": F4211,
        "join_pairs": [
            ("company_key_order_no",     "key_company_order"),
            ("document_order_invoice_e", "order_number"),
            ("order_type",               "order_type"),
            ("line_number",              "line_number"),
        ],
    },
    # ── F4201 — order header (3-col LEFT join) ─────────────────────────────────
    {
        "silver": F4201,
        "join_pairs": [
            ("company_key_order_no",     "key_company_order"),
            ("document_order_invoice_e", "order_number"),
            ("order_type",               "order_type"),
        ],
    },
    # ── F0010 — company constants (1-col INNER join) ───────────────────────────
    {
        "silver": F0010,
        "join_pairs": [
            ("company", "key_company_order"),
        ],
    },
    # ── F0101 — address book DQ gate (1-col INNER join on ship-to) ────────────
    {
        "silver": F0101,
        "join_pairs": [
            ("address_number", "ship_to"),
        ],
    },
    # ── F5549002 — scale ticket (4-col LEFT join, all LINE_KEYS) ──────────────
    {
        "silver": F5549002,
        "join_pairs": [
            ("company_key_order_no",     "key_company_order"),
            ("document_order_invoice_e", "order_number"),
            ("order_type",               "order_type"),
            ("line_number",              "line_number"),
        ],
    },
    # ── F41002 — UOM conversion (2-col LEFT join only) ────────────────────────
    # cost_centre excluded: build_fact does not filter by plant in the join
    #   → including it would under-scope affected rows and miss Gold updates.
    # related_uom excluded: it is a pre-join filter (UMRUM='TN'), not a join col.
    {
        "silver": F41002,
        "join_pairs": [
            ("identifier_short_item", "item_number"),
            ("uom",                   "transactional_uom"),
        ],
    },
]


def current_version(src):
    """Return the current (latest) Delta table version for a Silver source."""
    return (
        spark.sql("DESCRIBE HISTORY {}".format(sname(src)))
        .select(F.max("version"))
        .first()[0]
    )


def affected_lines(change_keys, join_pairs):
    """Find all LINE_KEYS in the Gold fact affected by a source change.

    change_keys : DataFrame of changed source-side key values (from CDF batch).
    join_pairs  : list of (source_col, fact_col) tuples — mirrors the
                  build_fact join condition for this source against F4211.

    Reads directly from the Gold fact (which already holds the Gold alias
    column names) to find which LINE_KEYS are affected.  This avoids having
    to re-alias Silver column names here and keeps the logic consistent with
    what is actually stored in Gold.
    """
    try:
        fact_snap = (
            spark.read.table(gname(FACT))
            .select(*LINE_KEYS)
            .alias("f")
        )
    except Exception:
        # Gold fact does not exist yet (first run before full load) — skip.
        return spark.createDataFrame(
            [],
            schema=(
                "key_company_order STRING, order_number STRING, "
                "order_type STRING, line_number DECIMAL"
            ),
        )
    c    = change_keys.alias("c")
    cond = None
    for scol, fcol in join_pairs:
        eq   = F.col("c.{}".format(scol)) == F.col("f.{}".format(fcol))
        cond = eq if cond is None else (cond & eq)
    return (
        fact_snap
        .join(c, cond, "inner")
        .select(*[F.col("f.{}".format(k)) for k in LINE_KEYS])
        .distinct()
    )

print("{} fact sources defined".format(len(FACT_SOURCES)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# 4) RECOMPUTE  (called only while holding _FACT_LOCK)
# ----------------------------------------------------------------------------
# recompute_fact(lines)
#   Takes a DataFrame of affected LINE_KEYS and refreshes exactly those rows
#   in the Gold fact table.
#
#   Flow:
#     1. Read F4211 spine, INNER join to lines   → narrow to affected rows only
#     2. Call build_fact() on that subset         → produces new correct Gold rows
#     3. MERGE DELETE from Gold all rows matching the affected LINE_KEYS
#     4. APPEND new rows from build_fact()
#
#   If build_fact() returns zero rows (e.g. the INNER gate on F0010 or F0101
#   now fails because a source row was deleted), those lines are removed from
#   Gold and nothing is appended — correct disappearance behaviour.
#
# NOTE on column name round-trip:
#   build_fact() expects raw F4211 Silver column names (company_key_order_no,
#   document_order_invoice_e, etc.).  LINE_KEYS use Gold aliases
#   (key_company_order, order_number, etc.).
#   order_type and line_number are identical in both Silver and Gold — no rename.
# ----------------------------------------------------------------------------

def recompute_fact(lines):
    """Delete and recompute Gold fact rows for the supplied LINE_KEYS."""
    if lines.rdd.isEmpty():
        return

    # Rename the two Gold alias columns back to Silver names so build_fact
    # can resolve them in its join conditions and column references.
    spine = (
        spark.read.table(sname(F4211))
        .withColumnRenamed("company_key_order_no",     "key_company_order")
        .withColumnRenamed("document_order_invoice_e", "order_number")
        .join(lines, LINE_KEYS, "inner")
        # Restore original Silver column names before passing to build_fact
        .withColumnRenamed("key_company_order", "company_key_order_no")
        .withColumnRenamed("order_number",       "document_order_invoice_e")
    )

    new_rows = build_fact(spine)

    dt        = DeltaTable.forName(spark, gname(FACT))
    line_cond = " AND ".join(
        ["t.{0} = s.{0}".format(k) for k in LINE_KEYS]
    )

    # Step 1: delete old Gold rows for the affected LINE_KEYS
    dt.alias("t").merge(lines.alias("s"), line_cond).whenMatchedDelete().execute()

    # Step 2: append freshly computed rows (no-op if build_fact returned empty)
    if not new_rows.rdd.isEmpty():
        (new_rows.write
         .format("delta")
         .mode("append")
         .saveAsTable(gname(FACT)))

print("recompute_fact defined")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# 5) STREAM BATCH HANDLER
# ----------------------------------------------------------------------------
# make_fact_handler(cfg, init_ver)
#   Factory that returns the foreachBatch handler for one FACT_SOURCES entry.
#
# init_ver: Delta version captured at full-load time for this source.
#   _commit_version <= init_ver  → seed data already in Gold → skip.
#   init_ver = -1 (resume path) → no version filtering, checkpoint drives offset.
#
# Handler flow per micro-batch:
#   1. Fast exit if batch is empty (common on quiet 30-s windows).
#   2. Skip seed rows on the first batch after a full load.
#   3. Collect distinct changed source keys (insert + update_postimage + delete).
#      update_preimage excluded — LINE_KEY columns are never updated in JDE.
#   4. affected_lines() traces those source keys to Gold LINE_KEYS.
#   5. Acquire _FACT_LOCK → recompute_fact() deletes old rows + appends new.
# ----------------------------------------------------------------------------

def make_fact_handler(cfg, init_ver):
    """Return a foreachBatch handler for one FACT_SOURCES entry."""
    src        = cfg["silver"]
    join_pairs = cfg["join_pairs"]
    src_keys   = [p[0] for p in join_pairs]   # source-side column names

    def handler(batch_df, batch_id):
        # ── 1. Fast exit on empty batch ───────────────────────────────────────
        if batch_df.rdd.isEmpty():
            return

        # ── 2. Skip seed data on the first batch after a full load ────────────
        if init_ver >= 0:
            batch_df = batch_df.filter(F.col("_commit_version") > init_ver)
        if batch_df.rdd.isEmpty():
            return

        # ── 3. Collect changed source keys ────────────────────────────────────
        changed = (
            batch_df
            .filter(F.col("_change_type").isin(
                "insert", "update_postimage", "delete"))
            .select(*src_keys)
            .distinct()
        )

        # ── 4. Trace to Gold LINE_KEYS ────────────────────────────────────────
        lines = affected_lines(changed, join_pairs)
        if lines.rdd.isEmpty():
            return

        # ── 5. Serialised Gold write ──────────────────────────────────────────
        with _FACT_LOCK:
            print("[{}] batch={} {} line key(s)".format(
                src[:14], batch_id, lines.count()))
            recompute_fact(lines)

    return handler

# ── is_spine guard — DISABLED ────────────────────────────────────────────────
# LINE_KEY columns (key_company_order, order_number, order_type, line_number)
# are confirmed to NEVER be updated in JDE source.  If they were, CDF would emit
# update_preimage (old identity) which the handler above ignores, leaving an
# orphan row in Gold.  The guard would detect and delete those old identities.
# Disabled because the scenario does not occur in production.
# See nb_silver_to_gold_eso7_v2_fact.py for the full guard reference block.

print("make_fact_handler defined")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------------------------------------------------------
# 6) RUN
# ----------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 6.1  Ensure Gold schema exists
# ---------------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))
print("Gold schema ensured: {}.{}".format(GOLD_LH, GOLD_SCHEMA))

# ---------------------------------------------------------------------------
# 6.2  Stop any leftover streams from a previous cell run in this Spark session
# ---------------------------------------------------------------------------
# Stopping a notebook cell does NOT stop Spark streaming queries — they survive
# until the Spark session is restarted or explicitly stopped here.
# In SJD mode each execution gets a brand-new session so spark.streams.active
# is always empty — this block is a harmless no-op in that context.
_our_names = {"fact__" + cfg["silver"] for cfg in FACT_SOURCES}
_stopped   = []
for _q in list(spark.streams.active):
    if _q.name in _our_names:
        _q.stop()
        _stopped.append(_q.name)
if _stopped:
    print("Stopped leftover streams: {}".format(_stopped))
else:
    print("No leftover streams to stop.")

# ---------------------------------------------------------------------------
# 6.3  Checkpoint existence check
# ---------------------------------------------------------------------------
def _checkpoints_exist():
    """Return True if the checkpoint root directory exists and is non-empty."""
    try:
        return bool(mssparkutils.fs.ls(CKPT_ROOT))
    except Exception:
        return False

# ---------------------------------------------------------------------------
# 6.4  Full load vs resume decision
# ---------------------------------------------------------------------------
_needs_full_load = (
    MANUAL_OVERWRITE
    or not spark.catalog.tableExists(gname(FACT))
    or not _checkpoints_exist()
)

if _needs_full_load:
    # ── FULL LOAD ─────────────────────────────────────────────────────────────
    print("== FULL LOAD ==")

    # build_fact reads fresh snapshots of all Silver sources internally.
    # The full F4211 spine is passed — no LINE_KEY scoping on the initial load.
    new_fact = build_fact(spark.read.table(sname(F4211)))

    (new_fact.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .option("delta.enableChangeDataFeed", "true")
     .saveAsTable(gname(FACT)))

    print("  {} rows={}".format(FACT, new_fact.count()))

    # Capture the current Delta version for every source table.
    # Streams start from startingVersion=init_ver; the first micro-batch
    # contains only the seed data (already in Gold) which the handler skips
    # via _commit_version > init_ver.  Only genuine new commits flow after that.
    _all_src  = [cfg["silver"] for cfg in FACT_SOURCES]
    _init_ver = {src: current_version(src) for src in _all_src}
    print("  init versions: {}".format(_init_ver))

    # Clear checkpoints so streams start cleanly from init_ver offsets.
    # Without this, a stale checkpoint from a previous run would cause the
    # stream to skip events between the checkpoint offset and init_ver.
    try:
        mssparkutils.fs.rm(CKPT_ROOT, True)
        print("  checkpoints cleared")
    except Exception as _e:
        print("  checkpoint clear skipped: {}".format(_e))

else:
    # ── RESUME FROM CHECKPOINT ────────────────────────────────────────────────
    print("== resuming from checkpoint ==")
    # Empty _init_ver → handlers use init_ver=-1 → no version filtering.
    # Spark reads checkpoint files and resumes from the last processed offset.
    _init_ver = {}

# ---------------------------------------------------------------------------
# 6.5  Start all 6 fact source streams
# ---------------------------------------------------------------------------
print("== starting {} streams (trigger={}) ==".format(len(FACT_SOURCES), TRIGGER))

for cfg in FACT_SOURCES:
    src = cfg["silver"]
    iv  = _init_ver.get(src, -1)
    sv  = iv if iv >= 0 else 0    # startingVersion must be >= 0

    (spark.readStream
          .format("delta")
          .option("readChangeFeed",  "true")
          .option("startingVersion", sv)
          .table(sname(src))
     .writeStream
          .foreachBatch(make_fact_handler(cfg, iv))
          .option("checkpointLocation",
                  "{}/fact__{}".format(CKPT_ROOT, src))
          .trigger(processingTime=TRIGGER)
          .queryName("fact__" + src)
          .start())

    print("  fact__{}  startingVersion={}  init_ver={}".format(src, sv, iv))

# ---------------------------------------------------------------------------
# 6.6  Block until any stream terminates (error or manual stop)
# ---------------------------------------------------------------------------
print("== all {} streams running — awaiting termination ==".format(
    len(FACT_SOURCES)))
spark.streams.awaitAnyTermination()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
