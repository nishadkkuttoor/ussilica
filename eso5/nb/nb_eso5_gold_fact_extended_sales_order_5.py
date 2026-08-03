#!/usr/bin/env python
# coding: utf-8

# ## nb_eso5_gold_fact_extended_sales_order_5
#
# **Gold `fact_extended_sales_order_5` processor** for Extended Sales Order 5 (Sandbox Load Report with
# PO Details). Builds and continuously refreshes ONE table — `lh_jde_gold.rpt.fact_extended_sales_order_5` —
# that serves the CORE report AND all FOUR Filter-Capture variations. There is deliberately NO second
# fact table.
#
# ── THE FIVE REPORTS AND HOW ONE FACT SERVES THEM ──────────────────────────────────────────────
# 1. hubble query.txt — "Sandbox Load Report with PO Details" (CORE)
#      SBXLOADPOVIEW (F4211 SX / 00750 / <>TL / <>HOLADD) LEFT F554201T, SBXUSSSAND, F0101.
# 2. Filter Capture/"...PO Details (New)"  — IDENTICAL query to the core (only the §5 date range
#      differs, and §5 filters are not applied here). Needs nothing extra.
# 3. Filter Capture/"...PO Details"        — a strict COLUMN SUBSET of the core with no SBXUSSSAND
#      join; its SDLITM='FRT' / SDNXTR<'581' are §5 report filters. Needs nothing extra.
# 4. Filter Capture/"...PO Details for HOLADD" — NEW ROWS. Its SBXLOADPOVIEWHOLADD view is a UNION of
#      (a) the F4211 SX lines where SDLITM = 'HOLADD'  — precisely the rows the core view EXCLUDES, and
#      (b) ORPHAN F4311 OX HOLADD purchase-order lines: PO lines with NO live (SDLTTR<>'980') SX HOLADD
#          line on the same load, back-filled from the load's FRT line (VR01/AN8/LOFA/GLDATE/LTTR/
#          NXTR/DOC) with UPRC=0, EXTAMT=0 and OXAMT/OXLTTR/OXNXTR taken from the PO line itself.
#      ⚠ That "NO live (SDLTTR<>'980')" test is a STATUS filter, so it is NOT applied here — ALL OX HOLADD
#          PO lines become rows and the test becomes the field `po_holadd_superseded` ('Y'/'N').
#          The orphan set = `po_holadd_superseded = 'N'`, which the report filters on.
# 5. Filter Capture/"SBX Load Reconciliation Report" — LOAD grain. Its SBXLOADDETAIL view is
#      F4201 INNER F4211 aggregated to one row per load, pivoting the lines into SANDWEIGHT /
#      EXTWEIGHT / MILES / LOFADET / WELLDET / ...PP / ...PB / FRTAMT / FSCAMT / SANDAMT / HOLAMT etc.
#      It KEEPS the TL and HOLADD lines the core view drops, and reads four more F554201T columns
#      (QCLGL1/2/3, QCFSTR3) plus the F4201 header attributes.
#
# ── FILTERS: ZERO. NOT ONE.───────────────────────────────────────
# The Gold layer applies NO report filter of any kind. There is no row-selecting predicate anywhere in
# this notebook: the fact is the WHOLE of Silver F4211 UNION the WHOLE of Silver F4311, and EVERY filter
# the five queries use is carried as a COLUMN for the Power BI report to filter on.
#   F4211 leg  — no WHERE at all:
#      SDDCTO='SX'        → the `document_type` column     (report filter)
#      SDKCOO='00750'     → the `company` column           (report filter)
#      SDLNTY<>'TL'       → row_class 'TEXT'               (report filter)
#      SDLITM<>'HOLADD'   → row_class 'HOLADD'             (report filter)
#   F4311 leg  — no WHERE at all (PO_LEG_UNFILTERED=True):
#      PDDCTO='OX', PDKCOO='00750', PDLITM='HOLADD'        → row_class 'PO_HOLADD' / 'PO_OTHER'
#      NOT EXISTS(live SX HOLADD)                          → the `po_holadd_superseded` field
#   Per-instance report slicers — never were applied:
#      ORDATE date range (all 5), SDLITM='FRT' + SDNXTR<'581' (base variation),
#      Reconciliation SDDOCO='22815083' and SDLTTR<>'980'  → the report owns these
#   The rate-source band F0101 `ABAT1 BETWEEN 'A '..'P ' OR 'R '..'ZZZ'` is a VALUE DEFINITION, not a
#      filter — it says which address rows count as a rate source. It lives in a CASE inside the `lofa`
#      aggregate, so it drops no row and an out-of-band facility gets a NULL rate.
# ⚠ The five queries' WHERE clauses CONFLICT anyway (reports 1-3 drop TL & HOLADD lines; report 4
#   REQUIRES HOLADD; report 5 KEEPS both). That conflict is the structural reason ONE fact can serve five
#   reports only if the discrimination happens at REPORT level. `row_class` is how it does.
#
# ── AND NO STATUS FILTERING AT ALL────────────────────────────────
# NOT ONE status_code / last-status / next-status predicate filters a row anywhere in this notebook. The
# status JOINS are all implemented and every status FIELD is stored, so the report owns the filtering:
#      stored: last_status (SDLTTR) · next_status (SDNXTR) · ox_last_status (PDLTTR) · ox_next_status
#              (PDNXTR) · load_last_status (recon CASE) · load_max_last_status (MXLTTR) ·
#              load_min_last_status (MILTTR) · po_holadd_superseded · ox_amount_gross
#      dropped: SDNXTR<'581', SDLTTR NOT IN ('980')   (report slicers)
#      converted from FILTER → FIELD:
#        · report 4's `NOT EXISTS(live SX HOLADD, SDLTTR<>'980')`  → `po_holadd_superseded` ('Y'/'N');
#          every OX HOLADD PO line is now a row, and the orphan set is `po_holadd_superseded='N'`.
#        · SXWEIGHT's `WHERE SDLTTR<>'980'` → a CONDITIONAL SUM (`SUM(CASE WHEN status<>'980' …)`), so the
#          status decides whether a line CONTRIBUTES to the value, not whether the row exists.
# Status inside a CASE (load_last_status, ox_amount) is a CALCULATION, not a filter — it drops no rows —
# so it stays; but its inputs (load_max/min_last_status, ox_amount_gross) are stored too, so the report
# can re-derive or override any of it.
#
# ⚠ WHAT IS *NOT* A FILTER — and therefore STAYS. A predicate inside a correlated subquery is the
#   DEFINITION of the value it computes, not a row filter. `MAX(SDDSC1) WHERE SDLITM='BOL'` is what BOL
#   *is*; `PDDCTO='OX'` is what makes the PO lookup a PO lookup; `SDDCTO='SO' AND SDCO='00400'` is the
#   SO-match; `SDPRP1='COM'` is what SXWEIGHT sums. Strip these and the columns do not become unfiltered —
#   they become WRONG. They are business logic and are kept exactly as the SQL states them.
#
# ── HOW THE FIVE ARE UNIFIED ───────────────────────────────────────────────────────────────────
# The fact stays at LINE grain and its row population is what the five queries draw from:
#      F4211 SX / 00750 lines (order-type + company APPLIED; TL / HOLADD kept, tagged by row_class)
#    + the WHOLE of F4311     (no order-type / item predicate — PO_LEG_UNFILTERED)
# Each row carries `row_class` — five MUTUALLY EXCLUSIVE classes, all CALCULATED, none filtered:
#      'LINE'      F4211 line, <>TL, <>HOLADD      → reports 1, 2, 3   and 5
#      'HOLADD'    F4211 line, <>TL,  =HOLADD      → report  4         and 5
#      'TEXT'      F4211 line,  =TL                →                       5
#      'PO_HOLADD' F4311 line, OX + HOLADD         → report  4
#      'PO_OTHER'  every other F4311 line          → NO report reads it (carried for symmetry only)
#   ALL reports  page filter : document_type='SX' AND company='00750'   [+ any status slicer they want]
#   report 1/2/3 page filter : ... AND row_class = 'LINE'
#   report 4     page filter : ... AND row_class IN ('HOLADD','PO_HOLADD')
#                                  AND po_holadd_superseded = 'N'
#   report 5     page filter : ... AND row_class IN ('LINE','HOLADD','TEXT')
# ⚠ THE PAGE FILTER IS NOT OPTIONAL. The fact is a strict SUPERSET of every report — F4211(SX/00750) ∪ the
#   WHOLE F4311. Without the filter the core report's SUM(total_amount) is not "slightly off": it is the sum
#   of every SX/00750 sales line AND every purchase order line in the fact.
# ⚠ NOTE `document_type` is forced to 'SX' on PO rows (the query hard-codes it so the UNION aligns with the
#   sales load), so `document_type='SX'` ALONE does NOT exclude purchase-order rows — `row_class` is what
#   separates them. The PO's own type is in `po_order_type`.
# Report 5's per-load pivots are DAX measures over these same lines (SUM/MAX with a row filter on
# item_number / product_category / sales_report_code_01) — that is why the line-level pivot inputs
# (units_ordered, item_weight, product_category, sales_report_code_01, line_type) are now stored.
#
# ── BUILD (BATCH) ──
#   • read the full Silver snapshot of every source, run build_fact() ONCE, overwrite the fact.
#   • MANUAL_OVERWRITE = True → drop + rebuild; False → build only if the fact is missing (re-run to refresh).
#   • ALL sources (F4211, F4311, F554201T, F0911, F43121, F0101, F4201) are read as STATIC snapshots —
#     no CDF / foreachBatch / checkpoints / streams. Results are IDENTICAL to the previous streaming
#     version's full-load seed (build_fact() is unchanged; only the incremental scaffolding was removed).
#
# JOINS (all LEFT/Outer):
#   SBXLOADPOVIEW LEFT F554201T   : SDKCOO=QCKCOO, SDDOCO=QCDOCO, SDDCTO=QCDCTO
#   SBXLOADPOVIEW LEFT SBXUSSSAND : SDDOCO=SDDOCO, SDDCTO=SDDCTO
#   SBXLOADPOVIEW LEFT F0101(LOFA): LOFA (SDVEND) = ABAN8
#   + SBXLOADDETAIL LEFT F4201    : SHKCOO=SDKCOO, SHDOCO=SDDOCO, SHDCTO=SDDCTO   (report 5 header)
#
# STAR SCHEMA: fact stores address FK codes (sold_to / ship_to / carrier / loading_facility). Names
# resolve through the REUSED rpt.dim_address_book role views; the USS/plant flags (uss_plant_sand /
# shipped_from / lofa_mcu) resolve through dim_uss_plant (F0005 55/UP, keyed by vendor=loading_facility)
# — JOINED IN THE SEMANTIC MODEL, not here. Only ABURAT (rate) stays read from F0101.
# NO DIRECT F0005 DEPENDENCY: Silver F0005 is never read here. The one build-time value still needed —
# the vendor's plant MCU (DRSPHD), an input to the SBXUSSSAND SOORDERNO row-level match — is read from
# the GOLD dim `dim_uss_plant`.  => run nb_eso5_gold_dim_uss_plant FIRST.
# Calculations = N/A.

# ----------------------------------------------------------------------------
# 1) CONFIG
# ----------------------------------------------------------------------------

# CONFIG + CONSTANTS
from pyspark.sql import functions as F
import json, time
from datetime import datetime, timezone

SILVER_LH     = "lh_jde_silver"
SILVER_SCHEMA = "jde"
GOLD_LH       = "lh_jde_gold"
GOLD_SCHEMA   = "rpt"

# ── refresh / runtime config (BATCH build) ──
MANUAL_OVERWRITE = True   # True = drop + rebuild from the full Silver snapshot; False = build only if missing

def sname(t): return "{}.{}.{}".format(SILVER_LH, SILVER_SCHEMA, t)
def gname(t): return "{}.{}.{}".format(GOLD_LH,   GOLD_SCHEMA,  t)

# TRUE ⇒ the notebook contains NO row-selecting predicate ANYWHERE: leg B takes the WHOLE of F4311, not
# just the `PDDCTO='OX' AND PDLITM='HOLADD'` lines report 4 consumes. Those two conditions become the
# `row_class` calculation instead ('PO_HOLADD' vs 'PO_OTHER'), and `item_number` (PDLITM) + `po_order_type`
# (PDDCTO) are on every PO row, so the report filters to OX/HOLADD itself.
# ⚠ VOLUME: the fact is then F4211 ∪ F4311 in full. The 'PO_OTHER' rows are carried for filtering symmetry;
#   none of the five reports reads them. Set False to restore the OX/HOLADD predicate.
PO_LEG_UNFILTERED = True

# ── report scaling ──────────────────────────────────────────────────────────────
# RAW JDE integers carry implied decimals: qty /1000 (3 implied dec), rate ABURAT*0.01, and — in the HOLADD
# variation only — SDUPRC/1000000 and SDAEXP/100. Those divisors are all implied-decimal decoding, which
# SILVER HAS ALREADY DONE, so they all drop out here and the two queries' scaling agrees. Only the
# BUSINESS factors survive: qty(COM) = units / 2000 (tons).
RATE_FACTOR     = 1.0     # ABURAT already decoded
TONS_DIVISOR    = 2000.0  # COM lines: units → tons

# ── Silver sources ──────────────────────────────────────────────────────────────
F4211    = "f4211_sales_order_detail_file"                    # snapshot (the load/order-line driver)
F4311    = "f4311_purchase_order_detail_file"                 # snapshot (OX status/amount + PO_HOLADD rows)
F554201T = "f554201t_sand_box_sales_order_qc_information"     # static (Sand PO Number / QC / legs)
F0911    = "f0911_account_ledger"                             # static (Carrier PO GL Post flag)
F43121   = "f43121_purchase_order_receiver_file"              # static (PO Receipt GL Date / GLPost doc)
F0101    = "f0101_address_book_master"                        # static — LOFA rate (ABURAT) only
F4201    = "f4201_sales_order_header_file"                    # static — report-5 header attrs (SHMCU/SHAN8/…)
# NOTE: F0005 is NOT read here. Its 55/UP attributes live in the Gold dim below.

# ── Gold READ (prerequisite dim — built by nb_eso5_gold_dim_uss_plant) ───────────
DIM_USS_PLANT = "dim_uss_plant"

# ── Gold target BUILT here ──────────────────────────────────────────────────────
FACT          = "fact_extended_sales_order_5"

print(f"ESO5 Gold fact processor (batch build) — target {gname(FACT)}")

# HELPERS
_SOFT_DELETE_COLS = ["is_delete", "deleted_date_time"]

# The JDE alias → snake_case column-name map is NOT literal — do not transliterate the JDE alias. The ones
# that caught me out: SDITWT=amount_unit_weight, SDSRP1=sales_reporting_code_01, PDUOM/SDUOM=uom_as_input,
# QCLGL1/2/3=descriptn_01/02/03, QCFSTR3=future_use_string_03.
def load_silver_table(table_name):
    df = spark.read.table(sname(table_name))
    if "is_delete" in df.columns:
        df = df.filter(F.col("is_delete") == 0)
    return df.select(*[c for c in df.columns if c not in _SOFT_DELETE_COLS])

def sk(*cols):
    return F.sha2(F.concat_ws("||", *[F.col(c).cast("string") if isinstance(c, str) else c.cast("string")
                                       for c in cols]), 256)

def load_scope_expr(kcoo_col, dcto_col, doco_col):
    """The per-load scope key, STORED on the fact. Vestigial under the batch build — it was the CDC delete
    scope in the previous streaming version; retained so the fact schema + semantic model are unchanged.
    The trim + long cast normalise the key ("00750||SX ||1184310.0" -> "00750||SX||1184310") so it is stable."""
    return sk(F.trim(F.col(kcoo_col)), F.trim(F.col(dcto_col)), F.col(doco_col).cast("long"))

def load_uss_plant_mcu():
    """Gold dim_uss_plant → (vendor_number, lofa_mcu). The ONLY F0005-derived value the fact needs at
    build time (the SOORDERNO SO-match MCU). Read from the DIM, never from Silver F0005."""
    if not spark.catalog.tableExists(gname(DIM_USS_PLANT)):
        raise RuntimeError(
            f"{gname(DIM_USS_PLANT)} not found — run nb_eso5_gold_dim_uss_plant FIRST. "
            "The fact needs its lofa_mcu (F0005 55/UP DRSPHD) for the SBXUSSSAND SOORDERNO match.")
    # vendor_number is DOUBLE (it must match the Double fact FK for the Direct Lake relationship);
    # the join below casts both sides to long so the comparison is exact.
    return (spark.read.table(gname(DIM_USS_PLANT))
            .select(F.col("vendor_number").alias("u_vend"),
                    F.trim(F.col("lofa_mcu")).alias("u_mcu"))
            .where(F.col("u_vend").isNotNull())
            .dropDuplicates(["u_vend"]))

# ----------------------------------------------------------------------------
# 2) FACT BUILDER
# ----------------------------------------------------------------------------

# FACT  fact_extended_sales_order_5  — ONE table, LINE grain, serving all five reports.
#   Grain = one F4211 SX order line (any item, any line type) PLUS one row per orphan F4311 OX
#   HOLADD PO line. `row_class` says which report(s) a row belongs to (see the header block).
# Display columns STORED on the fact — the GROUP BY grain. FOUR display columns are deliberately NOT
# stored (star schema — resolved via dimensions instead):
#   loading_facility_name  → dim_address_loading_facility.name_alpha (reused dim_address_book, F0101)
#   uss_plant_sand / shipped_from / lofa_mcu → dim_uss_plant.* (from F0005 55/UP, keyed by vendor)
FACT_CORE_COLS = [
    "load_number",          # SDDOCO
    "document_type",        # SDDCTO
    "company",              # SDKCOO
    "district",             # SDMCU
    "sold_to",              # SDAN8  (FK → dim_address_sold_to)
    "ship_to",              # SDSHAN (FK → dim_address_ship_to)
    "carrier",              # SDCARS (FK → dim_address_carrier)
    "customer_po",          # SDVR01
    "sand_po_number",       # F554201T QCDS50
    "uss_customer_po",      # SBXUSSSAND SOPONO
    "item_number",          # SDLITM
    "item_description",     # SDDSC1
    "order_date",           # SDTRDJ
    "gl_date",              # SDDGL
    "loading_facility",     # LOFA=SDVEND (FK → dim_address_loading_facility + dim_uss_plant)
    "uss_match",            # SBXUSSSAND MATCHFLAG
    "uss_so_order_no",      # SBXUSSSAND SOORDERNO
    "uss_so_weight",        # SBXUSSSAND SOWEIGHT
    "sbx_weight",           # SBXUSSSAND SXWEIGHT
    "so_alt_bol_no",        # SBXUSSSAND SOALTBOLNO
    "sand_ticket",          # SBXLOADPOVIEW SANDTKT
    "bol",                  # SBXLOADPOVIEW BOL
    "uom",                  # SDUOM
    "quantity",             # QTY (derived)
    "unit_price",           # SDUPRC
    "total_amount",         # SDAEXP
    "last_status",          # SDLTTR
    "next_status",          # SDNXTR
    "invoice_number",       # SDDOC
    "ox_last_status",       # F4311 OXLTTR
    "ox_next_status",       # F4311 OXNXTR
    "ox_amount",            # F4311 OXAMT
    "carrier_po_gl_post_flag",  # F0911 GLPOST
    "carrier_po_gl_post_flag_desc",  # GLPOST as a single "code - description" value, e.g. "P - Posted"
    "po_receipt_gl_date",   # F43121 GLDGJ
    "line_id",              # SDLNID
]
# STATUS — every status field the five queries touch, stored so the REPORT can do all status filtering.
# NO status predicate filters a row anywhere in this notebook. The three below are the ones
# that used to BE filters and are now fields; last_status / next_status / ox_last_status / ox_next_status /
# load_last_status are already in the lists above.
FACT_STATUS_COLS = [
    "next_status_num",       # numeric copy of next_status for the core report's SDNXTR<'581' page filter.
                             # An integer column makes `next_status_num < 581` unambiguous in Direct Lake
                             # (calculated columns are forbidden, so the numeric form must be physical).
    "po_holadd_superseded",  # 'Y' ⇔ the load has a LIVE (last_status<>'980') SX HOLADD sales line.
                             # WAS report 4's `NOT EXISTS(...)`.
    "po_order_type",         # PDDCTO on a PO row (document_type is forced to 'SX' by the query); NULL on
                             # F4211 rows. Lets the report filter the PO leg by its real document type.
    "load_max_last_status",  # MXLTTR — recon view's per-load MAX(SDLTTR)  (input to load_last_status)
    "load_min_last_status",  # MILTTR — recon view's per-load MIN(SDLTTR)  (input to load_last_status)
    "ox_amount_gross",       # the F4311 OX money with NO item/status condition — lets the report override
                             # ox_amount's baked-in `item='FRT' | (item='HOLADD' AND last_status<>'980')`
]
# Added so the FOUR Filter-Capture variations can be served from these same rows.
FACT_VARIATION_COLS = [
    "row_class",            # LINE | HOLADD | TEXT | PO_HOLADD  — the per-report row filter
    # report 5 (Reconciliation) pivot INPUTS — its SANDWEIGHT/EXTWEIGHT/MILES/LOFADET/WELLDET/...PP/
    # ...PB/FRTAMT/FSCAMT/SANDAMT/HOLAMT are DAX SUMs of these, filtered by item/category.
    "line_type",            # SDLNTY  ('TL' = the text lines the core view drops)
    "product_category",     # SDPRP1  ('COM' sand, 'FRT' freight)
    "sales_report_code_01", # SDSRP1  ('352' → SANDAMT)
    "units_ordered",        # SDUORG  — RAW decoded units (quantity above is the COM→tons version)
    "item_weight",          # SDITWT  → EXTWEIGHT
    "load_last_status",     # report 5's per-load SDLTTR CASE (see _load_last_status)
    # report 5 F554201T columns beyond QCDS50
    "leg_1",                # QCLGL1
    "leg_2",                # QCLGL2
    "leg_3",                # QCLGL3
    "qc_string_3",          # QCFSTR3
    # report 5 groups by the F4201 HEADER attributes, which need not equal the line's own values
    "header_district",      # SHMCU
    "header_sold_to",       # SHAN8
    "header_ship_to",       # SHSHAN
    "header_carrier",       # SHCARS
    "header_customer_po",   # SHVR01
    "header_order_date",    # SHTRDJ
]
FACT_GROUP_BY_COLS = FACT_CORE_COLS + FACT_STATUS_COLS + FACT_VARIATION_COLS
# SUMmed measure. Semi-additive (constant per LOFA).
FACT_MEASURE_COLS = ["lofa_rate"]
FACT_BUSINESS_COLS = FACT_GROUP_BY_COLS + FACT_MEASURE_COLS


def _pad30(c):
    """rpad(rtrim(rtrim(x,' '),'.'), 30, ' ') — trim trailing spaces then trailing dots, pad to 30."""
    return F.rpad(F.regexp_replace(F.rtrim(c), r"\.+$", ""), 30, " ")

def _pad25(c):
    """rpad(rtrim(x), 25, ' ')."""
    return F.rpad(F.rtrim(c), 25, " ")


# ── the two row legs ────────────────────────────────────────────────────────────
# Both legs are projected into ONE common intermediate schema so the whole downstream join chain
# (BOL / SANDTKT / OX / F43121 / F0911 / F554201T / SBXUSSSAND / SXWEIGHT / F0101 / F4201) is written
# once and applied to both. Leg-B's PO-side OX values ride along as _pox_* and win in the final select.
_LEG_COLS = ["l_kcoo", "l_doco", "l_dcto", "l_mcu", "l_an8", "l_shan", "l_cars", "l_vr01",
             "l_litm", "l_dsc1", "l_uom", "l_uorg", "l_itwt", "l_prp1", "l_srp1", "l_lnty",
             "l_trdj", "l_vend", "l_uprc", "l_aexp", "l_dgl", "l_lttr", "l_nxtr", "l_doc", "l_lnid",
             "row_class", "po_holadd_superseded", "l_po_dcto", "_pox_lttr", "_pox_nxtr", "_pox_amt"]

def _f4211_lines(f4211):
    """Leg A — the F4211 sales-order lines. NO FILTER OF ANY KIND. This leg is
    the WHOLE of Silver F4211; every predicate the five queries put in a WHERE is carried as a COLUMN and
    filtered in the Power BI report instead:
        SDDCTO='SX'      -> the `document_type` column   (report filter)
        SDKCOO='00750'   -> the `company` column         (report filter)
        SDLNTY<>'TL'     -> row_class 'TEXT'             (report filter)
        SDLITM<>'HOLADD' -> row_class 'HOLADD'           (report filter)
    The last two could never have been applied anyway: they CONFLICT across the five reports (1-3 drop TL
    and HOLADD, 4 REQUIRES HOLADD, 5 keeps both). That conflict is the structural reason one fact can serve
    five reports only if the discrimination happens at report level — which is exactly what `row_class` is.
    ⚠ With this leg unfiltered, the SO-match / `_load_aggregates` / OX helpers now read the SAME population
    they always needed: the SO leg lives on the SDDCTO='SO' AND SDCO='00400' rows that an SX filter removes."""
    sd = f4211
    row_class = (F.when(F.trim(F.col("line_type")) == "TL", F.lit("TEXT"))
                  .when(F.trim(F.col("identifier_second_item")) == "HOLADD", F.lit("HOLADD"))
                  .otherwise(F.lit("LINE")))
    return sd.select(
        F.trim(F.col("company_key_order_no")).alias("l_kcoo"),
        F.col("document_order_invoice_e").alias("l_doco"),
        F.trim(F.col("order_type")).alias("l_dcto"),
        F.trim(F.col("cost_center")).alias("l_mcu"),
        F.col("address_number").alias("l_an8"),
        F.col("address_number_ship_to").alias("l_shan"),
        F.col("carrier").alias("l_cars"),
        F.col("reference_01").alias("l_vr01"),
        F.trim(F.col("identifier_second_item")).alias("l_litm"),
        F.col("description_line_01").alias("l_dsc1"),
        F.col("uom_as_input").alias("l_uom"),
        F.col("units_transaction_qty").alias("l_uorg"),                     # SDUORG
        F.col("amount_unit_weight").cast("double").alias("l_itwt"),         # SDITWT
        F.trim(F.col("purchasing_report_code_01")).alias("l_prp1"),         # SDPRP1
        F.trim(F.col("sales_reporting_code_01")).alias("l_srp1"),           # SDSRP1
        F.trim(F.col("line_type")).alias("l_lnty"),                         # SDLNTY
        F.col("date_transaction_julian").alias("l_trdj"),
        F.col("primary_last_vendor_no").alias("l_vend"),
        F.col("amt_price_per_unit_02").alias("l_uprc"),
        F.col("amount_extended_price").alias("l_aexp"),
        F.col("dt_for_gl_and_vouch_01").alias("l_dgl"),
        F.trim(F.col("status_code_last")).alias("l_lttr"),
        F.trim(F.col("status_code_next")).alias("l_nxtr"),
        F.col("doc_voucher_invoice_e").alias("l_doc"),
        F.col("line_number").alias("l_lnid"),
        row_class.alias("row_class"),
        F.lit("N").alias("po_holadd_superseded"),   # only ever 'Y' on a PO_HOLADD row (leg B)
        F.lit(None).cast("string").alias("l_po_dcto"),   # PDDCTO — PO rows only
        F.lit(None).cast("string").alias("_pox_lttr"),
        F.lit(None).cast("string").alias("_pox_nxtr"),
        F.lit(None).cast("double").alias("_pox_amt"))

def _po_lines(f4311, la):
    """Leg B — the F4311 purchase-order lines. Report 4's second UNION branch is the OX HOLADD ones; with
    PO_LEG_UNFILTERED (the default) this leg takes the WHOLE table and `row_class` classifies each row
    instead, so the notebook holds NO row-selecting predicate at all:
        row_class = 'PO_HOLADD'  ⇔  PDDCTO='OX' AND PDLITM='HOLADD'   → report 4
                    'PO_OTHER'   ⇔  every other purchase-order line   → no report reads it
    Sales-side attributes are back-filled from the load's FRT sales line, and UPRC/EXTAMT are forced to 0
    — the money is on OXAMT. The back-fill and the
    ex-NOT-EXISTS flag both come from `la` (_load_aggregates), so this leg reads F4211 not at all; a PO line
    with no SX load behind it simply gets NULLs.

    ⚠ The `NOT EXISTS(live SX HOLADD, SDLTTR<>'980')` is GONE — a status test that decided whether a
    row EXISTS. Every HOLADD PO line is a row now, and the test is a FIELD:
        po_holadd_superseded = 'Y'  ⇔  the load already carries a live SX HOLADD sales line
    The orphan set is exactly `po_holadd_superseded = 'N'`, which report 4 filters on.

    ⚠ `document_type` is forced to 'SX' on every row here — the query hard-codes `'SX' DCTO` so the UNION
    lines up with the sales load. The PO's OWN document type is kept in `po_order_type` (PDDCTO)."""
    po = f4311 if PO_LEG_UNFILTERED else f4311.where(
        (F.trim(F.col("order_type")) == "OX") & (F.trim(F.col("identifier_2nd_item")) == "HOLADD"))

    # Back-fill + the ex-NOT-EXISTS flag come from the load's SX aggregates (the FRT
    # subqueries correlate on PDKCOO/PDDOCO with SDDCTO='SX' — hence the literal 'SX' in the join key).
    j = (po.alias("pd")
         .join(la, (F.trim(F.col("pd.company_key_order_no")) == F.col("_la_kcoo")) &
                   (F.col("pd.document_order_invoice_e") == F.col("_la_doco")) &
                   (F.col("_la_dcto") == F.lit("SX")), "left"))
    return j.select(
        F.trim(F.col("pd.company_key_order_no")).alias("l_kcoo"),
        F.col("pd.document_order_invoice_e").alias("l_doco"),
        F.lit("SX").alias("l_dcto"),                                   # the query hard-codes 'SX'
        F.trim(F.col("pd.cost_center")).alias("l_mcu"),                # PDMCU
        F.col("_fr_an8").alias("l_an8"),                               # from the FRT line
        F.col("pd.address_number_ship_to").cast("double").alias("l_shan"),   # PDSHAN
        F.col("pd.address_number").alias("l_cars"),                    # PDAN8 IS the carrier
        F.col("_fr_vr01").alias("l_vr01"),
        F.trim(F.col("pd.identifier_2nd_item")).alias("l_litm"),
        F.col("pd.description_line_01").alias("l_dsc1"),               # PDDSC1
        F.col("pd.uom_as_input").alias("l_uom"),                       # PDUOM
        F.col("pd.units_transaction_qty").cast("double").alias("l_uorg"),    # PDUORG
        F.lit(None).cast("double").alias("l_itwt"),
        F.lit(None).cast("string").alias("l_prp1"),
        F.lit(None).cast("string").alias("l_srp1"),
        F.lit(None).cast("string").alias("l_lnty"),
        F.col("pd.date_transaction_julian").cast("date").alias("l_trdj"),    # PDTRDJ
        F.col("_fr_vend").alias("l_vend"),                             # LOFA from the FRT line
        F.lit(0.0).alias("l_uprc"),                                    # UPRC = 0
        F.lit(0.0).alias("l_aexp"),                                    # EXTAMT = 0
        F.col("_fr_dgl").alias("l_dgl"),
        F.col("_fr_lttr").alias("l_lttr"),
        F.col("_fr_nxtr").alias("l_nxtr"),
        F.col("_fr_doc").alias("l_doc"),
        F.col("pd.line_number").cast("double").alias("l_lnid"),        # PDLNID
        # `PDDCTO='OX' AND PDLITM='HOLADD'` — the predicate for this UNION branch. With
        # PO_LEG_UNFILTERED it is no longer a filter: it CLASSIFIES the row. Only 'PO_HOLADD' rows feed
        # report 4; 'PO_OTHER' is every other purchase-order line, carried but read by no report.
        F.when((F.trim(F.col("pd.order_type")) == "OX") &
               (F.trim(F.col("pd.identifier_2nd_item")) == "HOLADD"), F.lit("PO_HOLADD"))
         .otherwise(F.lit("PO_OTHER")).alias("row_class"),
        F.coalesce(F.col("_live_holadd"), F.lit("N")).alias("po_holadd_superseded"),   # ex-NOT EXISTS
        F.trim(F.col("pd.order_type")).alias("l_po_dcto"),             # PDDCTO (document_type is forced
                                                                       # to 'SX' by the query, so the PO's
                                                                       # own type needs its own column)
        F.trim(F.col("pd.status_code_last")).alias("_pox_lttr"),       # OXLTTR = PDLTTR (row's own)
        F.trim(F.col("pd.status_code_next")).alias("_pox_nxtr"),       # OXNXTR = PDNXTR
        F.col("pd.amount_extended_price").cast("double").alias("_pox_amt"))   # OXAMT = PDAEXP


def _load_aggregates(f4211):
    """EVERY per-load value the five queries compute with a correlated subquery, in ONE pass over F4211.

    ⚠ THE POINT OF THIS FUNCTION: not one of the source predicates (SDLITM='BOL' / 'SANDTKTNBR' / 'HOLADD'
    / 'FRT', SDPRP1='COM', SDLTTR<>'980', SDDCTO='SX') appears in a WHERE. Every one is a CASE inside an
    aggregate, so it decides whether a line CONTRIBUTES TO A VALUE — never whether a row SURVIVES. No row
    is filtered out of anything. That is the difference between a calculation (kept: it IS the business
    logic) and a filter (gone: it belongs to the report).

    Grain (kcoo, doco, dcto) — the group key carries the order type and company, so an SX/00750 row reads
    exactly the values the `SDDCTO='SX' AND SDKCOO='00750'` subqueries produce, with no
    constant hard-coded anywhere."""
    item = F.trim(F.col("identifier_second_item"))
    lttr = F.trim(F.col("status_code_last"))
    return (f4211.groupBy(F.trim(F.col("company_key_order_no")).alias("_la_kcoo"),
                          F.col("document_order_invoice_e").alias("_la_doco"),
                          F.trim(F.col("order_type")).alias("_la_dcto"))
            .agg(
                # SBXLOADPOVIEW: BOL / SANDTKT  (were `WHERE SDLITM = 'BOL' / 'SANDTKTNBR'`)
                F.max(F.when(item == "BOL", F.col("description_line_01"))).alias("bol"),
                F.max(F.when(item == "SANDTKTNBR", F.col("description_line_01"))).alias("sand_ticket"),
                # SBXUSSSAND's M row = the load's SANDTKTNBR line (its vendor drives the 55/UP plant match)
                F.max(F.when(item == "SANDTKTNBR", F.col("primary_last_vendor_no"))).alias("_st_vend"),
                # SXWEIGHT  (was `WHERE SDDCTO='SX' AND SDPRP1='COM' AND SDLTTR<>'980'`)
                F.sum(F.when((F.trim(F.col("purchasing_report_code_01")) == "COM") & (lttr != "980"),
                             F.col("units_transaction_qty"))).alias("sbx_weight"),
                # report 4's `NOT EXISTS(live SX HOLADD, SDLTTR<>'980')` → a FIELD, not a row filter
                F.max(F.when((item == "HOLADD") & (lttr != "980"), F.lit("Y"))).alias("_live_holadd"),
                # report 4 leg B back-fills its sales-side attributes from the load's FRT line
                F.max(F.when(item == "FRT", F.col("reference_01"))).alias("_fr_vr01"),
                F.max(F.when(item == "FRT", F.col("address_number"))).alias("_fr_an8"),
                F.max(F.when(item == "FRT", F.col("primary_last_vendor_no"))).alias("_fr_vend"),
                F.max(F.when(item == "FRT", F.col("dt_for_gl_and_vouch_01"))).alias("_fr_dgl"),
                F.max(F.when(item == "FRT", lttr)).alias("_fr_lttr"),
                F.max(F.when(item == "FRT", F.trim(F.col("status_code_next")))).alias("_fr_nxtr"),
                F.max(F.when(item == "FRT", F.col("doc_voucher_invoice_e"))).alias("_fr_doc"),
                # report 5's per-load SDLTTR CASE + its two inputs (all three stored on the fact)
                F.max(lttr).alias("load_max_last_status"),                                  # MXLTTR
                F.min(lttr).alias("load_min_last_status"),                                  # MILTTR
                F.max(F.when((item != "HOLADD") & (F.col("amount_extended_price") != 0),
                             lttr)).alias("_alt"))
            .withColumn("load_last_status",
                        F.when((F.col("load_max_last_status") == "980") &
                               (F.col("load_min_last_status") == "980"), F.col("load_max_last_status"))
                         .otherwise(F.col("_alt")))
            .drop("_alt"))


def build_fact():
    f4211  = load_silver_table(F4211)
    qc     = load_silver_table(F554201T)
    f4311  = load_silver_table(F4311)
    f0911  = load_silver_table(F0911)
    f43121 = load_silver_table(F43121)
    ab     = load_silver_table(F0101)
    f4201  = load_silver_table(F4201)         # report-5 header

    # ── normalize the order/load key type ACROSS tables (bug fix 2026-07-21) ─────────────────────
    # SDDOCO / PDDOCO / PRDOCO are the SAME JDE data item (DOCO), but Silver can land them with
    # DIFFERENT physical types per table (e.g. F4311 as string, F4211/F43121 as decimal). A cross-table
    # equality JOIN on document_order_invoice_e then SILENTLY MISSES — invisible for leg A (all F4211),
    # but on the F4311 PO_HOLADD rows it wipes EVERY sales-side back-fill: sold_to / customer_po /
    # loading_facility / last_status / next_status / invoice_number / bol / sand_ticket / po_receipt_gl_date
    # / carrier_po_gl_post_flag — and gl_date collapses to coalesce(NULL, 1900-01-01). (ox_* survive because
    # they read F4311↔F4311.) Cast the key to ONE canonical type wherever it is carried so all doco joins
    # are long==long. DOCO is an integer order number (0 implied decimals), so the cast is lossless.
    _DOCO = "document_order_invoice_e"
    f4211  = f4211.withColumn(_DOCO,  F.col(_DOCO).cast("long"))
    f4311  = f4311.withColumn(_DOCO,  F.col(_DOCO).cast("long"))
    f43121 = f43121.withColumn(_DOCO, F.col(_DOCO).cast("long"))
    qc     = qc.withColumn(_DOCO,     F.col(_DOCO).cast("long"))
    f4201  = f4201.withColumn(_DOCO,  F.col(_DOCO).cast("long"))

    # ── ALL the per-load F4211 subqueries, in one filter-free pass (bol / sand_ticket / sbx_weight /
    #    load_last_status + inputs / the leg-B FRT back-fill / the ex-NOT-EXISTS flag) ──
    la = _load_aggregates(f4211)

    # ── the row population: F4211 SX/00750 lines  UNION  the WHOLE F4311 PO leg ──
    base = (_f4211_lines(f4211).select(*_LEG_COLS)
            .unionByName(_po_lines(f4311, la).select(*_LEG_COLS)))

    # ── F554201T — QCDS50 (Sand PO Number) + QCLGL1/2/3 + QCFSTR3, per (kcoo, doco, dcto).
    #    The legs are Silver `descriptn_01/02/03` and QCFSTR3 is `future_use_string_03` — the JDE→snake_case
    #    map is NOT literal, so do not guess the names from the alias. ──
    def _qcf(src, alias):
        return F.first(F.col(src), ignorenulls=True).alias(alias)
    qcv = (qc.groupBy(F.trim(F.col("company_key_order_no")).alias("qc_kcoo"),
                      F.col("document_order_invoice_e").alias("qc_doco"),
                      F.trim(F.col("order_type")).alias("qc_dcto"))
           .agg(F.first(F.trim(F.col("description_50_characters")), ignorenulls=True).alias("sand_po_number"),
                _qcf("descriptn_01", "leg_1"),                 # QCLGL1
                _qcf("descriptn_02", "leg_2"),                 # QCLGL2
                _qcf("descriptn_03", "leg_3"),                 # QCLGL3
                _qcf("future_use_string_03", "qc_string_3")))  # QCFSTR3

    # ── OX status + OX amount from F4311, by (item, load).
    #    `PDDCTO='OX'` and `PDKCOO='00750'` were WHERE constants; they are CASE conditions now, so no
    #    F4311 row is filtered away — they merely decide which rows contribute to the OX values. ──
    _is_ox = ((F.trim(F.col("order_type")) == "OX") &
              (F.trim(F.col("company_key_order_no")) == "00750"))
    ox = (f4311.groupBy(F.trim(F.col("identifier_2nd_item")).alias("_ox_item"),
                        F.col("document_order_invoice_e").alias("_ox_doco"))
          .agg(F.max(F.when(_is_ox, F.col("status_code_next"))).alias("_ox_nxtr"),
               F.max(F.when(_is_ox, F.col("status_code_last"))).alias("_ox_lttr"),
               F.sum(F.when(_is_ox, F.col("amount_extended_price"))).alias("_ox_amt")))

    # ── PO Receipt GL Date — F43121, by the line keys. `PRDCT='OV' AND PRMATC='1' AND PRDGL>1` are CASE
    #    conditions, not a WHERE: the value is the MAX over the matching receipts, no row is dropped. ──
    _rc_ok = ((F.trim(F.col("document_type")) == "OV") &
              (F.trim(F.col("match_type")) == "1") &
              (F.col("dt_for_gl_and_vouch_01").isNotNull()))
    recv = (f43121.groupBy(F.col("address_number").alias("_rc_pran8"),
                           F.trim(F.col("company_key_order_no")).alias("_rc_kcoo"),
                           F.col("document_order_invoice_e").alias("_rc_doco"),
                           F.trim(F.col("identifier_2nd_item")).alias("_rc_item"))
            .agg(F.max(F.when(_rc_ok, F.col("dt_for_gl_and_vouch_01"))).alias("po_receipt_gl_date")))

    # ── Carrier PO GL Post flag — F0911 doc ∈ F43121 PRDOC, linked by the line keys. The `GLDCT='OV'`,
    #    `GLKCO='00750'` and `PRDCT='OV'` constants ride inside the CASE; the doc linkage is a JOIN. ──
    recv_docs = f43121.select(F.col("doc_voucher_invoice_e").alias("_gd_prdoc"),
                              F.col("address_number").alias("_gd_pran8"),
                              F.trim(F.col("company_key_order_no")).alias("_gd_kcoo"),
                              F.col("document_order_invoice_e").alias("_gd_doco"),
                              F.trim(F.col("identifier_2nd_item")).alias("_gd_item"),
                              F.trim(F.col("document_type")).alias("_gd_dct"))
    glpost_docs = f0911.select(F.col("doc_voucher_invoice_e").alias("_gl_doc"),
                               F.col("gl_posted_code").alias("_gl_post"),
                               F.trim(F.col("document_type")).alias("_gl_dct"),
                               F.trim(F.col("company_key")).alias("_gl_kco"))
    glpost = (recv_docs.join(glpost_docs, F.col("_gd_prdoc") == F.col("_gl_doc"), "inner")
              .groupBy("_gd_pran8", "_gd_kcoo", "_gd_doco", "_gd_item")
              .agg(F.max(F.when((F.col("_gd_dct") == "OV") & (F.col("_gl_dct") == "OV") &
                                (F.col("_gl_kco") == "00750"), F.col("_gl_post")))
                    .alias("carrier_po_gl_post_flag")))

    # ── SBXUSSSAND — the load's SANDTKTNBR line (from `la`) INNER F554201T on (kcoo, doco, dcto).
    #    The `M.SDDCTO='SX' AND M.SDLITM='SANDTKTNBR' AND M.SDKCOO='00750'` conditions need no WHERE here: the
    #    item condition is already the CASE inside `la`, and the order type + company are the join keys. ──
    plant = load_uss_plant_mcu()   # F0005 55/UP DRSPHD, from the GOLD dim — never Silver F0005
    m2f = (la.alias("la")
           .join(qcv.alias("qc"),
                 (F.col("la._la_kcoo") == F.col("qc.qc_kcoo")) &
                 (F.col("la._la_doco") == F.col("qc.qc_doco")) &
                 (F.col("la._la_dcto") == F.col("qc.qc_dcto")), "inner")
           .join(plant, F.col("la._st_vend").cast("long") == F.col("u_vend").cast("long"), "left")
           .select(F.col("la._la_kcoo").alias("us_kcoo"),
                   F.col("la._la_doco").alias("us_doco"),
                   F.col("la._la_dcto").alias("us_dcto"),
                   F.col("la.sand_ticket").alias("us_sddsc1"),      # M.SDDSC1 (the sand-ticket number)
                   F.col("qc.sand_po_number").alias("us_qcds50"),   # F554201T.QCDS50
                   F.col("u_mcu").alias("lofa_mcu")))               # SO-match input only (not stored)

    # SO orders — matched to the sand load by padded pull_signal + reference_01. `L.SDDCTO='SO' AND
    # L.SDCO='00400'` are part of the MATCH, so they live in the JOIN CONDITION, not in a WHERE on F4211.
    so = f4211.select(F.col("pull_signal").alias("so_psig"),
                      F.col("reference_01").alias("so_vr01"),
                      F.col("document_order_invoice_e").cast("long").alias("so_doco"),  # → int64
                      F.col("cost_center").alias("so_mcu"),
                      F.trim(F.col("line_type")).alias("so_lnty"),
                      F.col("units_secondary_qty_or").alias("so_sqor"),
                      F.trim(F.col("order_type")).alias("so_dcto"),
                      F.trim(F.col("company")).alias("so_co"))
    matched = (m2f.alias("u").join(so.alias("s"),
                   (F.col("s.so_psig") == _pad30(F.col("u.us_sddsc1"))) &
                   (F.col("s.so_vr01") == _pad25(F.col("u.us_qcds50"))) &
                   (F.col("s.so_dcto") == "SO") & (F.col("s.so_co") == "00400"), "left"))
    sbxusssand = (matched.groupBy("us_kcoo", "us_doco", "us_dcto")
                  .agg(F.max(F.when(F.col("s.so_doco").isNotNull(), F.lit("Y"))).alias("uss_match"),
                       F.first(F.col("s.so_vr01"),  ignorenulls=True).alias("uss_customer_po"),   # SOPONO
                       F.first(F.col("s.so_psig"),  ignorenulls=True).alias("so_alt_bol_no"),     # SOALTBOLNO
                       # SOORDERNO — SO doco whose district (so_mcu) matches the vendor's plant MCU (DRSPHD)
                       F.first(F.when(F.trim(F.col("s.so_mcu")) == F.col("lofa_mcu"), F.col("s.so_doco")),
                               ignorenulls=True).alias("uss_so_order_no"),
                       # SOWEIGHT — sum secondary qty over matched SO 'S' lines (decoded; no /1000)
                       F.sum(F.when(F.col("s.so_lnty") == "S", F.col("s.so_sqor"))).alias("uss_so_weight")))

    # F0101 loading-facility lookup (LOFA = ABAN8): RATE only. The NAME (ABALPH) resolves via the
    # reused dim_address_loading_facility relationship; F0101 is read here ONLY for ABURAT (the rate),
    # which the reused dim_address_book does not carry (`user_reserved_amount` absent).
    #
    # The rate source is `SELECT … FROM F0101 WHERE ABAT1 BETWEEN 'A '..'P ' OR 'R '..'ZZZ'` — a
    # search-type band that says WHICH address-book rows COUNT AS a rate source (it excludes the 'Q'
    # band). That predicate is NOT deleted and is NOT a WHERE: it is a CASE
    # INSIDE THE AGGREGATE. Deleting it would not make the rate "unfiltered", it would make it WRONG —
    # a 'Q'-band facility would start reporting a rate that should be blank. As a CASE it drops nothing:
    # every address_number keeps its group, and an out-of-band facility simply yields a NULL rate.
    # (`address_type_01` is on the reused `dim_address_loading_facility` if the report ever wants to see
    #  or override the band.)  ABAT1 = address_type_01, rpad to 3 to mirror the padded 'A  '/'P  '/'ZZZ'.
    _at = F.rpad(F.rtrim(F.col("address_type_01")), 3, " ")
    _is_rate_source = (((_at >= F.lit("A  ")) & (_at <= F.lit("P  "))) |
                       ((_at >= F.lit("R  ")) & (_at <= F.lit("ZZZ"))))
    lofa = (ab.groupBy(F.col("address_number").alias("ab_an8"))
            .agg(F.first(F.when(_is_rate_source, F.col("user_reserved_amount")),
                         ignorenulls=True).alias("_aburat")))

    # F4201 sales-order HEADER — report 5 groups by SHMCU/SHAN8/SHSHAN/SHCARS/SHVR01/SHTRDJ, which are
    # header values and need not equal the line's own.
    hdr = (f4201.select(
                F.trim(F.col("company_key_order_no")).alias("h_kcoo"),          # SHKCOO
                F.col("document_order_invoice_e").alias("h_doco"),              # SHDOCO
                F.trim(F.col("order_type")).alias("h_dcto"),                    # SHDCTO
                F.trim(F.col("cost_center")).alias("header_district"),          # SHMCU
                F.col("address_number").cast("double").alias("header_sold_to"),         # SHAN8
                F.col("address_number_ship_to").cast("double").alias("header_ship_to"), # SHSHAN
                F.col("carrier").cast("double").alias("header_carrier"),        # SHCARS
                F.col("reference_01").alias("header_customer_po"),              # SHVR01
                F.col("date_transaction_julian").cast("date").alias("header_order_date"))  # SHTRDJ
           .dropDuplicates(["h_kcoo", "h_doco", "h_dcto"]))

    # ── derivations on the common line ──────────────────────────────────────────────
    # QTY = DECODE(SDPRP1,'COM',(SDUORG/1000)/2000, SDUORG/1000) — Silver is decoded, so only the
    # COM→tons factor survives. PO_HOLADD rows have no SDPRP1, so they take the plain-units branch,
    # which is what report 4's `pduorg / 1000` reduces to.
    qty = F.when(F.col("l_prp1") == "COM", F.col("l_uorg") / F.lit(TONS_DIVISOR)).otherwise(F.col("l_uorg"))
    gl_date = F.coalesce(F.col("l_dgl"), F.to_date(F.lit("1900-01-01")))
    # OXAMT — the core query charges it to the FRT line; the HOLADD variation charges it to a live
    # HOLADD line (SDLTTR<>'980'); a PO_HOLADD row carries its own PDAEXP. Union of the three rules.
    # This is a CASE (a calculation), not a filter — no row is dropped. But it does BAKE IN a status rule,
    # so `ox_amount_gross` below exposes the same money with NO item/status condition, letting the report
    # own the status decision entirely (`last_status` is on the row).
    _is_po_row = F.col("row_class").isin("PO_HOLADD", "PO_OTHER")
    ox_amount = (F.when(_is_po_row, F.coalesce(F.col("_pox_amt"), F.lit(0.0)))
                  .when(F.col("l_litm") == "FRT", F.coalesce(F.col("_ox_amt"), F.lit(0.0)))
                  .when((F.col("l_litm") == "HOLADD") & (F.col("l_lttr") != "980"),
                        F.coalesce(F.col("_ox_amt"), F.lit(0.0)))
                  .otherwise(F.lit(0.0)))
    ox_amount_gross = F.coalesce(F.col("_pox_amt"), F.col("_ox_amt"), F.lit(0.0))

    # ── assemble: base LEFT (per-load aggregates, OX, F43121, F0911, F554201T, SBXUSSSAND, F0101, F4201) ──
    # `la` carries bol / sand_ticket / sbx_weight / load_last_status(+MX,MI) / the leg-B back-fill / the
    # ex-NOT-EXISTS flag — one join now replaces the old bol + sandtkt + sxw + lls joins.
    j = (base.alias("sd")
         .join(la, (F.col("sd.l_kcoo") == F.col("_la_kcoo")) &
                   (F.col("sd.l_doco") == F.col("_la_doco")) &
                   (F.col("sd.l_dcto") == F.col("_la_dcto")), "left")
         .join(ox, (F.col("sd.l_litm") == F.col("_ox_item")) &
                   (F.col("sd.l_doco") == F.col("_ox_doco")), "left")
         .join(recv, (F.col("sd.l_cars") == F.col("_rc_pran8")) &
                     (F.col("sd.l_kcoo") == F.col("_rc_kcoo")) &
                     (F.col("sd.l_doco") == F.col("_rc_doco")) &
                     (F.col("sd.l_litm") == F.col("_rc_item")), "left")
         .join(glpost, (F.col("sd.l_cars") == F.col("_gd_pran8")) &
                       (F.col("sd.l_kcoo") == F.col("_gd_kcoo")) &
                       (F.col("sd.l_doco") == F.col("_gd_doco")) &
                       (F.col("sd.l_litm") == F.col("_gd_item")), "left")
         .join(qcv, (F.col("sd.l_kcoo") == F.col("qc_kcoo")) &
                    (F.col("sd.l_doco") == F.col("qc_doco")) &
                    (F.col("sd.l_dcto") == F.col("qc_dcto")), "left")          # join #1
         .join(sbxusssand, (F.col("sd.l_kcoo") == F.col("us_kcoo")) &
                           (F.col("sd.l_doco") == F.col("us_doco")) &
                           (F.col("sd.l_dcto") == F.col("us_dcto")), "left")   # join #2
         .join(lofa, F.col("sd.l_vend") == F.col("ab_an8"), "left")            # join #3 (LOFA=ABAN8)
         .join(hdr, (F.col("sd.l_kcoo") == F.col("h_kcoo")) &
                    (F.col("sd.l_doco") == F.col("h_doco")) &
                    (F.col("sd.l_dcto") == F.col("h_dcto")), "left"))   # report-5 header

    sel = j.select(
        # ── degenerate identifiers ──
        F.col("sd.l_doco").cast("long").alias("load_number"),       # SDDOCO (int64)
        F.col("sd.l_dcto").alias("document_type"),                  # SDDCTO
        F.col("sd.l_kcoo").alias("company"),                        # SDKCOO
        F.col("sd.l_mcu").alias("district"),                        # SDMCU
        # ── address FK codes ──
        F.col("sd.l_an8").alias("sold_to"),                         # SDAN8
        F.col("sd.l_shan").alias("ship_to"),                        # SDSHAN
        F.col("sd.l_cars").alias("carrier"),                        # SDCARS
        F.col("sd.l_vend").alias("loading_facility"),               # LOFA=SDVEND
        # ── degenerate attributes ──
        F.col("sd.l_vr01").alias("customer_po"),                    # SDVR01
        F.col("sand_po_number"),                                    # F554201T QCDS50
        F.col("uss_customer_po"),                                   # SBXUSSSAND SOPONO
        F.col("sd.l_litm").alias("item_number"),                    # SDLITM
        F.col("sd.l_dsc1").alias("item_description"),               # SDDSC1
        F.col("sd.l_trdj").alias("order_date"),                     # SDTRDJ
        gl_date.alias("gl_date"),                                   # SDDGL (null→1900-01-01)
        F.col("uss_match"),
        F.col("uss_so_order_no"), F.col("uss_so_weight"), F.col("sbx_weight"), F.col("so_alt_bol_no"),
        F.col("sand_ticket"), F.col("bol"),
        F.col("sd.l_uom").alias("uom"),                             # SDUOM
        qty.alias("quantity"),                                      # QTY (derived)
        F.col("sd.l_uprc").alias("unit_price"),                     # SDUPRC (0 on PO_HOLADD)
        F.col("sd.l_aexp").alias("total_amount"),                   # SDAEXP (0 on PO_HOLADD)
        F.col("sd.l_lttr").alias("last_status"),                    # SDLTTR
        F.col("sd.l_nxtr").alias("next_status"),                    # SDNXTR
        F.col("sd.l_nxtr").cast("int").alias("next_status_num"),     # SDNXTR as int — core report SDNXTR<'581'
        F.col("sd.l_doc").cast("long").alias("invoice_number"),     # SDDOC (int64)
        # OX status — an orphan PO row reports ITS OWN PDLTTR/PDNXTR, not the load-level MAX
        F.coalesce(F.col("sd._pox_lttr"), F.col("_ox_lttr")).alias("ox_last_status"),
        F.coalesce(F.col("sd._pox_nxtr"), F.col("_ox_nxtr")).alias("ox_next_status"),
        ox_amount.alias("ox_amount"),                               # OXAMT (three-way rule above)
        ox_amount_gross.alias("ox_amount_gross"),                   # same money, NO item/status condition
        F.col("carrier_po_gl_post_flag"),                           # F0911 (raw code, kept for filtering)
        # decoded G/L Posted Code as a single "code - description" display value: P/D -> "P - Posted",
        # blank (voucher exists, unposted) -> "Unposted", NULL (no matching OV voucher) -> blank,
        # any other code -> the code alone.
        F.when(F.col("carrier_po_gl_post_flag").isNull(), F.lit(None).cast("string"))
         .when(F.trim(F.col("carrier_po_gl_post_flag")).isin("P", "D"),
               F.concat(F.trim(F.col("carrier_po_gl_post_flag")), F.lit(" - Posted")))
         .when(F.trim(F.col("carrier_po_gl_post_flag")) == "", F.lit("Unposted"))
         .otherwise(F.trim(F.col("carrier_po_gl_post_flag")))
         .alias("carrier_po_gl_post_flag_desc"),
        F.col("po_receipt_gl_date"),                                # F43121
        # SDLNID — display the RAW JDE line number: 1.00 -> 1000.  Silver decoded the 3 implied decimals;
        # this puts them back. It is LOSSLESS *because* of the decimals, not
        # in spite of them: a fractional kit/component line 1.010 becomes 1010, so nothing is truncated —
        # the earlier `formatString: 0.###` workaround is no longer needed. round() first: 1.01 * 1000 is
        # 1009.9999999999999 in binary floating point, and a bare cast would floor it to 1009.
        F.round(F.col("sd.l_lnid") * F.lit(1000), 0).cast("long").alias("line_id"),
        # ── variation columns ──
        F.col("sd.row_class"),
        F.col("sd.l_lnty").alias("line_type"),                      # SDLNTY
        F.col("sd.l_prp1").alias("product_category"),               # SDPRP1
        F.col("sd.l_srp1").alias("sales_report_code_01"),           # SDSRP1
        F.col("sd.l_uorg").alias("units_ordered"),                  # SDUORG (raw decoded)
        F.col("sd.l_itwt").alias("item_weight"),                    # SDITWT
        F.col("sd.po_holadd_superseded"),                           # ex-NOT EXISTS (status → field)
        F.col("sd.l_po_dcto").alias("po_order_type"),                # PDDCTO (PO rows only)
        F.col("load_last_status"),                                  # report-5 per-load SDLTTR CASE
        F.col("load_max_last_status"), F.col("load_min_last_status"),   # MXLTTR / MILTTR (the CASE inputs)
        F.col("leg_1"), F.col("leg_2"), F.col("leg_3"), F.col("qc_string_3"),
        F.col("header_district"), F.col("header_sold_to"), F.col("header_ship_to"),
        F.col("header_carrier"), F.col("header_customer_po"), F.col("header_order_date"),
        # ── measure ──
        (F.col("_aburat") * F.lit(RATE_FACTOR)).alias("lofa_rate"),  # ABURAT
    ).distinct()

    # ── GROUP BY (outer): one row per display tuple; SUM the single measure ──
    agg = (sel.groupBy(*FACT_GROUP_BY_COLS)
           .agg(F.sum("lofa_rate").alias("lofa_rate")))

    df = (agg
          .withColumn("load_scope_key",
                      load_scope_expr("company", "document_type", "load_number"))   # vestigial key — retained so the fact schema is unchanged
          .withColumn("load_line_key", sk(*FACT_GROUP_BY_COLS)))
    df = df.dropDuplicates(["load_line_key"])
    return df.select("load_line_key", "load_scope_key", *FACT_BUSINESS_COLS)

# ----------------------------------------------------------------------------
# 3) FACT SOURCES
# ----------------------------------------------------------------------------

# Declares each Silver source and how it relates to the F4211 spine. This fact's join graph is rich
# (SBXLOADPOVIEW/SBXLOADDETAIL/SBXUSSSAND legs) and applied
# inside build_fact() / _f4211_lines() / _po_lines() / _load_aggregates(), so join_pairs are [] here (the
# joins are not simple FK pairs); the list documents the source inventory and drives the RUN source
# preflight. join: spine=F4211 SX driver, union=F4311 PO leg (contributes rows), static=lookup. The Gold
# prerequisite dim_uss_plant is validated separately by load_uss_plant_mcu().
FACT_SOURCES = [
    {"silver": F4211,    "join": "spine",  "join_pairs": []},   # SX order-line driver; LINE/HOLADD/TEXT rows
    {"silver": F4311,    "join": "union",  "join_pairs": []},   # OX PO lines; PO_HOLADD/PO_OTHER rows + OX status/amount
    {"silver": F554201T, "join": "static", "join_pairs": []},   # Sand PO Number / QC legs
    {"silver": F0911,    "join": "static", "join_pairs": []},   # Carrier PO GL Post flag
    {"silver": F43121,   "join": "static", "join_pairs": []},   # PO Receipt GL Date / GLPost doc
    {"silver": F0101,    "join": "static", "join_pairs": []},   # LOFA rate ABURAT
    {"silver": F4201,    "join": "static", "join_pairs": []},   # report-5 load header attrs
]

# ----------------------------------------------------------------------------
# 4) RUN
# ----------------------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS {}.{}".format(GOLD_LH, GOLD_SCHEMA))

# preflight — confirm every declared Silver source exists before building (the Gold dim_uss_plant is
# checked by load_uss_plant_mcu()).
for _s in FACT_SOURCES:
    print("  source {:<40s} {}".format(_s["silver"],
                                       "OK" if spark.catalog.tableExists(sname(_s["silver"])) else "MISSING"))

# BATCH BUILD — read the full Silver snapshot, run build_fact() once, overwrite the fact.
#   Address dims are REUSED (rpt.dim_address_book role views); dim_uss_plant is built by
#   nb_eso5_gold_dim_uss_plant (PREREQUISITE — read for the SOORDERNO match MCU).
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
