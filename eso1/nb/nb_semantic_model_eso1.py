#!/usr/bin/env python
# coding: utf-8

# ## nb_semantic_model_eso1
#
# Builds/refreshes the **Direct Lake** semantic model `billable_payable_freight`
# over the ESO1 Gold star. Creates the Direct Lake model, then the relationships
# (address/plant/item — NO date dimension), measures (freight deduped per shipment),
# and report-hygiene settings. Dates are the fact's raw date columns, sliced directly.
# Design: docs/ESO1_gold_layer_design.md §7.
#
# TWO facts share the conformed dims in this one model:
#   • fact_sales_order_freight   — Billable v Payable Freight (shipment/order-line grain)
#   • fact_sales_commission      — SOP0027 Commission (commission-line grain; F4211 line
#                                  metrics deduped via the is_primary_commission_line flag)
# The commission fact reuses dim_address_ship_to / dim_address_sold_to / dim_plant / dim_item
# and adds a salesperson role view (dim_address_salesperson) over dim_address_book.
#
# SELF-CONTAINED — no %run; declares its own constants inline. One of the 4
# independent nb/ notebooks (alongside nb_eso1_gold_streaming / nb_validate_gold_eso1
# / nb_maintenance_gold_eso1); none depends on another resolving by name.
# Requires semantic-link-labs (sempy_labs). Run AFTER nb_eso1_gold_streaming has
# seeded the tables (and the reused rpt dims exist).

# In[1]:


import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model

MODEL     = "billable_payable_freight"     # naming PDF: semantic model = {descriptive_purpose}
LAKEHOUSE = "lh_jde_gold"

# Single-schema Direct Lake model — ALL ESO1 Gold now lives in `rpt` (aligned 2026-07-16,
# alongside ESO4/ESO5 and the reused conformed dims):
#   NEW (rpt): fact_sales_order_freight, fact_sales_commission, dim_item
#   REUSED (rpt): dim_address_book role views + dim_plant + dim_mode_of_transport — NOT rebuilt, just referenced
# NOTE (2026-07-23): NO date dimension. Dates are the fact's raw date columns, sliced
#   directly (date-range + relative-date slicers). No time-intelligence / marked date table.
NEW_TABLES = ["fact_sales_order_freight", "fact_sales_commission", "dim_item",
              "dim_category_code_10",        # UDC 01/10 → category_code_10 description (built by nb_eso1_gold_dim_category_code_10)
              "dim_category_code_05",        # UDC 01/05 → category_code_05 description (built by nb_eso1_gold_dim_category_code_05)
              "dim_freight_handling_code"]   # UDC 42/FR → freight_handling_code description (built by nb_eso1_gold_dim_freight_handling_code)
RPT_TABLES = ["dim_address_ship_to", "dim_address_sold_to", "dim_address_carrier",
              "dim_address_parent", "dim_address_book_destination", "dim_address_salesperson",
              "dim_plant", "dim_mode_of_transport"]
MODEL_TABLES = NEW_TABLES + RPT_TABLES
def schema_of(t): return "rpt"                         # everything in rpt now
TABLE_SCHEMAS = [schema_of(t) for t in MODEL_TABLES]
print(f"Semantic model : {MODEL}  (Direct Lake, single schema rpt on {LAKEHOUSE})")


# In[2]:


# ── PREFLIGHT: REUSED-dimension + source checks before building the model ─────
# The model relates the fact to REUSED dims (rpt.dim_address_book role views +
# rpt.dim_plant) — these are NOT built here. Abort early with a clear message if a
# required source is missing rather than failing mid-generation.
NEW_REQUIRED    = [f"{LAKEHOUSE}.rpt.fact_sales_order_freight",
                   f"{LAKEHOUSE}.rpt.fact_sales_commission",
                   f"{LAKEHOUSE}.rpt.dim_item",
                   f"{LAKEHOUSE}.rpt.dim_category_code_10",
                   f"{LAKEHOUSE}.rpt.dim_category_code_05",
                   f"{LAKEHOUSE}.rpt.dim_freight_handling_code"]
REUSED_REQUIRED = [f"{LAKEHOUSE}.rpt.dim_address_book", f"{LAKEHOUSE}.rpt.dim_plant",
                   f"{LAKEHOUSE}.rpt.dim_mode_of_transport"]
REUSED_VIEWS    = [f"{LAKEHOUSE}.rpt.dim_address_ship_to",
                   f"{LAKEHOUSE}.rpt.dim_address_sold_to",
                   f"{LAKEHOUSE}.rpt.dim_address_carrier",
                   f"{LAKEHOUSE}.rpt.dim_address_parent",
                   f"{LAKEHOUSE}.rpt.dim_address_book_destination",
                   # salesperson role view (SCSLSP) — mirror ship_to/sold_to/carrier over dim_address_book.
                   # If absent, create it via nb_dim_address_book before the commission rels can resolve.
                   f"{LAKEHOUSE}.rpt.dim_address_salesperson"]

def _exists(fqn):
    try:
        if spark.catalog.tableExists(fqn):
            return True
    except Exception:
        pass
    try:                                   # role views may be SQL-endpoint-only
        spark.read.table(fqn).limit(1).collect()
        return True
    except Exception:
        return False

missing_hard  = [t for t in NEW_REQUIRED + REUSED_REQUIRED if not _exists(t)]
missing_views = [v for v in REUSED_VIEWS if not _exists(v)]

for t in NEW_REQUIRED + REUSED_REQUIRED:
    print(f"  {'OK     ' if t not in missing_hard else 'MISSING'} : {t}")
for v in REUSED_VIEWS:
    print(f"  {'OK     ' if v not in missing_views else 'no-spark'} : {v}  (reused role view)")

if missing_hard:
    raise Exception(
        "Cannot build the semantic model — required tables missing: " + ", ".join(missing_hard)
        + ". Seed the new tables with nb_eso1_gold_streaming; build the REUSED dims "
          "(rpt.dim_address_book, rpt.dim_plant) via their old_nb jobs first.")
if missing_views:
    print("WARN: role views not visible to Spark — they may exist only in the SQL endpoint "
          "(which Direct Lake binds to). If the model's address relationships fail to "
          "generate, recreate the role views (nb_dim_address_book): " + ", ".join(missing_views))
print("✓ preflight passed — required sources present")


# In[3]:


# ── Create the Direct Lake model (single schema; all rpt) ─────────────────────
try:
    labs.directlake.generate_direct_lake_semantic_model(
        dataset=MODEL,
        lakehouse=LAKEHOUSE,
        schema=TABLE_SCHEMAS,                 # per-table schema list (all rpt now)
        lakehouse_tables=MODEL_TABLES,
        overwrite=True,
    )
    print("✓ Direct Lake model generated")
except Exception as e:
    print("generate_direct_lake_semantic_model failed (or schema-list unsupported on this "
          "labs version) — create/refresh in the UI with per-table schema, then re-run the "
          "relationship/measure cells. Detail:", e)


# In[4]:


# ── Relationships + role-played dates + measures via the TOM ──────────────────
FACT      = "fact_sales_order_freight"
COMM_FACT = "fact_sales_commission"      # SOP0027 Commission (commission-line grain)

# (from_table, from_col, to_table, to_col, is_active) — REUSED dims as role views
RELATIONSHIPS = [
    ("dim_address_ship_to", "address_number",    FACT, "ship_to",          True),
    ("dim_address_sold_to", "address_number",    FACT, "bill_to",          True),   # bill-to role view
    ("dim_address_carrier", "address_number",    FACT, "carrier_number",        True),
    ("dim_address_parent",  "address_number",    FACT, "address_number_parent", True),  # parent-customer role view
    ("dim_address_book_destination", "address_number", FACT, "destination_port", True),  # dest-point role view (was dest_point_name_alpha)
    ("dim_item",            "item_number_short", FACT, "item_number_short", True),
    ("dim_plant",           "plant_code",        FACT, "branch_plant",     True),
    ("dim_mode_of_transport", "mot_code",        FACT, "mode_of_transport", True),  # UDC 00/TM code -> description
    ("dim_category_code_05",  "category_code_05", FACT, "category_code_05",  True),  # ABAC05 (UDC 01/05) -> description
    ("dim_freight_handling_code", "freight_handling_code", FACT, "freight_handling_code", True),  # SDFRTH (UDC 42/FR) -> description
    # ── fact_sales_commission (SOP0027) — conformed dims shared with the freight fact + a salesperson role view.
    #    A dim role view relating to BOTH facts is valid (star with two facts); each rel is dim→fact, single-direction.
    ("dim_address_salesperson", "address_number",    COMM_FACT, "salesperson",       True),  # SCSLSP (address-book number)
    ("dim_address_ship_to",     "address_number",    COMM_FACT, "ship_to",           True),  # SDSHAN
    ("dim_address_sold_to",     "address_number",    COMM_FACT, "sold_to",           True),  # SHAN8  (F4201 sold-to)
    ("dim_plant",               "plant_code",        COMM_FACT, "branch_plant",      True),  # SDMCU / SCMCU
    ("dim_item",                "item_number_short", COMM_FACT, "item_number_short", True),  # SDITM / SCITM
    ("dim_category_code_10",    "category_code_10",  COMM_FACT, "category_code_10",  True),  # ABAC10 (UDC 01/10) -> description
    # NO date relationships — there is no date dimension (2026-07-23). Each fact's raw
    # date columns are sliced directly; weekly grouping uses the fact's ship_year_week column.
]

# =============================================================================
# MEASURE CATALOG  (name · format · DAX · description)
# -----------------------------------------------------------------------------
# All measures are defined on FACT = fact_sales_order_freight. Freight-$ measures
# are deduped to SHIPMENT grain via SUMX(VALUES(shipment_number), CALCULATE(MAX(col)))
# because the freight buckets are denormalized and repeat across a shipment's order
# lines — a plain SUM would inflate them. Order-line measures are plain SUM/COUNT.
#
#  #  Measure                    Format    DAX                                                                                            Description
# ── FREIGHT $ (shipment-deduped) ─────────────────────────────────────────────
#  1 Billable Freight           $#,0      SUMX(VALUES(FACT[shipment_number]), CALCULATE(MAX(FACT[billable_freight])))                     Freight billed to the customer, once per shipment
#  2 Billable Fuel              $#,0      SUMX(VALUES(FACT[shipment_number]), CALCULATE(MAX(FACT[billable_fuel])))                        Fuel surcharge billed to the customer
#  3 Total Billable             $#,0      SUMX(VALUES(FACT[shipment_number]), CALCULATE(MAX(FACT[total_billable])))                       Total billed to customer = billable freight + fuel
#  4 Payable Freight            $#,0      SUMX(VALUES(FACT[shipment_number]), CALCULATE(MAX(FACT[payable_freight])))                      Freight owed to the carrier
#  5 Payable Fuel               $#,0      SUMX(VALUES(FACT[shipment_number]), CALCULATE(MAX(FACT[payable_fuel])))                         Fuel surcharge payable to the carrier
#  6 Total Payable              $#,0      SUMX(VALUES(FACT[shipment_number]), CALCULATE(MAX(FACT[total_payable])))                        Total owed to carrier = payable freight + fuel
#  7 Total Freight              $#,0      SUMX(VALUES(FACT[shipment_number]), CALCULATE(MAX(FACT[total_freight])))                        All charge codes for the shipment (raw FHNAMT total, H2)
# ── DERIVED MARGIN / COUNTS ──────────────────────────────────────────────────
#  8 Freight Variance           $#,0      [Billable Freight] - [Payable Freight]                                                         Freight-only margin (billable - payable)
#  9 Total Variance             $#,0      [Total Billable] - [Total Payable]                                                             Full freight+fuel margin
# 10 Freight CM %               0.0%      DIVIDE([Freight Variance], [Billable Freight])                                                 Freight contribution margin %
# 11 Total CM %                 0.0%      DIVIDE([Total Variance], [Total Billable])                                                     Total contribution margin %
# 12 Freight Shipments          #,0       DISTINCTCOUNT(FACT[shipment_number])                                                           Count of distinct shipments in context
# ── ORDER-LINE VOLUME (line grain) ───────────────────────────────────────────
# 13 Order Lines                #,0       COUNTROWS(FACT)                                                                                Count of order lines
# 14 Quantity Shipped Tons      #,0.00    SUM(FACT[quantity_shipped_tons])                                                               Shipped quantity converted to tons
# 15 Price Quantity Shipped     $#,0      SUM(FACT[price_quantity_shipped])                                                              Extended value = price x quantity shipped
# ── BOL WEIGH-TICKET WEIGHTS (F5549002, line grain) ──────────────────────────
# 16 Gross Weight               #,0       SUM(FACT[gross_weight])                                                                        Sum of BOL gross weight across a load's lines
# 17 Catch Weight               #,0       SUM(FACT[catch_weight])                                                                        Sum of BOL catch (scaled) weight
# ── DATA QUALITY ─────────────────────────────────────────────────────────────
# 18 Lines Missing Conversion   #,0       CALCULATE(COUNTROWS(FACT), FACT[missing_conversion_flag] = "Y")                                Lines with no tons-conversion rate
# ── ADDRESS DISPLAY (reused role views) ──────────────────────────────────────
# 19 Carrier Name               (text)    SELECTEDVALUE(dim_address_carrier[address_number]) & " - " & SELECTEDVALUE(dim_address_carrier[name_alpha])   Carrier as "20000049 - FUNDIS COMPANY INC"
# 20 Parent Name                (text)    SELECTEDVALUE(dim_address_parent[address_number]) & " - " & SELECTEDVALUE(dim_address_parent[name_alpha])     Parent customer, same format
# ── AGING ────────────────────────────────────────────────────────────────────
# 21 Days Past Due              #,0       DATEDIFF(MAX(FACT[requested_date]), TODAY(), DAY)                                              Days a line is past its requested date (SM Trucking-Past Due; as-of = TODAY())
#
# NOTE — no date-role measures: there is no date dimension. To view $ by GL/invoice
#   date, slice the fact's raw gl_date / invoice_date column on the visual.
# VARIATION-SPECIFIC measures (report-level, NOT built here — see PAGE_FILTER_CHEATSHEET.md):
#   Address Rate = SELECTEDVALUE(FACT[address_rate])                  -- 9 status-529 customer-rate reports (ABURAT)
#   Order with Multiple Shipments  -- DISTINCTCOUNT(FACT[shipment_number]) per order,  visual filter > 1
#   Shipment with Multiple Orders  -- DISTINCTCOUNT(FACT[order_number]) per shipment,  visual filter > 1
#   Days Since Invoice = DATEDIFF(MAX(FACT[invoice_date]), TODAY(), DAY)   -- Days-Since-Invoice report
#   Transaction Quantity = SUM(FACT[transaction_quantity])            -- Standridge / Sto Corp / Thai Tan / Tri-Iso (SDUORG)
# =============================================================================

# name -> (DAX, format, hidden)
MEASURES = {
    # freight $ — deduped to shipment grain (correct under any line-level filter)
    "Billable Freight": (f"SUMX(VALUES('{FACT}'[shipment_number]), CALCULATE(MAX('{FACT}'[billable_freight])))", "\\$#,0", False),
    "Billable Fuel":    (f"SUMX(VALUES('{FACT}'[shipment_number]), CALCULATE(MAX('{FACT}'[billable_fuel])))",    "\\$#,0", False),
    "Total Billable":   (f"SUMX(VALUES('{FACT}'[shipment_number]), CALCULATE(MAX('{FACT}'[total_billable])))",   "\\$#,0", False),
    "Payable Freight":  (f"SUMX(VALUES('{FACT}'[shipment_number]), CALCULATE(MAX('{FACT}'[payable_freight])))",  "\\$#,0", False),
    "Payable Fuel":     (f"SUMX(VALUES('{FACT}'[shipment_number]), CALCULATE(MAX('{FACT}'[payable_fuel])))",     "\\$#,0", False),
    "Total Payable":    (f"SUMX(VALUES('{FACT}'[shipment_number]), CALCULATE(MAX('{FACT}'[total_payable])))",    "\\$#,0", False),
    # all-charge-code shipment freight total (H2) — dedup at shipment grain exactly like the buckets above, so the
    # combined-freight reports (Baseline Finance, DE Orders, BP Freight) get the raw FHNAMT total, not just billable+payable
    "Total Freight":    (f"SUMX(VALUES('{FACT}'[shipment_number]), CALCULATE(MAX('{FACT}'[total_freight])))",    "\\$#,0", False),
    "Freight Variance": ("[Billable Freight] - [Payable Freight]", "\\$#,0", False),
    "Total Variance":   ("[Total Billable] - [Total Payable]",     "\\$#,0", False),
    "Freight CM %":     ("DIVIDE([Freight Variance], [Billable Freight])", "0.0%", False),
    "Total CM %":       ("DIVIDE([Total Variance], [Total Billable])",     "0.0%", False),
    "Freight Shipments": (f"DISTINCTCOUNT('{FACT}'[shipment_number])", "#,0", False),
    # order-line measures (line grain — plain SUM is correct)
    "Order Lines":            (f"COUNTROWS('{FACT}')", "#,0", False),
    "Quantity Shipped Tons":  (f"SUM('{FACT}'[quantity_shipped_tons])", "#,0.00", False),
    "Price Quantity Shipped": (f"SUM('{FACT}'[price_quantity_shipped])", "\\$#,0", False),
    # BOL weigh-ticket weights (M5, F5549002) — line grain, additive across a load's lines (max_weight is a
    # per-line capacity, not summable, so it stays a column not a measure)
    "Gross Weight":           (f"SUM('{FACT}'[gross_weight])", "#,0", False),
    "Catch Weight":           (f"SUM('{FACT}'[catch_weight])", "#,0", False),
    "Lines Missing Conversion": (f"CALCULATE(COUNTROWS('{FACT}'), '{FACT}'[missing_conversion_flag] = \"Y\")", "#,0", False),
    # NOTE: no date-role measures (no dim_date). To view $ by GL/invoice date, slice the
    # fact's raw gl_date / invoice_date column directly on the visual.
    # address-book role-view names, displayed as "address_number - name_alpha"
    # (e.g. "20000049 - FUNDIS COMPANY INC"); SELECTEDVALUE collapses to the single
    # related row under a fact-line filter context.
    "Carrier Name": ("SELECTEDVALUE(dim_address_carrier[address_number]) & \" - \" & SELECTEDVALUE(dim_address_carrier[name_alpha])", None, False),
    "Parent Name":  ("SELECTEDVALUE(dim_address_parent[address_number]) & \" - \" & SELECTEDVALUE(dim_address_parent[name_alpha])",   None, False),
    # days a line is past its requested date; report-level as-of = TODAY() (positive = past due)
    "Days Past Due": (f"DATEDIFF(MAX('{FACT}'[requested_date]), TODAY(), DAY)", "#,0", False),
}

# =============================================================================
# COMMISSION MEASURE CATALOG  —  fact_sales_commission (SOP0027)
# -----------------------------------------------------------------------------
# GRAIN = one row per F42005 commission line (SCKCOO+SCDCTO+SCDOCO+SCLNID+SCCMLN).
# Two families, and they DO NOT share a grain — this is the SOP0027 fan-trap that
# Hubble solves with its two-subquery DISTINCT split:
#   • F42005 commission amounts (SCTOTL/SCLRCS/SCCOMA) are TRUE commission-line grain
#     → plain SUM.
#   • F4211 line metrics (SDSOQS/SDAEXP/SDECST/SDPQOR) are ORDER-LINE grain but are
#     DENORMALIZED onto every commission line of that order line, so a plain SUM
#     multiplies them by the number of commission lines. Dedup to one row per order
#     line via the physical is_primary_commission_line = "Y" flag (the fact sets 'Y'
#     on exactly one commission line per KCOO+DCTO+DOCO+LNID). NO calculated column is
#     needed (and Direct Lake wouldn't support one) — the flag already exists on the fact.
# Hubble scaled SCTOTL/SCLRCS/SCCOMA ×0.01, SCCPCT ÷1000, SDAEXP/SDECST ×ShiftFactor →
#   Silver already applies the implied-decimal decode, so measures are plain SUM (no ×0.01).
#   ⚠ H1: shift_factor_applied = 1.0 placeholder on the line amounts (SDAEXP/SDECST) — see design §11.
#
#  Report col  Measure                          Source   Format   DAX
# ── F42005 commission amounts (commission-line grain — plain SUM) ─────────────
#  ReportCol1  Commission Sales Line Amount     SCTOTL   $#,0     SUM(COMM[amount_sales_total_line])
#  ReportCol2  Commission Line Cost             SCLRCS   $#,0     SUM(COMM[amount_sales_line_total_cost])
#  ReportCol3  Commission Amount                SCCOMA   $#,0     SUM(COMM[amount_commission])
# ── F4211 line context (order-line grain — dedup via is_primary_commission_line) ─
#  ReportCol4  Commission Quantity Shipped      SDSOQS   #,0.00   CALCULATE(SUM(COMM[quantity_shipped]),        COMM[is_primary_commission_line]="Y")
#  ReportCol5  Commission Extended Price        SDAEXP   $#,0     CALCULATE(SUM(COMM[extended_price]),          COMM[is_primary_commission_line]="Y")
#  ReportCol6  Commission Extended Cost         SDECST   $#,0     CALCULATE(SUM(COMM[extended_cost]),           COMM[is_primary_commission_line]="Y")
#  ReportCol7  Commission Primary Qty Ordered   SDPQOR   #,0.00   CALCULATE(SUM(COMM[primary_quantity_ordered]),COMM[is_primary_commission_line]="Y")
# ── SALESPERSON DISPLAY (role view) ──────────────────────────────────────────
#              Salesperson Name                 SCSLSP   (text)   SELECTEDVALUE(dim_address_salesperson[address_number]) & " - " & SELECTEDVALUE(dim_address_salesperson[name_alpha])
#
# Group-by / slicer attributes (NOT measures — the report's remaining SOP0027 columns):
#   percent_commission (SCCPCT), commission_code_type (SCCCTY), category_code_10 (ABAC10 code;
#   its description = dim_category_code_10[category_code_10_desc] via the UDC 01/10 relationship),
#   sales_reporting_code_05 (SDSRP5), branch_plant (SDMCU), uom_primary/uom_pricing,
#   line_type (SDLNTY), item_number_short (SDITM), second_item_number (SDLITM),
#   invoice_number (SDDOC), gl_date (SDDGL), actual_ship_date (SDADDJ) — slice these directly.
# =============================================================================

# name -> (DAX, format, hidden)  — all on COMM_FACT
COMM_MEASURES = {
    # F42005 commission-line grain — one row per commission line, not repeated → plain SUM
    "Commission Sales Line Amount":  (f"SUM('{COMM_FACT}'[amount_sales_total_line])",      "\\$#,0", False),
    "Commission Line Cost":          (f"SUM('{COMM_FACT}'[amount_sales_line_total_cost])", "\\$#,0", False),
    "Commission Amount":             (f"SUM('{COMM_FACT}'[amount_commission])",            "\\$#,0", False),
    # F4211 order-line context denormalized onto every commission line — dedup to the ONE
    # primary commission line per order line, else N× inflation (the SOP0027 fan-trap)
    "Commission Quantity Shipped":   (f"CALCULATE(SUM('{COMM_FACT}'[quantity_shipped]), '{COMM_FACT}'[is_primary_commission_line] = \"Y\")",         "#,0.00", False),
    "Commission Extended Price":     (f"CALCULATE(SUM('{COMM_FACT}'[extended_price]), '{COMM_FACT}'[is_primary_commission_line] = \"Y\")",           "\\$#,0", False),
    "Commission Extended Cost":      (f"CALCULATE(SUM('{COMM_FACT}'[extended_cost]), '{COMM_FACT}'[is_primary_commission_line] = \"Y\")",            "\\$#,0", False),
    "Commission Primary Qty Ordered":(f"CALCULATE(SUM('{COMM_FACT}'[primary_quantity_ordered]), '{COMM_FACT}'[is_primary_commission_line] = \"Y\")", "#,0.00", False),
    # salesperson display, same "number - name" pattern as Carrier/Parent Name
    "Salesperson Name": ("SELECTEDVALUE(dim_address_salesperson[address_number]) & \" - \" & SELECTEDVALUE(dim_address_salesperson[name_alpha])", None, False),
}

with connect_semantic_model(dataset=MODEL, readonly=False) as tom:
    # relationships
    for ft, fc, tt, tc, active in RELATIONSHIPS:
        try:
            tom.add_relationship(
                from_table=ft, from_column=fc, to_table=tt, to_column=tc,
                from_cardinality="One", to_cardinality="Many",
                cross_filtering_behavior="OneDirection", is_active=active)
        except Exception as e:
            print(f"  rel {ft}->{tt} skipped: {e}")

    # measures — freight fact
    for name, (dax, fmt, hidden) in MEASURES.items():
        try:
            tom.add_measure(table_name=FACT, measure_name=name, expression=dax,
                            format_string=fmt, hidden=hidden)
        except Exception as e:
            print(f"  measure {name} skipped: {e}")

    # measures — commission fact (SOP0027)
    for name, (dax, fmt, hidden) in COMM_MEASURES.items():
        try:
            tom.add_measure(table_name=COMM_FACT, measure_name=name, expression=dax,
                            format_string=fmt, hidden=hidden)
        except Exception as e:
            print(f"  measure {name} skipped: {e}")

    # No date table to mark — the model has no date dimension (2026-07-23).

    # hide keys / audit from report view (reused rpt dims keep their owners' hygiene).
    # The *_date_key ints remain on the fact but are now unused (no date dim) — hide them
    # so authors slice the raw date columns instead.
    HIDE = {
        FACT: ["sales_order_line_key", "order_scope_key",
               "order_date_key", "requested_date_key", "scheduled_pick_date_key",
               "promised_ship_date_key", "ship_date_key", "gl_date_key", "invoice_date_key",
               "cancel_date_key", "line_price_effective_date_key", "header_price_effective_date_key",
               "earliest_pickup_date_key", "latest_delivery_date_key",
               "shift_factor_applied"],
        # commission fact — hide keys, the retained-but-unused date_key ints, the dedup helper
        # flag (is_primary_commission_line drives the measures, not a report field), and lineage.
        COMM_FACT: ["sales_commission_key", "order_scope_key",
                    "commission_paid_date_key", "gl_date_key", "ship_date_key",
                    "is_primary_commission_line", "shift_factor_applied"],
        # No audit columns exist on the facts or dim_item (record_hash/is_deleted/source_commit_timestamp/
        # gold_updated_timestamp were removed in v2.2) — nothing to hide there.
        "dim_item": [],
    }
    for tbl, cols in HIDE.items():
        for c in cols:
            try:
                tom.model.Tables[tbl].Columns[c].IsHidden = True
            except Exception:
                pass

print("✓ relationships + measures applied. (No is_deleted filter needed — Silver soft-deletes are already "
      "excluded upstream in load_silver_table; the fact stores no is_deleted column.)")


# In[5]:


# ── Reference: report binding notes (no execution) ────────────────────────────
# • Page filter on every page:  fact_sales_order_freight[is_deleted] = False
# • Weekly cadence: group/slice by fact_sales_order_freight[ship_year_week] (Mon–Sun, derived on the fact).
# • Date filters: slice the fact's raw date columns directly (actual_ship_date/gl_date/invoice_date/…),
#   using a relative-date slicer for today/yesterday/MTD/YTD. No date dimension / time-intelligence.
# • Company slicer: fact_sales_order_freight[company]  (degenerate; SDCO filter value).
# • Freight $ visuals: use the MEASURES above (shipment-deduped) — never raw bucket columns.
# • Carrier/Parent/Ship-To names: dim_address_carrier[name_alpha] / dim_address_parent[name_alpha]
#   / dim_address_ship_to[name_alpha] (reused dim_address_book role views); or the [Carrier Name] /
#   [Parent Name] measures for card/tooltip contexts.
# • Plant: dim_plant[plant_name]. Ship-To address: dim_address_ship_to[city/state/...].
# • Reused role views are over rpt.dim_address_book (one physical dim) — Direct Lake reads them via the
#   SQL endpoint; if a view forces DirectQuery fallback, relate to the physical dim_address_book with
#   USERELATIONSHIP instead. Keep visuals on measures + dim attributes.
print("See cell comments for report binding notes.")
