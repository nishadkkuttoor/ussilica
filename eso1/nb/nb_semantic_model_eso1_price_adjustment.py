#!/usr/bin/env python
# coding: utf-8

# ## nb_semantic_model_eso1_price_adjustment
#
# Builds/refreshes the **Direct Lake** semantic model `sales_order_price_adjustment`
# over the single combined fact `rpt.fact_sales_order_price_adjustment`
# (sales-order-line × price-adjustment grain) plus the REUSED ESO1 conformed dims.
#
# SEPARATE model — it does NOT touch `billable_payable_freight`. The two models share the
# same physical `rpt` dimensions (read-only), so nothing about the existing model or its
# reports changes.
#
# ── One fact, two row types ──
# Each fact row is a line × whitelisted-adjustment (or a single base row when the line has
# no whitelisted adjustment). Line-grain values (extended_price, ordered_tons, shipped_tons,
# quantities) REPEAT across a line's adjustment rows, so every line-level measure sums with
# `is_primary_line_row = "Y"` (one row per line) to avoid N× inflation. Adjustment-level
# buckets ignore that flag and iterate the adjustment rows directly — adj_unit_price and the
# line's ordered_tons are on the SAME row (no cross-fact RELATED).
#
# ── Tons ──
# ordered_tons / shipped_tons are PHYSICAL columns on the fact (faithful zero-on-fail UOM
# cascade baked in by nb_eso1_gold_fact_sales_order_price_adjustment). Tons measures are a
# plain SUM of those columns — this model does NOT relate the shared dim_uom_conversion*.
# Product Price is gated to non-zero ordered_tons so freight/zero-conversion rows blank out.
#
# SELF-CONTAINED — declares its own constants inline. Requires semantic-link-labs
# (sempy_labs). Run AFTER the fact + the reused rpt dims exist.

# In[1]:


import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model

MODEL     = "sales_order_price_adjustment"
LAKEHOUSE = "lh_jde_gold"

FACT = "fact_sales_order_price_adjustment"

# NEW (rpt): the combined fact built by nb_eso1_gold_fact_sales_order_price_adjustment.
# REUSED (rpt): conformed dims already built for ESO1 — referenced, NOT rebuilt here.
NEW_TABLES = [FACT]
RPT_TABLES = ["dim_address_ship_to", "dim_address_sold_to", "dim_address_parent",
              "dim_item", "dim_plant", "dim_company",
              "dim_freight_handling_code", "dim_mode_of_transport",
              "dim_category_code_05"]     # snowflake off dim_address_ship_to (ABAC05 → UDC 01/05)
MODEL_TABLES = NEW_TABLES + RPT_TABLES
TABLE_SCHEMAS = ["rpt" for _ in MODEL_TABLES]
print(f"Semantic model : {MODEL}  (Direct Lake, single schema rpt on {LAKEHOUSE})")


# In[2]:


# ── PREFLIGHT: fact + reused dims present before building ──────────────────────
NEW_REQUIRED    = [f"{LAKEHOUSE}.rpt.{FACT}"]
REUSED_REQUIRED = [f"{LAKEHOUSE}.rpt.dim_item", f"{LAKEHOUSE}.rpt.dim_plant",
                   f"{LAKEHOUSE}.rpt.dim_company", f"{LAKEHOUSE}.rpt.dim_mode_of_transport",
                   f"{LAKEHOUSE}.rpt.dim_freight_handling_code"]
REUSED_VIEWS    = [f"{LAKEHOUSE}.rpt.dim_address_ship_to",
                   f"{LAKEHOUSE}.rpt.dim_address_sold_to",
                   f"{LAKEHOUSE}.rpt.dim_address_parent",
                   f"{LAKEHOUSE}.rpt.dim_category_code_05"]

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
        + ". Build the fact with nb_eso1_gold_fact_sales_order_price_adjustment, and ensure the "
          "reused rpt dims (dim_item/dim_plant/dim_company/…) exist.")
if missing_views:
    print("WARN: role views not visible to Spark — they may exist only in the SQL endpoint "
          "(which Direct Lake binds to). If the address/category relationships fail to generate, "
          "recreate the role views first: " + ", ".join(missing_views))
print("✓ preflight passed — required sources present")


# In[3]:


# ── Create the Direct Lake model (single schema; all rpt) ─────────────────────
try:
    labs.directlake.generate_direct_lake_semantic_model(
        dataset=MODEL,
        lakehouse=LAKEHOUSE,
        schema=TABLE_SCHEMAS,
        lakehouse_tables=MODEL_TABLES,
        overwrite=True,
    )
    print("✓ Direct Lake model generated")
except Exception as e:
    print("generate_direct_lake_semantic_model failed (or schema-list unsupported on this "
          "labs version) — create/refresh in the UI with per-table schema, then re-run the "
          "relationship/measure cell. Detail:", e)


# In[4]:


# ── Relationships (all dim → fact, One→Many, single-direction) ────────────────
# (from_table, from_col, to_table, to_col, is_active)
RELATIONSHIPS = [
    ("dim_address_ship_to", "address_number", FACT, "ship_to",               True),   # SDSHAN
    ("dim_address_sold_to", "address_number", FACT, "sold_to",               True),   # SDAN8
    ("dim_address_parent",  "address_number", FACT, "address_number_parent", True),   # SDPA8 (parent-customer selector, e.g. Pioneer)
    ("dim_item",            "item_number_short", FACT, "item_number_short",  True),   # SDITM
    ("dim_plant",           "plant_code",     FACT, "branch_plant",          True),   # SDMCU
    ("dim_company",         "company",        FACT, "company_key_order_no",  True),   # SDKCOO (=CCCO) → currency
    ("dim_freight_handling_code", "freight_handling_code", FACT, "freight_handling_code", True),  # SDFRTH (UDC 42/FR)
    ("dim_mode_of_transport", "mot_code",     FACT, "mode_of_transport",     True),   # SDMOT (UDC 00/TM)
    # snowflake: ship-to category-05 (ABAC05) → UDC 01/05 description, off the ship-to role view
    ("dim_category_code_05", "category_code_05", "dim_address_ship_to", "category_code_05", True),
]

# =============================================================================
# MEASURE CATALOG — all homed on the single combined fact
# -----------------------------------------------------------------------------
# Line-level measures dedup to one row per line with is_primary_line_row = "Y"
# (line values repeat across the adjustment fan). Adjustment-level buckets iterate the
# adjustment rows and multiply adj_unit_price by the line's ordered_tons (same row).
# =============================================================================
MEASURES = {
    # ── line-level (deduped per line via is_primary_line_row; SUMX(FILTER()) keeps page filters) ──
    "Sales Amount":        (f"SUMX(FILTER('{FACT}', '{FACT}'[is_primary_line_row] = \"Y\"), '{FACT}'[extended_price])", "\\$#,0.00", False),
    "Extended Cost":       (f"SUMX(FILTER('{FACT}', '{FACT}'[is_primary_line_row] = \"Y\"), '{FACT}'[extended_cost])", "\\$#,0.00", False),
    "Quantity Shipped":    (f"SUMX(FILTER('{FACT}', '{FACT}'[is_primary_line_row] = \"Y\"), '{FACT}'[quantity_shipped])", "#,0.00", False),
    "Primary Qty Ordered": (f"SUMX(FILTER('{FACT}', '{FACT}'[is_primary_line_row] = \"Y\"), '{FACT}'[primary_quantity_ordered])", "#,0.00", False),
    "Ordered Tons":        (f"SUMX(FILTER('{FACT}', '{FACT}'[is_primary_line_row] = \"Y\"), '{FACT}'[ordered_tons])", "#,0.00", False),
    "Shipped Tons":        (f"SUMX(FILTER('{FACT}', '{FACT}'[is_primary_line_row] = \"Y\"), '{FACT}'[shipped_tons])", "#,0.00", False),
    "Order Lines":         (f"DISTINCTCOUNT('{FACT}'[sales_order_line_key])", "#,0", False),
    # deferred-revenue lines (F49211 UDDEFF set), deduped per line
    "Deferred Revenue":    (f"SUMX(FILTER('{FACT}', '{FACT}'[is_primary_line_row] = \"Y\" && TRIM('{FACT}'[deferred_entries_flag]) <> \"\"), '{FACT}'[extended_price])", "\\$#,0.00", False),
    # ── product tons + price — fast aggregates over the precomputed fact classification.
    #    is_product_line (fact) = A03-priced OR NP/N3 net-priced, non-freight, non-charge/dryer item;
    #    is_primary_line_row dedups the adjustment fan to one row per line.
    #    Product Price is NET of the embedded Freight Hide. ──
    "Total Tons":    (f"SUM('{FACT}'[product_ordered_tons])", "#,0.00", False),
    "Product Price": (f"SUM('{FACT}'[product_ext_price]) - [Freight Hide]", "\\$#,0.00", False),
    "Price Per Ton": ("DIVIDE([Product Price], [Total Tons])", "\\$#,0.00", False),
    # ── freight-line buckets (F/FT lines by SDAITM prefix), deduped per line; Freight also adds
    #    the FRTTAXN/FRTTAXY adjustment rows (adj_unit_price × the line's ordered tons) ──
    "Freight":          (f"SUM('{FACT}'[freight_amount])", "\\$#,0.00", False),
    "Car Charges":      (f"SUM('{FACT}'[car_charges_amount])", "\\$#,0.00", False),
    # dryer-freight bucket (SOP0008) — F/FT freight lines with SDAITM prefix DRY, deduped per line
    "Dryer Freight Charge": (f"SUM('{FACT}'[dryer_freight_amount])", "\\$#,0.00", False),
    # ── adjustment-row buckets (precomputed ALUPRC × tons by print code) ──
    "Non Product":      (f"SUM('{FACT}'[non_product_amount])", "\\$#,0.00", False),
    "AL Severance Tax": (f"SUM('{FACT}'[al_severance_amount])", "\\$#,0.00", False),
    "Misc Billing":     (f"SUM('{FACT}'[misc_billing_amount])", "\\$#,0.00", False),
    # Freight Hide = SUM of the precomputed per-row FRTHIDE amount (adj_unit_price × qty-in-ALUOM).
    "Freight Hide":     (f"SUM('{FACT}'[freight_hide_amount])", "\\$#,0.00", False),
    # ── adjustment-row product price — priced ONLY for FRTHIDE = -(ALUPRC × ordered tons); the row is
    #    kept (0 via zero-hiding format) when the line has a whitelisted adjustment but no FRTHIDE, and
    #    BLANK (no row) when it has no whitelisted adjustment. ──
    "Adj Product Price": (f"VAR FrtHide = SUMX(FILTER('{FACT}', '{FACT}'[adj_based_on_value] <> 0 && '{FACT}'[price_adjustment_type] = \"FRTHIDE\" && TRIM('{FACT}'[adj_uom]) = \"TN\"), - '{FACT}'[adj_unit_price] * '{FACT}'[ordered_tons]) VAR HasWhitelisted = COUNTROWS(FILTER('{FACT}', '{FACT}'[adj_based_on_value] <> 0 && (LEFT('{FACT}'[price_adjustment_type], 2) = \"PP\" || '{FACT}'[price_adjustment_type] = \"A03\" || '{FACT}'[price_adjustment_type] = \"CASLB\" || '{FACT}'[price_adjustment_type] = \"FRTHIDE\" || '{FACT}'[price_adjustment_type] = \"FRTTAXN\" || '{FACT}'[price_adjustment_type] = \"FRTTAXY\" || '{FACT}'[price_adjustment_type] = \"COLPALN\" || '{FACT}'[price_adjustment_type] = \"COLPALT\" || '{FACT}'[price_adjustment_type] = \"ALST\"))) RETURN IF(HasWhitelisted > 0, FrtHide + 0, BLANK())", "\\$#,0.00", False),
    # ── SOP0025 base+adjustment interleave, driven by the disconnected 'Row Type' table ──
    "Price Value": ("VAR rt = SELECTEDVALUE('Row Type'[Row Type]) RETURN SWITCH(rt, \"Product Price\", [Product Price], \"Adjustment\", [Adj Product Price], [Product Price] + [Adj Product Price])", "\\$#,0.00;-\\$#,0.00;", False),
    "Tons Value":  ("VAR rt = SELECTEDVALUE('Row Type'[Row Type]) RETURN SWITCH(rt, \"Product Price\", [Total Tons], \"Adjustment\", BLANK(), [Total Tons])", "#,0.00", False),
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

    # ── SOP0025 disconnected 'Row Type' table — CREATE FIRST, before the measures that reference it
    #    (Price Value / Tons Value use 'Row Type'[Row Type]). Disconnected import DATATABLE, 2 rows,
    #    NO relationship. Robust: try the labs helper, else build it via raw TOM with explicit columns.
    _RT_EXPR = 'DATATABLE("Row Type", STRING, "Sort", INTEGER, {{"Product Price", 0}, {"Adjustment", 1}})'

    def _ensure_row_type():
        if any(t.Name == "Row Type" for t in tom.model.Tables):
            return "already present"
        try:
            tom.add_calculated_table(name="Row Type", expression=_RT_EXPR)
            return "add_calculated_table"
        except Exception as e1:
            # raw-TOM fallback — explicit calculated-table columns so they exist without a recalc
            import Microsoft.AnalysisServices.Tabular as TOM
            t = TOM.Table(); t.Name = "Row Type"
            p = TOM.Partition(); p.Name = "Row Type"
            src = TOM.CalculatedPartitionSource(); src.Expression = _RT_EXPR; p.Source = src
            t.Partitions.Add(p)
            for _cn, _dt in [("Row Type", TOM.DataType.String), ("Sort", TOM.DataType.Int64)]:
                c = TOM.CalculatedTableColumn(); c.Name = _cn; c.SourceColumn = "[" + _cn + "]"; c.DataType = _dt
                t.Columns.Add(c)
            tom.model.Tables.Add(t)
            return "raw TOM (add_calculated_table failed: {})".format(e1)

    try:
        print("✓ Row Type table ensured via", _ensure_row_type())
    except Exception as e:
        print("  ⚠ Row Type NOT created ({}). Add manually in Power BI (New Table): {} — "
              "Price Value / Tons Value error until it exists.".format(e, _RT_EXPR))
    # cosmetic column settings — SEPARATE try so a failure here never drops the table
    try:
        rt = tom.model.Tables["Row Type"]
        rt.Columns["Sort"].IsHidden = True
        rt.Columns["Row Type"].SortByColumn = rt.Columns["Sort"]
    except Exception as e:
        print("  (Row Type column hide/sort-by deferred — set in Desktop: {})".format(e))

    # measures — UPDATE in place if the measure already exists, else add. tom.add_measure throws on a
    # duplicate name (and the except would silently keep the OLD definition), so re-running the cell
    # without a full model regenerate would never apply edits — update-or-add makes redeploys reliable.
    _existing = {m.Name for m in tom.model.Tables[FACT].Measures}
    for name, (dax, fmt, hidden) in MEASURES.items():
        try:
            if name in _existing:
                _m = tom.model.Tables[FACT].Measures[name]
                _m.Expression = dax
                if fmt is not None:
                    _m.FormatString = fmt
                _m.IsHidden = hidden
                print(f"  measure {name} updated")
            else:
                tom.add_measure(table_name=FACT, measure_name=name, expression=dax,
                                format_string=fmt, hidden=hidden)
        except Exception as e:
            print(f"  measure {name} skipped: {e}")

    # hide keys / dedup flag / date-key ints from report view
    HIDE = {
        FACT: ["price_adjustment_key", "sales_order_line_key", "is_primary_line_row",
               "is_product_line", "product_ordered_tons", "product_ext_price", "freight_hide_amount",
               "non_product_amount", "al_severance_amount", "misc_billing_amount",
               "freight_amount", "car_charges_amount", "dryer_freight_amount", "conversion_to_tons_rate",
               "order_date_key", "requested_date_key", "ship_date_key",
               "invoice_date_key", "gl_date_key"],
    }
    for tbl, cols in HIDE.items():
        for c in cols:
            try:
                tom.model.Tables[tbl].Columns[c].IsHidden = True
            except Exception:
                pass

print("✓ relationships + measures applied on", MODEL)


# In[5]:


# ── Reference: report binding notes (no execution) ────────────────────────────
# • All report-specific selection is a PAGE FILTER on the fact (nothing baked into Gold):
#     - next status:     fact[next_status_num]  (e.g. = 620 / = 999 / between 574 and 620 / = 580)
#     - last status:     fact[last_status_num] < 980
#     - document type:   fact[order_type]  (include SO/CO or exclude SX/ST/SG)
#     - line type:       fact[line_type]  (exclude F/FT where the report does)
#     - GL / order date: fact[gl_date] / fact[order_date]  (date-range slicer)
#     - branch / customer: dim_plant[plant_code] (061/341) / dim_address_parent (Pioneer 10059472)
#     - search-type band:  dim_address_ship_to[address_type_01]  (ABAT1: A–P / R–ZZZ where the report joins F0101)
#     - item segment:      fact[item_segment_4] <> 'ZZ' ; GL class fact[gl_class]
# • SOP620 bucket columns: [Total Tons] [Non Product] [AL Severance Tax] [Misc Billing] [Freight]
#   [Car Charges] [Freight Hide] [Product Price] [Price Per Ton] — all measures above.
# • SOP0025 interleave: Matrix rows = line attributes then 'Row Type'[Row Type]; values = [Price Value] [Tons Value].
# • Currency: dim_company (CCCRCD). Names/city/state: dim_address_ship_to / dim_address_parent.
# • This model is independent of billable_payable_freight — the shared rpt dims are read-only here.
print("See cell comments for report binding notes.")
