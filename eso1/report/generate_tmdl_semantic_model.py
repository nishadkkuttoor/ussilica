#!/usr/bin/env python
# Generates report/billable_payable_freight.SemanticModel (Direct Lake TMDL) for the
# TWO-FACT ESO1 model (Billable v Payable Freight + Sales Commission) — reconciled to
# the hand-maintained twin on 2026-07-26: 12 tables / 14 relationships / 28 measures,
# mixed schema (freight fact + dim_item in otc; everything else rpt), NO is_deleted.
# Data below is transcribed from the twin; regenerating reproduces it (deterministic
# uuid5 lineageTags). Run:  python report/generate_tmdl_semantic_model.py
import os, uuid, json

ROOT = os.path.join(os.path.dirname(__file__), "billable_payable_freight.SemanticModel")
DEFN = os.path.join(ROOT, "definition")
TBLS = os.path.join(DEFN, "tables")
NS = uuid.UUID("e5072222-0000-4000-8000-000000000000")
def tag(s): return str(uuid.uuid5(NS, s))
def w(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)

FREIGHT = "fact_sales_order_freight"
COMMISSION = "fact_sales_commission"

# Mixed-schema Direct Lake: freight fact + dim_item live in otc; everything else in rpt
# (the reused conformed dims + the commission fact). Matches the hand twin exactly.
OTC_TABLES = {'dim_item', 'fact_sales_order_freight'}
def schema_of(t): return "otc" if t in OTC_TABLES else "rpt"

# Each table: cols = [(name, dataType, isHidden, sortByColumn|None)],
#             measures = [(name, dax, formatString|None, displayFolder|None)]
# Column/measure ORDER is significant — kept verbatim from the twin.
TABLES = {
    'fact_sales_order_freight': {
      "cols": [
        ('sales_order_line_key', 'string', True, None),
        ('order_scope_key', 'string', True, None),
        ('company', 'string', False, None),
        ('company_key_order_no', 'string', True, None),
        ('order_type', 'string', False, None),
        ('order_number', 'int64', False, None),
        ('line_number', 'double', False, None),
        ('shipment_number', 'int64', False, None),
        ('bol_number', 'string', False, None),
        ('invoice_number', 'int64', False, None),
        ('freight_handling_code', 'string', False, None),
        ('mode_of_transport', 'string', False, None),
        ('destination_port', 'int64', True, None),
        ('route_number', 'int64', False, None),
        ('ship_to', 'int64', True, None),
        ('bill_to', 'int64', True, None),
        ('carrier_number', 'int64', True, None),
        ('item_number_short', 'int64', True, None),
        ('branch_plant', 'string', True, None),
        ('ship_date_key', 'int64', True, None),
        ('gl_date_key', 'int64', True, None),
        ('invoice_date_key', 'int64', True, None),
        ('actual_ship_date', 'dateTime', False, None),
        ('ship_year_week', 'string', False, None),
        ('gl_date', 'dateTime', False, None),
        ('invoice_date', 'dateTime', False, None),
        ('second_item_number', 'string', False, None),
        ('line_type', 'string', True, None),
        ('item_name', 'string', False, None),
        ('uom', 'string', False, None),
        ('conversion_to_tons_rate', 'double', True, None),
        ('missing_conversion_flag', 'string', False, None),
        ('quantity_shipped', 'double', False, None),
        ('quantity_shipped_tons', 'double', False, None),
        ('price_per_unit', 'double', False, None),
        ('price_quantity_shipped', 'double', False, None),
        ('major_prod_code', 'string', False, None),
        ('minor_prod_code', 'string', False, None),
        ('freight_factor_value', 'double', True, None),
        ('billable_freight', 'double', True, None),
        ('billable_fuel', 'double', True, None),
        ('total_billable', 'double', True, None),
        ('payable_freight', 'double', True, None),
        ('payable_fuel', 'double', True, None),
        ('total_payable', 'double', True, None),
        ('total_freight', 'double', True, None),
        ('gross_weight', 'double', True, None),
        ('catch_weight', 'double', True, None),
        ('address_number_parent', 'int64', True, None),
        ('freight_variance', 'double', True, None),
        ('total_variance', 'double', True, None),
        ('is_primary_shipment_line', 'string', True, None),
      ],
      "measures": [
        ('Billable Freight', "SUMX(VALUES('fact_sales_order_freight'[shipment_number]), CALCULATE(MAX('fact_sales_order_freight'[billable_freight])))", '\\$#,0;-\\$#,0', 'Freight'),
        ('Billable Fuel', "SUMX(VALUES('fact_sales_order_freight'[shipment_number]), CALCULATE(MAX('fact_sales_order_freight'[billable_fuel])))", '\\$#,0;-\\$#,0', 'Freight'),
        ('Total Billable', "SUMX(VALUES('fact_sales_order_freight'[shipment_number]), CALCULATE(MAX('fact_sales_order_freight'[total_billable])))", '\\$#,0;-\\$#,0', 'Freight'),
        ('Payable Freight', "SUMX(VALUES('fact_sales_order_freight'[shipment_number]), CALCULATE(MAX('fact_sales_order_freight'[payable_freight])))", '\\$#,0;-\\$#,0', 'Freight'),
        ('Payable Fuel', "SUMX(VALUES('fact_sales_order_freight'[shipment_number]), CALCULATE(MAX('fact_sales_order_freight'[payable_fuel])))", '\\$#,0;-\\$#,0', 'Freight'),
        ('Total Payable', "SUMX(VALUES('fact_sales_order_freight'[shipment_number]), CALCULATE(MAX('fact_sales_order_freight'[total_payable])))", '\\$#,0;-\\$#,0', 'Freight'),
        ('Freight Variance', '[Billable Freight] - [Payable Freight]', '\\$#,0;-\\$#,0', 'Freight'),
        ('Total Variance', '[Total Billable] - [Total Payable]', '\\$#,0;-\\$#,0', 'Freight'),
        ('Freight CM %', 'DIVIDE([Freight Variance], [Billable Freight])', '0.0%', 'Freight'),
        ('Total CM %', 'DIVIDE([Total Variance], [Total Billable])', '0.0%', 'Freight'),
        ('Freight Shipments', "DISTINCTCOUNT('fact_sales_order_freight'[shipment_number])", '#,0', 'Counts'),
        ('Order Lines', "COUNTROWS('fact_sales_order_freight')", '#,0', 'Counts'),
        ('Quantity Shipped Tons', "SUM('fact_sales_order_freight'[quantity_shipped_tons])", '#,0.00', 'Volume'),
        ('Price Quantity Shipped', "SUM('fact_sales_order_freight'[price_quantity_shipped])", '\\$#,0;-\\$#,0', 'Volume'),
        ('Lines Missing Conversion', 'CALCULATE([Order Lines], \'fact_sales_order_freight\'[missing_conversion_flag]="Y")', '#,0', 'Quality'),
        ('Total Freight', "SUMX(VALUES('fact_sales_order_freight'[shipment_number]), CALCULATE(MAX('fact_sales_order_freight'[total_freight])))", '\\$#,0;-\\$#,0', 'Freight'),
        ('Gross Weight', "SUM('fact_sales_order_freight'[gross_weight])", '#,0', 'Volume'),
        ('Catch Weight', "SUM('fact_sales_order_freight'[catch_weight])", '#,0', 'Volume'),
        ('Carrier Name', 'SELECTEDVALUE(dim_address_carrier[address_number]) & " - " & SELECTEDVALUE(dim_address_carrier[name_alpha])', None, 'Names'),
        ('Parent Name', 'SELECTEDVALUE(dim_address_parent[address_number]) & " - " & SELECTEDVALUE(dim_address_parent[name_alpha])', None, 'Names'),
      ],
    },
    'fact_sales_commission': {
      "cols": [
        ('sales_commission_key', 'string', True, None),
        ('order_scope_key', 'string', True, None),
        ('company', 'string', False, None),
        ('company_key_order_no', 'string', True, None),
        ('order_type', 'string', False, None),
        ('order_number', 'int64', False, None),
        ('line_number', 'double', False, None),
        ('commission_line_number', 'int64', False, None),
        ('salesperson', 'int64', True, None),
        ('commission_code_type', 'string', False, None),
        ('ship_to', 'int64', True, None),
        ('sold_to', 'int64', True, None),
        ('branch_plant', 'string', True, None),
        ('item_number_short', 'int64', True, None),
        ('commission_paid_date_key', 'int64', True, None),
        ('gl_date_key', 'int64', True, None),
        ('ship_date_key', 'int64', True, None),
        ('commission_paid_date', 'dateTime', False, None),
        ('gl_date', 'dateTime', False, None),
        ('actual_ship_date', 'dateTime', False, None),
        ('percent_commission', 'double', False, None),
        ('amount_commission', 'double', True, None),
        ('amount_related_commission', 'double', False, None),
        ('percent_related_commission', 'double', False, None),
        ('flat_commission_amount', 'double', False, None),
        ('amount_per_unit', 'double', False, None),
        ('amount_sales_total_line', 'double', True, None),
        ('amount_sales_line_total_cost', 'double', True, None),
        ('amount_line_gross_margin', 'double', False, None),
        ('amount_line_eligible_margin', 'double', False, None),
        ('extended_price', 'double', True, None),
        ('extended_cost', 'double', True, None),
        ('quantity_shipped', 'double', True, None),
        ('primary_quantity_ordered', 'double', True, None),
        ('invoice_number', 'int64', False, None),
        ('second_item_number', 'string', False, None),
        ('line_type', 'string', False, None),
        ('uom_primary', 'string', False, None),
        ('uom_pricing', 'string', False, None),
        ('sales_reporting_code_05', 'string', False, None),
        ('status_code_next', 'string', False, None),
        ('status_code_last', 'string', False, None),
        ('category_code_10', 'string', False, None),
        ('sold_to_search_type', 'string', False, None),
        ('is_primary_commission_line', 'string', True, None),
        ('shift_factor_applied', 'double', True, None),
      ],
      "measures": [
        ('Commission Sales Line Amount', "SUM('fact_sales_commission'[amount_sales_total_line])", '\\$#,0;-\\$#,0', 'Commission'),
        ('Commission Line Cost', "SUM('fact_sales_commission'[amount_sales_line_total_cost])", '\\$#,0;-\\$#,0', 'Commission'),
        ('Commission Amount', "SUM('fact_sales_commission'[amount_commission])", '\\$#,0;-\\$#,0', 'Commission'),
        ('Commission Quantity Shipped', 'CALCULATE(SUM(\'fact_sales_commission\'[quantity_shipped]), \'fact_sales_commission\'[is_primary_commission_line]="Y")', '#,0.00', 'Volume'),
        ('Commission Extended Price', 'CALCULATE(SUM(\'fact_sales_commission\'[extended_price]), \'fact_sales_commission\'[is_primary_commission_line]="Y")', '\\$#,0;-\\$#,0', 'Volume'),
        ('Commission Extended Cost', 'CALCULATE(SUM(\'fact_sales_commission\'[extended_cost]), \'fact_sales_commission\'[is_primary_commission_line]="Y")', '\\$#,0;-\\$#,0', 'Volume'),
        ('Commission Primary Qty Ordered', 'CALCULATE(SUM(\'fact_sales_commission\'[primary_quantity_ordered]), \'fact_sales_commission\'[is_primary_commission_line]="Y")', '#,0.00', 'Volume'),
        ('Salesperson Name', 'SELECTEDVALUE(dim_address_salesperson[address_number]) & " - " & SELECTEDVALUE(dim_address_salesperson[name_alpha])', None, 'Names'),
      ],
    },
    'dim_item': {
      "cols": [
        ('item_number_short', 'int64', True, None),
        ('item_name', 'string', False, None),
        ('uom_weight', 'string', False, None),
      ],
      "measures": [

      ],
    },
    'dim_address_ship_to': {
      "cols": [
        ('address_number', 'int64', True, None),
        ('name_alpha', 'string', False, None),
        ('address_type_01', 'string', False, None),
        ('standard_industry_code', 'string', False, None),
        ('city', 'string', False, None),
        ('state', 'string', False, None),
        ('country', 'string', False, None),
        ('zip_code_postal', 'string', False, None),
        ('address_line_01', 'string', False, None),
        ('address_line_02', 'string', False, None),
        ('address_line_03', 'string', False, None),
        ('address_line_04', 'string', False, None),
        ('has_postal_address', 'string', False, None),
      ],
      "measures": [

      ],
    },
    'dim_address_sold_to': {
      "cols": [
        ('address_number', 'int64', True, None),
        ('name_alpha', 'string', False, None),
        ('address_type_01', 'string', False, None),
        ('standard_industry_code', 'string', False, None),
        ('city', 'string', False, None),
        ('state', 'string', False, None),
        ('country', 'string', False, None),
        ('zip_code_postal', 'string', False, None),
        ('address_line_01', 'string', False, None),
        ('address_line_02', 'string', False, None),
        ('address_line_03', 'string', False, None),
        ('address_line_04', 'string', False, None),
        ('has_postal_address', 'string', False, None),
      ],
      "measures": [

      ],
    },
    'dim_address_carrier': {
      "cols": [
        ('address_number', 'int64', True, None),
        ('name_alpha', 'string', False, None),
        ('address_type_01', 'string', False, None),
        ('standard_industry_code', 'string', False, None),
        ('city', 'string', False, None),
        ('state', 'string', False, None),
        ('country', 'string', False, None),
        ('zip_code_postal', 'string', False, None),
        ('address_line_01', 'string', False, None),
        ('address_line_02', 'string', False, None),
        ('address_line_03', 'string', False, None),
        ('address_line_04', 'string', False, None),
        ('has_postal_address', 'string', False, None),
      ],
      "measures": [

      ],
    },
    'dim_address_parent': {
      "cols": [
        ('address_number', 'int64', True, None),
        ('name_alpha', 'string', False, None),
        ('address_type_01', 'string', False, None),
        ('standard_industry_code', 'string', False, None),
        ('city', 'string', False, None),
        ('state', 'string', False, None),
        ('country', 'string', False, None),
        ('zip_code_postal', 'string', False, None),
      ],
      "measures": [

      ],
    },
    'dim_address_book_destination': {
      "cols": [
        ('address_number', 'int64', True, None),
        ('name_alpha', 'string', False, None),
        ('address_type_01', 'string', False, None),
        ('city', 'string', False, None),
        ('state', 'string', False, None),
        ('country', 'string', False, None),
        ('zip_code_postal', 'string', False, None),
      ],
      "measures": [

      ],
    },
    'dim_address_salesperson': {
      "cols": [
        ('address_number', 'int64', True, None),
        ('name_alpha', 'string', False, None),
        ('address_type_01', 'string', False, None),
        ('city', 'string', False, None),
        ('state', 'string', False, None),
        ('country', 'string', False, None),
        ('zip_code_postal', 'string', False, None),
      ],
      "measures": [

      ],
    },
    'dim_plant': {
      "cols": [
        ('plant_code', 'string', False, None),
        ('plant_name', 'string', False, None),
        ('plant_name_compressed', 'string', True, None),
        ('plant_category_code_02', 'string', False, None),
        ('related_business_unit', 'string', False, None),
        ('company', 'string', False, None),
        ('category_code_cost_ct_020', 'string', False, None),
        ('state', 'string', False, None),
        ('parent_plant_code', 'string', False, None),
        ('last_refreshed_timestamp', 'dateTime', True, None),
      ],
      "measures": [

      ],
    },
    'dim_mode_of_transport': {
      "cols": [
        ('mot_code', 'string', True, None),
        ('description', 'string', False, None),
      ],
      "measures": [

      ],
    },
    'dim_category_code_10': {
      "cols": [
        ('category_code_10', 'string', True, None),
        ('category_code_10_desc', 'string', False, None),
      ],
      "measures": [

      ],
    },
}

# (from_table[MANY], from_col, to_table[ONE], to_col, is_active)
REL = [
    ('fact_sales_order_freight', 'ship_to', 'dim_address_ship_to', 'address_number', True),
    ('fact_sales_order_freight', 'bill_to', 'dim_address_sold_to', 'address_number', True),
    ('fact_sales_order_freight', 'carrier_number', 'dim_address_carrier', 'address_number', True),
    ('fact_sales_order_freight', 'item_number_short', 'dim_item', 'item_number_short', True),
    ('fact_sales_order_freight', 'branch_plant', 'dim_plant', 'plant_code', True),
    ('fact_sales_order_freight', 'address_number_parent', 'dim_address_parent', 'address_number', True),
    ('fact_sales_order_freight', 'mode_of_transport', 'dim_mode_of_transport', 'mot_code', True),
    ('fact_sales_order_freight', 'destination_port', 'dim_address_book_destination', 'address_number', True),
    ('fact_sales_commission', 'salesperson', 'dim_address_salesperson', 'address_number', True),
    ('fact_sales_commission', 'ship_to', 'dim_address_ship_to', 'address_number', True),
    ('fact_sales_commission', 'sold_to', 'dim_address_sold_to', 'address_number', True),
    ('fact_sales_commission', 'branch_plant', 'dim_plant', 'plant_code', True),
    ('fact_sales_commission', 'item_number_short', 'dim_item', 'item_number_short', True),
    ('fact_sales_commission', 'category_code_10', 'dim_category_code_10', 'category_code_10', True),
]

# ── WIRING CHECK ─────────────────────────────────────────────────────────────
# Every relationship must resolve to an emitted table+column; otc set must be exactly
# the two new tables; every other table must be rpt. Fail fast on a typo.
_cols_of = {t: {c[0] for c in spec["cols"]} for t, spec in TABLES.items()}
_errs = []
for ft, fc, tt, tc, _ in REL:
    for tbl, col in [(ft, fc), (tt, tc)]:
        if tbl not in TABLES: _errs.append(f"relationship references unknown table '{tbl}'")
        elif col not in _cols_of[tbl]: _errs.append(f"relationship references unknown column '{tbl}[{col}]'")
if OTC_TABLES != {FREIGHT, "dim_item"}:
    _errs.append(f"OTC_TABLES drifted: {OTC_TABLES}")
for t in TABLES:
    if t not in OTC_TABLES and schema_of(t) != "rpt":
        _errs.append(f"non-otc table '{t}' must be rpt")
if _errs:
    raise SystemExit("Wiring check FAILED:\n  - " + "\n  - ".join(_errs))

print("Wiring check OK — {} tables, {} relationships, {} measures".format(
    len(TABLES), len(REL), sum(len(t["measures"]) for t in TABLES.values())))
for t in TABLES:
    print(f"  [{schema_of(t)}] {t}  cols={len(TABLES[t]['cols'])} meas={len(TABLES[t]['measures'])}")

T = "\t"
for tname, t in TABLES.items():
    L = [f"table {tname}", f"{T}lineageTag: {tag('t:'+tname)}", ""]
    for mname, dax, fmt, folder in t["measures"]:
        L.append(f"{T}measure '{mname}' = {dax}")
        if fmt is not None:    L.append(f"{T}{T}formatString: {fmt}")
        if folder is not None: L.append(f"{T}{T}displayFolder: {folder}")
        L.append(f"{T}{T}lineageTag: {tag('m:'+tname+'.'+mname)}")
        L.append("")
    for cname, dt, hidden, sortby in t["cols"]:
        L.append(f"{T}column {cname}")
        L.append(f"{T}{T}dataType: {dt}")
        if hidden: L.append(f"{T}{T}isHidden")
        L.append(f"{T}{T}summarizeBy: none")
        L.append(f"{T}{T}sourceColumn: {cname}")
        if sortby: L.append(f"{T}{T}sortByColumn: {sortby}")
        L.append(f"{T}{T}lineageTag: {tag('c:'+tname+'.'+cname)}")
        L.append("")
    L += [f"{T}partition {tname} = entity", f"{T}{T}mode: directLake", f"{T}{T}source",
          f"{T}{T}{T}entityName: {tname}", f"{T}{T}{T}schemaName: {schema_of(tname)}",
          f"{T}{T}{T}expressionSource: DatabaseQuery", ""]
    w(os.path.join(TBLS, f"{tname}.tmdl"), "\n".join(L) + "\n")

RL = []
for ft, fc, tt, tc, active in REL:
    rid = tag(f"r:{ft}.{fc}->{tt}.{tc}")
    RL += [f"relationship {rid}"]
    if not active: RL += [f"{T}isActive: false"]
    RL += [f"{T}fromColumn: {ft}.{fc}", f"{T}toColumn: {tt}.{tc}", ""]
w(os.path.join(DEFN, "relationships.tmdl"), "\n".join(RL) + "\n")

expr = (
"expression DatabaseQuery =\n"
f"{T}{T}let\n"
f"{T}{T}    Source = Sql.Database(\"<SQL_ANALYTICS_ENDPOINT>\", \"<LH_JDE_GOLD_DATABASE>\")\n"
f"{T}{T}in\n"
f"{T}{T}    Source\n"
f"{T}lineageTag: {tag('expr:DatabaseQuery')}\n"
f"{T}annotation PBI_IncludeFutureArtifacts = False\n"
)
w(os.path.join(DEFN, "expressions.tmdl"), expr)

refs = "\n".join(f"ref table {n}" for n in TABLES)
model = (
"model Model\n"
f"{T}culture: en-US\n"
f"{T}defaultPowerBIDataSourceVersion: powerBI_V3\n"
f"{T}discourageImplicitMeasures\n"
f"{T}sourceQueryCulture: en-US\n\n"
f"{T}annotation PBI_QueryOrder = [\"DatabaseQuery\"]\n\n"
+ refs + "\n"
)
w(os.path.join(DEFN, "model.tmdl"), model)

w(os.path.join(DEFN, "database.tmdl"), "database\n\tcompatibilityLevel: 1604\n")
w(os.path.join(ROOT, "definition.pbism"), json.dumps({"version": "4.0", "settings": {}}, indent=2) + "\n")
w(os.path.join(ROOT, ".platform"), json.dumps({
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
    "metadata": {"type": "SemanticModel", "displayName": "billable_payable_freight",
                 "description": "ESO1 Billable v Payable Freight — single-fact Direct Lake model."},
    "config": {"version": "2.0", "logicalId": "00000000-0000-0000-0000-000000000000"},
}, indent=2) + "\n")

print("Generated TMDL semantic model at:", ROOT)
