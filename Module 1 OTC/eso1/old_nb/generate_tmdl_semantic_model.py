#!/usr/bin/env python
# Generates the eso1_billable_payable_freight.SemanticModel TMDL folder
# (Direct Lake), matching the model built by nb_semantic_model_eso1.
import os, uuid, textwrap, json

ROOT = os.path.join(os.path.dirname(__file__), "eso1_billable_payable_freight.SemanticModel")
DEFN = os.path.join(ROOT, "definition")
TBLS = os.path.join(DEFN, "tables")
NS = uuid.UUID("e5071111-0000-4000-8000-000000000000")
def tag(s): return str(uuid.uuid5(NS, s))
def w(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)

CUR = r"\$#,0.00;-\$#,0.00"
PCT = "0.0%"
NUM = "#,0.00"
INT = "#,0"

# ── column defs: (name, dataType, hidden) ; summarizeBy none everywhere ────────
ADDR_COLS = [
    ("address_number", "int64", True), ("name_alpha", "string", False),
    ("address_type_01", "string", False), ("standard_industry_code", "string", False),
    ("city", "string", False), ("state", "string", False), ("country", "string", False),
    ("zip_code_postal", "string", False), ("address_line_01", "string", False),
    ("address_line_02", "string", False), ("address_line_03", "string", False),
    ("address_line_04", "string", False),
]
ADDR_VIEWS = ["dim_address_sold_to", "dim_address_ship_to", "dim_address_carrier",
              "dim_address_loading_port", "dim_address_ocean_carrier", "dim_address_destination"]

# Per-table Direct Lake schema: new ESO1 tables in otc, reused dims/views in rpt.
OTC_TABLES = {"fact_sales_order_line", "fact_freight_audit", "dim_shipment"}
def schema_of(tname): return "otc" if tname in OTC_TABLES else "rpt"

TABLES = {
    "fact_sales_order_line": {
        "cols": [
            ("order_line_key","string",True),("order_number","int64",False),("line_number","double",False),
            ("shipment_number","int64",True),("branch_plant","string",True),("bill_to","int64",True),
            ("ship_to","int64",True),("carrier_number","int64",True),("loading_port","int64",True),
            ("ocean_carrier","int64",True),("port_of_destination","int64",True),
            ("second_item_number","string",False),("item_name","string",False),("uom","string",False),
            ("units_transaction_qty_unconv","double",False),("units_transaction_qty_tons","double",False),
            ("quantity_shipped_tons","double",False),("units_open_tons","double",False),
            ("revenue_dollars","double",False),("price_per_unit","double",False),("item_unit_cost","double",False),
            ("major_prod_code","string",False),("minor_prod_code","string",False),
            ("status","string",False),("status_sort","int64",True),
            ("missing_conversion_flag","string",False),("cost_confidence","string",False),
            ("is_deleted","boolean",True),
        ],
        "sortby": {"status": "status_sort"},
        "measures": [
            ("Total Tons","CALCULATE(SUM(fact_sales_order_line[units_transaction_qty_tons]), fact_sales_order_line[is_deleted]=FALSE())",NUM,"Volume"),
            ("Quantity Shipped Tons","CALCULATE(SUM(fact_sales_order_line[quantity_shipped_tons]), fact_sales_order_line[is_deleted]=FALSE())",NUM,"Volume"),
            ("Open Tons","CALCULATE(SUM(fact_sales_order_line[units_open_tons]), fact_sales_order_line[is_deleted]=FALSE())",NUM,"Volume"),
            ("Revenue $","CALCULATE(SUM(fact_sales_order_line[revenue_dollars]), fact_sales_order_line[is_deleted]=FALSE())",CUR,"Margin"),
            ("Cost $","CALCULATE(SUMX(fact_sales_order_line, fact_sales_order_line[item_unit_cost] * fact_sales_order_line[units_transaction_qty_tons]), fact_sales_order_line[is_deleted]=FALSE())",CUR,"Margin"),
            ("Margin $","[Revenue $] - [Cost $]",CUR,"Margin"),
            ("Margin %","DIVIDE([Margin $], [Revenue $])",PCT,"Margin"),
            ("Order Lines","CALCULATE(DISTINCTCOUNT(fact_sales_order_line[order_line_key]), fact_sales_order_line[is_deleted]=FALSE())",INT,"Counts"),
            ("Lines Missing Conversion","CALCULATE([Order Lines], fact_sales_order_line[missing_conversion_flag]=\"Y\")",INT,"Quality"),
            ("% High-Confidence Cost","DIVIDE(CALCULATE([Order Lines], fact_sales_order_line[cost_confidence]=\"HIGH\"), [Order Lines])",PCT,"Quality"),
        ],
    },
    "fact_freight_audit": {
        "cols": [
            ("shipment_number","int64",True),("company","string",False),("carrier_number","int64",True),
            ("ship_to","int64",True),("branch_plant","string",True),("route_number","int64",False),
            ("gl_date","dateTime",False),("actual_ship_date","dateTime",False),
            ("billable_freight","double",False),("billable_fuel","double",False),("total_billable","double",False),
            ("payable_freight","double",False),("payable_fuel","double",False),("total_payable","double",False),
            ("freight_variance","double",False),("total_variance","double",False),("is_deleted","boolean",True),
        ],
        "sortby": {},
        "measures": [
            ("Billable Freight","CALCULATE(SUM(fact_freight_audit[billable_freight]), fact_freight_audit[is_deleted]=FALSE())",CUR,"Freight"),
            ("Billable Fuel","CALCULATE(SUM(fact_freight_audit[billable_fuel]), fact_freight_audit[is_deleted]=FALSE())",CUR,"Freight"),
            ("Total Billable","CALCULATE(SUM(fact_freight_audit[total_billable]), fact_freight_audit[is_deleted]=FALSE())",CUR,"Freight"),
            ("Payable Freight","CALCULATE(SUM(fact_freight_audit[payable_freight]), fact_freight_audit[is_deleted]=FALSE())",CUR,"Freight"),
            ("Payable Fuel","CALCULATE(SUM(fact_freight_audit[payable_fuel]), fact_freight_audit[is_deleted]=FALSE())",CUR,"Freight"),
            ("Total Payable","CALCULATE(SUM(fact_freight_audit[total_payable]), fact_freight_audit[is_deleted]=FALSE())",CUR,"Freight"),
            ("Freight Variance","[Billable Freight] - [Payable Freight]",CUR,"Freight"),
            ("Total Variance","[Total Billable] - [Total Payable]",CUR,"Freight"),
            ("Freight CM %","DIVIDE([Freight Variance], [Billable Freight])",PCT,"Freight"),
            ("Total CM %","DIVIDE([Total Variance], [Total Billable])",PCT,"Freight"),
            ("Freight Shipments","CALCULATE(DISTINCTCOUNT(fact_freight_audit[shipment_number]), fact_freight_audit[is_deleted]=FALSE())",INT,"Counts"),
        ],
    },
    "dim_shipment": {
        "cols": [
            ("shipment_number","int64",False),("company","string",False),("carrier_number","int64",True),
            ("ship_to","int64",True),("branch_plant","string",True),("mode_of_transport","string",False),
            ("freight_handling_code","string",False),("route_number","int64",False),
            ("gl_date","dateTime",False),
            ("actual_ship_date","dateTime",False),("has_order_line","string",False),
            ("has_freight","string",False),("is_active","string",True),
        ],
        "sortby": {}, "measures": [],
    },
    "dim_plant": {
        # 10 cols; matches the hand-maintained conformed tmdl (ESO1 active/old_nb, ESO4, ESO5).
        # Order MUST stay as-is so generated output is byte-identical to the hand tmdl.
        "cols": [
            ("plant_code","string",False),("plant_name","string",False),
            ("plant_name_compressed","string",True),("plant_category_code_02","string",False),
            ("related_business_unit","string",False),("company","string",False),
            ("category_code_cost_ct_020","string",False),("state","string",False),
            ("parent_plant_code","string",False),("last_refreshed_timestamp","dateTime",True),
        ],
        "sortby": {}, "measures": [],
    },
}
for v in ADDR_VIEWS:
    TABLES[v] = {"cols": ADDR_COLS, "sortby": {}, "measures": []}

REL = [
    ("fact_sales_order_line","bill_to","dim_address_sold_to","address_number"),
    ("fact_sales_order_line","ship_to","dim_address_ship_to","address_number"),
    ("fact_sales_order_line","carrier_number","dim_address_carrier","address_number"),
    ("fact_sales_order_line","loading_port","dim_address_loading_port","address_number"),
    ("fact_sales_order_line","ocean_carrier","dim_address_ocean_carrier","address_number"),
    ("fact_sales_order_line","port_of_destination","dim_address_destination","address_number"),
    ("fact_sales_order_line","branch_plant","dim_plant","plant_code"),
    ("fact_sales_order_line","shipment_number","dim_shipment","shipment_number"),
    ("fact_freight_audit","carrier_number","dim_address_carrier","address_number"),
    ("fact_freight_audit","ship_to","dim_address_ship_to","address_number"),
    ("fact_freight_audit","branch_plant","dim_plant","plant_code"),
    ("fact_freight_audit","shipment_number","dim_shipment","shipment_number"),
]

# ── emit table files ──────────────────────────────────────────────────────────
T = "\t"
for tname, t in TABLES.items():
    L = [f"table {tname}", f"{T}lineageTag: {tag('t:'+tname)}", ""]
    for mname, dax, fmt, folder in t["measures"]:
        L += [f"{T}measure '{mname}' = {dax}",
              f"{T}{T}formatString: {fmt}",
              f"{T}{T}displayFolder: {folder}",
              f"{T}{T}lineageTag: {tag('m:'+tname+'.'+mname)}", ""]
    for cname, dt, hidden in t["cols"]:
        L.append(f"{T}column {cname}")
        L.append(f"{T}{T}dataType: {dt}")
        if hidden:
            L.append(f"{T}{T}isHidden")
        L.append(f"{T}{T}summarizeBy: none")
        L.append(f"{T}{T}sourceColumn: {cname}")
        if cname in t["sortby"]:
            L.append(f"{T}{T}sortByColumn: {t['sortby'][cname]}")
        L.append(f"{T}{T}lineageTag: {tag('c:'+tname+'.'+cname)}")
        L.append("")
    L += [f"{T}partition {tname} = entity",
          f"{T}{T}mode: directLake",
          f"{T}{T}source",
          f"{T}{T}{T}entityName: {tname}",
          f"{T}{T}{T}schemaName: {schema_of(tname)}",
          f"{T}{T}{T}expressionSource: DatabaseQuery", ""]
    w(os.path.join(TBLS, f"{tname}.tmdl"), "\n".join(L) + "\n")

# ── relationships.tmdl ────────────────────────────────────────────────────────
RL = []
for ft, fc, tt, tc in REL:
    rid = tag(f"r:{ft}.{fc}->{tt}.{tc}")
    RL += [f"relationship {rid}",
           f"{T}fromColumn: {ft}.{fc}",
           f"{T}toColumn: {tt}.{tc}", ""]
w(os.path.join(DEFN, "relationships.tmdl"), "\n".join(RL) + "\n")

# ── expressions.tmdl (Direct Lake SQL endpoint — fill placeholders per env) ───
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

# ── model.tmdl ────────────────────────────────────────────────────────────────
refs = "\n".join(f"ref table {n}" for n in TABLES)
model = (
"model Model\n"
f"{T}culture: en-US\n"
f"{T}defaultPowerBIDataSourceVersion: powerBI_V3\n"
f"{T}discourageImplicitMeasures\n"
f"{T}sourceQueryCulture: en-US\n\n"
f"{T}annotation PBI_QueryOrder = [\"DatabaseQuery\"]\n\n"
+ refs + "\n\nref table DatabaseQuery\n"
)
# DatabaseQuery is an expression, not a table; reference expression instead
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

# ── database.tmdl ─────────────────────────────────────────────────────────────
w(os.path.join(DEFN, "database.tmdl"), "database\n\tcompatibilityLevel: 1604\n")

# ── definition.pbism + .platform ──────────────────────────────────────────────
w(os.path.join(ROOT, "definition.pbism"), json.dumps({"version": "4.0", "settings": {}}, indent=2) + "\n")
w(os.path.join(ROOT, ".platform"), json.dumps({
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
    "metadata": {"type": "SemanticModel", "displayName": "eso1_billable_payable_freight",
                 "description": "ESO1 Billable v Payable Freight — Direct Lake model."},
    "config": {"version": "2.0", "logicalId": "00000000-0000-0000-0000-000000000000"},
}, indent=2) + "\n")

print("Generated TMDL semantic model at:", ROOT)
for dp, _, fs in os.walk(ROOT):
    for f in fs:
        print("  ", os.path.relpath(os.path.join(dp, f), ROOT))
