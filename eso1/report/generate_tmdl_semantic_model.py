#!/usr/bin/env python
# Generates report/billable_payable_freight.SemanticModel (Direct Lake TMDL) for the
# TWO-FACT ESO1 model (Billable v Payable Freight + Sales Commission) — reconciled to
# the hand-maintained twin: 17 tables / 19 relationships / 40 measures, ALL rpt schema (all
# Direct Lake), NO is_deleted. dim_second_item = physical Direct Lake dim (distinct second_item_number
# + Included/Excluded flag) for the large-exclusion-list slicer; built by nb_eso1_gold_dim_second_item.
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

# Direct Lake: every table lives in the rpt schema of lh_jde_gold — matches the notebooks
# (which write to GOLD_SCHEMA='rpt') and the runtime builder nb_semantic_model_eso1.py.
def schema_of(t): return "rpt"

# Optional model DISPLAY name per table (field-list label) — the physical Delta table / entityName /
# dict key stays as-is; only the emitted table/partition/ref/relationship references use the display name.
TABLE_DISPLAY = {'dim_second_item': 'Second Item Filter', 'dim_order_number': 'Order Filter'}
def disp_t(t): return TABLE_DISPLAY.get(t, t)
def qname(n):  return f"'{n}'" if any(c in n for c in " '\"") else n   # quote names with spaces

# Each table: cols = [(name, dataType, isHidden, sortByColumn|None [, summarizeBy='none' [, formatString|None]])],
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
        ('document_type', 'string', False, None),
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
        ('requested_date', 'dateTime', False, None),
        ('second_item_number', 'string', False, None),
        ('third_item_number', 'string', False, None),
        ('line_type', 'string', True, None),
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
        ('production_code', 'string', False, None),
        ('production_ship_notes', 'string', False, None),
        ('order_reference', 'string', False, None),
        ('routing_notes', 'string', False, None),
        ('equipment_type', 'string', False, None),
        ('inland_delterms', 'string', False, None),
        ('incoterms', 'string', False, None),
        ('date_requested_ship', 'dateTime', False, None),
        ('release_date', 'dateTime', False, None),
        ('route_container_count', 'double', True, None),
        # ── Mak Export Orders ocean-booking display cols (physical on fact; exposed for the export model) ──
        ('sold_to_lob_category_05', 'string', False, None),    # F03012 AIAC05 (Sales Rep grouping / E26 export flag)
        ('scheduled_pick_date', 'dateTime', False, None),      # SDPDDJ (labelled "Production Date")
        ('cancel_date', 'dateTime', False, None),              # SDCNDJ (Cancellation Date)
        ('date_earliest_pickup', 'dateTime', False, None),     # BADEPU (Sail Date)
        ('date_earliest_delivery', 'dateTime', False, None),   # BADEDL (ETA Date)
        ('ocean_carrier', 'int64', True, None),                # BA55OCCR — FK to dim_address_ocean_carrier (hidden; decoded via dim + Ocean Carrier Name)
        ('booking_no', 'string', False, None),                 # BA55BKNO (Booking Number) — ⚠ verify string vs int64 vs physical schema
        ('vessel_name', 'string', False, None),                # BA55VLNO (Vessel Name)
        ('voyage_number', 'string', False, None),              # BA55VONO (Voyage No) — ⚠ verify string vs int64 vs physical schema
        # ── Ottawa Whole Grain Truck (Packaged) display cols (physical on fact; exposed for the report) ──
        ('sales_reporting_code_03', 'string', False, None),    # SDSRP3 (Pack Code; also the PKG page filter)
        ('delivery_instruct_line_01', 'string', False, None),  # SHDEL1 (Delivery Instructions Line 1)
        ('delivery_instruct_line_02', 'string', False, None),  # SHDEL2 (Delivery Instructions Line 2)
        # ── SOP000x Next-Status 620 / SOP sales-status page display cols (physical on fact; exposed for the SOP model) ──
        ('user_reserved_reference', 'string', False, None),   # SDURRF
        ('transaction_originator', 'string', False, None),    # SDTORG
        ('user_id', 'string', False, None),                   # SDUSER
        ('time_of_day', 'int64', False, None),                # SDTDAY
        ('hold_orders_code', 'string', False, None),          # SHHOLD
        ('deferred_entries_flag', 'string', False, None),     # F49211 UDDEFF (Cycle Billing Entries Flag)
        ('gl_class', 'string', False, None),                  # SDGLC
        ('status_code_last', 'string', False, None),          # SDLTTR
        ('status_code_next', 'string', False, None),          # SDNXTR
        ('last_status_num', 'int64', True, None),             # SDLTTR int copy (page range-filter)
        ('next_status_num', 'int64', True, None),             # SDNXTR int copy (page range-filter)
        ('container_id', 'string', False, None),              # SDCNID (Vehicle No.)
        ('pull_signal', 'string', False, None),               # SDPSIG (Sand Ticket)
        ('reference_01', 'string', False, None),              # SDVR01 (Customer PO Number)
        ('location', 'string', False, None),                  # SDLOCN
        ('lot_number', 'string', False, None),                # SDLOTN
        ('date_updated', 'dateTime', False, None),            # SDUPMJ
        ('order_date', 'dateTime', False, None),              # SDTRDJ (line order date)
        ('pricing_issue_remark', 'string', False, None),      # derived: 'Unit Price Zero' / 'No effective price'
        # SOP620 adjustment buckets — per-line F4074 ALUPRC-sum x shipped tons; summable columns (Product Price = extended_price).
        ('adj_non_product', 'double', False, None, 'sum', '\\$#,0.00'),
        ('adj_al_severance_tax', 'double', False, None, 'sum', '\\$#,0.00'),
        ('adj_misc_billing', 'double', False, None, 'sum', '\\$#,0.00'),
        ('adj_freight', 'double', False, None, 'sum', '\\$#,0.00'),
        ('adj_car_charges', 'double', False, None, 'sum', '\\$#,0.00'),
        ('adj_freight_hide', 'double', False, None, 'sum', '\\$#,0.00'),
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
        ('Ocean Carrier Name', 'SELECTEDVALUE(dim_address_ocean_carrier[address_number]) & " - " & SELECTEDVALUE(dim_address_ocean_carrier[name_alpha])', None, 'Names'),
        ('Days Past Due', "DATEDIFF(MAX('fact_sales_order_freight'[requested_date]), TODAY(), DAY)", '#,0', 'Aging'),
        ('Ordered Tons', "SUMX('fact_sales_order_freight', 'fact_sales_order_freight'[transaction_quantity] * COALESCE('fact_sales_order_freight'[conversion_to_tons_rate], 0))", '#,0.00', 'Volume'),
        ('Container Count', "SUMX(VALUES('fact_sales_order_freight'[shipment_number]), CALCULATE(MAX('fact_sales_order_freight'[route_container_count])))", '#,0', 'Volume'),
        # SOP620 pricing (user-validated vs Hubble): Product Price = line extended price (SDAEXP); Price Per Ton = it / tons.
        ('Product Price', "SUM('fact_sales_order_freight'[extended_price])", '\\$#,0.00', 'Pricing'),
        ('Price Per Ton', "DIVIDE([Product Price], [Quantity Shipped Tons])", '\\$#,0.00', 'Pricing'),
        # Short Ship Notifications — raw line quantities + the cancel-date notification-window diff (page-filter =1).
        ('Short Ship Shipped Qty', "SUM('fact_sales_order_freight'[quantity_shipped])", '#,0.00', 'Short Ship'),
        ('Short Ship Ordered Qty', "SUM('fact_sales_order_freight'[primary_quantity_ordered])", '#,0.00', 'Short Ship'),
        ('Short Ship Transaction Qty', "SUM('fact_sales_order_freight'[transaction_quantity])", '#,0.00', 'Short Ship'),
        ('Short Ship Cancelled Qty', "SUM('fact_sales_order_freight'[cancelled_qty])", '#,0.00', 'Short Ship'),
        ('Open Qty', "SUM('fact_sales_order_freight'[open_qty])", '#,0.00', 'Volume'),
        # Open-order-report quantity aliases (report-label-friendly; same sums as the Short Ship * measures)
        ('Order Qty', "SUM('fact_sales_order_freight'[transaction_quantity])", '#,0.00', 'Quantities'),
        ('Primary Qty Ordered', "SUM('fact_sales_order_freight'[primary_quantity_ordered])", '#,0.00', 'Quantities'),
        ('Primary Qty Loaded', "SUM('fact_sales_order_freight'[quantity_shipped])", '#,0.00', 'Quantities'),
        ('Primary Qty Open', "SUM('fact_sales_order_freight'[open_qty])", '#,0.00', 'Quantities'),
        ('Days Since Cancel', "DATEDIFF(MAX('fact_sales_order_freight'[cancel_date]), TODAY(), DAY)", '#,0', 'Short Ship'),
        # Ottawa (updated) conditional buckets — qty + matching line-count pairs; all over base cols already on the fact.
        # KG lines: transaction qty where uom = KG.
        ('KG Ordered Qty', "SUMX(FILTER('fact_sales_order_freight', TRIM('fact_sales_order_freight'[uom]) = \"KG\"), 'fact_sales_order_freight'[transaction_quantity])", '#,0.00', 'Ottawa Buckets'),
        ('KG Line Count', "COUNTROWS(FILTER('fact_sales_order_freight', TRIM('fact_sales_order_freight'[uom]) = \"KG\")) + 0", '#,0', 'Ottawa Buckets'),
        # Freight-type lines (SDLNTY in F / FT / CA): primary ordered qty.
        ('Freight-Type Ordered Qty', "SUMX(FILTER('fact_sales_order_freight', TRIM('fact_sales_order_freight'[line_type]) = \"F\" || TRIM('fact_sales_order_freight'[line_type]) = \"FT\" || TRIM('fact_sales_order_freight'[line_type]) = \"CA\"), 'fact_sales_order_freight'[primary_quantity_ordered])", '#,0.00', 'Ottawa Buckets'),
        ('Freight-Type Line Count', "COUNTROWS(FILTER('fact_sales_order_freight', TRIM('fact_sales_order_freight'[line_type]) = \"F\" || TRIM('fact_sales_order_freight'[line_type]) = \"FT\" || TRIM('fact_sales_order_freight'[line_type]) = \"CA\")) + 0", '#,0', 'Ottawa Buckets'),
        # Confirmed-shipped lines (last status 530 AND next status 560): primary ordered qty.
        ('Confirmed Ordered Qty', "SUMX(FILTER('fact_sales_order_freight', 'fact_sales_order_freight'[last_status_num] = 530 && 'fact_sales_order_freight'[next_status_num] = 560), 'fact_sales_order_freight'[primary_quantity_ordered])", '#,0.00', 'Ottawa Buckets'),
        ('Confirmed Line Count', "COUNTROWS(FILTER('fact_sales_order_freight', 'fact_sales_order_freight'[last_status_num] = 530 && 'fact_sales_order_freight'[next_status_num] = 560)) + 0", '#,0', 'Ottawa Buckets'),
        # Backorder/cancel lines (last status in 520 / 914): primary ordered qty.
        ('Backorder Ordered Qty', "SUMX(FILTER('fact_sales_order_freight', 'fact_sales_order_freight'[last_status_num] = 520 || 'fact_sales_order_freight'[last_status_num] = 914), 'fact_sales_order_freight'[primary_quantity_ordered])", '#,0.00', 'Ottawa Buckets'),
        ('Backorder Line Count', "COUNTROWS(FILTER('fact_sales_order_freight', 'fact_sales_order_freight'[last_status_num] = 520 || 'fact_sales_order_freight'[last_status_num] = 914)) + 0", '#,0', 'Ottawa Buckets'),
        # SOP620 F4074 adjustment buckets — line extended price (SDAEXP) attributed by ALAPRP1 print code (materialized as adj_* on the fact).
        ('Non Product', "SUM('fact_sales_order_freight'[adj_non_product])", '\\$#,0.00', 'SOP620 Adjustments'),
        ('AL Severance Tax', "SUM('fact_sales_order_freight'[adj_al_severance_tax])", '\\$#,0.00', 'SOP620 Adjustments'),
        ('Misc Billing', "SUM('fact_sales_order_freight'[adj_misc_billing])", '\\$#,0.00', 'SOP620 Adjustments'),
        ('Freight', "SUM('fact_sales_order_freight'[adj_freight])", '\\$#,0.00', 'SOP620 Adjustments'),
        ('Car Charges', "SUM('fact_sales_order_freight'[adj_car_charges])", '\\$#,0.00', 'SOP620 Adjustments'),
        ('Freight Hide', "SUM('fact_sales_order_freight'[adj_freight_hide])", '\\$#,0.00', 'SOP620 Adjustments'),
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
        ('item_segment_04', 'string', False, None),   # IMSEG4 (was fact.item_segment_04)
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
        ('category_code_05', 'string', False, None),   # ABAC05 (was fact.category_code_05)
        ('category_code_14', 'string', False, None),   # ABAC14 (was fact.category_code_14)
        ('address_rate', 'double', False, None),       # ABURAT (was fact.address_rate)
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
        ('category_code_05', 'string', False, None),   # ABAC05 (was fact.sold_to_category_05)
        ('category_code_10', 'string', False, None),   # ABAC10 (was fact.sold_to_category_10)
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
    'dim_address_ocean_carrier': {   # role view over rpt.dim_address_book — decodes fact.ocean_carrier (BA55OCCR)
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
    'dim_category_code_05': {
      "cols": [
        ('category_code_05', 'string', True, None),
        ('category_code_05_desc', 'string', False, None),
      ],
      "measures": [

      ],
    },
    'dim_freight_handling_code': {
      "cols": [
        ('freight_handling_code', 'string', True, None),
        ('freight_handling_code_desc', 'string', False, None),
      ],
      "measures": [

      ],
    },
    'dim_uom_conversion': {   # REUSED F41003 std UOM->TN dim (lh_jde_gold.rpt); Tier-B tons fallback keyed on from_uom
      "cols": [
        ('from_uom', 'string', True, None),     # PK — joins fact_sales_order_freight.uom (SDUOM / uom_as_input)
        ('std_factor', 'double', False, None),  # standard UOM->TN factor (fwd UMRUM='TN', rev 1/factor)
      ],
      "measures": [

      ],
    },
    'dim_second_item': {   # physical Direct Lake dim (rpt) — distinct second_item_number + one Included/Excluded flag per Ottawa variation (built by nb_eso1_gold_dim_second_item)
      "cols": [
        ('second_item_number', 'string', False, None),
        # one flag column per Ottawa variation (display name ≠ physical col); slice ='Included'
        (('WG Truck Packaged Filter', 'whole_grain_truck_packaged_filter'), 'string', False, None),
        (('WG Truck Bulk Filter',     'whole_grain_truck_bulk_filter'),     'string', False, None),
        (('WG Rail Bulk Filter',      'whole_grain_rail_bulk_filter'),      'string', False, None),
        (('Ground Packaged Filter',   'ground_packaged_filter'),            'string', False, None),
        (('Ground Bulk Filter',       'ground_bulk_filter'),                'string', False, None),
        (('ASTM Packaged Filter',     'astm_packaged_filter'),              'string', False, None),
      ],
      "measures": [

      ],
    },
    'dim_order_number': {   # physical Direct Lake dim (rpt) — distinct order_number + one Included/Excluded flag per whitelist report (built by nb_eso1_gold_dim_order_number)
      "cols": [
        ('order_number', 'int64', False, None),
        # one flag column per order-whitelist report (display name ≠ physical col); slice ='Included'
        (('Mak Export Filter', 'mak_export_filter'), 'string', False, None),   # Mak Export Orders (~70 SDDOCO whitelist)
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
    ('fact_sales_order_freight', 'ocean_carrier', 'dim_address_ocean_carrier', 'address_number', True),
    ('fact_sales_order_freight', 'uom', 'dim_uom_conversion', 'from_uom', True),   # std UOM->TN fallback (rpt), many:1
    ('fact_sales_order_freight', 'second_item_number', 'dim_second_item', 'second_item_number', True),   # physical exclusion dim, many:1
    ('fact_sales_order_freight', 'order_number', 'dim_order_number', 'order_number', True),   # physical order-whitelist dim, many:1
    ('fact_sales_commission', 'salesperson', 'dim_address_salesperson', 'address_number', True),
    ('fact_sales_commission', 'ship_to', 'dim_address_ship_to', 'address_number', True),
    ('fact_sales_commission', 'sold_to', 'dim_address_sold_to', 'address_number', True),
    ('fact_sales_commission', 'branch_plant', 'dim_plant', 'plant_code', True),
    ('fact_sales_commission', 'item_number_short', 'dim_item', 'item_number_short', True),
    ('fact_sales_commission', 'category_code_10', 'dim_category_code_10', 'category_code_10', True),
    ('dim_address_ship_to', 'category_code_05', 'dim_category_code_05', 'category_code_05', True),   # snowflake: ship-to cat05 -> UDC 01/05 decode (was fact.category_code_05)
    ('fact_sales_order_freight', 'freight_handling_code', 'dim_freight_handling_code', 'freight_handling_code', True),
    # BIDIRECTIONAL: Tier-2 F4074 reports filter the freight fact BY the adjustment whitelist (many->one),
    # so cross-filtering must flow both ways. 6th element = bothDirections.
]

# ── WIRING CHECK ─────────────────────────────────────────────────────────────
# Every relationship must resolve to an emitted table+column; otc set must be exactly
# the two new tables; every other table must be rpt. Fail fast on a typo.
_cols_of = {t: {(c[0][0] if isinstance(c[0], tuple) else c[0]) for c in spec["cols"]} for t, spec in TABLES.items()}
_errs = []
for _rel in REL:
    ft, fc, tt, tc = _rel[0], _rel[1], _rel[2], _rel[3]
    for tbl, col in [(ft, fc), (tt, tc)]:
        if tbl not in TABLES: _errs.append(f"relationship references unknown table '{tbl}'")
        elif col not in _cols_of[tbl]: _errs.append(f"relationship references unknown column '{tbl}[{col}]'")
for t in TABLES:
    if schema_of(t) != "rpt":
        _errs.append(f"table '{t}' must be rpt")
if _errs:
    raise SystemExit("Wiring check FAILED:\n  - " + "\n  - ".join(_errs))

print("Wiring check OK — {} tables, {} relationships, {} measures".format(
    len(TABLES), len(REL), sum(len(t["measures"]) for t in TABLES.values())))
for t in TABLES:
    print(f"  [{schema_of(t)}] {t}  cols={len(TABLES[t]['cols'])} meas={len(TABLES[t]['measures'])}")

T = "\t"
for tname, t in TABLES.items():
    L = [f"table {qname(disp_t(tname))}", f"{T}lineageTag: {tag('t:'+tname)}", ""]
    for mname, dax, fmt, folder in t["measures"]:
        L.append(f"{T}measure '{mname}' = {dax}")
        if fmt is not None:    L.append(f"{T}{T}formatString: {fmt}")
        if folder is not None: L.append(f"{T}{T}displayFolder: {folder}")
        L.append(f"{T}{T}lineageTag: {tag('m:'+tname+'.'+mname)}")
        L.append("")
    is_calc = bool(t.get("calc"))   # DAX calculated table (import; e.g. DATATABLE) vs Direct Lake entity
    for col in t["cols"]:
        cname, dt, hidden, sortby = col[0], col[1], col[2], col[3]
        # cname may be "src" (display == sourceColumn) or ("Display Name", "source_column")
        disp, src = cname if isinstance(cname, tuple) else (cname, cname)
        summ = col[4] if len(col) > 4 else "none"       # optional summarizeBy (default none)
        cfmt = col[5] if len(col) > 5 else None          # optional per-column formatString
        _decl = f"'{disp}'" if any(ch in disp for ch in " '\"") else disp   # quote names with spaces
        L.append(f"{T}column {_decl}")
        L.append(f"{T}{T}dataType: {dt}")
        if hidden: L.append(f"{T}{T}isHidden")
        L.append(f"{T}{T}summarizeBy: {summ}")
        # calc-table columns reference the DAX-produced column ([name]); Direct Lake columns bind a Delta column
        L.append(f"{T}{T}sourceColumn: {'['+src+']' if is_calc else src}")
        if cfmt is not None: L.append(f"{T}{T}formatString: {cfmt}")
        if sortby: L.append(f"{T}{T}sortByColumn: {sortby}")
        L.append(f"{T}{T}lineageTag: {tag('c:'+tname+'.'+src)}")
        L.append("")
    if is_calc:
        L += [f"{T}partition {qname(disp_t(tname))} = calculated", f"{T}{T}mode: import",
              f"{T}{T}source = {t['calc']}", ""]
    else:
        L += [f"{T}partition {qname(disp_t(tname))} = entity", f"{T}{T}mode: directLake", f"{T}{T}source",
              f"{T}{T}{T}entityName: {tname}", f"{T}{T}{T}schemaName: {schema_of(tname)}",
              f"{T}{T}{T}expressionSource: DatabaseQuery", ""]
    w(os.path.join(TBLS, f"{tname}.tmdl"), "\n".join(L) + "\n")

RL = []
for _rel in REL:
    ft, fc, tt, tc, active = _rel[0], _rel[1], _rel[2], _rel[3], _rel[4]
    both = _rel[5] if len(_rel) > 5 else False
    rid = tag(f"r:{ft}.{fc}->{tt}.{tc}")
    RL += [f"relationship {rid}"]
    if not active: RL += [f"{T}isActive: false"]
    if both: RL += [f"{T}crossFilteringBehavior: bothDirections"]
    RL += [f"{T}fromColumn: {qname(disp_t(ft))}.{fc}", f"{T}toColumn: {qname(disp_t(tt))}.{tc}", ""]
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

refs = "\n".join(f"ref table {qname(disp_t(n))}" for n in TABLES)
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
