# Extended Sales Order 1 — Power BI Report Wireframe (Billable v Payable Freight)

> Maps the spec's display columns + freight calculations to the **`eso1_billable_payable_freight`** Direct Lake model (facts, role-playing dims, `dim_shipment` bridge, measures).
> Audience: Logistics / Supply Chain · Cadence: weekly. Last updated: 2026-06-16.

## Grain note (drives the page design)
Freight $ live at **shipment** grain (`fact_freight_audit`); item/qty/price live at **order-line** grain (`fact_sales_order_line`). So:
- **Freight measures** are shown at **shipment / order** grain (Pages 1–2).
- **Line detail** (item, qty, price) is a **drill-through** (Page 3) — never put freight $ on a line row (it would repeat across the shipment's lines).
- `dim_shipment` is the bridge that lets a shipment/carrier/date selection filter both facts at once.

---

## Page 1 — Freight Summary (exec overview)
```
┌───────────────────────────────────────────────────────────────────────────────────┐
│  BILLABLE v PAYABLE FREIGHT — Summary           Week: [▼ wk]  Co: [▼]  Carrier: [▼] │
│                                                  FHC: [▼ DLV/PP]   Mode: [▼]         │
├───────────────┬───────────────┬───────────────┬───────────────┬───────────────────┤
│ Total Billable│ Total Payable │ Total Variance│  Total CM %   │ Freight Shipments │
│   $1,240,310  │   $1,090,455  │   +$149,855   │    12.1%      │       3,418       │
│  [card]       │  [card]       │  [card]       │  [card]       │  [card]           │
├───────────────┴───────────────┴───────────────┴───────────────┴───────────────────┤
│  Billable vs Payable by Week                  │  Variance by Carrier (top 10)       │
│  [clustered column: X=Week,                   │  [bar: Y=dim_address_carrier         │
│   Y=Total Billable, Total Payable]            │   [name_alpha], X=Freight Variance] │
│                                               │                                     │
├───────────────────────────────────────────────┼─────────────────────────────────────┤
│  Freight CM % by Carrier                      │  Variance by Ship-To State (map/bar) │
│  [bar: Y=carrier name, X=Freight CM %]        │  [filled map or bar: state, Variance]│
└───────────────────────────────────────────────┴─────────────────────────────────────┘
```
| Element | Model binding |
|---|---|
| Cards | `[Total Billable]`, `[Total Payable]`, `[Total Variance]`, `[Total CM %]`, `[Freight Shipments]` |
| Slicers | `dim_shipment[actual_ship_date]` (Week), `fact_freight_audit[company]`, `dim_address_carrier[name_alpha]`, `…[freight_handling_code]`, `…[mode_of_transport]` |
| Billable vs Payable by Week | axis = date (Week); values `[Total Billable]`, `[Total Payable]` |
| Variance by Carrier | axis `dim_address_carrier[name_alpha]`; value `[Freight Variance]` |
| CM % by Carrier | axis carrier name; value `[Freight CM %]` |
| Variance by State | `dim_address_ship_to[state]`; value `[Total Variance]` |

---

## Page 2 — Billable v Payable Detail (the audit grid, shipment/order grain)
```
┌───────────────────────────────────────────────────────────────────────────────────┐
│  Billable v Payable — Detail        [slicers inherited]      [drill ▼ to Line Detail]│
├──────┬─────────┬─────────┬────────┬──────┬──────┬────────┬────────┬────────┬────────┤
│Order#│Shipment#│Ship-To  │Carrier │ FHC  │Route#│Bill Fr │Pay Fr  │Variance│ CM %   │
│      │         │ Name    │ Name   │      │      │ + Fuel │ + Fuel │        │        │
├──────┼─────────┼─────────┼────────┼──────┼──────┼────────┼────────┼────────┼────────┤
│162710│ 884301  │ ACME TX │ SWIFT  │ DLV  │ 5521 │ $4,210 │ $3,980 │ +$230  │  5.5%  │
│  …   │   …     │   …     │  …     │  …   │  …   │   …    │   …    │   …    │   …    │
├──────┴─────────┴─────────┴────────┴──────┴──────┼────────┼────────┼────────┼────────┤
│  TOTAL                                          │$1.24M  │$1.09M  │+$150K  │ 12.1%  │
└─────────────────────────────────────────────────┴────────┴────────┴────────┴────────┘
   (matrix: rows = Order# + Shipment#; right-click row → drill through to Line Detail)
```
| Column | Model binding |
|---|---|
| Order # | `fact_sales_order_line[order_number]` (or `dim_shipment` related) |
| Shipment # | `dim_shipment[shipment_number]` |
| Ship-To Name | `dim_address_ship_to[name_alpha]` |
| Carrier Name | `dim_address_carrier[name_alpha]` |
| FHC | `dim_shipment[freight_handling_code]` |
| Route # | `dim_shipment[route_number]` |
| Billable Freight / Fuel / **Bill Fr+Fuel** | `[Billable Freight]`, `[Billable Fuel]`, `[Total Billable]` |
| Payable Freight / Fuel / **Pay Fr+Fuel** | `[Payable Freight]`, `[Payable Fuel]`, `[Total Payable]` |
| Variance | `[Freight Variance]` (and `[Total Variance]`) |
| CM % | `[Freight CM %]` / `[Total CM %]` |
| GL Date / Ship Date | `fact_freight_audit[gl_date]`, `dim_shipment[actual_ship_date]` |

Conditional formatting: Variance < 0 → red; CM % data bars.

---

## Page 3 — Line Detail (drill-through from Page 2)
```
┌───────────────────────────────────────────────────────────────────────────────────┐
│  Line Detail — Order 1627109 / Shipment 884301        [← Back]                       │
├──────┬───────────┬──────────────┬─────┬──────────┬──────────┬────────┬─────────────┤
│Line# │2nd Item # │ Item Name    │ UoM │ Qty Ship │ Tons     │ UoM $  │ Maj/Min Code│
├──────┼───────────┼──────────────┼─────┼──────────┼──────────┼────────┼─────────────┤
│ 1.000│ 60125     │ FRAC SAND 40 │ TN  │  24.00   │  24.00   │ $58.20 │  MX / 100   │
│  …   │   …       │   …          │ …   │   …      │   …      │   …    │     …       │
└──────┴───────────┴──────────────┴─────┴──────────┴──────────┴────────┴─────────────┘
   Header strip: Bill-To Name · Ship-To address (Addr1/2, City, State, Zip, Country) ·
                 BOL # · Transport Mode · Invoice # / Invoice Date · Status
```
| Field | Model binding |
|---|---|
| Line # | `fact_sales_order_line[line_number]` |
| 2nd Item # | `fact_sales_order_line[second_item_number]` |
| Item Name | `fact_sales_order_line[item_name]` |
| UoM / Qty Shipped / Tons | `[uom]`, `[units_transaction_qty_unconv]`, `[Quantity Shipped Tons]` |
| UoM $ (price) | `fact_sales_order_line[price_per_unit]` |
| Maj/Min Prod Code | `[major_prod_code]`, `[minor_prod_code]` |
| Bill-To Name | `dim_address_sold_to[name_alpha]` |
| Ship-To address | `dim_address_ship_to[address_line_01/02, city, state, zip_code_postal, country]` |
| BOL # / Transport Mode | `[bol_number]`, `[mode_of_transport]` |
| Invoice # / Date | `[invoice_number]`, `[invoice_date]` |
| Status | `fact_sales_order_line[status]` (sorted by `status_sort`) |

---

## Page 4 — Exceptions / Data Quality (optional)
```
┌───────────────────────────────────────────────────────────────────────────────────┐
│  Freight DQ & Exceptions                                                            │
├───────────────┬───────────────┬───────────────┬─────────────────────────────────────┤
│ Lines Missing │ % High-Conf   │ Shipments w/   │  Top |Variance| Outliers (table)    │
│ Conversion    │ Cost          │ no Freight $   │  Order#·Shipment#·Carrier·Variance  │
│   [card]      │   [card]      │   [card]       │  [table, sorted desc by |Variance|] │
└───────────────┴───────────────┴───────────────┴─────────────────────────────────────┘
```
| Element | Model binding |
|---|---|
| Lines Missing Conversion | `[Lines Missing Conversion]` |
| % High-Confidence Cost | `[% High-Confidence Cost]` |
| Shipments w/ no Freight $ | `dim_shipment` filtered `has_freight = 'N'` (count) |
| Outliers table | `dim_shipment[shipment_number]`, carrier name, `[Total Variance]` sorted by abs |

---

## Spec column → model crosswalk (the §6 "Column Names" list)
| Spec column | Model source |
|---|---|
| 2nd Item Number | `fact_sales_order_line[second_item_number]` |
| Invoice Date / Document # | `[invoice_date]` / `[invoice_number]` |
| Line Type / Order Type / Order # | `[line_type]` / `[order_type]` / `[order_number]` |
| Carrier # / Carrier Name | `[carrier_number]` / `dim_address_carrier[name_alpha]` |
| Company / Plant | `[company]` / `dim_plant[plant_name]` |
| FHC | `[freight_handling_code]` |
| Ship Date / GL Date | `[actual_ship_date]` / `fact_freight_audit[gl_date]` |
| Shipment # / BOL # | `dim_shipment[shipment_number]` / `[bol_number]` |
| Ship To / Ship To Name | `[ship_to]` / `dim_address_ship_to[name_alpha]` |
| Address 1/2, City, Country, Zip, State | `dim_address_ship_to[address_line_01/02, city, country, zip_code_postal, state]` |
| Transport Mode / UoM | `[mode_of_transport]` / `[uom]` |
| Quantity Shipped / UoM Price | `[Quantity Shipped Tons]` (or `units_transaction_qty_unconv`) / `[price_per_unit]` |
| Major / Minor Prod Code | `[major_prod_code]` / `[minor_prod_code]` |
| Route Number | `dim_shipment[route_number]` |
| Billable Freight Total / Billable Fuel TRN | `[Billable Freight]` / `[Billable Fuel]` |
| Total Billable | `[Total Billable]` |
| Payable Freight TRN / Payable Fuel TRN | `[Payable Freight]` / `[Payable Fuel]` |
| Total Payable | `[Total Payable]` |
| Freight Variance / Freight CM % | `[Freight Variance]` / `[Freight CM %]` |
| Total Variance / Total CM % | `[Total Variance]` / `[Total CM %]` |

## Build notes
- All pages share slicers (Week, Company, Carrier, FHC, Mode) synced via the slicer sync pane.
- `is_deleted` rows are already excluded (baked into base measures); add a report filter `dim_shipment[is_active] = 'Y'` for table browsing.
- Direct Lake: keep visuals on aggregated measures + dim attributes (no high-cardinality raw columns on a single visual) to stay in Direct Lake and within the 5-min freshness window.
- "TRN" (per-transaction) vs "APM" diagnostics in the original Hubble spec are intermediate calcs — the report exposes the **summed** buckets + variance + CM%, per the verified Silver analysis (ratios = SUM/SUM in DAX, never per row).
