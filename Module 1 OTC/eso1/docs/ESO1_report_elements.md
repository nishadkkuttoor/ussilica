# ESO1 — Power BI Report Elements (bound to `billable_payable_freight`)

Concrete, buildable element list derived from the spec **`Extended Sales Order 1.docx`**
(via `001 OTC Reports/ESO1_report_wireframe.md`) and bound to the **single-fact** Direct Lake
semantic model. Audience: Logistics / Supply Chain. `[Brackets]` = model **measure**;
`table[column]` = a **dimension/fact column**. Existing = already in the PBIR (`report/…Report/`);
New = suggested add.

> **v2.1 model (2026-07-01).** One consolidated fact **`fact_sales_order_freight`** (order-line grain,
> freight denormalized to shipment grain and **DAX-deduped**). Dims: `dim_item`; reused `dim_address_book` role views
> (`dim_address_ship_to`/`_sold_to`/`_carrier`) and
> `dim_plant` (`rpt`). The old two-fact tables (`fact_freight_audit`, `fact_sales_order_line`,
> `dim_shipment`) are **retired** — every binding below is on the single fact or a dim. Cost/margin is
> **out of scope** for this spec (removed). The fact **hard-deletes** (no `is_deleted`/`is_active` column),
> so no soft-delete page filter is needed.

## Measure inventory (what you have to work with)
- **Freight $ (deduped per shipment via `SUMX(VALUES(shipment_number),…)`):** `[Billable Freight]`
  `[Billable Fuel]` `[Total Billable]` `[Payable Freight]` `[Payable Fuel]` `[Total Payable]`
  `[Freight Variance]` `[Total Variance]` `[Freight CM %]` `[Total CM %]` `[Freight Shipments]`
- **Volume (line grain — plain SUM):** `[Quantity Shipped Tons]` `[Price Quantity Shipped]` `[Order Lines]`
- **Dates (no date dimension):** there are no role-played date measures. To view $ by GL or invoice date, slice the
  fact's raw `gl_date` / `invoice_date` column on the visual directly (relative-date slicers for MTD/YTD).
- **Quality (derived from `fact_sales_order_freight[missing_conversion_flag]`):** `[Lines Missing Conversion]`
  `= CALCULATE([Order Lines], fact_sales_order_freight[missing_conversion_flag] = "Y")`.

## Global slicers (sync across all pages)
| Slicer | Binding | Style |
|---|---|---|
| Week / Ship Date | `fact_sales_order_freight[actual_ship_date]`; Week grain via `fact_sales_order_freight[ship_year_week]` | Between (date range) |
| Company | `fact_sales_order_freight[company_key_order_no]` | Dropdown |
| Carrier | `dim_address_carrier[name_alpha]` | Dropdown (search) |
| Freight Handling Code | `fact_sales_order_freight[freight_handling_code]` | Tile (DLV/PP) |
| Mode of Transport | `fact_sales_order_freight[mode_of_transport]` | Dropdown |
| Line Type | `fact_sales_order_freight[line_type]` | Dropdown |
| Status (Last) | `fact_sales_order_freight[status_code_last]` | Dropdown |
| **Price Adjustment Type** | `fact_sales_order_freight[price_adjustment_type]` | Dropdown (search) |

> **v2.1 slicer fields on the fact** (all business WHERE filters were removed — filtering happens here):
> `price_adjustment_type`, `standard_industry_code`, `category_code_05`, `category_code_14`, `search_type`,
> `uom_structure`, `payment_terms`, `item_segment_04`. Add any of these as page/pane slicers as needed;
> `company_key_order_no`, `line_type`, `status_code_last` replace the former hard filters.

---

## Page 1 — Freight Summary (exec overview)
| # | Element | Visual type | Bindings | Status |
|---|---|---|---|---|
| 1 | Total Billable | Card | `[Total Billable]` | Existing |
| 2 | Total Payable | Card | `[Total Payable]` | Existing |
| 3 | Total Variance | Card | `[Total Variance]` (red if <0) | Existing |
| 4 | Total CM % | Card | `[Total CM %]` | Existing |
| 5 | Freight Shipments | Card | `[Freight Shipments]` | New |
| 6 | Billable vs Payable by Week | Clustered column | Axis `fact_sales_order_freight[ship_year_week]`; Values `[Total Billable]`, `[Total Payable]` | Existing |
| 7 | Variance by Carrier (top 10) | Bar | Axis `dim_address_carrier[name_alpha]`; Value `[Freight Variance]`; Top-N=10 filter | Existing |
| 8 | Freight CM % by Carrier | Bar | Axis `dim_address_carrier[name_alpha]`; Value `[Freight CM %]` | New |
| 9 | Variance by Ship-To State | Filled map *or* bar | Location `dim_address_ship_to[state]`; Value `[Total Variance]` | New |
| 10 | Company / Carrier / FHC / Mode | Slicers | see Global slicers | Existing (Company, Carrier) |

---

## Page 2 — Billable v Payable Detail (audit grid, shipment/order grain)
**Visual: Matrix (or Table).** Rows = `fact_sales_order_freight[order_number]` + `fact_sales_order_freight[shipment_number]`.
Right-click a row → **drill through to Page 3**.

| Column | Binding | Status |
|---|---|---|
| Order # | `fact_sales_order_freight[order_number]` | Existing |
| Shipment # | `fact_sales_order_freight[shipment_number]` | Existing |
| Ship-To Name | `dim_address_ship_to[name_alpha]` | Existing |
| Carrier Name | `dim_address_carrier[name_alpha]` | Existing |
| FHC | `fact_sales_order_freight[freight_handling_code]` | Existing |
| Route # | `fact_sales_order_freight[route_number]` | Existing |
| Billable Freight | `[Billable Freight]` | New (split detail) |
| Billable Fuel | `[Billable Fuel]` | New |
| Total Billable | `[Total Billable]` | Existing |
| Payable Freight | `[Payable Freight]` | New |
| Payable Fuel | `[Payable Fuel]` | New |
| Total Payable | `[Total Payable]` | Existing |
| Variance | `[Freight Variance]` (data bar; red <0) | Existing |
| CM % | `[Freight CM %]` | Existing |
| GL Date / Ship Date | `fact_sales_order_freight[gl_date]` / `[actual_ship_date]` | New |

Slicers inherited (synced). Row-total band shows the grand totals.

> **Freight-grain caution:** `[Billable/Payable …]` measures are deduped to shipment grain, so they stay correct even
> though the fact is order-line grain. Do **not** bind the raw `fact_sales_order_freight[total_billable]` *column* on a
> line-level visual — it repeats across the shipment's lines. Use the measures, or filter `is_primary_shipment_line="Y"`.

---

## Page 3 — Line Detail (drill-through target; filter card = `shipment_number`)
**Visual: Table.** Drill-through field: `fact_sales_order_freight[shipment_number]`. Add a **Back** button.

| Field | Binding | Status |
|---|---|---|
| Line # | `fact_sales_order_freight[line_number]` | Existing |
| 2nd Item # | `fact_sales_order_freight[second_item_number]` | Existing |
| Item Name | `dim_item[item_name]` | Existing |
| UoM | `fact_sales_order_freight[uom]` | Existing |
| Qty Shipped | `fact_sales_order_freight[transaction_quantity]` | Existing |
| Tons | `[Quantity Shipped Tons]` | Existing |
| UoM $ (price) | `fact_sales_order_freight[price_per_unit]` | Existing |
| Maj / Min Prod Code | `fact_sales_order_freight[major_prod_code]` / `[minor_prod_code]` | Existing |
| Price Adj Type | `fact_sales_order_freight[price_adjustment_type]` | New (v2.1) |
| Status | `fact_sales_order_freight[status_code_last]` | New |

**Header strip (cards / multi-row card):** Bill-To Name `dim_address_sold_to[name_alpha]`;
Ship-To address `dim_address_ship_to[address_line_01/02, city, state, zip_code_postal, country]`;
BOL # `fact_sales_order_freight[bol_number]`; Transport Mode `fact_sales_order_freight[mode_of_transport]`
(raw JDE code); Invoice # / Date `fact_sales_order_freight[invoice_number]` / `[invoice_date]`.

---

## Page 4 — Exceptions / Data Quality
| # | Element | Visual type | Bindings | Status |
|---|---|---|---|---|
| 1 | Lines Missing Conversion | Card | `[Lines Missing Conversion]` | Existing |
| 2 | Missing Conversion by UoM | Bar | Axis `fact_sales_order_freight[uom]`; Value `[Lines Missing Conversion]` | Existing |
| 3 | Shipments w/ no Freight $ | Card | `[Freight Shipments]` filtered to `[Total Billable] = 0 && [Total Payable] = 0` (define `[Shipments No Freight $]`) | New (replaces `has_freight`) |
| 4 | Order Lines by Missing-Conversion flag | Column | Axis `fact_sales_order_freight[missing_conversion_flag]`; Value `[Order Lines]` | New (replaces `has_freight`) |
| 5 | Top \|Variance\| Outliers | Table | `fact_sales_order_freight[shipment_number]`, `dim_address_carrier[name_alpha]`, `[Total Variance]`, `[Total CM %]`; sort desc by Variance | Existing |

> `has_freight` no longer exists as a column (it lived on the retired `dim_shipment`). Derive "no freight $"
> from the deduped measures instead: `Shipments No Freight $ = CALCULATE([Freight Shipments], FILTER(VALUES(fact_sales_order_freight[shipment_number]), [Total Billable] = 0 && [Total Payable] = 0))`.

---

## Build rules (Direct Lake + spec fidelity)
- **Grain discipline:** never put freight $ **columns** on a line-level row — freight is shipment grain and would
  repeat across the shipment's lines. Use the deduped **measures** (`SUMX(VALUES(shipment_number), MAX(...))`) on
  Pages 1–2; line item/qty/price stays on Page 3. `is_primary_shipment_line="Y"` is the simpler anchor alternative.
- **Ratios are measures** (`DIVIDE(SUM,SUM)`) — never average a per-row %.
- **No soft-delete filter needed:** the fact **hard-deletes** (rows removed outright — no `is_deleted`/`is_active`
  column), so column-only/table visuals need no exclusion filter. (`dim_item` still soft-deletes if you surface it.)
- **v2.1 filtering is in the model, not the ETL:** all former Hubble WHERE filters were removed from the fact; apply
  Company / Line Type / Status / Price-Adjustment-Type / SIC / etc. as slicers or pane filters using the denormalized
  fact columns listed under Global slicers.
- **Dates (no date dimension):** slice the fact's raw date columns directly (relative-date slicers for MTD/YTD) — to
  view $ by GL or invoice date, put `fact_sales_order_freight[gl_date]` / `[invoice_date]` on the visual. Weekly
  grouping uses `fact_sales_order_freight[ship_year_week]`. No marked date table, no time-intelligence.
- **Stay in Direct Lake:** build on measures + dim attributes; avoid dumping high-cardinality raw keys onto one
  visual. Keeps the 5-min freshness window.
- **Interactions:** Page-1 charts cross-filter the cards; Page-2 matrix drills through to Page-3 on
  `shipment_number`; slicers synced on all pages.
- **Conditional formatting (set in Desktop):** Variance `<0` → red font; CM % data bars.
