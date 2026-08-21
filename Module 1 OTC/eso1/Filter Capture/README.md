# ESO1 Filter Capture — Variation Catalog & Power BI Filter Reference

**Purpose.** This folder holds ~110 legacy Hubble/JDE SQL report definitions that the ESO1 report
("Billable v Payable Freight" / Sales Order + Freight) must reproduce as **filter variations**. This
README documents, for every variation: the **business filter** it represents, the **Power BI page-level
filter** that reproduces it, and any **field gap** in the current Gold layer. The core query is
`../ESO1 Hubble Query.txt`; all JDE→Silver column names below are validated against `../full_metadata.json`
(authoritative metadata for the 25 tables used across ESO1: F4211, F42119, F4201, F0101, F0116, F4981,
F4074, F4101, F41002/F41003, F49211, F4941, F5642B01/B11, F0005/F0006/F0010, F42005, F4106, F03012, …).

**Governing principle (same as ESO5): the Gold layer applies NO report filters.** The fact carries the
full, unfiltered row population and every field the variations filter/display on; each report page then
applies its own slicers. The notebook already removed the business WHERE clauses
(`nb_eso1_gold_fact_sales_order_freight.py` line ~290: *"business WHERE filters removed — company /
line_type='S' / status<>980"*), so **company, order type, line type and status are already page-ready.**

---

## 1. Recommendation — UPDATE the existing notebooks; do NOT create new fact/dim tables

Of the ~110 variations, **~106 are sales-order-LINE grain** (driven by `F4211`), or simple roll-ups of
that grain (doc+item or ship-date summaries that a Power BI visual aggregates). They differ **only in
their WHERE filters** — plant, customer, parent, status window, item, SIC, product class, mode, date. A
single unfiltered order-line fact serves all of them.

**Therefore:**

| Object | Action |
|---|---|
| `fact_sales_order_freight` | **UPDATE** — add the gap columns in §3 + optionally the `F42119` history rows. Stays ONE fact, LINE grain, no filters. |
| `dim_item` | **UPDATE (minor)** — already covers item name/segment/UoM; add unit-weight UoM if needed (§3). |
| Reused dims (`rpt.dim_address_book` role views, `rpt.dim_plant`) | **REUSE — unchanged.** They keep serving the base ship-to / sold-to / carrier **name + address** via the fact's `ship_to` / `bill_to` / `carrier_number` FKs. The Filter Capture slicer/display attributes are **NOT** added to these dims — they are **denormalized onto the fact** via role-alias joins (see §3-B and §3.5). |
| **NEW `fact_sales_commission` (F42005)** | **Create ONLY IF** commission reporting is in scope — `SOP0027 - Commission.sql` is a genuinely different grain (commission line: salesperson, commission %, commission $). The only true separate-fact candidate. |
| "Multiple shipments / multiple orders" counts | **DAX measures**, not tables — `DISTINCTCOUNT` of shipment-per-order and order-per-shipment on the existing fact. |

**Why not new tables for the rest:** every other variation is the same F4211 line grain. Splitting them
into per-report facts would duplicate the same data dozens of times and defeat the one-fact/page-filter
design. The correct move is to widen the single fact's **columns** (and row population), never its count.

---

## 2. The page-level filter vocabulary (fact columns → what they replace)

Every variation's WHERE clause maps to one of these **already-present** fact columns. Configure these as
Power BI **page** (or report) filters. Values below are the recurring ones seen across the catalog.

| Fact column | JDE | Typical page-filter values seen |
|---|---|---|
| `company` | SDKCOO / SDCO | `00400`, `00390`, `00330`, `00640`, `00645`, `00750` |
| `order_type` (document type) | SDDCTO | `SO`, `CO`, `S1`, `SE`, `SZ`, `SM`, `ST`, `SX`, `SG` |
| `line_type` | SDLNTY | `S` (stock), `W`, `F`/`FT` (freight), `TL` (text) |
| `branch_plant` | SDMCU | plant codes: `501`, `561`, `521`, `151`, `171`, `061/062`, `071`, `321/341/161/181`, … |
| `status_code_last` | SDLTTR | `<> '980'` (not cancelled); range `520`–`528`; `< '980'` |
| `status_code_next` | SDNXTR | `529`, `530`, `560`, `561`, `577`, `580`, `620`, `999`; ranges `525`–`573`, `< '561'`, `< '620'`, `< '575'` |
| `ship_to` | SDSHAN | specific customer address numbers |
| `bill_to` (= sold-to / SDAN8) | SDAN8 | specific sold-to numbers |
| `address_number_parent` | SDPA8 | parent-customer numbers (the "by-customer" reports) |
| `carrier_number` | SDCARS | carriers |
| `item_number_short` / `second_item_number` | SDITM / SDLITM | item numbers, item exclusion lists, `LIKE 'MISC%'`, `<>'MISC BILLING'` |
| `standard_industry_code` (ship-to) | ABSIC | `F`, `FA`, `FB`, `ISPF` (frac-sand family); `NOT F%` (non-frac) |
| `category_code_05` / `category_code_14` (ship-to) | ABAC05 / ABAC14 | sales-team/region lists; `SAR` |
| `search_type` (ship-to) | ABAT1 | `A`–`P` OR `R`–`ZZZ` (the standard "real address" screen — already applied on the ship-to join) |
| `sales_reporting_code_01` / `major_prod_code`(SRP2) / `sales_reporting_code_03` / `minor_prod_code`(SRP4) | SDSRP1/2/3/4 | `BLK`, `PKG` (SRP3); `>110` / `<111` (SRP4) |
| `mode_of_transport` | SDMOT | rail set (`R%`, `RCP`, …), `OCE`, exclusions |
| `freight_handling_code` | SDFRTH | `PP`, `DLV`, `CC` |
| `payment_terms` | SDPTC | `CC`, `CTD` (cash-in-advance) |
| `hold_orders_code` | SDHOLD | blank / on-hold |
| `reference_01` (customer PO) | SDVR01 | e.g. literal `NPO` |
| dates: `order_date`,`requested_date`,`actual_ship_date`,`promised_ship_date`,`gl_date`,`invoice_date`,`cancel_date` | SDTRDJ/SDDRQJ/SDADDJ/SDPDDJ/SDDGL/SDIVD/SDCNDJ | date-range, "today", MTD/YTD, aging slicers |

> **Relative-date & fiscal-period filters** (e.g. `(126191 - SDADDJ)=0` "shipped today", MTD/period math
> via `F0010`) are **not** stored constants — reproduce them with a **Power BI date table + relative-date
> slicers** on the stored date columns. Do not bake them into Gold.

---

## 3. Fields to ADD to the Gold layer (source-to-target additions)

These are the columns/attributes the variations filter or display on that the current fact/dims do **not**
carry. Grouped by priority. (Verified against `nb_eso1_gold_fact_sales_order_freight.py`.)

> ### ✅ Implementation status (2026-07-15) — the high-value set is DONE
> The following were **added to `fact_sales_order_freight`** this pass (21 columns + the F42119 union):
> `extended_price` (SDAEXP ⭐), `extended_cost`, `currency_code` (SDBCRC — from F4211, **no F0010 needed**),
> `backorder_qty`/`cancelled_qty`/`qty_to_date`/`open_qty`, `line_description_1/2`, `date_updated`,
> `address_rate` (ship-to ABURAT), sold-to attrs `sold_to_name`/`sold_to_search_type`/`sold_to_category_05`/`_10`
> (new F0101 join on SDAN8), and F0116 ship-to postal address `ship_to_city`/`_state`/`_zip`/`_address_1`/`_2`/`_country`
> (new `f0116_address_by_date` source, latest-effective). **F42119 (Sales Order History)** is unioned into the
> row population and added as a 3rd union source. Its Silver name `f42119_sales_order_history_file` is now
> **CONFIRMED** in `full_metadata.json` (`table_name = sales_order_history_file`, identical 268-column `SD*`
> schema to F4211 — which is exactly why the `UNION ALL` is schema-safe). The notebook still **guards** the
> union with `tableExists` so it runs F4211-only if F42119 has not yet been ingested to Silver, then unions
> the closed/purged history rows once it is. Rebuild via `MANUAL_OVERWRITE=True` (schema change).
>
> **Already present — do NOT re-add** (README earlier over-flagged these): `gl_class`, `delivery_instruct_line_01/02`
> (from F4201). `user_reserved_amount` (SDURAB) is already surfaced as **`bol_number`**. `SDUORG`/`SDPQOR`/`SDSOQS`
> = `transaction_quantity`/`primary_quantity_ordered`/`quantity_shipped` (all present).
>
> **Deferred** (niche / not in the high-value set): F4074 adjustment detail (ALUPRC/ALUOM/ALBSDVAL), F49211
> UDDEFF, ocean-booking role names + vessel/voyage#/incoterm/booking dates, weigh-ticket weights (F5549002),
> serial/lot/location, `SDVR02/03`, `SDPSIG/SDZON/SDUSER/SDASN/SDURCD/SDPROV`, `SDSRP5`, plant/salesperson via
> dims. **Commission fact (F42005): OUT OF SCOPE** per decision. See §3-C/§5.

### 3-A. Row population (affects WHICH rows the fact holds)
| Gap | Source | Notes |
|---|---|---|
| **Historical / closed order lines** | **`F42119`** (Sales Order History) | ~40 variations `UNION ALL F42119` with live F4211 to include recently-closed/purged lines. Add F42119 as a 3rd union source. **Design decision — see §5.** |

### 3-B. High-value columns (very common — add first)
| Target column | JDE | Source | Used by (examples) |
|---|---|---|---|
| **`extended_price`** ⭐ | SDAEXP | F4211 | ~60 reports `SUM(SDAEXP)` — the primary sales-amount measure. **Currently absent.** |
| **`ship_to_city` / `ship_to_state` / `ship_to_zip` / `ship_to_address_1/2` / `ship_to_country`** | ALCTY1/ALADDS/ALADDZ/ALADD1/ALADD2/ALCTR | **F0116** (address-by-date) | ~40 open-order/invoice reports. Distinct from the fact's F4981 *freight* city/state. ✅ **Denormalized onto the fact** via the latest-effective `adr` (F0116) join. |
| **`delivery_instruction_1/2`** | SHDEL1/SHDEL2 | F4201 header | ~20 open-order reports. |
| **`currency_code`** | CCCRCD | F0010 | ~30 reports display it. |
| **`user_reserved_amount`** | SDURAB | F4211 | ~15 reports. |
| **`backorder_qty` / `cancelled_qty` / `qty_to_date`** | SDSOBK / SDSOCN / SDQTYT | F4211 | ~8 "by-customer" open-order summaries (measures). |
| **`open_qty`** | SDUOPN | F4211 | ~4 packaged/ground reports. |
| **`gl_class`** | SDGLC | F4211 | demurrage (`AE%`/`AJ%`), invoiced (`<>DZ01`, `<>26AN`), commission. |
| **sold-to attributes**: `sold_to_name`, `sold_to_search_type`, `sold_to_category_05`, `sold_to_category_10` | ABALPH/ABAT1/ABAC05/ABAC10 of SDAN8 (or F03012 AIAC05) | F0101 / F03012 | Days Since Invoice (sold-to ABAT1), SM Inside Sales / Mak / Orders on Hold (sold-to category). ✅ **Denormalized onto the fact** via a dedicated sold-to F0101 role join (`so` on SDAN8). |
| **`address_rate`** | ABURAT (ship-to) | F0101 | ~8 rate reports (ADM×6, Chevron, Ingredion, Grain Processing, Past 31 Days) — their **sole measure**. (Also used by ESO5.) |
| **`line_description_1/2`** | SDDSC1/SDDSC2 | F4211 | orders-on-hold, unbilled-AR, load reports (fact has item_name, not the line's own text). |
| **`parent_name`** | ABALPH of SDPA8 | F0101 | several "by-parent" reports. **Deferred** — `address_number_parent` (the FK) is on the fact; when the name is needed, denormalize it via a 4th parent F0101 role join (consistent with `st`/`so`/`dp`), not a new dim. |
| **`date_updated`** | SDUPMJ | F4211 | many open-order reports. |

### 3-C. Medium / niche columns
| Target | JDE | Source | Used by |
|---|---|---|---|
| `extended_cost` | SDECST | F4211 | SOP0025, commission |
| `serial_number` / `lot_number` / `location` | SDSERN / SDLOTN / SDLOCN | F4211 | SOP0020-lot, Shipped-without-Confirmation |
| `reference_02` / `reference_03` | SDVR02 / SDVR03 | F4211 | Ovintiv, Shipped-without-Confirmation (IFS order) |
| `supplier` | SDVEND | F4211 | SBX Unbilled AR |
| `print_signal` / `zone` / `entered_by_user` | SDPSIG / SDZON / SDUSER | F4211 | load reports, Orders-on-Hold, SOP status reports |
| `next_ship_status`/`user_reserved_code`/`provider` | SDASN / SDURCD / SDPROV | F4211 | SOP status reports |
| **F4074 adjustment detail**: `adj_unit_price` / `adj_uom` / `adj_basis_value` / `adj_gl_class` | ALUPRC / ALUOM / ALBSDVAL / ALGLC | F4074 | SOP0006/0007/0008/0025/577/580 (fact keeps only `price_adjustment_type`; add the picked row's amounts) |
| **shipment scheduled date** | UDDEFF | **F49211** | SOP0006/0007/577/580 (different table from F4941 routing) |
| **ocean-booking role names + detail**: `load_port_name` / `dest_port_name` / `ocean_carrier_name`, `incoterm`, `booking_pickup/deliver dates`, `vessel_number` / `voyage_number` | ABALPH of BA55LODP/DSTPT/OCCR; BA55INCO; BADLPU/BADEPU/BADEDL; BA55VLNO/BA55VONO | F0101 / F5642B01 | export/ocean reports (04a, AP Minerals, Mak, Luhe, Profiltra, Thai Tan) — fact joins F0101 for dest-point only + has `vessel_name` only |
| **weigh-ticket weights**: `gross_weight` / `tare_weight` / `max_weight` | MIGRWT / MICTWT / MIMXWT | **F5549002** (custom) | Halliburton, Ovintiv WTX (2 reports) |
| `alt_item_number` | SDAITM | F4211 | Daily NPO, Ottawa Rail |
| `sales_reporting_code_05` | SDSRP5 | F4211 | commission only |

### 3-D. Resolve via dimensions (no fact change — add relationship/attribute)
| Attribute | Source | Dim |
|---|---|---|
| Branch/plant **description** (MCDL01) | F0006 | reused `rpt.dim_plant` (Past 31 Days, Shipped-without-Confirmation) |
| **Salesperson description** (UDC) | F0005 | a UDC/salesperson dim (Orders-on-Hold, Zero-Unit-Price) |
| `unit_weight_uom` (IMUWUM) | F4101 | `dim_item` (minor) |

### 3-E. Already covered — do NOT re-add (agent over-flags corrected)
`sales_reporting_code_03` (SDSRP3) and `minor_prod_code` (SDSRP4) **are already stored** — SRP1/2/3/4 all
present. `category_code_05/14`, `standard_industry_code`, `search_type`, `payment_terms`,
`address_number_parent` (SDPA8), `promised_ship_date` (SDPDDJ), UoM→TN conversion (F41002 + DAX
`dim_uom_conversion`) are all present. `SDPQOR` vs `primary_quantity_ordered` — verify the two order
quantities are both stored.

---

## 3.5 Consolidated join model (duplicate joins removed)

The core query and the 108 variations, between them, reference the same handful of JDE tables over and over —
each report re-joins F0101, F0116, F4074, F4201, F4981 etc. and, for the open-order set, re-pastes
`UNION ALL proddta.F42119`. The Gold fact **consolidates every one of those into a single optimized join
graph** at order-**line** grain (`nb_eso1_gold_fact_sales_order_freight.py`, `build_fact`). Because the
whole population lives in one fact, no variation needs its own joins — each just page-filters columns (§2).

**Driver:** `F4211` **`UNION ALL`** `F42119` (alias `sd`) — live + history, identical 268-col `SD*` schema.

| # | Alias | Silver source | Type | Join key | Supplies (used by) |
|---|---|---|---|---|---|
| 1 | `sh` | F4201 order header | INNER | company_key_order_no + order# + order_type | `delivery_instruction_1/2`, header attrs (open-order set) |
| 2 | `b11` | F5642B11 booking detail¹ | LEFT | + line# + shipment# | `seal_no` (ocean) |
| 3 | `b01` | F5642B01 booking header¹ | LEFT | + shipment# | booking#, dest-port, container ct, ocean terms, vessel, pickup/deliver dates (export/ocean) |
| 4 | `dp` | **F0101 (role: destination-port)** | LEFT | `b01.destination_port` | dest-point name (ocean) |
| 5 | `st` | **F0101 (role: ship-to)** | LEFT | `sd.address_number_ship_to` | `standard_industry_code`, `category_code_05/14`, `search_type`, **`address_rate`** (rate reports) |
| 6 | `so` | **F0101 (role: sold-to, SDAN8)** | LEFT | `sd.address_number` | `sold_to_name/_search_type/_category_05/_10` (Days-Since-Invoice, SM Inside Sales, Mak, Orders-on-Hold) |
| 7 | `im` | F4101 item master | LEFT | item short-# | item name / segment / UoM |
| 8 | `us` | F41002 UOM structure | LEFT | item + uom | `uom_structure` |
| 9 | `ci` | F41002/F41003 UoM→TN cascade | LEFT | item + from-uom | tons conversion (DAX fallback) |
| 10 | `al` | F4074 price-adjustment ledger¹ | LEFT | + line# | `price_adjustment_type`, `freight_factor_value` (SOP/finance) |
| 11 | `rt` | F4941 shipment routing¹ | LEFT | shipment# | `route_number` |
| 12 | `fr` | F4981 freight-audit buckets | LEFT | shipment# | billable/payable freight & fuel (master measures) |
| 13 | `adr` | **F0116 address-by-date¹** | LEFT | `sd.address_number_ship_to` | `ship_to_city/_state/_zip/_address_1/_2/_country` (open-order/invoice set) |

¹ *Pre-collapsed to one row per join key before the LEFT join (window `row_number` or `groupBy/first`), so
consolidating these many-per-key sources cannot fan the line grain out.*

### Duplicate joins removed / consolidated

| Table | In the 109 raw queries | After consolidation |
|---|---|---|
| **F0101 (Address Book)** | joined **up to 3× per report** — separately for ship-to (SIC/rate), sold-to (name/category), and ocean destination-port — and again for parent | **one join per role**, reused by all: `st` + `so` + `dp`, each **denormalizing its attributes onto the fact**. Every variation's address lookup maps to an existing alias. *(parent-name role not yet aliased → deferred; when added, a 4th `pa` role denormalized on the fact, same pattern.)* |
| **F0116 (Address by Date)** | independently re-joined in ~40 open-order/invoice variations for the ship-to postal address | **one** latest-effective join (`adr`) |
| **F42119 (Sales Order History)** | `UNION ALL proddta.F42119` copy-pasted into ~40 open-order variations | **one** guarded union in the driver |
| **F4074 (Price Adjustment Ledger)** | master + ~10 SOP/finance variations each re-derive the freight/price adjustment | **one** deterministic single-row pick (`al`) |
| **F4201 (Order Header)** | joined by most variations for delivery instructions / order-level attrs | **one** inner join (`sh`) |
| **F4981 / F4941 / F5642B01+B11** | re-joined across load & ocean variations | **one** pre-collapsed join apiece (`fr` / `rt` / `b01`,`b11`) |

**Result:** ~13 joins + 1 union serve **all 109 reports**. Adding a variation = adding page filters, never joins.

### What deliberately stays OUT of the Gold joins (→ Power BI page filters)

All **row-selecting** predicates from the variations — company, order/line type, status windows
(`status_code_next/last`), plant, customer/parent, SIC, product class, mode, freight/payment/hold codes, and
all date ranges — are **not** applied in Gold; they become the page filters of §2/§4. Only **value-computing**
predicates remain in the model: the freight-bucket `CASE`s inside `transform_freight_buckets`, the join
`ON` conditions, and the address-book search-type screen carried as the `st`/`so` role *attributes* (not as a
row filter). Removing those would make columns **wrong**, not unfiltered — see the filter-vs-calculation rule
in the design doc. The final `dropDuplicates(["sales_order_line_key"])` keeps one row per line across any
live/history (F4211/F42119) overlap.

---

## 4. Variation catalog (all files, by family)

Each row: the file, the **business filter** it represents, and the **Power BI page filter** (fact columns).
Filters shown are the row-selecting ones; the ubiquitous ship-to `ABAT1 A–P/R–ZZZ` search-type screen is
already applied on the fact's ship-to join and is omitted below.

### Family 1 — Customer freight-rate lookups (measure = ship-to `ABURAT`)
> Grain: doc+item. **GAP: `address_rate`** (their only measure). PBI: filter `ship_to`, `status_code_next`,
> `status_code_last`; show `Address Rate` measure.

| File | Page filter |
|---|---|
| ADM Cedar Rapids | `ship_to=20022727` · `next=529` · `last∈520–528` |
| ADM Clinton | `ship_to=20010840` · `next=529` · `last∈520–528` |
| ADM Deerfield | `ship_to=20010852` · `next=529` · `last∈520–528` |
| ADM Enderlin | `ship_to=20010848` · `next=529` · `last∈520–528` |
| ADM Lloydminster | `ship_to=10117212` · `next=529` · `last∈520–528` |
| ADM Windsor | `ship_to=20010836` · `next=529` · `last∈520–528` |
| Chevron | `ship_to=20011635` · `next=529` · `last∈520–528` |
| Ingredion | `ship_to=20011316` · `next=529` · `last∈520–528` |
| Grain Processing | `ship_to=20012646` · `next=529` · `last∈520–528` |

### Family 2 — Open orders by branch/plant (`branch_plant` + `status_code_next`, company 00400)
> Grain: order-line / doc+item. GAPS: `F42119`, `ship_to_city/state`, `delivery_instruction_1/2`,
> `currency_code`, `date_updated`, `extended_price`. PBI: filter `branch_plant`, `status_code_next`, `company`.

| File | Page filter |
|---|---|
| 23.Dubberly Open Orders | `plant=561` · `next=560` · `co=00400` |
| Montpelier Open Orders | `plant=521` · `next=560` · `co=00400` |
| Ottawa Open Orders | `plant=501` · `next∈530,560` · `line_type∈S,W` · `uom<>EA` |
| 24.Florisil Open Orders | `plant=351` · `next<561` · `co=00400` |
| 25.Hurtsboro Open Orders | `plant=581` · `next=560` · `co=00400` |
| Berkeley Springs Open Orders | `plant=151` · `next=560` · `co=00400` |
| Columbia Open Orders | `plant=171` · `next∈530,560` · `co=00400` |
| Columbia Open Bagged Orders | `plant=171` · `next∈530,560` · `uom=BG` |
| Mapleton Open Orders | `plant=131` · `next=560` · `co=00400` |
| Rockwood Open Orders | `plant=511` · `next=560` · `co=00400` |
| 40 Jackson Open Orders | `plant=071` · `next=560` · `co=00400` · `doc=SO` |
| Millen-Open Order Report | `plant=551` · `next∈530,560` · `doc∈SO,SG` |

### Family 3 — Open orders by parent / ship-to customer (`address_number_parent` or `ship_to`, status window 525–573)
> Grain: order-line / doc+item. GAPS: `F42119`, `backorder_qty`/`cancelled_qty`/`qty_to_date` (measures),
> `ship_to_city/state`, `currency_code`. PBI: filter `address_number_parent` (or `ship_to`), `status_code_next`.

| File | Page filter |
|---|---|
| Colortech | `parent=10116596` · `next∈525–573` |
| Solvay Open Order Report | `ship_to=20015341` · `next∈525–573` |
| Napa | `parent=20022384` · `next∈525–573` · `line_type=S` · `last<>980` |
| Tri-Iso | `parent=20022632` · `next∈525–573` · `line_type=S` · `last<>980` |
| Safety Kleen | `parent=20022519` · `next∈525–573` · `line_type=S` · `last<>980` |
| Amalgamated Sugar Company | `ship_to=10125974` · `next∈525–573` · `line_type=S` · `last<>980` |
| Grainger | `parent=20022190` · `next∈525–573` · `line_type=S` · `last<>980` |
| L&M Environmental | `parent=20022305` · `next∈525–573` · `line_type=S` · `last<>980` |
| Leslie's Poolsmart | `parent=20022322` · `next∈525–573` · `line_type=S` · `last<>980` |
| Standridge Open Order Report | `parent=20022569` · `next∈525–573` |
| Ampacet | `ship_to=20015949` · `next∈525–573` |
| CCC Plastics | `ship_to=10162144` · `sold_to=10162143` · `next∈525–573` |
| Sto Corp | `parent=20022576` · `next∈525–573` |
| Cargill | `ship_to=20011381` · `next<575` · `line_type=S` |
| Polyfil | `ship_to=20015955` · `next∈525–573` |

### Family 4 — Open orders by company/region/date-range & product class
> Grain: order-line. GAPS: `F42119`, `ship_to_city/state`, `delivery_instruction_1/2`, `currency_code`,
> `user_reserved_amount`, `open_qty`. PBI: filter `requested_date` range, `status_code_next<561`, `company`,
> plus `standard_industry_code`, `mode_of_transport`, product `sales_reporting_code_03/minor_prod_code`, item lists.

| File | Page filter |
|---|---|
| Open Order Report | `req_date≥2016` · `next=560` · `co∈00400,00390,00330` |
| Open Order Report - LF2022 | `order=1552979` · `next=560` · `co=00400` · `SIC=F` |
| 36 ISPCAMSP-Open Order Report | `doc=SO` · `next∈530,560` · `cat14=SAR` |
| SM-Open Order Report | `doc=SO` · `next∈530,560` · `co=00400` · `SIC<>F` |
| SM Inside Sales-Open Order Report | `next∈530,560` · `cat05∈(S15,S13,…)` |
| SM Planning Open Orders | `req_date≥2016` · `next<561` · `co∈400,390,330` · `SIC<>F` |
| SM Past Due Orders | `doc=SO` · `next∈530,560` · `co=00400` · `SIC not F%` · `requested_date < today` |
| SM Trucking-Past Due Orders | `doc=SO` · `next∈530,560` · `mode not R%` · `freight=PP` · `SIC not F%` · `requested_date<today` |
| Cash In Advance-Open Order Report | `next∈530,560` · `payment_terms∈CC,CTD` |
| Daily Open Orders Report | `doc=SO` · `next=560` · `co=00400` · `SIC∈F,FA,FB` |
| Daily Open Orders Report NPO Aging | `last<980` · `doc=SO` · `next=560` · `item not MISC%/TR%` · `line_type not F/FT` · `cust_PO=NPO` · `SIC∈F,FA,FB` |
| Supply Chain Planning Llamasoft Open Orders | `doc=SO` · `next=560` · `SIC∈F,FA,FB,ISPF` |
| Ottawa - All Open Orders | `req_date≥2016` · `plant=501` · `next<561` · `uom<>EA` · `co∈400,390,330` |
| Ottawa Open Rail Orders | `plant=501` · `next=560` · `mode∈(rail set)` · item exclusions |
| Ottowa - ASTM - Packaged | `plant∈061,062` · `next<999` · `SRP4>110` |
| Ottowa - Ground - Packaged | `plant=501` · `next<561` · `SRP3=PKG` · `uom<>EA` · item whitelist |
| Ottowa - Ground- Bulk | `plant=501` · `next<561` · `SRP3=BLK` · `uom<>EA` · item list |
| Ottowa - Whole Grain Rail - Bulk | `plant=501` · `next<561` · `SRP3=BLK` · `mode=RCP` · item exclusions |
| Ottowa - Whole Grain Truck - Bulk | `plant=501` · `next<561` · `SRP3=BLK` · `mode not RCP` · `uom not EA` |
| Ottowa - Whole Grain Truck - Packaged | (as Ottawa All Open) + item exclusions + `SRP3=PKG` |
| Pacific Open Orders-Ground Products | `plant=501` · items(3) · `next<561` · `uom<>EA` · `co∈400,390,330` |
| Pacific Open Orders-Rail | `plant=061` · `next<999` · `mode R%` · `co=00400` |
| Pacific Open Orders-Whole Grain | `plant=061` · `next<570` · `SRP4<111` · `uom=TN` |

### Family 5 — Load / loaded-tons reports (`actual_ship_date` relative + parent/plant, SIC=F)
> Grain: doc+item / ship-date summary. GAPS: `currency_code`, `F42119`, fiscal-period (report-side date
> table), Halliburton/Ovintiv **weigh-ticket weights (F5549002)**. PBI: filter `address_number_parent`/`branch_plant`,
> relative `actual_ship_date`, `standard_industry_code=F`.

| File | Page filter |
|---|---|
| Pioneer WTX - Loaded Tons Update | `plant∈321,341,161,181` · `ship_to∈(2)` · `parent=10112037` · `ship_date=today` · `SIC=F` |
| Cudd WTX - Loaded Tons Update | `plant∈321,341,9705,9786,161,181` · `parent=10058491` · `ship_date=today` · `SIC=F` |
| 21.DE Central Operating WTX - Loaded Tons Update | `doc=SO` · `plant∈321,341,161,181` · `parent=10152005` · `ship_date=today` · `SIC=F` |
| DE Central Operating - Previous Day Load Report | `doc=SO` · `plant∈321,341,161,181` · `parent=10152005` · `ship_date=yesterday` |
| Gore Daily Load Report | `doc=SO` · `parent=10086687` · `ship_date=today` |
| Regional MInes - Load Report | `doc=SO` · `plant∈(7)` · `ship_date=today` · `SIC=F` |
| Ovintiv WTX Load Report - MTD ALL | `parent=10061291` · `doc=SO` · `co=00400` · `plant∈321,341,181,161` · ship_date MTD |
| Ovintiv Year to Date Load Report | `parent=10061291` · `ship_date≥YTD-start` · `item<>MISC BILLING` |
| 41 Liberty Oilfield Load Report | `doc=SO` · `parent=10065562` · ship_date current fiscal period |
| Halliburton Load Report | `parent=10043240` · `doc=SO` · `co=00400` · ship_date current fiscal month · **+ weights** |
| Continental Load Report - MC | `doc=SO` · `plant=081` · `parent=10112501` · `item<>MISC BILLING` · `ship_date=yesterday` (previous-day load report — NOT a Cargill clone; corrected 2026-07-16 from source SQL) |

### Family 6 — Invoiced / monthly / finance (`status_code_next=999` + gl/invoice-date period)
> Grain: order-line (invoice-status). GAPS: `currency_code`, fiscal-period, `gl_class`, F4074 adjustment
> detail, F0116 address, `extended_price`. PBI: filter `status_code_next=999`, `gl_date`/`invoice_date` range,
> `address_number_parent`, `gl_class`.

| File | Page filter |
|---|---|
| SCP Monthly Invoice Report | `parent=20022530` · `next=999` · `last<>980` · `co∈00640,00645` · invoice prior fiscal period |
| CL National Accounts Monthly Invoice Report | `parent∈(8 nat'l accts)` · `next=999` · `last<>980` · invoice current fiscal month |
| Leslie's Poolmart Monthly Invoice Report | `parent=20022322` · `next=999` · `last<>980` · `item=CPOOL24LV35LPGBX` · `co∈00640,45` · invoice_year=2025 |
| Baseline Report | `ship_date=single day` · `doc∈SO,CO` |
| Baseline Report (Finance) | `ship_date∈FY2024` · `SIC∈(~150 list)` · F4074 freight adj types |
| DE Orders | `line_type=S` · `last<>980` · `co∈00640,00645` · F4074 adj types |
| SOP0025 Monthly Sales Report - Detail | `plant=061` · `doc∈SO,CO` · `last<980` · `next=999` · `line_type not F/FT` · gl_date month · `gl_class<>26AN` |
| SOP0007 Invoiced Orders | `doc not ST,SG` · `last<>980` · `next=999` · `gl_class<>DZ01` · `line_type<>TL` · gl_date month |
| SOP0008 Pioneer Natural Resources Sales | `plant=341` · `doc∈SO,CO` · `last<>980` · `next=999` · gl_date period · `parent=10059472` |
| BP Freight and Fuel (Combined) Jan 2026 | `gl_date∈Jan-2026` · `freight∈DLV,PP` · `line_type=S` · `next=999` · `SIC<>F` · `ship_to=20022745` |
| Sherwin Williams MTD Shipments | `ship_date≥MTD` · `doc=ST` · `ship_to∈(internal ~28)` · `next>560` |
| Days Since Invoice | `company=00750` · **sold-to `search_type=CB`** (needs sold-to attr) |
| REI DEMURRAGE MONTHLY BILLINGS | `ship_date≥2020` · `gl_class LIKE AE%/AJ%` · `item LIKE MISC%` · `next=999` · `SIC<>F` |
| SBX Unbilled AR | `order_type=SX` · `next<620` · `company=00750` |

### Family 7 — Export / ocean-booking
> Grain: order-line + booking. GAPS: ocean role names, booking dates, vessel/voyage#, incoterm, **sold-to
> category (F03012 AIAC05)**. PBI: filter `order_type`/`ship_to`/parent + ocean `mode_of_transport`.

| File | Page filter |
|---|---|
| 04a Export Open Orders | `doc=SE` · `co∈00640,00645` · `line_type=S` · `plant=651` · `next not 980,999` · ocean mode |
| AP Minerals | `next<>999` · `last<>980` · `line_type=S` · `parent=20022844` · ocean mode |
| Mak Export Orders | `order∈(72 numbers)` · `last<>980` · sold-to `cat05=E26` · `line_type=S` |
| Luhe | `next<>999` · `last<>980` · `line_type=S` · `parent=10112541` · ocean mode |
| Profiltra | `next<>999` · `last<>980` · `line_type=S` · `ship_to∈(2)` |
| Thai Tan | `next<>999` · `last<>980` · `line_type=S` · `ship_to∈(3)` |

### Family 8 — Status-window inquiries (SOP)
> Grain: order-line. GAPS: **F49211 UDDEFF**, `gl_class`, lot/serial/location, F4074 adjustment detail,
> F0116 address. PBI: filter `status_code_next` (specific) + doc-type excludes.

| File | Page filter |
|---|---|
| SOP0020 Sales Order Inquiry | `company=00400` (full inquiry) |
| SOP0020 Sales Order Inquiry with lot | `company=00400` + shows lot/serial |
| SOP0020Lite | `ship_date=single day` · `order_type=ST` |
| SOP0020Lite - 90 Day Transload Sales | `doc∈SO,CO` · plant `LIKE %97/%98` (transload) · `SIC∈F,FA,FB` · ship_date last 90 days |
| SOP0006 Shipped NOT Invoiced Order Inquiry | `doc∈SO,CO` · `last<980` · `next∈574–620` · order_date after · F4074 adj types |
| SOP000x - Sales Orders at Next Status 577 | `doc<>SX` · `last<980` · `next=577` · F4074 adj types |
| SOP000x - Sales Orders at Next Status 580 - SO CO & ST | `doc<>SX` · order_date≥2015 · `last<980` · `next=580` · F4074 adj types |
| SOP000x - Sales Orders at Next Status 620 | `doc<>SX` · `last<980` · `next=620` · F4074 adj types |

### Family 9 — Exceptions & scheduler reports
> Mostly order-line exception lists; **two are distinct grains (→ DAX measures)**.

| File | Page filter / handling |
|---|---|
| Orders with Zero Unit Price - Scheduler | `unit_price=0` (or no effective F4106 base price) · `line_type=S` · `co∈00640,45` · `doc∈S1,SE,SZ` · `last<>980` · `next<620`. **GAP: F4106 base-price test + F0005 salesperson** → add a `has_effective_price` flag or exception view. |
| Orders on Hold for Pricing - Scheduler | header `hold∈ZP,WP` OR zone=Y · `co∈640,645` · `doc∈S1,SE,SZ` · `line_type=S`. GAP: `zone`, sold-to/parent names, salesperson. |
| Shipped without Shipment Confirmation - Scheduler | `ship_date>` · `co∈640,645` · `last<>980` · `line_type=S` · has container id · `next<571` · `doc∈S1,SE,SZ`. GAP: location/lot, F0006 plant name. |
| Short Ship Notifications - Scheduler | `plant∈651,661` · `line_type=S` · `doc=SE` · `next=999` · `last=980` (cancelled) · `co∈640,645` · cancel_date=yesterday. GAP: `cancelled_qty`. |
| Past 31 Days Shipment Report | `doc=SO` · `SIC∈F,ISPF` · no hold · ship_date last ~40 days. GAP: F0006 plant desc, `address_rate`. |
| **Order with Multiple Shipments - Scheduler** | **Order-header aggregate** — `co∈640,645` · `last<>980` · `doc∈S1,SE,SZ,SM,SO` · `next<620` · orders with `DISTINCTCOUNT(shipment_number) > 1`. → **DAX measure**, not a fact row. |
| **Shipment with Multiple Orders - Scheduler** | **Shipment aggregate** — `co∈640,645` · `last<>980` · `doc∈S1,SE,SZ,SM,SO` · `next<620` · shipments with `DISTINCTCOUNT(order_number) > 1`. → **DAX measure**, not a fact row. |

### Family 10 — Commission (SEPARATE FACT)
| File | Handling |
|---|---|
| SOP0027 - Commission | **Commission-line grain (`F42005`)** — salesperson, commission %, `SCTOTL/SCLRCS/SCCOMA` amounts, extended cost. Filters: `next=999`, `last<>980`, `line_type not F/FT`, gl_date year, `doc∈SO,CO`, `co<>00750`. → **new `fact_sales_commission`** if commission reporting is in scope; do NOT fold into the freight fact. |

### Baseline (not a variation)
| File | Note |
|---|---|
| ESO1 Hubble Query | The MASTER report the fact was built from (F4211 ⋈ F4981 ⋈ F5642 booking; `line_type=S`, `last<>980`, `co∈00640,00645`, F4074 freight-adjust types). Defines the current column set. |

---

## 5. Open design decisions (confirm before implementing)

1. **F42119 history — include? → RESOLVED: YES (implemented).** ~40 "open order" variations `UNION ALL
   F42119` to pick up recently closed/purged lines, so the fact unions it (3rd union source). Silver
   name `f42119_sales_order_history_file` confirmed in `full_metadata.json`; union is `tableExists`-guarded
   so it degrades to F4211-only until F42119 is ingested to Silver.
2. **Commission fact** — build `fact_sales_commission` (F42005) only if SOP0027 is in ESO1's scope.
3. **Sold-to / parent / ship-to F0116 attributes — dim or fact? → RESOLVED: denormalized onto the FACT.**
   The Filter Capture slicer/display attributes — ship-to `standard_industry_code`/`category_code_05`/`_14`/
   `search_type`/`address_rate` (`st`), sold-to `sold_to_name`/`_search_type`/`_category_05`/`_10` (`so`),
   dest-point `dest_point_name_alpha` (`dp`), and F0116 `ship_to_city`/`_state`/`_zip`/`_address_1`/`_2`/
   `_country` (`adr`) — are landed **directly on the line-grain fact** via role-alias joins, **not** added to
   the address dims. The reused `rpt.dim_address_book` role views stay **unchanged**, still serving the base
   ship-to/sold-to/carrier **name + address** through the fact's `ship_to`/`bill_to`/`carrier_number` FKs.
   *Rationale:* these are page-level slicer/display fields on a single line-grain fact — denormalizing keeps
   every variation a pure page-filter on fact columns and avoids editing the shared conformed dims (owned by
   other jobs). Parent name stays deferred and, when needed, follows the same denormalized role-join pattern.
4. **Weigh-ticket weights (F5549002)** and **F49211 UDDEFF** — add only if Halliburton/Ovintiv and the SOP
   status reports are in scope.
5. **Exception logic** (Zero Unit Price's F4106 test, Multiple-Shipments/Orders counts) — implement as DAX
   measures / a computed flag, not new fact rows.

**None of these change the core answer: extend the one fact + the shared dims; do not build per-report facts.**
