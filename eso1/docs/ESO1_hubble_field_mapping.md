# ESO1 Hubble Query — Field Mapping (JDE → snake_case)

**Purpose:** authoritative source-to-target column map for the **Extended Sales Order 1 (ESO1) — Billable v Payable Freight** report. Resolves every field selected in the deployed Hubble "BvP Combined" query (`ESO1 Hubble Query.txt`, outermost `SELECT`, lines 1–77) to the `snake_case_field` label defined in `full_metadata.txt`. Use this for the Gold fact (`lh_jde_gold.rpt.fact_sales_order_freight`) S2T mapping.

- **Source query:** `ESO1 Hubble Query.txt` (Oracle/Hubble, PRODDTA schema)
- **Metadata dictionary:** `full_metadata.txt` (per-table `column_name → snake_case_field`)
- **Final projection:** 77 columns = 63 attributes + 5 measures + 9 internal row-ID hashes
- **Grain:** one row per sales-order line (F4211 `SDKCOO`+`SDDOCO`+`SDDCTO`+`SDLNID`)
- **Companion docs:** `ESO1 Filter Capture/README.md` (variation catalog + consolidated join model), `ESO1 Filter Capture/PAGE_FILTER_CHEATSHEET.md` (exact per-report Power BI page-slicer settings for all 107 variations), `docs/ESO1_gold_layer_design.md` (Gold architecture)

---

## Full field list in query order (77)

Exact order of the outer `SELECT` (lines 1–77). Rows 1–63 = attributes, 64–68 = measures, 69–77 = Hubble internal row-ID hashes (exclude from Gold).

| # | Hubble alias | snake_case_field |
|---|---|---|
| 1 | F4201_SHHOLD | hold_orders_code |
| 2 | F0101_ABSIC | standard_industry_code |
| 3 | C_F4211_SDMCU | cost_center *(calc copy)* |
| 4 | F4211_SDMCU | cost_center |
| 5 | F4211_SDAN8 | address_number |
| 6 | F4211_SDSHAN | address_number_ship_to |
| 7 | F0101_ABALPH | name_alpha *(ship-to)* |
| 8 | F0116_ALCTY1 | city *(ship-to)* |
| 9 | F0116_ALADDS | state *(ship-to)* |
| 10 | F4211_SDDOCO | document_order_invoice_e |
| 11 | F4211_SDLNID | line_number *(÷1000)* |
| 12 | F4211_SDDCTO | order_type |
| 13 | F4211_SDTRDJ | date_transaction_julian |
| 14 | F4211_SDDRQJ | date_requested_julian |
| 15 | F4211_SDPDDJ | scheduled_pick_date |
| 16 | F4211_SDPPDJ | date_promised_ship_julian |
| 17 | F4211_SDADDJ | actual_ship_date |
| 18 | F4211_SDLTTR | status_code_last |
| 19 | F4211_SDNXTR | status_code_next |
| 20 | F5642B11_AK55SELN | seal_no |
| 21 | F4211_SDCNID | container_id |
| 22 | F4211_SDMOT | mode_of_transport |
| 23 | F4211_SDUOM | uom_as_input |
| 24 | F4211_SDUPRC | amt_price_per_unit_02 *(÷1e6)* |
| 25 | F4211_SDUOM4 | uom_pricing |
| 26 | F4211_SDPEFJ | date_price_effective_date *(line)* |
| 27 | F4201_SHPEFJ | date_price_effective_date *(header)* |
| 28 | F4211_SDFRTH | freight_handling_code |
| 29 | F4211_SDCARS | carrier |
| 30 | F5642B01_BA55BKNO | booking_no |
| 31 | F5642B01_BADEPU | date_earliest_pickup |
| 32 | F5642B01_BADLDL | date_latest_delivery |
| 33 | F0101_1_ABALPH | name_alpha *(dest point)* |
| 34 | F5642B01_BA55DSTPT | destination_port |
| 35 | F5642B01_BA55NCON | no_of_container |
| 36 | F5642B01_BA55OCDLT | ocean_del_terms |
| 37 | F5642B01_BA55VLNO | vessel_name |
| 38 | F4211_SDLITM | identifier_second_item |
| 39 | F4211_SDPA8 | address_number_parent |
| 40 | F4211_SDURAB | user_reserved_number |
| 41 | F4211_SDVR01 | reference_01 |
| 42 | F4211_SDSRP1 | sales_reporting_code_01 |
| 43 | F4211_SDTORG | transaction_originator |
| 44 | F4201_SHDEL1 | delivery_instruct_line_01 |
| 45 | F4201_SHDEL2 | delivery_instruct_line_02 |
| 46 | F4211_SDODCT | original_document_type |
| 47 | F4211_SDOORN | original_po_so_number |
| 48 | F4211_SDODOC | original_document_no |
| 49 | F4211_SDLNTY | line_type |
| 50 | F4211_SDCNDJ | cancel_date |
| 51 | F4981_FHCTY1 | city *(freight)* |
| 52 | F4981_FHADDS | state *(freight)* |
| 53 | F4981_FHADDZ | zip_code_postal |
| 54 | F4211_SDDOC | doc_voucher_invoice_e |
| 55 | F4211_SDIVD | date_invoice_julian |
| 56 | F4211_SDURRF | user_reserved_reference |
| 57 | F4101_IMUWUM | uom_weight |
| 58 | F4211_SDSRP2 | sales_reporting_code_02 |
| 59 | F4211_SDSRP3 | sales_reporting_code_03 |
| 60 | F4211_SDSRP4 | sales_reporting_code_04 |
| 61 | F4211_SDGLC | gl_class |
| 62 | F41002_UMCONV | conversion_factor *(÷1e7)* |
| 63 | F4211_SDSHPN | shipment_number |
| 64 | ReportColumn8 | units_quantity_shipped *(SUM SDSOQS)* |
| 65 | ReportColumn9 | units_primary_qty_order *(SUM SDPQOR)* |
| 66 | ReportColumn13 | units_transaction_qty *(SUM SDUORG)* |
| 67 | ReportColumn14 | amt_price_per_unit_02 *(SUM F4074 ALUPRC)* |
| 68 | ReportColumn15 | net_amount *(SUM F4981 FHNAMT × ShiftFactor)* |
| 69 | RowIDX_F4211_2_XRowID | *(internal hash)* |
| 70 | RowIDX_F4211_1_XRowID | *(internal hash)* |
| 71 | RowIDX_F4211_0_XRowID | *(internal hash)* |
| 72 | RowIDX_F4981_2_XRowID | *(internal hash)* |
| 73 | RowIDX_F4981_1_XRowID | *(internal hash)* |
| 74 | RowIDX_F4981_0_XRowID | *(internal hash)* |
| 75 | RowIDX_F4074_2_XRowID | *(internal hash)* |
| 76 | RowIDX_F4074_1_XRowID | *(internal hash)* |
| 77 | RowIDX_F4074_0_XRowID | *(internal hash)* |

---

## Attributes (63) — grouped by source table

### F4211 — Sales Order Detail (driver)
| Hubble alias | JDE col | snake_case_field | Notes |
|---|---|---|---|
| C_F4211_SDMCU | SDMCU | cost_center | calc copy of branch/plant |
| F4211_SDMCU | SDMCU | cost_center | branch/plant |
| F4211_SDAN8 | SDAN8 | address_number | sold-to |
| F4211_SDSHAN | SDSHAN | address_number_ship_to | |
| F4211_SDDOCO | SDDOCO | document_order_invoice_e | sales order number |
| F4211_SDLNID | SDLNID | line_number | ÷1000 |
| F4211_SDDCTO | SDDCTO | order_type | |
| F4211_SDTRDJ | SDTRDJ | date_transaction_julian | order date |
| F4211_SDDRQJ | SDDRQJ | date_requested_julian | |
| F4211_SDPDDJ | SDPDDJ | scheduled_pick_date | |
| F4211_SDPPDJ | SDPPDJ | date_promised_ship_julian | |
| F4211_SDADDJ | SDADDJ | actual_ship_date | |
| F4211_SDLTTR | SDLTTR | status_code_last | |
| F4211_SDNXTR | SDNXTR | status_code_next | |
| F4211_SDCNID | SDCNID | container_id | |
| F4211_SDMOT | SDMOT | mode_of_transport | degenerate code (raw F4211 SDMOT on the fact) |
| F4211_SDUOM | SDUOM | uom_as_input | |
| F4211_SDUPRC | SDUPRC | amt_price_per_unit_02 | ÷1,000,000 |
| F4211_SDUOM4 | SDUOM4 | uom_pricing | |
| F4211_SDPEFJ | SDPEFJ | date_price_effective_date | line; collides w/ F4201 |
| F4211_SDFRTH | SDFRTH | freight_handling_code | |
| F4211_SDCARS | SDCARS | carrier | |
| F4211_SDLITM | SDLITM | identifier_second_item | |
| F4211_SDPA8 | SDPA8 | address_number_parent | |
| F4211_SDURAB | SDURAB | user_reserved_number | |
| F4211_SDVR01 | SDVR01 | reference_01 | customer PO |
| F4211_SDSRP1 | SDSRP1 | sales_reporting_code_01 | |
| F4211_SDTORG | SDTORG | transaction_originator | |
| F4211_SDODCT | SDODCT | original_document_type | |
| F4211_SDOORN | SDOORN | original_po_so_number | |
| F4211_SDODOC | SDODOC | original_document_no | |
| F4211_SDLNTY | SDLNTY | line_type | filter = 'S' |
| F4211_SDCNDJ | SDCNDJ | cancel_date | |
| F4211_SDDOC | SDDOC | doc_voucher_invoice_e | invoice number |
| F4211_SDIVD | SDIVD | date_invoice_julian | |
| F4211_SDURRF | SDURRF | user_reserved_reference | |
| F4211_SDSRP2 | SDSRP2 | sales_reporting_code_02 | |
| F4211_SDSRP3 | SDSRP3 | sales_reporting_code_03 | |
| F4211_SDSRP4 | SDSRP4 | sales_reporting_code_04 | |
| F4211_SDGLC | SDGLC | gl_class | |
| F4211_SDSHPN | SDSHPN | shipment_number | freight join key |

### F4201 — Sales Order Header
| Hubble alias | JDE col | snake_case_field | Notes |
|---|---|---|---|
| F4201_SHHOLD | SHHOLD | hold_orders_code | |
| F4201_SHPEFJ | SHPEFJ | date_price_effective_date | header; collides w/ F4211 |
| F4201_SHDEL1 | SHDEL1 | delivery_instruct_line_01 | |
| F4201_SHDEL2 | SHDEL2 | delivery_instruct_line_02 | |

### F0101 — Address Book (ship-to & destination-point)
| Hubble alias | JDE col | snake_case_field | Notes |
|---|---|---|---|
| F0101_ABSIC | ABSIC | standard_industry_code | |
| F0101_ABALPH | ABALPH | name_alpha | ship-to name |
| F0101_1_ABALPH | ABALPH | name_alpha | destination-point name (2nd join) |

### F0116 — Address Book Address
| Hubble alias | JDE col | snake_case_field | Notes |
|---|---|---|---|
| F0116_ALCTY1 | ALCTY1 | city | ship-to; collides w/ F4981 |
| F0116_ALADDS | ALADDS | state | ship-to; collides w/ F4981 |

### F5642B11 — Shipment booking line (US Silica custom)
| Hubble alias | JDE col | snake_case_field | Notes |
|---|---|---|---|
| F5642B11_AK55SELN | AK55SELN | seal_no | |

### F5642B01 — Shipment booking header (US Silica custom)
| Hubble alias | JDE col | snake_case_field | Notes |
|---|---|---|---|
| F5642B01_BA55BKNO | BA55BKNO | booking_no | |
| F5642B01_BADEPU | BADEPU | date_earliest_pickup | |
| F5642B01_BADLDL | BADLDL | date_latest_delivery | |
| F5642B01_BA55DSTPT | BA55DSTPT | destination_port | joins F0101_1 on BA55DSTPT=ABAN8 |
| F5642B01_BA55NCON | BA55NCON | no_of_container | |
| F5642B01_BA55OCDLT | BA55OCDLT | ocean_del_terms | |
| F5642B01_BA55VLNO | BA55VLNO | vessel_name | |

### F4981 — Freight Audit History
| Hubble alias | JDE col | snake_case_field | Notes |
|---|---|---|---|
| F4981_FHCTY1 | FHCTY1 | city | freight; collides w/ F0116 |
| F4981_FHADDS | FHADDS | state | freight; collides w/ F0116 |
| F4981_FHADDZ | FHADDZ | zip_code_postal | |

### F4101 — Item Master
| Hubble alias | JDE col | snake_case_field | Notes |
|---|---|---|---|
| F4101_IMUWUM | IMUWUM | uom_weight | |

### F41002 — Item UOM Conversion
| Hubble alias | JDE col | snake_case_field | Notes |
|---|---|---|---|
| F41002_UMCONV | UMCONV | conversion_factor | ÷10,000,000; join UMRUM='TN' |

---

## Measures (5)
| Hubble alias | Aggregation | JDE col | snake_case_field | Notes |
|---|---|---|---|---|
| ReportColumn8 | SUM(F4211) | SDSOQS | units_quantity_shipped | quantity shipped (tons driver) |
| ReportColumn9 | SUM(F4211) | SDPQOR | units_primary_qty_order | price quantity |
| ReportColumn13 | SUM(F4211) | SDUORG | units_transaction_qty | original order qty |
| ReportColumn14 | SUM(F4074) | ALUPRC | amt_price_per_unit_02 | price-adjustment amount |
| ReportColumn15 | SUM(F4981 × ShiftFactor) | FHNAMT | net_amount | freight $ (billable/payable driver); `FHNAMT × NVL(ShiftFactor,0.01)` |

---

## Internal row-ID hashes (9 — NOT business fields, exclude from Gold)
`RowIDX_F4211_0/1/2_XRowID`, `RowIDX_F4981_0/1/2_XRowID`, `RowIDX_F4074_0/1/2_XRowID` —
Hubble `DBMS_UTILITY.GET_HASH_VALUE` row identifiers (plumbing for incremental refresh). Keys hashed:
- F4211: `SDKCOO+SDDOCO+SDDCTO+SDLNID`
- F4981: `FHUK01`
- F4074: (F4074 key)

---

## Query filters (WHERE) — the deployed Hubble query *(source reference only)*
> **v2.1: none of these are applied in the Gold build.** The fact carries all `is_delete=0` lines; each of these
> former filters is now either a denormalized **slicer field** on the fact (so Power BI applies it) or a relaxed LEFT
> join. Listed here only to document the original Hubble source query.
>
> **For the 107 Filter Capture variations' page-slicer settings** (the exact `fact_column = value` filters each
> report applies), see **`ESO1 Filter Capture/PAGE_FILTER_CHEATSHEET.md`** — the master's five filters below are
> just the baseline; the cheat-sheet enumerates every variation's slicers, the DAX-measure reports, and the F42119
> data-prerequisite.

1. `F4211.SDCO IN ('00640','00645')` — company → now the `company`/`company_key_order_no` slicer columns
2. `F4211.SDLNTY = 'S'` — stock line type → now the `line_type` slicer column
3. `NOT (F4211.SDLTTR IN ('980'))` — exclude cancelled → now the `status_code_last` slicer column
4. `F4074.ALAST IS NULL OR ALAST IN ('A03','FRTHIDE','FRTTAXN','FRTTAXY')` → now the `price_adjustment_type` slicer column
5. INNER ship-to gate: `F0101.ABAT1 BETWEEN 'A' AND 'P' OR BETWEEN 'R' AND 'ZZZ'` → join relaxed to LEFT; `search_type` slicer column

---

## Name-collision resolution (as implemented in the Gold fact)
Same `snake_case` from different tables — qualified with a role prefix in `fact_sales_order_freight` (✅ = built in `nb_eso1_gold_fact_sales_order_freight`):
| Collision | Sources | Gold landing |
|---|---|---|
| cost_center | F4211 SDMCU (×2) | ✅ `branch_plant` (used once; calc copy dropped) |
| name_alpha | F0101 (ship-to) / F0101_1 (dest point) | ship-to → `dim_address_ship_to.name_alpha` (dim); dest-point → ✅ fact `dest_point_name_alpha` |
| city | F0116 (ship-to) / F4981 (freight) | ship-to → ✅ fact `ship_to_city` (F0116, denormalized v2.9); freight → ✅ fact `freight_city` |
| state | F0116 (ship-to) / F4981 (freight) | ship-to → ✅ fact `ship_to_state` (F0116, denormalized v2.9); freight → ✅ fact `freight_state` |
| date_price_effective_date | F4211 (line) / F4201 (header) | ✅ `line_price_effective_date` / `header_price_effective_date` (+ `*_key`) |

---

## Join map (FROM clause)
| Table | Alias | Join | Keys |
|---|---|---|---|
| F4211 | driver | — | — |
| F0101 | ship-to | INNER | ABAN8 = SDSHAN (+ ABAT1 gate) |
| F0116 | ship-to addr | INNER | ALAN8 = ABAN8 |
| F4201 | header | INNER | SHKCOO/SHDOCO/SHDCTO = SDKCOO/SDDOCO/SDDCTO |
| F5642B11 | booking line | LEFT | AKSHPN/AKKCOO/AKDOCO/AKDCTO/AKLNID = SDSHPN/SDKCOO/SDDOCO/SDDCTO/SDLNID |
| F5642B01 | booking hdr | LEFT | BAKCOO/BADOCO/BADCTO/BASHPN = SDKCOO/SDDOCO/SDDCTO/SDSHPN |
| F0101_1 | dest point | INNER (to F5642B01) | ABAN8 = BA55DSTPT |
| F4981 | freight audit | LEFT | FHSHPN = SDSHPN |
| F4101 | item | LEFT | IMITM = SDITM |
| F41002 | UOM conv | LEFT | UMITM = SDITM, UMUM = SDUOM, UMRUM = 'TN' |
| F4074 | price adj | LEFT | ALDOCO/ALDCTO/ALKCOO/ALLNID = SDDOCO/SDDCTO/SDKCOO/SDLNID (+ ALAST gate) |

---

## Gold landing — `fact_sales_order_freight` final column names (as built)

Maps every query field to where it lands in Gold. Built by `nb/nb_eso1_gold_fact_sales_order_freight.py`
(`FACT_BUSINESS_COLS` + `build_fact`); `dim_item` is built by `nb/nb_eso1_gold_dim_item.py`
(`build_dim_item`).
**Grain guard:** F5642B11/F5642B01 are pre-collapsed to one row per join key (`b11d`/`b01d`,
`F.first` ignore-nulls) before denormalizing, so booking/ocean fields can't fan the line grain out.

### A. Fact columns from F4211 / F4201 (order-line grain)
| Query field (snake_case) | Source | Gold fact column |
|---|---|---|
| document_order_invoice_e | F4211 SDDOCO | `order_number` |
| order_type | F4211 SDDCTO | `order_type` |
| company_key_order_no *(SDKCOO)* / company *(SDCO)* | F4211 | `company_key_order_no` / `company` |
| line_number | F4211 SDLNID | `line_number` |
| shipment_number | F4211 SDSHPN | `shipment_number` |
| doc_voucher_invoice_e | F4211 SDDOC | `invoice_number` |
| user_reserved_number | F4211 SDURAB | `bol_number` |
| original_document_type / original_po_so_number / original_document_no | F4211 SDODCT/SDOORN/SDODOC | `original_document_type` / `original_po_so_number` / `original_document_no` |
| reference_01 | F4211 SDVR01 | `reference_01` |
| user_reserved_reference | F4211 SDURRF | `user_reserved_reference` |
| hold_orders_code | F4201 SHHOLD | `hold_orders_code` |
| status_code_last / status_code_next | F4211 SDLTTR/SDNXTR | `status_code_last` / `status_code_next` |
| — *(derived from status_code_next)* | calc | `next_status_num` — physical INT `CAST(TRIM(status_code_next) AS INT)` for Direct Lake range-filtering (M6); blank/non-numeric → NULL |
| freight_handling_code | F4211 SDFRTH | `freight_handling_code` (+ `freight_handling_code_audit`) |
| mode_of_transport | F4211 SDMOT | `mode_of_transport` *(degenerate code on the fact)* |
| container_id | F4211 SDCNID | `container_id` |
| transaction_originator | F4211 SDTORG | `transaction_originator` |
| delivery_instruct_line_01 / _02 | F4201 SHDEL1/SHDEL2 | `delivery_instruct_line_01` / `delivery_instruct_line_02` |
| gl_class | F4211 SDGLC | `gl_class` |
| sales_reporting_code_01 / _03 | F4211 SDSRP1/SDSRP3 | `sales_reporting_code_01` / `sales_reporting_code_03` |
| sales_reporting_code_02 / _04 | F4211 SDSRP2/SDSRP4 | `major_prod_code` / `minor_prod_code` |
| address_number_ship_to / address_number / carrier | F4211 SDSHAN/SDAN8/SDCARS | `ship_to` / `bill_to` / `carrier_number` (FKs → dim_address_book role views) |
| address_number_parent | F4211 SDPA8 | `address_number_parent` *(denormalized; no parent role view)* |
| cost_center | F4211 SDMCU | `branch_plant` (FK → dim_plant) |
| identifier_short_item | F4211 SDITM | `item_number_short` (FK → dim_item) |
| identifier_second_item | F4211 SDLITM | `second_item_number` |
| line_type | F4211 SDLNTY | `line_type` |
| uom_as_input / uom_primary / uom_pricing | F4211 SDUOM/—/SDUOM4 | `uom` / `uom_primary` / `uom_pricing` |
| conversion_factor | F41002 item factor (F41003 fallback via reused `dim_uom_conversion` dim, DAX `RELATED`) | `conversion_to_tons_rate` (NULL if unresolved) (+ `missing_conversion_flag`) |

### B. Fact date columns (sliced directly — no date dimension)
Dates are stored as **raw columns on the fact** and sliced directly in Power BI (date-range + relative-date slicers);
there is **no `dim_date`**. Each raw date still has a `*_key` (`yyyyMMdd`) int column, but it is **unused** (no dim to
join to) and hidden. Weekly grouping uses the derived fact column `ship_year_week` (see §F).
| Query field | Source | Fact raw date | Fact date key (unused) |
|---|---|---|---|
| date_transaction_julian | F4211 SDTRDJ | `order_date` | `order_date_key` |
| date_requested_julian | F4211 SDDRQJ | `requested_date` | `requested_date_key` |
| scheduled_pick_date | F4211 SDPDDJ | `scheduled_pick_date` | `scheduled_pick_date_key` |
| date_promised_ship_julian | F4211 SDPPDJ | `promised_ship_date` | `promised_ship_date_key` |
| actual_ship_date | F4211 SDADDJ | `actual_ship_date` | `ship_date_key` |
| dt_for_gl_and_vouch_01 | F4211 SDDGL | `gl_date` | `gl_date_key` |
| date_invoice_julian | F4211 SDIVD | `invoice_date` | `invoice_date_key` |
| cancel_date | F4211 SDCNDJ | `cancel_date` | `cancel_date_key` |
| date_price_effective_date (line) | F4211 SDPEFJ | `line_price_effective_date` | `line_price_effective_date_key` |
| date_price_effective_date (header) | F4201 SHPEFJ | `header_price_effective_date` | `header_price_effective_date_key` |
| date_earliest_pickup | F5642B01 BADEPU | `date_earliest_pickup` | `earliest_pickup_date_key` |
| date_latest_delivery | F5642B01 BADLDL | `date_latest_delivery` | `latest_delivery_date_key` |

### C. Fact measures / numerics (line grain — DAX SUM)
| Query field | Source | Gold fact column |
|---|---|---|
| units_quantity_shipped *(ReportColumn8)* | F4211 SDSOQS | `quantity_shipped` (+ calc `quantity_shipped_tons`) |
| units_primary_qty_order *(ReportColumn9)* | F4211 SDPQOR | `primary_quantity_ordered` |
| units_transaction_qty *(ReportColumn13)* | F4211 SDUORG | `transaction_quantity` |
| amt_price_per_unit_02 | F4211 SDUPRC | `price_per_unit` (+ calc `price_quantity_shipped`) |
| amt_price_per_unit_02 *(ReportColumn14)* | F4074 ALUPRC | `freight_factor_value` |
| net_amount *(ReportColumn15)* | F4981 FHNAMT | freight buckets (see D) |

### D. Denormalized booking / ocean / freight (shipment grain; DAX-deduped via `SUMX(VALUES(shipment_number),…)`)
| Query field | Source | Gold fact column |
|---|---|---|
| seal_no | F5642B11 AK55SELN | `seal_no` |
| booking_no | F5642B01 BA55BKNO | `booking_no` |
| destination_port | F5642B01 BA55DSTPT | `destination_port` |
| name_alpha (dest point) | F0101_1 ABALPH | `dest_point_name_alpha` |
| no_of_container / ocean_del_terms / vessel_name | F5642B01 BA55NCON/BA55OCDLT/BA55VLNO | `no_of_container` / `ocean_del_terms` / `vessel_name` |
| city / state / zip_code_postal (freight) | F4981 FHCTY1/FHADDS/FHADDZ | `freight_city` / `freight_state` / `freight_zip` |
| net_amount buckets | F4981 FHNAMT | `billable_freight`, `billable_fuel`, `total_billable`, `payable_freight`, `payable_fuel`, `total_payable`, `total_freight`, `freight_variance`, `total_variance`, `shift_factor_applied` |
| net_amount total (all charge codes) | F4981 FHNAMT | `total_freight` — `SUM(net_amount)` over ALL rows per shipment (H2; the combined-freight reports' raw total, which the billable/payable buckets under-count outside `{BFR,FSC,FSB,PFR}`) |
| — | F4941 RSRTN | `route_number` |
| — | calc | `is_primary_shipment_line` (freight-dedup anchor) |

### E. Lands in a DIMENSION (not the fact)
| Query field | Source | Dimension column |
|---|---|---|
| name_alpha (ship-to) | F0101 ABALPH | `dim_address_ship_to.name_alpha` — ship-to **name** stays dim-served (only *sold-to* name is on the fact, F.3) |
| uom_weight | F4101 IMUWUM | `dim_item.uom_weight` |

> **Model = hybrid, by design (see README §5 decision #3).** The REUSED, read-only `rpt.dim_address_book`
> role views serve the base ship-to / sold-to / carrier **name** (and base address) via the fact's
> `ship_to`/`bill_to`/`carrier_number` FKs. All **Filter Capture slicer/display attributes are denormalized
> onto the fact**, NOT added to the dims:
> - **v2.1** — ship-to `standard_industry_code`/`category_code_05`/`_14`/`search_type` (role `st`), see F.1.
> - **v2.9** — ship-to `address_rate` (`st`); F0116 ship-to `ship_to_city`/`_state`/`_zip`/`_address_1`/`_2`/
>   `_country` (`adr`); sold-to `sold_to_name`/`_search_type`/`_category_05`/`_10` (role `so`), see F.3.
>
> So ship-to postal address (city/state/zip/lines/country) now lives **on the fact** (F0116), not the dim.

### F.1 Filter (slicer) fields denormalized onto the fact (v2.1)
Added so Power BI can slice the fact directly (all former Hubble WHERE filters were removed; filtering moved here).
| Fact column | Source | JDE | Notes |
|---|---|---|---|
| `price_adjustment_type` | F4074 | ALAST | **actual** value, one row/line (`row_number` pick — no aggregation / no comma-list) |
| `freight_factor_value` | F4074 | ALUPRC | actual value from the same one-row/line pick (was: aggregated) |
| `standard_industry_code` | F0101 (ship-to) | ABSIC | LEFT join `st` on `address_number_ship_to` |
| `category_code_05` | F0101 (ship-to) | ABAC05 | `report_code_add_book_005` |
| `category_code_14` | F0101 (ship-to) | ABAC14 | `report_code_add_book_014` |
| `search_type` | F0101 (ship-to) | ABAT1 | `address_type_01` |
| `uom_structure` | F41002 | UMUSTR | item + input-UoM lookup |
| `payment_terms` | F4211 | SDPTC | `payment_terms_code_01` |
| `item_segment_04` | F4101 | IMSEG4 | `segment_04` |

### F.2 Removed from ALL built tables (v2.2)
The CDC/audit columns `is_deleted`, `source_commit_timestamp`, `gold_updated_timestamp`, `record_hash` are **not stored on
any built table** (fact **or** `dim_item` — now built by two separate notebooks:
`nb_eso1_gold_fact_sales_order_freight` builds the fact, `nb_eso1_gold_dim_item` builds `dim_item`). Each notebook is a
**batch full-snapshot overwrite** (reads the full Silver snapshot → `build_*()` once → `DROP TABLE IF EXISTS` + plain
`mode("overwrite")` write), so there is no CDC delete+append, no MERGE upsert/delete, and no `_change_type`/`init_ver`
logic. No `record_hash`, no soft-delete flag. (`order_scope_key` remains on the schema but is now **vestigial** — it was
the old CDC delete scope, kept only so the schema/semantic model are unchanged.)

### F.3 Filter Capture additions (v2.9, 2026-07-15)
Added so the ~110 **ESO1 Filter Capture** query variations can be served from the one fact at **Power BI page level**
(Gold applies no report filters — see `ESO1 Filter Capture/README.md`). 21 columns + a sold-to F0101 role join + the
F0116 ship-to postal address + the F42119 history union. Silver names verified in `full_metadata.json` (all 25 ESO1 tables).

| Fact column | Source | JDE | snake_case (Silver) | Notes |
|---|---|---|---|---|
| `extended_price` | F4211 | SDAEXP | `amount_extended_price` | ⭐ the sales-amount measure (`SUM` in ~60 variations); was missing |
| `extended_cost` | F4211 | SDECST | `amount_extended_cost` | |
| `currency_code` | F4211 | SDBCRC | `currency_code_base` | line domestic currency (**no F0010 source** — F0010/CCCRCD not in Silver) |
| `backorder_qty` | F4211 | SDSOBK | `units_quan_backor_held` | |
| `cancelled_qty` | F4211 | SDSOCN | `units_quantity_canceled` | |
| `qty_to_date` | F4211 | SDQTYT | `quantity_shipped_to_date` | |
| `open_qty` | F4211 | SDUOPN | `units_open_quantity` | |
| `line_description_1` | F4211 | SDDSC1 | `description_line_01` | the **line's** own text (≠ `item_name` from F4101) |
| `line_description_2` | F4211 | SDDSC2 | `description_line_02` | |
| `date_updated` | F4211 | SDUPMJ | `date_updated` | |
| `address_rate` | F0101 (ship-to `st`) | ABURAT | `user_reserved_amount` | sole measure of the customer-rate variations (ADM/Chevron/Ingredion/…) |
| `sold_to_name` | F0101 (**sold-to** `so`) | ABALPH | `name_alpha` | new LEFT join `so` on `address_number = SDAN8` |
| `sold_to_search_type` | F0101 (sold-to `so`) | ABAT1 | `address_type_01` | Days-Since-Invoice filters the **sold-to** search-type |
| `sold_to_category_05` | F0101 (sold-to `so`) | ABAC05 | `report_code_add_book_005` | |
| `sold_to_category_10` | F0101 (sold-to `so`) | ABAC10 | `report_code_add_book_010` | |
| `ship_to_city` | F0116 (`adr`) | ALCTY1 | `city` | latest-effective per address (`date_beginning_effective` desc) |
| `ship_to_state` | F0116 (`adr`) | ALADDS | `state` | ≠ F4981 `freight_state` |
| `ship_to_zip` | F0116 (`adr`) | ALADDZ | `zip_code_postal` | |
| `ship_to_address_1` | F0116 (`adr`) | ALADD1 | `address_line_01` | |
| `ship_to_address_2` | F0116 (`adr`) | ALADD2 | `address_line_02` | |
| `ship_to_country` | F0116 (`adr`) | ALCTR | `country` | |

**Row population:** `f4211_sales_order_detail_file` **UNION ALL `f42119_sales_order_history_file`** (Sales Order History)
for the open-order variations' closed/purged lines. F42119's Silver name is **CONFIRMED in `full_metadata.json`**
(`table_name = sales_order_history_file`, identical 268-col schema to F4211) → still guarded by `_load_optional`:
unioned only if the table exists in Silver, else fact = F4211
only. **Confirm the exact name.**

**Already present — NOT re-added:** `gl_class` (SDGLC), `delivery_instruct_line_01/02` (SHDEL1/2). `user_reserved_amount`
(SDURAB=`user_reserved_number`) is already surfaced as **`bol_number`**. SDUORG/SDPQOR/SDSOQS =
`transaction_quantity`/`primary_quantity_ordered`/`quantity_shipped`.

### F.4 Residual-gap columns (v2.12, 2026-07-22) — the 20 that were deferred, now IMPLEMENTED
After re-reading `full_metadata.json` (26 tables — F5549002 added last), every source was confirmed in Silver, so all
structural gaps became notebook-only column adds. Fact now **145 business + 2 keys**.

| Fact column | Source | JDE | snake_case (Silver) | Notes |
|---|---|---|---|---|
| `has_effective_price` | F4106 (`item_base_price_file`) | derived | — | M1 — 'Y' iff a non-zero base price EXISTS for (2nd-item, plant, ship-to) whose effective window covers actual_ship_date (Zero-Price Branch-B `NOT EXISTS`, inverted; `left_semi`, no fan-out) |
| `zone_number` | F4211 | SDZON | `zone_number` | M2 — Orders on Hold for Pricing |
| `line_hold` | F4211 | SDHOLD | `hold_orders_code` | M3 — the LINE hold (≠ the header `hold_orders_code` from F4201 SHHOLD); Past 31 Days |
| `is_ocean_route` | F4941 | RSMOT | `mode_of_transport` | M4 — 'Y' if any routing step is 'OCE' (04a/AP Minerals/Luhe) |
| `route_container_count` | F4941 | RSNCTR | `number_of_containers` | M4 — SUM per shipment (04a's SUM(RSNCTR)) |
| `gross_weight`/`catch_weight`/`max_weight` | F5549002 (`mxp_bol_interface_detail`) | MIGRWT/MICTWT/MIMXWT | `gross_weight`/`catch_weight`/`maximum_weight` | M5 — weigh-ticket weights, one row/line; Silver pre-decoded (no /10000, /100). Gross/Catch also DAX SUM measures |
| `pull_signal` | F4211 | SDPSIG | `pull_signal` | 7 load reports |
| `reference_02` | F4211 | SDVR02 | `reference_02_vendor` | |
| `reference_03` | F4211 | SDVR03 | `reference_ucis_no` | IFS order no |
| `vendor_number` | F4211 | SDVEND | `primary_last_vendor_no` | SBX Unbilled AR |
| `price_adjustment_schedule` | F4211 | SDASN | `price_adjustment_schedule_n` | |
| `user_reserved_code` | F4211 | SDURCD | `user_reserved_code` | |
| `price_override_code` | F4211 | SDPROV | `price_override_code` | |
| `user_id` | F4211 | SDUSER | `user_id` | Llamasoft / SOP000x |
| `lot_number`/`serial_number`/`location` | F4211 | SDLOTN/SDSERN/SDLOCN | `lot`/`serial_number_lot`/`location` | SOP0020-with-lot, SOP000x-620, Shipped-w/o-Confirmation |
| `sales_reporting_code_05` | F4211 | SDSRP5 | `sales_reporting_code_05` | |
| `sold_to_lob_category_05` | F03012 (`customer_master_by_line_of_business`) | AIAC05 | `report_code_add_book_005` | joined on the sold-to (SDAN8=AIAN8), collapsed one-row-per-address; Mak's E26 filter |
| `deferred_entries_flag` | F49211 (`sales_order_detail_file_tag_file`) | UDDEFF | `deferred_entries_flag` | LEFT on the 4 line keys (1:1 tag file); SOP0006 / SOP000x |

**Still open:** only **H1 ShiftFactor** (a data-reconciliation — F0010 present, not a source gap). Every source the 107 variations reference is now on the fact. Commission (F42005) is served by the separate `fact_sales_commission`.

### F. Date handling (no date dimension)
> **There is NO `dim_date`.** Dates are the fact's **raw date columns** (§B) sliced **directly** in Power BI —
> date-range slicers plus relative-date slicers for today/yesterday/MTD/YTD. No marked date table, no DAX
> time-intelligence. Weekly cadence uses the fact string column **`ship_year_week`** (Mon–Sun ISO label, e.g.
> `2026-W25`, derived off `actual_ship_date`) as the "by week" axis. Sentinel/junk JDE dates (e.g. `1952-12-31`,
> `2824-08-29`) are **nulled** by a `clean_date()` guard (valid window `2000-01-01 … Dec 31 of current year + 25`, self-extending) so they don't
> appear in raw-date slicers. The `*_date_key` ints still exist on the fact but are **unused and hidden**.

### G. Excluded from Gold
The 9 `RowIDX_*_XRowID` Hubble internal row-ID hashes (incremental-refresh plumbing).
