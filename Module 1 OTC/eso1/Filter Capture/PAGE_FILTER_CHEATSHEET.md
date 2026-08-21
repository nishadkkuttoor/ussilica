# ESO1 — Power BI Page-Filter Configuration Cheat-Sheet

**For report builders.** Each legacy Hubble/JDE report becomes ONE Power BI page (or a shared page + slicer)
over the single unfiltered Gold fact `lh_jde_gold.eso1.fact_sales_order_freight`. This sheet gives the **exact
slicer settings** for all 107 variations. Values were extracted directly from the SQL in this folder
(2026-07-16), not from a summary — where the older README catalog and the SQL disagreed, the SQL wins.

> **Ctrl-F the report name** to jump to its row. Grouped by family; families that share a template are called
> out in **§B (reusable pages)** so you can build one page for many reports.

---

## A. Universal rules — read once, apply to every page

1. **Ship-to search-type screen is already applied.** Every legacy report has
   `F0101.ABAT1 BETWEEN 'A '..'P ' OR 'R '..'ZZZ'` (the "real address" screen). It is baked into the fact's
   ship-to join — **do NOT add it as a page filter.** It never appears in the tables below.
2. **F42119 history is already in the fact — *if the source prerequisite is met*.** Reports marked **⟲hist**
   union `proddta.F42119` (closed/purged lines). The fact unions F42119 automatically, so those rows are
   present — **no page action needed.** (One report, **Sto Corp**, reads *only* history — it still works
   because the fact carries both.)
   > **⚠ Data prerequisite (platform, not report-builder):** the F42119 union is **guarded** (`tableExists`).
   > It only contributes rows when **`f42119_sales_order_history_file` is ingested to Silver** — read as a
   > static batch snapshot, no CDF required (F42119 is the fact's optional 3rd union source, alongside F4211 and
   > F4981). Until then the fact
   > runs **F4211-only** and every ⟲hist page silently loses its closed/purged (recently-completed) lines —
   > the page filters are still correct, but the row population is incomplete. If a ⟲hist report looks like
   > it's missing recently-closed orders, this is why: confirm `f42119_sales_order_history_file` is present in
   > Silver (the fact build should report it unioned), not the skip message
   > `(F42119 not in Silver — history skipped)`.
3. **Freight-code (F4074 `ALAST`) whitelists are measure-side, not page filters.** Reports marked **⟐frt**
   carry an `ALAST IN ('A03','FRTHIDE','PP06'…)` list on the F4074 join. That governs which freight-adjustment
   rows aggregate into the billable/payable-freight measures — it is handled in the DAX measure, **not** a page
   slicer. Do not build a slicer for it.
4. **Dates → relative-date slicers on a date table.** The legacy Julian math is translated here to intent
   (`today`, `yesterday`, `MTD`, `YTD`, `current fiscal period`, `requested_date < today`, `last 90 days`).
   Build a Power BI date table related to the fact's date columns and use relative-date slicers. **Fixed Julian
   floors** (e.g. `requested_date ≥ 2016`, `ship ≥ 2020`) become a fixed "from date X onward" range slicer.
5. **Fiscal-period reports** (SCP, CL National, Halliburton, Ovintiv MTD, Liberty, monthly invoice reports)
   derive their window from company constants F0010 (`CCPNC`/`CCARFJ`/`CCDFF`). Reproduce with a **fiscal
   calendar** date table + a "current period" / "prior period" relative slicer — not literal dates.
6. **`branch_plant` values are space-padded** in JDE (`N'         561'`). The fact stores them trimmed — filter
   on the trimmed value shown here (e.g. `561`, `061`, `081`).

**Legend:** ⟲hist = F42119 history union (auto) · ⟐frt = F4074 freight-code measure filter (auto) ·
⚑DAX = needs a DAX measure, not a slicer · ⚠gap = uses an attribute not yet on the fact (see §D).

---

## B. Reusable pages (build once, drive many with a slicer)

| Template page | Fixed slicers | Vary by | Covers |
|---|---|---|---|
| **Customer rate / status-529** | `status_code_next` = 529 · `status_code_last` BETWEEN 520 AND 528 · show **Address Rate** measure | `ship_to` slicer | ADM Cedar Rapids/Clinton/Deerfield/Enderlin/Lloydminster/Windsor, Chevron, Ingredion, Grain Processing (**9**) |
| **Branch-plant open orders** | `company` = 00400 · `status_code_next` slicer | `branch_plant` slicer | Dubberly, Florisil, Hurtsboro, Jackson, Berkeley Springs, Columbia, Mapleton, Mauricetown, Millen, Montpelier, Regional Mines, Rockwood (**12**) |
| **Parent / ship-to open orders** | `status_code_next` BETWEEN 525 AND 573 · `line_type` = S · `status_code_last` <> 980 | `address_number_parent` or `ship_to` slicer | Napa, L&M Environmental, Leslie's Poolsmart, Safety Kleen, Grainger, Amalgamated Sugar, Colortech, Solvay, Ampacet, CCC Plastics, Polyfil, Standridge, Sto Corp, Tri-Iso (**14**) |
| **WTX loaded-tons** | `order_type` = SO · `standard_industry_code` = F · `second_item_number` <> 'MISC BILLING' · ship-date relative | `address_number_parent` + `branch_plant` slicers + date grain (today/yesterday/MTD/YTD) | Pioneer, Cudd, 21.DE Central, DE Central Previous-Day, Gore, Continental-MC, Ovintiv MTD, Ovintiv YTD, Liberty, Halliburton (**10**) |
| **Ottawa 501 product-class** | `branch_plant` = 501 · `status_code_next` < 561 · `uom` <> EA · `company` IN (00400,00390,00330) · `second_item_number` IN (145-item whitelist) | `sales_reporting_code_03` (BLK/PKG) + `mode_of_transport` (RCP / not RCP / any) slicers | Ottowa Ground-Packaged, Ground-Bulk, Whole-Grain Rail-Bulk, Whole-Grain Truck-Bulk, Whole-Grain Truck-Packaged (**5**) |
| **SOP next-status inquiry** | `order_type` <> SX · `status_code_last` < 980 | `status_code_next` slicer (577 / 580 / 620) | SOP000x-577, SOP000x-580, SOP000x-620 (**3**) |

---

## C. Master lookup — all 107 variations

Slicer values are **exact**. `next` = `status_code_next`, `last` = `status_code_last`, `co` = `company`,
`plant` = `branch_plant`, `SIC` = `standard_industry_code` (ship-to), `parent` = `address_number_parent`,
`item2` = `second_item_number`. Operators are literal (`=`, `<>`, `<`, `IN`, `BETWEEN`, `LIKE`, `NOT LIKE`).

### Family 1 — Customer rate / status-529 lookups
*Measure = `address_rate` (ABURAT). See template B.*

| Report | Page-level slicers | Date | Flags |
|---|---|---|---|
| ADM Cedar Rapids | `ship_to`=20022727 · `next`=529 · `last` BETWEEN 520 AND 528 | — | |
| ADM Clinton | `ship_to`=20010840 · `next`=529 · `last` BETWEEN 520 AND 528 | — | |
| ADM Deerfield | `ship_to`=20010852 · `next`=529 · `last` BETWEEN 520 AND 528 | — | |
| ADM Enderlin | `ship_to`=20010848 · `next`=529 · `last` BETWEEN 520 AND 528 | — | |
| ADM Lloydminster | `ship_to`=10117212 · `next`=529 · `last` BETWEEN 520 AND 528 | — | |
| ADM Windsor | `ship_to`=20010836 · `next`=529 · `last` BETWEEN 520 AND 528 | — | |
| Chevron | `ship_to`=20011635 · `next`=529 · `last` BETWEEN 520 AND 528 | — | |
| Ingredion | `ship_to`=20011316 · `next`=529 · `last` BETWEEN 520 AND 528 | — | |
| Grain Processing | `ship_to`=20012646 · `next`=529 · `last` BETWEEN 520 AND 528 | — | |

### Family 2 — Open orders by branch/plant (company 00400)
| Report | Page-level slicers | Date | Flags |
|---|---|---|---|
| 23.Dubberly Open Orders | `plant`=561 · `next`=560 · `co`=00400 | — | ⟲hist |
| 24.Florisil Open Orders | `plant`=351 · `next`<561 · `co`=00400 | — | ⟲hist |
| 25.Hurtsboro Open Orders | `plant`=581 · `next`=560 · `co`=00400 | — | ⟲hist |
| 40 Jackson Open Orders | `order_type`=SO · `plant`=071 · `next`=560 · `co`=00400 | — | ⟲hist |
| Berkeley Springs Open Orders | `plant`=151 · `next`=560 · `co`=00400 | — | ⟲hist |
| Columbia Open Orders | `plant`=171 · `next` IN (530,560) · `co`=00400 | — | ⟲hist |
| Columbia Open Bagged Orders | `plant`=171 · `next` IN (530,560) · `uom`=BG · `co`=00400 | — | ⟲hist |
| Mapleton Open Orders | `plant`=131 · `next`=560 · `co`=00400 | — | ⟲hist |
| Mauricetown Open Orders | `plant`=261 · `next` IN (530,560) · `co`=00400 | — | ⟲hist |
| Millen-Open Order Report | `order_type` IN (SO,SG) · `plant`=551 · `next` IN (530,560) · `co`=00400 | — | ⟲hist |
| Montpelier Open Orders | `plant`=521 · `next`=560 · `co`=00400 | — | ⟲hist |
| Regional MInes - Load Report | `plant`=511 · `next`=560 · `co`=00400 | — | ⟲hist |
| Rockwood Open Orders | `plant`=511 · `next`=560 · `co`=00400 *(identical filters to Regional Mines)* | — | ⟲hist |
| Ottawa Open Orders | `plant`=501 · `next` IN (530,560) · `line_type` IN (S,W) · `uom_primary`<>EA · `co`=00400 | — | ⟲hist |

### Family 3 — Open orders by parent / ship-to (status window 525–573)
| Report | Page-level slicers | Date | Flags |
|---|---|---|---|
| Colortech | `parent`=10116596 · `next` BETWEEN 525 AND 573 | — | ⟲hist |
| Solvay Open Order Report | `ship_to`=20015341 · `next` BETWEEN 525 AND 573 | — | ⟲hist |
| Napa | `parent`=20022384 · `line_type`=S · `next` BETWEEN 525 AND 573 · `last`<>980 | — | |
| Tri-Iso | `parent`=20022632 · `line_type`=S · `next` BETWEEN 525 AND 573 · `last`<>980 | — | ⚑DAX |
| Safety Kleen | `parent`=20022519 · `line_type`=S · `next` BETWEEN 525 AND 573 · `last`<>980 | — | |
| Amalgamated Sugar Company | `ship_to`=10125974 · `line_type`=S · `next` BETWEEN 525 AND 573 · `last`<>980 | — | |
| Grainger | `parent`=20022190 · `line_type`=S · `next` BETWEEN 525 AND 573 · `last`<>980 | — | |
| L&M Environmental | `parent`=20022305 · `line_type`=S · `next` BETWEEN 525 AND 573 · `last`<>980 | — | |
| Leslie's Poolsmart | `parent`=20022322 · `line_type`=S · `next` BETWEEN 525 AND 573 · `last`<>980 | — | |
| Standridge Open Order Report | `parent`=20022569 · `next` BETWEEN 525 AND 573 | — | ⚑DAX |
| Sto Corp | `parent`=20022576 · `next` BETWEEN 525 AND 573 | — | ⟲hist (history-only source) |
| Ampacet | `ship_to`=20015949 · `next` BETWEEN 525 AND 573 | — | ⟲hist |
| CCC Plastics | `ship_to`=10162144 · `bill_to`=10162143 · `next` BETWEEN 525 AND 573 | — | ⟲hist |
| Polyfil | `ship_to`=20015955 · `next` BETWEEN 525 AND 573 | — | ⟲hist |
| Cargill | `ship_to`=20011381 · `next`<575 · `line_type`=S | — | ⟲hist |

### Family 4 — Open orders by company / region / product class
| Report | Page-level slicers | Date | Flags |
|---|---|---|---|
| Open Order Report | `next`=560 · `co` IN (00400,00390,00330) | `requested_date` ≥ 2016 (fixed floor) | ⟲hist |
| Open Order Report - LF2022 | `order_number`=1552979 · `order_type`=SO · `next`=560 · `co`=00400 · `SIC`=F | `requested_date` ≥ 2016 | ⟲hist |
| 36 ISPCAMSP-Open Order Report | `order_type`=SO · `next` IN (530,560) · `category_code_14`=SAR | — | ⟲hist |
| SM-Open Order Report | `order_type`=SO · `next` IN (530,560) · `co`=00400 · `SIC` NOT IN (F) | — | ⟲hist |
| SM Inside Sales-Open Order Report | `next` IN (530,560) · `category_code_05` IN (S15,S13,S36,S21,S26,S30,S33,S45,S46,E18,E19,S03,S05,S08) | — | ⟲hist |
| SM Planning Open Orders | `next`<561 · `co` IN (00400,00390,00330) · `SIC` NOT IN (F) | `requested_date` ≥ 2016 | ⟲hist |
| SM Past Due Orders | `order_type`=SO · `next` IN (530,560) · `co`=00400 · `SIC` NOT LIKE 'F%' | `requested_date` < today | ⟲hist |
| SM Trucking-Past Due Orders | `order_type`=SO · `next` IN (530,560) · `mode_of_transport` NOT LIKE 'R%' · `freight_handling_code`=PP · `co`=00400 · `SIC` NOT LIKE 'F%' | `requested_date` < today | ⟲hist |
| Cash In Advance-Open Order Report | `next` IN (530,560) · `payment_terms` IN (CC,CTD) | — | ⟲hist |
| Daily Open Orders Report | `order_type`=SO · `next`=560 · `co`=00400 · `SIC` IN (F,FA,FB) | — | ⟲hist |
| Daily Open Orders Report NPO Aging | `order_type`=SO · `last`<980 · `next`=560 · `item2` NOT LIKE 'MISC%' AND NOT LIKE 'TR%' · `line_type` NOT IN (F,FT) · `reference_01`=NPO · `SIC` IN (F,FA,FB) | — | ⟲hist |
| Supply Chain Planning Llamasoft Open Orders | `order_type`=SO · `next`=560 · `SIC` IN (F,FA,FB,ISPF) | — | |
| Ottawa - All Open Orders | `plant`=501 · `next`<561 · `uom`<>EA · `co` IN (00400,00390,00330) | `requested_date` ≥ 2016 | ⟲hist |
| Ottawa Open Rail Orders | `plant`=501 · `next`=560 · `co`=00400 · `mode_of_transport` IN (RC1,RCP,RCS,RCX,RSP,RU1,RU2,RU7,RU8,RUT,WNF,RCC,UTP,RU3,UTR,UTS) · `item2` NOT IN (50065B00000,50064B00000,50063B00000) | `requested_date` ≥ 2016 | ⟲hist |
| Ottowa - ASTM - Packaged | `plant`=501 · `next`<561 · `uom`<>EA · `co` IN (00400,00390,00330) · `item2` IN (50081P18150,50087P19150,50084P20150) | `requested_date` ≥ 2016 | ⟲hist |
| Ottowa - Ground - Packaged | `plant`=501 · `next`<561 · `sales_reporting_code_03`=PKG · `uom`<>EA · `co` IN (00400,00390,00330) · `item2` IN (145-item whitelist ⁋) | `requested_date` ≥ 2016 | ⟲hist |
| Ottowa - Ground- Bulk | `plant`=501 · `next`<561 · `sales_reporting_code_03`=BLK · `uom`<>EA · `co` IN (00400,00390,00330) · `item2` IN (145-item whitelist ⁋) | `requested_date` ≥ 2016 | ⟲hist |
| Ottowa - Whole Grain Rail - Bulk | `plant`=501 · `next`<561 · `sales_reporting_code_03`=BLK · `uom`<>EA · `mode_of_transport`=RCP · `co` IN (00400,00390,00330) · `item2` IN (145-item whitelist ⁋) | `requested_date` ≥ 2016 | ⟲hist |
| Ottowa - Whole Grain Truck - Bulk | `plant`=501 · `next`<561 · `sales_reporting_code_03`=BLK · `uom`<>EA · `mode_of_transport`<>RCP · `co` IN (00400,00390,00330) · `item2` IN (145-item whitelist ⁋) | `requested_date` ≥ 2016 | ⟲hist |
| Ottowa - Whole Grain Truck - Packaged | `plant`=501 · `next`<561 · `sales_reporting_code_03`=PKG · `uom`<>EA · `co` IN (00400,00390,00330) · `item2` IN (145-item whitelist ⁋) | `requested_date` ≥ 2016 | ⟲hist |
| Pacific Open Orders-Ground Products | `plant` IN (061,062) · `next`<999 · `minor_prod_code`>110 · `co`=00400 | — | ⟲hist |
| Pacific Open Orders-Rail | `plant`=061 · `next`<999 · `mode_of_transport` LIKE 'R%' · `co`=00400 | — | ⟲hist |
| Pacific Open Orders-Whole Grain | `plant`=061 · `next`<570 · `minor_prod_code`<111 · `co`=00400 · `uom`=TN | — | ⟲hist |

⁋ *The 145-item `item2` whitelist is byte-identical across the 5 Ground/Whole-Grain files (e.g. 06069B00000,
15115P30170, 97064B00000). Build it once as a shared slicer selection / a lookup group.*

### Family 5 — Load / loaded-tons reports
| Report | Page-level slicers | Date | Flags |
|---|---|---|---|
| Pioneer WTX - Loaded Tons Update | `order_type`=SO · `plant` IN (321,341,161,181) · `ship_to` IN (10112039,10115878) · `parent`=10112037 · `item2`<>'MISC BILLING' · `SIC`=F | `actual_ship_date` = today | ⟲hist |
| Cudd WTX - Loaded Tons Update | `order_type`=SO · `plant` IN (321,341,9705,9786,161,181) · `parent`=10058491 · `item2`<>'MISC BILLING' · `SIC`=F | `actual_ship_date` = today | ⟲hist |
| 21.DE Central Operating WTX - Loaded Tons Update | `order_type`=SO · `plant` IN (321,341,161,181) · `parent`=10152005 · `item2`<>'MISC BILLING' · `SIC`=F | `actual_ship_date` = today | ⟲hist |
| DE Central Operating - Previous Day Load Report | `order_type`=SO · `plant` IN (321,341,161,181) · `parent`=10152005 | `actual_ship_date` = yesterday | ⟲hist |
| Gore Daily Load Report | `order_type`=SO · `parent`=10086687 · `item2`<>'MISC BILLING' | `actual_ship_date` = today | ⟲hist |
| Continental Load Report - MC | `order_type`=SO · `plant`=081 · `parent`=10112501 · `item2`<>'MISC BILLING' | `actual_ship_date` = yesterday | ⟲hist |
| Ovintiv WTX Load Report - MTD ALL | `order_type`=SO · `co`=00400 · `plant` IN (321,341,181,161) · `parent`=10061291 | `actual_ship_date` = current fiscal month (MTD) | |
| Ovintiv Year to Date Load Report | `parent`=10061291 · `item2`<>'MISC BILLING' | `actual_ship_date` ≥ year-start (YTD) | ⟲hist |
| 41 Liberty Oilfield Load Report - Scheduler | `order_type`=SO · `parent`=10065562 · `item2`<>'MISC BILLING' | `actual_ship_date` = current fiscal period | ⟲hist |
| Halliburton Load Report | `order_type`=SO · `co`=00400 · `parent`=10043240 | `actual_ship_date` = current fiscal month (MTD) | |

### Family 6 — Invoiced / monthly / finance (next = 999 + period)
| Report | Page-level slicers | Date | Flags |
|---|---|---|---|
| SCP Monthly Invoice Report - Scheduler | `parent`=20022530 · `next`=999 · `last`<>980 · `co` IN (00640,00645) | `invoice_date` = prior fiscal month | |
| CL National Accounts Monthly Invoice Report - Scheduler | `parent` IN (20021899,20021943,20022190,20022384,20022427,20022496,20022631,20021987) · `next`=999 · `last`<>980 | `invoice_date` = prior month | |
| Leslie's Poolmart Monthly Invoice Report - Scheduler | `parent`=20022322 · `next`=999 · `last`<>980 · `item2`=CPOOL24LV35LPGBX · `co` IN (00640,00645) | `invoice_date` in year (2025) | |
| Baseline Report | `order_type` IN (SO,CO) | `actual_ship_date` = single run-date | ⟲hist |
| Baseline Report (Finance) | `SIC` IN (~140-code whitelist) | `actual_ship_date` in year (2024) | ⟐frt |
| DE Orders | `line_type`=S · `last`<>980 · `co` IN (00640,00645) | — | ⟐frt |
| SOP0025 Monthly Sales Report - Detail | `plant`=061 · `order_type` IN (SO,CO) · `last`<980 · `next`=999 · `line_type` NOT IN (F,FT) | `gl_date` = month | ⟐frt |
| SOP0007 Invoiced Orders | `order_type` NOT IN (ST,SG) · `last`<>980 · `next`=999 · `gl_class` NOT IN (DZ01) · `line_type` NOT IN (TL) | `gl_date` = month | ⟐frt |
| SOP0008 Pioneer Natural Resources Sales | `plant`=341 · `order_type` IN (SO,CO) · `last`<>980 · `next`=999 · `parent`=10059472 | `gl_date` = month | ⟐frt |
| BP Freight and Fuel (Combined) January 2026 | `ship_to`=20022745 · `freight_handling_code` IN (DLV,PP) · `line_type`=S · `next`=999 · `SIC`<>F | `gl_date` in month (Jan 2026) | ⟐frt |
| Sherwin Williams MTD Shipments | `order_type`=ST · `ship_to` IN (9001–9009,9012–9028,9040,9041 — 29 SW plants) · `next`>560 | `actual_ship_date` ≥ MTD | ⟲hist |
| Days Since Invoice | `co`=00750 · `sold_to_search_type`=CB | — | ⚑DAX |
| REI DEMURRAGE MONTHLY BILLINGS | `gl_class` LIKE '%AE%' OR '%AJ%' · `item2` LIKE 'MISC%' · `next`=999 · `SIC`<>F | `actual_ship_date` ≥ 2020 (fixed floor) | ⟲hist |
| SBX Unbilled AR | `order_type`=SX · `next`<620 · `co`=00750 | — | ⟲hist |

### Family 7 — Export / ocean-booking
| Report | Page-level slicers | Date | Flags |
|---|---|---|---|
| 04a Export Open Orders | `order_type`=SE · `co` IN (00640,00645) · `line_type`=S · `plant`=651 · `next` NOT IN (980,999) | — | ⚠gap (ocean mode) |
| AP Minerals | `parent`=20022844 · `line_type`=S · `next`<>999 · `last`<>980 | — | ⚠gap (ocean mode) |
| Mak Export Orders | `order_number` IN (~71 explicit orders) · `line_type`=S · `last`<>980 | — | ⚠gap (sold-to cat E26 — redundant w/ order list) |
| Luhe | `parent`=10112541 · `line_type`=S · `next`<>999 · `last`<>980 | — | ⚠gap (ocean mode) |
| Profiltra | `ship_to` IN (10113353,10162229) · `line_type`=S · `next`<>999 · `last`<>980 | — | |
| Thai Tan | `ship_to` IN (10114438,10114633,10114634) · `line_type`=S · `next`<>999 · `last`<>980 | — | ⚑DAX |

### Family 8 — SOP status-window inquiries
| Report | Page-level slicers | Date | Flags |
|---|---|---|---|
| SOP0020 Sales Order Inquiry | `co`=00400 | — | |
| SOP0020 Sales Order Inquiry with lot | `co`=00400 *(+ show lot/serial/location)* | — | |
| SOP0020Lite | `order_type`=ST | `actual_ship_date` = single day (run-time param) | |
| SOP0020Lite - 90 Day Transload Sales | `order_type` IN (SO,CO) · `branch_plant` LIKE '%97%' OR '%98%' · `SIC` IN (F,FA,FB) | `actual_ship_date` ≥ today − 90 | ⚠gap (plant LIKE) |
| SOP0006 Shipped NOT Invoiced Order Inquiry | `order_type` IN (SO,CO) · `last`<980 · `next` BETWEEN 574 AND 620 | `order_date` ≥ 2015 (fixed floor) | ⟐frt |
| SOP000x - Sales Orders at Next Status 577 | `order_type`<>SX · `last`<980 · `next`=577 | — | ⟐frt |
| SOP000x - Sales Orders at Next Status 580 - SO CO & ST | `order_type`<>SX · `last`<980 · `next`=580 | `order_date` ≥ 2015 (fixed floor) | ⟐frt |
| SOP000x - Sales Orders at Next Status 620 | `order_type`<>SX · `last`<980 · `next`=620 | — | ⟐frt |

### Family 9 — Exceptions & scheduler
| Report | Page-level slicers | Date | Flags |
|---|---|---|---|
| Orders with Zero Unit Price - Scheduler | `line_type`=S · `co` IN (00640,00645) · `order_type` IN (S1,SE,SZ) · `last`<>980 · **Branch A:** `price_per_unit`=0 AND `next`<620 · **Branch B:** `price_per_unit`<>0 AND has-shipped AND no effective base price | — | ⚠gap (Branch B needs `has_effective_price` flag) |
| Orders on Hold for Pricing - Scheduler | `co` IN (00640,00645) · `order_type` IN (S1,SE,SZ) · `line_type`=S · `last`<>980 · (`hold_orders_code` IN (ZP,WP) OR `zone_number`=Y) | — | ⚠gap (`zone_number`/SDZON not on fact) |
| Shipped without Shipment Confirmation - Scheduler | `co` IN (00640,00645) · `last`<>980 · `line_type`=S · `container_id` assigned (<>' ' and <>'.') · `next`<571 · `order_type` IN (S1,SE,SZ) · has actual ship date | — | |
| Short Ship Notifications - Scheduler | `plant` IN (651,661) · `line_type`=S · `order_type`=SE · `next`=999 · `last`=980 · `co` IN (00640,00645) | `cancel_date` = yesterday | |
| Past 31 Days Shipment Report | `order_type`=SO · `SIC` IN (F,ISPF) · line hold `SDHOLD` < 0 (no hold) | `actual_ship_date` in last ~40 days | ⚠gap (LINE hold; fact carries HEADER hold) |
| Order with Multiple Shipments - Scheduler | `co` IN (00640,00645) · `order_type` IN (S1,SE,SZ,SM,SO) · `last`<>980 · `next`<620 | — | ⚑DAX: `DISTINCTCOUNT(shipment_number)` per order > 1 |
| Shipment with Multiple Orders - Scheduler | `co` IN (00640,00645) · `order_type` IN (S1,SE,SZ,SM,SO) · `last`<>980 · `next`<620 | — | ⚑DAX: `DISTINCTCOUNT(order_number)` per shipment > 1 |

### Family 10 — Commission (OUT OF SCOPE)
| Report | Filters (reference only) | Date | Flags |
|---|---|---|---|
| SOP0027 - Commission | `next`=999 · `last`<>980 · `line_type` NOT IN (F,FT) · `item2` NOT IN ('MISC BILLING','EXPEDITE FEE') · `order_type` IN (SO,CO) · `co`<>00750 | `gl_date` = year | **Out of scope** — commission %/$ come from **F42005** (separate fact `fact_sales_commission`, not built) |

### Baseline (not a variation page)
| Report | Note |
|---|---|
| ESO1 Hubble Query | The **master** the fact was built from (`line_type`=S · `last`<>980 · `co` IN (00640,00645) · F4074 freight-adjust types). Defines the fact's column set — not a report page. |

---

## D. Attributes not yet on the fact (⚠gap) — resolution

These few reports reference something the fact doesn't currently carry. Options per case:

| Report(s) | Missing attribute | Resolution |
|---|---|---|
| 04a Export, AP Minerals, Luhe | **Ocean mode** — legacy filters `F4941.RSMOT='OCE'` on the *shipment routing* table | The fact's own `mode_of_transport` (SDMOT) is available; if it reliably carries `OCE` for ocean lines, slice on it. If exactness vs. the routing table matters, add a shipment-mode column. Practically: combine with the export `order_type`/`plant`/`parent` filters already listed. |
| Mak Export Orders | **Sold-to category `F03012.AIAC05='E26'`** (customer-master-by-LOB) | Redundant here — the report already filters an explicit **71-order whitelist** (`order_number IN …`), which fully scopes it. Use the order list; ignore E26. |
| Orders on Hold for Pricing | **`zone_number` (SDZON='Y')** line pricing-review flag | `zone_number` exists in Silver (SDZON) but is not among the fact's current columns. Add it (1 column) if this report is in scope, then slice `(hold_orders_code IN (ZP,WP) OR zone_number=Y)`. |
| Orders with Zero Unit Price | **No-effective-base-price test** (`NOT EXISTS` on F4106 by item/plant/ship-to/date) | Not a single column — needs a precomputed `has_effective_price` flag on the fact (or a DAX measure). Branch A (`price_per_unit=0`) works today with existing columns. |
| Past 31 Days Shipment Report | **LINE hold `SDHOLD`** | The fact stores the **header** hold (`hold_orders_code` = F4201.SHHOLD). This report keys on the *line* hold. Add the line-hold column if exact parity is required; otherwise the header hold is a close proxy. |
| SOP0020Lite - 90 Day Transload | **`branch_plant` LIKE '%97%'/'%98%'** (transload plants) | `branch_plant` is on the fact; use a "contains 97 or 98" slicer / a plant group, rather than an equality. |

All other 100+ reports map entirely onto existing fact columns.

---

## E. Special-handling summary

- **⚑DAX (7 reports)** — not a simple slicer:
  - *Order with Multiple Shipments* → measure `DISTINCTCOUNT(shipment_number)` grouped by order, visual filter > 1.
  - *Shipment with Multiple Orders* → measure `DISTINCTCOUNT(order_number)` grouped by shipment, visual filter > 1.
  - *Days Since Invoice* → measure from `MAX(invoice_date)` → "days since" (the ABAT1='CB' scope IS a real slicer: `sold_to_search_type`).
  - *Standridge, Sto Corp, Thai Tan, Tri-Iso* → the `GET_HASH_VALUE` RowID sums are distinct-count helpers → `DISTINCTCOUNT`; the real metric is `SUM(transaction_quantity)` (SDUORG). Their row filters (in the table above) are normal slicers.
- **⟐frt (9 reports)** — the F4074 `ALAST` freight-code list is applied inside the billable/payable-freight
  **measure**, not as a page slicer. Nothing to build on the page for it.
- **⟲hist** — informational only; the F42119 union is already in the fact.
- **Out of scope** — SOP0027 Commission (F42005).

---

*Generated 2026-07-16 from the SQL in `ESO1 Filter Capture/`. Companion to `README.md` (variation catalog +
consolidated join model) and `docs/ESO1_gold_layer_design.md`. Fact-column vocabulary and the denormalized
address model are documented in `docs/ESO1_hubble_field_mapping.md`.*
