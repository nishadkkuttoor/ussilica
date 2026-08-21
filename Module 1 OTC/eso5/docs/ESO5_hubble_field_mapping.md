# ESO5 — Hubble → Silver → Gold Field Mapping (Source-to-Target)

S2T for `nb_eso5_gold_fact_extended_sales_order_5` + the reused address dims. JDE columns per
`eso5/full_metadata.txt`; joins/columns per `eso5/Extended Sales Order 5.docx`; Hubble SQL
(`eso5/hubble query.txt`) is the reference. Silver is already decoded (Julian→date, implied decimals
resolved, snake_case). **All docx §4 joins applied; NO §5 filters applied. §7 Calculations = N/A.**

## Silver spine source (batch snapshot)
| JDE | Silver table | Role |
|---|---|---|
| F4211 | `f4211_sales_order_detail_file` | **spine (snapshot)** — SBXLOADPOVIEW base (SX lines) + BOL/SANDTKT/OX-weight/SO-match |

## Static-snapshot sources (read on the fact)
| JDE | Silver table | Role on the fact |
|---|---|---|
| F554201T | `f554201t_sand_box_sales_order_qc_information` | Sand PO Number (QCDS50); join kcoo/doco/dcto |
| F4311 | `f4311_purchase_order_detail_file` | OX Last/Next status + OX Amount |
| F0911 | `f0911_account_ledger` | Carrier PO GL Post flag (GLPOST) |
| F43121 | `f43121_purchase_order_receiver_file` | PO Receipt GL Date (PRDGL) + GLPost doc linkage |
| F0101 | `f0101_address_book_master` | **rate (ABURAT) ONLY** — the `lofa_rate` measure input (name → reused dim) |

> **F0005 is NOT read by the fact notebook.** Its 55/UP attributes live in `dim_uss_plant` (below). The
> fact reads that **Gold dim** (not Silver F0005) for the one build-time value it needs: `lofa_mcu`
> (DRSPHD), the `L.SDMCU IN (…)` input of the SOORDERNO match. → `dim_uss_plant` is a **prerequisite**.

## Gold read (prerequisite)
| Gold table | Read for | Why |
|---|---|---|
| `lh_jde_gold.rpt.dim_uss_plant` | `lofa_mcu` (DRSPHD) by vendor | SOORDERNO SO-match input — replaces the direct Silver F0005 read |

## Dimension sources
| JDE | Gold dim | Built by |
|---|---|---|
| F0005 `55/UP` | **`dim_uss_plant`** (vendor_number → uss_plant_sand / shipped_from / lofa_mcu) | `nb_eso5_gold_dim_uss_plant.py` (NEW — same pattern as ESO4 `nb_eso4_gold_dim_udc.py`) |
| F0101 | **`rpt.dim_address_book`** role views (sold/ship/carrier + loading via `_loading_port`) | REUSED — existing `old_nb/nb_dim_address_book` |
| F0006 | **`rpt.dim_plant`** (`plant_code` = MCMCU; description `plant_name` = MCDL01) | REUSED conformed dim, built upstream (NOT by ESO5). District migrated here from `dim_business_unit` 2026-07-26 |

> **Star schema.** The fact stores FK codes; descriptions resolve through dimensions. F0005's display
> flags → new `dim_uss_plant` (keyed by vendor = `loading_facility`), **joined in the semantic model**;
> F0101's name → reused `dim_address_book`. F0101 is still read on the fact only for `ABURAT` (rate,
> absent from the reused dim). No new address dim; no `dim_date`.

## Report columns (docx §6) → Gold model  (39 display columns, in docx order)

The fact **stores 35**; 4 columns (#16/#17/#18/#19) resolve through dimensions (star schema).
| # | Heading | Source | JDE col | `snake_case` (Silver) | Gold fact column |
|---|---|---|---|---|---|
| 1 | Load Number | SBXLOADPOVIEW | SDDOCO | `document_order_invoice_e` | `load_number` |
| 2 | Document Type | SBXLOADPOVIEW | SDDCTO | `order_type` | `document_type` |
| 3 | Company | SBXLOADPOVIEW | SDKCOO | `company_key_order_no` | `company` |
| 4 | District | SBXLOADPOVIEW | SDMCU | `cost_center` | `district` — the raw code (FK / grain / filter), shown as-is (matches Hubble). FK → reused `dim_plant.plant_code`; name via `dim_plant[plant_name]` (F0006 MCDL01). Migrated from `dim_business_unit` 2026-07-26 — design §6a |
| 5 | Customer | SBXLOADPOVIEW | SDAN8 | `address_number` | `sold_to` (FK → dim_address_sold_to) |
| 6 | Ship To | SBXLOADPOVIEW | SDSHAN | `address_number_ship_to` | `ship_to` (FK → dim_address_ship_to) |
| 7 | Carrier | SBXLOADPOVIEW | SDCARS | `carrier` | `carrier` (FK → dim_address_carrier) |
| 8 | Customer PO | SBXLOADPOVIEW | SDVR01 | `reference_01` | `customer_po` |
| 9 | Sand PO Number | F554201T | QCDS50 | `description_50_characters` | `sand_po_number` |
| 10 | USS Customer PO# | SBXUSSSAND | SOPONO | *(SO-match L3.SDVR01)* | `uss_customer_po` |
| 11 | Item Number | SBXLOADPOVIEW | SDLITM | `identifier_second_item` | `item_number` |
| 12 | Item Description | SBXLOADPOVIEW | SDDSC1 | `description_line_01` | `item_description` |
| 13 | Order Date | SBXLOADPOVIEW | SDTRDJ | `date_transaction_julian` | `order_date` |
| 14 | GL Date | SBXLOADPOVIEW | SDDGL | `dt_for_gl_and_vouch_01` | `gl_date` (null→1900-01-01) |
| 15 | Loading Facility | SBXLOADPOVIEW | LOFA=SDVEND | `primary_last_vendor_no` | `loading_facility` (FK → dim_address_loading_facility) |
| 16 | Loading Facility Name | F0101 | ABALPH | `name_alpha` | **dim_address_loading_facility** `name_alpha` (reused; not on fact) |
| 17 | LOFA MCU | SBXUSSSAND | LOFAPLANTMCU | *(F0005 55/UP DRSPHD)* | **dim_uss_plant** `lofa_mcu` |
| 18 | Shipped From | SBXUSSSAND | PLANTTRANSLOAD | *(DRSPHD CASE)* | **dim_uss_plant** `shipped_from` |
| 19 | USS PLANT SAND (Y/N) | SBXUSSSAND | USSSAND | *(DRSPHD CASE)* | **dim_uss_plant** `uss_plant_sand` |
| 20 | USS Match (Y/N) | SBXUSSSAND | MATCHFLAG | *(SO-match exists)* | `uss_match` |
| 21 | USS SO Order # | SBXUSSSAND | SOORDERNO | *(SO-match L.SDDOCO, MCU∈plant)* | `uss_so_order_no` |
| 22 | USS SO Weight | SBXUSSSAND | SOWEIGHT | *(Σ SO `units_secondary_qty_or`, lnty=S)* | `uss_so_weight` |
| 23 | SBX Weight | SBXUSSSAND | SXWEIGHT | *(Σ SX/COM `units_transaction_qty`, lttr≠980)* | `sbx_weight` |
| 24 | SO Alt BOL # | SBXUSSSAND | SOALTBOLNO | *(SO-match L2.SDPSIG)* | `so_alt_bol_no` |
| 25 | Sand Ticket | SBXLOADPOVIEW | SANDTKT | *(MAX SDDSC1 where item=SANDTKTNBR)* | `sand_ticket` |
| 26 | BOL | SBXLOADPOVIEW | BOL | *(MAX SDDSC1 where item=BOL)* | `bol` |
| 27 | Unit of Measure | SBXLOADPOVIEW | SDUOM | `uom_as_input` | `uom` |
| 28 | Quantity per UoM | SBXLOADPOVIEW | QTY | *(DECODE SDPRP1='COM'…)* | `quantity` |
| 29 | Unit Price | SBXLOADPOVIEW | SDUPRC | `amt_price_per_unit_02` | `unit_price` |
| 30 | Total Amount | SBXLOADPOVIEW | EXTAMT=SDAEXP | `amount_extended_price` | `total_amount` |
| 31 | Last Status | SBXLOADPOVIEW | SDLTTR | `status_code_last` | `last_status` |
| 32 | Next Status | SBXLOADPOVIEW | SDNXTR | `status_code_next` | `next_status` |
| 33 | Invoice Number | SBXLOADPOVIEW | SDDOC | `doc_voucher_invoice_e` | `invoice_number` |
| 34 | OX Last Status | SBXLOADPOVIEW | OXLTTR | *(MAX F4311 PDLTTR, OX)* | `ox_last_status` |
| 35 | OX Next Status | SBXLOADPOVIEW | OXNXTR | *(MAX F4311 PDNXTR, OX)* | `ox_next_status` |
| 36 | OX Amount | SBXLOADPOVIEW | OXAMT | *(Σ F4311 PDAEXP if item=FRT)* | `ox_amount` |
| 37 | Carrier PO GL Post Flag | SBXLOADPOVIEW | GLPOST | *(F0911 OV GLPOST via F43121)* | `carrier_po_gl_post_flag` |
| 38 | PO Receipt GL Date | SBXLOADPOVIEW | GLDGJ | *(MAX F43121 PRDGL, OV/matc=1)* | `po_receipt_gl_date` |
| 39 | Line ID | SBXLOADPOVIEW | SDLNID | `line_number` | `line_id` — the **raw JDE value**: `1.00` → **`1000`** (`round(line_number * 1000)`, int64). Silver decoded the 3 implied decimals; the notebook puts them back. Lossless: a kit line `1.010` → `1010` |

## Measure (Hubble ReportColumn1)
| Hubble | Amount | Gold | Model measure |
|---|---|---|---|
| ReportColumn1 | `F0101.ABURAT × 0.01` | `lofa_rate` (`user_reserved_amount × RATE_FACTOR`) | **Rate** = SUM(lofa_rate) |

Additive numerics also exposed as SUM measures (report totals): Total Amount, OX Amount, Quantity,
USS SO Weight, SBX Weight; plus Load Count = DISTINCTCOUNT(load_number), Line Count = COUNTROWS.

## Derivations / assumptions (design §5)
| Gold col | Definition |
|---|---|
| `quantity` | `if purchasing_report_code_01='COM' then units_transaction_qty/2000 else units_transaction_qty` (Silver decoded → drop Hubble `/1000`) |
| `sbx_weight` / `uss_so_weight` | Σ decoded units over the scoped SX/SO lines (drop `/1000`) |
| `lofa_rate` | `user_reserved_amount × 1.0` (Silver decoded → Hubble `×0.01` not needed) — semi-additive per LOFA |
| `uss_plant_sand`/`shipped_from`/`lofa_mcu` | **`dim_uss_plant`** (not on the fact) — F0005 `55/UP` DRSPHD by vendor: `1<x<9000`→Y/PLANT, `>9000`→TRANSLOAD, else N/3RDPARTY |
| `uss_match`/`uss_so_order_no`/`uss_customer_po`/`so_alt_bol_no`/`uss_so_weight` | F4211 SO orders (company 00400) matched by padded `pull_signal`/`reference_01` (§8 verify). `uss_so_order_no` additionally requires `so.cost_center = dim_uss_plant.lofa_mcu` (read from the **Gold dim**, not Silver F0005) |
| `load_scope_key` | sha2(company ‖ document_type ‖ load_number) — CDC delete scope (the load) |
| `load_line_key` | sha2 of the 58 GROUP BY columns — unique row key (`row_class` is in it, which is what stops a `PO_HOLADD` row colliding with an F4211 line of the same `line_id`) |

## Filter-Capture variation columns (added 2026-07-14)
The four `eso5/Filter Capture/` queries are served from this same fact. `…PO Details (New)` is an
identical query to the core and `…PO Details` is a column subset of it — neither needs a thing. The other
two do:

| Fact column | Hubble | Notes |
|---|---|---|
| `row_class` | — | `LINE` / `HOLADD` / `TEXT` / `PO_HOLADD` — which report(s) the row belongs to. **Every page filters on it** (design §7b) |
| `line_type` | SDLNTY | the `TL` text lines the core view drops but the reconciliation report keeps |
| `product_category` | SDPRP1 → `purchasing_report_code_01` | `COM` (sand) / `FRT` — drives SANDWEIGHT, EXTWEIGHT, FRTAMT, PROP, LOFA |
| `sales_report_code_01` | SDSRP1 → **`sales_reporting_code_01`** | `'352'` → SANDAMT (keys off the reporting code, **not** the item number) |
| `units_ordered` | SDUORG → `units_transaction_qty` | RAW decoded units — `quantity` is the report-facing version (COM ÷ 2000 → tons); the pivots need the undivided value |
| `item_weight` | SDITWT → **`amount_unit_weight`** | → EXTWEIGHT (`product_category='FRT'`) |
| `load_last_status` | the reconciliation view's per-load `SDLTTR` CASE | `980` when the load is fully cancelled, else `MAX(SDLTTR)` over its non-HOLADD, amount≠0 lines. Precomputed (not a plain aggregate) |
| `leg_1` / `leg_2` / `leg_3` | F554201T QCLGL1/2/3 → **`descriptn_01` / `_02` / `_03`** | reconciliation report only |
| `qc_string_3` | F554201T QCFSTR3 → **`future_use_string_03`** | reconciliation report only |
| `header_district` / `_sold_to` / `_ship_to` / `_carrier` / `_customer_po` / `_order_date` | F4201 SHMCU / SHAN8 / SHSHAN / SHCARS / SHVR01 / SHTRDJ | the reconciliation view groups by the **header** values, which need not equal the line's own |

> ⚠ **The JDE→snake_case map is not literal — look every name up in `eso5/full_metadata.txt`.** The five
> bolded above are where transliterating the alias gives the wrong answer (`item_weight`,
> `sales_report_code_01`, `leg_1/2/3`, `free_form_string_3`, `uom_transaction_qty` are all **not** real
> Silver columns). Note `PDUOM` and `SDUOM` share the same Silver name, `uom_as_input`. The *fact's*
> column names are unaffected — only the sources they read from.

`PO_HOLADD` rows (orphan F4311 OX HOLADD purchase-order lines — design §3d) set `unit_price` and
`total_amount` to **0**, take `ox_amount`/`ox_last_status`/`ox_next_status` from the PO row itself, use
**PDAN8 as the carrier**, and back-fill `customer_po`/`sold_to`/`loading_facility`/`gl_date`/`last_status`/
`next_status`/`invoice_number` from the load's FRT line.

## Grain & GROUP BY
Hubble's outer query `GROUP BY`s its display columns and `SUM`s the single `ReportColumn1`. The fact
stores **58** group-by columns — the core report's 35 (4 of its 39 moved to dims; they are functionally
dependent on `loading_facility`, so the grain is unchanged) plus 6 status columns and the 17 above
(+ `lofa_rate` = 59 business + 2 keys = **61 stored**). The inner `SELECT DISTINCT` carries
the F0101 PK (ABAN8). The fact reproduces this: reconstruct → `.distinct()` (inner) →
`groupBy(grain).agg(sum(lofa_rate))` (outer). The extra columns only *refine* the group-by — they cannot
split a Hubble row, because `line_id` is already in it.

**Grain = one line**: every F4211 SX / 00750 line (TL and HOLADD lines **no longer dropped**) plus one row
per orphan F4311 OX HOLADD PO line.

## Filters — NONE are applied in the notebook (2026-07-14; reaffirmed 2026-07-20)

Per user direction, the five queries are used **only** to identify source tables, fields, joins, business
logic, calculations and transformations. **Not one row filter is applied.** Each is either dropped or
carried as a column the report filters on:

| Source predicate | Where it went |
|---|---|
| `SDDCTO = 'SX'` | column **`document_type`** — report filters `= "SX"` |
| `SDKCOO = '00750'` | column **`company`** — report filters `= "00750"` |
| `SDLNTY <> 'TL'` | **`row_class = 'TEXT'`** — report excludes |
| `SDLITM <> 'HOLADD'` | **`row_class = 'HOLADD'`** — report excludes |
| docx §5 slicers (Sold To / Ship To / LOFA / Load# / Statuses / Order Date 5-1–5-31 / GL Date / Customer PO / Carrier / Item# / GL Post) | **dropped** |
| the variations' own slicers: `ORDATE BETWEEN …`, `SDLITM='FRT'`, `SDNXTR<'581'`, `SHDOCO IN (22815083)`, `SDLTTR NOT IN ('980')` | **dropped** |
| F0101 `ABAT1 BETWEEN 'A'..'P' OR 'R'..'ZZZ'` (search-type) | **kept as a VALUE DEFINITION** — a CASE inside the `lofa` aggregate, not a `WHERE`. It decides which address rows count as a rate source; deleting it would make `lofa_rate` wrong, not unfiltered. Drops no row. |
| **any** status predicate that dropped a row — `NOT EXISTS(live SX HOLADD, SDLTTR<>'980')`, SXWEIGHT's `WHERE SDLTTR<>'980'` | **filter → FIELD** (`po_holadd_superseded`) / **filter → conditional sum**. See below |

### Status — zero status filtering in the notebook
Per user direction, no `status_code` / last-status / next-status predicate filters a row. All the status
**joins** are implemented and every status **field** is stored so the report can filter on it:

| Fact column | Hubble | Notes |
|---|---|---|
| `last_status` / `next_status` | SDLTTR / SDNXTR | the line's own |
| `ox_last_status` / `ox_next_status` | F4311 PDLTTR / PDNXTR | a `PO_HOLADD` row reports its **own**, not the load MAX |
| `load_last_status` | recon per-load `SDLTTR` CASE | a calculation (drops no rows) |
| `load_max_last_status` / `load_min_last_status` | MXLTTR / MILTTR | the CASE's inputs — stored so the report can re-derive or override it |
| `po_holadd_superseded` | report 4's `NOT EXISTS(...)` | `'Y'` ⇔ the load has a live (`<>'980'`) SX HOLADD sales line. **Hubble's orphan set = `'N'`** |
| `ox_amount_gross` | F4311 OX sum, unconditioned | `ox_amount` bakes in `item='FRT'` / `item='HOLADD' AND last_status<>'980'`; this exposes the same money without it |

> **The dividing line:** a status predicate in a `WHERE` / `NOT EXISTS` / anti-join **removes rows** —
> that's a filter, and it's gone. A status reference inside a `CASE` or conditional aggregate **computes a
> value** — that's a calculation; removing it wouldn't make the column unfiltered, it would make it
> **wrong** (and an aggregate over *other* rows cannot be rebuilt at report level). Those stay, with every
> input stored alongside.

⇒ The fact is a strict **superset** of every report: all of F4211, plus the orphan OX HOLADD PO rows.
**Every report page must filter `document_type` + `company` + `row_class`** (design §7b) — omit it and the
core report sums the whole table.

### What is NOT a filter (and is therefore kept) ⚠
A predicate **inside a correlated subquery** is the *definition of the value it computes*. `MAX(SDDSC1)
WHERE SDLITM='BOL'` is what BOL **means**; strip it and `bol` doesn't become unfiltered, it becomes wrong.
These stay, exactly as the SQL states them: `SDLITM='BOL'`/`'SANDTKTNBR'` (bol / sand_ticket); `PDDCTO='OX'
AND PDKCOO='00750'` (ox_*); `PRDCT='OV' AND PRMATC='1' AND PRDGL>1` (po_receipt_gl_date); `GLDCT='OV' AND
GLKCO='00750'` (carrier_po_gl_post_flag); `SDDCTO='SO' AND SDCO='00400'` (+ `SDLNTY='S'`) (the SO-match /
SOWEIGHT); `SDDCTO='SX' AND SDPRP1='COM' AND SDLTTR<>'980'` (sbx_weight); `PDDCTO='OX' AND PDLITM='HOLADD'`
+ `NOT EXISTS(live SX HOLADD)` (the PO_HOLADD class); `55/UP` (dim_uss_plant).

> **Consequence:** those lookups remain `00750`/`00400`-scoped, so a non-`00750` fact row carries NULL
> `ox_*` / `carrier_po_gl_post_flag` / `po_receipt_gl_date` / `sand_po_number` / `uss_*`. Invisible to the
> five reports (they all filter `company="00750"`) — but the fact must not be read unfiltered.
