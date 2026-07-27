# ESO5 Semantic Model — Relationships & Measures

Model: `eso5/report/sandbox_load_po.SemanticModel` (Direct Lake). Fact: `fact_extended_sales_order_5`.
Generated from the TMDL definitions. Star schema — the fact stores address FK codes; names resolve
through the **reused `rpt.dim_address_book`** role views.

## Tables (7) — who builds what

**ESO5 builds 2 of the 7 model tables. The other 5 are reused conformed Gold assets** — that is the point
of the star schema, not a gap. Everything lives in `lh_jde_gold.rpt`.

| Model table | Physical Gold object | Built by | Run before the fact? |
|---|---|---|---|
| **`fact_extended_sales_order_5`** | `rpt.fact_extended_sales_order_5` | **ESO5** — `nb_eso5_gold_fact_extended_sales_order_5.py` | — |
| **`dim_uss_plant`** | `rpt.dim_uss_plant` | **ESO5** — `nb_eso5_gold_dim_uss_plant.py` | ✅ **YES — prerequisite.** The fact reads its `lofa_mcu` for the SOORDERNO match |
| `dim_address_sold_to` | `rpt.dim_address_sold_to` | reused role view over `rpt.dim_address_book` | already exists |
| `dim_address_ship_to` | `rpt.dim_address_ship_to` | reused role view | already exists |
| `dim_address_carrier` | `rpt.dim_address_carrier` | reused role view | already exists |
| `dim_address_loading_facility` | `rpt.dim_address_loading_port` ⚠ *(note the name differs — the model table binds to the existing `_loading_port` view)* | reused role view | already exists |
| `dim_plant` | `rpt.dim_plant` | reused conformed dim (F0006; built upstream — NOT by ESO5) | already exists (migrated from `dim_business_unit` 2026-07-26) |

## Relationships (6)

All single-direction, **many-to-one** from the fact (fact = many side). The four `dim_address_*` are
role views over `rpt.dim_address_book`; `dim_uss_plant` is the F0005 55/UP plant dim; `dim_plant`
is the reused conformed F0006 plant dim (built upstream).

| # | From (fact column) | To (dim column) | Cardinality | Active | JDE |
|---|---|---|---|---|---|
| 1 | `fact_extended_sales_order_5.sold_to` | `dim_address_sold_to.address_number` | many → 1 | ✅ | SDAN8 (Customer) |
| 2 | `fact_extended_sales_order_5.ship_to` | `dim_address_ship_to.address_number` | many → 1 | ✅ | SDSHAN (Ship To) |
| 3 | `fact_extended_sales_order_5.carrier` | `dim_address_carrier.address_number` | many → 1 | ✅ | SDCARS (Carrier) |
| 4 | `fact_extended_sales_order_5.loading_facility` | `dim_address_loading_facility.address_number` | many → 1 | ✅ | LOFA=SDVEND (name) |
| 5 | `fact_extended_sales_order_5.loading_facility` | `dim_uss_plant.vendor_number` | many → 1 | ✅ | LOFA=SDVEND (USS/plant flags) |
| 6 | `fact_extended_sales_order_5.district` | `dim_plant.plant_code` | many → 1 | ✅ | SDMCU (District — code shown from the fact; name via `dim_plant[plant_name]`) |

> **`loading_facility` drives two relationships** — to `dim_address_loading_facility` (name via `name_alpha`)
> and to `dim_uss_plant` (`uss_plant_sand` / `shipped_from` / `lofa_mcu`). Both active; a single fact
> column may be the many-side of multiple relationships to different tables.
>
> **Address dims are REUSED** (all role views already exist from prior reports; no dim notebook). The
> `dim_address_loading_facility` model table binds (`entityName`) to the existing `rpt.dim_address_loading_port`.
> **`dim_uss_plant` is NEW** (`schemaName rpt`, `nb_eso5_gold_dim_uss_plant`). **No date dimension** —
> `order_date` / `gl_date` / `po_receipt_gl_date` are native `dateTime` columns sliced off the fact.

## Base measures (8)

All defined on `fact_extended_sales_order_5`.

| Measure | DAX | Format | Folder |
|---|---|---|---|
| **Rate** ⚠ | `SUMX(VALUES([loading_facility]), CALCULATE(MAX([lofa_rate])))` | `\$#,0.00;-\$#,0.00` | Amounts |
| **Total Amount** | `SUM(fact_extended_sales_order_5[total_amount])` | `\$#,0;-\$#,0` | Amounts |
| **OX Amount** | `SUM(fact_extended_sales_order_5[ox_amount])` | `\$#,0;-\$#,0` | Amounts |
| **Quantity** | `SUM(fact_extended_sales_order_5[quantity])` | `#,0.00` | Weights & Qty |
| **USS SO Weight** ⚠ | `SUMX(VALUES([load_scope_key]), CALCULATE(MAX([uss_so_weight])))` | `#,0.00` | Weights & Qty |
| **SBX Weight** ⚠ | `SUMX(VALUES([load_scope_key]), CALCULATE(MAX([sbx_weight])))` | `#,0.00` | Weights & Qty |
| **Load Count** ⚠ | `DISTINCTCOUNT(fact_extended_sales_order_5[load_scope_key])` | `#,0` | Counts |
| **Line Count** | `COUNTROWS(fact_extended_sales_order_5)` | `#,0` | Counts |

> ⚠ **The four marked measures are DEDUPED, not plain SUMs (fixed 2026-07-14).** `lofa_rate` is constant
> per **loading facility**, and `sbx_weight` / `uss_so_weight` are constant per **load** — but the fact is
> at LINE grain, so each value is physically repeated on every line of that load. A plain `SUM` multiplies
> them by the line count at any total, subtotal or roll-up. `SUMX(VALUES(key), CALCULATE(MAX(…)))` counts
> each load / facility exactly once, whatever the grain of the visual.
>
> `load_scope_key` (not `load_number`) is the dedup key because it is
> `sha2(company ‖ document_type ‖ load_number)` — the true load identity. `load_number` alone would
> conflate two loads sharing a document number under different companies or document types, which the fact
> now spans. It is hidden, but DAX can reference it.
>
> `Total Amount`, `OX Amount`, `Quantity` and `Line Count` are genuinely **per-line** and correctly remain
> `SUM` / `COUNTROWS`. So are the 19 reconciliation measures below — they sum line-level columns filtered
> by item, which is exactly what a per-load pivot means.

> ⚠ **These 8 are deliberately UNSCOPED** so one measure serves all five reports. The notebook applies the
> two F4211 filters shared by all five (`SDDCTO='SX' AND SDKCOO='00750'`, 2026-07-15) but still holds every
> `row_class` **and the whole F4311**, so the fact is a superset of each report. Every report page must
> therefore filter on **three** columns (design §7b):
> `document_type = "SX"` AND `company = "00750"` AND `row_class` — `= "LINE"` for core/(New)/(no-USS),
> `IN ("HOLADD","PO_HOLADD")` for for-HOLADD, `IN ("LINE","HOLADD","TEXT")` for Reconciliation.
> **Without it `Total Amount` still adds the HOLADD/TEXT rows and every F4311 PO row.** The `document_type`
> and `company` page filters remain necessary — they exclude PO rows (`document_type` forced to `'SX'`,
> `company` any). Set it at **report level** so a new visual cannot omit it.

## Reconciliation measures (19) — *SBX Load Reconciliation Report*

The SBXLOADDETAIL view's per-load pivots, expressed over the same line-grain rows (design §3e/§7c). Each
one restricts to `row_class IN ("LINE","HOLADD","TEXT")` internally — F4311 rows are excluded and the reconciliation
view only ever sees F4211 lines — so they are safe wherever they are used.

| Hubble | Measure | DAX | Folder |
|---|---|---|---|
| SANDWEIGHT | **Sand Weight** | `CALCULATE(SUM([units_ordered]), [product_category]="COM", [row_class] IN ("LINE","HOLADD","TEXT"))` | Reconciliation\Weights |
| EXTWEIGHT | **Ext Weight** | `CALCULATE(SUM([item_weight]), [product_category]="FRT", [row_class] IN ("LINE","HOLADD","TEXT"))` | Reconciliation\Weights |
| SANDWEIGHT/2000 | **Sand Tons** | `DIVIDE([Sand Weight], 2000)` | Reconciliation\Weights |
| EXTWEIGHT/2000 | **Ext Tons** | `DIVIDE([Ext Weight], 2000)` | Reconciliation\Weights |
| MILES | **Miles** | `CALCULATE(SUM([units_ordered]), [item_number]="FRT", …)` | Reconciliation\Detention & Miles |
| LOFADET | **LOFA Detention Hours** | `CALCULATE(SUM([units_ordered]), [item_number]="LOFADET", …)` | Reconciliation\Detention & Miles |
| WELLDET | **Well Detention Hours** | `CALCULATE(SUM([units_ordered]), [item_number]="WELLDET", …)` | Reconciliation\Detention & Miles |
| LOFAAMT | **LOFA Amount** | `CALCULATE(SUM([total_amount]), [item_number]="LOFADET", …)` | Reconciliation\Amounts |
| WELLAMT | **Well Amount** | `CALCULATE(SUM([total_amount]), [item_number]="WELLDET", …)` | Reconciliation\Amounts |
| LOFAPPAMT | **LOFA PP Amount** | `CALCULATE(SUM([total_amount]), [item_number]="LOFADETPP", …)` | Reconciliation\Amounts |
| WELLPPAMT | **Well PP Amount** | `CALCULATE(SUM([total_amount]), [item_number]="WELLDETPP", …)` | Reconciliation\Amounts |
| LOFAPBAMT | **LOFA PB Amount** | `CALCULATE(SUM([total_amount]), [item_number]="LOFADETPB", …)` | Reconciliation\Amounts |
| WELLPBAMT | **Well PB Amount** | `CALCULATE(SUM([total_amount]), [item_number]="WELLDETPB", …)` | Reconciliation\Amounts |
| FRTAMT | **Freight Amount** | `CALCULATE(SUM([total_amount]), [product_category]="FRT", …)` | Reconciliation\Amounts |
| FSCAMT | **Fuel Surcharge Amount** | `CALCULATE(SUM([total_amount]), [item_number]="FSC", …)` | Reconciliation\Amounts |
| SANDAMT | **Sand Amount** | `CALCULATE(SUM([total_amount]), [sales_report_code_01]="352", …)` — keys off **SDSRP1**, not the item | Reconciliation\Amounts |
| HOLAMT | **Holding Amount** | `CALCULATE(SUM([total_amount]), [item_number]="HOLADD", …)` | Reconciliation\Amounts |
| PROP | **Proppant** | `CALCULATE(MAX([item_description]), [product_category]="COM", …)` | Reconciliation |
| LOFA | **Load LOFA** | `CALCULATE(MAX([loading_facility]), [product_category]="COM", …)` | Reconciliation |

## Load-level measures (3) — *added 2026-07-20*

The Reconciliation report displays three values that Hubble computes as `MAX(...)` per load. They cannot be
grouping columns — `gl_date`, `invoice_number` and `next_status` can all differ line to line, so grouping
would split one load into several rows — and the model sets `discourageImplicitMeasures`, so a column
cannot simply be dropped into a visual and aggregated. They therefore need explicit measures.

| Hubble | Measure | DAX | Folder |
|---|---|---|---|
| SBXLOADDETAIL_GLDATE | **Load GL Date** | `CALCULATE(MAX([gl_date]), [row_class] IN ("LINE","HOLADD","TEXT"))` | Reconciliation |
| SBXLOADDETAIL_SDDOC | **Load Invoice No** | `CALCULATE(MAX([invoice_number]), [row_class] IN ("LINE","HOLADD","TEXT"))` | Reconciliation |
| SBXLOADDETAIL_SDNXTR | **Load Next Status** | `CALCULATE(MAX([next_status]), [row_class] IN ("LINE","HOLADD","TEXT"))` | Reconciliation |

> **`Load Next Status` is not `load_last_status`.** The first is the line-level `next_status` rolled up with
> `MAX`; the second is a *derived* per-load CASE reproduced in the notebook (design §3e) and stored as a
> physical column. The Reconciliation report uses both, for different columns.


EXTAMT reuses the base **Total Amount**. The view's per-load `SDLTTR` CASE is precomputed on the fact as
the `load_last_status` column (it is not expressible as a plain aggregate).

> **Rate is semi-additive** — `lofa_rate = ABURAT×1.0` is a per-Loading-Facility rate (Hubble's only
> summed `ReportColumn1`). `SUM` inflates it across multiple lines sharing a LOFA; use it filtered to a
> single facility, or switch to an aggregation that fits the rate semantics.

## Calculated columns (0)

There are **no** DAX calculated columns (docx §7 = N/A — no business calculation like ESO4's
`business_stream`). The **58 group-by** columns + the measure input `lofa_rate` (= 59 business + 2 keys
`load_line_key`/`load_scope_key` = **61 stored**, matching the TMDL) are **Gold-derived columns**
computed in the Spark notebook (`nb_eso5_gold_fact_extended_sales_order_5.py`) and physically stored on the fact:
the core report's 35 display columns + 6 status columns + the 17 the four variations need (`row_class`, the
reconciliation pivot inputs, the F554201T legs, and the F4201 `header_*` attributes). Four of the core report's display
columns live in dimensions instead: `loading_facility_name` in `dim_address_loading_facility`
(`name_alpha`); `uss_plant_sand` / `shipped_from` / `lofa_mcu` in `dim_uss_plant`.

## Column visibility
- **Hidden (fact):** `lofa_rate` (→ Rate measure), `load_line_key`, `load_scope_key` (grain/scope keys).
- **Visible (fact):** the 35 core display columns — including the four address **FK codes** (`sold_to`
  / `ship_to` / `carrier` / `loading_facility`), because Hubble displays the address *numbers* as values —
  plus the 17 variation columns. **`row_class`, `document_type` and `company` must stay visible**: they
  are the three columns every report page filters on (§7b), and the fact is unfiltered without them.
- **Via dims:** `loading_facility_name` (dim_address_loading_facility) and the three USS/plant flags
  (dim_uss_plant) — surfaced on visuals through the `loading_facility` relationships.
