# ESO4 — Hubble → Silver → Gold Field Mapping (Source-to-Target)

S2T for `nb_eso4_gold_fact_sales_tax_reconciliation` + ESO4 dims. JDE columns per
`eso4/full_metadata.txt`; joins/columns per `eso4/Extended Sales Order 4.docx`; Hubble SQL
(`eso4/hubble query.txt`) is the reference. Silver is already decoded (Julian→date, implied
decimals resolved, snake_case). **All docx joins applied; no filters applied.**

## Streamed sources (CDF)
| JDE | Silver table (assumed) | Role |
|---|---|---|
| F4211 | `f4211_sales_order_detail_file` | fact stream + scope |
| F03B11 | `f03b11_customer_ledger` | fact stream + AR amounts |

## Static-snapshot sources (read by the FACT notebook)
| JDE | Silver table (assumed) | Role on the fact |
|---|---|---|
| F0006 | `f0006_business_unit_master` | `business_stream` calc input (MCRP20). Plant attrs live in the reused `dim_plant` |
| F0101 | `f0101_address_book_master` | fact FK code `sic_code` (ABSIC); address relationships via **reused `rpt.dim_address_book`** |
| F0116 | `f0116_address_by_date` | fact FK code `jurisdiction` (ALADDS) + degenerate `county` (ALCOUN) |

## Dimension sources
| JDE | Silver table (assumed) | Gold dim | Built by |
|---|---|---|---|
| F0006 | `f0006_business_unit_master` | **reused** `rpt.dim_plant` (plant_name, business-stream code `category_code_cost_ct_020`) | existing `dim_plant` builder (not owned by ESO4) |
| F0005 | `f0005_user_defined_code_values` | `dim_sic` (01/SC → sic_description) + `dim_state` (00/S → state_name) | `nb_eso4_gold_dim_udc.py` |
| F0101⋈F0116 | (reused) | `rpt.dim_address_book` role views | existing `nb_dim_address_book` |

> **Star schema.** The fact stores FK **codes** only; descriptions resolve through dims:
> `plant`→ reused `rpt.dim_plant` (key `plant_code`), `sic_code`→`dim_sic`, `jurisdiction`→`dim_state`,
> `ship_to`/`sold_to`/`parent_number`→ reused `rpt.dim_address_book` role views. F0005 is **no longer read
> on the fact**.
> `county` and `business_stream` stay on the fact (county absent from the reused address dim;
> business_stream is a cross-table calc).

## Fact field mapping
| JDE col | snake_case (Silver) | Gold fact column | Notes |
|---|---|---|---|
| SDKCO | company_key | document_company | also join to RPOKCO; part of scope + avalara_code |
| SDDOC | doc_voucher_invoice_e | invoice_number | join to RPODOC; scope; avalara_code |
| SDDCT | document_type | document_type | join to RPODCT; scope; avalara_code |
| SDDOCO | document_order_invoice_e | order_number | |
| SDDCTO | order_type | order_type | |
| SDLNID | line_number | *(join only)* | = RPLNID |
| SDMCU | cost_center | plant | FK → dim_plant.plant_code (= MCMCU) |
| SDEXR1 | tax_explanation_code_01 | tax_explanation_code | |
| SDDGL | dt_for_gl_and_vouch_01 | gl_date | date sliced directly off the fact (no dim_date) |
| SDAN8 | address_number | sold_to | FK → dim_address_sold_to (docx label swap — design §5) |
| SDSHAN | address_number_ship_to | ship_to | FK → dim_address_ship_to; drives F0101 join |
| SDPA8 | address_number_parent | parent_number | |
| RPODOC | original_document_no | *(join only)* | = SDDOC |
| RPODCT | original_document_type | *(join only)* | = SDDCT |
| RPOKCO | company_key_original | *(join only)* | = SDKCO |
| RPLNID | line_number | *(join only)* | = SDLNID |
| RPDOC | doc_voucher_invoice_e | *(DISTINCT only)* | F03B11 PK — inner DISTINCT; NOT in GROUP BY → summed away, not stored |
| RPDCT | document_type | *(DISTINCT only)* | F03B11 PK — inner DISTINCT; NOT stored |
| RPKCO | company_key | *(DISTINCT only)* | F03B11 PK — inner DISTINCT; NOT stored |
| RPSFX | document_pay_item | *(DISTINCT only)* | F03B11 PK (pay item) — inner DISTINCT; NOT stored |
| RPTXA1 | tax_area_01 | tax_area | |
| RPDSVJ | date_service_currency | service_tax_date | date sliced directly off the fact (no dim_date) |
| RPATXA | amount_taxable | taxable_amount | × SHIFT_FACTOR (1.0) → measure |
| RPATXN | amount_tax_exempt | non_taxable_amount | × SHIFT_FACTOR → measure |
| RPSTAM | amt_tax_02 | tax_amount | × SHIFT_FACTOR → measure |
| RPAG | amount_gross | gross_amount | × SHIFT_FACTOR → measure |
| MCMCU | cost_center | *(join key)* | = SDMCU |
| MCDL01 | description_001 | *(dim_plant only)* | plant_name — resolved via `plant`→dim_plant, NOT on fact |
| MCRP20 | category_code_cost_ct_020 | *(calc input; dim only)* | Business Stream calc input; also on dim_plant as `category_code_cost_ct_020`, NOT on fact |
| ABSIC | standard_industry_code | sic_code | **FK → dim_sic**; also Business Stream calc input |
| ABAN8 | address_number | *(join key)* | = SDSHAN; = ALAN8 |
| ALADDS | state | jurisdiction | **FK → dim_state** — raw code (e.g. `CO`); state name resolved in dim_state |
| ALCOUN | county_address | county | degenerate (not in reused address dim) |

**F0005 (dim sources, no longer read on the fact):**
| JDE col | snake_case (Silver) | Gold dim column | Notes |
|---|---|---|---|
| DRKY | user_defined_code | dim_sic.sic_code / dim_state.state_code | UDC value; 01/SC → dim_sic, 00/S → dim_state |
| DRDL01 | description_001 | dim_sic.sic_description / dim_state.state_name | UDC description |

## Report table columns (docx §6 "Column Names")

The columns the docx specifies for display in the report table, in docx order, each augmented with the
Silver `snake_case_field` (`eso4/full_metadata.txt`) and the final Gold fact column. Docx numbering
skips **#6** (absent in the source). Amount columns (#10–13) surface as measures, not table columns.

The **Gold model column** shows where each display value now lives in the **star schema** — on the fact,
or resolved through a dimension via a relationship.

| # | Heading (docx) | Table | JDE col | `snake_case_field` (Silver) | Gold model column |
|---|---|---|---|---|---|
| 1 | Document Company | F4211 | SDKCO | `company_key` | fact `document_company` |
| 2 | Plant | F4211 | SDMCU | `cost_center` | fact `plant` (FK) |
| 3 | Plant Name | F0006 | MCDL01 | `description_001` | **dim_plant** `plant_name` |
| 4 | Business Stream | Calculation | — | *(calc §7)* | fact `business_stream` |
| 5 | Tax Explanation Code | F4211 | SDEXR1 | `tax_explanation_code_01` | fact `tax_explanation_code` |
| 7 | Tax Area | F03B11 | RPTXA1 | `tax_area_01` | fact `tax_area` |
| 8 | GL Date | F4211 | SDDGL | `dt_for_gl_and_vouch_01` | fact `gl_date` |
| 9 | Service/Tax Date | F03B11 | RPDSVJ | `date_service_currency` | fact `service_tax_date` |
| 10 | Taxable Amount | F03B11 | RPATXA | `amount_taxable` | fact `taxable_amount` → measure |
| 11 | Non-Taxable Amount | F03B11 | RPATXN | `amount_tax_exempt` | fact `non_taxable_amount` → measure |
| 12 | Tax Amount | F03B11 | RPSTAM | `amt_tax_02` | fact `tax_amount` → measure |
| 13 | Gross Amount | F03B11 | RPAG | `amount_gross` | fact `gross_amount` → measure |
| 14 | Document Type | F4211 | SDDCT | `document_type` | fact `document_type` |
| 15 | Invoice Number | F4211 | SDDOC | `doc_voucher_invoice_e` | fact `invoice_number` |
| 16 | Avalara Code | Calculation | — | *(calc §7, inferred)* | fact `avalara_code` |
| 17 | Jurisdiction | F0116 | ALADDS | `state` | fact `jurisdiction` (FK) → **dim_state** `state_name` |
| 18 | County | F0116 | ALCOUN | `county_address` | fact `county` |
| 19 | Ship To | F4211 | SDSHAN | `address_number_ship_to` | fact `ship_to` (FK) → dim_address_ship_to |
| 20 | Sold To | F4211 | SDAN8 | `address_number` | fact `sold_to` (FK) → dim_address_sold_to |
| 21 | Parent Number | F4211 | SDPA8 | `address_number_parent` | fact `parent_number` (FK) → dim_address_parent |
| 22 | Order Number | F4211 | SDDOCO | `document_order_invoice_e` | fact `order_number` |
| 23 | Order Type | F4211 | SDDCTO | `order_type` | fact `order_type` |
| 24 | SIC Code | F0101 | ABSIC | `standard_industry_code` | fact `sic_code` (FK) → dim_sic |
| 25 | SIC Description | F0005 | DRDL01 | `description_001` | **dim_sic** `sic_description` |
| 26 | Business Stream (raw) | F0006 | MCRP20 | `category_code_cost_ct_020` | **dim_plant** `category_code_cost_ct_020` |

> **#19/#20 label swap** (per the notebook's current SDAN8→`sold_to` / SDSHAN→`ship_to`): the JDE fields
> are inverted relative to the docx labels — open item, design §5.
> **#4 vs #26:** #4 is the fact calc classification (`business_stream`); #26 is the raw code that feeds
> it, also available on **dim_plant** (`category_code_cost_ct_020`), not on the fact.
> **#3 / #25 / #26** moved off the fact into dimensions (star schema); the report reaches them via
> relationships. **#25 SIC Description** and the state name (#17) resolve via F0005 UDC dims
> (`dim_sic` 01/SC, `dim_state` 00/S) — the UDC system/types are inferred (design §5).

## Calculations
| Gold column | Definition |
|---|---|
| business_stream | docx §7 CASE on trim(ABSIC) × trim(MCRP20) → 'O&G' / 'ISP' |
| avalara_code | **inferred** = concat(invoice_number, document_type, document_company) (Hubble XID_CUSTOM_8501fecff7aa51). SDDOC (int64) cast to string → `11843107`; SDKCO kept string so `00400` leading zeros survive → e.g. `11843107RI00400` |
| shift_factor_applied | constant 1.0 (Hubble NVL(ShiftFactor,0.01) de-scale not needed — Silver decoded) |
| document_scope_key | sha2(document_company‖document_type‖invoice_number) — CDC delete scope |
| sales_tax_line_key | sha2 of the GROUP BY columns — unique row key |

## Grain & GROUP BY (per `hubble query.txt`)

Hubble's outer query **`GROUP BY`s the report display columns and `SUM`s the four amounts**; the
F03B11 PK (`RPDOC/RPDCT/RPKCO/RPSFX`) sits in the inner `SELECT DISTINCT` only, so pay items sharing
a display tuple collapse into one summed row. The fact matches this: `sel` (with the PK) `.distinct()`
= inner DISTINCT, then `groupBy(FACT_GROUP_BY_COLS).agg(sum(...))` = outer GROUP BY.

**Fact grain = the Hubble GROUP BY tuple** (`sales_tax_line_key = sha2` of these columns):

`document_company, invoice_number, document_type, order_number, order_type, plant, ship_to, sold_to,
parent_number, tax_explanation_code, tax_area, avalara_code, business_stream, sic_code, jurisdiction,
county, gl_date, service_tax_date` (18 keys).

- **Summed** (Hubble `SUM`): `taxable_amount, non_taxable_amount, tax_amount, gross_amount`.
- **Carried** (functionally dependent, `first()`): the constant `shift_factor_applied`. (In the star
  schema `plant_name` / plant business-stream code / `sic_description` / state name live in dims, so they are
  no longer carried on the fact.)
- **The plant business-stream code (MCRP20) is dropped from the GROUP BY** — functionally dependent on `plant`
  (→ dim_plant), so the grain is unchanged; it feeds the `business_stream` calc at build time but is not stored.
- Hubble's duplicate GROUP BY keys are collapsed: `RPOKCO=SDKCO`, `RPODOC=SDDOC`, `RPODCT=SDDCT`, the
  `C_*` copies, and `XID_CUSTOM_8501fca6bf83d39 = concat(SDDOC,SDDCT)` (dependent on invoice+doc type).

## Hubble ReportColumns → measures
| Hubble | Amount | Measure |
|---|---|---|
| ReportColumn1 | RPATXA × ShiftFactor | Taxable Amount |
| ReportColumn2 | RPATXN × ShiftFactor | Non-Taxable Amount |
| ReportColumn3 | RPSTAM × ShiftFactor | Tax Amount |
| ReportColumn4 | RPAG × ShiftFactor | Gross Amount |

## Not mapped
- Hubble `dwtemp…` ShiftFactor company table → constant `SHIFT_FACTOR`.

> **Note:** SIC Description (F0005 DRDL01) and the jurisdiction state name are now modeled as **dimensions**
> (`dim_sic` 01/SC, `dim_state` 00/S), resolved via relationships rather than denormalized on the fact.
> F0005 is still absent from the docx §4 join table / Hubble SQL, so the UDC system/types (`01/SC`, `00/S`)
> remain assumptions to confirm (design §5 open items).
