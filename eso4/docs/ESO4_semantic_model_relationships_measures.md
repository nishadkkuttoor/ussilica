# ESO4 Semantic Model — Relationships & Measures

Model: `eso4/report/sales_tax_reconciliation.SemanticModel` (Direct Lake).
Fact: `fact_sales_tax_reconciliation`. Generated from the TMDL definitions.

## Relationships (6)

All are single-direction, **many-to-one** from the fact to a dimension (fact = many side,
cross-filter single). `dim_plant` and `dim_address_*` are **reused** existing Gold dims (`rpt.dim_plant`;
`dim_address_*` role views over `rpt.dim_address_book`) — ESO4 does not build them; `dim_sic` / `dim_state`
are F0005 UDC reference dims (built by `nb_eso4_gold_dim_udc.py`).

| # | From (fact column) | To (dim column) | Cardinality | Active | Notes |
|---|---|---|---|---|---|
| 1 | `fact_sales_tax_reconciliation.plant` | `dim_plant.plant_code` | many → 1 | ✅ Active | reused `rpt.dim_plant` — Plant Name / Business Stream code (`category_code_cost_ct_020`) |
| 2 | `fact_sales_tax_reconciliation.ship_to` | `dim_address_ship_to.address_number` | many → 1 | ✅ Active | role view `rpt.dim_address_ship_to` |
| 3 | `fact_sales_tax_reconciliation.sold_to` | `dim_address_sold_to.address_number` | many → 1 | ✅ Active | role view `rpt.dim_address_sold_to` |
| 4 | `fact_sales_tax_reconciliation.parent_number` | `dim_address_parent.address_number` | many → 1 | ✅ Active | role view `rpt.dim_address_parent` (create once) |
| 5 | `fact_sales_tax_reconciliation.sic_code` | `dim_sic.sic_code` | many → 1 | ✅ Active | SIC Description (F0005 UDC 01/SC) |
| 6 | `fact_sales_tax_reconciliation.jurisdiction` | `dim_state.state_code` | many → 1 | ✅ Active | State name, e.g. `CO`→`Colorado` (F0005 UDC 00/S) |

> **Star schema.** The fact stores FK **codes** only (`plant`, `ship_to`/`sold_to`/`parent_number`,
> `sic_code`, `jurisdiction`); descriptions (`plant_name`, `category_code_cost_ct_020`, `sic_description`,
> state name) live in the dimensions and are surfaced on visuals via these relationships. `county` and
> `business_stream` stay on the fact (no dim: county absent from the reused address dim; business_stream
> is a cross-table calc).
>
> **No date dimension.** `dim_date` was removed from ESO4; dates are sliced directly off the fact's
> own `gl_date` / `service_tax_date` (`dateTime`) columns.

## Measures (7)

All defined on `fact_sales_tax_reconciliation`.

| Measure | DAX | Format | Folder |
|---|---|---|---|
| **Taxable Amount** | `SUM(fact_sales_tax_reconciliation[taxable_amount])` | `\$#,0;-\$#,0` | Tax |
| **Non-Taxable Amount** | `SUM(fact_sales_tax_reconciliation[non_taxable_amount])` | `\$#,0;-\$#,0` | Tax |
| **Tax Amount** | `SUM(fact_sales_tax_reconciliation[tax_amount])` | `\$#,0;-\$#,0` | Tax |
| **Gross Amount** | `SUM(fact_sales_tax_reconciliation[gross_amount])` | `\$#,0;-\$#,0` | Tax |
| **Effective Tax Rate** | `DIVIDE([Tax Amount], [Taxable Amount])` | `0.0%` | Tax |
| **Invoice Count** | `DISTINCTCOUNT(fact_sales_tax_reconciliation[avalara_code])` | `#,0` | Counts |
| **Tax Lines** | `COUNTROWS(fact_sales_tax_reconciliation)` | `#,0` | Counts |

> **Grain note:** the fact is pre-aggregated to the **Hubble `GROUP BY` grain** (one row per display
> tuple; the four amounts are `SUM`med across F03B11 pay items — see `hubble query.txt` / mapping doc).
> `SUM`-based measures give identical totals; **Tax Lines** counts these grouped reconciliation rows
> (not individual pay-item lines), matching Hubble. **Invoice Count** (`DISTINCTCOUNT(avalara_code)`) is
> unaffected since `avalara_code` is a grouping key.

## Calculated columns (0)

**There are NO DAX calculated columns in this model — on any of the 7 tables.**

This is not a stylistic choice: **Direct Lake tables cannot carry DAX calculated columns.** Every
column must exist physically in the underlying Delta file. A calculated column on a Direct Lake table
either fails to deploy or silently forces the table into DirectQuery fallback, losing the performance
the architecture exists for.

> ⚠ **Changed 2026-07-17 (Gate 1 review, finding 2).** This model previously defined **Tax Status** as
> a DAX calculated column — `IF(fact_sales_tax_reconciliation[tax_amount] > 0, "Taxable", "Non-Taxable")`
> — on a `mode: directLake` fact. That was invalid. It is now the **physical** Gold column `tax_status`,
> materialised by `nb_eso4_gold_fact_sales_tax_reconciliation.py` *after* the GROUP BY (so it derives
> from the **summed** `tax_amount`, exactly as the DAX did) and imported as an ordinary `sourceColumn`.
> Requires the pending `OVERWRITE=True` reload to materialise.

**Gold-derived columns (computed upstream, imported as regular columns):** `tax_status`,
`business_stream`, `avalara_code`, `document_scope_key`, `sales_tax_line_key`, `shift_factor_applied`
are all computed in the Spark notebook and physically stored on the fact. See the mapping doc /
design §5 for their logic. (`dim_plant` is a reused Gold dim built elsewhere, so ESO4 owns no
dimension-side derived columns.)

## Source amounts (measure inputs → Hubble ReportColumns)

| Fact column | JDE (F03B11) | Hubble | Scaling |
|---|---|---|---|
| `taxable_amount` | RPATXA `amount_taxable` | ReportColumn1 | × `shift_factor_applied` (1.0) |
| `non_taxable_amount` | RPATXN `amount_tax_exempt` | ReportColumn2 | × 1.0 |
| `tax_amount` | RPSTAM `amt_tax_02` | ReportColumn3 | × 1.0 |
| `gross_amount` | RPAG `amount_gross` | ReportColumn4 | × 1.0 |
