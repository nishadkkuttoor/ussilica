# ESO4 — Sales Tax Reconciliation (Power BI Report)

Direct Lake report over `../sales_tax_reconciliation.SemanticModel` (PBIR format, mirrors ESO1's
`.Report` structure). Audience: **Tax Department** — Avalara reconciliation.

## Pages
| Page | displayName | Content |
|---|---|---|
| `overview` | **Tax Summary** | Slicers (Business Stream, Tax Area, GL Date) · cards (Taxable / Non-Taxable / Tax / Gross / Effective Tax Rate) · column chart Tax Amount by Business Stream · bar chart Tax + Taxable by Jurisdiction |
| `detail` | **Reconciliation Detail** | The primary deliverable — a `tableEx` with every docx §6 column (Document Company → SIC Code) plus the four tax measures + Effective Tax Rate; slicers on Invoice Number and Jurisdiction |
| `byJurisdiction` | **Tax by Jurisdiction** | `tableEx` Jurisdiction / County / Business Stream × Taxable/Non-Taxable/Tax/Gross/Rate/Invoice Count; bar chart Tax by Tax Area; Business-Stream + Tax-Area slicers |

## Notes
- **No report-level filter** (the ESO1 `is_deleted=False` filter does not apply — the ESO4 fact
  stores no audit columns). `filterConfig.filters` is empty.
- All 53 field references validated against the semantic model TMDL (entities / columns / measures
  all resolve).
- The reconciliation table uses the fact's denormalized attributes (plant_name, jurisdiction,
  county, sic_code, business_stream) plus `dim_address_sold_to.name_alpha` (Sold To Name), so it
  renders without requiring every dimension relationship — dims remain available for slicing/role-play.
- Base theme `CY24SU10`; 1280×720 FitToPage pages, matching ESO1.

## To use
Open the workspace in Fabric / Power BI Desktop (PBIR) with the sibling
`sales_tax_reconciliation.SemanticModel`. Confirm the open items in
`eso4/docs/ESO4_gold_layer_design.md` §8 (Avalara Code, Ship/Sold labels) before publishing.
