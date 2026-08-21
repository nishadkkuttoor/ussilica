# Extended Sales Order 4 (ESO4) — Workspace

**Client:** US Silica · **Platform:** Microsoft Fabric (F64) · **Layer target:** `lh_jde_gold`
**Started:** 2026-07-08 · **Status:** 🟢 BUILD v1.0 — Gold notebooks + semantic model generated (pending Fabric run)

> All new ESO4 artifacts (analysis, design, notebooks, scripts, queries, semantic model,
> pipelines) live under **this folder**. ESO1 artifacts stay where they are (repo root,
> `docs/`, `nb/`, `report/`, `pipelines/`) and are **not** modified from here.

## Why this folder exists
ESO1 (Billable v Payable Freight) is **not yet complete** — it is paused while awaiting the
remaining required resources. ESO4 work starts in parallel in this isolated workspace so the
two efforts don't collide. See the ESO1 status memory for the paused state and its open items.

## Source of truth (requirements)
- **`001 OTC Reports/Extended Sales Order 4.docx`** — the ESO4 report specification.
- **`001 OTC Reports/Filter Selections and Report Locations (OTC) (1).xlsx`** — filter/slicer
  and report-placement matrix (shared across the OTC/ESO reports; find the ESO4 sheet/section).
- **`Fabric_Naming_Convention_Guidelines.pdf`** — naming conventions (apply to all assets).

## Folder layout
| Path | Holds |
|---|---|
| `eso4/docs/` | Silver data analysis, Hubble field mapping, Gold layer design, report elements, runbook |
| `eso4/nb/` | Fabric PySpark notebooks (Silver→Gold, validation, maintenance, semantic model) |
| `eso4/queries/` | Hubble / source SQL and exploration queries |
| `eso4/report/` | Power BI semantic model (TMDL) + report |
| `eso4/pipelines/` | Fabric data pipelines + schedules |

## Conventions (carried from ESO1)
- Fabric asset prefixes `lh_/wh_/pl_/dpl_`; `dim_/fact_` tables; **snake_case** columns.
- Silver source `lh_jde_silver.jde.*` — already decoded (Julian→date, implied decimals
  resolved). (`SILVER_SCHEMA = "jde"` — 2026-07-26; was `cdf` → `jde_cdc` → `jde`. ESO4 is a batch build, no CDF.)
- Gold notebooks are **self-contained** (no `%run`); streaming pattern = Delta CDF →
  `foreachBatch` → CDC write, mirroring `nb_silver_to_gold_eso7_v2*`.
- Reuse conformed Gold dims where they exist (e.g. `rpt.dim_address_book` role views,
  `rpt.dim_plant`, `eso7.dim_uom_conversion`) rather than rebuilding.

## Deliverables (v1.0, 2026-07-08)
- **Notebooks** (`eso4/nb/`) — CDF-streaming, ESO1 architecture; all write to `lh_jde_gold.rpt`:
  - `nb_eso4_gold_fact_sales_tax_reconciliation.py` — fact (**star schema — FK codes only**), streams
    F4211 + F03B11, static F0006/F0101/F0116. F0005 is **not** read here.
  - `nb_eso4_gold_dim_udc.py` — **dim_sic** (F0005 01/SC) + **dim_state** (F0005 00/S) from one F0005 stream.
  - **dim_plant is REUSED** — the `plant` FK relates to the existing `rpt.dim_plant` (F0006, key `plant_code`);
    ESO4's model no longer references a business-unit dim. (`nb_eso4_gold_dim_business_unit.py` was deleted
    2026-07-26 — ESO5, its last consumer, was migrated to `rpt.dim_plant` too.)
  - **No date dimension** — `gl_date` / `service_tax_date` are native `dateTime` columns on the fact,
    sliced directly in the report; ESO4 has no `dim_date`.
  - **dim_address is REUSED** — `lh_jde_gold.rpt.dim_address_book` role views (`rpt.dim_address_ship_to` /
    `_sold_to` / `_parent`); ESO4 does not build an address dim. Create the `rpt.dim_address_parent` view once.
- **Semantic model** (`eso4/report/sales_tax_reconciliation.SemanticModel/`) — Direct Lake TMDL:
  **7 tables, 6 relationships** (all active), 7 measures, `Tax Status` calc column. Star schema: fact
  holds FK codes; descriptions resolve via reused dim_plant / dim_sic / dim_state / reused address dims.
- **Power BI report** (`eso4/report/ESO4_Sales_Tax_Reconciliation.Report/`, PBIR) — 3 pages
  (Tax Summary · Reconciliation Detail · Tax by Jurisdiction); all field refs validated vs TMDL.
- **Docs** (`eso4/docs/`) — `ESO4_gold_layer_design.md`, `ESO4_hubble_field_mapping.md`,
  `ESO4_semantic_model_relationships_measures.md`.

## Open items (see design §8)
Confirm: Avalara Code definition (inferred), Ship-To/Sold-To label swap (kept per docx), Silver
table names (F03B11/F0006/F0116/F0005), CDF enabled at/before init_ver, flip `OVERWRITE→False` in all
three notebooks after the first healthy full-load run. **SIC Description + Jurisdiction name** are modeled
as F0005 UDC dims (`dim_sic` 01/SC, `dim_state` 00/S) — confirm the **inferred** UDC system/types.

## Status log
- **2026-07-08** — Workspace created; requirements analyzed (docx + full_metadata + hubble query);
  Gold fact + dim notebooks, semantic model, and design/mapping docs generated.
- **2026-07-09/10** — Iterated: removed `dim_date` (dates on the fact); fact aggregated to the Hubble
  `GROUP BY` grain; `avalara_code` decimal fix; added SIC Description + jurisdiction state name;
  **converted to a full star schema** — fact stores FK codes only, new `dim_sic` + `dim_state` (F0005 UDC)
  built by `nb_eso4_gold_dim_udc.py`, model now 7 tables / 6 relationships. Gold schema set to
  `lh_jde_gold.rpt`. Next: run the **three** notebooks in Fabric (`OVERWRITE=True` once, then `False`).
