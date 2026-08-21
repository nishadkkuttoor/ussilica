# Extended Sales Order 5 (ESO5) — Workspace

**Client:** US Silica · **Platform:** Microsoft Fabric (F64) · **Layer target:** `lh_jde_gold`
**Started:** 2026-07-10 · **Status:** 🟢 BUILD v2.0 — fact renamed `fact_extended_sales_order_5`, **all Gold-layer filters removed**, notebooks + semantic model + report + docs regenerated (pending Fabric run)

> All new ESO5 artifacts (analysis, design, notebooks, scripts, queries, semantic model,
> pipelines) live under **this folder**. ESO1 (repo root / `docs/` / `nb/` / `report/` /
> `pipelines/`) and ESO4 (`eso4/`) artifacts stay where they are and are **not** modified from here.

## Why this folder exists
ESO4 (Sales Tax with Business Stream — Avalara reconciliation) is **not yet complete** — it is
paused while awaiting the remaining required resources (a healthy Fabric run + the inferred-item
confirmations). ESO5 work starts in parallel in this isolated workspace so the efforts don't
collide. ESO1 remains paused as well. See the ESO4 / ESO1 status memories for their open items.

## Source of truth (requirements)
- **`001 OTC Reports/Extended Sales Order 5.docx`** — the ESO5 report specification (present; not yet analyzed).
- **`001 OTC Reports/Filter Selections and Report Locations (OTC) (1).xlsx`** — filter/slicer
  and report-placement matrix (shared across the OTC/ESO reports; find the ESO5 sheet/section).
- **`Fabric_Naming_Convention_Guidelines.pdf`** — naming conventions (apply to all assets).
- **Expected once provided** (as with ESO4): a `full_metadata.txt` (JDE→snake_case field metadata)
  and a `hubble query.txt` (the reference SQL). Drop these into `eso5/` when available.

## Folder layout
| Path | Holds |
|---|---|
| `eso5/docs/` | Silver data analysis, Hubble field mapping, Gold layer design, report elements, runbook |
| `eso5/nb/` | Fabric PySpark notebooks (Silver→Gold, validation, maintenance) |
| `eso5/queries/` | Hubble / source SQL and exploration queries |
| `eso5/report/` | Power BI semantic model (TMDL) + report |
| `eso5/pipelines/` | Fabric data pipelines + schedules |

## Conventions (carried from ESO1 / ESO4)
- Fabric asset prefixes `lh_/wh_/pl_/dpl_`; `dim_/fact_` tables; **snake_case** columns.
- Silver source `lh_jde_silver.<schema>.*` — already decoded (Julian→date, implied decimals
  resolved). Note: all notebooks read the Silver schema as `jde` (`SILVER_SCHEMA = "jde"` — 2026-07-26, was `jde_cdc`) and
  write Gold to `lh_jde_gold.rpt`; confirm the correct schemas for ESO5 before building.
- Gold notebooks are **self-contained** (no `%run`); batch pattern = read the full Silver snapshot →
  `build_*()` → overwrite, gated by `MANUAL_OVERWRITE`, mirroring the ESO4 notebooks.
- **Reuse conformed Gold dims** where they exist (`rpt.dim_address_book` role views, `rpt.dim_plant`
  — District dimensions here, `eso7.dim_uom_conversion`, and `dim_sic` / `dim_state` from ESO4) rather
  than rebuilding.
- **Star schema by default** (ESO4 decision): fact stores FK codes + degenerate dims + measures;
  descriptions live in dimensions and resolve via relationships.

## Templates worth copying from ESO4
- Fact processor: `eso4/nb/nb_eso4_gold_fact_sales_tax_reconciliation.py` (batch build:
  snapshot → `build_fact()` → overwrite, Hubble GROUP BY grain, star-schema FK columns).
- UDC reference dims (one source → multiple dims): `eso4/nb/nb_eso4_gold_dim_udc.py`.
- Semantic model + PBIR report: `eso4/report/…SemanticModel` / `…Report`.
- Docs pattern: `eso4/docs/ESO4_gold_layer_design.md`, `ESO4_hubble_field_mapping.md`,
  `ESO4_semantic_model_relationships_measures.md`.

## Deliverables (v2.0, 2026-07-20)
- **Notebooks** (`eso5/nb/`) — batch build (2026-07-26; ESO4 architecture), write `lh_jde_gold.rpt`.
  **Run order: the dim FIRST, then the fact.**
  - `nb_eso5_gold_dim_uss_plant.py` — **dim_uss_plant** (F0005 **55/UP**, keyed by vendor) →
    uss_plant_sand / shipped_from / lofa_mcu. Same implementation pattern/structure as ESO4's F0005 dim
    (`nb_eso4_gold_dim_udc.py`). **Address dims REUSED** (`rpt.dim_address_book` role views already exist;
    loading role binds to the existing `_loading_port`); F0101 name reused.
  - `nb_eso5_gold_fact_extended_sales_order_5.py` — **ONE fact for the core report AND all four Filter-Capture
    variations** (no second fact table). Streams **F4211 + F4311**; static F554201T/F0911/F43121/F0101/
    F4201. Reconstructs **four** Hubble custom views (SBXLOADPOVIEW + SBXUSSSAND + SBXLOADPOVIEWHOLADD +
    SBXLOADDETAIL) via pre-aggregated joins, applies the §4 top-level LEFT joins, Hubble GROUP BY grain
    (**59 business cols + 2 keys = 61 stored**, incl. `lofa_rate`; matches the TMDL's 61 `sourceColumn`s). **No direct F0005 dependency** — the flags join in the
    semantic model; the fact reads only `lofa_mcu` from the **Gold `dim_uss_plant`** (prerequisite) for
    the SOORDERNO SO-match.
- **Semantic model** (`eso5/report/sandbox_load_po.SemanticModel/`) — Direct Lake TMDL: **7 tables,
  6 relationships** (all active), **30 measures** (8 base + 19 reconciliation pivots + 3 load-level),
  **0 calc columns**
  (docx §7 = N/A). Fact table = `fact_extended_sales_order_5`.
- **Power BI report** (`eso5/report/ESO5_Sandbox_Load_PO.Report/`, PBIR) — 3 pages (Load Detail:
  39-col table + slicers · Load Summary: cards/charts · HOLADD: superseded-filtered detail, added 2026-07-21);
  all field refs validated vs TMDL.
- **Docs** (`eso5/docs/`) — `ESO5_gold_layer_design.md`, `ESO5_hubble_field_mapping.md`,
  `ESO5_semantic_model_relationships_measures.md`, and
  **[`ESO5_powerbi_report_guide.md`](docs/ESO5_powerbi_report_guide.md)** — everything the notebook
  deliberately does NOT do: the required page filters, how to avoid duplicate rows (⚠ the HOLADD
  double-count), and the per-report null analysis. Plus two DOCX deliverables in the same folder:
  **`ESO5_Notebook_Guide.docx`** (fact + dim notebooks explained) and **`ESO5_Report_Filters_Guide.docx`**
  (every filter in all five queries → how to implement it in Power BI).

## The five reports, one fact — and NO filters in the notebook
`eso5/hubble query.txt` is the core; `eso5/Filter Capture/` holds four variations. All five are served by
`fact_extended_sales_order_5`.

**The queries are used only for source tables, fields, joins, business logic, calculations and
transformations — the notebook applies NO row filter of any kind.** The fact holds **all of F4211 and all
of F4311**. Each report is defined purely by its page filter:

| Report | Page filter (⚠ REQUIRED — design §7b) |
|---|---|
| Sandbox Load Report w/ PO Details (core), …(New), …(no-USS) | `row_class="LINE"` · `document_type="SX"` · `company="00750"` |
| …for HOLADD | `row_class IN ("HOLADD","PO_HOLADD")` · `po_holadd_superseded="N"` · `document_type="SX"` · `company="00750"` |
| SBX Load Reconciliation | `row_class IN ("LINE","HOLADD","TEXT")` · `document_type="SX"` · ⚠ **no `company` filter** — Hubble's `SBXLOADDETAIL` has none (design §8.12) |

**Omit the filter and the core report sums every sales AND purchase order in the company.** Set it at
report level so a new visual cannot forget it.

> ⚠ **`row_class` is the ONLY column separating sales rows from purchase-order rows.** `document_type` is
> forced to `"SX"` on PO rows (the query hard-codes it so the UNION aligns with the sales load), so
> filtering on `document_type` + `company` alone lets all of F4311 through.

**Status filtering is 100 % the report's job too** (design §7d) — no `status_code` / last-status /
next-status predicate filters a row anywhere in the notebook. All the status joins are implemented and
every status field is stored: `last_status`, `next_status`, `ox_last_status`, `ox_next_status`,
`load_last_status`, `load_max_last_status` (MXLTTR), `load_min_last_status` (MILTTR),
`po_holadd_superseded`, `ox_amount_gross`. The two status predicates that used to *drop rows* became
fields: report 4's `NOT EXISTS(live SX HOLADD, SDLTTR<>'980')` is now **`po_holadd_superseded`** (Hubble's
orphan set = `"N"`), and SXWEIGHT's `WHERE SDLTTR<>'980'` is now a conditional sum.

> A predicate *inside a correlated subquery* is **not** a filter — it is the definition of the value
> (`MAX(SDDSC1) WHERE SDLITM='BOL'` is what BOL *means*). Those stay. See design §3a-bis.

## Open items (see design §8)
**Resolved 2026-07-20:** ✅ Silver table names (all eight verified against the root `full_metadata.json`
+ `converted_tables_22.json`, and every Silver *column* the notebook reads checked too — 0 missing);
✅ F0005 **55/UP CONFIRMED** from the core query itself (`drsy='55' and drrt='UP'`, DRKY = numeric vendor,
DRSPHD = plant MCU, thresholds verbatim from its CASE arms); ✅ schemas (`SILVER_SCHEMA=jde` [was `jde_cdc`], `GOLD=lh_jde_gold.rpt`).

**Still open — confirm:** Qty/weight de-scale (drop `/1000`, keep `/2000` for COM — the five queries
*disagree with each other* on scaling, which is itself evidence the divisors are implied-decimal decoding
rather than business maths); SBXUSSSAND SO-match semantics (the padded `pull_signal`/`reference_01` join);
`ABURAT×0.01` rate meaning; ⚠ **measure the first `MANUAL_OVERWRITE=True` batch build** — with no filters
the fact is the whole of F4211 ∪ F4311 (design §8.15) — then flip `MANUAL_OVERWRITE→False`. (Batch: no CDF
needed on F4211/F4311/F0005.) Address dims are **reused** (no notebook); the loading role
binds to the existing `rpt.dim_address_loading_port`.

> **Silver column names live in `eso5/full_metadata.txt` — look them up, don't transliterate the JDE
> alias.** The map is not literal: SDITWT is `amount_unit_weight`, SDSRP1 is `sales_reporting_code_01`,
> PDUOM/SDUOM are `uom_as_input`, QCLGL1/2/3 are `descriptn_01/02/03`, QCFSTR3 is `future_use_string_03`.

## Status log
- **2026-07-10** — Workspace created; requirements analyzed (docx + full_metadata + hubble query);
  Gold fact + dim notebooks, semantic model + report, and design/mapping/measures docs generated.
- **2026-07-13** — **F0005 removed from the fact.** The fact notebook no longer reads Silver F0005; its
  55/UP attributes are served by `dim_uss_plant` (joined in the semantic model), and the fact reads only
  `lofa_mcu` from that **Gold dim** for the SOORDERNO match. Dim helpers realigned to ESO4's
  `nb_eso4_gold_dim_udc.py` structure.
- **2026-07-14** — **One fact for all five reports.** Folded the four `Filter Capture/` variations into
  `fact_extended_sales_order_5` instead of building separate facts. The row population widened to their union
  (the `<>TL` / `<>HOLADD` predicates became the `row_class` tag rather than WHERE clauses; orphan F4311 OX
  HOLADD PO lines added as new rows), F4201 + four F554201T columns added for the reconciliation report,
  and its per-load pivots implemented as 19 DAX measures over the same line-grain rows. **F4311 is now a
  streamed source** (it contributes rows, not just columns) ⇒ enable CDF on it. Silver names for the
  variation columns corrected from `full_metadata.txt` after the first run flagged them
  (SDITWT/SDSRP1/PDUOM/QCLGL1-3/QCFSTR3 — the JDE→snake_case map is not literal).
- **2026-07-14 (later)** — **All row filters removed from the notebook.** `SDDCTO='SX'` and
  `SDKCOO='00750'` dropped from the base population and carried as the `document_type` / `company` columns
  instead, joining `row_class` as report-side filters. **The fact is now the whole of F4211** — watch the
  seed volume (design §8.15).
- **2026-07-14 (final)** — **ZERO `.where()` on any source table** (user: *"All filters will be handled in
  the Power BI report… The notebook should perform only the required joins, calculations, transformations,
  and business logic"*). The derivation predicates (`SDLITM='BOL'`, `PDDCTO='OX'`, `SDCO='00400'`,
  `SDPRP1='COM'`, the status tests…) were not deleted — deleting them makes the columns *wrong*, not
  unfiltered — they were **relocated** into CASEs inside aggregates and into JOIN conditions, the only two
  places that cannot drop a row. Values are unchanged; an SX/00750 row reads exactly what Hubble's
  correlated subqueries returned, because the order type and company are now *group keys* rather than
  hard-coded constants. `_load_aggregates()` folds every per-load F4211 subquery into **one pass** (was
  four scans). The lone exception is leg B's `PDDCTO='OX' AND PDLITM='HOLADD'`, which *defines* what report
  4's UNION branch contributes — gated by **`PO_LEG_UNFILTERED`** (§3d, §8.17).
- **2026-07-14 → 2026-07-26 (District dimension)** — the fact's `district` column is **unchanged** (raw
  code — FK / grain / filter — shown as-is, matching Hubble). The District is dimensioned through the
  **reused conformed `rpt.dim_plant`** (F0006; `district → dim_plant.plant_code`; name via `plant_name`);
  6th relationship. **Migrated 2026-07-26** from ESO4's `rpt.dim_business_unit` (that builder was retired,
  ESO5 being its last consumer). `dim_plant` has no `"code - description"` concat, but that was never
  placed on a visual, so no report output changed — for `771010 - SBX TRANS DISTRICT-WEST TEXAS` as one
  field, show `district` + `plant_name` as two columns. **Line ID** shows the raw JDE value: `1.00` →
  **`1000`** (`round(line_number * 1000)`, int64). Lossless — a kit line `1.010` becomes `1010`.
- **2026-07-14 (`PO_LEG_UNFILTERED = True`)** — that last predicate removed too. Leg B now takes the
  **whole of F4311**; `PDDCTO='OX' AND PDLITM='HOLADD'` became the `row_class` calculation
  (`PO_HOLADD` vs the never-read `PO_OTHER`). **The notebook now contains no row-selecting predicate
  anywhere.** Fact = **F4211 ∪ F4311 in full** — ⚠ measure the seed (design §4, §8.17). **Next:** run
  `nb_eso5_gold_dim_uss_plant` then `nb_eso5_gold_fact_extended_sales_order_5` in Fabric (`OVERWRITE=True` once,
  then `False`), and set the report filters — **`row_class` is mandatory on every page**.
- **2026-07-15 (Re-apply the SHARED filters)** — reversed course: apply the query filters **except** the
  date range (all five) and Reconciliation's `SDDOCO='22815083'` + `SDLTTR<>'980'`. Because the five WHERE
  clauses conflict, only the **two filters shared by all five** land on the F4211 leg — `SDDCTO='SX' AND
  SDKCOO='00750'` (in `_f4211_lines`) — plus the F0101 rate-source `ABAT1` range (a LEFT-join qualifier,
  nulls the rate, drops no row). User confirmed: **keep TL/HOLADD as `row_class`** (not filters) and **keep
  the whole F4311** (`PO_LEG_UNFILTERED` stays `True`). Fact = **F4211(SX/00750) ∪ F4311 in full** — smaller
  F4211 seed than before. The dropped-`ABAT1` open item is now moot (design §8.18). `row_class` +
  `document_type` + `company` page filters still **mandatory** (they exclude the PO rows).
- **2026-07-20 (v2.0 — rename + ALL filters removed again)** — reversed 2026-07-15 for good, per direction:
  *"Do not apply any filters during Gold-layer processing. Filtering should be implemented only in the Power
  BI report."*
  - **Fact renamed `fact_sandbox_load_po` → `fact_extended_sales_order_5`** everywhere: the notebook (file
    included → `nb_eso5_gold_fact_extended_sales_order_5.py`), the TMDL table + partition + all 27 measures
    + 6 relationships + `model.tmdl`, all 10 PBIR visuals, and every doc. 0 residual references.
  - **`SDDCTO='SX'` and `SDKCOO='00750'` dropped from `_f4211_lines`** — carried by the existing
    `document_type` / `company` columns instead. The fact is now **the whole of F4211 ∪ the whole of F4311**.
  - **F0101 `ABAT1` band relocated, not deleted** — it defines which address rows *count as a rate source*,
    so it moved out of a `WHERE` and into a **CASE inside the `lofa` aggregate**. `lofa_rate` values are
    unchanged and no row is dropped. Deleting it would have made the rate **wrong**, not unfiltered.
  - **The notebook now holds zero row-selecting predicates.** The only `.where()` / `.filter()` calls left
    are the universal exclusions the Gold rule permits: Silver `is_delete = 0`, and an invalid-key guard on
    the `dim_uss_plant` lookup.
  - **Why this is safe:** the notebook keys every load-level lookup on **`kcoo + doco + dcto`**, where
    Hubble keyed on `doco + dcto` alone and leaned on a global `00750` filter to stay inside one company.
    Company is part of the *key* here, so an unfiltered base cannot leak across companies (design §3a).
  - ⚠ **Reconciliation report correction** — Hubble's `SBXLOADDETAIL` has **no company predicate** at all
    (its entire base WHERE is `SHDCTO='SX'`). Now that Gold no longer forces `00750`, that report should
    **omit** the `company` page filter to reconcile exactly (design §8.12).
  - Verified: both notebooks parse; the fact stores **58 business columns + 2 keys = 60**, matching the
    TMDL's 60 `sourceColumn` entries exactly — no drift in either direction.
- **2026-07-26 (schema `jde` · STREAMING→BATCH · ESO4 conventions)** — three notebook changes, none of which
  change report results: (1) Silver schema `jde_cdc`→`jde` (variable `SRC_SCHEMA`→`SILVER_SCHEMA`); (2) District
  dimensioned through the reused `rpt.dim_plant` (migrated from ESO4's retired `rpt.dim_business_unit`, its last
  consumer — the visuals only ever showed the raw `fact[district]` code, so nothing rendered differently); (3)
  **both notebooks converted from CDF-streaming to plain BATCH builds** (read full snapshot → `build_*()` →
  overwrite, gated by `MANUAL_OVERWRITE`; removed streams/handlers/checkpoints/`init_ver`/`awaitAnyTermination`
  and the Gold CDF write option — no CDF needed on `jde`), then all CONFIG/RUN conventions aligned to ESO4
  (`SILVER_LH`/`GOLD_LH` + `gname()`, bare table-name constants, `build_fact`/`build_dim_uss_plant`, inline write,
  `# 1) CONFIG / 2) …BUILDER / 3–4) …` sections). Build transforms are AST-proven byte-identical; the design doc,
  hubble-field-mapping, and this README were updated to batch.
