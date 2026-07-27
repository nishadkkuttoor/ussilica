# Extended Sales Order 4 — Gold Layer Design (Sales Tax with Business Stream / Avalara Reconciliation)

**Report:** Extended Sales Order 4 · **Stakeholder:** Lisa Covington · **Audience:** Tax Department
**Purpose:** Sales Order + AR + Avalara detail to facilitate **Avalara reconciliation**
(mirrors Hubble report *TX002_Sales Tax with Business Stream Summary — Reconciliation Version*).
**Platform:** Microsoft Fabric (F64) · **Lakehouse:** `lh_jde_gold` · **Schema:** `rpt`
**Sources:** `lh_jde_silver.jde.*` (already decoded — Julian→date, implied decimals resolved, snake_case; `SILVER_SCHEMA = "jde"` — 2026-07-26, was `jde_cdc`)
**Date:** 2026-07-08 · **Version:** 1.4 (2026-07-26 — dim_business_unit replaced by reused dim_plant)

> **v1.4 changelog (2026-07-26)** — **ESO4's `plant` relationship moved from `dim_business_unit` to the reused
> `rpt.dim_plant`.** ESO4's `dim_business_unit.tmdl` was **removed from the model** and the `plant` FK now relates to
> the existing `rpt.dim_plant` (key `plant_code` = MCMCU) — the same F0006 master, already built by another job, so
> ESO4 no longer builds a business-unit dim of its own. The report's **Plant Name** now resolves from
> `dim_plant.plant_name` (identical values — same F0006 MCDL01 source); the plant's business-stream code (MCRP20) is
> available on `dim_plant.category_code_cost_ct_020`. **No fact / results change** — the fact still carries the same
> `plant` FK code and still reads F0006 for the `business_stream` calc; only the dimension it points at changed.
> ESO4's own pipeline is now **two** notebooks (fact + `dim_udc`) reusing `dim_plant` + `dim_address_book`.
> **`nb_eso4_gold_dim_business_unit.py` and its `dim_business_unit.tmdl` were DELETED** — ESO5 (its last
> consumer) was migrated to `rpt.dim_plant` the same day, so nothing builds or reads `rpt.dim_business_unit`
> any more. (The `rpt.dim_business_unit` Delta table, if it exists on Fabric from a prior run, is now orphaned
> and can be dropped.)

> **v1.3 changelog (2026-07-25)** — **Notebook structure aligned to `nb_silver_to_gold_eso7_v2_fact.py`** (structure
> ONLY — every transform body and all results unchanged). The three notebooks now use the ESO7 skeleton: a banner
> docstring, numbered `# 1) CONFIG / 2) …BUILDER / 3) … / 4) RUN` sections, split `SILVER_LH`/`SILVER_SCHEMA`/`GOLD_LH`/
> `GOLD_SCHEMA` + `sname()`/`gname()` helpers, `build_fact()` / `build_dim*()` builders (was `transform_*`),
> `load_silver_table` via `spark.read.table`, and a `# RUN` block ending in a JSON exit payload
> (`notebookutils.notebook.exit`). **`OVERWRITE` renamed to `MANUAL_OVERWRITE`** (same semantics). The fact notebook
> gains a declarative **`FACT_SOURCES`** list (each Silver source + its `join_pairs` to the F4211 spine + join type),
> wired to a source-existence preflight. A `# 2) FACT BUILDER` note explains why the `FACT_*` column lists exist in
> ESO4 (pre-aggregated GROUP BY grain) but not in ESO7 (atomic line grain — see Finding 1). **No CDF is re-added** —
> ESO4 stays a plain snapshot batch build (ESO7 v2 itself uses CDF `availableNow`). The **gate-review docx was updated
> to Revision 4** to match: `transform_fact`→`build_fact` / `OVERWRITE`→`MANUAL_OVERWRITE` name citations, the key
> evidence anchors (grain `FACT_GROUP_BY_COLS`, address collapse + ABAT1 band, Business Stream CASE, inferred-value
> confirm-list) re-cited to the current files; the streaming-machinery citations kept as-reviewed (that code no longer
> exists). No finding status changed — Finding 1 still open, Finding 5 closed (Rev 3).

> **v1.2 changelog (2026-07-25)** — **Streaming removed; the three processors are now BATCH builds.** No Change
> Data Feed, `foreachBatch`, checkpoints, `init_ver`, or `awaitAnyTermination` — each notebook reads the full Silver
> snapshot, runs its (unchanged) transform, and overwrite-writes its Gold table(s). The **`OVERWRITE`** switch is
> kept: `True` drops + rebuilds; `False` builds only if the target is missing. **Results are identical** to the
> previous version's `OVERWRITE=True` full load (transforms unchanged; only the incremental-refresh scaffolding was
> removed). Re-run / schedule the notebooks to refresh. See §1.

> **v1.1 changelog (2026-07-25)** — **Hubble `ABAT1` search-type band applied** to the F0101 ⋈ F0116 address
> lookup as a value-qualification (not a fact-row filter), mirroring ESO5. **Closes gate-review Finding 5** — ESO4
> now matches Hubble on `business_stream`/`jurisdiction`/`county`; out-of-band ship-tos blank via the LEFT join, no
> F4211 row dropped. Single edit in `build_fact` (the one build path). See §2 and §5.
> ⚠ Needs a `MANUAL_OVERWRITE=True` reload to re-materialize the affected attribute values. (v1.0 = 2026-07-08 initial build.)

> Built with the same transform structure and naming as ESO1, but as **batch builds** (streaming
> removed in v1.2). **All docx §4 joins are applied; NO docx §5 filters are applied** (filters are
> report slicers, denormalized onto the fact instead).

---

## 1. Architecture

```
 lh_jde_silver.jde.*  (Delta — full-snapshot reads)
   F4211  F03B11  F0006  F0101  F0116                    F0005  (dim_udc only)
        │
        ▼
 TWO self-contained processors (independent Fabric jobs — BATCH build, gated by MANUAL_OVERWRITE):
   ┌─ nb_eso4_gold_fact_sales_tax_reconciliation ─ reads F4211 + F03B11 (+ F0006/F0101/F0116) → fact
   │     · STAR SCHEMA — stores FK codes only; F0005 is NOT read here · own MANUAL_OVERWRITE switch · FACT_SOURCES preflight
   └─ nb_eso4_gold_dim_udc ─────────── reads F0005 → dim_sic (01/SC) + dim_state (00/S)

 (No date dimension — dates are sliced directly off the fact's gl_date / service_tax_date columns.)

 REUSED (built by existing jobs — NOT rebuilt for ESO4):
   • lh_jde_gold.rpt.dim_plant (F0006 business-unit/plant master; key plant_code = MCMCU) — supplies the
     plant relationship (plant_name, business-stream code category_code_cost_ct_020). ESO4 no longer builds
     a business-unit dim; F0006 is still read by the FACT for the business_stream calc.
   • lh_jde_gold.rpt.dim_address_book (F0101 ⋈ F0116 latest-effective; role views rpt.dim_address_ship_to /
     _sold_to / _parent) — supplies the ship_to / sold_to / parent_number address relationships.
        ▼
 lh_jde_gold.rpt.* (fact + dim_sic + dim_state + reused dim_plant + reused dim_address_book)
        →  Direct Lake semantic model  sales_tax_reconciliation.SemanticModel  (7 tables, 6 relationships)
```

Each processor is a **batch build** (streaming removed v1.2; ESO7 structure v1.3, 2026-07-25): it reads
the full Silver snapshot, runs its `build_fact()` / `build_dim*()` transform, and **overwrite-writes**
its Gold table(s). The **`MANUAL_OVERWRITE`** switch (top of each notebook) controls reprocessing —
`True` drops + rebuilds; `False` builds only if the target is missing and otherwise leaves it untouched.
There is **no Change Data Feed, no checkpoints, no `init_ver`, no `foreachBatch`/`awaitAnyTermination`**,
and no audit columns are stored on any Gold table. Each notebook follows the ESO7 skeleton
(`# 1) CONFIG / 2) BUILDER / 3) … / 4) RUN`, `sname()`/`gname()`, JSON exit payload); the fact notebook
adds a declarative `FACT_SOURCES` list + a source-existence preflight. Re-run a notebook (or schedule it)
whenever the Silver data should be re-materialized. Results are identical to the previous streaming
version's full `MANUAL_OVERWRITE=True` load — the transforms are
unchanged; only the incremental-refresh scaffolding was removed.

---

## 2. Sources & joins (docx §4 — every join applied, NO fact-row filters; ABAT1 address-band excepted — see §5)

| From | To | Type | From col (JDE / snake) | To col (JDE / snake) |
|---|---|---|---|---|
| F4211 | F03B11 | **INNER** | SDDOC `doc_voucher_invoice_e` | RPODOC `original_document_no` |
| F4211 | F03B11 | **INNER** | SDDCT `document_type` | RPODCT `original_document_type` |
| F4211 | F03B11 | **INNER** | SDKCO `company_key` | RPOKCO `company_key_original` |
| F4211 | F03B11 | **INNER** | SDLNID `line_number` | RPLNID `line_number` |
| F4211 | F0006 | LEFT | SDMCU `cost_center` | MCMCU `cost_center` |
| F4211 | F0101 | LEFT | SDSHAN `address_number_ship_to` | ABAN8 `address_number` |
| F0101 | F0116 | **INNER** | ABAN8 `address_number` | ALAN8 `address_number` |

- The `F4211 LEFT (F0101 INNER F0116)` nesting is preserved: F0101⋈F0116 is joined **first**, then
  LEFT-joined to F4211 (so a missing address never drops an F4211 line).
- **The F0101 side carries Hubble's `ABAT1` search-type band** (`A  `..`P  ` / `R  `..`ZZZ`, excludes `Q`) as a
  **value-qualification on the address lookup** — an out-of-band ship-to keeps its F4211 line but gets NULL
  `sic_code`/`jurisdiction`/`county` (mirrors Hubble's WHERE-filtered LEFT-joined subquery; no fact row dropped).
  Full rationale + closure of gate-review Finding 5 in **§5**. Also latest-effective on F0116 (`row_number()` over
  `date_beginning_effective` desc) so a moved address doesn't fan the grain out.
- **F0005 UDC descriptions are modeled as DIMENSIONS, not fact joins (star schema).** The fact stores the
  raw FK codes `sic_code` (ABSIC) and `jurisdiction` (ALADDS); `nb_eso4_gold_dim_udc.py` builds two F0005
  reference dims that the model relates to:
  - **`dim_sic`** — F0005 `01/SC` (`user_defined_code`→`sic_code`, `description_001`→`sic_description`);
    fact relationship `sic_code → dim_sic.sic_code`.
  - **`dim_state`** — F0005 `00/S` (`user_defined_code`→`state_code`, `description_001`→`state_name`);
    fact relationship `jurisdiction → dim_state.state_code` (resolves `CO`→`Colorado`).
  - The UDC system/types (`01/SC`, `00/S`) are **inferred** (F0005 is not in the docx §4 joins — see §5).
- **Hubble-only join dropped:** `F03B11 → company ShiftFactor` (a Hubble `dwtemp…` temp table). It
  supplied `NVL(ShiftFactor, 0.01)` to de-scale RAW JDE integer amounts. Our Silver is already
  decoded, so it is represented by the constant `SHIFT_FACTOR = 1.0` (same as ESO1); the fact
  carries `shift_factor_applied` for lineage.
- **Silver table names assumed** (verify against the actual lakehouse): `f4211_sales_order_detail_file`,
  `f03b11_customer_ledger`, `f0006_business_unit_master`, `f0101_address_book_master`,
  `f0116_address_by_date`, `f0005_user_defined_code_values`.

---

## 3. Grain & keys

- **Grain:** one row per **Hubble `GROUP BY` tuple** (`hubble query.txt`) — the report display columns.
  Hubble's outer query SUMs the four amounts across F03B11 pay items; its inner `SELECT DISTINCT`
  carries the F03B11 PK (RPDOC/RPDCT/RPKCO/RPSFX) only so each pay item is counted once, then the
  outer `GROUP BY` (which excludes the PK) collapses pay items sharing a display tuple into one summed
  row. The fact reproduces this: `sel.distinct()` (with the PK) = inner DISTINCT →
  `groupBy(FACT_GROUP_BY_COLS).agg(sum(amounts), first(shift_factor))`.
- `sales_tax_line_key` — unique per fact row = `sha2` of the 18 GROUP BY columns (document_company,
  invoice_number, document_type, order_number, order_type, plant, ship_to, sold_to, parent_number,
  tax_explanation_code, tax_area, avalara_code, business_stream, sic_code, jurisdiction, county,
  gl_date, service_tax_date); the fact `dropDuplicates` on it (defensive — the GROUP BY already makes
  it unique). **The F03B11 PK is no longer stored** (summed away); **the plant business-stream code (MCRP20)
  was dropped from the grain** (functionally dependent on `plant` → dim_plant — see §5 star schema).
- `document_scope_key` = `sha2(document_company ‖ document_type ‖ invoice_number)` — a coarser
  invoice-document key still **stored on the fact** (hidden in the model). It was the CDC delete-scope in
  the pre-v1.2 streaming version; under the batch build it is **vestigial** (retained so the fact schema
  and TMDL are unchanged — see v1.2 changelog). It costs nothing and can be dropped later if a schema
  change is made (would also require removing it from the fact TMDL).

---

## 4. Fact `fact_sales_tax_reconciliation` — column mapping (docx §6)

| Report column (§6 heading) | Fact column | Source | Notes |
|---|---|---|---|
| Document Company | `document_company` | F4211 SDKCO | |
| Invoice Number | `invoice_number` | F4211 SDDOC | |
| Document Type | `document_type` | F4211 SDDCT | |
| Order Number | `order_number` | F4211 SDDOCO | |
| Order Type | `order_type` | F4211 SDDCTO | |
| Plant | `plant` | F4211 SDMCU | **FK → reused `dim_plant`** (key `plant_code`) |
| Plant Name | *(dim_plant)* | F0006 MCDL01 | **in the dim, not on the fact** — resolved via `plant` FK |
| Business Stream | `business_stream` | **calc §7** | ABSIC × MCRP20 (fact calc; degenerate) |
| Business Stream (raw) | *(dim_plant)* | F0006 MCRP20 | `category_code_cost_ct_020` — **in the dim, not on the fact** |
| Tax Explanation Code | `tax_explanation_code` | F4211 SDEXR1 | |
| Tax Area | `tax_area` | F03B11 RPTXA1 | |
| GL Date | `gl_date` | F4211 SDDGL | `dateTime`; sliced directly off the fact |
| Service/Tax Date | `service_tax_date` | F03B11 RPDSVJ | `dateTime`; sliced directly off the fact |
| Taxable Amount | `taxable_amount` | F03B11 RPATXA | × SHIFT_FACTOR |
| Non-Taxable Amount | `non_taxable_amount` | F03B11 RPATXN | × SHIFT_FACTOR |
| Tax Amount | `tax_amount` | F03B11 RPSTAM | × SHIFT_FACTOR |
| Gross Amount | `gross_amount` | F03B11 RPAG | × SHIFT_FACTOR |
| Avalara Code | `avalara_code` | **calc (inferred)** | see §5 |
| Jurisdiction | `jurisdiction` | F0116 ALADDS | **FK → `dim_state`** (raw code `CO`); state name in the dim — **inferred UDC, see §5** |
| County | `county` | F0116 ALCOUN | degenerate (not in the reused address dim) |
| Ship To | `ship_to` | F4211 SDSHAN | **FK → `dim_address_ship_to`** — **see swap note §5** |
| Sold To | `sold_to` | F4211 SDAN8 | **FK → `dim_address_sold_to`** — **see swap note §5** |
| Parent Number | `parent_number` | F4211 SDPA8 | **FK → `dim_address_parent`** |
| SIC Code | `sic_code` | F0101 ABSIC | **FK → `dim_sic`** |
| SIC Description | *(dim_sic)* | F0005 DRDL01 | **in the dim, not on the fact** — resolved via `sic_code` FK — **inferred UDC, see §5** |

Plus hidden keys `sales_tax_line_key`, `document_scope_key`, and `shift_factor_applied`. The F03B11 PK
(`RPDOC/RPDCT/RPKCO/RPSFX`) is used inside `build_fact` for the inner DISTINCT only and is **not
stored** on the fact (Hubble's `GROUP BY` excludes it; the amounts are summed across pay items).

---

## 5. Calculations & decisions

**Business Stream (docx §7 — implemented verbatim):**
```
ABSIC='F'  AND MCRP20='ENG'          → 'O&G'
ABSIC<>'F' AND MCRP20='ENG'          → 'ISP'
ABSIC<>'F' AND MCRP20='SHR'          → 'ISP'
ABSIC='F'  AND MCRP20='SHR'          → 'O&G'
MCRP20 NOT IN ('ENG','SHR')          → 'ISP'
```
Computed on the fact (needs `ABSIC` from F0101 and `MCRP20` from F0006, both joined). Values trimmed.

**Avalara Code — ⚠ INFERRED (docx §6 says "Calculation — See Below" but §7 never defines it).**
Implemented from the Hubble custom column `XID_CUSTOM_8501fecff7aa51` =
`RTRIM(LTRIM(NVL(SDDOC,-999999999))) || RTRIM(LTRIM(NVL(SDDCT,''))) || RTRIM(LTRIM(NVL(SDKCO,'')))`
→ `concat(invoice_number, document_type, document_company)`. **Confirm with the stakeholder.** (A
second Hubble composite `SDDOC || SDDCT` also exists if the shorter key is intended.)

**Ship To / Sold To — ⚠ label swap kept per docx.** docx §6 maps **Ship To ← SDAN8** and
**Sold To ← SDSHAN**, which is the reverse of the usual JDE meaning (SDSHAN = ship-to). The design
follows the docx literally. The join `F4211.SDSHAN = F0101.ABAN8` still drives the address lookup,
so `sold_to` (SDSHAN) is the address resolved to Jurisdiction/County/SIC; `ship_to` (SDAN8) and
`parent_number` (SDPA8) are role-played to the same address dim. **Confirm intended labels.**

**Star schema (full dimensional model — adopted per user request).** The fact stores **FK codes +
degenerate dims + measures only**; every resolvable description lives in a dimension and is surfaced on
visuals through a relationship:
- `plant` → reused `rpt.dim_plant` (key `plant_code`; plant_name, business-stream code
  `category_code_cost_ct_020`) — F0006. ESO4 does **not** build this dim.
- `sic_code` → `dim_sic`, `jurisdiction` → `dim_state` — F0005 UDC reference dims (see below).
- `ship_to` / `sold_to` / `parent_number` → reused `rpt.dim_address_book` role views — F0101⋈F0116.
Two things **cannot** move into a dim and stay on the fact: **`county`** (absent from the reused address
dim, which we don't own) and **`business_stream`** (a cross-table calc). The plant business-stream code
(MCRP20) was dropped from the fact grain (functionally dependent on `plant`); `plant_name` /
`sic_description` / state name are no longer denormalized on the fact.

**SIC Description → `dim_sic` (F0005 01/SC)** (built by `nb_eso4_gold_dim_udc.py`) — `user_defined_code`
→ `sic_code`, `description_001` → `sic_description`; fact relationship `sic_code → dim_sic.sic_code`.
Same 40/AT lookup shape ESO7 uses for `dim_status`. **The UDC system/type (`01/SC`) is INFERRED** —
F0005 is not in the docx §4 joins / Hubble SQL, so it is an assumption to confirm.

**Jurisdiction state name → `dim_state` (F0005 00/S)** (built by `nb_eso4_gold_dim_udc.py`) — Hubble
displays the full state name (`Colorado`), not the raw `ALADDS` code (`CO`). `user_defined_code` →
`state_code`, `description_001` → `state_name`; fact relationship `jurisdiction → dim_state.state_code`
resolves the name. **The UDC system/type (`00/S`) is INFERRED** — confirm.

**ShiftFactor = 1.0** — see §2. **No fact-row filters** — every docx §5 filter (Parent/Sold To/Ship
To/Company/Business Stream/Plant/GL Date range/Service-Tax Date/Invoice#/Order#/Tax Code/Tax
Area/Jurisdiction) and the Hubble `SDDGL BETWEEN` are **not** applied.

**⚠ EXCEPTION — Hubble `ABAT1` search-type band IS applied (2026-07-25)** as a **value-qualification on the
F0101 ⋈ F0116 address lookup only** (not a fact-row filter), mirroring the ESO5 pattern: `WHERE (ABAT1 BETWEEN
'A  ' AND 'P  ') OR (ABAT1 BETWEEN 'R  ' AND 'ZZZ')` (excludes the `Q` band). Because the address lookup is
**LEFT-joined** to F4211, an out-of-band ship-to keeps its fact row and simply gets NULL
`sic_code`/`jurisdiction`/`county` — exactly as Hubble's LEFT-join-of-a-WHERE-filtered-subquery. This **closes
gate-review Finding 5** (ESO4 now matches Hubble on `business_stream`/`jurisdiction`/`county`); no F4211 line is
dropped, so the Gold "no business filters" rule holds. ⚠ Needs a `MANUAL_OVERWRITE=True` reload to re-materialize the
affected attribute values.

---

## 6. Dimensions

All dims live in `lh_jde_gold.rpt` (per the notebooks' `GOLD_SCHEMA`).

| Dim (Gold table) | Source | Key | Attributes | Built by |
|---|---|---|---|---|
| **`rpt.dim_plant`** | F0006 | `plant_code` (MCMCU) | plant_name (MCDL01), category_code_cost_ct_020 (MCRP20 business-stream code), company (MCCO), state (MCADDS), plant_category_code_02, related_business_unit, parent_plant_code | **REUSED** — existing `dim_plant` job (**not** rebuilt for ESO4) |
| `dim_sic` | F0005 `01/SC` | `sic_code` (DRKY) | sic_description (DRDL01) | `nb_eso4_gold_dim_udc` (batch) |
| `dim_state` | F0005 `00/S` | `state_code` (DRKY) | state_name (DRDL01) | `nb_eso4_gold_dim_udc` (batch) |
| **`rpt.dim_address_book`** (role views `rpt.dim_address_ship_to` / `_sold_to` / `_parent`) | F0101 ⋈ F0116 latest-effective | `address_number` (ABAN8) | name_alpha (ABALPH), address_type_01 (ABAT1), standard_industry_code (ABSIC), city/state/country/zip, address_line_01–04, has_postal_address | **REUSED** — existing job `nb_dim_address_book` (**not** rebuilt for ESO4) |

`dim_sic` + `dim_state` are two Type-1 reference dims built from **one** F0005 source in
`nb_eso4_gold_dim_udc` (split by UDC system/type in each transform), mirroring ESO7's `dim_status`.

**No date dimension.** ESO4 has no `dim_date`. The fact carries `gl_date` and `service_tax_date` as
native `dateTime` columns, sliced/filtered directly in the report (Power BI's built-in date hierarchy
covers year/quarter/month). The former `gl_date_key` / `service_tax_date_key` FK columns were dropped.

**dim_address is REUSED, not rebuilt** — the existing `lh_jde_gold.rpt.dim_address_book` (one row per
`address_number`, F0101 ⋈ F0116 latest-effective) already serves every address lookup across projects.
Role views give each of the fact's `ship_to` / `sold_to` / `parent_number` FKs an independent **active**
relationship (Power BI disallows >1 active relationship between the same table pair): `rpt.dim_address_ship_to`
and `rpt.dim_address_sold_to` **already exist**; **`rpt.dim_address_parent` must be created once** (see §8).
Note the reused dim has **no County** column — `county` (ALCOUN) stays denormalized on the **fact** (no
dim to hold it). `sic_code` (ABSIC) and `jurisdiction` (ALADDS) are stored on the fact as **FK codes**
that relate to `dim_sic` / `dim_state` for their descriptions (F0101/F0116 are read only to source those
codes + county — the descriptions come from F0005 via the dims, not denormalized).

---

## 7. Semantic model — `sales_tax_reconciliation.SemanticModel` (Direct Lake)

**Tables (7):** `fact_sales_tax_reconciliation`, `dim_plant`, `dim_sic`, `dim_state`,
`dim_address_ship_to`, `dim_address_sold_to`, `dim_address_parent` — **all `schemaName: rpt`** (the
notebooks write to `lh_jde_gold.rpt`; `dim_plant` and the address role views are reused Gold dims —
`rpt.dim_plant` and the role views over `rpt.dim_address_book`). **No `dim_date`.**

**Relationships** (fact = many side, 6 total, all active): `plant→dim_plant.plant_code`;
`sic_code→dim_sic.sic_code`; `jurisdiction→dim_state.state_code`; `ship_to/sold_to/parent_number →
dim_address_*.address_number`. Dates are not modeled through a dimension — `gl_date` / `service_tax_date`
are sliced directly off the fact.

**Measures** (`Tax` / `Counts` folders): Taxable Amount, Non-Taxable Amount, Tax Amount, Gross
Amount (SUM); Effective Tax Rate = `DIVIDE([Tax Amount],[Taxable Amount])`; Invoice Count =
`DISTINCTCOUNT(avalara_code)`; Tax Lines = `COUNTROWS`.

**Calculated columns: NONE.** Direct Lake tables cannot carry DAX calculated columns. `tax_status`
(`"Taxable"` when the summed `tax_amount > 0`, else `"Non-Taxable"`) is a **physical** Gold column
materialised by the fact notebook *after* the GROUP BY — computed pre-aggregation it would derive from
a single F03B11 pay item and would have to join the grain. (Was a DAX calculated column until
2026-07-17; corrected per Gate 1 review finding 2. Same Direct Lake constraint bars calculated
columns model-wide.)

---

## 8. Open items / to verify
1. **Avalara Code** definition (inferred) — confirm the composite.
2. **Ship To / Sold To** labels (docx swap) — confirm.
3. **Silver table names** for F03B11 / F0006 / F0005 — confirm exact lakehouse names
   (`f0005_user_defined_code_values` assumed from the ESO7 notebook).
4. ~~CDF enabled at/before `init_ver` on every streamed source~~ — **N/A since v1.2** (batch build; no CDF/streaming). Sources are read as full snapshots.
5. **`MANUAL_OVERWRITE=True` → `False`** in both ESO4 notebooks (fact, **`dim_udc`**) after
   the first healthy full-load run. (`dim_plant` is reused — not built or reloaded by ESO4.)
6. **Create the `rpt.dim_address_parent` role view** (one-time, alongside the existing ship_to/sold_to):
   `CREATE OR ALTER VIEW rpt.dim_address_parent AS SELECT * FROM rpt.dim_address_book;`
   (ship_to / sold_to views already exist per `nb_dim_address_book`.)
7. **F0005 UDC dims** — `dim_sic` (`01/SC`) + `dim_state` (`00/S`), both with **inferred** system/types —
   confirm with the Tax team. Fact relates `sic_code → dim_sic`, `jurisdiction → dim_state`.
8. **Gold schema = `lh_jde_gold.rpt`** (per the notebooks' `GOLD_SCHEMA`). All TMDL entity partitions
   that ESO4 owns (fact, dim_sic, dim_state) use `schemaName: rpt`; the reused `dim_plant` /
   `dim_address_*` partitions also point at `rpt`.
