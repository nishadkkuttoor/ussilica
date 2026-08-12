# ESO1 — Master Reference

**Extended Sales Order 1 · US Silica · JDE on Microsoft Fabric**
**Status as of 2026-07-25** · Design **v2.15** (+v2.15.1 addendum: `dim_category_code_10` — ABAC10→description, UDC 01/10; commission fact F4211-driven; NO date dimension) · Code-complete, **not yet run on Fabric** (validate before trusting any number).

This is the single entry-point for the ESO1 work. It consolidates the architecture, the two-fact decision and
its rationale, the query-to-notebook gap analysis (all structural gaps closed), every notebook and deliverable,
the open items, and the key gotchas. Detailed per-topic docs are linked throughout.

---

## 1. What ESO1 is

The **Billable v Payable Freight** report ("BvP Combined" Hubble query) plus **107 Filter Capture variations** —
legacy Hubble/JDE reports that all read the same JDE **F4211 sales-order line** and differ almost entirely in
their WHERE clause. The goal: serve them all from a small Gold star schema on Fabric, with **all report filters
applied at the Power BI page level** (Gold applies none).

- **107 variations** + the master BvP query + **SOP0027 Commission** = 109 reports analyzed.
- **Coverage today:** 99 fully served · 8 with an H1 $-reconciliation pending · 1 (commission) on its own fact.

---

## 2. Architecture — TWO facts, one grain each

```
lh_jde_silver.jde.*  (Delta — read as static batch snapshots; 26 tables in full_metadata.json)
        │  batch full-snapshot read + static lookups → build_*()
        ▼
lh_jde_gold.rpt
  ├─ fact_sales_order_freight   (order-LINE grain)          ← nb_eso1_gold_fact_sales_order_freight.py
  ├─ fact_sales_commission      (commission-LINE grain)     ← nb_eso1_gold_fact_sales_commission.py
  ├─ dim_item                   (F4101)                     ← nb_eso1_gold_dim_item.py
  ├─ dim_category_code_10        (F0005 UDC 01/10, ABAC10)   ← nb_eso1_gold_dim_category_code_10.py
  └─ (REUSED, owned by old_nb)  dim_address_book (+ ship_to/sold_to/carrier/parent/salesperson role views), dim_plant
        │
        ▼  Direct Lake semantic model → Power BI (all filtering page-level)
```

**Design principles**
- **One unfiltered fact per grain** + Power BI page slicers. No report WHERE filter lives in a notebook (only Silver
  `is_delete=0`, the CDC soft-delete).
- **Reuse conformed dims** (address book role views, plant); build only the genuinely new `dim_item`. **No date
  dimension** — dates are sliced directly from the fact's raw date columns (weekly grouping via `ship_year_week`).
- **Batch full-snapshot overwrite** Silver→Gold (no CDF / `foreachBatch` / streaming / 30 s trigger); each notebook reads
  the full Silver snapshot → `build_*()` once → plain overwrite, no audit columns.
- **Silver source schema = `lh_jde_silver.jde`** (`SILVER_SCHEMA = "jde"` in all notebooks — **schema→`jde` and
  variable `SRC_SCHEMA`→`SILVER_SCHEMA` on 2026-07-26; was `jde_cdc`**, same as ESO4/ESO5). Sources are read as static batch snapshots — no CDF required.
- **Silver is pre-decoded** (Julian→date, implied decimals resolved) → no scaling in Gold except genuine business math
  (÷2000 lb→ton). `shift_factor_applied = 1.0` is a lineage placeholder (see H1).

---

## 3. The facts

| Fact | Grain | Serves | Driver / key sources | Columns |
|---|---|---|---|---|
| **fact_sales_order_freight** | sales-order line (`KCOO+DCTO+DOCO+LNID`) | the 106 non-commission variations + master | F4211 ∪ F42119; F4201, F0101 ×3, F0116, F5642B01/11, F4101, F41002, F4941, F4981, **F4106, F5549002, F03012, F49211** (**F4074 removed v2.17** — price-adjustment logic moved to `fact_price_adjustment`) | line grain; **price-adjustment cols removed v2.17** (row-count-neutral) |
| **fact_price_adjustment** | F4074 adjustment (one row per adjustment) | SOP-family bucket reports (SOP0006/7/8/0025, SOP000x 577/580/620, BP Freight, CL National Accounts) | **Silver F4074 only** (line values pulled from the freight fact via the relationship / RELATED) | **9 cols; new 2026-08-12 (v2.17)** |
| **fact_sales_commission** | sales line × commission record (`KCOO+DCTO+DOCO+LNID+CMLN`, CMLN nullable) | SOP0027 Commission | **F4211∪F42119 driver** + **LEFT F42005** + F4201 + F0101 (flipped 2026-07-24) | **44 biz + 2 keys = 46 stored** |
| **dim_category_code_10** | UDC 01/10 (ABAC10 code → description) | SOP0027 Commission | F0005 (batch) | 2 cols; new 2026-07-25 |

All relate to the reused dims on natural keys (`fact_price_adjustment` relates **many:1 to `fact_sales_order_freight`** on `sales_order_line_key`, so every line dim is reachable, and the 11 bucket measures live on it); no surrogate keys; each build is a deterministic full-snapshot overwrite (rerun-safe). `fact_price_adjustment` is **self-contained** — it rebuilds the line context from Silver (same derivations as the freight fact), so the two facts have **no run-order dependency**.

### Why two facts (the grain boundary — decided 2026-07-22, kept)
You split a fact by **grain**, never by filter. The 106 sales-line reports differ only by WHERE → one fact. SOP0027's
measures come from **F42005**, whose grain is **finer than the line** (one line can carry several commission records,
per salesperson/rule). You cannot denormalize a *finer* grain onto a coarser one without either fanning the line out
(corrupting every other report's line/shipment totals) or aggregating away the salesperson/rate detail that *is* the
report. Contrast with freight (F4981), which is **coarser** than the line (shipment grain) → denormalized onto each
line + DAX `SUMX(VALUES(shipment_number),…)` dedup, so it stays *in* the one fact. **Coarser-than-line ⇒ same fact;
finer-than-line ⇒ its own fact.** Two facts sharing conformed dims is the correct model, not a compromise.

---

## 4. Gap analysis — ALL structural gaps CLOSED (only H1 remains)

Full detail: **`ESO1_Query_to_Notebook_Gap_Analysis.docx`** (regenerated) · **`ESO1_Notebook_Implementation_Verification.docx`**.

Every table/join/column/measure the 107 variations reference is now on the fact — **24→26 Silver tables confirmed in
`full_metadata.json`**, so all gaps became notebook-only adds. Closures:

| Gap (original) | Closed by |
|---|---|
| H2 freight total not exhaustive | `total_freight` (SUM all FHNAMT/shipment) + **Total Freight** DAX measure |
| M1 F4106 base-price NOT EXISTS | `has_effective_price` flag (F4106 `left_semi`, effective-window covered) |
| M2 SDZON | `zone_number` |
| M3 SDHOLD line-hold | `line_hold` (distinct from header `hold_orders_code`) |
| M4 F4941 RSMOT/RSNCTR | `is_ocean_route` + `route_container_count` (route aggregate) |
| M5 F5549002 weights | `gross_weight` / `catch_weight` / `max_weight` + DAX weight measures |
| M6 next-status range filters | `next_status_num` (physical INT copy) |
| deferred display (SDPSIG/SDVR02/03/SDVEND/SDASN/SDURCD/SDPROV/SDUSER/lot/serial/location/SDSRP5/SDRORN/SDTDAY) | 14 F4211 cols + `original_promised_date` (SHOPDJ) + `related_address_3` (ABAN83) |
| F4074 adj detail (ALAST/ALAPRP1/ALUPRC/ALGLC/ALBSDVAL/ALUOM/ALFVTR) | **moved to `fact_price_adjustment` v2.17** — `price_adjustment_type` / `adj_print_code` / `adj_unit_price` / `adj_gl_class` / `adj_based_on_value` / `adj_uom` / `adj_factor_value` (per-adjustment grain, no longer the single `row_number` pick on the freight fact) |
| extended ocean-booking (BA55VONO/LODP/OCCR/REF1-3/BADLPU) | `voyage_number` / `loading_port` / `ocean_carrier` / `booking_reference_1-3` / `date_latest_pickup` |
| F03012 AIAC05 / F49211 UDDEFF | `sold_to_lob_category_05` / `deferred_entries_flag` |
| SOP0027 Commission (was OUT OF SCOPE) | **fact_sales_commission** (second fact) |

**Covered by an equivalent already on the fact (no column added):** F0010.CCCRCD → `currency_code` (SDBCRC);
F0010 fiscal (CCPNC/CCARFJ/CCDFF) → a Power BI fiscal calendar; header fields SHVR01/SHCARS/SHPA8/SHAN8/SHPTC/SHTRDJ →
their line-level equivalents; F0005 salesperson **name** → a report-layer UDC lookup (the code is on the fact).

---

## 5. The ONE remaining open item — H1 ShiftFactor

Both facts carry `shift_factor_applied = 1.0`. Hubble multiplied amounts by a **per-company factor** (default 0.01):
SDAEXP on SCP / CL National / Leslie's Poolmart / Ovintiv-MTD / Halliburton, and FHNAMT on Baseline-Finance /
DE-Orders / BP-Freight — **8 reports' dollar figures**. If Silver's decode ≠ that factor, those amounts are mis-scaled
(up to ~100×). **This is a data-reconciliation, not a missing source** — F0010 IS in Silver, so the factor can be
sourced if the decode doesn't already match. Reconcile one invoice + one shipment against Hubble before publishing $.

---

## 6. Notebooks (`eso1/nb/`)

| Notebook | Builds / does |
|---|---|
| `nb_eso1_gold_fact_sales_order_freight.py` | the 164-col line-grain freight fact; `build_fact()` ← F4211 (∪F42119 if present) + F4981 + lookups → plain overwrite |
| `nb_eso1_gold_fact_sales_commission.py` | the 46-col commission fact (F4211-driven, LEFT F42005 — flipped 2026-07-24); `build_fact()` ← F4211 driver + F42005 → plain overwrite |
| `nb_eso1_gold_dim_item.py` | `dim_item` (F4101); `build_dim_item()` → plain overwrite |
| `nb_eso1_gold_dim_category_code_10.py` | `dim_category_code_10` (F0005 UDC 01/10, ABAC10 → description); `build_dim()` → plain overwrite — mirrors ESO4 `dim_udc` |
| `nb_semantic_model_eso1.py` | Direct Lake relationships + DAX measures (freight buckets SUMX-deduped, Total Freight, weights, DISTINCTCOUNT, Days-Since, Total Tons) |
| `nb_validate_gold_eso1.py` / `nb_maintenance_gold_eso1.py` | RI/health validation · read-only maintenance status |

Reused dims (`old_nb/`) are **never rebuilt here** — their own jobs own them. Each notebook is **self-contained** (no `%run`).

---

## 7. Deliverables & where to find things

**Reference docs (`eso1/docs/`)**
- `ESO1_MASTER_REFERENCE.md` — *this file* (entry point)
- `ESO1_gold_layer_design.md` — authoritative Gold design (v2.15; §4.6 = commission fact, F4211-driven)
- `ESO1_hubble_field_mapping.md` — per-field JDE alias → Silver → Gold map (§F.4 = residual-gap cols)
- `ESO1_Core_Report_Reference.docx` — the master BvP report (joins/columns/measures)
- `ESO1_Filter_Capture_Variations_Reference.docx` — all 107 variations (one doc)
- `variations/ESO1_Variations_F01..F10_*.docx` — the same, split per family (10 docx)
- `ESO1_Query_to_Notebook_Gap_Analysis.docx` — gap analysis (regenerated: all closed, only H1)
- `ESO1_Notebook_Implementation_Verification.docx` — 5-pass adversarial correctness verification
- older: `ESO1_field_classification.docx`, `ESO1_report_elements.md`, `ESO1_runbook.md`, `ESO1_report_wireframe.md`,
  `ESO1_star_schema_design.md`, `ESO1_Silver_Data_Analysis.docx`

**Sources (`eso1/`)**
- `Filter Capture/` — the 107 legacy SQL files + `ESO1 Hubble Query.sql` (master) + `README.md` (variation catalog +
  consolidated join model) + `PAGE_FILTER_CHEATSHEET.md` (exact per-report page slicers for all 107)
- `queries/ESO1 Hubble Query.txt`, `report/`, `pipelines/`, `old_nb/` (superseded ESO1 assets)

---

## 8. Report-level filters are NOT in the notebooks

Confirmed by inspection: `build_fact` applies no report WHERE filter. Every one of the ~300 distinct predicates
across the 107 variations is a **Power BI page slicer** on a fact column. The master's five filters map as:
company / line_type / status_code_last → removed (slicers); F4074 ALAST whitelist → relocated
(`price_adjustment_type` slicer + freight DAX); F0101 ABAT1 band → relocated (`search_type` slicer, join relaxed to
LEFT). See `PAGE_FILTER_CHEATSHEET.md` for each report's exact slicer settings.

---

## 9. Open items (before "done")

1. **H1 ShiftFactor** — reconcile the 8 $-reports' decode vs the per-company factor (§5). The only analytical unknown.
2. **D1 — commission population — ✅ RESOLVED 2026-07-24 (flipped to F4211-driven).** `fact_sales_commission` was
   F42005-driven and *excluded* non-commissioned sales lines; SOP0027 (F4211-driven) *includes* them (blank commission).
   Owner chose Hubble fidelity → **driver flipped to F4211 ∪ F42119, F42005 LEFT-joined** (grain = sales line ×
   commission record; non-commissioned lines now appear with null commission; orphan F42005 commissions drop). Schema
   unchanged. Details: `docs/SOP0027_Commission_Driver_Investigation.md` + design §4.6 v2.15. Remaining Fabric action:
   `MANUAL_OVERWRITE=True` rebuild (grain/row-set change), then flip False.
3. **Fabric reload** — run one-off `MANUAL_OVERWRITE=True` (dims first, then facts) to materialize the v2.15 schema + build the
   commission fact; then flip `MANUAL_OVERWRITE=False`. All sources must exist under
   **`lh_jde_silver.jde`** (`SILVER_SCHEMA = "jde"` 2026-07-26, was `SRC_SCHEMA = "jde_cdc"`) and are read as static batch snapshots (no CDF required) — including the new
   F4106, F5549002, F03012, F49211, F42005.
4. **Reconcile load-by-load against Hubble** — nothing has been run on Fabric; correctness is code-reviewed, not proven.
   Start with the reusable-template families and the reports the new columns unblock.
5. **F42119 prerequisite** — must be present in Silver, else ⟲hist reports lose
   closed/purged lines.

---

## 10. Gotchas & lessons (carry forward)

- **⚠ Same JDE alias ≠ same Silver name across tables.** F4211 vs F42119 drift on 9 columns; the biting one:
  **SDLITM = `identifier_second_item` (F4211) vs `identifier_2nd_item` (F42119)** — a bare `unionByName` silently NULLs
  `second_item_number` for every history row (item2 is a heavy filter). **Rename before any cross-table union.** Always
  diff the two `column_name→snake_case_field` maps in `full_metadata.json`.
- **Silver names are NOT literal transliterations** — always look up the JDE alias in `full_metadata.json`/`.txt`
  (e.g. SDPSIG=`pull_signal`, SDVR03=`reference_ucis_no`, SDHOLD=`hold_orders_code`).
- **ShiftFactor (H1)** — the single biggest correctness risk; Silver decoded vs Hubble's per-company factor.
- **next-status is a string** — range filters need the physical `next_status_num` INT (Direct Lake can't range-filter
  a string reliably). Same fix ESO5 made.
- **INNER→LEFT relaxation** — Hubble's INNER ship-to F0101 / F0116 / dest-point are relaxed to LEFT so no line drops;
  the fact is a **superset** — a report needing the INNER-drop population re-imposes it as a page filter (e.g.
  ship_to not blank).
- **F4074 one-row-per-line** — collapsed via `row_number` (ordered by price_adjustment_type) to keep line grain; a line
  with several adjustments surfaces one — acceptable per the no-filter design, noted as a limitation.
- **Freight & weights are pre-decoded** — do NOT re-apply Hubble's /100, /1000000, /10000, ×ShiftFactor.
- **Grain guards** — every many-per-key source is pre-collapsed (b11d/b01d/F0116-latest/F4074-pick/route/freight/wt/lob/
  tag) and a final `dropDuplicates([sales_order_line_key])` guarantees one row per line.

---

*Maintained alongside the code. When the facts change, update `ESO1_gold_layer_design.md` (authoritative) and this
summary. Last updated 2026-07-22.*
