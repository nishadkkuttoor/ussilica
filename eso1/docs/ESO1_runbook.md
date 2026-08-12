# ESO1 — Billable v Payable Freight + SOP0027 Commission · End-to-End Procedure (Runbook)

Step-by-step build/deploy/operate procedure for the ESO1 reports on Microsoft Fabric.
**Three facts + BATCH full-snapshot overwrite + REUSED conformed dimensions.** Grounded in the
notebooks under `nb/`, the model/report under `report/`, and the full design in
`docs/ESO1_gold_layer_design.md` (v2.17 — batch; `fact_price_adjustment` added). Order: **verify reused dims → run the five Gold builders (batch overwrite) →
validate → semantic model → report → maintain → deploy → operate**.

> **Two report subsystems, one shared Gold star (design v2.16).** ESO1 serves two Power BI reports off conformed
> dimensions in `lh_jde_gold.rpt`:
> - **Billable v Payable Freight** → `fact_sales_order_freight` (order-line grain, freight denormalized) —
>   built by `nb_eso1_gold_fact_sales_order_freight` + `nb_eso1_gold_dim_item`.
> - **SOP0027 Commission** → `fact_sales_commission` (sales line × commission-record grain, F4211-driven, F42005
>   LEFT-joined) — built by `nb_eso1_gold_fact_sales_commission` + `nb_eso1_gold_dim_category_code_10`.
>
> Both facts live in the **one** Direct Lake model `billable_payable_freight` and share the reused address/plant/item
> dims. Every filter is a **Power BI page-level** slicer — Gold applies no business filters. **This runbook covers both.**
> ⚠ **Current asymmetry:** `nb_validate_gold_eso1` and `nb_maintenance_gold_eso1` are still **freight-only** (see §3, §7,
> and Open item F) — the commission fact + `dim_category_code_10` are built but not yet validated/maintained.

---

## 0. Prerequisites (one-time)

1. **Lakehouse** `lh_jde_gold` exists with:
   - **`rpt`** schema — the **REUSED** conformed dimensions (built by the `old_nb` jobs);
   - **`rpt`** schema — the new ESO1 tables **built here** (`fact_sales_order_freight`, `dim_item`,
     `fact_sales_commission`, `dim_category_code_10`), auto-created by the notebooks via
     `CREATE SCHEMA IF NOT EXISTS lh_jde_gold.rpt`. **There is no date dimension** — dates are the facts' raw date
     columns, sliced directly in Power BI.
2. **Silver** `jde.*` is landing from Bronze→Silver (already fully decoded — Julian→date, implied
   decimals resolved, snake_case, real NULLs). The builders read it as **static batch snapshots** (`SILVER_SCHEMA = "jde"`;
   no CDF required).
3. Spark **environment/pool** available; workspace on a **Fabric capacity (F64)**.
4. Import the **7 `nb/` notebooks** and **attach `lh_jde_gold`** as the default lakehouse on each:
   - **Freight subsystem:** `nb_eso1_gold_dim_item` (builds `dim_item`),
     `nb_eso1_gold_fact_sales_order_freight` (builds `fact_sales_order_freight`).
   - **Commission subsystem:** `nb_eso1_gold_dim_category_code_10` (builds `dim_category_code_10` from F0005
     UDC 01/10), `nb_eso1_gold_fact_sales_commission` (builds `fact_sales_commission`).
   - **Shared:** `nb_validate_gold_eso1`, `nb_maintenance_gold_eso1`, `nb_semantic_model_eso1`.

   **All seven are self-contained — no `%run`**, so each declares its own constants/transforms inline and can be
   imported/run independently (nothing to resolve across notebooks, so the `%run` "notebook not found" failure can't
   occur). The **four builders** (two dim, two fact) are **independent Fabric jobs**, each with its own
   `MANUAL_OVERWRITE` switch (no streams, no checkpoints), and may run in any order subject to dims-before-facts.

---

## 1. Reused dimensions — verify, do NOT rebuild

ESO1 **reuses** the existing conformed dimensions and relates to them on their **natural keys** — it does
**not** create duplicate customer/carrier/plant dims. These are owned and refreshed by their own `old_nb`
jobs; this solution only reads them.

| Reused dim (in `lh_jde_gold.rpt`) | Natural key | Fact column → key | Supplies |
|---|---|---|---|
| `dim_address_book` | `address_number` | freight: `ship_to`, `bill_to`, `carrier_number`; commission: `ship_to`, `sold_to`, `salesperson` → `address_number` | names (`name_alpha`), address, city/state/zip/country |
| ↳ role views `dim_address_ship_to`, `dim_address_sold_to`, `dim_address_carrier`, **`dim_address_salesperson`** | `address_number` | ship-to / bill-to(sold-to) / carrier / **salesperson (SCSLSP, commission)** roles (each gets its own active relationship) |
| `dim_plant` | `plant_code` | `branch_plant` → `plant_code` | `plant_name`, business unit, company, parent plant |

> **ESO1-built dims the facts also relate to (NOT reused old_nb dims — built here, Step 2):**
> `dim_item` (F4101; both facts) and **`dim_category_code_10`** (F0005 UDC 01/10; commission fact only — resolves the
> sold-to ABAC10 `category_code_10` code to its description). These follow the same build/verify lifecycle as the facts.

**Verify they are present and current** (Type-1 dims). If a fresh workspace, run the `old_nb` builders
once first: `nb_dim_address_book` (creates `dim_address_book` **and** the role views via
`CREATE OR REPLACE VIEW` — including **`dim_address_salesperson`**, which must exist before the commission
relationships bind) and `nb_dim_plant`.

**Reused-dimension guards** — every ESO1 entry point checks these before depending on them, so a missing or
stale reused dim surfaces immediately (constants `R_DIM_AB`/`R_DIM_PLANT`/role views live in
each self-contained notebook; base Delta dims are **hard requirements**, role views are checked best-effort since they
may be SQL-endpoint-only):

| Notebook | When | Behavior if a base reused dim is missing/unsound |
|---|---|---|
| `nb_eso1_gold_fact_sales_order_freight` (Step 2b) | preflight before build | **aborts** — exists · non-empty (base dims) |
| `nb_eso1_gold_dim_item` (Step 2a) | — | **no reused-dimension preflight** (builds only `dim_item` from F4101) |
| `nb_eso1_gold_dim_category_code_10` (Step 2c) | — | **no reused-dimension preflight**; builds only `dim_category_code_10` from F0005 (soft — absent F0005 is a warning, not fatal) |
| `nb_eso1_gold_fact_sales_commission` (Step 2d) | preflight before build | **aborts** if `dim_address_book`/`dim_plant` missing; also requires **F4211 (driver)** + **F42005** in Silver |
| `nb_semantic_model_eso1` (Step 5) | preflight before model bind | **aborts** (base dims + new tables incl. `fact_sales_commission`, `dim_category_code_10`; role views → warn only) |
| `nb_validate_gold_eso1` (Step 3) | §0 health + fact↔dim RI | logged **pass/fail** (incl. role-view subset consistency) — ⚠ **freight fact only** (commission not yet covered) |
| `nb_maintenance_gold_eso1` (Step 7) | read-only status | reused dims **never** OPTIMIZE/VACUUM'd; flags `⚠ STALE` > 7 days — ⚠ owns **freight fact + `dim_item` only** (commission tables not yet listed) |

> The role-playing views never promote through deployment pipelines — recreate them per stage (Step 8).
> Note: `dim_item_cost_cascade` is **no longer used** (cost/margin is out of scope for this spec).

---

## 2. Run the four Gold builders — PRIMARY runtime

**Four self-contained BATCH notebooks, each doing its own full rebuild** — each inlines all its transform logic
(no `%run`). They are **four independent Fabric jobs**, each with its own `MANUAL_OVERWRITE` switch, and — subject to
dims-before-facts — may run in any order. Each notebook reads the full Silver snapshot of its sources → runs `build_*()`
**once** → **plain overwrite** of its Gold table (no Change Data Feed, no streams, no checkpoints). To operate: **run each
once with `MANUAL_OVERWRITE=True`** (full rebuild), confirm healthy, then **set each back to `False`** (build-if-missing).

> **Build order within a subsystem: dims before the fact.** The fact preflights its dims. So on a first build run
> `nb_eso1_gold_dim_item` before `nb_eso1_gold_fact_sales_order_freight`, and `nb_eso1_gold_dim_category_code_10` before
> `nb_eso1_gold_fact_sales_commission`. Across subsystems the four are otherwise independent.
>
> **v2.17 — `nb_eso1_gold_fact_price_adjustment` is F4074-ONLY (reads Silver F4074 alone; no fact dependency).** It stores
> just the F4074 detail + the order-line key; line values come from the order-line fact via the relationship (RELATED in
> the measures), so nothing is duplicated. ETL = "read F4074, project, key" — minimal CU + storage, no run-order constraint.
> Both facts still need a one-time `MANUAL_OVERWRITE=True` rebuild (freight fact to materialize the earlier F4074/adjustment
> removal — row-count-neutral; `fact_price_adjustment` to build the new table), then redeploy the semantic model → flip both
> back to `False`. ⚠ Verify once that the padj keys join the freight keys (same JDE KCOO/DCTO/DOCO/LNID).

> **Section layout (matches ESO4/ESO5).** Facts: `1) CONFIG · 2) FACT BUILDER · 3) FACT SOURCES · 4) RUN`. Dims:
> `1) CONFIG · 2) DIM BUILDER · 3) RUN`. Each RUN = `CREATE SCHEMA IF NOT EXISTS` → preflight → `build_*()` once →
> `DROP TABLE IF EXISTS` + plain `mode("overwrite")` write → a JSON exit payload (`notebookutils.notebook.exit`). The
> `DROP` first ensures a clean recreate that also clears any old streaming-era `enableChangeDataFeed` property — the Gold
> tables are written **without** Gold CDF (matching ESO4/ESO5).

### 2a. `nb_eso1_gold_dim_item` — builds `lh_jde_gold.rpt.dim_item`

1. **CONFIG + DIM BUILDER inlined** (`load_silver_table`, `build_dim_item`) — **no reused-dimension preflight** (dim_item
   is built solely from F4101 and depends on no `rpt` dim).
2. **RUN** — `build_dim_item()` reads the full F4101 snapshot, selects `item_number_short` / `item_name` / `uom_weight`,
   `dropDuplicates`, and the RUN **overwrites** `dim_item`. `MANUAL_OVERWRITE=True` rebuilds; `False` builds only if the
   table is missing.

### 2b. `nb_eso1_gold_fact_sales_order_freight` — builds `lh_jde_gold.rpt.fact_sales_order_freight`

1. **CONFIG + FACT BUILDER inlined** (`build_uom_cascades`, `transform_freight_buckets`, `_add_effective_price_flag`,
   `build_fact`; UoM = item-specific `F41002` only, F41003 fallback via the reused `dim_uom_conversion` dim (built by
   `nb_silver_to_gold_dim_f41003.py`) using DAX `RELATED`, ESO7 v2 approach). The fact stores **raw date columns**
   (`order_date`, `actual_ship_date`, `gl_date`, `invoice_date`, …) sliced directly in Power BI — there is **no date
   dimension**; the `*_date_key` ints remain but are **unused**. **v2.1: all business WHERE filters are removed**
   (no `company`/`line_type`/`status`/`ALAST` whitelist/ship-to gate/`vendor_invoice`) — the fact carries **all**
   `is_delete=0` lines. Slicer fields are denormalized onto the fact (`price_adjustment_type`, `standard_industry_code`,
   `category_code_05/14`, `search_type`, `uom_structure`, `payment_terms`, `item_segment_04`); F4074 keeps **actual**
   values at one row/line; freight denormalized + `is_primary_shipment_line` anchor; **natural keys**; the fact stores
   **no audit columns**. `order_scope_key` is stored but **vestigial** (was the CDC delete scope; kept so the schema /
   semantic model are unchanged). F4101 and every other lookup are read as **static snapshots**.
2. **Reused-dimension preflight** — **aborts** if `rpt.dim_address_book` / `rpt.dim_plant` is missing or empty (role
   views best-effort), so we never build a fact whose FKs point at absent dims.
3. **RUN** — `build_fact()` reads F4211 (∪ F42119 if present) + 14 lookup snapshots, joins per design §4,
   `dropDuplicates(["sales_order_line_key"])`, and the RUN **overwrites** `fact_sales_order_freight`. A `FACT_SOURCES`
   inventory drives a source preflight (prints OK / MISSING / OPTIONAL-missing before building; F42119 optional).
   **Run `MANUAL_OVERWRITE=True` once after a schema change**, then set it back to `False`.

### 2c. `nb_eso1_gold_dim_category_code_10` — builds `lh_jde_gold.rpt.dim_category_code_10`

1. **CONFIG + DIM BUILDER inlined** (`load_silver_table`, `build_dim`) — **no reused-dimension preflight** (built solely
   from **F0005 UDC 01/10**: `product_code='01'` ∧ `user_defined_codes='10'` → `category_code_10` (DRKY) +
   `category_code_10_desc` (DRDL01)). Mirrors ESO4 `nb_eso4_gold_dim_udc`. `CAT10_SYS`/`CAT10_TYPE` at the top set the UDC
   (standard JDE — confirm if US Silica remapped AC10's edit UDC).
2. **RUN** — `build_dim()` reads the full F0005 snapshot, filters to UDC 01/10, and the RUN **overwrites**
   `dim_category_code_10`. **Run once with `MANUAL_OVERWRITE=True`**, then set back to `False`.

### 2d. `nb_eso1_gold_fact_sales_commission` — builds `lh_jde_gold.rpt.fact_sales_commission`

1. **CONFIG + FACT BUILDER inlined** (`_line_context`, `build_fact`). **F4211-DRIVEN** (∪ F42119 history) with **F42005
   LEFT-joined** for the commission columns, F4201 header LEFT-joined for `sold_to`, F0101 LEFT-joined for
   `category_code_10` — matching `SOP0027 - Commission.sql`. Grain = **one row per sales line × commission record**; a
   line with no commission appears once (null commission cols); a line with N commission records fans to N rows (F4211
   line metrics dedup in DAX via `is_primary_commission_line="Y"`). **All business WHERE filters are removed** — every
   SOP0027 predicate is a Power BI page filter (`status_code_next`/`status_code_last`, `sold_to IS NOT NULL`, the ABAT1
   band, …); the fact carries each filter column. Stores **raw date columns** + unused `*_date_key` ints (no date
   dimension) and **no audit columns**. **44 business + 2 keys = 46 stored columns.** `order_scope_key` is vestigial.
2. **Preflight** — **aborts** if `rpt.dim_address_book` / `rpt.dim_plant` is missing/empty, or if **F4211 (driver)** or
   **F42005** is absent from Silver. F42119 is optional context (unioned when present).
3. **RUN** — `build_fact()` reads F4211 (∪ F42119) + F42005/F4201/F0101 snapshots, joins per §4.6,
   `dropDuplicates(["sales_commission_key"])`, and the RUN **overwrites** `fact_sales_commission`. `FACT_SOURCES` =
   F4211 spine · F42005 left · F42119 optional-union · F4201 · F0101. **⚠ pending — run `MANUAL_OVERWRITE=True` once for
   the initial build (and after the driver flip / any schema change), then set to `False`.**

> **First run per notebook: `MANUAL_OVERWRITE=True`** materializes the batch table (and, via the `DROP`, clears any old
> streaming-era Gold CDF property); then set it to `False` for build-if-missing. **No Change Data Feed is required** on
> any Silver source — the builders read static snapshots. Refresh = re-run the notebook (or a scheduled pipeline), not a
> 30 s stream.

> Validation (Step 3) is a separate notebook; run it after the first build and ad-hoc thereafter.

---

## 3. Validate (`nb_validate_gold_eso1`)

Runs the full suite and writes `lh_jde_gold.rpt.eso1_validation_log`.

**§0 Reused-dimension health** — the most thorough reused-dim gate (base Delta dims are **hard**; role views are
best-effort via the SQL-endpoint-aware `_exists` helper, so an endpoint-only view is reported "not spark-visible
(OK)" rather than failing):
- **0a (hard):** `rpt.dim_address_book` & `rpt.dim_plant` exist · non-empty · unique PK (`address_number` / `plant_code`).
- **0b (hard if Spark-visible):** role views non-empty + a consistent subset of the base address book (no orphans).
- **0c (informational):** attribute usability — `name_alpha` / `plant_name` populated % (so report labels aren't blank).
- **0d (informational):** coverage — % of the fact's distinct `ship_to`/`bill_to`/`carrier_number`/`branch_plant` that resolve in the dim.
- **0e (informational):** freshness — newest `last_refreshed_timestamp` per reused dim.

Then: record-count reconciliation vs Silver F4211 (v2.1: **unfiltered** — all `is_delete=0` lines); completeness on
mandatory keys; duplicate detection (`sales_order_line_key` unique — one row per line; one freight bucket set per
shipment); **key integrity / fact↔dim RI**
against the **reused dims** (`ship_to`/`bill_to`/`carrier_number` → `rpt.dim_address_book.address_number`;
`branch_plant` → `rpt.dim_plant.plant_code`; `item_number_short` → `dim_item`) — the
**hard** orphan gate (left-anti = 0), which **skips gracefully** if a reused dim is absent; report reconciliation
(anchor dedup == SUMX dedup). Fails the run on any failed check.

> ⚠ **Commission not yet validated.** `nb_validate_gold_eso1` currently checks **`fact_sales_order_freight`** only —
> it has no `fact_sales_commission` reconciliation (row-count vs `F4211 ∪ F42119`, `sales_commission_key` uniqueness,
> `is_primary_commission_line` exactly-one-per-line, FK→`dim_category_code_10`/`dim_address_salesperson` RI). Until the
> validation notebook is extended (Open item F), verify the commission fact manually after its first `MANUAL_OVERWRITE=True`
> load: `sales_commission_key` unique, one `is_primary_commission_line='Y'` per sales line, and non-commissioned lines
> present with null commission columns (matching Hubble).

---

## 4. Batch build detail (the four Gold builders from Step 2)

The builds started in Step 2 — supporting detail. **Four independent batch jobs**; each reads the full Silver snapshot of
its sources, runs `build_*()` once, and overwrites its Gold table. **No Change Data Feed, no `readStream`, no
`foreachBatch`, no checkpoints, no continuous trigger** — the streaming machinery (handlers, `recompute_fact` /
`upsert_dim_item`, `_FACT_LOCK`, `init_ver`, the full-load/resume gate) was removed in v2.16. **No audit columns.**

| Source(s) read | Job | build action | Target | Write |
|---|---|---|---|---|
| F4101 | dim_item | select item cols → `dropDuplicates(item_number_short)` | `dim_item` | plain overwrite (no audit) |
| F0005 (UDC 01/10) | dim_category_code_10 | filter UDC 01/10 → `category_code_10` + desc | `dim_category_code_10` | plain overwrite (no audit) |
| F4211 (∪ F42119) + 14 lookups | freight fact | joins per design §4 → `dropDuplicates(sales_order_line_key)` | `fact_sales_order_freight` | plain overwrite (no audit) |
| F4211 driver (∪ F42119) + F42005/F4201/F0101 | commission fact | joins per §4.6 → `dropDuplicates(sales_commission_key)` | `fact_sales_commission` | plain overwrite (no audit) |

- **Reused dims are NOT built here** — `rpt.dim_address_book`/role views and `rpt.dim_plant` are refreshed by their own jobs.
- **`FACT_SOURCES` preflight (facts):** each fact declares a `{silver, join, join_pairs}` inventory that the RUN uses to
  print each source's presence (OK / MISSING / OPTIONAL-missing) before building. F42119 is optional (unioned when present).
- **Plain overwrite, no Gold CDF:** each RUN `DROP TABLE IF EXISTS` first (clears any old streaming-era
  `enableChangeDataFeed` property) then `mode("overwrite").option("overwriteSchema","true").saveAsTable(...)`. The Gold
  tables carry no `record_hash` / `is_deleted` / `source_commit_timestamp` / `gold_updated_timestamp`.
- **Exit payload:** each notebook ends with
  `notebookutils.notebook.exit(json.dumps({status, table, rows, elapsed_sec, end_time_utc}))`.
- **Result-identical to the old streaming full-load seed** — `build_*()` is the old `transform_*()` with only the dead
  streaming scope-filter (`restrict_*`) removed (AST-verified 2026-07-26); transforms, column lists, and constants are
  byte-identical.
- **Concurrency:** the four write **different** Gold tables, so they never conflict; dims-before-facts is the only ordering.

> Optional scheduled refresh: chain the four builders (dims first, then the two facts) → `nb_validate_gold_eso1` in a
> `pl_fact_sales_order_freight` / `pl_fact_sales_commission` pipeline on whatever cadence the reporting SLA needs.

---

## 5. Build / bind the semantic model (Direct Lake)

**One Direct Lake model, `billable_payable_freight`, holding BOTH facts** over the shared conformed dims.
Two equivalent representations — keep in sync:
- **Runtime builder:** `nb/nb_semantic_model_eso1.py` (sempy_labs) — generates the model from **12 tables**
  (`fact_sales_order_freight`, `fact_sales_commission`, `dim_item`, `dim_category_code_10` built here + reused
  `dim_address_*` role views incl. **`dim_address_salesperson`** / `dim_plant` / `dim_mode_of_transport`, all in `rpt`),
  **14 relationships** (freight: ship-to/bill-to/carrier/parent/destination → role views, branch_plant → dim_plant,
  item, mode_of_transport; commission: salesperson/ship-to/sold-to → role views, branch_plant, item,
  **category_code_10 → dim_category_code_10**), and **28 measures** (20 freight, shipment-deduped; 8 commission —
  plain-SUM commission amounts + `is_primary_commission_line`-deduped F4211 line metrics + Salesperson Name), keys
  hidden. **No date table** — dates slice off each fact's raw date columns.
- **Declarative twin (the report binds to this):** `report/billable_payable_freight.SemanticModel/` (TMDL) —
  **hand-maintained** (12 tables / 14 relationships / 28 measures). ⚠ `report/generate_tmdl_semantic_model.py` is
  **stale** (predates the commission fact, the reused-dim adds, and the `otc`→`rpt` schema move) — **do not regenerate
  without reconciling**; the hand-maintained twin is the source of truth.

The notebook's **In[2] preflight aborts if a base reused dim or new table is missing** — including
`fact_sales_commission` and `dim_category_code_10` (role views incl. `dim_address_salesperson` → warn only, since
Direct Lake binds them via the SQL endpoint) — so the model never binds against absent sources.

**Before binding, fill the Direct Lake source** in `…SemanticModel/definition/expressions.tmdl`:
`<SQL_ANALYTICS_ENDPOINT>` (the `lh_jde_gold` SQL endpoint) and `<LH_JDE_GOLD_DATABASE>`.

---

## 6. Deploy the report

`report/ESO1_Billable_v_Payable_Freight.Report/` (PBIR, 4 pages: overview / detail / lineDetail /
exceptions). `definition.pbir` binds `byPath → ../billable_payable_freight.SemanticModel`. Already baked in:
Detail→lineDetail **drill-through** on `shipment_number`, and
conditional formatting (Variance<0 red, CM% data bars). Ship-to/carrier names resolve via
`dim_address_ship_to[name_alpha]` / `dim_address_carrier[name_alpha]`; plant via `dim_plant[plant_name]`.

> **v2.1/v2.2:** CDC deletes rows outright (no `is_deleted` column on **any** built table), so the old **report-level
> `is_deleted = False` filter must be removed** — it references a non-existent column on both the fact and `dim_item`.
> The new slicer fields (`price_adjustment_type`, `standard_industry_code`, `category_code_05/14`, `search_type`,
> `uom_structure`, `payment_terms`, `item_segment_04`) are available on the fact for report filters.

> ⚠ **SOP0027 Commission report/page not yet authored.** The PBIR under `report/` is the **freight** report only
> (4 pages, all freight). The semantic model carries the commission fact + its 8 measures + `dim_category_code_10` /
> `dim_address_salesperson`, but **no report or page binds to them yet**. To surface SOP0027, add a page (or a separate
> `.Report` bound to the same `billable_payable_freight` model) with the commission measures and the page-level filters
> from `docs/SOP0027 - Commission.docx` (§3): `status_code_next='999'`, `status_code_last<>'980'`, the `line_type` /
> `second_item_number` exclusions, `order_type IN ('SO','CO')`, `company<>'00750'`, `sold_to IS NOT NULL`, and the
> ABAT1 `sold_to_search_type` band — all page slicers (Gold applies none). Show the category description via
> `dim_category_code_10[category_code_10_desc]`.

Open once in Power BI Desktop to confirm rendering, then promote Dev→Test→Prod with **`dpl_jde`**.
**Remember:** schedules, SQL views (the address role views), and lakehouse *data* never promote — recreate
views and re-point the Direct Lake source per stage.

---

## 7. Maintenance (`nb_maintenance_gold_eso1`)

OPTIMIZE (V-Order) + VACUUM (`RETAIN 168 HOURS`) on the tables **this solution owns**. Run on a **separate
schedule, never during a build run**. Cell In[2] is a **read-only reused-dimension status check** — it
confirms the reused dims are **out of scope** (never OPTIMIZE/VACUUM'd here) and reports their
`last_refreshed_timestamp`, flagging `⚠ STALE` if older than 7 days (a heads-up that the owning `old_nb` job may need a
nudge). Reused `rpt` dims are optimized by their own jobs.

> ⚠ **Commission tables not yet maintained.** `nb_maintenance_gold_eso1` currently sets
> `TABLES = [fact_sales_order_freight, dim_item]` — it does **not** OPTIMIZE/VACUUM `fact_sales_commission` or
> `dim_category_code_10`. Until it's extended (Open item F — a one-line change: add both to `TABLES`), maintain the
> commission tables manually (`OPTIMIZE lh_jde_gold.rpt.fact_sales_commission`; the small `dim_category_code_10` rarely
> needs it) or fold them into the same schedule.

---

## 8. Operate & monitor (steady state)

| Cadence | Action |
|---|---|
| Scheduled / on-demand | Re-run the four builders (dims first, then the two facts) with `MANUAL_OVERWRITE=True` — or a `pl_*` pipeline on the reporting SLA. Direct Lake reflects the rebuilt tables within the framing window. |
| On build failure | Re-run the affected notebook/job; a batch overwrite is **idempotent** (it rebuilds the whole table from the current Silver snapshot — no checkpoint/offset state to recover). Check Monitoring Hub → Spark logs. |
| Hourly/nightly (optional) | `nb_maintenance_gold_eso1` OPTIMIZE/VACUUM (freight fact + `dim_item`; add the commission tables manually until Open item F closes). A full overwrite already rewrites the whole table, so this is less critical than under the old streaming design. |
| Refresh the data | Re-run the relevant builder with `MANUAL_OVERWRITE=True`. Then run `nb_validate_gold_eso1` (freight) / spot-check the commission fact. |
| Daily glance | **exceptions** page — `[Lines Missing Conversion]`, `[Freight Shipments]`, variance outliers. |
| Per release | Recreate address role views + re-point the Direct Lake source in the new stage. |

---

## Open items (must close for exact tie-out)
- **A. ShiftFactor** — **both facts** use `shift_factor_applied = 1.0` placeholder on the F4211 line amounts
  (`extended_price`/`extended_cost`); the per-company company-constants table is not among the sources. Until joined,
  those $ are directionally correct, not tie-out exact (commission amounts from F42005 are Silver-decoded, unaffected).
- **B. Route #** — sourced from `f4941_shipment_routing_steps` (RSRTN); frequently 0. Confirm vs F4981 route.
- **C. F4211 GL date (SDDGL)** — Silver column name unconfirmed; resolved defensively at runtime (else NULL). Confirm it.
- **D. Load-time audit** — **no built table stores** `source_commit_timestamp` (v2.2; the batch overwrite carries no audit columns). If a load-time audit is ever needed, stamp a build timestamp in the RUN section (or rely on the exit payload's `end_time_utc`) rather than a per-row column.
- **E. TMDL placeholders** — fill `<SQL_ANALYTICS_ENDPOINT>` / `<LH_JDE_GOLD_DATABASE>` per stage.
- **F. Commission not in validate/maintenance** — `nb_validate_gold_eso1` and `nb_maintenance_gold_eso1` are still
  **freight-only**. Extend validate with a `fact_sales_commission` block (row-count vs `F4211 ∪ F42119`,
  `sales_commission_key` uniqueness, one `is_primary_commission_line='Y'` per line, FK RI to `dim_category_code_10` /
  `dim_address_salesperson` / dim_item / dim_plant) and add `fact_sales_commission` + `dim_category_code_10` to the
  maintenance `TABLES` list. Also confirm **AC10 = UDC 01/10** (standard JDE) is US Silica's edit UDC (else adjust
  `CAT10_SYS`/`CAT10_TYPE` in `nb_eso1_gold_dim_category_code_10`).
- **G. First batch build pending on Fabric** — none of the four batch builders has run on Fabric yet (v2.16 conversion).
  First build order: `nb_eso1_gold_dim_item` + `nb_eso1_gold_dim_category_code_10` (dims), then
  `nb_eso1_gold_fact_sales_order_freight` + `nb_eso1_gold_fact_sales_commission` (facts), each with
  `MANUAL_OVERWRITE=True`; confirm healthy, then set each back to `False`.

---

### Quick dependency order (TL;DR)
`verify reused dims (rpt.dim_address_book + role views incl. dim_address_salesperson, rpt.dim_plant) →
[FREIGHT] nb_eso1_gold_dim_item (build_dim_item ← F4101) +
nb_eso1_gold_fact_sales_order_freight (reused-dim preflight → build_fact ← F4211 ∪ F42119 + 14 lookups) +
[COMMISSION] nb_eso1_gold_dim_category_code_10 (build_dim ← F0005 UDC 01/10) →
nb_eso1_gold_fact_sales_commission (reused-dim + F4211/F42005 preflight → build_fact ← F4211 driver + F42005 LEFT)
[each = BATCH: read full Silver snapshot → build_*() once → DROP + plain overwrite (no CDF/streams/checkpoints), gated by
MANUAL_OVERWRITE / table-missing; four independent jobs; within a subsystem run the dim before its fact] →
validate (freight; commission = manual for now) → semantic model (one model, both facts; fill source) → report (byPath)
→ maintenance (optional schedule; freight owned, commission manual) → deploy via dpl_jde`
