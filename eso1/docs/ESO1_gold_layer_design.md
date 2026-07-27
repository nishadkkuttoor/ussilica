# Extended Sales Order 1 — Gold Layer Design (Billable v Payable Freight)

**Report:** Billable v Payable Freight · **Stakeholder:** Allison Stadnick · **Audience:** Logistics / Supply Chain
**Platform:** Microsoft Fabric (F64) · **Lakehouse:** `lh_jde_gold` · **Schema:** `eso1` (Extended Sales Order 1)
**Sources:** `lh_jde_silver.jde.*` (already fully decoded — Julian→date, implied decimals resolved, snake_case; read as **static batch snapshots** — **`SILVER_SCHEMA = "jde"` (2026-07-26; was `SRC_SCHEMA = "jde_cdc"`)**; same schema as ESO4/ESO5. No CDF required — see §5/§6.)
**Date:** 2026-07-26 · **Version:** 2.16 (**ALL FOUR Gold notebooks converted STREAMING→BATCH** + restructured to the ESO4/ESO5 section layout — no CDF/foreachBatch/checkpoints/streams; `MANUAL_OVERWRITE` full-snapshot rebuild; results proven identical to the old full-load seed. See §5/§6. Prior 2.15: **`fact_sales_commission` flipped to F4211-driven** — matches the SOP0027 query; non-commissioned lines now appear, orphan F42005 commissions drop; resolves open item D1. See §4.6. Prior 2.14: **NO date dimension — `dim_date` DROPPED.** Prior 2.12: **+22 residual-gap columns** on the freight fact. Prior 2.11: `fact_sales_commission` §4.6. Prior 2.10: `next_status_num` + `total_freight`.)

> **v2.16 (2026-07-26) — STREAMING → BATCH (all four Gold notebooks), no results change.** Per user direction ("match the
> ESO4 notebook structure; replace streaming with ESO4's execution pattern; don't change logic/data types/schema/results"),
> the four ESO1 Gold builders were restructured to the ESO4/ESO5 section layout and converted from continuous CDF streaming
> to a **batch full-snapshot rebuild**:
> - **`nb_eso1_gold_fact_sales_order_freight`** — 4 sections **1) CONFIG · 2) FACT BUILDER · 3) FACT SOURCES · 4) RUN**.
> - **`nb_eso1_gold_fact_sales_commission`** — same 4 sections (`FACT_SOURCES` = F4211 spine · F42005 left · F42119 optional-union · F4201 · F0101).
> - **`nb_eso1_gold_dim_item`** and **`nb_eso1_gold_dim_category_code_10`** — 3 sections **1) CONFIG · 2) DIM BUILDER · 3) RUN**.
> - **Removed** from every notebook: all `readStream`/`readChangeFeed`, `foreachBatch` handlers, `recompute_fact` /
>   `upsert_dim_item` / `_upsert_dim`, `_write_new_table`, `cdc_merge`-style writes, `_FACT_LOCK`/`threading`,
>   `current_version`, the checkpoint / `init_ver` / full-load-vs-resume gate (`_checkpoints_exist`, `_start_ver`,
>   stop-leftover-streams), `awaitAnyTermination`, `TRIGGER`/`CKPT`/`ENV`, the `DeltaTable` import.
> - **Added:** `MANUAL_OVERWRITE` (replaces `OVERWRITE`); a `FACT_SOURCES` inventory on each fact; a batch **RUN** =
>   `CREATE SCHEMA` → preflight → `build_*()` **once** → **plain overwrite** → JSON exit payload (`notebookutils.notebook.exit`).
> - **Renames (no logic change):** `transform_fact`→`build_fact`, `transform_dim_item`→`build_dim_item`,
>   `transform_dim`→`build_dim`; the dead streaming scope-filters (`restrict_orders` / `restrict_ship` / `restrict_item` /
>   `restrict_keys`) — never exercised on a full build — were dropped.
> - **Plain overwrite, NO Gold CDF** (matches ESO4/ESO5; each RUN `DROP TABLE IF EXISTS` first to clear the old
>   streaming-era `enableChangeDataFeed` property).
> - ✅ **Proven result-identical** to the old full-load seed by AST comparison: each `build_*()` body equals its old
>   `transform_*()` full build (the removed `restrict_*` branch was dead when called with `None`), and every helper,
>   the `FACT_BUSINESS_COLS` lists, and all constants are byte-identical. Output schemas unchanged.
> - **Refresh cadence is now scheduled** (re-run the notebook / a pipeline) instead of a 30 s continuous trigger — see §6.
>   NOT YET RUN on Fabric: `MANUAL_OVERWRITE = True` for the first rebuild of each (**dims first, then the two facts**),
>   then flip to `False`.

> **v2.15.1 addendum (2026-07-25)** — **new dimension `dim_category_code_10`** resolves the sold-to category-code-10
> (ABAC10) description for SOP0027. The report displays both the code *and* its description; per the star-schema rule
> (fact stores FK codes, dims resolve descriptions — as ESO4 does with `dim_sic`/`dim_state`), the description lives in a
> **separate reused dim**, not denormalized on the fact. New notebook **`nb_eso1_gold_dim_category_code_10.py`** builds
> `rpt.dim_category_code_10` (`category_code_10` → `category_code_10_desc`) from **F0005 UDC 01/10** (standard JDE —
> AC01–AC30 edit against 01/01–01/30), via the same CDF-streaming pattern as ESO4 `dim_udc`. The fact is unchanged
> (**44 biz + 2 keys = 46 stored**); it keeps the `category_code_10` code as the FK. Model gains one relationship
> `dim_category_code_10[category_code_10] → fact_sales_commission[category_code_10]` (semantic builder + TMDL twin:
> +1 table, +1 rel → **12 tables / 14 rels**). Run the new dim notebook (`OVERWRITE=True`) before the model binds.
>
> **v2.15 changes (2026-07-24)** — **`fact_sales_commission` driver flipped F42005 → F4211 ∪ F42119** (F42005 now LEFT-joined) so the fact matches SOP0027's F4211-driven query: grain = sales line × commission record; non-commissioned lines appear with null commission (like Hubble); orphan F42005 commissions whose order isn't in Silver `F4211∪F42119` drop (~76%, data-completeness). Added `status_code_next`/`status_code_last` page-filter columns (**46 stored cols**); `sales_commission_key` uses a `__NOCOMM__` sentinel for null `commission_line_number`. Schema unchanged → semantic model + TMDL twin unaffected (commission fact + `dim_address_salesperson` role view were added to both same session). Resolves **D1**. Details in §4.6 and `docs/SOP0027_Commission_Driver_Investigation.md`. Freight fact unchanged.

> **v2.14 changes (2026-07-23)** — **`dim_date` dropped; ESO1 has no date dimension** (reverses v2.13, which was authored the same day and never ran on Fabric). Decision rationale: the report binds every date to **raw fact date columns**, not to `dim_date`; no visual used the role-played measures; and under Direct Lake a date table only earns its keep for time-intelligence (`TOTALYTD`/`SAMEPERIODLASTYEAR`), which no ESO1 report needs. What changed:
> - **Deleted `nb_eso1_gold_dim_date.py`** and the `dim_date` table from the model (TMDL twin + `nb_semantic_model_eso1`): removed the date-role relationships, `mark_as_date_table`, and the two role-played measures (`Total Billable (GL Date)` / `(Invoice Date)`). To view $ by GL/invoice date, slice the fact's raw `gl_date` / `invoice_date` column on the visual.
> - **Dates = raw fact columns**, sliced directly: date-range slicers, and **relative-date slicers** for today/yesterday/MTD/YTD (the WTX loaded-tons family). No marked date table, **no DAX time-intelligence**.
> - **New fact column `ship_year_week`** (Mon–Sun ISO label `YYYY-Www`, off `actual_ship_date`) replaces `dim_date[year_week]` for the weekly "Billable vs Payable by Week" chart (its axis was repointed to this column). This is also a real fix: that chart previously bound to `actual_ship_date` (daily grain), so weekly bucketing was never actually happening.
> - **Sentinel-date hygiene (`clean_date`)** — JDE junk dates (probed live: `1952-12-31` zero-dates, `2824-08-29` corrupt Julians) are **nulled** on all raw date columns outside the valid window **`2000-01-01` … Dec 31 of (current year + 25)** — the upper bound is computed via `current_date()` and **self-extends each run** so future promised/delivery dates are never clipped (`VALID_DATE_LO` / `VALID_YEARS_AHEAD` in **both** facts). Without a date dim to constrain them, these would otherwise show up as selectable values in raw-date slicers.
> - The `*_date_key` int columns **remain on the fact but are unused** (no dim to join) and are hidden in the model — kept to avoid a wider schema change; may be dropped later.
> - **Schema change** (adds `ship_year_week`, nulls sentinels) → one-off `OVERWRITE=True` reload, then flip False.

> **v2.13 (2026-07-23) — SUPERSEDED same day by v2.14.** Briefly built `dim_date` as a generated calendar spine (`nb_eso1_gold_dim_date.py`); reverted — see v2.14. Never ran on Fabric.

> **v2.12 changes (2026-07-22)** — after re-reading `full_metadata.json` (26 tables), **every** source the 107 variations need is confirmed present in Silver (F5549002 was added last), so all residual structural gaps became notebook-only. Added **22 columns** to `fact_sales_order_freight` (fact now **147 business + 2 keys = 149 stored**):
> - **M1 `has_effective_price`** — `_add_effective_price_flag`: 'Y' iff an **F4106** (`item_base_price_file`) row EXISTS for (second_item_number, branch_plant, ship_to) with non-zero price whose effective window covers `actual_ship_date` — the inverse of Orders-with-Zero-Unit-Price Branch-B `NOT EXISTS`. A `left_semi` to the distinct line key keeps the grain (no fan-out).
> - **M2 `zone_number`** (SDZON) · **M3 `line_hold`** (SDHOLD — the LINE hold, distinct from the header `hold_orders_code`).
> - **M4** on the F4941 route aggregate: **`is_ocean_route`** ('Y' if any routing step is RSMOT='OCE') + **`route_container_count`** (SUM(RSNCTR)).
> - **M5** via a new **F5549002** (`mxp_bol_interface_detail`) LEFT join, collapsed one-row-per-line: **`gross_weight`** (MIGRWT) · **`catch_weight`** (MICTWT) · **`max_weight`** (MIMXWT). Silver pre-decoded — no /10000, /100. Additive **Gross Weight** / **Catch Weight** DAX measures added to `nb_semantic_model_eso1`.
> - **Deferred F4211 display (12):** `pull_signal` (SDPSIG) · `reference_02` (SDVR02) · `reference_03` (SDVR03) · `vendor_number` (SDVEND) · `price_adjustment_schedule` (SDASN) · `user_reserved_code` (SDURCD) · `price_override_code` (SDPROV) · `user_id` (SDUSER) · `lot_number` (SDLOTN) · `serial_number` (SDSERN) · `location` (SDLOCN) · `sales_reporting_code_05` (SDSRP5).
> - **`sold_to_lob_category_05`** (F03012 `customer_master_by_line_of_business` AIAC05, joined on the sold-to SDAN8=AIAN8, collapsed one-row-per-address — Mak's E26 filter) + **`deferred_entries_flag`** (F49211 `sales_order_detail_file_tag_file` UDDEFF, LEFT on the 4 line keys — SOP0006/000x).
> - **Still OUT:** only **H1 ShiftFactor** — a data-reconciliation, not a source gap (F0010 present). Every source the 107 variations reference is now on the fact. Schema change → one-off `OVERWRITE=True` reload materializes all 22, then flip False.

> **v2.10 changes (2026-07-22)** — two columns added after the full query-vs-notebook gap analysis (**`docs/ESO1_Query_to_Notebook_Gap_Analysis.docx`** — master BvP + all 107 variations verified against the fact notebook). Reflected in §4.3/§4.4:
> - **`next_status_num` (M6)** — a physical INT copy `CAST(TRIM(status_code_next) AS INT)` in the fact `select`. Direct Lake can't reliably range-filter the **string** status, but 5 reports do (SBX `next<620`, SM Planning `next<561`, Cargill `next<575`, SOP0006 `BETWEEN 574 AND 620`, Order-with-Multiple-Shipments `next<620`). Blank/non-numeric → NULL and excluded, matching Hubble's numeric comparison. Filter `next_status_num` in Power BI; keep displaying the string `status_code_next`. (Same fix ESO5 already made.)
> - **`total_freight` (H2)** — `SUM(net_amount)` over **ALL** F4981 rows per shipment (any `billable_payable`/`charge_code_01`), added to `transform_freight_buckets()` and denormalized at shipment grain like the other buckets. The combined-freight reports (Baseline Finance, DE Orders, BP Freight) sum the whole shipment's FHNAMT; the billable/payable buckets **under-count** it if a charge code falls outside `{BFR,FSC,FSB,PFR}`. Exposed via a new DAX measure **Total Freight** = `SUMX(VALUES(shipment_number), CALCULATE(MAX(total_freight)))` in `nb_semantic_model_eso1` (never a raw SUM — it's shipment grain).
> - **Does NOT resolve H1 (ShiftFactor).** `total_freight` gives the *complete* freight total, but every freight $ is still × `shift_factor_applied = 1.0` rather than the real per-company factor (§11 open item) — reconcile ShiftFactor before trusting the dollars.
> - **Schema change** → one-off `OVERWRITE=True` full reload materializes both columns (the fact notebook is already set to True), then flip back to False.

> **v2.9 changes (2026-07-15)** — support the ~110 **ESO1 Filter Capture** query variations at **Power BI page level** (Gold applies no report filters; carry every field). Companion docs: **`ESO1 Filter Capture/README.md`** (analysis, variation catalog + consolidated join model) and **`ESO1 Filter Capture/PAGE_FILTER_CHEATSHEET.md`** (2026-07-16 — exact per-report PBI page-slicer settings for all 107 variations, extracted from source SQL; also the authority on which reports need DAX measures vs. slicers and on the F42119 CDF data-prerequisite). Reflected in §1/§4:
> - **Decision: UPDATE the one fact — do NOT build per-report facts.** ~106 of ~110 variations are the same F4211 order-line grain differing only by WHERE filters. One unfiltered fact + page filters serves them. The **one** genuine separate need = commission (F42005), **now built as a second fact `fact_sales_commission`** (§4.6, v2.11 — different grain: commission line, finer than the sales line); the two "Multiple Shipments/Orders" reports are DAX `DISTINCTCOUNT` measures.
> - **+21 fact columns** (§4.4): `extended_price` (SDAEXP — the previously-missing sales-amount measure), `extended_cost` (SDECST), `currency_code` (SDBCRC — from F4211, **no F0010 source needed**), `backorder_qty`/`cancelled_qty`/`qty_to_date`/`open_qty` (SDSOBK/SDSOCN/SDQTYT/SDUOPN), `line_description_1`/`_2` (SDDSC1/2 — the LINE's own text, distinct from item_name), `date_updated` (SDUPMJ), `address_rate` (ship-to ABURAT — sole measure of the customer-rate variations), sold-to `sold_to_name`/`sold_to_search_type`/`sold_to_category_05`/`_10`, and F0116 ship-to `ship_to_city`/`_state`/`_zip`/`_address_1`/`_2`/`_country`.
> - **New joins** (§4.1): **F0101 sold-to role** (`so`, `ABAN8 = SDAN8`) for the sold-to name/search-type/category (the fact previously joined F0101 for **ship-to only** — Days-Since-Invoice etc. filter the *sold-to* search-type); and **F0116** (`f0116_address_by_date`, collapsed to the **latest-effective** row per address via `date_beginning_effective` desc) for the ship-to postal address (distinct from the F4981 *freight* city/state).
> - **F42119 (Sales Order History) unioned into the row population** and added as an **optional 3rd CDF stream** — the ~40 open-order variations `UNION ALL F42119` for closed/purged lines. Its Silver name `f42119_sales_order_history_file` is **CONFIRMED in `full_metadata.json`** (`table_name = sales_order_history_file`, identical 268-col `SD*` schema to F4211 — so the `UNION ALL` is schema-safe). Still **guarded** by `_load_optional` / `_F42119_PRESENT` (unioned/streamed only if the table is present in Silver, else the fact is live-F4211-only). Streaming refactored to a `_STREAMED` list.
> - **Already present — not re-added** (README had over-flagged them): `gl_class`, `delivery_instruct_line_01/02`. `user_reserved_amount` (SDURAB) is already surfaced as `bol_number`. SDUORG/SDPQOR/SDSOQS = `transaction_quantity`/`primary_quantity_ordered`/`quantity_shipped` (present). `dim_item` needs no change.
> - **Deferred** (niche, not high-value): F4074 adj detail (ALUPRC/ALUOM/ALBSDVAL), F49211 UDDEFF, ocean-booking role names + vessel/voyage#/incoterm/booking dates, weigh-ticket weights (F5549002), serial/lot/location, SDVR02/03, SDPSIG/SDZON/SDUSER/SDASN/SDURCD/SDPROV, SDSRP5; plant/salesperson via dims. Requires a one-time `OVERWRITE=True` reprocess (schema change).
>
> **v2.8 changes (2026-07-08)** — reflected in §6:
> - **Root cause fixed:** an **incomplete checkpoint** (dir created with `metadata/`+`sources/` but **no committed batch
>   offset** — e.g. a stream that died on its first read) passed the old `_checkpoints_exist()` (which only checked the dir
>   was non-empty), so the gate chose **RESUME** (`init_ver=-1`). With no usable offset Delta cold-started the CDF reader at
>   `startingVersion=0`, and version 0 predates CDF enablement → `DELTA_MISSING_CHANGE_DATA` for range `[0, N]`.
> - **`_checkpoints_exist()` now checks the `offsets/` sub-dir** (`mssparkutils.fs.ls(f"{p}/offsets")`) per stream: a
>   checkpoint with no committed offset is treated as **ABSENT**, forcing a **FULL LOAD** that re-establishes `init_ver` at a
>   CDF-valid version. Applied in **both** notebooks.
> - **`_start_ver(iv, tbl)` resume fallback is `current_version(tbl)`, never `0`** — `current_version` always exists and is
>   `>=` the version CDF was enabled at, so even if an offset-less checkpoint reached the stream start it can't read pre-CDF
>   version 0. On a genuine resume the committed offset still drives; `startingVersion` is only the cold-start fallback.
> - **Recovery for an existing poisoned checkpoint:** run the affected notebook once with `OVERWRITE=True` (whole notebook,
>   so the **gate cell** runs) — it drops+reseeds and `mssparkutils.fs.rm(CKPT, True)` wipes the bad checkpoint; then set
>   `OVERWRITE=False`. Business/transform logic unchanged.

> **v2.7 changes (2026-07-08)** — reflected in §1/§4:
> - **UoM → TN conversion now follows the `nb_silver_to_gold_eso7_v2_fact` approach.** The in-notebook cascade dropped
>   its **standard-UoM `F41003`** leg — `build_uom_cascades()` now returns a **single** item-specific `F41002` union
>   (UMRUM='TN' fwd + UMUM='TN' reciprocal rev), and `conv_rate = coalesce(when uom='TN' → 1.0, F41002 item factor)`.
>   The `F41003` fallback is served by the **reused `dim_uom_conversion` dimension** (`lh_jde_gold.eso7.dim_uom_conversion`;
>   grain `from_uom` → `std_factor`), built/maintained by **`nb_silver_to_gold_dim_f41003.py`** and applied as the Tier-B
>   fallback in the **Total Tons** DAX measure via `RELATED`.
> - **Unresolved conversions stay `NULL`** (the blanket `1.0` default was removed), so `conversion_to_tons_rate` and
>   `quantity_shipped_tons` are `NULL` for lines with no item-specific factor; `missing_conversion_flag='Y'` still marks them.
> - `F41003` is **no longer a notebook source** (constant retired). The separate `uom_str` lookup for `uom_structure`
>   (F41002 UMUSTR, a filter attribute) is **unchanged**. Requires a one-time `OVERWRITE=True` reprocess (values change).

> **v2.6 changes (2026-07-07)** — reflected in §1/§3/§5/§6/§9/§10:
> - **Split the single `nb_eso1_gold_streaming` processor into TWO self-contained notebooks.** Business/transform logic is
>   **byte-for-byte unchanged** (transforms, `FACT_BUSINESS_COLS`, `recompute_fact`/`upsert_dim_item`, the full-load/resume
>   LOGIC, and the two built tables `fact_sales_order_freight` + `dim_item` are all identical). Only the notebook
>   **packaging** changed:
>   - **`nb_eso1_gold_dim_item`** — builds/streams **only** `lh_jde_gold.rpt.dim_item`. ONE CDF stream
>     `dim__f4101_item_master`←F4101; checkpoint root `Files/checkpoints/eso1_dim_<ENV>`; its **own** `OVERWRITE` flag;
>     handler `make_dim_item_handler(init_ver)`→`upsert_dim_item`. **No reused-dim preflight** (dim_item has no reused-dim
>     dependency). Helpers: `sname`, `load_silver_table`, `current_version`, `_write_new_table`, `transform_dim_item`,
>     `upsert_dim_item`.
>   - **`nb_eso1_gold_fact_sales_order_freight`** — builds/streams **only** `lh_jde_gold.rpt.fact_sales_order_freight`
>     (order-line grain). TWO CDF streams `fact__f4211_sales_order_detail_file`←F4211 and
>     `fact__f4981_freight_audit_history`←F4981; checkpoint root `Files/checkpoints/eso1_fact_<ENV>`; its **own** `OVERWRITE`
>     flag; handlers `make_fact_f4211_handler(init_ver)` / `make_fact_f4981_handler(init_ver)`→`recompute_fact` (serialised
>     by `_FACT_LOCK`). **Owns the reused-dim preflight** (aborts if `rpt.dim_address_book` / `rpt.dim_plant` are
>     missing/empty). Reads **F4101 as a STATIC snapshot** (for `item_name` / `item_segment_04`) but does **not** stream
>     F4101 and does **not** build `dim_item`.
>   - **Separate checkpoint roots are REQUIRED, not cosmetic** — each notebook's full load runs `mssparkutils.fs.rm(CKPT,
>     True)` on its own root; a shared root would let one notebook's full load cross-wipe the other's checkpoints. The two
>     notebooks are **independent Fabric jobs** (can run concurrently), each with its own `OVERWRITE` lifecycle.
>   - **Total streams across the two jobs = 3** — dim_item notebook = **1** stream; fact notebook = **2** streams.
> - **Reconciled `startingVersion = init_ver`** (NOT `init_ver+1`) — `init_ver` is the seed-time version; it exists and,
>   with CDF enabled, carries change data, and the handler discards it via `_commit_version <= init_ver`. `init_ver+1` would
>   be beyond the latest version at seed time and Delta rejects it ("Cannot time travel to version N"). REQUIREMENT:
>   `delta.enableChangeDataFeed = true` must be recorded **at or before `init_ver`** on each streamed source (F4101 for the
>   dim; F4211/F4981 for the fact) — otherwise Delta raises `DELTA_MISSING_CHANGE_DATA` for that version.
> - **Reconciled per-stream `_checkpoints_exist()`** — it now checks **every** per-stream checkpoint dir (not just a
>   non-empty root) via `mssparkutils.fs.ls(<per-stream path>)`; a missing per-stream checkpoint forces a **FULL LOAD** (so
>   no stream falls back to `startingVersion=0`). Each notebook checks only its own stream(s)' checkpoint dir(s).
>
> **v2.5 changes (2026-07-07)** — reflected in §5/§6/§9/§10:
> - **Streaming section refactored to EXACT `nb_silver_to_gold_eso7_v2` parity.** Business/transform logic is
>   **unchanged** (transforms, `FACT_BUSINESS_COLS`, `recompute_fact`/`upsert_dim_item`, the full-load/resume LOGIC, and the
>   two built tables `fact_sales_order_freight` + `dim_item` are all identical). Only the streaming **plumbing** and code
>   standards changed:
>   - **ESO7-style handler factories** — the generic quarantine wrapper `make_handler(name, fn)` + `_dim_item_fn` /
>     `_fact_from_f4211` / `_fact_from_f4981` (+ `_skip_seed`, `_CHANGE_TYPES`) are replaced by three named factories
>     `make_dim_item_handler(init_ver)` / `make_fact_f4211_handler(init_ver)` / `make_fact_f4981_handler(init_ver)`. Each
>     returns a handler that inlines: empty-batch guard → seed-skip (`if init_ver >= 0: filter _commit_version > init_ver`)
>     → empty guard → filter `_change_type ∈ (insert, update_postimage, delete)` → business call → a plain `print(...)`.
>   - **QUARANTINE REMOVED** — the robust-`foreachBatch` mechanism is gone: no `make_handler` wrapper, no
>     `persist(MEMORY_ONLY)`/`unpersist`, no UTC+local log banner, no `_quarantine` routing; the
>     `eso1.eso1_stream_quarantine` table and `QUARANTINE` constant are deleted. **A failing micro-batch now fails the
>     stream** (no per-batch isolation, no quarantine) — matching ESO7.
>   - **`mssparkutils.fs` directly** — the `_fs()` importlib fallback util is removed; `_checkpoints_exist()` and the
>     checkpoint clear call `mssparkutils.fs.ls(CKPT)` / `mssparkutils.fs.rm(CKPT, True)` directly, exactly like ESO7.
>   - **Inline per-source stream starts** — the `start_cdf_stream(...)` helper is removed; the three streams are started
>     inline (explicit `spark.readStream…writeStream` blocks), like ESO7.
>   - **queryName / checkpoint scheme → `dim__<src>` / `fact__<src>`** — the stream identifiers and checkpoint sub-paths are
>     now `dim__f4101_item_master`, `fact__f4211_sales_order_detail_file`, `fact__f4981_freight_audit_history` (was
>     `dim_item` / `fact_f4211` / `fact_f4981`). The stop-leftover set `_STREAM_NAMES` holds these three names.
>   - **`sname(table_name)` helper added** (== ESO7 `sname`); `load_silver_table` / `current_version` route through it.
>   - **NEXT RUN MUST BE A FULL LOAD** — because the queryNames and checkpoint sub-paths changed, resuming against the old
>     checkpoint dirs would be inconsistent; the next run must full-load to reseed and re-establish checkpoints at the new
>     paths. Already covered — `OVERWRITE` is currently `True`.
>
> **v2.4 changes (2026-07-06)** — reflected in §1/§3/§5/§6/§7/§9/§10:
> - **Mode-of-transport description dimension removed entirely** — its JDE UDC (`00/TM`) source table is no longer read;
>   there is no mode-of-transport transform, upsert, or stream, and no UDC lookup for it. The fact **still carries the
>   `mode_of_transport` CODE column** (F4211 SDMOT), but there is **no mode-of-transport description dimension** anymore.
> - **`dim_date` reclassified — no longer built here** (`build_dim_date` removed). ⚠ **Final state per v2.14 (2026-07-23): ESO1 has NO date dimension at all** — `dim_date` is neither built nor reused; dates are the fact's raw date columns sliced directly (see the v2.14 block). Disregard this bullet's "owned elsewhere" wording. The `*_date_key` ints remain on the fact but are unused.
> - **Streams 4 → 3** — exactly THREE Silver CDF streams: `dim__f4101_item_master`←F4101 (→ built table `dim_item`),
>   `fact__f4211_sales_order_detail_file`←F4211, `fact__f4981_freight_audit_history`←F4981 (both → built table
>   `fact_sales_order_freight`). `init_ver` snapshots F4101/F4211/F4981 only. *(queryName/checkpoint scheme updated to the
>   `dim__/fact__` form in v2.5 — see the v2.5 block above.)*
> - **Two ESO7-style streaming-scaffolding additions** (from `nb_silver_to_gold_eso7_v2`, In[7b]): **(a) stop leftover
>   streams** — before the full-load/resume decision the notebook scans `spark.streams.active` and stops any query named
>   `dim__f4101_item_master`/`fact__f4211_sales_order_detail_file`/`fact__f4981_freight_audit_history` left alive from a
>   previous run in the same Spark session (stopping a cell does NOT
>   stop Spark streaming queries); **(b) checkpoint-existence full-load gate** — the full-load condition is now
>   `OVERWRITE or not tableExists(fact) or not _checkpoints_exist()`, so missing checkpoints force a FULL LOAD (re-establishing
>   `init_ver`) instead of resuming from version 0 and reprocessing history into Gold. Helper `_checkpoints_exist()` lists
>   `Files/checkpoints/eso1_<ENV>` via `mssparkutils.fs` directly.
>
> **v2.3 changes (2026-07-06)** — reflected in §1/§3/§5/§6/§7/§9/§10:
> - **Silver source schema renamed `jde` → `cdf`** — every Silver source is now read from `lh_jde_silver.cdf.<table>`
>   (previously the `jde` schema); Change Data Feed is enabled on the `cdf` schema tables. The notebook constant is `SRC_SCHEMA = "cdf"`.
>   (v2.3 also introduced a mode-of-transport description dimension; that dimension was **removed in v2.4** — see above.)
>
> **v2.2 changes (2026-07-01)** — CDC mechanism aligned to `nb_silver_to_gold_eso7_v2`; reflected in §3/§5/§6:
> - **NO audit columns on ANY built table.** `is_deleted`, `source_commit_timestamp`, `gold_updated_timestamp`,
>   `record_hash` are removed from **both** `fact_sales_order_freight` **and** `dim_item` (v2.1 still kept them on `dim_item`).
>   The `record_hash()` helper and the whole `cdc_merge()` function are gone.
> - **Fact CDC = delete-scope + append** (`recompute_fact`): for the changed orders, delete the fact rows for those
>   `order_scope_key`s, then **append** the freshly recomputed lines — one path for insert/update/delete, no `record_hash`
>   gate, no soft-delete flag. Fact writes are serialised across the two fact streams by a `threading.Lock`.
> - **`dim_item` CDC = MERGE upsert + MERGE delete** (`upsert_dim_item`): `whenMatchedUpdateAll/whenNotMatchedInsertAll`
>   for changed items, `whenMatchedDelete` for removed items. No audit columns, no `record_hash`.
> - **`_change_type` + `init_ver` seed-skip**: handlers act only on `insert`/`update_postimage`/`delete` CDF rows; on a full
>   load each source's Delta version is snapshotted as `init_ver`, streams start there and skip `_commit_version <= init_ver`
>   (the seed rows). On resume `init_ver = -1` and the checkpoint drives the offset.
> - **Gold tables written with `delta.enableChangeDataFeed = true`** (fact + `dim_item`). No date dimension.
>
> **v2.1 changes (2026-07-01)** — data-shape changes, still current:
> - **All business WHERE filters removed** — the fact carries **all** rows (former Hubble hard filters
>   `company IN (00640,00645)` / `line_type='S'` / `status_code_last<>980` / F0101 ship-to address-type gate /
>   F4074 `ALAST` whitelist / F4981 `vendor_invoice<>'NULL'` are all dropped). Filtering moves to the semantic model / Power BI.
> - **Filter (slicer) fields denormalized onto the fact** — `price_adjustment_type`, `standard_industry_code`,
>   `category_code_05`, `category_code_14`, `search_type`, `uom_structure`, `payment_terms`, `item_segment_04`.
>   (Ship-to `SIC`/cat-05/cat-14/`search_type` are now denormalized onto the fact **and** still available on the reused ship-to dim.)
> - **F4074 = actual value, one row per line** — `price_adjustment_type` (ALAST) and `freight_factor_value` (ALUPRC) are the
>   real row values from a deterministic `row_number` pick (no aggregation / no comma-list), so the fact stays one row per line.

Authoritative inputs for this design:
- **`001 OTC Reports/Extended Sales Order 1.docx`** — primary source: business rules, SQL joins, filters, column list, freight calculations (Hubble "BvP Combined").
- **`ESO1_Silver_Data_Analysis.docx`** — verified data facts (grain, keys, `is_delete`, freight buckets) from the top-50-row Silver inspection.
- **`Fabric_Naming_Convention_Guidelines.pdf`** — all asset/column names below comply (snake_case, `lh_/wh_/pl_/dpl_`, `dim_/fact_`, leading-digit spell-out, trailing-digit zero-pad).

> **Scope:** Gold layer only. Bronze and Silver are complete and are **not** regenerated. This version replaces
> the previous two-fact design (`fact_sales_order_line` + `fact_freight_audit` + `dim_shipment`) with a **single
> consolidated fact** per the current requirement. The superseded notebooks are listed in §12.

---

## 1. Architecture

```
 lh_jde_silver.jde.*  (Delta — read as STATIC batch snapshots; no CDF required)
   F4211  F4201  F0101  F0116  F4101  F41002  F4074  F4981  F5642B01  F5642B11  F4941  F42005  F0005  [F42119 — optional/confirmed]
        │  (spark.read.table — full snapshot per source)
        ▼
 FOUR self-contained BATCH processors (independent Fabric jobs — MANUAL_OVERWRITE full-snapshot rebuild; no streams/checkpoints):
   ┌─ nb_eso1_gold_dim_item                  ── build_dim_item()  ← F4101                         · own MANUAL_OVERWRITE · no preflight
   ├─ nb_eso1_gold_dim_category_code_10      ── build_dim()       ← F0005 UDC 01/10               · own MANUAL_OVERWRITE · no preflight
   ├─ nb_eso1_gold_fact_sales_order_freight  ── build_fact()      ← F4211(∪F42119) + 14 lookups   · own MANUAL_OVERWRITE · owns reused-dim preflight
   └─ nb_eso1_gold_fact_sales_commission     ── build_fact()      ← F4211(∪F42119) driver + F42005 LEFT + F4201 + F0101 · own MANUAL_OVERWRITE · owns preflight
        │                                                                                            │
        ▼                                                                                            ▼
 lh_jde_gold.rpt  (NEW built here — each notebook: read full Silver snapshot → build_*() ONCE → plain overwrite, no audit cols, no Gold CDF)
   ├─ dim_item                   (F4101)                 ← nb_eso1_gold_dim_item
   ├─ dim_category_code_10       (F0005 UDC 01/10)       ← nb_eso1_gold_dim_category_code_10
   ├─ fact_sales_order_freight   (ONE consolidated fact, order-line grain, freight denormalized)   ← nb_eso1_gold_fact_sales_order_freight
   └─ fact_sales_commission      (SOP0027; sales line × commission record)                          ← nb_eso1_gold_fact_sales_commission
        │   relate (natural keys) to ──►  lh_jde_gold.rpt  (REUSED, maintained by old_nb jobs)
        │                                    ├─ dim_address_book  + role views ship_to / sold_to / carrier / salesperson
        │                                    └─ dim_plant
        │   NO date dimension — dates are the facts' raw date columns (sliced directly; weekly = fact.ship_year_week)
 Semantic model  billable_payable_freight  (Direct Lake, single schema rpt)  →  Power BI report
```

**Design principles**
- **Single consolidated freight fact** at **order-line grain** — mirrors the Hubble flat dataset (F4211 driver, freight joined in).
  A second fact `fact_sales_commission` is split **on grain** for SOP0027 (§4.6).
- **Reuse conformed dims** — relate to the existing `rpt.dim_address_book` (role views) and `rpt.dim_plant` on natural keys;
  build only the genuinely-new `dim_item` (+ `dim_category_code_10` for SOP0027). **No date dimension** — dates are raw fact columns.
- **Batch build (full-snapshot overwrite)** Silver→Gold — each notebook reads the full Silver snapshot of every source,
  runs `build_*()` **once**, and **overwrites** its target. No `readStream` / Change Data Feed / `foreachBatch` / checkpoints /
  continuous trigger. `MANUAL_OVERWRITE = True` rebuilds; `False` builds only if the target is missing. (Same execution pattern
  as ESO4 / ESO5 / `nb_silver_to_gold_eso7_v2_fact`.)
- **No audit columns on any built table** — a full overwrite replaces the whole table each run, so there is no
  `record_hash` / `is_deleted` / `source_commit_timestamp` / `gold_updated_timestamp` and no CDC merge/delete logic.
- **Natural keys throughout** — no surrogate keys; the build is deterministic and idempotent (re-running reproduces the table).
- **Self-contained notebooks (no `%run`)** — each `nb/` notebook declares its own constants/transforms inline, so there is
  no cross-notebook dependency to resolve. The four Gold builders are independent Fabric jobs.
- **Direct Lake** — native Delta tables, liquid clustering, V-Order, deletion vectors; ratios computed in DAX (SUM/SUM).

---

## 2. Grain, keys, and the freight decision

| | Detail |
|---|---|
| **Fact grain** | One row per **sales-order line**: `company_key_order_no` + `order_type` + `order_number` + `line_number` |
| **Business key** | `sales_order_line_key = sha2(company_key_order_no‖order_type‖order_number‖line_number)` |
| **Freight grain** | Freight $ (F4981) are **shipment** grain — coarser than the line. **Decision:** denormalize the true shipment freight buckets onto every line of the shipment, flag exactly one line per shipment `is_primary_shipment_line='Y'`, and **dedup in DAX** with `SUMX(VALUES(shipment_number), …)` so totals never inflate. (See §8 measures.) |

**Why a single fact is spec-faithful:** the Hubble "BvP Combined" query (spec §8) is itself one flat result set at
order-line grain — F4211 is the driver and the freight aggregate (`FHNAMT × ShiftFactor`) is joined on `shipment_number`.
We reproduce that shape, and resolve the documented fan-out risk (Silver analysis §8.3) with the denormalize + DAX-dedup pattern.

---

## 3. Dimensions — REUSE conformed dims; build only what's genuinely new

**Reuse, do not duplicate.** The branch/plant and address dimensions already exist as conformed Gold assets
(`lh_jde_gold.rpt.*`, built/maintained by the `old_nb` jobs). ESO1 **relates to them on their natural keys** instead
of creating parallel copies. **One** genuinely-new dim is built-and-owned here (no existing equivalent): `dim_item`.
**There is no date dimension** — dates are the fact's raw date columns, sliced directly (weekly grouping via the
fact's `ship_year_week`).

| Dimension | Status | Schema | Relate fact on | Used for |
|---|---|---|---|---|
| `dim_address_book` (+ role views `dim_address_ship_to`, `dim_address_sold_to`, `dim_address_carrier`) | **REUSE** | `rpt` | `ship_to` / `bill_to` / `carrier_number` → `address_number` | ship-to & bill-to names + address, carrier name |
| `dim_plant` | **REUSE** | `rpt` | `branch_plant` → `plant_code` | plant name/attributes |
| *(no date dim)* | — | — | dates are raw fact columns (sliced directly); weekly = `fact.ship_year_week` | date filters, weekly cadence |
| `dim_item` | **NEW** (built here) | `rpt` | `item_number_short` → `item_number_short` | item description / weight UoM |

Reused dims are **not rebuilt or maintained** by this solution (no OPTIMIZE here) — their own jobs own
them; ESO1 only reads them (and checks the base dims exist). **No built table stores audit columns** (v2.2): `dim_item`
(and `dim_category_code_10`) are **Type-1 full-overwrite rebuilds** (no `record_hash`/`is_deleted`); each fact is a
full-snapshot overwrite (§5). None of `is_deleted`/`source_commit_timestamp`/`gold_updated_timestamp`/`record_hash` exist
on any built table.
**No surrogate keys on the fact** — natural keys throughout (matches the reused dims' keys; the build is deterministic/idempotent).

### 3.1 Dates — NO date dimension (v2.14)
ESO1 has **no `dim_date`**. Under Direct Lake a date table only earns its keep for DAX time-intelligence
(`TOTALYTD` / `SAMEPERIODLASTYEAR`), which no ESO1 report needs, and the report binds every date to raw fact columns
anyway. So dates are handled entirely on the fact:
- **Raw date columns** (`order_date`, `actual_ship_date`, `gl_date`, `invoice_date`, `cancel_date`, …) are stored as
  `DateType` and **sliced directly** — date-range slicers, and **relative-date slicers** for today/yesterday/MTD/YTD
  (the WTX loaded-tons family). No marked date table; **no time-intelligence**.
- **Weekly cadence** uses the fact column **`ship_year_week`** (Mon–Sun ISO label `YYYY-Www`, derived off
  `actual_ship_date`) — replaces the former `dim_date[year_week]`.
- **"$ by GL/invoice date"** is done by slicing the raw `gl_date` / `invoice_date` column on the visual (the old
  role-played `Total Billable (GL Date)` / `(Invoice Date)` measures are removed).
- **Sentinel hygiene (`clean_date`)** — JDE junk dates (probed live: `1952-12-31` zero-dates, `2824-08-29` corrupt
  Julians) are nulled on every raw date outside `2000-01-01 … Dec 31 of (current year + 25)` (self-extending upper
  bound via `current_date()`), so they never appear in a slicer.
- The `*_date_key` int columns (`date_key()` helper, `yyyyMMdd`) **remain on the fact but are unused** (no dim to join)
  and are hidden in the model — kept to avoid a wider schema change; may be dropped later.

### 3.2 `dim_address_book` (+ role views) — REUSE (`rpt`)
Existing conformed dim, PK `address_number`. Columns include `name_alpha` (ship-to / carrier name), `address_type_01`,
`city`, `state`, `country`, `zip_code_postal`, `address_line_01..04`, `has_postal_address`. Role-played via the existing
SQL views `dim_address_ship_to`, `dim_address_sold_to` (bill-to), `dim_address_carrier` so each role gets its own active
relationship. ESO1 relates `fact.ship_to/bill_to/carrier_number → address_number`. (Carrier name isn't resolved in the
deployed Hubble query but the model supplies it cleanly via the carrier role view.)

### 3.3 `dim_plant` — REUSE (`rpt`)
Existing conformed dim, PK `plant_code`. Columns `plant_name`, `plant_name_compressed`, `plant_category_code_02`,
`related_business_unit`, `company`, `parent_plant_code`. ESO1 relates `fact.branch_plant → plant_code` (SDMCU).

### 3.4 `dim_item` — F4101 (NEW, `eso1`)
| Target | Source | JDE | Notes |
|---|---|---|---|
| `item_number_short` | F4101 | IMITM | **PK (natural)** = F4211.SDITM |
| `item_name` | F4101 | IMDSC1 | description_line_01 |
| `uom_weight` | F4101 | IMUWUM | |

F4101 has `is_delete` → filtered to 0. (No existing item dimension among the reusable assets, so this is built new.)

### 3.5 Reused-dimension checks (guard matrix)
Because ESO1 **depends on** `rpt.dim_address_book` (+ role views) and `rpt.dim_plant` but does **not** build them, every
entry point guards them at the right moment. The base dims (Delta) are **hard requirements**; the role views may live
only in the **SQL endpoint** (which Direct Lake binds to) and so are checked **best-effort** (Spark may not see them) —
the shared `_exists()` helper tries `spark.catalog.tableExists` then falls back to `spark.read.table(...).limit(1)`.

| Notebook | When | Check | On failure |
|---|---|---|---|
| `nb_eso1_gold_fact_sales_order_freight` | In[7] preflight, before seed + streams begin | base dims exist · non-empty; role views best-effort | **raises** (don't seed/stream a fact with FKs to absent dims) |
| `nb_eso1_gold_dim_item` | — (no reused-dim dependency) | none — `dim_item` has no FK to `rpt.*` | n/a |
| `nb_semantic_model_eso1` | preflight, before model bind | base dims + new tables exist; role views best-effort | **raises** on base/new; **warn** on role views |
| `nb_validate_gold_eso1` | §0 health + RI checks | exist · non-empty · unique PK · **role-view subset-of-base consistency** · freshness; + fact↔dim left-anti RI | logged **pass/fail** gate |
| `nb_maintenance_gold_eso1` | read-only status | confirm reused dims are **out of scope** (never OPTIMIZE/VACUUM them); report freshness, flag `⚠ STALE` > 7 days | informational only |

Each `nb/` notebook is **self-contained** (no `%run`): the two processors inline the full transforms
(`nb_eso1_gold_dim_item` the dim transform, `nb_eso1_gold_fact_sales_order_freight` the fact transform); `nb_validate` /
`nb_maintenance` inline the constants (`R_DIM_AB`, `R_DIM_PLANT`, role views) + `_exists` they need;
`nb_semantic_model` is standalone. Reused dims are never rebuilt or maintained here — their `old_nb` jobs own them.

---

## 4. Source-to-target: fact `fact_sales_order_freight`

**Driver:** `f4211_sales_order_detail_file` **UNION ALL `f42119_sales_order_history_file`** (v2.9 — history rows for
the open-order variations; guarded/optional, name confirmed in `full_metadata.json` — see the v2.9 block). **Join logic = Hubble spec §8**
(reproduced below). Silver is pre-decoded, so **no Julian/decimal scaling** is applied downstream.

### 4.1 Joins (join topology per spec §8; all previously-INNER gates relaxed to LEFT in v2.1)
| # | Table (Silver) | Type | Condition |
|---|---|---|---|
| 1 | `f4201_sales_order_header_file` | INNER | KCOO=SDKCOO, DOCO=SDDOCO, DCTO=SDDCTO |
| 2 | `f0101_address_book_master` (ship-to attrs) | LEFT | ABAN8 = SDSHAN — **no address-type gate** (was INNER `ABAT1 ∈ [A..P]∪[R..ZZZ]`); supplies denormalized filter attrs SIC/cat-05/cat-14/search_type |
| 3 | `f5642b11_…_detail` | LEFT | KCOO,DOCO,DCTO,LNID,**SHPN** (pre-collapsed `b11d`) |
| 4 | `f5642b01_…_header` | LEFT | KCOO,DOCO,DCTO,**SHPN** (pre-collapsed `b01d`) |
| 5 | `f4101_item_master` | LEFT | IMITM = SDITM (also supplies `item_segment_04`) |
| 6 | `…conversion_factors` (F41002) | LEFT | item-specific conversion UMITM=SDITM, UMRUM='TN', UMUM=SDUOM (**F41003 std fallback moved to DAX RELATED**); **plus** a separate `uom_str` lookup (item+input-UoM) for `uom_structure` (UMUSTR) |
| 7 | `f4074_price_adjustment_ledger_file` | LEFT | KCOO,DOCO,DCTO,LNID — **no `ALAST` whitelist**; collapsed to **one actual row per line** (`row_number` pick) |
| 8 | `f4981_freight_audit_history` (buckets) | LEFT | on `shipment_number` (pre-aggregated, §4.3) |
| 9 | `f4941_shipment_routing_steps` | LEFT | on `shipment_number` → `route_number` (RSRTN) |
| 10 | `f0101_address_book_master` (**sold-to** role, v2.9) | LEFT | ABAN8 = **SDAN8** — sold-to `name_alpha`/`address_type_01`/`report_code_add_book_005`/`_010` (the ship-to join #2 does not cover the sold-to; Days-Since-Invoice etc. filter the sold-to search-type) |
| 11 | `f0116_address_by_date` (**ship-to** postal address, v2.9) | LEFT | ALAN8 = SDSHAN, collapsed to the **latest-effective** row per address (`date_beginning_effective` desc) → `ship_to_city/_state/_zip/_address_1/_2/_country` (distinct from F4981 freight city/state) |

### 4.2 Filters — **REMOVED in v2.1 (fact carries all rows)**
All former Hubble WHERE filters are **dropped**; filtering is delegated to the semantic model / Power BI slicers (§4.4
filter fields). Specifically removed:
- ~~`line_type = 'S'`~~
- ~~`status_code_last <> '980'`~~
- ~~`company` (SDCO) `IN ('00640','00645')`~~ — `company` / `company_key_order_no` are still carried as columns for slicing
- ~~F4074 `price_adjustment_type` whitelist~~ — `price_adjustment_type` is carried as an actual value for slicing instead
- ~~F0101 `address_type_01` ship-to gate (INNER)~~ — join relaxed to LEFT
- ~~F4981 `vendor_invoice_number <> 'NULL'` gate~~ (§4.3)

> **`is_delete = 0` is NOT a business filter** — it is the CDC soft-delete mechanism of the Silver layer and is still
> applied inside `load_silver_table` (drops Silver rows tombstoned by the source). Only the *report* WHERE filters were removed.

### 4.3 Freight buckets (F4981 → shipment grain, then joined in)
Per Silver analysis §6.2. **v2.1: the `vendor_invoice_number <> 'NULL'` gate is removed** — all F4981 rows contribute.
`is_delete = 0` still applies (CDC soft-delete, not a report filter).

| Bucket | Rule |
|---|---|
| `billable_freight` | `SUM(net_amount)` where `billable_payable='B'` AND `charge_code_01='BFR'` |
| `billable_fuel` | … `'B'` AND `charge_code_01 ∈ ('FSC','FSB')` |
| `payable_freight` | … `'P'` AND `charge_code_01='PFR'` |
| `payable_fuel` | … `'P'` AND `charge_code_01='FSC'` |
| `total_billable` | `billable_freight + billable_fuel` |
| `total_payable` | `payable_freight + payable_fuel` |
| `total_freight` | `SUM(net_amount)` over **ALL** F4981 rows for the shipment — any `billable_payable`/`charge_code_01` (**v2.10 / H2**: the combined-freight reports — Baseline Finance, DE Orders, BP Freight — sum the whole shipment's FHNAMT; the billable/payable buckets **under-count** it if a charge code falls outside `{BFR,FSC,FSB,PFR}`). Deduped in DAX (measure **Total Freight**). |
| `freight_variance` | `billable_freight − payable_freight` |
| `total_variance` | `total_billable − total_payable` |
| `freight_cm_pct`, `total_cm_pct` | **DAX only** (SUM/SUM) — never stored per row |

`shift_factor_applied = 1.0` (identity). Silver `net_amount` is already decimal-resolved; the per-company **ShiftFactor**
(Hubble tie-out) comes from a company-constants table **not** among the 11 sources (open gap — §11).

### 4.4 Fact columns (spec §6 column list + buckets + keys/audit)
> **Full as-built column-by-column S2T:** see **`docs/ESO1_hubble_field_mapping.md` → "Gold landing — `fact_sales_order_freight` final column names"** (tables A–G). The summary below covers the column groups; the field-mapping doc is authoritative per-field.

**Natural FK columns (no surrogate keys):** `ship_to`, `bill_to`, `carrier_number` → `dim_address_book.address_number`;
`branch_plant` → `dim_plant.plant_code`; `item_number_short` → `dim_item`. **No date relationships** — the `*_date_key`
ints remain on the fact but are unused (no date dimension); dates are sliced from the raw date columns.
`address_number_parent` (SDPA8) is carried denormalized (no parent role view).
**Degenerate dims:** `company`, `company_key_order_no`, `order_type`, `order_number`, `line_number`, `shipment_number`,
`bol_number`, `invoice_number`, `original_document_type`/`original_po_so_number`/`original_document_no`, `reference_01`,
`user_reserved_reference`, `hold_orders_code`, `status_code_last`/`status_code_next`, `freight_handling_code`(+`_audit`),
`mode_of_transport`, `route_number`, `container_id`, `transaction_originator`, `delivery_instruct_line_01`/`_02`, `gl_class`,
`sales_reporting_code_01`/`_03`, `uom_pricing`.
**Denormalized booking/ocean (shipment grain, DAX-deduped):** `seal_no`, `booking_no`, `destination_port`,
`dest_point_name_alpha`, `no_of_container`, `ocean_del_terms`, `vessel_name`, `freight_city`/`freight_state`/`freight_zip`.
**Added measures (line grain):** `primary_quantity_ordered` (SDPQOR), `transaction_quantity` (SDUORG).
**Filter (slicer) fields denormalized onto the fact (v2.1):** `price_adjustment_type` (F4074 ALAST), `standard_industry_code`
(ship-to F0101 ABSIC), `category_code_05` (ABAC05), `category_code_14` (ABAC14), `search_type` (ABAT1), `uom_structure`
(F41002 UMUSTR), `payment_terms` (F4211 SDPTC), `item_segment_04` (F4101 IMSEG4). These support Power BI slicers directly.
**Filter Capture additions (v2.9, 2026-07-15) — 21 columns for the ~110 variations (README §3):**
`extended_price` (SDAEXP), `extended_cost` (SDECST), `currency_code` (SDBCRC), `backorder_qty` (SDSOBK),
`cancelled_qty` (SDSOCN), `qty_to_date` (SDQTYT), `open_qty` (SDUOPN), `line_description_1`/`_2` (SDDSC1/2),
`date_updated` (SDUPMJ), `address_rate` (ship-to ABURAT), `sold_to_name`/`sold_to_search_type`/`sold_to_category_05`/`_10`
(sold-to F0101 join #10), `ship_to_city`/`_state`/`_zip`/`_address_1`/`_2`/`_country` (F0116 join #11). All page-filter/display fields;
no report filter is applied in Gold.
> **Grain guards (keep the order-line grain):** F5642B11/F5642B01 are pre-collapsed to one row per join key (`b11d`/`b01d`,
> `F.first` ignore-nulls); **F4074** is reduced to one actual row per line via a `row_number` pick; `uom_str` is one row per
> item+input-UoM; and a final `dropDuplicates(["sales_order_line_key"])` guarantees exactly one fact row per line.

| Target | Source | JDE | Notes |
|---|---|---|---|
| `second_item_number` | F4211 | SDLITM | leading-digit spell-out (naming §4.3) |
| `line_type` | F4211 | SDLNTY | |
| `order_number`/`order_type`/`company_key_order_no`/`company` | F4211 | SDDOCO/SDDCTO/SDKCOO/SDCO | |
| `line_number` | F4211 | SDLNID | |
| `branch_plant` | F4211 | SDMCU | FK→dim_branch_plant |
| `ship_to`/`bill_to` | F4211 | SDSHAN/SDAN8 | FK→dim_customer |
| `carrier_number` | F4211 | SDCARS | FK→dim_carrier |
| `freight_handling_code` | F4211 | SDFRTH | (F4981 FHFRTH = `freight_handling_code_audit` alt) |
| `next_status_num` | calc | — | **v2.10 / M6**: physical INT `CAST(TRIM(status_code_next) AS INT)`. Direct Lake can't reliably range-filter the string status (5 reports use `next < 561/575/620` or `BETWEEN 574 AND 620`); blank/non-numeric → NULL & excluded, matching Hubble. Filter this in Power BI; display the string `status_code_next`. |
| `actual_ship_date` | F4211 | SDADDJ | raw date, sliced directly; `ship_year_week` derived from it |
| `gl_date` | F4211 | SDDGL | raw date, sliced directly |
| `invoice_date`/`invoice_number` | F4211 | SDIVD/SDDOC | invoice_date = raw date, sliced directly |
| `shipment_number` | F4211 | SDSHPN | degenerate bridge |
| `bol_number` | F4211 | SDURAB | user reserved |
| `mode_of_transport` | F4211 | SDMOT | |
| `uom` | F4211 | SDUOM | |
| `quantity_shipped` | F4211 | SDSOQS | raw units |
| `conversion_to_tons_rate` | F41002 | UMCONV | item-specific TN factor (F41003 fallback via DAX); **NULL if unresolved**; `missing_conversion_flag` |
| `quantity_shipped_tons` | calc | — | `quantity_shipped × conversion_to_tons_rate` (**NULL when rate unresolved**) |
| `price_per_unit` | F4211 | SDUPRC | |
| `price_quantity_shipped` | calc | — | `price_per_unit × quantity_shipped` (spec col 29) |
| `major_prod_code`/`minor_prod_code` | F4211 | SDSRP2/SDSRP4 | |
| `freight_factor_value` | F4074 | ALUPRC | **actual row value**, one row/line (`row_number` pick — no aggregation) |
| `price_adjustment_type` | F4074 | ALAST | actual row value (slicer); same one-row/line pick as `freight_factor_value` |
| `standard_industry_code`/`category_code_05`/`category_code_14`/`search_type` | F0101 (ship-to) | ABSIC/ABAC05/ABAC14/ABAT1 | denormalized filter attrs (LEFT join) |
| `uom_structure` | F41002 | UMUSTR | filter attr (item+input-UoM lookup) |
| `payment_terms` | F4211 | SDPTC | filter attr |
| `item_segment_04` | F4101 | IMSEG4 | filter attr |
| `route_number` | F4941 | RSRTN | LEFT join (often 0) |
| `item_name` | F4101 | IMDSC1 | also on dim_item |
| `billable_freight`,`billable_fuel`,`total_billable`,`payable_freight`,`payable_fuel`,`total_payable`,`total_freight`,`freight_variance`,`total_variance` | F4981 | FHNAMT | shipment buckets (§4.3), denormalized (`total_freight` = all-charge-code shipment total, **v2.10 / H2**) |
| `is_primary_shipment_line` | calc | — | 'Y' on 1 line/shipment (freight dedup anchor) |
| `extended_price` | F4211 | SDAEXP | v2.9 — the sales-amount measure (`SUM` in ~60 variations); was missing |
| `extended_cost` | F4211 | SDECST | v2.9 |
| `currency_code` | F4211 | SDBCRC | v2.9 — line domestic currency (no F0010 source needed) |
| `backorder_qty`/`cancelled_qty`/`qty_to_date`/`open_qty` | F4211 | SDSOBK/SDSOCN/SDQTYT/SDUOPN | v2.9 — order-qty measures |
| `line_description_1`/`line_description_2` | F4211 | SDDSC1/SDDSC2 | v2.9 — the LINE's own text (≠ item_name from F4101) |
| `date_updated` | F4211 | SDUPMJ | v2.9 |
| `address_rate` | F0101 (ship-to) | ABURAT | v2.9 — sole measure of the customer-rate variations (ADM/Chevron/…) |
| `sold_to_name`/`sold_to_search_type`/`sold_to_category_05`/`sold_to_category_10` | F0101 (sold-to, join #10) | ABALPH/ABAT1/ABAC05/ABAC10 | v2.9 — sold-to attrs (SDAN8); ship-to join #2 doesn't cover these |
| `ship_to_city`/`ship_to_state`/`ship_to_zip`/`ship_to_address_1`/`ship_to_address_2`/`ship_to_country` | F0116 (join #11) | ALCTY1/ALADDS/ALADDZ/ALADD1/ALADD2/ALCTR | v2.9 — latest-effective ship-to postal address (≠ F4981 freight city/state) |
| `shift_factor_applied` | const | — | 1.0 placeholder |
| `sales_order_line_key` | calc | — | business key (hash); **unique — one row per line** |
| `order_scope_key` | calc | — | `sha2(company_key_order_no‖order_type‖order_number)` — **vestigial** under the batch build (was the CDC delete scope); still stored so the schema/semantic model are unchanged (§5) |
| ~~`is_deleted`,`source_commit_timestamp`,`gold_updated_timestamp`,`record_hash`~~ | — | — | **no audit columns on any built table (v2.2)** — see §5 |

### 4.5 UoM → TN conversion (ESO7 v2 fact approach — v2.7)
Item-specific `F41002` only: `conv_rate = coalesce(when uom='TN' → 1.0, F41002 item factor)` where the F41002 union is
UMRUM='TN' fwd + UMUM='TN' reciprocal rev (`1/factor`). The standard-UoM `F41003` leg is **no longer cascaded in the
notebook** — that fallback is served by the reused **`dim_uom_conversion`** dimension (`lh_jde_gold.eso7.dim_uom_conversion`,
grain `from_uom` → `std_factor`; built/maintained by **`nb_silver_to_gold_dim_f41003.py`**) and applied as the Tier-B leg
of the **Total Tons** DAX measure via `RELATED`. Unresolved conversions stay **`NULL`** (no blanket
`1.0`), so `missing_conversion_flag='Y'` marks them and `quantity_shipped_tons` is `NULL` for those lines. **v2.1:** F4074 (many adjustments/line) is
collapsed to **one actual row per line** with a `row_number` pick *before* the join (not a post-projection `DISTINCT`), so
the line grain is guaranteed and the MERGE key stays unique.

### 4.6 Second fact — `fact_sales_commission` (SOP0027 Commission) — **v2.15 (2026-07-24 — F4211-DRIVEN)**
The ONE report the order-line freight fact cannot serve — SOP0027's commission measures come from the JDE Sales
Commission ledger **F42005**. It is a **separate fact by grain** (split on grain, never on filter): F42005 is
one row per commission record `SCKCOO+SCDCTO+SCDOCO+SCLNID+**SCCMLN**` (sales line × salesperson × commission rule),
which cannot be denormalized onto the coarser freight fact without fanning it out. Built by
**`nb_eso1_gold_fact_sales_commission.py`** → `lh_jde_gold.rpt.fact_sales_commission`, same conventions as the
freight fact (self-contained, **batch full-snapshot overwrite** — v2.16, no audit columns, **no report filters** —
all page-level).

**⚠ v2.15 driver flip (2026-07-24):** the earlier F42005-driven build silently *dropped* sales lines with no
commission, whereas SOP0027's query is **F4211-driven** (`FROM F4211 … LEFT JOIN F42005`) and *shows* them (blank
commission). Flipped to match: **driver = F4211 ∪ F42119**, F42005 LEFT-joined. Grain is now **sales line ×
commission record** — a no-commission line appears once with NULL commission columns; a line with N commission
records fans to N rows. This also drops orphan F42005 commissions whose order isn't in Silver `F4211∪F42119`
(~76% — a data-completeness reality, orders older than the load window), exactly as Hubble's INNER F4211 does. See
`docs/SOP0027_Commission_Driver_Investigation.md`. Schema unchanged → semantic model + TMDL twin unaffected.

- **Grain / keys:** `sales_commission_key = sha2(company_key_order_no‖order_type‖order_number‖line_number‖COALESCE(commission_line_number,'__NOCOMM__'))`
  (sentinel keeps no-commission lines unique); `order_scope_key = sha2(company_key_order_no‖order_type‖order_number)` (CDC delete scope).
- **Driver = F4211 ∪ F42119** (all sales lines; F42119 history rows for `next=999` lines purged from active F4211).
  **F42005 LEFT-joined** on `KCOO+DCTO+DOCO+LNID` for the commission columns (null when a line earns none). **F4201**
  header LEFT-joined for `sold_to` (SHAN8); **F0101** LEFT-joined (ABAT1 gate **relaxed**, no filter) for
  `category_code_10` (ABAC10 — the FK code only; its description is resolved by the **`dim_category_code_10`** UDC dim,
  not denormalized). The query's INNER F4201/F0101 + ABAT1 band → Power BI page filters (`sold_to IS NOT NULL`,
  `sold_to_search_type` in A–P/R–ZZZ).
- **Sources (batch, v2.16):** `FACT_SOURCES` = F4211 spine (driver) · F42005 LEFT (commission) · F42119 optional-union
  (history) · F4201 static (header) · F0101 static (sold-to). All read as full snapshots inside `build_fact()`; the
  F42119 history rows are unioned into the line context when present (a `next=999` line purged from active F4211).
- **44 business + 2 keys = 46 stored columns** (incl. `status_code_next`/`status_code_last` for the page filters).
  `category_code_10`'s description is resolved by the **`dim_category_code_10`** dimension (below), not stored on the fact.
  Commission measures (F42005, Silver pre-decoded — no ×0.01 / ÷1000):
  `percent_commission` (SCCPCT), `amount_commission` (SCCOMA), `amount_related_commission` (SCCOMR),
  `percent_related_commission` (SCCPCR), `flat_commission_amount` (SCFCA), `amount_per_unit` (SCAPUN),
  `amount_sales_total_line` (SCTOTL), `amount_sales_line_total_cost` (SCLRCS), `amount_line_gross_margin` (SCMRGL),
  `amount_line_eligible_margin` (SCELIL). Identity: `salesperson` (SCSLSP → FK dim_address_book), `commission_code_type`
  (SCCCTY), `commission_line_number` (null when no commission). Sales-line context (from the F4211 driver; repeats
  across a line's commission rows — **DAX-dedup via `CALCULATE(SUM(…), is_primary_commission_line="Y")`**):
  `extended_price`/`extended_cost`/`quantity_shipped`/
  `primary_quantity_ordered`/`invoice_number`/`second_item_number`/`line_type`/`uom_primary`/`uom_pricing`/
  `sales_reporting_code_05`. Dates: `commission_paid_date` (SCCMDJ), `gl_date`, `actual_ship_date` (+ `*_date_key`).
- **Dims reused:** `dim_address_book` role views (ship_to / sold_to / **dim_address_salesperson** — added to the model + TMDL twin 2026-07-24), `dim_plant`, `dim_item`, and **`dim_category_code_10`** (ABAC10 → description, UDC 01/10 — new dim, 2026-07-25; built by `nb_eso1_gold_dim_category_code_10.py`). No date dimension. See the two-fact model in [semantic model memory].
- **Same H1 caveat:** the line amounts still carry `shift_factor_applied = 1.0`, not the real per-company factor —
  reconcile before trusting the dollars. Commission amounts are Silver-decoded, no scaling.
- **Companion measures** (add to a commission semantic model, all shipment-safe / line-dedup as noted): `Commission $`
  = SUM(`amount_commission`); `Commission %` (weighted); `Sales Line Amount` = `SUMX(VALUES(sales-line), MAX(extended_price))`.

---

## 5. Build execution — batch, audit-free full-snapshot overwrite (v2.16)

**No built table stores audit columns** (v2.2). Each notebook reads the full Silver snapshot of every source, runs
`build_*()` **once**, and **overwrites** its target table — there is no `cdc_merge`, no `record_hash`, no soft-delete flag,
and no per-order/per-item merge. A full overwrite replaces the whole table each run, so "deletes" and "updates" are handled
implicitly: a row absent from the current Silver snapshot is simply absent from the rebuilt table.

**Every built table — plain overwrite (no Gold CDF).** The RUN section drops the table first (to clear the old
streaming-era `enableChangeDataFeed` property) then writes:
```python
if MANUAL_OVERWRITE or not spark.catalog.tableExists(T_TARGET):
    spark.sql(f"DROP TABLE IF EXISTS {T_TARGET}")
    new = build_fact()                                    # or build_dim_item() / build_dim() — read full snapshots, join, project
    (new.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(T_TARGET))                            # plain overwrite — NOT enableChangeDataFeed (matches ESO4/ESO5)
```
- **`fact_sales_order_freight` / `fact_sales_commission`** — `build_fact()` re-derives the whole fact from the current
  static snapshots (exactly the §4 joins), `dropDuplicates` on the business key, and the RUN overwrites. The vestigial
  `order_scope_key` (the old CDC delete scope) is still stored so the schema/semantic model are unchanged.
- **`dim_item` / `dim_category_code_10`** — `build_dim_item()` / `build_dim()` are Type-1 rebuilds: select the business
  columns, `dropDuplicates` on the natural PK, overwrite. No MERGE upsert/delete.
- **`MANUAL_OVERWRITE`** — `True` drops + rebuilds; `False` builds only if the target is missing (so a plain re-run refreshes).
- There is no date dimension — dates are the facts' raw date columns, sliced directly.

> **Result-identical to the previous streaming full-load seed.** `build_*()` is the old `transform_*()` with only the dead
> streaming scope-filter (`restrict_*`, never exercised on a full build) removed; the transforms, `FACT_BUSINESS_COLS`
> lists, helpers, and constants are byte-identical (AST-verified 2026-07-26). See §6 and the v2.16 changelog note.

---

## 6. Batch build implementation (full-snapshot overwrite)

Each of the four Gold builders is a **self-contained batch notebook** (transforms + build + overwrite, no `%run`),
running as an **independent Fabric job**. They share the ESO4/ESO5 section layout:

- **Facts — 4 sections** `1) CONFIG · 2) FACT BUILDER · 3) FACT SOURCES · 4) RUN`:
  - **`nb_eso1_gold_fact_sales_order_freight`** — `build_fact()` reads F4211 (∪ F42119 if present) + 14 lookup snapshots
    (F4201/F0101/F4101/F0116/F41002/F4074/F4941/F4981/F5642B01/F5642B11/F5549002/F03012/F49211/F4106), joins per §4,
    `dropDuplicates(["sales_order_line_key"])`, overwrites `fact_sales_order_freight`. **Owns** the reused-dim preflight (§3.5).
  - **`nb_eso1_gold_fact_sales_commission`** — `build_fact()` drives F4211 (∪ F42119), LEFT-joins F42005, F4201, F0101,
    overwrites `fact_sales_commission` (§4.6). Owns its reused-dim + F4211-driver + F42005 preflight.
  - Each declares a **`FACT_SOURCES`** inventory (`{silver, join, join_pairs}`) that drives a RUN **source preflight**
    (prints OK / MISSING / OPTIONAL-missing before building; F42119 is optional).
- **Dims — 3 sections** `1) CONFIG · 2) DIM BUILDER · 3) RUN`:
  - **`nb_eso1_gold_dim_item`** — `build_dim_item()` ← F4101; **no** reused-dim preflight (dim_item has no FK to `rpt.*`).
  - **`nb_eso1_gold_dim_category_code_10`** — `build_dim()` ← F0005 UDC 01/10 (same shape as ESO4 `dim_udc`).

**RUN section (all four):** `CREATE SCHEMA IF NOT EXISTS` → preflight (reused dims where applicable + source existence) →
`build_*()` once → `DROP TABLE IF EXISTS` + plain `mode("overwrite")` write → a JSON **exit payload**
(`notebookutils.notebook.exit`, `{status, table, rows, elapsed_sec, end_time_utc}`).

```python
# 4) RUN — batch build (nb_eso1_gold_fact_sales_order_freight; identical shape in all four notebooks)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
# … reused-dim preflight (facts) + source preflight over FACT_SOURCES …
if MANUAL_OVERWRITE or not spark.catalog.tableExists(T_FACT):
    spark.sql(f"DROP TABLE IF EXISTS {T_FACT}")
    new = build_fact()
    (new.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_FACT))
notebookutils.notebook.exit(json.dumps(_exit_payload))
```

**What was removed (v2.16, streaming → batch):** all `readStream` / `readChangeFeed`, the `foreachBatch` handler factories
(`make_dim_item_handler` / `make_fact_f4211_handler` / `make_fact_f4981_handler` / `make_fact_f42005_handler`),
`recompute_fact` / `upsert_dim_item` / `_upsert_dim`, `_write_new_table`, `_FACT_LOCK` / `threading`, `current_version`,
the checkpoint machinery (`_checkpoints_exist`, `_start_ver`, `init_ver`, stop-leftover-streams), the full-load-vs-resume
gate, `awaitAnyTermination`, `TRIGGER` / `CKPT` / `ENV`, and the `DeltaTable` import. No checkpoints, no Change Data Feed,
no continuous trigger.

**Refresh cadence:** the fact/dim are refreshed by **re-running the notebook** (or a Fabric pipeline / schedule) with
`MANUAL_OVERWRITE = True`, not by a 30 s continuous stream. A `pl_*` pipeline can chain the four builders (dims first, then
the two facts) → `nb_validate_gold_eso1`, on whatever schedule the reporting SLA needs.

> **First run per notebook: `MANUAL_OVERWRITE = True`** to materialize the batch schema (and clear any old streaming-era
> Gold CDF property), then set it to `False` for subsequent build-if-missing behavior. Build **dims first, then the two facts**.

---

## 7. Semantic model (`billable_payable_freight`, Direct Lake)

### 7.1 Relationships (single direction, dim → fact)
| From (dim, 1) | To (fact, *) | Active |
|---|---|---|
| `dim_address_ship_to[address_number]` (rpt) | `fact…[ship_to]` | ✅ active |
| `dim_address_sold_to[address_number]` (rpt) | `fact…[bill_to]` | ✅ active |
| `dim_address_carrier[address_number]` (rpt) | `fact…[carrier_number]` | ✅ active |
| `dim_item[item_number_short]` (rpt) | `fact…[item_number_short]` | ✅ |
| `dim_plant[plant_code]` (rpt) | `fact…[branch_plant]` | ✅ |

**No date relationships** — there is no date dimension. Dates are the fact's raw date columns, sliced directly
(weekly grouping via `fact…[ship_year_week]`). All relationships are 1:* with single cross-filter (dim → fact). The
built ESO1 tables (fact, `dim_item`) and the reused address/plant dims all live in `rpt`; the generator
emits a per-table `schemaName`. The three address roles use the existing `dim_address_*` views so each gets an active
relationship; if a view forces Direct Lake → DirectQuery fallback, relate to the physical `dim_address_book` with
`USERELATIONSHIP` instead.

### 7.2 Key measures (ratios = SUM/SUM; freight deduped per shipment)
```DAX
-- Freight $ deduped to shipment grain (correct under any line-level filter)
Total Billable  = SUMX(VALUES(fact_sales_order_freight[shipment_number]),
                       CALCULATE(MAX(fact_sales_order_freight[total_billable])))
Total Payable   = SUMX(VALUES(fact_sales_order_freight[shipment_number]),
                       CALCULATE(MAX(fact_sales_order_freight[total_payable])))
Billable Freight= SUMX(VALUES(fact_sales_order_freight[shipment_number]),
                       CALCULATE(MAX(fact_sales_order_freight[billable_freight])))
Payable Freight = SUMX(VALUES(fact_sales_order_freight[shipment_number]),
                       CALCULATE(MAX(fact_sales_order_freight[payable_freight])))
Total Variance  = [Total Billable] - [Total Payable]
Freight Variance= [Billable Freight] - [Payable Freight]
Freight CM %    = DIVIDE([Freight Variance], [Billable Freight])
Total CM %      = DIVIDE([Total Variance],  [Total Billable])
Freight Shipments = DISTINCTCOUNT(fact_sales_order_freight[shipment_number])

-- Order-line measures (line grain — plain SUM is correct)
Quantity Shipped Tons = SUM(fact_sales_order_freight[quantity_shipped_tons])
Price Quantity Shipped= SUM(fact_sales_order_freight[price_quantity_shipped])
Order Lines           = COUNTROWS(fact_sales_order_freight)

-- No date-role measures (no date dimension). For "$ by GL/invoice date", slice the
-- fact's raw gl_date / invoice_date column on the visual.
```
> The `MAX(...)` inside `SUMX(VALUES(shipment_number), …)` returns the single denormalized bucket value per shipment, so
> the freight is counted **once per shipment** regardless of how many lines are in filter context — no inflation.
> `is_primary_shipment_line='Y'` is available as a simpler alternative (`CALCULATE(SUM(...), fact[is_primary_shipment_line]="Y")`).

### 7.3 Performance & refresh
- **Direct Lake** on native Delta tables (no import refresh) → each batch overwrite is visible to the model within the
  framing window after the notebook runs. Keep visuals on **measures + dim attributes**; avoid dumping high-cardinality raw
  keys on one visual (prevents Direct Lake → DirectQuery fallback).
- **Liquid clustering**: fact on `(branch_plant, shipment_number)`; freight lookups on `shipment_number`.
- **Deletion vectors** ON; **V-Order** ON (Fabric default); optional **OPTIMIZE/VACUUM** (`nb_maintenance_gold_eso1`). A full
  overwrite already rewrites the whole table each run (so it does not accumulate the many small files frequent MERGEs would),
  making frequent OPTIMIZE less critical than under the streaming design.
- **Refresh cadence** — batch: re-run the builder (or a scheduled pipeline) on the reporting SLA, rather than a 30 s stream.
- **Model hygiene:** **no built table has `is_deleted`/`record_hash`** (v2.2 — a full overwrite replaces the table), so **no
  `is_deleted = false` filter is needed anywhere** (facts or dims). Hide the hash/scope keys
  (`sales_order_line_key`, `order_scope_key`, `*_date_key`); set `year_week`/`status_sort` sort-by columns.

---

## 8. Validation (`nb_validate_gold_eso1`)

| Check | Method |
|---|---|
| **Reused-dimension health** (§0, preflight) | base dims `rpt.dim_address_book` & `rpt.dim_plant` exist · non-empty · unique PK (**hard**); role views (best-effort via `_exists`, SQL-endpoint-only is OK) non-empty + subset-of-base consistency; **attribute usability** (`name_alpha`/`plant_name` populated %); **coverage** (% of fact `ship_to`/`bill_to`/`carrier_number`/`branch_plant` resolving in the dim); `last_refreshed_timestamp` — last three informational |
| **Record-count reconciliation** | Gold fact line count vs Silver F4211 line count (v2.1: **no §4.2 filters** — all `is_delete=0` lines land) |
| **Data completeness** | null-rate on mandatory keys (`order_number`, `shipment_number`, FKs); missing-conversion rate |
| **Duplicate detection** | `sales_order_line_key` unique (count of keys with >1 row = 0); shipment buckets unique per shipment |
| **Key integrity** | every fact FK resolves in its dim (left-anti = 0): ship_to/carrier/item/branch_plant/date keys |
| **Fact ↔ dimension RI** | no orphan dims referenced; no fact key absent from dim |
| **Report reconciliation** | Gold `SUM(total_billable)`/`total_payable`/`total_variance` (deduped by shipment) vs Hubble "BvP Combined" control totals; freight-shipment count |
| **Freight dedup proof** | `SUM` over `is_primary_shipment_line='Y'` == `SUMX(VALUES(shipment),MAX(bucket))` |

Validation writes a `lh_jde_gold.rpt.eso1_validation_log` row per run (check, expected, actual, pass/fail, timestamp).

---

## 9. Artifacts (all names per the naming PDF)

| Artifact | Name | Purpose |
|---|---|---|
| Lakehouse | `lh_jde_gold` | Gold store |
| Schema | `eso1` | Extended Sales Order 1 (order-to-cash domain) |
| Notebook | `nb_eso1_gold_dim_item` | **BATCH (dim) — self-contained** builder: `build_dim_item()` ← F4101 → overwrite `dim_item`; `MANUAL_OVERWRITE`; no preflight (§6) |
| Notebook | `nb_eso1_gold_dim_category_code_10` | **BATCH (dim) — self-contained** builder: `build_dim()` ← F0005 UDC 01/10 → overwrite `dim_category_code_10`; `MANUAL_OVERWRITE`; no preflight |
| Notebook | `nb_eso1_gold_fact_sales_order_freight` | **BATCH (fact) — self-contained** builder: reused-dim + source preflight + `build_fact()` (F4211 ∪ F42119 + 14 lookups) → overwrite `fact_sales_order_freight`; `MANUAL_OVERWRITE`; 4-section layout |
| Notebook | `nb_eso1_gold_fact_sales_commission` | **BATCH (fact) — self-contained** builder: reused-dim + F4211/F42005 preflight + `build_fact()` (F4211 driver, F42005 LEFT) → overwrite `fact_sales_commission`; `MANUAL_OVERWRITE`; 4-section layout (§4.6) |
| Notebook | `nb_validate_gold_eso1` | validation suite — **self-contained** |
| Notebook | `nb_maintenance_gold_eso1` | OPTIMIZE / VACUUM — **self-contained** |
| Notebook | `nb_semantic_model_eso1` | Direct Lake model + relationships + measures — **self-contained** |
| | | *all notebooks have **no `%run`** — each declares its own constants/transforms inline* |
| Fact | `lh_jde_gold.rpt.fact_sales_order_freight` | single consolidated freight fact |
| Fact | `lh_jde_gold.rpt.fact_sales_commission` | SOP0027 commission fact (sales line × commission record; §4.6) |
| Dim (NEW) | `lh_jde_gold.rpt.dim_item` | genuinely-new dim built here (no date dimension — dates are raw fact columns) |
| Dim (NEW) | `lh_jde_gold.rpt.dim_category_code_10` | ABAC10 → description (UDC 01/10) for SOP0027 |
| Dims (REUSED) | `lh_jde_gold.rpt.dim_address_book` (+ `dim_address_ship_to`/`_sold_to`/`_carrier`/`_salesperson` views), `lh_jde_gold.rpt.dim_plant` | conformed; built by `old_nb`/other jobs |
| Pipeline (optional) | `pl_fact_sales_order_freight` | batch schedule: dims (`nb_eso1_gold_dim_item`, `nb_eso1_gold_dim_category_code_10`) → facts (`nb_eso1_gold_fact_sales_order_freight`, `nb_eso1_gold_fact_sales_commission`) → `nb_validate_gold_eso1` |
| Deployment pipeline | `dpl_jde` | dev→test→prod promotion |
| Semantic model | `billable_payable_freight` | Direct Lake |

---

## 10. Build & run order
Batch builds — **dims first, then the two facts** (facts read F4101/F0005-derived context; the dims must exist for the
model to bind). Set `MANUAL_OVERWRITE = True` on the first run of each notebook, then flip to `False`.
0. Ensure the REUSED dims exist (`rpt.dim_address_book` + role views, `rpt.dim_plant`) — built by their `old_nb`/other jobs.
1. **`nb_eso1_gold_dim_item`** (batch dim job) → `build_dim_item()` ← F4101 → overwrite `dim_item`. No preflight.
2. **`nb_eso1_gold_dim_category_code_10`** (batch dim job) → `build_dim()` ← F0005 UDC 01/10 → overwrite `dim_category_code_10`.
3. **`nb_eso1_gold_fact_sales_order_freight`** (batch fact job) → reused-dim + source preflight → `build_fact()` → overwrite
   `fact_sales_order_freight`.
4. **`nb_eso1_gold_fact_sales_commission`** (batch fact job) → reused-dim + F4211/F42005 preflight → `build_fact()` →
   overwrite `fact_sales_commission` (§4.6).
5. `nb_validate_gold_eso1` → confirm counts / keys / reconciliation (run after each rebuild).
6. `nb_semantic_model_eso1` → (re)generate the Direct Lake model + measures.
7. `nb_maintenance_gold_eso1` → optional OPTIMIZE/VACUUM. *(Steps 1–4 can be chained by a `pl_*` pipeline on the reporting SLA.)*

---

## 11. Open items (documented assumptions, not blockers)
1. **ShiftFactor** — per-company multiplier for exact Hubble $ tie-out is in a company-constants table outside the 11 sources; `shift_factor_applied = 1.0` until wired in.
2. **Route #** — sourced from `f4941_shipment_routing_steps` (RSRTN) per the chosen option; frequently `0`. F4981 also carries a route number if F4941 proves unreliable.
3. **CDC commit timestamp** — **no built table stores** `source_commit_timestamp` (v2.2). If a load-time audit is ever
   needed, capture the CDF `_commit_version`/`_commit_timestamp` in the handler rather than persisting a per-row column.
4. **Company filter column** — filter on `company` (SDCO) per the deployed WHERE; "Company" display = `company_key_order_no` (SDKCOO) per the column list. Both retained.

## 12. Superseded by this version (archived)
**Archived to `old_nb/` (2026-06-23):** `nb_fact_sales_order_line.py`, `nb_fact_freight_audit.py`,
`nb_dim_shipment.py`, `nb_maintenance_eso1.py` — the two-fact build. Retained for reference; do not run alongside the
single-fact build.

**Also archived to `old_nb/` (originally merged into the combined `nb_eso1_gold_streaming`):** `nb_backfill_gold_eso1.py`,
`nb_stream_silver_to_gold.py`. **And `nb_eso1_transforms.py`** — now archived too: **every `nb/` notebook is
self-contained** (no `%run`). The two processors inline the full transforms; `nb_validate` / `nb_maintenance` inline the
few constants + `load_silver_table` they need; `nb_semantic_model` was already standalone. This removes the cross-notebook
`%run` dependency entirely (which avoids the "notebook not found" resolution failure).

**Split into two self-contained notebooks (2026-07-07, v2.6):** the combined `nb_eso1_gold_streaming` processor was
**split** into `nb_eso1_gold_dim_item` (builds `dim_item`; 1 CDF stream; checkpoint root `eso1_dim_<ENV>`; own `OVERWRITE`;
no preflight) and `nb_eso1_gold_fact_sales_order_freight` (builds the fact; 2 CDF streams; checkpoint root
`eso1_fact_<ENV>`; own `OVERWRITE`; owns the reused-dim preflight; reads F4101 statically). The pre-split
`nb_eso1_gold_streaming` is archived to `old_nb/`. Transforms/business logic are byte-for-byte unchanged.

**Rewritten in place (not archived):** `nb/nb_semantic_model_eso1.py` now contains the single-fact Direct Lake model.

**Also archived to `old_nb/` (2026-06-23):** `generate_tmdl_semantic_model.py` and
`eso1_billable_payable_freight.SemanticModel/` (old two-fact TMDL generator + output). The model is now generated by
`nb_semantic_model_eso1` (§7).

**Rebound (2026-06-23):** `report/ESO1_Billable_v_Payable_Freight.Report` (PBIR) now binds (`byPath`) to a new local
Direct Lake model `report/billable_payable_freight.SemanticModel`, generated by `report/generate_tmdl_semantic_model.py`
(matches the workspace model from `nb_semantic_model_eso1`). All 26 visual field references were remapped to the single
fact + new dims and verified to resolve; obsolete fields (cost %, status, has_freight) were remapped/removed. Fill the
SQL-endpoint placeholders in `…SemanticModel/definition/expressions.tmdl` before refresh.
