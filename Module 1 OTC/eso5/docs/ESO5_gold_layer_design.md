# ESO5 — Gold Layer Design (`fact_extended_sales_order_5`)

**Report:** Extended Sales Order 5 — *Sandbox Load Report with PO Details*. Stakeholder Kristyn Roe;
audience Sandbox Logistics/Supply Chain; on-demand. Sources: `eso5/Extended Sales Order 5.docx` (spec),
`eso5/full_metadata.txt` (JDE→snake_case), `eso5/hubble query.txt` (reference SQL), and
`eso5/Filter Capture/*.txt` (the four report variations). **All docx §4 joins applied; NO docx §5 filters
applied.**

Built to the **ESO4 BATCH architecture** (self-contained notebook: read the full Silver snapshot →
`transform_*()` → overwrite, gated by `MANUAL_OVERWRITE`). §7 Calculations = **N/A** (no business calc
like ESO4's business_stream).

> **⚙ 2026-07-26 — STREAMING → BATCH (both ESO5 notebooks).** `nb_eso5_gold_fact_extended_sales_order_5` and
> `nb_eso5_gold_dim_uss_plant` were converted from the CDF-streaming design to plain **batch builds**, matching
> ESO4's current notebooks. Removed: `readStream`/`writeStream`/`foreachBatch`, the per-source handlers +
> `recompute_fact` / `_upsert_dim`, checkpoints, `init_ver`, `awaitAnyTermination`, `trigger(30s)`, and the Gold
> `delta.enableChangeDataFeed` write option. **`OVERWRITE` → `MANUAL_OVERWRITE`**; each notebook now reads the full
> source snapshot, runs its (byte-for-byte unchanged) `transform_*()`, and overwrites its Gold table, ending with a
> JSON exit payload. **Results are IDENTICAL** to the previous streaming full-load seed (only the incremental
> scaffolding was removed). CDF is no longer required on F4211 / F4311 / F0005. Structure also follows ESO4's
> numbered sections (fact: CONFIG / FACT BUILDER / FACT SOURCES / RUN; dim: CONFIG / DIM BUILDER / RUN). **The
> §1 architecture below is rewritten for batch; any residual "CDC recompute" / "stream" wording in the later
> business-logic sections is SUPERSEDED — read it as "the transform each batch build runs."**

### The five reports, one fact (2026-07-14)

`fact_extended_sales_order_5` is the **single** fact for the core report **and all four Filter-Capture
variations** — there is deliberately no second fact table.

| # | Query | Relationship to the core |
|---|---|---|
| 1 | `hubble query.txt` — Sandbox Load Report with PO Details | the **core**: SBXLOADPOVIEW ⟕ F554201T, SBXUSSSAND, F0101 |
| 2 | `…PO Details (New)` | **identical query** to the core; only the §5 date range differs → nothing to build |
| 3 | `…PO Details` | strict **column subset** of the core, SBXUSSSAND join dropped; its `SDLITM='FRT'` / `SDNXTR<'581'` are §5 filters → nothing to build |
| 4 | `…PO Details for HOLADD` | **new rows.** Its `SBXLOADPOVIEWHOLADD` view is a UNION of (a) the F4211 SX lines *where* `SDLITM='HOLADD'` — exactly the rows the core view **excludes** — and (b) **orphan F4311 OX HOLADD PO lines** (§3d) |
| 5 | `SBX Load Reconciliation Report` | **different grain.** Its `SBXLOADDETAIL` view is F4201 ⋈ F4211 aggregated to **one row per load**, pivoting the lines into SANDWEIGHT / EXTWEIGHT / MILES / LOFADET / FRTAMT / HOLAMT …; it **keeps** the TL and HOLADD lines the core drops, and adds F4201 + four more F554201T columns (§3e) |

They are unified by keeping the fact at **line grain**, **widening the row population** to the union of
all five, and tagging every row with **`row_class`** (§7b). Report 5's per-load pivots then become **DAX
measures** over those same lines — which is why the line-level pivot inputs (`units_ordered`,
`item_weight`, `product_category`, `sales_report_code_01`, `line_type`) are now stored on the fact.

---

## 1. Architecture

```
 lh_jde_silver.<jde>.*  (Delta — full-snapshot batch reads, no CDF)
   F0005  (snapshot)                  F4211 (snapshot; the load/order-line driver)
        ▼                             F4311 (snapshot; OX status/amount AND the PO_HOLADD rows)
 nb_eso5_gold_dim_uss_plant                   │   + STATIC snapshot lookups:
   [RUN FIRST]                                │     F554201T, F0911, F43121, F0101, F4201
        · read F0005 snapshot, UDC 55/UP      │   (NO F0005 — the fact never reads it)
        · → lh_jde_gold.rpt.dim_uss_plant     ▼
          (vendor_number → uss_plant_sand /  nb_eso5_gold_fact_extended_sales_order_5 ─ BATCH build
           shipped_from / lofa_mcu)            · reconstructs FOUR Hubble custom views:
        └──── lofa_mcu (DRSPHD) ───────────►     SBXLOADPOVIEW + SBXUSSSAND + SBXLOADPOVIEWHOLADD
              SOORDERNO SO-match input only      + SBXLOADDETAIL, then the §4 top-level LEFT joins
              (Gold→Gold read, not Silver F0005) · → lh_jde_gold.rpt.fact_extended_sales_order_5
                                                    (58 grain cols + lofa_rate + 2 keys = 61; row_class tags the rows)
 (address dims are REUSED role views that already exist — see §6)
        ▼
 lh_jde_gold.rpt.fact + dim_uss_plant + rpt.dim_address_* → Direct Lake model  sandbox_load_po.SemanticModel
```

**Run order: `nb_eso5_gold_dim_uss_plant` → `nb_eso5_gold_fact_extended_sales_order_5`.** The fact raises a clear
`RuntimeError` if `dim_uss_plant` is missing.

**Batch build — all sources read as full snapshots.** The dim reads the whole F0005; the fact reads the whole
F4211 **and** F4311 (both contribute rows — F4211 the SX lines, F4311 the `PO_HOLADD` class, §3d) plus the five
static lookups (F554201T, F0911, F43121, F0101, F4201). Each run reads the current snapshot, runs
`transform_*()`, and overwrites its Gold table (`MANUAL_OVERWRITE=True` drops + rebuilds; `False` builds only if
the table is missing). Re-run the notebooks to refresh. **No CDF is required on any source** (the previous design
streamed F0005/F4211/F4311 via CDF; that was removed 2026-07-26 — see the note above).
`SILVER_SCHEMA = "jde"` (2026-07-26,
was `SRC_SCHEMA = "jde_cdc"`; ESO4's confirmed Silver schema); Gold via `GOLD_LH = "lh_jde_gold"` + `GOLD_SCHEMA = "rpt"` + `gname()` (2026-07-26 ESO4-aligned; was combined `GOLD_SCHEMA = "lh_jde_gold.rpt"`) → `lh_jde_gold.rpt` (all ESO5 Gold tables — the fact,
`dim_uss_plant`, and the reused conformed dims — live in the shared `rpt` schema, same as ESO4).

---

## 2. Silver sources (per `full_metadata.txt`)

| JDE | Silver table | Role |
|---|---|---|
| F4211 | `f4211_sales_order_detail_file` | **snapshot (spine)** — SBXLOADPOVIEW base (SX lines) + BOL/SANDTKT/OX-weight/SO-match subqueries |
| F4311 | `f4311_purchase_order_detail_file` | **snapshot (union)** — OX Next/Last status + OX Amount (PO detail), **and the source of the `PO_HOLADD` rows** (report 4, §3d) |
| F554201T | `f554201t_sand_box_sales_order_qc_information` | QC/sand-ticket table — Sand PO Number (QCDS50); join keys QCKCOO/QCDOCO/QCDCTO. Report 5 also reads **QCLGL1/2/3 + QCFSTR3** |
| F0911 | `f0911_account_ledger` | Carrier PO GL Post flag (GLPOST) |
| F43121 | `f43121_purchase_order_receiver_file` | PO Receipt GL Date (PRDGL) + GLPost doc linkage |
| F4201 | `f4201_sales_order_header_file` | **report 5 only** — the SBXLOADDETAIL header attributes it groups by (SHMCU/SHAN8/SHSHAN/SHCARS/SHVR01/SHTRDJ) |
| F0005 | `f0005_user_defined_code_values` | **`dim_uss_plant` ONLY** (UDC **55/UP**, DRSPHD → USS/plant flags). **NOT read by the fact notebook** |
| F0101 | `f0101_address_book_master` | Loading Facility **rate** (ABURAT) only — read on the fact for the `lofa_rate` measure. Name (ABALPH) resolves via the **reused `rpt.dim_address_book`** relationship |

> **Silver column names come from `eso5/full_metadata.txt` — do not transliterate the JDE alias.**
> The map is not literal, and the columns the four variations need are exactly where that bites. All
> resolved (2026-07-14), the non-obvious ones being:
>
> | JDE | Silver | (what a literal guess would give) |
> |---|---|---|
> | SDITWT | `amount_unit_weight` | ~~item_weight~~ |
> | SDSRP1 | `sales_reporting_code_01` | ~~sales_report_code_01~~ |
> | PDUOM / SDUOM | `uom_as_input` | ~~uom_transaction_qty~~ |
> | QCLGL1 / QCLGL2 / QCLGL3 | `descriptn_01` / `_02` / `_03` | ~~leg_1/2/3~~ |
> | QCFSTR3 | `future_use_string_03` | ~~free_form_string_3~~ |
>
> The *fact's* column names are unchanged (`item_weight`, `sales_report_code_01`, `leg_1/2/3`,
> `qc_string_3`) — only the Silver sources they read from. F4201's own columns (SHKCOO/SHDOCO/SHDCTO/
> SHMCU/SHAN8/SHSHAN/SHCARS/SHVR01/SHTRDJ) all transliterate normally.

> **Dimensionalized (per user request 2026-07-10):** F0005's display flags and F0101's name are **not**
> stored on the fact — they resolve through dimensions. F0005 → new **`dim_uss_plant`**; F0101 name →
> reused `dim_address_book`. F0101 is still read on the fact for the single value the reused dim lacks —
> `ABURAT` (the rate).
>
> **No direct F0005 dependency in the fact (2026-07-13):** the fact notebook never opens Silver F0005.
> The one F0005-derived value it still needs at build time — the vendor's plant MCU (`DRSPHD`), the
> `L.SDMCU IN (…)` predicate of the SOORDERNO subquery (§3b) — is read from the **Gold `dim_uss_plant`**
> (`load_uss_plant_mcu()`), making the dim a **prerequisite** of the fact. The three display flags are
> joined in the **semantic model** (`fact.loading_facility → dim_uss_plant.vendor_number`).

---

## 3. Custom-view reconstruction

### 3a. SBXLOADPOVIEW  (base = F4211 — **NO FILTER AT ALL**)

**Current direction (2026-07-20): the Gold layer applies NO filter of any kind.** The five queries are
read only for source tables, fields, joins, business logic, calculations and transformations. Every
row-selecting predicate in all five is carried as a **column** and applied in the Power BI report.
(This supersedes the 2026-07-15 position, which briefly re-applied `SDDCTO='SX' AND SDKCOO='00750'` on
the F4211 leg and the F0101 `ABAT1` band as `WHERE` clauses. Both are gone.)

The five WHERE clauses **conflict** anyway (reports 1-3 drop TL & HOLADD lines; report 4 *requires*
HOLADD; report 5 *keeps* both), which is the structural reason one fact can serve five reports only if
the discrimination happens at report level.

| Source predicate | Treatment |
|---|---|
| `SDDCTO = 'SX'` | **NOT applied** → the **`document_type`** column; the report filters `= "SX"`. |
| `SDKCOO = '00750'` | **NOT applied** → the **`company`** column; the report filters `= "00750"`. |
| F0101 `ABAT1 BETWEEN 'A '..'P ' OR 'R '..'ZZZ'` | **NOT a filter — a VALUE DEFINITION.** It says which address rows *count as a rate source*. Relocated into a **CASE inside the `lofa` aggregate** (§3a-bis), so it drops no row; an out-of-band facility gets a NULL `lofa_rate`, exactly as Hubble's LEFT JOIN produced. |
| `SDLNTY <> 'TL'` | **NOT applied** — conflicts (report 5 keeps TL) → **`row_class = 'TEXT'`**, report filters via `row_class`. |
| `SDLITM <> 'HOLADD'` | **NOT applied** — conflicts (report 4 needs HOLADD) → **`row_class = 'HOLADD'`**, report filters via `row_class`. |
| F4311 `PDDCTO='OX'`, `PDKCOO='00750'`, `PDLITM='HOLADD'`, `NOT EXISTS(live SX HOLADD)` | **NOT applied** — `PO_LEG_UNFILTERED=True` (user chose "keep whole F4311"); carried as `row_class`/`po_holadd_superseded`. |
| `ORDATE BETWEEN …` (all 5), `SDLITM='FRT'` + `SDNXTR<'581'` (base), `SHDOCO IN (…)` + `SDLTTR NOT IN ('980')` (Recon) | **NOT applied** — per-instance report slicers, explicitly excluded / same category as the date. |

> **⚠ Why removing `SDKCOO='00750'` is SAFE here — and would not have been in Hubble.** Hubble's
> `SBXLOADPOVIEW ⟕ SBXUSSSAND` join is on `SDDOCO + SDDCTO` **only**; it relies on both sides being
> independently hard-filtered to `00750` to stay inside one company. This notebook instead joins
> **`kcoo + doco + dcto`** on `qcv`, `sbxusssand`, `la`, `recv`, `glpost` and `hdr`, and `_load_aggregates`
> **groups by `(kcoo, doco, dcto)`**. The company is part of the *key* rather than a global constant, so an
> unfiltered base cannot leak across companies. That is what makes the no-filter position implementable at
> all. (The one lookup still keyed the Hubble way is `ox`, on `(item, doco)` — see §8.14.)

> **A second consequence, in our favour:** the SO-match / `_load_aggregates` / OX helpers always read the
> **full** Silver F4211 — the SO leg needs the `SDDCTO='SO' AND SDCO='00400'` rows an SX filter removes.
> With the base leg unfiltered they now read exactly the same population, so there is no longer a
> filtered/unfiltered split to reason about.

### 3a-bis. The derivations keep their logic — as CASEs and JOIN conditions, never WHEREs ⚠

**Position (2026-07-20): there is NO row-population `.where()` clause on any source table.** The only
`.where()` / `.filter()` calls left in the notebook are the universal exclusions the Gold rule permits —
the Silver soft-delete flag (`is_delete = 0`) and an invalid-key guard on the `dim_uss_plant` lookup.
Every source predicate from the five queries is either a **report filter** (carried as a column, §3a) or
a **derivation** — the *definition of the value it computes*: delete `SDLITM='BOL'` and `bol` doesn't
become "unfiltered", it becomes **wrong**.
So the derivations are all kept, but **relocated** out of `WHERE` into the only two places that cannot drop a row:

* a **CASE inside an aggregate** — the predicate decides whether a line *contributes to a value*;
* a **JOIN condition** — the predicate decides what *matches what*.

| Derivation | Source predicate | Now implemented as |
|---|---|---|
| `bol` / `sand_ticket` | `SDLITM='BOL'` / `'SANDTKTNBR'` | `MAX(CASE WHEN item=… THEN dsc1 END)` in `_load_aggregates` |
| `sbx_weight` (SXWEIGHT) | `SDDCTO='SX' AND SDPRP1='COM' AND SDLTTR<>'980'` | conditional `SUM`; the order type is the group key |
| `load_last_status` (+ MX/MI) | `<>'HOLADD' AND SDAEXP<>0` | CASE inside the aggregate |
| leg-B FRT back-fill | `SDDCTO='SX' AND SDLITM='FRT'` | `MAX(CASE WHEN item='FRT' THEN … END)` |
| `po_holadd_superseded` | `NOT EXISTS(live SX HOLADD, SDLTTR<>'980')` | `MAX(CASE WHEN item='HOLADD' AND status<>'980' THEN 'Y' END)` |
| `ox_*` | `PDDCTO='OX' AND PDKCOO='00750'` | CASE inside the F4311 aggregate |
| `po_receipt_gl_date` | `PRDCT='OV' AND PRMATC='1' AND PRDGL>1` | CASE inside the F43121 aggregate |
| `carrier_po_gl_post_flag` | `GLDCT='OV' AND GLKCO='00750'`, `PRDCT='OV'` | CASE; the doc linkage stays a JOIN |
| SBXUSSSAND base | `SDDCTO='SX' AND SDLITM='SANDTKTNBR' AND SDKCOO='00750'` | the item is already a CASE in `_load_aggregates`; the order type + company are the **join keys** |
| SO-match | `SDDCTO='SO' AND SDCO='00400'` (+ `SDLNTY='S'` for SOWEIGHT) | **JOIN condition** — it *is* the match |
| `ox_amount` | `item='FRT'` / `item='HOLADD' AND status<>'980'` | CASE (`ox_amount_gross` exposes it unconditioned) |

**The values are bit-for-bit what Hubble computes** — an SX/`00750` row reads exactly what its correlated
subqueries would have returned, because the order type and company are now *group keys* rather than
hard-coded constants. Nothing is filtered; everything is still calculated.

> A by-product worth having: `_load_aggregates()` now computes **all** the per-load F4211 subqueries
> (bol, sand_ticket, sbx_weight, load_last_status + MXLTTR/MILTTR, the leg-B back-fill, the
> ex-`NOT EXISTS` flag) in **one pass** over F4211, replacing four separate scans.

> **⚠ Consequence — the enrichment lookups stay `00750`/`00400`-scoped.** Those constants come from the
> source SQL and were **not** rewritten into company correlations, because inventing a join the queries
> never had would be a bigger liberty than keeping the constant. So a fact row from some *other* company
> (which now exists, since the base filter is gone) will carry **NULL** `ox_*`, `carrier_po_gl_post_flag`,
> `po_receipt_gl_date`, `sand_po_number` and the `uss_*` columns. This is invisible to all five reports —
> they all filter `company = "00750"` — but it is why the fact must not be read unfiltered. See §8.14.

Direct columns: SDKCOO, SDDOCO, SDDCTO, SDVR01, SDAN8, SDSHAN, SDCARS, SDMCU, SDLITM, SDDSC1, SDUOM(UOM),
SDUPRC, SDAEXP(EXTAMT), SDLTTR, SDNXTR, SDDOC, SDLNID, SDVEND(LOFA).

Derived (correlated subqueries → pre-aggregated joins):
| Out col | Definition | Implementation |
|---|---|---|
| `bol` | `MAX(SDDSC1)` of same (SDDOCO,SDDCTO) where SDLITM='BOL' | agg F4211 filtered item='BOL' by (doco,dcto) |
| `sand_ticket` | `MAX(SDDSC1)` of same (SDDOCO,SDDCTO) where SDLITM='SANDTKTNBR' | agg F4211 filtered item='SANDTKTNBR' by (doco,dcto) |
| `qty` | `DECODE(SDPRP1='COM', units/2000, units)` | see §5 (Qty de-scale) |
| `order_date` | SDTRDJ Julian→date | Silver `date_transaction_julian` (already a date) |
| `gl_date` | SDDGL, 0/null → 1900-01-01 | `dt_for_gl_and_vouch_01`; coalesce null→1900-01-01 |
| `ox_next_status` | `MAX(F4311.PDNXTR)` where OX/00750/PDLITM=SDLITM/PDDOCO=SDDOCO | agg F4311 (OX,00750) by (item,doco) |
| `ox_last_status` | `MAX(F4311.PDLTTR)` same scope | agg F4311 (OX,00750) by (item,doco) |
| `ox_amount` | if SDLITM='FRT' then `SUM(F4311.PDAEXP)` (OX/00750/item/doco) else 0 | agg F4311 (OX,00750) by (item,doco); applied only when item='FRT' |
| `carrier_po_gl_post_flag` | `MAX(F0911.GLPOST)` where OV/00750 and GLDOC ∈ (F43121 PRDOC where SDCARS=PRAN8, kcoo/doco/item match, PRDCT='OV') | F43121(OV)⋈F0911(OV,00750) on doc; agg by the F4211 line keys |
| `po_receipt_gl_date` | `MAX(F43121.PRDGL)` where SDCARS=PRAN8, kcoo/doco/item match, PRDCT='OV', PRMATC='1', PRDGL>1 | agg F43121 (OV, matc=1, prdgl>1) by (pran8,kcoo,doco,item) → join on SDCARS/SDKCOO/SDDOCO/SDLITM |

### 3b. SBXUSSSAND  (base = F4211 M where SX/SANDTKTNBR/00750  INNER JOIN F554201T on doco/dcto/kcoo)
Grain: one row per (SDDOCO, SDDCTO) sand-ticket load. Direct: SANDTKTPO (QCDS50), SANDTKTNO (M.SDDSC1),
SDVEND, SDAN8, SDSHAN, SDMCU. Derived:
| Out col | Definition |
|---|---|
| `soorderno` | distinct L.SDDOCO from F4211 L where L.pull_signal = rpad(rtrim(M.sddsc1,'.'),30) AND L.reference_01 = rpad(F554201T.qcds50,25) AND L.dcto='SO' AND L.co='00400' AND L.SDMCU ∈ (55/UP DRSPHD where DRKY=M.SDVEND) → **the MCU comes from `dim_uss_plant.lofa_mcu`, not Silver F0005** |
| `soaltbolno` | distinct L2.pull_signal, same match (SO/00400) |
| `sopono` | distinct L3.reference_01, same match (SO/00400) |
| `matchflag` | 'Y' if any L4 match (SO/00400) else null |
| `usssand` | **→ `dim_uss_plant.uss_plant_sand`** (not on the fact): 1<DRSPHD<9000 → 'Y' else 'N' |
| `lofaplantmcu` | **→ `dim_uss_plant.lofa_mcu`** (not on the fact) — raw DRSPHD special-handling code |
| `plantransload` | **→ `dim_uss_plant.shipped_from`** (not on the fact): >9000 → 'TRANSLOAD'; 1<x<9000 → 'PLANT'; else '3RDPARTY' |
| `sxweight` | `SUM(SDUORG)` where SX/SDPRP1='COM'/SDLTTR<>'980'/SDDOCO=M.SDDOCO (de-scaled) |
| `soweight` | `SUM(L1.SDSQOR)` where SO-match (as soorderno) AND L1.line_type='S' |

> The SBXUSSSAND SO-match subqueries join F4211 **SO orders (company 00400)** to the sand-ticket load
> by a **string-padded pull_signal / reference_01 match** — intricate; implemented faithfully as joins,
> flagged for verification (§8).

### 3c. Top-level joins (docx §4 "SQL Table Joins" — all LEFT/Outer)
| From | To | Keys |
|---|---|---|
| SBXLOADPOVIEW | F554201T | SDKCOO=QCKCOO, SDDOCO=QCDOCO, SDDCTO=QCDCTO |
| SBXLOADPOVIEW | SBXUSSSAND | SDDOCO=SDDOCO, SDDCTO=SDDCTO |
| SBXLOADPOVIEW | F0101 (LOFA) | LOFA (SDVEND) = ABAN8 |
| SBXLOADDETAIL | F4201 (header) | SHKCOO=SDKCOO, SHDOCO=SDDOCO, SHDCTO=SDDCTO — *report 5* |

### 3d. SBXLOADPOVIEWHOLADD  (report 4 — "…PO Details for HOLADD")
A `UNION` of two legs. **Leg A** is the F4211 SX / 00750 / `<>TL` lines where `SDLITM = 'HOLADD'` — the
exact complement of the core view's `<>HOLADD`, so it needs no new source, only the row-population widening
above (`row_class = 'HOLADD'`). Its one behavioural difference is OXAMT, which it charges to a **live**
HOLADD line (`SDLITM='HOLADD' AND SDLTTR<>'980'`) where the core charges it to the FRT line.

**Leg B** (`row_class = 'PO_HOLADD'`) is new: **F4311 purchase-order lines** —
`PDDCTO='OX' AND PDLITM='HOLADD'`. Hubble emits only the *orphan* ones, via **`NOT EXISTS`** a live SX
HOLADD sales line (`SDLTTR<>'980'`) on the same load — a holding charge raised on the PO that never made
it back onto the sales order.

> ⚠ **`PDDCTO='OX' AND PDLITM='HOLADD'` is NOT applied either** — **`PO_LEG_UNFILTERED = True`**. Leg B
> takes the **whole of F4311**, and those two conditions become the `row_class` *calculation* instead:
> `'PO_HOLADD'` when `PDDCTO='OX' AND PDLITM='HOLADD'` (the rows report 4 consumes), `'PO_OTHER'` for every
> other purchase-order line. **With this, the notebook contains no row-selecting predicate anywhere.**
> `item_number` (PDLITM) and `po_order_type` (PDDCTO) are on every PO row, so the report can filter the PO
> leg however it likes.
>
> **Cost:** the fact is now **F4211 ∪ F4311 in full**. The `PO_OTHER` rows are carried for symmetry; **no
> report reads them**. See §4 (volume) and §8.17.
>
> ⚠ **`document_type` is forced to `'SX'` on every PO row** — the query hard-codes `'SX' DCTO` so the UNION
> lines up with the sales load, and `load_scope_key` depends on it (a PO change must land in the SX load's
> CDC scope). **Therefore `document_type = "SX"` alone does NOT exclude purchase-order rows** — `row_class`
> is what separates them, and the PO's own type lives in `po_order_type`. This is the sharpest trap in the
> model: a report page that filters only on `document_type` and `company` will silently include all of
> F4311.

> ⚠ **That `NOT EXISTS` is a STATUS filter, so it is NOT applied (2026-07-14, §7d).** *Every* OX HOLADD PO
> line becomes a row, and the test is **calculated onto it** instead as
> **`po_holadd_superseded`** (`'Y'` ⇔ the load already carries a live SX HOLADD sales line).
> **Hubble's orphan set is exactly `po_holadd_superseded = 'N'`**, which report 4 filters on. The
> `left_anti` join is gone; a plain `LEFT JOIN` to a per-load conditional aggregate replaces it. No row is
> dropped — the status decides a *field*, not the row population.

Its sales-side attributes are back-filled from the load's **FRT line**
(`MAX(SDVR01/SDAN8/SDVEND/SDDGL/SDLTTR/SDNXTR/SDDOC)`), exactly as the query's correlated subqueries do,
and:

| Out col | Leg B value |
|---|---|
| `unit_price`, `total_amount` | **0** — the money is on `ox_amount`, not the sales line |
| `ox_amount` / `ox_last_status` / `ox_next_status` | the PO row's **own** PDAEXP / PDLTTR / PDNXTR (not the load-level `MAX` the other classes use) |
| `carrier` | **PDAN8** (on a PO, the address number *is* the carrier) |
| `ship_to`, `district`, `item_description`, `uom`, `units_ordered`, `order_date`, `line_id` | PDSHAN, PDMCU, PDDSC1, PDUOM, PDUORG, PDTRDJ, PDLNID |
| `document_type` | hard-coded **`'SX'`** (the query does this) — which keeps a `PO_HOLADD` row's `load_scope_key` aligned with the sales load it back-fills from |

`ox_amount` is therefore a **three-way** rule on the unified fact: `PO_HOLADD` → its own PDAEXP;
`item='FRT'` → the OX sum (core); `item='HOLADD' AND last_status<>'980'` → the OX sum (report 4); else 0.

### 3e. SBXLOADDETAIL  (report 5 — "SBX Load Reconciliation Report")
`F4201 INNER JOIN F4211` on (SHDOCO/SHDCTO/SHKCOO), `SHDCTO='SX'`, aggregated to **one row per load** —
a *different grain*, and the only reason a naive reading would demand a second fact table. It does not:
every one of its outputs is a **pivot of the very F4211 lines this fact already stores**, so they are
implemented as **DAX measures** (§7c) over `row_class IN ('LINE','HOLADD','TEXT')` rather than as rows.

What that costs the fact is five stored line-level *inputs* — `units_ordered` (raw SDUORG, undivided),
`item_weight` (SDITWT), `product_category` (SDPRP1), `sales_report_code_01` (SDSRP1), `line_type` (SDLNTY)
— plus:

| Out col | Definition | Where it lives now |
|---|---|---|
| `load_last_status` | `CASE WHEN MAX(SDLTTR)=980 AND MIN(SDLTTR)=980 THEN 980 ELSE MAX(SDLTTR) over the load's lines with SDLITM<>'HOLADD' AND SDAEXP<>0 END` | **stored** per row (constant within a load) — the CASE is not expressible as a plain aggregate, so it is precomputed in `_load_last_status()` |
| `leg_1/2/3`, `qc_string_3` | F554201T QCLGL1/2/3, QCFSTR3 | **stored** (⚠ inferred Silver names) |
| `header_district / _sold_to / _ship_to / _carrier / _customer_po / _order_date` | F4201 SHMCU/SHAN8/SHSHAN/SHCARS/SHVR01/SHTRDJ | **stored** — report 5 groups by the **header** values, which need not equal the line's own |
| SANDWEIGHT, EXTWEIGHT, MILES, LOFADET, WELLDET, …PP, …PB, EXTAMT, FRTAMT, FSCAMT, SANDAMT, HOLAMT, PROP, LOFA | pivots of the load's lines | **DAX measures** (§7c) |

> Report 5's view has **no `SDKCOO='00750'` predicate** (it inherits company only through F4201). The fact
> keeps `00750`, so if sandbox SX loads ever exist under another company key the reconciliation report
> would miss them. Every other query in the set hard-codes `00750`, so this is treated as an artefact of
> the view rather than a real requirement — **flagged in §8.**

---

## 4. Grain & keys
- **Grain = one line.** As of 2026-07-20 **no filter is applied at all** (§3a), so the population is
  **every F4211 sales-order line** (any company, any document type, any item, any line type — including TL
  text lines and HOLADD lines) **plus every F4311 purchase-order line** (`PO_LEG_UNFILTERED=True`).
  TL / HOLADD stay tagged by `row_class`; `document_type` / `company` / `row_class` (§7b) are what each
  report filters on.
- ⚠⚠ **Volume — the fact is the WHOLE of F4211 ∪ the WHOLE of F4311.** This is the largest the fact can
  be, and it is deliberate: Gold holds the complete clean dataset and the report decides what to show.
  **Measure the first `MANUAL_OVERWRITE=True` batch build** (§8.15).
  Expect a large multiple of the original SX/`00750` row count and a correspondingly heavy build.
  Every re-run rebuilds the whole fact (batch), and Direct Lake only
  pages in the columns and rows a query touches — but the build and the Delta footprint both grow,
  and the `PO_OTHER` rows are read by **no report**. Measure the first full load against the F64 capacity /
  5-min SLA. Two escape hatches, in order of bluntness: set `PO_LEG_UNFILTERED = False` (drops the
  never-read `PO_OTHER` rows), then reinstate `document_type='SX'` in `_f4211_lines()`.
- The fact **stores 58** grain columns + `lofa_rate` = **59 business + 2 keys (`load_line_key`,
  `load_scope_key`) = 61 stored** (matches the TMDL's 61 `sourceColumn`s). Four of the core report's 39 display columns are
  deliberately absent (`loading_facility_name`, `uss_plant_sand`, `shipped_from`, `lofa_mcu`) — they
  resolve via dimensions (§6) and are functionally dependent on `loading_facility` (a grain key), so the
  grain is unchanged. Hubble `GROUP BY`s its display columns and `SUM`s the single measure
  `ReportColumn1 = ABURAT*0.01`; the inner `SELECT DISTINCT` carries the F0101 PK (ABAN8). The fact
  reproduces this: reconstruct → `.distinct()` (inner) → `groupBy(grain).agg(sum(lofa_rate))` (outer).
  The extra grain columns only *refine* the group-by — they cannot split a Hubble row, because `line_id`
  is already in it.
- `load_line_key` = `sha2` of the grain columns — unique per fact row (`dropDuplicates`). `row_class` is
  part of it, which is what keeps a `PO_HOLADD` row from colliding with an F4211 line that happens to
  share its `line_id`.
- `load_scope_key` = `sha2(company_key_order_no ‖ order_type ‖ document_order_invoice_e)` — the per-load key.
  **Vestigial under the batch build** (it was the CDC delete scope in the previous streaming version;
  retained so the fact schema + semantic model are unchanged, like ESO4's `document_scope_key`). Leg B still
  hard-codes `document_type='SX'` so a `PO_HOLADD` row's key aligns with the sales load it back-fills from.

---

## 5. Calculations & decisions
- **§7 = N/A** — no business calculation. The only measure is `lofa_rate = F0101.ABURAT × 0.01`
  (loading-facility rate). It is semi-additive (constant per LOFA); exposed as a `SUM` measure but
  really a per-facility rate — flag on the report.
- **Qty de-scale (⚠ assumption).** Hubble `Qty = DECODE(SDPRP1='COM', (SDUORG/1000)/2000, SDUORG/1000)`.
  The `/1000` de-scales RAW JDE integer qty (3 implied decimals). **Silver is already decoded**, so we
  drop the `/1000` and keep the business factor: `qty = if purchasing_report_code_01='COM' then
  units_transaction_qty/2000 else units_transaction_qty`. Same reasoning for `sxweight`/`soweight`
  (drop `/1000`). Confirm the Silver scaling.
- **View-defining predicates KEPT; §5 report filters SKIPPED.** SBXLOADPOVIEW/SBXUSSSAND internal
  WHERE constants (`SX`, `00750`, `00400`, `SANDTKTNBR`, `<>'TL'`, `<>'HOLADD'`, `55/UP`) are part of
  the §4 **custom-view definitions** — they define "a sandbox load" and are kept (like ESO4's
  structural joins). The §5 Filters (Sold To/Ship To/LOFA/Load#/Last-Next Status/**Order Date
  5/1–5/31**/GL Date/Customer PO/Carrier/Item#/GL Post flag — all blank except the Order-Date range,
  which is the Hubble outer `ORDATE BETWEEN 126121 AND 126151`) are **report slicers → NOT applied**.
- **Dates on the fact (no `dim_date`)** — ESO4 decision carried forward: `order_date`, `gl_date`,
  `po_receipt_gl_date` are native columns, sliced directly.
- **Company/District** (SDKCOO='00750', SDMCU) are degenerate fact columns.

---

## 6. Dimensions
Star schema (ESO4 pattern): the fact stores **FK codes**; descriptions resolve through dimensions.

**Address dims — REUSED** (`rpt.dim_address_book` role views; all exist from prior reports, no notebook):

| Fact FK | Model dim table | Reused physical view (`rpt.*`) | JDE |
|---|---|---|---|
| `sold_to` | `dim_address_sold_to` | `dim_address_sold_to` (exists) | SDAN8 |
| `ship_to` | `dim_address_ship_to` | `dim_address_ship_to` (exists) | SDSHAN |
| `carrier` | `dim_address_carrier` | `dim_address_carrier` (exists) | SDCARS |
| `loading_facility` | `dim_address_loading_facility` | `dim_address_loading_port` (exists) | LOFA (SDVEND) |

`Loading Facility Name` (ABALPH) resolves via the `loading_facility → dim_address_loading_facility`
relationship (`name_alpha`) — **not stored on the fact**.

**`dim_uss_plant` — NEW** (`nb_eso5_gold_dim_uss_plant`, F0005 `55/UP`, batch build, `schemaName rpt`).
Same implementation pattern + structure as ESO4's F0005 dim (`eso4/nb/nb_eso4_gold_dim_udc.py`):
UDC-filtered Type-1 dim — reads the full F0005 snapshot, runs `build_dim_uss_plant()`, and overwrites the
dim (`MANUAL_OVERWRITE`). (Was a CDF stream + `_upsert_dim` MERGE before the 2026-07-26 batch conversion.)

| Column | Source | Notes |
|---|---|---|
| `vendor_number` (key) | F0005 DRKY (numeric) | = `loading_facility` (LOFA=SDVEND) |
| `uss_plant_sand` | DRSPHD | `1<x<9000`→'Y' else 'N' |
| `shipped_from` | DRSPHD | `>9000`→'TRANSLOAD'; `1<x<9000`→'PLANT'; else '3RDPARTY' |
| `lofa_mcu` | DRSPHD | raw special-handling code — also the fact's build-time SOORDERNO match input |

The fact's `loading_facility` FK drives **two** active relationships — to `dim_address_loading_facility`
(name) and to `dim_uss_plant` (flags). **The fact never reads Silver F0005**; it reads `lofa_mcu` from
this Gold dim, so **`dim_uss_plant` must be built before the fact** (the fact raises `RuntimeError` if the
table is absent). **F0101 → reused, no new dim** (per user: reuse the existing F0101
dimension). No `dim_date`; no item dim.

> **Loading role reuse:** `old_nb` created the loading role view as `rpt.dim_address_loading_port`, not
> `_loading_facility`. All role views are identical (`SELECT * FROM dim_address_book`), so the ESO5 model
> table `dim_address_loading_facility` binds (`entityName`) to the existing `rpt.dim_address_loading_port`
> — keeping the report-facing name while reusing a view that is guaranteed to exist. (If a
> `rpt.dim_address_loading_facility` view already exists, point `entityName` there instead.)

`Loading Facility Name` (ABALPH) resolves via the `loading_facility → dim_address_loading_facility`
relationship (`name_alpha`) — **no longer denormalized on the fact**. `ABURAT` (rate) is not in the
reused dim → carried on the fact as the `lofa_rate` measure input (the only reason F0101 is still read).
No `dim_date`. No item dimension (item description is F4211 `SDDSC1`, a degenerate line attribute).

### 6a. `dim_plant` — the District dimension (REUSED; 2026-07-26 migrated from dim_business_unit)

Hubble shows `SDMCU` as a bare code (`771010`), and the ESO5 report table shows exactly that — the
fact's raw `district` code. The District is dimensioned through the **reused conformed `rpt.dim_plant`**
(F0006 Business Unit / Plant Master, built by an upstream job — ESO5 does **not** read F0006), keyed on
`plant_code` = MCMCU = the same value as `SDMCU`.

> **History.** 2026-07-14 this pointed at ESO4's `rpt.dim_business_unit` and a physical
> `business_unit_display` concat was added there to render `"771010 - SBX TRANS DISTRICT-WEST TEXAS"`.
> **2026-07-26 the relationship was migrated to the reused `rpt.dim_plant`** and the ESO4-owned
> `dim_business_unit` builder was retired (ESO5 was its last consumer). The `business_unit_display`
> concat was never actually placed on any ESO5 visual — the report tables show `fact[district]` (the raw
> code) — so this migration changes **no report output**.

| | |
|---|---|
| **Fact** | `district` is **unchanged** — still the raw code (SDMCU). It is the FK, a grain column, and what the report filters on and displays. |
| **Relationship** | `fact.district → dim_plant.plant_code` (string → string; 6th relationship) |
| **Description** | `dim_plant[plant_name]` (F0006 MCDL01, e.g. `"SBX TRANS DISTRICT-WEST TEXAS"`), available via the relationship. |

> **Note — no single "code - description" field.** `dim_plant` is an external conformed dim and carries
> `plant_code` and `plant_name` as separate columns, with **no `business_unit_display` concat** (and
> Direct Lake cannot build one in DAX). If the report ever needs `"771010 - SBX TRANS DISTRICT-WEST
> TEXAS"` as one field, show `fact[district]` (code) and `dim_plant[plant_name]` (name) as two columns,
> or request a concat column on the shared `rpt.dim_plant` upstream.

---

## 7. Semantic model — `sandbox_load_po.SemanticModel` (Direct Lake)
Tables (7): `fact_extended_sales_order_5` + reused address role dims `dim_address_sold_to` / `_ship_to` /
`_carrier` / `_loading_facility` (`schemaName: rpt`; `_loading_facility` binds to the existing
`rpt.dim_address_loading_port`) + new `dim_uss_plant` (`schemaName: rpt`) + reused `dim_plant`
(`schemaName: rpt` — §6a). Relationships (fact = many side, all active, **6**):
`sold_to`/`ship_to`/`carrier`/`loading_facility` → `dim_address_*.address_number`;
`loading_facility` → `dim_uss_plant.vendor_number`; **`district` → `dim_plant.plant_code`**.
**No new relationships were needed for the four variations** — report 5 shows SHAN8/SHSHAN/SHCARS/LOFA as
raw numbers (it never joins F0101 for a name), so the `header_*` columns are plain attributes.

Measures (30): the 8 base ones — Rate (SUM lofa_rate), Total Amount, OX Amount, Quantity, USS SO Weight,
SBX Weight (SUMs), Load Count (DISTINCTCOUNT), Line Count (COUNTROWS) — plus the 19 reconciliation pivots
(§7c) and **3 load-level measures** (`Load GL Date` / `Load Invoice No` / `Load Next Status`, added
2026-07-20: the Reconciliation report needs GLDATE/SDDOC/SDNXTR as MAX-per-load, and the model discourages
implicit measures). No date dimension.

### 7a. Column types (Direct Lake — TMDL must match the PHYSICAL Delta type)
Direct Lake binds to the Delta column type and **will not implicitly widen**; a relationship whose FK and
PK differ is rejected outright:
> *"data types of Direct Lake relationship between … `[loading_facility]`(Double) and …
> `[vendor_number]`(Int64) are incompatible"*

Silver decodes JDE numerics to **Double**, so:
| Column(s) | Physical | Why |
|---|---|---|
| `sold_to` / `ship_to` / `carrier` / `loading_facility` (fact) + `address_number` (address dims) + `vendor_number` (`dim_uss_plant`) | **double** | Every relationship endpoint. `dim_uss_plant.vendor_number` must be cast to **double** (`DIM_KEY_TYPE`), NOT long — a `long` cast is what triggered the error above |
| `load_number` (SDDOCO), `invoice_number` (SDDOC), `uss_so_order_no` (SO SDDOCO) | **int64** (explicit `.cast("long")`) | Whole document numbers — cast in the notebook so the model type is honest |
| `line_id` (SDLNID) | **int64** | The **raw JDE line number**: `1.00` → **`1000`** (2026-07-14). Silver decoded SDLNID's 3 implied decimals; the notebook puts them back — `round(line_number * 1000)`. This is *lossless because of* the decimals, not in spite of them: a fractional kit/component line `1.010` becomes `1010`, so nothing truncates, and the old `formatString: 0.###` workaround is gone. The `round()` matters — `1.01 * 1000` is `1009.9999999999999` in binary floating point and a bare cast would floor it to `1009` |

> **✅ Verified against `eso5/nb/old` (2026-07-26).** Every fact/dim **output-column data type above is UNCHANGED**
> from the user's old Fabric notebooks (`eso5/nb/old/*.ipynb`) — the streaming→batch conversion + ESO4 convention
> alignment preserved all casts. A full cast-by-cast diff (old `.ipynb` vs current `.py`) showed the only `.cast()`
> differences were inside the REMOVED streaming machinery (the `restrict_loads` / `restrict_keys` CDC-scope joins and
> the F4311 handler's `F.lit("SX").alias("order_type")` re-stamp) — none of which is an output column. So the
> double / int64 types the TMDL binds to are exactly what the old notebooks produced. (⚠ Contrast **ESO4**, whose 5
> identifier `.cast("long")`s WERE stripped 2026-07-26 to match ITS old `fab_25-07-2026` notebook — ESO5 needed no such change.)

> ⚠ `load_scope_key` is built once by the `load_scope_expr()` helper from the fact's own trimmed/int64
> columns (`company`, `document_type`, `load_number`). Under the batch build it is **vestigial** — retained
> only so the fact schema is unchanged. (In the previous streaming version it was ALSO computed from the raw
> Silver CDF columns in `recompute_fact` and had to normalise both sides — trim + long cast — so the two sha2
> inputs matched; that CDC path is gone, but the same normalisation is kept so the stored key stays stable.)

### 7b. The report filter contract ⚠ NOT OPTIONAL — THE FACT IS A SUPERSET

The notebook applies **no filter at all** (§3a), so the fact is the widest possible **superset** of every
report: *all* F4211 lines — every company, every document type, every item, text lines, HOLADD lines —
**plus the whole of F4311** (`PO_LEG_UNFILTERED=True`). **The reports are defined ENTIRELY by their page
filters.** Three columns do the work, and all three are now load-bearing: `document_type` and `company` are
no longer a double-up on a base filter, they are the only thing scoping a report to SX / 00750.

| Column | Values | Replaces |
|---|---|---|
| `document_type` | `SX`, `SO`, … | the views' `SDDCTO='SX'` |
| `company` | `00750`, `00400`, … | the views' `SDKCOO='00750'` |
| `row_class` | `LINE` / `HOLADD` / `TEXT` / `PO_HOLADD` (mutually exclusive) | `SDLNTY<>'TL'` and `SDLITM<>'HOLADD'` |

| `row_class` | The rows | Reports |
|---|---|---|
| `LINE` | F4211 line, not TL, not HOLADD | 1 Sandbox Load Report w/ PO Details · 2 (New) · 3 (no-USS) · **5** |
| `HOLADD` | F4211 line, not TL, item = HOLADD | **4** for HOLADD · **5** |
| `TEXT` | F4211 line, line type = TL | **5** |
| `PO_HOLADD` | F4311 line, `PDDCTO='OX' AND PDLITM='HOLADD'` (§3d) | **4** for HOLADD |
| `PO_OTHER` | every other F4311 purchase-order line | **none** — carried for symmetry only |

> ⚠ **`row_class` is the only column that separates sales rows from purchase-order rows.**
> `document_type` is forced to `'SX'` on PO rows (§3d), so filtering on `document_type` + `company` alone
> lets **all of F4311** through.

**Every report page applies all three:**

| Report | Page filter |
|---|---|
| 1, 2, 3 (core + New + no-USS) | `document_type = "SX"` AND `company = "00750"` AND `row_class = "LINE"` |
| 4 (for HOLADD) | `document_type = "SX"` AND `company = "00750"` AND `row_class IN ("HOLADD","PO_HOLADD")` AND **`po_holadd_superseded = "N"`** |
| 5 (Reconciliation) | `document_type = "SX"` AND `company = "00750"` AND `row_class IN ("LINE","HOLADD","TEXT")` |

Plus whatever **status** slicers each report wants — see §7d; they are *all* the report's to apply now.

> **⚠⚠ Forget the filter and the core report's `Total Amount` is not "slightly off" — it is the sum of
> every sales-order line and every purchase-order line in JDE.** Since 2026-07-20 nothing is pre-filtered:
> a missing page filter pulls in non-SX documents, other companies, the HOLADD and TEXT rows, and the whole
> F4311 PO leg. Set `document_type` + `company` + `row_class` as a **report-level** filter (not page level)
> so a newly added visual physically cannot omit them.

The §7c reconciliation measures each restrict to `row_class IN ("LINE","HOLADD","TEXT")` internally, so
they are safe wherever used — including on a page with no `row_class` filter at all. ⚠ **Corrected
2026-07-20:** they previously guarded `<> "PO_HOLADD"`, which did **not** exclude `PO_OTHER` and, being a
boolean `CALCULATE` predicate (= `FILTER(ALL(row_class), …)`), **replaced** the page filter instead of
intersecting with it. `Miles` double-counted F4311 freight lines as a result. The *base* measures (`Total Amount`, `Quantity`, `Line Count`, …) are deliberately left
unscoped so one measure serves all five reports — they rely entirely on the page filter.

### 7d. Status — ZERO status filtering in the notebook (2026-07-14)

**Per user direction: no `status_code` / last-status / next-status / any status-related predicate filters
a row anywhere in the notebook.** The status *joins* are all implemented and every status *field* is
stored, so status filtering is entirely the Power BI report's job.

**Stored status fields** (all filterable in the report):

| Column | Source | Notes |
|---|---|---|
| `last_status` | SDLTTR | the line's own |
| `next_status` | SDNXTR | the line's own |
| `ox_last_status` | F4311 PDLTTR | a `PO_HOLADD` row reports its **own** PDLTTR, not the load-level MAX |
| `ox_next_status` | F4311 PDNXTR | ditto |
| `load_last_status` | recon view's per-load `SDLTTR` CASE | a *calculation*, see below |
| `load_max_last_status` | MXLTTR | the CASE's inputs, exposed so the report can audit or re-derive it |
| `load_min_last_status` | MILTTR | ″ |
| `po_holadd_superseded` | ex-`NOT EXISTS` | `'Y'` ⇔ the load has a live (`<>'980'`) SX HOLADD sales line |
| `ox_amount_gross` | F4311 PDAEXP sum | the OX money with **no** item/status condition |

**What happened to each status predicate:**

| Source predicate | Disposition |
|---|---|
| `SDNXTR < '581'` (report 3), `SDLTTR NOT IN ('980')` (report 5) | **dropped** — pure report slicers |
| `NOT EXISTS(live SX HOLADD, SDLTTR<>'980')` (report 4) | **filter → field**: `po_holadd_superseded`. All OX HOLADD PO lines are now rows; the orphan set is `= "N"` |
| SXWEIGHT's `WHERE SDLTTR<>'980'` | **filter → conditional sum**: `SUM(CASE WHEN status<>'980' THEN units END)`. The status now decides whether a line *contributes to the value*, not whether the row *exists* |
| `load_last_status`'s `MAX=980 AND MIN=980` CASE | **kept** — a CASE is a *calculation*; it drops no rows. Its inputs are stored (above) so the report can override it |
| `ox_amount`'s `item='HOLADD' AND last_status<>'980'` | **kept** — likewise a CASE. But it does bake a status rule into the value, so `ox_amount_gross` exposes the same money unconditioned |

> **The dividing line.** A status predicate in a `WHERE` / `NOT EXISTS` / anti-join **removes rows** — that
> is a filter, and it is gone. A status reference inside a `CASE` or a conditional aggregate **computes a
> value** — that is a calculation, it removes nothing, and removing *it* would not make the column
> "unfiltered", it would make the column **wrong and unrecoverable** (an aggregate over other rows cannot
> be rebuilt at report level). Those stay, and every input they consume is stored alongside so the report
> can always re-derive its own version.

### 7c. Reconciliation measures (report 5)
The SBXLOADDETAIL pivots (§3e), as DAX over the line-grain rows. All restrict to
`row_class IN ("LINE","HOLADD","TEXT")` — exactly SBXLOADDETAIL's population, since that view is
`F4201 INNER JOIN F4211` and never reads F4311.

| Hubble | Measure | Definition |
|---|---|---|
| SANDWEIGHT | `Sand Weight` | `SUM(units_ordered)` where `product_category = "COM"` |
| EXTWEIGHT | `Ext Weight` | `SUM(item_weight)` where `product_category = "FRT"` |
| `SANDWEIGHT/2000`, `EXTWEIGHT/2000` | `Sand Tons`, `Ext Tons` | the two above ÷ 2000 |
| MILES | `Miles` | `SUM(units_ordered)` where `item_number = "FRT"` |
| LOFADET / WELLDET | `LOFA Detention Hours`, `Well Detention Hours` | `SUM(units_ordered)` where `item_number = "LOFADET"` / `"WELLDET"` |
| LOFAAMT / WELLAMT / LOFAPPAMT / WELLPPAMT / LOFAPBAMT / WELLPBAMT | `LOFA Amount`, `Well Amount`, `LOFA PP Amount`, `Well PP Amount`, `LOFA PB Amount`, `Well PB Amount` | `SUM(total_amount)` where `item_number =` the matching detention item |
| FRTAMT | `Freight Amount` | `SUM(total_amount)` where `product_category = "FRT"` |
| FSCAMT | `Fuel Surcharge Amount` | `SUM(total_amount)` where `item_number = "FSC"` |
| SANDAMT | `Sand Amount` | `SUM(total_amount)` where **`sales_report_code_01 = "352"`** — keys off SDSRP1, *not* the item number |
| HOLAMT | `Holding Amount` | `SUM(total_amount)` where `item_number = "HOLADD"` |
| EXTAMT | *(reuse `Total Amount`)* | `SUM(total_amount)` over the load's lines |
| PROP / LOFA | `Proppant`, `Load LOFA` | `MAX(item_description)` / `MAX(loading_facility)` where `product_category = "COM"` — the **sand line's** values, not the current row's |
| SDLTTR (per-load CASE) | *(column `load_last_status`)* | precomputed on the fact — see §3e |

---

## 8. Open items / to verify
1. **Qty / weight de-scale** — confirm Silver `units_transaction_qty` / `units_secondary_qty_or` are
   decoded (drop `/1000`, keep `/2000` for COM).
2. **SBXUSSSAND SO-match** — the string-padded `pull_signal`/`reference_01` correlated join (SO orders,
   company 00400) is intricate; verify the match semantics + the 55/UP DRSPHD thresholds. Note the
   `L.SDMCU IN (…)` predicate now compares against `dim_uss_plant.lofa_mcu` (**one row per vendor** — the
   dim is deduped on `vendor_number`, whereas Hubble's `IN` would allow several DRSPHD per vendor; confirm
   55/UP is one-row-per-vendor).
3. **`ABURAT` scaling / meaning** — `user_reserved_amount × 0.01`; confirm it is the intended "rate".
4. ~~**Silver table names** assumed from the metadata headers.~~ ✅ **RESOLVED 2026-07-20** — all eight were
   verified against the root `full_metadata.json` (25 tables) and `eso5/converted_tables_22.json` (8 tables,
   exactly the ESO5 working set): `f4211_sales_order_detail_file`, `f4311_purchase_order_detail_file`,
   `f43121_purchase_order_receiver_file`, `f0911_account_ledger`,
   `f554201t_sand_box_sales_order_qc_information`, `f4201_sales_order_header_file`,
   `f0101_address_book_master`, `f0005_user_defined_code_values`. **Every Silver COLUMN the notebook reads
   was checked against that metadata too — 0 missing.**
5. **Schemas** — `SILVER_LH=lh_jde_silver` / `SILVER_SCHEMA=jde` (2026-07-26, was `SRC_SCHEMA=jde_cdc`), `GOLD_LH=lh_jde_gold` / `GOLD_SCHEMA=rpt` (via `gname()` → `lh_jde_gold.rpt`; fact + `dim_uss_plant` + reused dims
   all in the shared `rpt` schema, same as ESO4). ✅ resolved 2026-07-15 — fact notebook, `dim_uss_plant`
   notebook, and both TMDL partitions now all say `rpt`.
6. **Batch build (2026-07-26) — no CDF needed.** Both notebooks read full Silver snapshots; no
   `delta.enableChangeDataFeed` is required on F4211 / F4311 / F0005. First run with `MANUAL_OVERWRITE=True`,
   then **flip `MANUAL_OVERWRITE→False`** in both notebooks after the first healthy run (re-run to refresh).
   **Run order: dim_uss_plant FIRST, then the fact** (the fact reads the dim for the SOORDERNO match MCU).
7. **Address dims are REUSED (no notebook)** — all `rpt.dim_address_*` role views already exist from prior
   reports. The model's `dim_address_loading_facility` binds to the existing `rpt.dim_address_loading_port`;
   repoint `entityName` only if a `_loading_facility` view exists instead.
8. ~~**`dim_uss_plant` (F0005 55/UP)** inferred.~~ ✅ **RESOLVED 2026-07-20 — CONFIRMED from the source
   SQL.** The core query uses the UDC four times (SOORDERNO / USSSAND / LOFAPLANTMCU / PLANTTRANSLOAD):
   `select F0005.drsphd from prodctl.F0005 where F0005.drsy='55' and F0005.drrt='UP' and
   TO_Number(rtrim(F0005.drky,' ')) = M.sdvend`. That pins DRSY=`55`, DRRT=`UP`, DRKY = the numeric vendor
   number (the dim PK), DRSPHD = the plant MCU — and the `>1 AND <9000` / `>9000` thresholds are verbatim
   from the same query's CASE arms. It read as "inferred" only because F0005 is absent from the docx §4
   join list. Keyed by `vendor_number` (= `loading_facility` = LOFA = SDVEND).
9. All sources (F4211/F4311/F0911/F43121/F554201T/F0101/F4201 + the `dim_uss_plant` MCU lookup) are read as
   full snapshots on every batch build; the Gold tables refresh whenever the notebooks are re-run — the ESO4
   batch pattern. (No source is streamed any more — see the 2026-07-26 note in §Intro/§1.)

### Opened by the one-fact consolidation (2026-07-14)
10. ~~Inferred Silver names.~~ **RESOLVED 2026-07-14** — all seven were looked up in
    `eso5/full_metadata.txt` and corrected in the notebook (see the §2 note); the temporary `_opt()` /
    `load_silver_optional()` fail-safe scaffolding has been removed now that every name is verified.
11. **⚠ The §7b page filter (`document_type` + `company` + `row_class`) must be on every report page.**
    The F4211 leg is now SX/00750-scoped (2026-07-15), but the fact still carries all row classes **and the
    whole F4311**, so without the page filter the core report still adds the HOLADD/TEXT rows and every PO
    row. Prefer a **report-level** filter so a new visual cannot omit it.
12. **Report 5 genuinely has NO company predicate — and now the fact can honour that.** ⚠ **Reopened and
    resolved the other way, 2026-07-20.** Verified against the source: `SBXLOADDETAIL` is
    `F4201 INNER JOIN F4211` whose *entire* base WHERE is `F4201.SHDCTO='SX'` — no company predicate, no
    line-type predicate. While the fact forced `00750` (2026-07-15) the reconciliation report could not
    reconcile to Hubble for any other company; with the filter gone it can. **Consequence for the report:
    the Reconciliation page should filter `document_type="SX"` + `row_class IN ("LINE","HOLADD","TEXT")`
    and *omit* `company`** to match Hubble exactly. Adding `company="00750"` is a defensible divergence
    (consistency with the other four reports) but it must be a decision, not an accident. Power BI guide §1.
13. **`load_last_status` `'980'` comparison** is a string compare (`status_code_last` is a char field in
    JDE); Hubble writes `MXLTTR = 980` numerically. Equivalent for a zero-padded 3-char code — confirm
    Silver does not store it with padding/other width.
14. **⚠ The enrichment lookups are still `00750`/`00400`-scoped** (§3a-bis) — those constants live inside
    the source subqueries and were not rewritten into company correlations. A non-`00750` fact row will
    therefore carry NULL `ox_*` / `carrier_po_gl_post_flag` / `po_receipt_gl_date` / `sand_po_number` /
    `uss_*`. Harmless for all five reports (they filter `company="00750"`), but it means the fact must not
    be consumed unfiltered, and it is the thing to revisit if a second sandbox company ever appears.
15. ⚠ **Volume** (§4) — the `MANUAL_OVERWRITE=True` batch build materialises **the whole of F4211 plus the whole of
    F4311** (2026-07-20: no filter anywhere). This is the biggest build the design can produce. **Measure it
    on the first run** and check row count / file size against the F64 capacity budget. If it
    proves impractical the knobs, in preference order, are: (a) set `PO_LEG_UNFILTERED=False` to restore
    `PDDCTO='OX' AND PDLITM='HOLADD'` on the PO leg — the `PO_OTHER` rows are read by **no** report, so this
    costs nothing in report fidelity; then (b) reintroduce a base filter, which reverses the current
    direction and must be agreed first.
16. **Report 4's back-fill picks the FRT line** via `MAX(...)` per load (as Hubble does). If a load can
    carry more than one FRT line the `MAX` is arbitrary-but-deterministic, exactly as in the source query.
17. **`PO_LEG_UNFILTERED = True`** (§3d) — leg B takes the **whole of F4311** (user reaffirmed 2026-07-15:
    "keep whole F4311"). Two things to watch:
    (a) **Volume** — the fact is F4211(SX/00750) ∪ F4311 in full, and the `PO_OTHER` rows (every
    purchase-order line that is not an OX HOLADD) are read by **no report**. Measure the seed; set the
    constant to `False` to restore the `PDDCTO='OX' AND PDLITM='HOLADD'` predicate if it is too big.
    (b) **`document_type` is `'SX'` on PO rows** — filtering a report page on `document_type` + `company`
    alone will silently admit all of F4311. **`row_class` is mandatory on every page.**
18. **All filters removed again (2026-07-20) — this is the current position.** Nothing from any of the five
    queries filters a row in Gold. The 2026-07-15 re-application of `SDDCTO='SX' AND SDKCOO='00750'` and of
    the F0101 `ABAT1` band is **reverted**. The `ABAT1` band survives as a CASE inside the `lofa` aggregate
    (a value definition, §3a), so `lofa_rate` is unchanged and `address_type_01` still need not be stored —
    it is available on the reused `dim_address_loading_facility` if the report wants to see or override the
    band.
