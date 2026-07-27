# ESO5 — Power BI Report Guide (filters, duplicates, nulls)

**Everything the notebook deliberately does NOT do.** As of **2026-07-20 the notebook applies NO filter of
any kind** — not one row-selecting predicate anywhere. The fact is **the whole of F4211 ∪ the whole of
F4311**. *All* filtering — document type, company, TL/HOLADD selection, status, dates, the base FRT/NXTR
slicers, the Recon load/status slicers — is the report's job, and this document is the contract.

> ⚠ This changed on 2026-07-20. Between 2026-07-15 and then, `SDDCTO='SX'` and `SDKCOO='00750'` were
> applied in Gold. They are not any more, so the **`document_type` and `company` page filters are no longer
> a belt-and-braces double-up — they are now the only thing scoping a report to SX / 00750.**

Model: `eso5/report/sandbox_load_po.SemanticModel` (Direct Lake).
Design: [`ESO5_gold_layer_design.md`](ESO5_gold_layer_design.md) — §7b (filter contract), §7d (status).

---

## 1. The filter contract — REQUIRED on every page

The fact is a strict **superset** of every report. Three columns define which report you are looking at:

| Column | Values | Replaces the query's… |
|---|---|---|
| `document_type` | `SX`, `SO`, … | `SDDCTO = 'SX'` |
| `company` | `00750`, `00400`, … | `SDKCOO = '00750'` |
| `row_class` | `LINE` / `HOLADD` / `TEXT` / `PO_HOLADD` / `PO_OTHER` | `SDLNTY <> 'TL'`, `SDLITM <> 'HOLADD'`, and the F4311 leg's `PDDCTO='OX' AND PDLITM='HOLADD'` |

### `row_class` — five mutually-exclusive classes

| Class | The rows | Used by |
|---|---|---|
| `LINE` | F4211 line, not TL, not HOLADD | Core · (New) · (no-USS) · **Reconciliation** |
| `HOLADD` | F4211 line, not TL, item = HOLADD | **for HOLADD** · **Reconciliation** |
| `TEXT` | F4211 line, line type = TL | **Reconciliation** |
| `PO_HOLADD` | F4311 line, `OX` + item `HOLADD` | **for HOLADD** |
| `PO_OTHER` | every other F4311 purchase-order line | **nothing** — carried for symmetry only |

### Per-report page filters

| Report | Filter |
|---|---|
| **Core** — Sandbox Load Report w/ PO Details<br>**(New)** and **(no-USS)** variants | `document_type = "SX"`<br>`company = "00750"`<br>`row_class = "LINE"` |
| **…for HOLADD** | `document_type = "SX"`<br>`company = "00750"`<br>`row_class IN ("HOLADD","PO_HOLADD")`<br>**`po_holadd_superseded = "N"`** |
| **SBX Load Reconciliation** | `document_type = "SX"`<br>`row_class IN ("LINE","HOLADD","TEXT")`<br>⚠ **no `company` filter** — see note |

> ⚠ **Set `document_type`, `company` and `row_class` at REPORT level, not page level.** A new visual that
> forgets them does not produce a slightly-wrong number — it sums every sales order *and* every purchase
> order in the company.
>
> ⚠ **`document_type` is forced to `"SX"` on purchase-order rows** (the source query hard-codes `'SX' DCTO`
> so its UNION lines up with the sales load). So `document_type` + `company` alone do **not** exclude PO
> rows — **`row_class` is the only column that separates sales rows from purchase-order rows.** The PO's
> own document type lives in `po_order_type`.

---

## 2. Avoiding duplicate rows

**The fact contains no duplicate rows.** Every row is unique on `load_line_key`, deduped at build time.
Anything doubled in a visual comes from one of the four causes below — and only the first is a *true*
duplicate.

### 2.1 The real one: the HOLADD double-count ⚠

A holding charge can exist **twice** — once as an F4211 `SX` HOLADD **sales** line, and once as an F4311
`OX` HOLADD **purchase-order** line. Hubble suppressed the PO copy with a `NOT EXISTS`. That test is a
status filter, so the notebook does not apply it; it is calculated onto every row instead:

> **`po_holadd_superseded = 'Y'`** ⇔ the load already carries a live (`last_status <> '980'`) SX HOLADD
> sales line, i.e. **this PO row is a duplicate of a sales line**.
> **Hubble's orphan set is exactly `po_holadd_superseded = "N"`.**

**Filter `po_holadd_superseded = "N"` on the HOLADD page.** Without it, every already-invoiced holding
charge appears twice and `Total Amount` / `OX Amount` double.

### 2.2 Missing `row_class` filter — a load repeated once per class

One load contributes its `LINE` rows *and* its `TEXT` rows *and* its `HOLADD` rows — and the whole of
F4311 rides along. The load number repeats, which reads as duplication. Fixed by §1.

### 2.3 Load-level values repeated on every line — inflated totals ⚠

The subtle one. These columns are **constant per load** but physically stored on **every line** of that
load, because the fact is at line grain:

`sbx_weight` · `uss_so_weight` · `load_last_status` · `load_max_last_status` · `load_min_last_status` ·
`bol` · `sand_ticket` · `sand_po_number` · `uss_*` · `header_*` · `leg_1/2/3` · `qc_string_3`

and `lofa_rate` is constant per **loading facility**.

At line grain they display correctly. But the moment you **total, subtotal, or roll up**, `SUM` multiplies
them by the line count.

**FIXED in the model (2026-07-14).** Four measures shipped with this defect and have been rewritten:

```dax
SBX Weight    = SUMX(VALUES(fact_extended_sales_order_5[load_scope_key]),
                     CALCULATE(MAX(fact_extended_sales_order_5[sbx_weight])))

USS SO Weight = SUMX(VALUES(fact_extended_sales_order_5[load_scope_key]),
                     CALCULATE(MAX(fact_extended_sales_order_5[uss_so_weight])))

Rate          = SUMX(VALUES(fact_extended_sales_order_5[loading_facility]),
                     CALCULATE(MAX(fact_extended_sales_order_5[lofa_rate])))

Load Count    = DISTINCTCOUNT(fact_extended_sales_order_5[load_scope_key])
```

`SUMX(VALUES(key), …)` counts each load (or facility) **once**, however many lines it has.

**Why `load_scope_key` and not `load_number`:** `load_scope_key` is
`sha2(company ‖ document_type ‖ load_number)` — the *true* load identity. `load_number` alone would conflate
two loads that share a document number under different companies or document types, which the fact now
spans. It is a hidden column, but DAX can still reference it.

`Total Amount`, `OX Amount`, `Quantity` and `Line Count` are genuinely per-line and remain `SUM` /
`COUNTROWS`. The 19 reconciliation measures (§7c) sum **line-level** columns (`units_ordered`, `item_weight`,
`total_amount`) filtered by item, which is exactly what the per-load pivot means.

> ⚠ **Fixed 2026-07-20 — they were NOT correct as-is.** All 19 guarded `row_class <> "PO_HOLADD"`, which
> does not exclude **`PO_OTHER`** (any F4311 line that is not OX+HOLADD). Those rows carry
> `item_number = PDLITM` and `units_ordered = PDUORG`, so every measure keyed on `item_number`
> double-counted — `Miles` returned 108 where Hubble returned 54. And because a boolean predicate inside
> `CALCULATE` is `FILTER(ALL(column), …)`, the guard **replaced** the page's `row_class` filter rather than
> intersecting with it, so a correctly-built page could not prevent it. All 19 now use the positive set
> `row_class IN ("LINE","HOLADD","TEXT")`, which is self-sufficient.

### 2.4 The Reconciliation page must be at LOAD grain

Its native grain is **one row per load**. Put only load-level fields on the visual —
`load_number`, `header_*`, `bol`, `sand_ticket`, `load_last_status`, `leg_1/2/3` — plus the 19
reconciliation measures (design §7c).

**Drop `line_id`, `item_number`, or any line-level column onto it and it fans out to one row per line**,
which looks exactly like duplication.

### 2.5 Verify the dimension keys are unique

All six relationships are **many-to-one**, so they cannot fan out — *provided the dim keys are unique*. If
`rpt.dim_address_book` has more than one row per `address_number`, or `rpt.dim_plant` more than one
per `plant_code`, Power BI silently degrades the relationship to many-to-many and **every fact row
multiplies**. Check after loading:

```sql
SELECT address_number, COUNT(*) FROM rpt.dim_address_book GROUP BY address_number HAVING COUNT(*) > 1;
SELECT plant_code,     COUNT(*) FROM rpt.dim_plant        GROUP BY plant_code     HAVING COUNT(*) > 1;
```

---

## 3. Null values, per report

Structural analysis of the five queries (which fields *can* be null and why), for deciding page filters.
Every null comes from a **LEFT JOIN** that misses, a **scalar subquery** with no row, an **aggregate over an
empty set**, or a **`DECODE`/`CASE` with no `ELSE`**. Base F4211/F4311 columns are never null — JDE pads and
zero-fills.

### 3.1 Core (and **(New)**, which is the identical query)

| Fact column | Null when |
|---|---|
| `sand_po_number` | LEFT JOIN — load has no F554201T QC row |
| `bol` | load has no `item_number = 'BOL'` line |
| `sand_ticket` | load has no `item_number = 'SANDTKTNBR'` line |
| `ox_last_status`, `ox_next_status` | no OX purchase-order line for that (item, load) |
| `ox_amount` | **only on FRT lines** — `SUM` over an empty OX set (non-FRT lines get `ELSE 0`) |
| `carrier_po_gl_post_flag` | no matching F0911 `OV` document |
| `po_receipt_gl_date` | no matched F43121 receipt (`match_type='1'`, real date) |
| *(dim)* `name_alpha`, and `lofa_rate` | LOFA has no F0101 row — **see §4** |
| **`uss_match`** | **null, not `'N'`** — `SELECT DISTINCT 'Y'` returns nothing when there is no SO match |
| `uss_so_order_no` | no SO match, *or* the SO's MCU is not the vendor's 55/UP plant |
| `uss_customer_po`, `so_alt_bol_no`, `uss_so_weight` | no SO match |
| `sbx_weight` | load has no COM line, or all COM lines are cancelled (`last_status = '980'`) |
| *(dim_uss_plant)* `uss_plant_sand`, `shipped_from`, `lofa_mcu` | **vendor not in UDC 55/UP** |

> ⚠ **The whole USS block goes null together.** SBXUSSSAND is `F4211 (SANDTKTNBR) INNER JOIN F554201T`,
> then LEFT-joined on. A load with no sand-ticket line *or* no QC row loses all nine fields at once.

Never null: `load_number`, `document_type`, `company`, `district`, `sold_to`, `ship_to`, `carrier`,
`customer_po`, `item_number`, `item_description`, `order_date`, `gl_date`, `loading_facility`, `uom`,
`quantity`, `unit_price`, `total_amount`, `last_status`, `next_status`, `invoice_number`, `line_id`.

### 3.2 …PO Details (no-USS)
The core's list **minus the nine USS fields** — that join does not exist in this query.

### 3.3 …for HOLADD

**Leg A** (`row_class = 'HOLADD'`): as the core's non-USS set. `ox_amount` is null when the line is a *live*
HOLADD (`last_status <> '980'`) with no OX PO behind it.

**Leg B** (`row_class = 'PO_HOLADD'`) — **the nulls concentrate here.** Every sales-side attribute is a
correlated `MAX(…)` over the load's **FRT** line, so if the load has no FRT line they go null *together*:

| Fact column | Null when |
|---|---|
| `customer_po`, `sold_to`, `loading_facility`, `last_status`, `next_status`, `invoice_number` | the load has no SX **FRT** line |
| *(dim)* `name_alpha`, `lofa_rate` | cascades from a null `loading_facility` |
| `line_type`, `product_category`, `sales_report_code_01`, `item_weight` | **always** — these are F4211 columns and a PO row has none |

Never null on leg B: `district`, `ship_to`, `carrier`, `item_number`, `item_description`, `uom`,
`quantity`, `order_date`, `unit_price` / `total_amount` (forced to 0), `ox_last_status` / `ox_next_status` /
`ox_amount` (the row's own PD values), `line_id`, and `gl_date` (its CASE maps null → 1900-01-01).

### 3.4 SBX Load Reconciliation — almost every pivot is nullable

Every pivot is `SUM(DECODE(…))` / `MAX(DECODE(…))` **with no default**, so each is null when the load has no
line of that kind:

| Null when the load has no… | Measures |
|---|---|
| **COM (sand) line** | Sand Weight, Sand Tons, Proppant, Load LOFA |
| **FRT line** | Ext Weight, Ext Tons, Miles, Freight Amount |
| **detention line** | LOFA/Well Detention Hours, LOFA/Well Amount, …PP Amount, …PB Amount |
| **FSC / SRP1='352' / HOLADD line** | Fuel Surcharge Amount, Sand Amount, Holding Amount |
| **BOL / SANDTKTNBR line** | `bol`, `sand_ticket` |
| **F554201T row** (LEFT JOIN) | `sand_po_number`, `leg_1`, `leg_2`, `leg_3`, `qc_string_3` |

Two special cases:
- **`load_last_status`** is null when the load is *not* all-980 **and** has no non-HOLADD line with a
  non-zero amount — a load whose only real money is a holding charge.
- Hubble's `ReportColumn1` is `MAX(FLOOR(TO_NUMBER(NULL)))` — **always null by construction.** It is a
  placeholder for "this report has no measure". Ignore it; there is no fact column for it.

### 3.5 Nulls in the FILTER fields — what silently disappears

A filter on a nullable column **drops the null rows**: `NULL <> 'TL'` is `NULL`, not `TRUE`, and Power BI
excludes `BLANK` from a column filter unless you include it explicitly.

| Filter field | Used by | Nulls? |
|---|---|---|
| `document_type`, `company`, `item_number`, `order_date` | all | never null |
| **`line_type`** | core / (New) / (no-USS) / HOLADD leg A (`<> 'TL'`) | ⚠ **NULL on every PO row** |
| **`po_order_type`** | HOLADD leg B (`= 'OX'`) | ⚠ **NULL on every F4211 row** |
| **`load_last_status`** | Reconciliation (`NOT IN ('980')`) | ⚠ **nullable** (see §3.4) |
| `next_status` | (no-USS) (`< '581'`) | null on PO rows — not reachable there (`row_class = "LINE"`) |
| `po_holadd_superseded` | HOLADD (`= 'N'`) | never null — always `'Y'` or `'N'` |

> ⚠ **`line_type` is the dangerous one.** It is null on every purchase-order row, and three of the five
> queries filter on it. Applied at **page** level on the HOLADD report, `line_type <> "TL"` **silently
> deletes the entire PO leg** — the report loses exactly the orphan holding charges it exists to show.
> **Scope `line_type` and `po_order_type` inside a `row_class` filter; never put them at page level.**

> **`load_last_status` null → excluded, matching Hubble.** Oracle evaluates `NOT (NULL IN ('980'))` to
> `NULL`, so Hubble drops those loads; Power BI drops blanks the same way. If you *want* them:
> `load_last_status <> "980" || ISBLANK(load_last_status)`.

---

## 4. The F0101 rate search-type range — a value definition, not a filter

The core query restricts its F0101 LEFT JOIN to `ABAT1 BETWEEN 'A'–'P' OR 'R'–'ZZZ'` (address search type —
everything except the `Q` band). Even under the no-filter rule this is **kept**, because it is not a report
filter: it says *which address-book rows count as a rate source*. Deleting it would not make `lofa_rate`
"unfiltered", it would make it **wrong** — a `Q`-band facility would start reporting a rate that Hubble
shows as blank.

So it lives in a **CASE inside the `lofa` aggregate**, never a `WHERE`. No row is dropped and every loading
facility keeps its group.

- **Hubble:** an address outside those search types → **blank** `lofa_rate`.
- **Ours:** → **NULL** `lofa_rate`. ✅ same behaviour, and no row lost either way.

`address_type_01` need not be stored on the fact — and it **is** available on the reused
`dim_address_loading_facility`, so a report that wants to see or override the band can. (`name_alpha`
resolves through that same dim, which is not `ABAT1`-scoped; the rate is the only field the range affects.)

---

## 5. Display bindings — two fields do not render as their fact column

| Report field | Bind to | Notes |
|---|---|---|
| **District** — code `771010` | `fact[district]` (the raw code — FK / grain / filter key; this is what the report table shows, matching Hubble) | for the name, add `dim_plant[plant_name]` as a second column |
| **Line ID** — `1000` | `fact[line_id]` — already the raw JDE value (`1.00` → `1000`) | — |

**District migrated to `rpt.dim_plant` (2026-07-26).** The reused `dim_plant` has `plant_code` and
`plant_name` as **separate** columns and no single `"code - description"` concat (Direct Lake cannot
build one in DAX). The ESO5 tables already show `fact[district]` (the raw code), so nothing needs to
change; if `"771010 - SBX TRANS DISTRICT-WEST TEXAS"` is wanted as one field, show `fact[district]` +
`dim_plant[plant_name]` as two columns, or request a concat column on the shared `rpt.dim_plant`. Design §6a.

---

## 6. Pre-flight checklist

- [ ] `document_type = "SX"` + `company = "00750"` + `row_class` set at **report** level
- [ ] **`po_holadd_superseded = "N"`** on the HOLADD page (prevents the double-counted holding charge)
- [ ] `line_type` / `po_order_type` scoped inside `row_class` — **never** at page level
- [x] ~~`SBX Weight`, `USS SO Weight`, `Rate`, `Load Count` rewritten to dedup per load/facility~~ — **done in the model** (§2.3)
- [ ] Reconciliation page carries **no line-level column** (§2.4)
- [ ] `dim_address_book` / `dim_plant` keys confirmed unique (§2.5)
- [ ] District shown from `fact[district]` (raw code); optional `dim_plant[plant_name]` for the name (§5)
- [ ] Decide on `address_type_01` (§4)
