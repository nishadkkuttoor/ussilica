# ESO4 — Option B Rebuild Sketch (Atomic Pay-Item Grain)

**Companion to:** `ESO4_Finding1_Decision_Brief.md` · **Date:** 2026-07-25 · **Status:** SKETCH (scoping only — no code changed)

Purpose: show concretely what the Option-B rebuild involves so the effort is understood *before* the
data-owner ruling. The headline: **most of the work is already done** — the joins, calcs, dims,
relationships, and the four amount measures are unchanged. The rebuild is mainly *removing* a step
(the Spark GROUP BY) and *keeping* the pay-item key that is currently thrown away.

---

## 1. Target grain & key

| | Today (summary) | Option B (atomic) |
|---|---|---|
| **One row =** | one Hubble display tuple (18 attrs), amounts summed | one **F03B11 invoice pay item** (with its F4211 line context) |
| **Stored key** | `sales_tax_line_key = sha2(18 group cols)` | `sales_tax_line_key = sha2(F03B11 PK)` = `sha2(ar_company_key‖ar_document_type‖ar_document_no‖ar_pay_item)` |
| **Pay-item PK (RPKCO/RPDCT/RPDOC/RPSFX)** | **dropped** (summed away) | **retained** as stored grain columns |
| **Amounts** | pre-summed per display tuple | per pay item (additive — summed in DAX at query time) |
| `document_scope_key` | unchanged | unchanged (invoice-document delete/scope key) |

---

## 2. The fact-notebook change (`build_fact`)

The join block, ABAT1 band, latest-effective address, `business_stream`, `avalara_code`, and the four
amount expressions are **untouched**. The change is at the tail — **delete the aggregation, keep the
atomic projection**:

```python
# --- UNCHANGED: joins + calcs + the projection that already keeps the F03B11 PK ---
sel = j.select(... ar_company_key, ar_document_type, ar_document_no, ar_pay_item, ... ).distinct()
#              ^ the inner SELECT DISTINCT — THIS BECOMES THE FACT (already one row per pay item)

# --- REMOVED: the Spark-side GROUP BY that collapsed pay items into display tuples ---
# agg = sel.groupBy(*FACT_GROUP_BY_COLS).agg(F.sum(...), F.first("shift_factor_applied"))   # ← deleted

# --- CHANGED: keys off the pay-item PK; tax_status per pay item (see §5) ---
df = (sel
      .withColumn("tax_status", F.when(F.col("tax_amount") > 0, F.lit("Taxable")).otherwise(F.lit("Non-Taxable")))
      .withColumn("document_scope_key", sk("document_company","document_type","invoice_number"))
      .withColumn("sales_tax_line_key",
                  sk("ar_company_key","ar_document_type","ar_document_no","ar_pay_item")))   # F03B11 PK
df = df.dropDuplicates(["sales_tax_line_key"])
return df.select("sales_tax_line_key","document_scope_key", *FACT_BUSINESS_COLS)   # FACT_BUSINESS_COLS now includes the 4 PK cols
```

**Column-list simplification:** the `FACT_GROUP_BY_COLS` / `FACT_MEASURE_COLS` / `FACT_CARRY_COLS`
split disappears (there is no GROUP BY to feed). It collapses to **one flat `FACT_BUSINESS_COLS`**
list: the 4 PK columns + the 18 former display attrs (now plain degenerate-dim / FK columns) + the 4
amounts (now additive measures) + `shift_factor_applied` + `tax_status`. The `# 2) FACT BUILDER` note
that explains the group-by lists is deleted (they no longer differ from ESO7 — the fact is now atomic,
like ESO7).

---

## 3. Schema / stored columns

- **+4 columns** (the retained F03B11 PK): `ar_company_key`, `ar_document_type`, `ar_document_no`,
  `ar_pay_item` (hidden in the model; they are the grain, not report fields).
- The other 24 business columns are **the same columns** (only their *meaning* shifts from
  "summed per tuple" to "per pay item").
- New count: **28 business + 2 keys = 30 stored** (was 24 + 2 = 26).
- **Row count grows** to the number of qualifying invoice pay items — expected and desired (that is
  the atomic detail the summary grain discarded).

---

## 4. Semantic model & measures — mostly a no-op

Because the report already aggregates through **SUM measures**, moving detail into the fact is
**transparent to the model** — Power BI/Direct Lake re-aggregates the pay-item rows to whatever grain
a visual groups by. Per-measure:

| Measure | DAX | Change under Option B |
|---|---|---|
| Taxable Amount | `SUM(fact[taxable_amount])` | **none** — now sums pay items instead of pre-summed rows; same result |
| Non-Taxable Amount | `SUM(fact[non_taxable_amount])` | **none** |
| Tax Amount | `SUM(fact[tax_amount])` | **none** |
| Gross Amount | `SUM(fact[gross_amount])` | **none** |
| Effective Tax Rate | `DIVIDE([Tax Amount],[Taxable Amount])` | **none** (measure of measures) |
| Invoice Count | `DISTINCTCOUNT(fact[avalara_code])` | **none** |
| Tax Lines | `COUNTROWS(fact)` | **now correct** — counts actual pay-item lines → **Finding 7 auto-resolved** |

Relationships (plant → dim_plant; ship_to/sold_to/parent → dim_address_*; sic_code → dim_sic;
jurisdiction → dim_state) are **unchanged** — the FK codes still live on the fact. The dims
(reused dim_plant, dim_sic, dim_state, reused address role views) are **unchanged**.

---

## 5. The one real subtlety: `tax_status`

This is the only place "aggregate in DAX" needs a genuine decision. `tax_status` is derived from
`tax_amount`, and today it is computed once from the **summed** amount per display tuple. At atomic
grain a single display tuple can contain both taxable and exempt pay items, so:

- **If `tax_status` is only DISPLAYED (not sliced):** make it a **DAX measure**
  `IF([Tax Amount] > 0, "Taxable", "Non-Taxable")`. This is display-grain-correct (matches today) **and
  cleaner** — it reverses the Finding-2 physical-column workaround, since Direct Lake *does* allow
  measures (only calculated columns are forbidden). **Recommended.**
- **If `tax_status` is used as a SLICER / group-by:** keep a **physical per-pay-item** column (as
  built above). Then a mixed display tuple legitimately splits into a Taxable and a Non-Taxable row.
  Confirm with the Tax team whether that matches Hubble's intent before choosing this.

Action: confirm how the report uses `tax_status` (display vs slicer). Default to the **measure**.

---

## 6. Reconciliation becomes an aggregation comparison

Today: Hubble row ↔ Gold row, one-for-one. Under Option B, the Gate-2 check sums the atomic fact up
to the Hubble grain and compares — the same numbers, one aggregation away:

```python
# Gate-2 tie-out (atomic fact → Hubble display grain):
fact.groupBy(*HUBBLE_18_ATTRS).agg(F.sum("taxable_amount"), F.sum("non_taxable_amount"),
                                   F.sum("tax_amount"), F.sum("gross_amount"))
# ...should equal the Hubble TX002 output row-for-row.
```

The gate review's reconciliation ladder (row count → grand totals → by Business Stream → by
jurisdiction → spot-check → Invoice Count) is unaffected; only the "row count" step reframes to the
pay-item count.

---

## 7. What is UNCHANGED (the bulk of the build)

- All joins (F4211 INNER F03B11; F0006 LEFT; F0101 ⋈ F0116 LEFT) + the **ABAT1 band** + the
  **latest-effective** address pick.
- `business_stream` CASE, `avalara_code`, the four amount expressions, `SHIFT_FACTOR`.
- The batch + ESO7 notebook structure (`build_fact`, `sname`/`gname`, `MANUAL_OVERWRITE`, `FACT_SOURCES`,
  RUN + exit payload).
- All three dims and all six relationships; four amount measures; Effective Tax Rate; Invoice Count.
- The `document_scope_key`.

---

## 8. Findings resolved / improved by Option B

- **Finding 1 (HIGH) — CLOSED.** The fact is now atomic and reusable; no rule exception needed.
- **Finding 7 (LOW) — CLOSED for free.** `Tax Lines = COUNTROWS(fact)` now counts real pay-item lines,
  so the name is no longer misleading.
- **Finding 2 (context).** `tax_status` can return to a clean DAX **measure** (§5) instead of the
  physical-column workaround, if it is display-only.

---

## 9. Step-by-step + effort

1. **Fact notebook** — delete the `groupBy/agg`; return `sel` (atomic); add the 4 PK columns to
   `FACT_BUSINESS_COLS`; re-key `sales_tax_line_key` off the PK; decide `tax_status` (§5). Collapse the
   `FACT_*` lists to one. *(~½ day)*
2. **TMDL (fact)** — add the 4 PK columns (hidden); if `tax_status` → measure, remove the physical
   column + add the measure; re-point `sales_tax_line_key`. *(~¼ day)*
3. **Re-verify the 7 measures** against the atomic fact (all should be unchanged except Tax Lines /
   tax_status). *(~¼ day)*
4. **Design doc** — rewrite §3 grain & keys; note Findings 1 & 7 closed. *(~¼ day)*
5. **First load** `MANUAL_OVERWRITE=True`, then the Gate-2 aggregation tie-out (§6). *(gated on Fabric)*

**Estimate: roughly one build-and-review cycle (~1–1.5 days of build/doc work)** before the same
first-load + reconciliation that Option A also requires. No migration (nothing is live).

---

## 10. Risks / open decisions

- **`tax_status` display-vs-slicer** (§5) — the only genuine design choice; default to a DAX measure.
- **Row-count / performance** — atomic grain is larger, but F03B11 sales-tax pay items are a bounded
  set and Direct Lake sums are cheap; no concern at F64.
- **Nothing else regresses** — every other number is identical (the SUM measures see the same total),
  so the reconciliation risk is *lower* than a redesign usually implies.

---

**Bottom line:** Option B is mostly *deletion* (the GROUP BY) plus *retention* (the PK). It closes
Findings 1 and 7, needs one small `tax_status` decision, and leaves every dollar figure identical — all
for ~1 build cycle while nothing is live. This sketch is scoping only; no notebook has been changed.
