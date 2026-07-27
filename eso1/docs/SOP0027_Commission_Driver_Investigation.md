# SOP0027 Commission — Driver Investigation & Re-architecture

**Date:** 2026-07-24
**Notebook:** `eso1/nb/nb_eso1_gold_fact_sales_commission.py`
**Query:** `eso1/Filter Capture/SOP0027 - Commission.sql`
**Outcome:** fact driver flipped **F42005 → F4211** to match the query (Option 1: flip driver, keep LEFT joins, Gold stays filter-free).

---

## 1. Trigger — "most `gl_date` values are empty"

Reported that in the commission report most `dt_for_gl_and_vouch_01` (`gl_date`) values were blank. `gl_date` on the fact comes from the F4211 line context (`ln.dt_for_gl_and_vouch_01`) via the join from the commission driver.

Two possible causes were separated up front:
- **Case A (expected):** JDE only stamps `SDDGL` at Sales Update (R42800); un-posted lines carry `0` → nulled by `clean_date`. Would resolve under the report's status/date page filters.
- **Case B (defect):** the F4211 line-context join is missing → **all** `ln.*` columns null together.

---

## 2. Diagnostics (run on Fabric)

### 2.1 Null pattern — all context columns null together → Case B
```
 rows |gl_date_null|ext_price_null|ship_date_null|item2_null
10874 |    8228     |    8228      |    8247      |   8228
F42119 exists: True
```
`gl_date`, `extended_price`, `actual_ship_date`, `second_item_number` are null on the **same ~8,228 rows (76%)** → not a `gl_date` issue; the whole F4211 line context is missing. `F42119` history table **exists**, so "history not loaded" was ruled out.

### 2.2 Order-level vs line-level match → data completeness, not key drift
```
commission order-lines: 10797 | match at ORDER+LINE: 2645   (24.5%)
commission orders     :  8913 | match at ORDER     : 2203   (24.7%)
F42005 line_number sample: 1.0, 1.4, 24.0, 50.0, 120.0 ...
F4211  line_number sample: 1.006, 1.059, 4.010, 4.201 ...
```
ORDER match (24.7%) ≈ ORDER+LINE match (24.5%) → the **whole order** isn't in Silver `F4211 ∪ F42119`, not a line-key problem. `line_number` samples share the same decoded scale → **no key/scale drift**.

**Conclusion:** ~75% of F42005 commission records reference sales orders that don't exist in Silver `F4211 ∪ F42119` (F42005 ledger reaches back further than the F4211/F42119 load window). Same pattern as the ESO5 finding (~76%).

### 2.3 Spot-check: a specific F4211 order that *didn't* show (order 1635581)
```
fact rows for order 1635581: 0
F42005 rows for order 1635581: 0
```
A valid, completed (`status 999`), sales-updated (`program_id = EP42800`) F4211 line with a real `gl_date`, but **zero commission records**. Confirmed the reverse symptom of the same root cause.

---

## 3. Root cause — driver inversion

| | Query (SOP0027) | Notebook (before) |
|---|---|---|
| Driver | **F4211** | **F42005** |
| Commission join | `LEFT JOIN F42005` | driver |
| Line with **no commission** | **shown** (blank commission cols) | **dropped** |
| F42005 with **no F4211 line** (the 76%) | dropped (INNER F4211) | kept, null context |

Both symptoms (empty `gl_date` on 76% of rows; order 1635581 missing) are the same architectural difference: the notebook drove from **F42005**, so it kept orphan commissions (null line context) and dropped non-commissioned sales lines — the opposite of Hubble's F4211-driven query.

---

## 4. Join cross-check (query vs notebook, before the change)

Keys were all correct/equivalent; the differences were structural:
- **Driver inverted** (F4211 vs F42005).
- **F4201 header:** query INNER, notebook LEFT.
- **F0101 sold-to:** query INNER + ABAT1 band filter; notebook LEFT + no filter (carried as `sold_to_search_type`).
- **F0101 keyed to sold-to `SHAN8`** in both (correct — not the ship-to).
- **ShiftFactor temp** → constant `1.0` (Silver pre-decoded; H1 caveat).
- Notebook adds **F42119** union (history context) not present in the query.

---

## 5. Decision

**Option 1 — flip the driver to F4211 ∪ F42119, keep all joins LEFT (Gold stays filter-free).**

Rejected alternatives:
- *Match join TYPES (INNER F4201/F0101)* — would drop rows in Gold.
- *Exact query incl. ABAT1 in Gold* — bakes a business filter into the fact, violating the standing "all filters at Power BI page level" rule.

The query's INNER/ABAT1 semantics are reproduced as **Power BI page filters** instead: `sold_to IS NOT NULL` and `sold_to_search_type` in A–P / R–ZZZ.

---

## 6. Changes applied to `nb_eso1_gold_fact_sales_commission.py`

New join model:
```
ln = F4211 ∪ F42119      (DRIVER — all sales lines)
  LEFT JOIN sc = F42005  on KCOO+DCTO+DOCO+LNID     (commission optional)
  LEFT JOIN sh = F4201   on KCOO+DCTO+DOCO           (sold-to)
  LEFT JOIN cc = F0101   on sold_to (SHAN8)          (category)
```
- Grain keys (`company_key_order_no`, `order_type`, `order_number`, `line_number`, `company`) now from the `ln` driver → never null.
- `commission_line_number` from LEFT `sc` → null for non-commissioned lines.
- `restrict_orders` semi-join moved to the `ln` driver.
- `sales_commission_key` coalesces the nullable `commission_line_number` to a `__NOCOMM__` sentinel (deterministic, unique).
- `is_primary_commission_line` unchanged (`asc_nulls_last` → the single no-commission row is primary; line metrics still dedup once).
- Preflight now requires **F4211 (driver)** + F42005; docstring, FACT header, handler comments updated.

**Behavior now:** non-commissioned lines (e.g. order 1635581) appear with null commission; orphan F42005 commissions (the 76%) drop → matches Hubble.

**Schema unchanged** → the semantic model (`nb_semantic_model_eso1.py`) and the TMDL twin need **no edits**. `SUM(amount_commission)` ignores the new nulls; the `is_primary`-based line measures still dedup correctly. Byte-compiles clean.

---

## 7. Follow-ups

1. **Rebuild with `MANUAL_OVERWRITE = True`** on Fabric (grain + row set change), then flip back to `False`.
2. **Page filters to reproduce Hubble:** `sold_to IS NOT NULL`, `sold_to_search_type` in A–P/R–ZZZ, `status_code_next="999"`, `status_code_last<>"980"`, `gl_date` range (report year, not the query's hardcoded 2021), `order_type IN (SO,CO)`, `company<>"00750"`, `line_type NOT IN (F,FT)`, `second_item_number NOT IN (MISC BILLING, EXPEDITE FEE)`.
3. **Stale docs to update:**
   - `docs/SOP0027 - Commission.docx` §4 still says *Grain = one row per F42005 commission record / Driver: F42005*.
   - Memory `project-eso1-commission-fact.md` still says *Driver F42005 + LEFT F4211*.

---

## 8. Reference — the empty-`gl_date` breakdown

Two distinct null sources for `gl_date`:
1. `clean_date` nulling a `0`/sentinel Julian (a line that reached the fact but was never sales-updated) — small subset.
2. **Missing F4211 line context** (76% of commission records had no line) — the bulk, now resolved by the driver flip (those orphan commissions drop instead of appearing null).
