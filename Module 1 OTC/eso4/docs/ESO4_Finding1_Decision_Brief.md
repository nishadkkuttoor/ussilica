# ESO4 — Finding 1 Decision Brief: Fact Grain

**Prepared for:** Data Owner / Data Governance (ESO4 sponsor: Lisa Covington, Tax Department)
**Prepared by:** Gold Layer build + review team
**Date:** 2026-07-25 · **Status:** OPEN — decision required
**Decision needed before:** the first load of `fact_sales_tax_reconciliation` onto Fabric (see *Why now*)

---

## 1. The decision in one sentence

**At what grain should the ESO4 Gold fact be stored — the report-shaped *summary* grain it is built at
today, or the atomic *invoice pay-item* grain that keeps it reusable for future reporting?** This is a
design-governance decision, not a code bug; it needs an explicit ruling rather than being allowed to
stand by default.

---

## 2. Background (plain language)

ESO4 produces the **Sales Tax with Business Stream Summary** report, used to **reconcile JDE sales tax
against Avalara** (mirrors Hubble report TX002). It is served by one Gold fact table,
`fact_sales_tax_reconciliation`.

That table is currently built **pre-summarised to exactly the shape of this one report**: one row per
combination of the report's ~18 display attributes (company, invoice, document/order, plant, ship-to,
sold-to, tax area, jurisdiction, business stream, dates, …), with the four tax amounts **already added
up** across the underlying invoice pay items. In doing so it **drops the invoice pay-item identity**
(the F03B11 primary key) — the finest level of detail in the source.

Nothing has been loaded to Fabric yet: **no data exists, and no report or user consumes the table.**

---

## 3. The conflict

The Gold Layer Design Rule requires facts to be **reusable across future reporting needs** — i.e. stored
at an **atomic** grain so that *any* future report can re-aggregate them, rather than baking one report's
summary into the warehouse.

The current fact is baked to **one report's** summary grain. It reconciles beautifully today (a Hubble
row maps to a Gold row one-for-one), but it is **single-purpose by construction**.

---

## 4. What today's design gives you — and what it costs

**Gives you (why it was built this way):**
- **Simplest possible Avalara reconciliation** — Gold row ↔ Hubble row is a direct comparison, no
  re-aggregation needed.
- Smaller table; measures are plain sums.

**Costs you (what is given up — unrecoverable without a rebuild):**
- **No drill to pay-item level.** The invoice pay-item key is summed away; the fact can never break a
  number down to the individual AR pay item, nor join to any other AR-based fact at that level.
- **No other breakdowns.** One row is a fixed display tuple. A future report wanting sales tax **by pay
  item, by due date, by AR status,** or any attribute not already in the 18 **cannot be served** from
  this table.
- **Fragile to change.** Adding even one attribute changes the grain, forcing a **full rebuild and
  re-verification of all seven measures** — versus simply adding a column to an atomic fact.

---

## 5. Options

| # | Option | What it means | Trade-off | Cost to do |
|---|---|---|---|---|
| **A** | **Accept the summary grain** (documented exception) | Keep the fact as-is; record a formal exception to the reusability rule, treating it as a **purpose-built reconciliation asset**, not a general sales-tax fact. | Defensible *if* this is genuinely a one-report utility. The cost lands later, on whoever needs the next sales-tax/AR report. | **Zero now** |
| **B** | **Rebuild at atomic (pay-item) grain**, aggregate in DAX | Store one row per F03B11 invoice pay item (keep the PK); the report sums in the semantic model instead of in Spark. **Scoped in detail in the companion `ESO4_Finding1_OptionB_Rebuild_Sketch`.** | **Fully satisfies the Gold rule** and makes the data reusable. Hubble reconciliation becomes an *aggregation* comparison rather than row-by-row. | Fact redesign + re-verify the 7 measures — **cheapest done NOW** (nothing to migrate) |
| **C** | Keep summary fact **and** add an atomic fact later | Two facts over the same data. | Explicitly creates the **duplicate table the rule exists to prevent**; ongoing double-maintenance. | Highest over time — **not recommended** |

---

## 6. Recommendation

**Default to Option B (atomic pay-item grain) unless the data owner affirms this is a permanent,
single-report reconciliation utility — in which case Option A with a recorded exception.** Option C is
not recommended.

A simple test decides it:

> **Is it plausible that, within ~12 months, another report or analysis will need JDE sales-tax / AR
> data at a different breakdown** (by pay item, due date, AR status, or joined to another AR fact)?
> - **Yes / unsure →  Option B.** Reusability is the whole point of the Gold layer, and B is *free right
>   now* but expensive once the table is live.
> - **No, this is genuinely one report forever →  Option A,** with a signed exception recorded here.

The review team's lean: **B**, because the deciding cost (the rebuild) is at its minimum today and only
grows, while the reusability benefit compounds.

> **Option B is already scoped.** The companion **`ESO4_Finding1_OptionB_Rebuild_Sketch`** (.md / .docx) shows the
> concrete before/after — it is mostly *delete the Spark GROUP BY + retain the pay-item key* — and confirms every
> dollar figure stays identical, that Finding 7 auto-closes, and that the effort is ~1 build cycle with no migration.
> Read it before choosing, so the cost of B is a known quantity rather than an estimate.

---

## 7. Why now (time-sensitivity)

This decision is **free to make today and expensive to reverse later**, in one direction only:

- **Nothing is loaded and nothing consumes the table**, so switching to the atomic grain now is a code
  change with **no data migration and no downstream breakage**.
- **The moment the table goes live** — first load + a published report pointing at it — changing the
  grain means a migration, re-pointing every measure and visual, and coordinating with consumers.

The window closes at first load. Please rule **before** the initial `MANUAL_OVERWRITE=True` build.

---

## 8. What each choice triggers next

- **Option A:** record the exception (Section 9); no code change; proceed to the first load and Gate 2
  reconciliation as-is.
- **Option B:** rebuild `fact_sales_tax_reconciliation` at F03B11 pay-item grain (retain the PK), move
  the four sums into DAX measures, re-verify the 7 measures + the fact↔dim relationships, update the
  TMDL and the design doc; then first load. Adds roughly one build-and-review cycle — the full
  before/after and step-by-step are in **`ESO4_Finding1_OptionB_Rebuild_Sketch`**.

---

## 9. Decision record

| Field | Entry |
|---|---|
| **Decision (A / B / C)** | ________________________ |
| **Rationale** | ________________________ |
| **If A — exception approved by** | ________________________ (name / role) |
| **Reuse horizon considered** | ________________________ (the Section 6 test) |
| **Decided by** | ________________________ |
| **Date** | ________________________ |

---

**References:** `eso4/docs/ESO4_Finding1_OptionB_Rebuild_Sketch` (.md / .docx — the concrete Option B plan) ·
`eso4/docs/ESO4_Gate_Review.docx` (Finding 1, full analysis + what-is-lost table) ·
`eso4/docs/ESO4_gold_layer_design.md` (§3 grain & keys). Everything else in ESO4 (joins, ABAT1/Finding 5,
star schema, measures) is settled — **this grain ruling is the last open Gate 1 item.**
