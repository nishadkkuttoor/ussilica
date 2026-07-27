# Extended Sales Order 1 — Star Schema Design (Gold Layer, Near‑Real‑Time)

> **Report:** Billable v Payable Freight · **Platform:** Microsoft Fabric (F64) · **Gold lakehouse:** `lh_jde_gold` (schema `rpt`)
> **Latency target:** Power BI current within **5 minutes** of Event Hub. **Refresh:** scheduled notebook every 5 min, **incremental MERGE**.
> Built on verified Silver facts (`ESO1_Silver_Data_Analysis.docx`), the source dictionary (`table_metadata.md`), and the `old_nb/` conventions. Last updated: 2026-06-16.

## 1. Approach — two NEW current‑state facts + reuse the 3 dimensions

This project is **near‑real‑time** (CDC every 5 min). The previous project's `customer_order_line` fact uses a **daily snapshot** pattern (`snapshot_date`, DELETE‑today+APPEND) designed for historical accumulation — **it is NOT reused here**. Re‑appending a full snapshot every 5 minutes would inflate storage, exceed the compute budget, and add latency.

Instead ESO1 builds **two new current‑state facts maintained by incremental MERGE**:
- **`fact_sales_order_line`** — order‑line grain (replaces, for this project, the historical `customer_order_line`).
- **`fact_freight_audit`** — shipment grain (billable vs payable freight $; the report's headline measure, not modelled anywhere yet).

The **3 dimensions are reused** (`dim_address_book`, `dim_plant`, `dim_item_cost_cascade`) — they are Type‑1 current‑state reference data, not historical snapshots.

> Naming: this project uses the **`fact_` prefix** (per `Fabric_Naming_Convention_Guidelines.pdf`), even though the previous project's facts had no prefix.

## 2. Pattern — current‑state upsert (not snapshot)

| | Previous project (historical) | This project (near‑real‑time) |
|---|---|---|
| Grain | one row per key **per `snapshot_date`** | one **current** row per key |
| Refresh | daily, DELETE+APPEND full snapshot | **scheduled notebook every 5 min**, incremental **MERGE** |
| Time columns | 7 `snapshot_*` columns | `source_commit_timestamp` (data "as of") + `gold_updated_timestamp` (merge time) |
| Deletes | n/a | CDC soft‑delete → `is_deleted` flag |
| Compute | rebuild each day | touch only keys changed since the **watermark** |
| Storage | grows by full row count per day | bounded to current row count |

## 3. Architecture / data flow
```
JDE (PRODDTA) ──Debezium/CDC──> Azure Event Hub ──> Bronze (append, raw CDC)
        ──> Silver (MERGE to decoded current-state; is_delete soft-delete)
        ──> Gold fact (scheduled notebook /5 min: incremental MERGE)
```
- **Watermark:** each Gold run reads only Silver rows whose CDC commit timestamp > last processed watermark (stored per fact). No full scan.
- Decoding (Julian→date, implied decimals, snake_case, NULLs) is already done Bronze→Silver — not repeated in Gold.

## 4. New fact — `fact_sales_order_line` (current‑state)
| Property | Value |
|---|---|
| Grain | one **current** row per order line |
| Business key | `company_key_order + order_type + document_order_invoice_e + line_number` |
| Merge key | `order_line_key` = hash(business key) — also the Power BI relationship key |
| Sources | F4211 (driver) + F41002/F4074/F4201/F4101/F5642B01/F5642B11 (Silver), `dim_item_cost_cascade` (cost) |
| Write | `MERGE` on `order_line_key`: UPDATE matched · INSERT new · set `is_deleted=true` on CDC delete |

**Hard filters (carried from the v7/house logic):** `company IN ('00640','00645')` · `line_type='S'` · `status_code_last <> '980'` · `status_code_next < '561'` · `order_type IN ('SE','SZ','S1','ST','SG')`; INNER DQ gate `dim_address_book.address_type_01` (ship‑to) in A–P / R–ZZZ.

**Column groups (business content):**
- **Keys / degenerate:** `order_line_key`, `company`, `key_company_order`, `order_number`, `order_type`, `line_number`, `shipment_number` (bridge), `bol_number`.
- **Dimension keys:** `item_number_short`, `branch_plant` (→`dim_plant`), `ship_to`/`bill_to`/`carrier_number`/`loading_port`/`ocean_carrier`/`destination_port` (→`dim_address_book` roles), cost via (`branch_plant`,`item_number_short`,`effective_year`) → `dim_item_cost_cascade`.
- **Event dates:** `order_date`, `requested_date`, `scheduled_pick_date`, `promised_ship_date`, `actual_ship_date`, `invoice_date`, `effective_date`.
- **Measures:** `units_transaction_qty_tons`, `quantity_shipped_tons`, `units_open_tons`, `revenue_dollars`, `price_per_unit_TN`, `item_unit_cost`, `calculated_latest_lane_price`, `factor_value`/`adjustment_price_unit`.
- **Status / freight / ocean:** `status`, `status_sort`, `hold_orders_code`, `freight_handling_code`, `mode_of_transport`, `seal_number`, `booking_number`, ocean fields.
- **NRT/audit (new):** `is_deleted`, `source_commit_timestamp`, `gold_updated_timestamp`, `record_hash`.

> Fan‑out guards retained: F4074 collapsed via DISTINCT per (order,line); F5642B11 join includes `shipment_number`; F0116 latest‑effective in the dim.

## 5. New fact — `fact_freight_audit` (current‑state)
| Property | Value |
|---|---|
| Grain | one **current** row per shipment (F4981 pre‑bucketed) |
| Business / merge key | `shipment_number` |
| Sources | F4981 (`is_delete=0`); `route_number` from F4981 `FHRTN` (see §12) |
| Write | incremental: find shipments with any changed F4981 row since watermark → re‑aggregate **those shipments only** → `MERGE` on `shipment_number` |

**Bucketing (confirmed against data):** `SUM(net_amount)` with `vendor_invoice_number <> 'NULL'`, `is_delete=0`:

| Measure | billable_payable | charge_code_01 |
|---|---|---|
| `billable_freight` | B | BFR |
| `billable_fuel` | B | FSC / FSB |
| `payable_freight` | P | PFR |
| `payable_fuel` | P | FSC |

**Derived:** `total_billable`, `total_payable`, `freight_variance = billable_freight − payable_freight`, `total_variance = total_billable − total_payable`. **Ratios (CM %)** computed in DAX as `DIVIDE(SUM(num),SUM(den))` after aggregation — never per row.

**Keys / degenerate:** `shipment_number`, `company`, `carrier_number`/`ship_to` (→`dim_address_book`), `branch_plant` (→`dim_plant`), `route_number`, `mode_of_transport`, `freight_handling_code`, ship/GL dates.
**NRT/audit:** `is_deleted`, `source_commit_timestamp`, `gold_updated_timestamp`, `shift_factor_applied`.

> ⚠️ **ShiftFactor dependency:** per‑company factor scaling `net_amount` to tie to Hubble lives in a company‑constants table **not among the 11 sources**. Default to the JDE fallback `NVL(ShiftFactor, 0.01)` and expose `shift_factor_applied` until the constants table is wired in.

## 6. Reused dimensions (current‑state, lighter refresh)
| Dimension | Key | Role | Refresh |
|---|---|---|---|
| `dim_address_book` | `address_number` | carrier / ship‑to / sold‑to / destination / loading‑port / ocean‑carrier (role views) | periodic (slow‑changing) |
| `dim_plant` | `plant_code` | branch / plant | periodic |
| `dim_item_cost_cascade` | `plant_code + item_short + year` | cost/ton, margin | periodic |

Dims change slowly → refresh on a lighter cadence than the 5‑min facts; the facts MERGE against the current dim rows.

## 7. Consolidated target model
| Object | Type | Action | Grain / Key | Refresh |
|---|---|---|---|---|
| **`fact_sales_order_line`** | FACT | 🆕 Create | order line / `order_line_key` | scheduled ~5‑min MERGE |
| **`fact_freight_audit`** | FACT | 🆕 Create | shipment / `shipment_number` | scheduled ~5‑min MERGE |
| `dim_address_book` | DIM | ♻️ Reuse | `address_number` | periodic |
| `dim_plant` | DIM | ♻️ Reuse | `plant_code` | periodic |
| `dim_item_cost_cascade` | DIM | ♻️ Reuse | `plant_code+item_short+year` | periodic |

**Build 2 facts · reuse 3 dims.** Not used: `supplychain_warehouse_capacity`, `extended_item_availability`, `made_loads_data`, `no_home_inventory`, and the previous `customer_order_line`.

## 8. Source‑to‑target coverage (all 11 sources)
| Source | Lands in | As |
|---|---|---|
| F4211 | `fact_sales_order_line` | FACT spine |
| F41002 | `fact_sales_order_line` | transform lookup → tons |
| F4074 | `fact_sales_order_line` | `factor_value`/adjustment (pre‑agg) |
| F4201 | `fact_sales_order_line` | hold code, delivery instr. (denorm) |
| F4101 | `fact_sales_order_line` | item name/descriptions (denorm) |
| F5642B01 | `fact_sales_order_line` | booking/ocean/dest (denorm) |
| F5642B11 | `fact_sales_order_line` | seal / production notes |
| F4981 | `fact_freight_audit` | billable/payable buckets |
| F4941 | `fact_freight_audit` | `route_number` (or use F4981 FHRTN) |
| F0101 + F0116 | `dim_address_book` | DIMENSION |
| (plant master) | `dim_plant` | DIMENSION |
| (cost cascade) | `dim_item_cost_cascade` | DIMENSION |

## 9. Bus matrix
| Dimension | fact_sales_order_line | fact_freight_audit |
|---|:--:|:--:|
| dim_address_book — carrier | ✓ | ✓ |
| dim_address_book — ship‑to | ✓ | ✓ |
| dim_address_book — sold‑to / dest | ✓ | – |
| dim_plant | ✓ | ~ origin |
| dim_item_cost_cascade | ✓ | ✗ (multi‑item shipment) |
| company | ✓ | ✓ |
| shipment_number (degenerate bridge) | ✓ | ✓ |

## 10. Relationships
- `fact_freight_audit` = one row per shipment; `fact_sales_order_line` = many lines per shipment.
- Both relate to `dim_address_book` (carrier/ship‑to) and `dim_plant`; `shipment_number` is the conformed **degenerate bridge**.
- **Do not** join freight $ onto each order line (re‑creates the shipment→line fan‑out). Keep freight on its own fact; slice by shared dims + `shipment_number`.

## 11. Near‑real‑time platform design (protects 5‑min SLA on F64)
- **Scheduled notebook every 5 min**, **incremental MERGE** only (watermark on CDC commit timestamp) — never full rebuild.
- **`record_hash`** to skip no‑op updates (no write if unchanged).
- **Liquid clustering** on `branch_plant` + `shipment_number` (high‑selectivity filter/join cols) — not snapshot partitioning.
- **Deletion vectors** + scheduled **`OPTIMIZE` / V‑Order** to keep frequent small‑file merges cheap and Direct Lake fast.
- **Soft delete** (`is_deleted`) so CDC removals propagate without breaking Direct Lake; Power BI / role views filter `is_deleted = false`.
- Narrow, integer‑keyed facts → **Direct Lake** (no DirectQuery fallback) → sub‑5‑min refresh at minimum compute.

## 12. Open items
1. **Route #** — default source `f4981.FHRTN` (already in the freight fact, no extra join); `f4941.route_number` alternative (often 0).
2. **ShiftFactor** — locate the company‑constants table for exact Hubble tie‑out; fallback `0.01` until then.

## 13. Diagram (logical)
```
   dim_plant ──┐                              ┌── dim_address_book (carrier / ship-to / sold-to / dest / ports)
               │                              │
      fact_sales_order_line ── shipment_number ── fact_freight_audit
       (NEW, line grain, /5min MERGE)  (bridge)   (NEW, shipment grain, /5min MERGE)
               │
     dim_item_cost_cascade (cost/ton)
```
