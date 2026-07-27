# eso1_billable_payable_freight — Semantic Model (TMDL, Direct Lake)

Declarative TMDL export of the model built by `nb/nb_semantic_model_eso1.py`. Lives as a **sibling of the report** so `ESO1_Billable_v_Payable_Freight.Report/definition.pbir` (`byPath: ../eso1_billable_payable_freight.SemanticModel`) resolves in git / Power BI Desktop / Fabric.

## Contents
```
.platform · definition.pbism
definition/
  model.tmdl            model props + ref table for the 10 tables
  database.tmdl         compatibilityLevel 1604
  expressions.tmdl      Direct Lake source (Sql.Database — FILL PLACEHOLDERS)
  relationships.tmdl    12 relationships (Many fact → One dim, single direction)
  tables/               10 tables (Direct Lake `partition … = entity`; schema otc for the
                        3 new ESO1 tables, rpt for the reused dims/views)
    fact_sales_order_line · fact_freight_audit · dim_shipment · dim_plant
    dim_address_{sold_to,ship_to,carrier,loading_port,ocean_carrier,destination}
```
21 measures (Freight / Volume / Margin / Counts / Quality), `is_deleted=FALSE()` baked into base measures, `status` sorted by `status_sort`, keys/audit columns hidden.

## Fill before binding (Direct Lake source)
In `definition/expressions.tmdl` replace:
- `<SQL_ANALYTICS_ENDPOINT>` — the `lh_jde_gold` SQL analytics endpoint connection string (per workspace/stage).
- `<LH_JDE_GOLD_DATABASE>` — the lakehouse database (name or GUID) behind that endpoint.

Each table's `partition` binds `entityName` (table/view) + `schemaName` via `expressionSource: DatabaseQuery`. **Mixed schema:** `fact_sales_order_line`, `fact_freight_audit`, `dim_shipment` bind to `otc`; `dim_plant` and the six `dim_address_*` views bind to `rpt`. The six `dim_address_*` entities are the **SQL views** created by `nb_dim_address_book`.

## Notes
- This is a **curated** column set (relationship keys + report/measure columns). Add more lakehouse columns by appending `column …` blocks with a Direct Lake `sourceColumn`.
- `compatibilityLevel 1604` + `defaultPowerBIDataSourceVersion powerBI_V3` target current Fabric. Adjust if your tenant differs.
- Deploy with the report via `dpl_jde`; set the Direct Lake source per stage with a deployment rule (see `pipelines/dpl_jde_deployment_checklist.md`).
- Keep this TMDL and `nb_semantic_model_eso1` in sync — the notebook is the runtime builder; this is the git-declarative twin for `byPath` + deployment.
