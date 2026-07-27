# Extended Sales Order 1 — Source Table Metadata

> JDE → Microsoft Fabric (medallion) source dictionary for the **Billable v Payable Freight** report.
> Schema: `PRODDTA`. Captured for the build + naming-governance review. Generated 2026-06-16.

## Conventions used in these tables
- **jde_type**: `A`=Alpha/String, `S`=Numeric (Short/Integer), `P`=Numeric (Packed Decimal), `O`="O (unknown)" (sporadic generator quirk on free-text fields — lands as `STRING`).
- **dec** = `implied_decimals` → Silver divides the raw value by `10^dec`.
- **fabric** = target Fabric type. **null** = nullable (`Y`/`N`).
- **key** = `PK` (primary-key member) or a unique-index name.
- **udc** = `product_code/type` for UDC-coded fields; all resolve via `jde.f0004`/`jde.f0005` (the UDC lookup SQL is identical per field and omitted here).
- **snake_case** = `snake_case_field` as supplied. ⚠️ marks a defect noted during validation.
- Date columns with `dec`/precision 6 are **Julian (CYYDDD)** → convert in Silver.
- Column order is **as supplied** by the source extract (not re-sorted).

## Tables
| # | Table | Cols | Grain / PK | Role in report |
|---|-------|------|------------|----------------|
| 1 | [F4211](#f4211--sales-order-detail) | 268 | `KCOO+DCTO+DOCO+LNID` (line) | Driver — sales order detail line |
| 2 | [F0101](#f0101--address-book-master) | 95 | `AN8` (PK not flagged) | Ship-To / Carrier / Dest name, market code |
| 3 | [F0116](#f0116--address-by-date) | 21 | `AN8+EFTB` (PK not flagged) | Ship-To address block |
| 4 | [F4201](#f4201--sales-order-header) | 133 | `KCOO+DCTO+DOCO` (header) | Hold code, delivery instr., price-eff date |
| 5 | [F4101](#f4101--item-master) | 209 | `IMITM` (item) | Item dimension, weight UoM |
| 6 | [F41002](#f41002--item-uom-conversion-factors) | 16 | `MCU+ITM+UM+RUM` | UoM → short-tons conversion |
| 7 | [F4074](#f4074--price-adjustment-detail-advanced-pricing) | 70 | 11-col (adjustment) | Freight factor value (`ALFVTR`) |
| 8 | [F4981](#f4981--freight-audit-history) | 94 | `FHUK01` (freight rec) | Billable/payable freight $ (shipment grain) |
| 9 | [F5642B11](#f5642b11--custom-transportation-shipment-line) | 54 | `SHPN+DOCO+DCTO+LNID+KCOO` | Custom — seal no (line grain) |
| 10 | [F5642B01](#f5642b01--custom-transportation-booking) | 92 | `SHPN+DOCO+DCTO+KCOO` | Custom — destination port (booking grain) |
| 11 | [F4941](#f4941--shipment-routing-steps) | 91 | `RSSHPN+RSRSSN` (PK not flagged) | Route number (`RSRTN`) — spec mapping; shipment-step grain |

> `F0101_1` in the SQL is a **self-join alias of F0101** (destination-port name), not a separate table.
> `F4941` is the spec's "Route Number" source but was **dropped** from the rewritten SQL; `F4981.FHRTN` is the already-joined alternative.

---

## F4211 — Sales Order Detail
**PK / grain:** `SDKCOO + SDDCTO + SDDOCO + SDLNID` (index `F4211_0`) — one row per order line. **268 columns.**

| col_id | column | alias | friendly_label | jde | dec | fabric | null | key | udc | snake_case |
|---|---|---|---|---|---|---|---|---|---|---|
| 124 | SDFRTH | FRTH | FreightHandlingCode | A | 0 | STRING | Y | | 42/FR | freight_handling_code |
| 120 | SDROUT | ROUT | RouteCode | A | 0 | STRING | Y | | 42/RT | route_code |
| 161 | SDRCD | RCD | ReasonCode | A | 0 | STRING | Y | | 42/RC | reason_code |
| 121 | SDSTOP | STOP | StopCode | A | 0 | STRING | Y | | 42/SP | stop_code |
| 39 | SDTHGD | THGD | ThruGrade | A | 0 | STRING | Y | | 40/LG | thru_grade |
| 38 | SDFRGD | FRGD | FromGrade | A | 0 | STRING | Y | | 40/LG | from_grade |
| 46 | SDNXTR | NXTR | StatusCodeNext | A | 0 | STRING | Y | | 40/AT | status_code_next |
| 47 | SDLTTR | LTTR | StatusCodeLast | A | 0 | STRING | Y | | 40/AT | status_code_last |
| 122 | SDZON | ZON | ZoneNumber | A | 0 | STRING | Y | | 40/ZN | zone_number |
| 114 | SDEUSE | EUSE | EndUse | A | 0 | STRING | Y | | 40/EU | end_use |
| 113 | SDLOB | LOB | LineofBusiness | A | 0 | STRING | Y | | 40/LB | lineof_business ⚠️ (line_of_business) |
| 63 | SDPRP5 | PRP5 | PurchasingReportCode5 | A | 0 | STRING | Y | | 41/P5 | purchasing_report_code_05 |
| 60 | SDPRP2 | PRP2 | PurchasingReportCode2 | A | 0 | STRING | Y | | 41/P2 | purchasing_report_code_02 |
| 58 | SDSRP5 | SRP5 | SalesReportingCode5 | A | 0 | STRING | Y | | 41/S5 | sales_reporting_code_05 |
| 55 | SDSRP2 | SRP2 | SalesReportingCode2 | A | 0 | STRING | Y | | 41/S2 | sales_reporting_code_02 |
| 61 | SDPRP3 | PRP3 | PurchasingReportCode3 | A | 0 | STRING | Y | | 41/P3 | purchasing_report_code_03 |
| 59 | SDPRP1 | PRP1 | PurchasingReportCode1 | A | 0 | STRING | Y | | 41/P1 | purchasing_report_code_01 |
| 54 | SDSRP1 | SRP1 | SalesReportingCode1 | A | 0 | STRING | Y | | 41/S1 | sales_reporting_code_01 |
| 56 | SDSRP3 | SRP3 | SalesReportingCode3 | A | 0 | STRING | Y | | 41/S3 | sales_reporting_code_03 |
| 57 | SDSRP4 | SRP4 | SalesReportingCode4 | A | 0 | STRING | Y | | 41/S4 | sales_reporting_code_04 |
| 125 | SDSHCM | SHCM | ShippingCommodityClass | A | 0 | STRING | Y | | 41/E | shipping_commodity_class |
| 62 | SDPRP4 | PRP4 | PurchasingReportCode4 | A | 0 | STRING | Y | | 41/P4 | purchasing_report_code_04 |
| 126 | SDSHCN | SHCN | ShippingConditionsCode | A | 0 | STRING | Y | | 41/C | shipping_conditions_code |
| 119 | SDMOT | MOT | ModeOfTransport | A | 0 | STRING | Y | | 00/TM | mode_of_transport |
| 252 | SDOSTP | OSTP | OrganizationTypeStructur | A | 0 | STRING | Y | | 01/TS | organization_type_structur |
| 64 | SDUOM | UOM | UnitOfMeasureAsInput | A | 0 | STRING | Y | | 00/UM | uom_as_input |
| 136 | SDVLUM | VLUM | VolumeUnitOfMeasure | A | 0 | STRING | Y | | 00/UM | volume_uom |
| 134 | SDWTUM | WTUM | WeightUnitOfMeasure | A | 0 | STRING | Y | | 00/UM | weight_uom |
| 130 | SDUOM2 | UOM2 | UnitOfMeasureSecondary | A | 0 | STRING | Y | | 00/UM | uom_secondary |
| 132 | SDUOM4 | UOM4 | UnitOfMeasurePricing | A | 0 | STRING | Y | | 00/UM | uom_pricing |
| 163 | SDGWUM | GWUM | UnitOfMeasureGrossWt | A | 0 | STRING | Y | | 00/UM | uom_gross_wt |
| 128 | SDUOM1 | UOM1 | UnitOfMeasurePrimary | A | 0 | STRING | Y | | 00/UM | uom_primary |
| 80 | SDAPUM | APUM | UnitOfMeasureEntUP | A | 0 | STRING | Y | | 00/UM | uom_ent_up |
| 14 | SDRCTO | RCTO | RelatedOrderType | A | 0 | STRING | Y | | 00/DT | related_order_type |
| 237 | SDXCTO | XCTO | CrossDockOrderType | A | 0 | STRING | Y | | 00/DT | cross_dock_order_type |
| 10 | SDOCTO | OCTO | OriginalOrderType | A | 0 | STRING | Y | | 00/DT | original_order_type |
| 100 | SDODCT | ODCT | OriginalDocumentType | A | 0 | STRING | Y | | 00/DT | original_document_type |
| 98 | SDDCT | DCT | DocumentType | A | 0 | STRING | Y | | 00/DT | document_type |
| 106 | SDEXR1 | EXR1 | TaxExplanationCode1 | A | 0 | STRING | Y | | 00/EX | tax_explanation_code_01 |
| 116 | SDNTR | NTR | NatureOfTransaction | A | 0 | STRING | Y | | 00/NT | nature_of_transaction |
| 115 | SDDTYS | DTYS | DutyStatus | A | 0 | STRING | Y | | 40/DS | duty_status |
| 167 | SDUPC1 | UPC1 | PriceCode1 | A | 0 | STRING | Y | | 40/P1 | price_code_01 |
| 169 | SDUPC3 | UPC3 | PriceCode3 | A | 0 | STRING | Y | | 40/P3 | price_code_03 |
| 168 | SDUPC2 | UPC2 | PriceCode2 | A | 0 | STRING | Y | | 40/P2 | price_code_02 |
| 229 | SDHOLD | HOLD | HoldOrdersCode | A | 0 | STRING | Y | | 42/HC | hold_orders_code |
| 166 | SDLCOD | LCOD | CodeLocationTaxStat | A | 0 | STRING | Y | | 46/LT | code_location_tax_stat |
| 74 | SDOTQY | OTQY | OtherQuantity12 | A | 0 | STRING | Y | | 40/OQ | other_quantity_12 |
| 165 | SDSBLT | SBLT | SubledgerType | A | 0 | STRING | Y | | 00/ST | subledger_type |
| 88 | SDRYIN | RYIN | PaymentInstrumentA | A | 0 | STRING | Y | | 00/PY | payment_instrument_a |
| 172 | SDCRMD | CRMD | CorrespondenceMethod | A | 0 | STRING | Y | | 00/SM | correspondence_method |
| 86 | SDINMG | INMG | PrintMessage1 | A | 0 | STRING | Y | | 40/PM | print_message_01 |
| 92 | SDASN | ASN | PriceAdjustmentScheduleN | A | 0 | STRING | Y | | 40/AS | price_adjustment_schedule_n |
| 138 | SDORPR | ORPR | OrderRepriceCategory | A | 0 | STRING | Y | | 40/PI | order_reprice_category |
| 137 | SDRPRC | RPRC | RepriceBasketPriceCat | A | 0 | STRING | Y | | 40/PI | reprice_basket_price_cat |
| 93 | SDPRGR | PRGR | PricingCategory | A | 0 | STRING | Y | | 40/PI | pricing_category |
| 3 | SDDCTO | DCTO | OrderType | A | 0 | STRING | N | PK | 00/DT | order_type |
| 4 | SDLNID | LNID | LineNumber | P | 3 | DOUBLE | N | PK | | line_number |
| 2 | SDDOCO | DOCO | DocumentOrderInvoiceE | S | 0 | BIGINT | N | PK | | document_order_invoice_e |
| 1 | SDKCOO | KCOO | CompanyKeyOrderNo | A | 0 | STRING | N | PK | | company_key_order_no |
| 44 | SDDSC2 | DSC2 | DescriptionLine2 | O | 0 | STRING | Y | | | description_line_02 |
| 184 | SDURRF | URRF | UserReservedReference | A | 0 | STRING | Y | | | user_reserved_reference |
| 246 | SDSPATTN | SPATTN | ShipToAttention | A | 0 | STRING | Y | | | ship_to_attention |
| 240 | SDPOE | POE | PortOfEntryExit | A | 0 | STRING | Y | | | port_of_entry_exit |
| 36 | SDLOCN | LOCN | Location | A | 0 | STRING | Y | | | location |
| 123 | SDCNID | CNID | ContainerID | A | 0 | STRING | Y | | | container_id |
| 141 | SDGLC | GLC | GlClass | A | 0 | STRING | Y | | | gl_class |
| 180 | SDURCD | URCD | UserReservedCode | A | 0 | STRING | Y | | | user_reserved_code |
| 140 | SDCMGP | CMGP | InventoryCostingMeth | A | 0 | STRING | Y | | | inventory_costing_meth |
| 45 | SDLNTY | LNTY | LineType | A | 0 | STRING | Y | | | line_type |
| 37 | SDLOTN | LOTN | Lot | A | 0 | STRING | Y | | | lot |
| 196 | SDIR01 | IR01 | IntegrationReference01 | A | 0 | STRING | Y | | | integration_reference_01 |
| 198 | SDIR03 | IR03 | IntegrationReference03 | A | 0 | STRING | Y | | | integration_reference_03 |
| 197 | SDIR02 | IR02 | IntegrationReference02 | A | 0 | STRING | Y | | | integration_reference_02 |
| 200 | SDIR05 | IR05 | IntegrationReference05 | A | 0 | STRING | Y | | | integration_reference_05 |
| 257 | SDALLSTS | ALLSTS | AllocationStatus | A | 0 | STRING | Y | | | allocation_status |
| 127 | SDSERN | SERN | SerialNumberLot | A | 0 | STRING | Y | | | serial_number_lot |
| 199 | SDIR04 | IR04 | IntegrationReference04 | A | 0 | STRING | Y | | | integration_reference_04 |
| 204 | SDPSIG | PSIG | PullSignal | A | 0 | STRING | Y | | | pull_signal |
| 267 | SDPMPN | PMPN | ProductionNumber | A | 0 | STRING | Y | | | production_number |
| 254 | SDCATNM | CATNM | CatalogName | A | 0 | STRING | Y | | | catalog_name |
| 43 | SDDSC1 | DSC1 | DescriptionLine1 | A | 0 | STRING | Y | | | description_line_01 |
| 31 | SDVR01 | VR01 | Reference1 | A | 0 | STRING | Y | | | reference_01 |
| 202 | SDVR03 | VR03 | ReferenceUCISNo | A | 0 | STRING | Y | | | reference_ucis_no |
| 32 | SDVR02 | VR02 | Reference2Vendor | A | 0 | STRING | Y | | | reference_02_vendor |
| 34 | SDLITM | LITM | Identifier2ndItem | A | 0 | STRING | Y | | | identifier_second_item |
| 35 | SDAITM | AITM | Identifier3rdItem | A | 0 | STRING | Y | | | identifier_third_item |
| 7 | SDCO | CO | Company | A | 0 | STRING | Y | | | company |
| 8 | SDOKCO | OKCO | CompanyKeyOriginal | A | 0 | STRING | Y | | | company_key_original |
| 96 | SDKCO | KCO | CompanyKey | A | 0 | STRING | Y | | | company_key |
| 235 | SDXKCO | XKCO | CrossDockCmpyKeyOrderNo | A | 0 | STRING | Y | | | cross_dock_cmpy_key_order_no |
| 260 | SDCMCO | CMCO | CustomerMasterCompany | A | 0 | STRING | Y | | | customer_master_company |
| 101 | SDOKC | OKC | DocumentCompanyOriginal | A | 0 | STRING | Y | | | document_company_original |
| 12 | SDRKCO | RKCO | CompanyKeyRelated | A | 0 | STRING | Y | | | company_key_related |
| 16 | SDDMCT | DMCT | ContractNumberDistributi | A | 0 | STRING | Y | | | contract_number_distributi |
| 230 | SDHDBU | HDBU | BusinessUnitHeader9 | A | 0 | STRING | Y | | | business_unit_header_09 |
| 243 | SDPMTN | PMTN | PromotionID | A | 0 | STRING | Y | | | promotion_id |
| 48 | SDEMCU | EMCU | CostCenterHeader | A | 0 | STRING | Y | | | cost_center_header |
| 231 | SDDMBU | DMBU | BusinessUnitDemand | A | 0 | STRING | Y | | | business_unit_demand |
| 6 | SDMCU | MCU | CostCenter | A | 0 | STRING | Y | | | cost_center |
| 164 | SDSBL | SBL | Subledger | A | 0 | STRING | Y | | | subledger |
| 9 | SDOORN | OORN | OriginalPoSoNumber | A | 0 | STRING | Y | | | original_po_so_number |
| 160 | SDCMCG | CMCG | CommissionCategory | A | 0 | STRING | Y | | | commission_category |
| 13 | SDRORN | RORN | RelatedPoSoNumber | A | 0 | STRING | Y | | | related_po_so_number |
| 49 | SDRLIT | RLIT | ItemNumberRelatedKit | A | 0 | STRING | Y | | | item_number_related_kit |
| 5 | SDSFXO | SFXO | OrderSuffix | A | 0 | STRING | Y | | | order_suffix |
| 228 | SDMERL | MERL | MeRevisionLevel | A | 0 | STRING | Y | | | me_revision_level |
| 239 | SDXSFX | XSFX | CrossDockOrderSuffix | A | 0 | STRING | Y | | | cross_dock_order_suffix |
| 222 | SDRFRV | RFRV | RevisionReason | A | 0 | STRING | Y | | | revision_reason |
| 94 | SDCLVL | CLVL | PricingCategoryLevel1 | A | 0 | STRING | Y | | | pricing_category_level_01 |
| 87 | SDPTC | PTC | PaymentTermsCode01 | A | 0 | STRING | Y | | | payment_terms_code_01 |
| 232 | SDBCRC | BCRC | CurrencyCodeBase | A | 0 | STRING | Y | | | currency_code_base |
| 173 | SDCRCD | CRCD | CurrencyCodeFrom | A | 0 | STRING | Y | | | currency_code_from |
| 186 | SDUSER | USER | UserId | A | 0 | STRING | Y | | | user_id |
| 187 | SDPID | PID | ProgramId | A | 0 | STRING | Y | | | program_id |
| 188 | SDJOBN | JOBN | WorkStationId | A | 0 | STRING | Y | | | work_station_id |
| 105 | SDTXA1 | TXA1 | TaxArea1 | A | 0 | STRING | Y | | | tax_area_01 |
| 185 | SDTORG | TORG | TransactionOriginator | A | 0 | STRING | Y | | | transaction_originator |
| 217 | SDBSC | BSC | BuyingSegmentCode | A | 0 | STRING | Y | | | buying_segment_code |
| 218 | SDCBSC | CBSC | CurrentBuyingSegmentCode | A | 0 | STRING | Y | | | current_buying_segment_code |
| 205 | SDRLNU | RLNU | ReleaseNumber | A | 0 | STRING | Y | | | release_number |
| 79 | SDTPC | TPC | TemporaryPriceYN | A | 0 | STRING | Y | | | temporary_price_yn |
| 145 | SDSO02 | SO02 | SalesOrderStatus02 | A | 0 | STRING | Y | | | sales_order_status_02 |
| 264 | SDKITDIRTY | KITDIRTY | KitComponentDirtyFlag | A | 0 | STRING | Y | | | kit_component_dirty_flag |
| 151 | SDSO08 | SO08 | SalesOrderStatus08 | A | 0 | STRING | Y | | | sales_order_status_08 |
| 147 | SDSO04 | SO04 | SalesOrderStatus04 | A | 0 | STRING | Y | | | sales_order_status_04 |
| 154 | SDSO11 | SO11 | SalesOrderStatus11 | A | 0 | STRING | Y | | | sales_order_status_11 |
| 170 | SDSWMS | SWMS | StatusInWarehouse | A | 0 | STRING | Y | | | status_in_warehouse |
| 149 | SDSO06 | SO06 | SalesOrderStatus06 | A | 0 | STRING | Y | | | sales_order_status_06 |
| 108 | SDPRIO | PRIO | PriorityProcessing | A | 0 | STRING | Y | | | priority_processing |
| 155 | SDSO12 | SO12 | SalesOrderStatus12 | A | 0 | STRING | Y | | | sales_order_status_12 |
| 104 | SDTAX1 | TAX1 | TaxableYN | A | 0 | STRING | Y | | | taxable_yn |
| 221 | SDPEND | PEND | PendingApprovalFlag | A | 0 | STRING | Y | | | pending_approval_flag |
| 192 | SDSO17 | SO17 | SalesOrderStatus17 | A | 0 | STRING | Y | | | sales_order_status_17 |
| 78 | SDPROV | PROV | PriceOverrideCode | A | 0 | STRING | Y | | | price_override_code |
| 109 | SDRESL | RESL | ResolutionCodeBC | A | 0 | STRING | Y | | | resolution_code_bc |
| 107 | SDATXT | ATXT | AssociatedText | A | 0 | STRING | Y | | | associated_text |
| 194 | SDSO19 | SO19 | SalesOrderStatus19 | A | 0 | STRING | Y | | | sales_order_status_19 |
| 191 | SDSO16 | SO16 | SalesOrderStatus16 | A | 0 | STRING | Y | | | sales_order_status_16 |
| 144 | SDSO01 | SO01 | SalesOrderStatus01 | A | 0 | STRING | Y | | | sales_order_status_01 |
| 259 | SDOSCOREO | OSCOREO | ScoreOverride | A | 0 | STRING | Y | | | score_override |
| 111 | SDSBAL | SBAL | SubstitutesAllowedYN | A | 0 | STRING | Y | | | substitutes_allowed_yn |
| 241 | SDPMTO | PMTO | PaymentTermsOverride | A | 0 | STRING | Y | | | payment_terms_override |
| 195 | SDSO20 | SO20 | SalesOrderStatus20 | A | 0 | STRING | Y | | | sales_order_status_20 |
| 150 | SDSO07 | SO07 | SalesOrderStatus07 | A | 0 | STRING | Y | | | sales_order_status_07 |
| 156 | SDSO13 | SO13 | SalesOrderStatus13 | A | 0 | STRING | Y | | | sales_order_status_13 |
| 265 | SDOCITT | OCITT | OCIn-TransitFlag | A | 0 | STRING | Y | | | oc_in_transit_flag |
| 193 | SDSO18 | SO18 | SalesOrderStatus18 | A | 0 | STRING | Y | | | sales_order_status_18 |
| 152 | SDSO09 | SO09 | SalesOrderStatus09 | A | 0 | STRING | Y | | | sales_order_status_09 |
| 157 | SDSO14 | SO14 | SalesOrderStatus14 | A | 0 | STRING | Y | | | sales_order_status_14 |
| 171 | SDUNCD | UNCD | WoOrderFreezeCode | A | 0 | STRING | Y | | | wo_order_freeze_code |
| 89 | SDDTBS | DTBS | BasedonDate | A | 0 | STRING | Y | | | basedon_date ⚠️ (based_on_date) |
| 110 | SDBACK | BACK | BackordersAllowedYN | A | 0 | STRING | Y | | | backorders_allowed_yn |
| 84 | SDCSTO | CSTO | CostOverrideCode | A | 0 | STRING | Y | | | cost_override_code |
| 73 | SDCOMM | COMM | CommittedHS | A | 0 | STRING | Y | | | committed_hs |
| 146 | SDSO03 | SO03 | SalesOrderStatus03 | A | 0 | STRING | Y | | | sales_order_status_03 |
| 216 | SDDUAL | DUAL | DualUnitOfMeasureItem | A | 0 | STRING | Y | | | dual_uom_item |
| 214 | SDXDCK | XDCK | CrossDockFlag | A | 0 | STRING | Y | | | cross_dock_flag |
| 139 | SDORP | ORP | OrderRepricedIndicator | A | 0 | STRING | Y | | | order_repriced_indicator |
| 158 | SDSO15 | SO15 | SalesOrderStatus15 | A | 0 | STRING | Y | | | sales_order_status_15 |
| 112 | SDAPTS | APTS | PartialShipmntsAllowY | A | 0 | STRING | Y | | | partial_shipmnts_allow_y |
| 148 | SDSO05 | SO05 | SalesOrderStatus05 | A | 0 | STRING | Y | | | sales_order_status_05 |
| 153 | SDSO10 | SO10 | SalesOrderStatus10 | A | 0 | STRING | Y | | | sales_order_status_10 |
| 159 | SDACOM | ACOM | ApplyCommissionYN | A | 0 | STRING | Y | | | apply_commission_yn |
| 255 | SDALLOC | ALLOC | AllocationFlag | A | 0 | STRING | Y | | | allocation_flag |
| 51 | SDCPNT | CPNT | ComponentNumber | S | 1 | DOUBLE | Y | | | component_number |
| 50 | SDKTLN | KTLN | LineNumberKitMaster | S | 3 | DOUBLE | Y | | | line_number_kit_master |
| 75 | SDUPRC | UPRC | AmtPricePerUnit2 | P | 6 | DOUBLE | Y | | | amt_price_per_unit_02 |
| 175 | SDFPRC | FPRC | AmountListPriceForeign | P | 4 | DOUBLE | Y | | | amount_list_price_foreign |
| 135 | SDITVL | ITVL | AmountUnitVolume | P | 4 | DOUBLE | Y | | | amount_unit_volume |
| 85 | SDTCST | TCST | ExtendedCostTransfer | P | 4 | DOUBLE | Y | | | extended_cost_transfer |
| 162 | SDGRWT | GRWT | GrossWeight | P | 4 | DOUBLE | Y | | | gross_weight |
| 81 | SDLPRC | LPRC | AmtListPricePerUnit | P | 4 | DOUBLE | Y | | | amt_list_price_per_unit |
| 82 | SDUNCS | UNCS | AmountUnitCost | P | 4 | DOUBLE | Y | | | amount_unit_cost |
| 178 | SDFUC | FUC | AmountForeignUnitCost | P | 4 | DOUBLE | Y | | | amount_foreign_unit_cost |
| 133 | SDITWT | ITWT | AmountUnitWeight | P | 4 | DOUBLE | Y | | | amount_unit_weight |
| 91 | SDFUN2 | FUN2 | TradeDiscountOld | P | 4 | DOUBLE | Y | | | trade_discount_old |
| 176 | SDFUP | FUP | AmtForPricePerUnit | P | 4 | DOUBLE | Y | | | amt_for_price_per_unit |
| 65 | SDUORG | UORG | UnitsTransactionQty | P | 3 | DOUBLE | Y | | | units_transaction_qty |
| 71 | SDQTYT | QTYT | QuantityShippedToDate | P | 3 | DOUBLE | Y | | | quantity_shipped_to_date |
| 238 | SDXLLN | XLLN | CrossDockLineNumber | P | 3 | DOUBLE | Y | | | cross_dock_line_number |
| 129 | SDPQOR | PQOR | UnitsPrimaryQtyOrder | P | 3 | DOUBLE | Y | | | units_primary_qty_order |
| 40 | SDFRMP | FRMP | FromPotency | P | 3 | DOUBLE | Y | | | from_potency |
| 41 | SDTHRP | THRP | ThruPotency | P | 3 | DOUBLE | Y | | | thru_potency |
| 131 | SDSQOR | SQOR | UnitsSecondaryQtyOr | P | 3 | DOUBLE | Y | | | units_secondary_qty_or |
| 70 | SDUOPN | UOPN | UnitsOpenQuantity | P | 3 | DOUBLE | Y | | | units_open_quantity |
| 15 | SDRLLN | RLLN | RelatedPoSoLineNo | P | 3 | DOUBLE | Y | | | related_po_so_line_no |
| 53 | SDKTP | KTP | NumbOfCpntPerParent | P | 3 | DOUBLE | Y | | | numb_of_cpnt_per_parent |
| 95 | SDCADC | CADC | DiscountCash | P | 3 | DOUBLE | Y | | | discount_cash |
| 233 | SDODLN | ODLN | OriginalDocumentLineNo | P | 3 | DOUBLE | Y | | | original_document_line_no |
| 90 | SDTRDC | TRDC | DiscountTrade | P | 3 | DOUBLE | Y | | | discount_trade |
| 66 | SDSOQS | SOQS | UnitsQuantityShipped | P | 3 | DOUBLE | Y | | | units_quantity_shipped |
| 223 | SDMCLN | MCLN | MatrixControlLine | P | 3 | DOUBLE | Y | | | matrix_control_line |
| 11 | SDOGNO | OGNO | OriginalLineNumber | P | 3 | DOUBLE | Y | | | original_line_number |
| 69 | SDSONE | SONE | UnitsQuantityFuture | P | 3 | DOUBLE | Y | | | units_quantity_future |
| 67 | SDSOBK | SOBK | UnitsQuanBackorHeld | P | 3 | DOUBLE | Y | | | units_quan_backor_held |
| 68 | SDSOCN | SOCN | UnitsQuantityCanceled | P | 3 | DOUBLE | Y | | | units_quantity_canceled |
| 258 | SDOSCORE | OSCORE | OrderScore | P | 3 | DOUBLE | Y | | | order_score |
| 72 | SDQRLV | QRLV | QuantityRelieved | P | 3 | DOUBLE | Y | | | quantity_relieved |
| 182 | SDURAT | URAT | UserReservedAmount | P | 2 | DOUBLE | Y | | | user_reserved_amount |
| 262 | SDKITAMTDOM | KITAMTDOM | AccumulatedAmountInvoiced(Domestic) | P | 2 | DOUBLE | Y | | | accumulated_amount_invoiced_domestic |
| 263 | SDKITAMTFOR | KITAMTFOR | AccumulatedAmountInvoiced(Foreign) | P | 2 | DOUBLE | Y | | | accumulated_amount_invoiced_foreign |
| 76 | SDAEXP | AEXP | AmountExtendedPrice | P | 2 | DOUBLE | Y | | | amount_extended_price |
| 179 | SDFEC | FEC | AmountForeignExtCost | P | 2 | DOUBLE | Y | | | amount_foreign_ext_cost |
| 77 | SDAOPN | AOPN | AmountOpen1 | P | 2 | DOUBLE | Y | | | amount_open_01 |
| 83 | SDECST | ECST | AmountExtendedCost | P | 2 | DOUBLE | Y | | | amount_extended_cost |
| 177 | SDFEA | FEA | AmountForeignExtPrice | P | 2 | DOUBLE | Y | | | amount_foreign_ext_price |
| 22 | SDTRDJ | TRDJ | DateTransactionJulian | S | 0 | INT | Y | | | date_transaction_julian |
| 21 | SDDRQJ | DRQJ | DateRequestedJulian | S | 0 | INT | Y | | | date_requested_julian |
| 24 | SDADDJ | ADDJ | ActualShipDate | S | 0 | INT | Y | | | actual_ship_date |
| 30 | SDPPDJ | PPDJ | DatePromisedShipJu | S | 0 | INT | Y | | | date_promised_ship_julian |
| 208 | SDRLDJ | RLDJ | DateRelease | S | 0 | INT | Y | | | date_release |
| 27 | SDDGL | DGL | DtForGLAndVouch1 | S | 0 | INT | Y | | | dt_for_gl_and_vouch_01 |
| 28 | SDRSDJ | RSDJ | DateReleaseJulian | S | 0 | INT | Y | | | date_release_julian |
| 23 | SDPDDJ | PDDJ | ScheduledPickDate | S | 0 | INT | Y | | | scheduled_pick_date |
| 234 | SDOPDJ | OPDJ | DateOriginalPromisde | S | 0 | INT | Y | | | date_original_promisde |
| 29 | SDPEFJ | PEFJ | DatePriceEffectiveDate | S | 0 | INT | Y | | | date_price_effective_date |
| 26 | SDCNDJ | CNDJ | CancelDate | S | 0 | INT | Y | | | cancel_date |
| 25 | SDIVD | IVD | DateInvoiceJulian | S | 0 | INT | Y | | | date_invoice_julian |
| 181 | SDURDT | URDT | UserReservedDate | S | 0 | INT | Y | | | user_reserved_date |
| 189 | SDUPMJ | UPMJ | DateUpdated | S | 0 | INT | Y | | | date_updated |
| 190 | SDTDAY | TDAY | TimeOfDay | P | 0 | BIGINT | Y | | | time_of_day |
| 174 | SDCRR | CRR | CurrencyConverRateOv | P | 0 | BIGINT | Y | | | currency_conver_rate_ov |
| 42 | SDEXDP | EXDP | DaysPastExpiration | P | 0 | BIGINT | Y | | | days_past_expiration |
| 207 | SDRLTM | RLTM | TimeRelease | P | 0 | BIGINT | Y | | | time_release |
| 211 | SDOPTT | OPTT | TimeOriginalPromised | P | 0 | BIGINT | Y | | | time_original_promised |
| 266 | SDOCCARDNO | OCCARDNO | OCKanbanCardNo | P | 0 | BIGINT | Y | | | oc_kanban_card_no |
| 248 | SDPRCIDLN | PRCIDLN | PartnerContactLineNumID | P | 0 | BIGINT | Y | | | partner_contact_line_num_id |
| 203 | SDDEID | DEID | DemandUniqueKey | P | 0 | BIGINT | Y | | | demand_unique_key |
| 213 | SDPSTM | PSTM | TimeFuture2 | P | 0 | BIGINT | Y | | | time_future_02 |
| 253 | SDUKID | UKID | UniqueKeyIDInternal | P | 0 | BIGINT | Y | | | unique_key_id_internal |
| 215 | SDXPTY | XPTY | CrossDockingPriority | P | 0 | BIGINT | Y | | | cross_docking_priority |
| 261 | SDKITID | KITID | KitIdentifier | P | 0 | BIGINT | Y | | | kit_identifier |
| 249 | SDCCIDLN | CCIDLN | CustomerContactLineNumberID | P | 0 | BIGINT | Y | | | customer_contact_line_number_id |
| 212 | SDPDTT | PDTT | PromisedPickTime | P | 0 | BIGINT | Y | | | promised_pick_time |
| 247 | SDPRAN8 | PRAN8 | PartnerAddressNumber | P | 0 | BIGINT | Y | | | partner_address_number |
| 250 | SDSHCCIDLN | SHCCIDLN | ShipToCusContactLineNumID | P | 0 | BIGINT | Y | | | ship_to_cus_contact_line_num_id |
| 226 | SDPRJM | PRJM | ProjectNumber | P | 0 | BIGINT | Y | | | project_number |
| 251 | SDOPPID | OPPID | OpportunityId | P | 0 | BIGINT | Y | | | opportunity_id |
| 220 | SDDVAN | DVAN | AddressNumberDeliveredTo | P | 0 | BIGINT | Y | | | address_number_delivered_to |
| 52 | SDRKIT | RKIT | RelatedKitComponent | P | 0 | BIGINT | Y | | | related_kit_component |
| 201 | SDSOOR | SOOR | SourceOfOrder | S | 0 | BIGINT | Y | | | source_of_order |
| 256 | SDFULPID | FULPID | FulfillmentPlanID | S | 0 | BIGINT | Y | | | fulfillment_plan_id |
| 209 | SDDRQT | DRQT | RequestedDeliveryTime | S | 0 | BIGINT | Y | | | requested_delivery_time |
| 245 | SDAAID | AAID | ParentNumber | S | 0 | BIGINT | Y | | | parent_number |
| 17 | SDDMCS | DMCS | ContractSupplementDistri | S | 0 | BIGINT | Y | | | contract_supplement_distri |
| 210 | SDADTM | ADTM | ActualShipmentTime | S | 0 | BIGINT | Y | | | actual_shipment_time |
| 236 | SDXORN | XORN | CrossDockOrderNumber | S | 0 | BIGINT | Y | | | cross_dock_order_number |
| 227 | SDOSEQ | OSEQ | SequenceNumber | S | 0 | BIGINT | Y | | | sequence_number |
| 102 | SDPSN | PSN | PickSlipNumber | S | 0 | BIGINT | Y | | | pick_slip_number |
| 19 | SDSHAN | SHAN | AddressNumberShipTo | S | 0 | BIGINT | Y | | | address_number_ship_to |
| 99 | SDODOC | ODOC | OriginalDocumentNo | S | 0 | BIGINT | Y | | | original_document_no |
| 219 | SDCORD | CORD | NumberChangeOrder | S | 0 | BIGINT | Y | | | number_change_order |
| 142 | SDCTRY | CTRY | Century | S | 0 | BIGINT | Y | | | century |
| 143 | SDFY | FY | FiscalYear1 | S | 0 | BIGINT | Y | | | fiscal_year_01 |
| 224 | SDSHPN | SHPN | ShipmentNumber | S | 0 | BIGINT | Y | | | shipment_number |
| 103 | SDDELN | DELN | DeliveryNumber | S | 0 | BIGINT | Y | | | delivery_number |
| 97 | SDDOC | DOC | DocVoucherInvoiceE | S | 0 | BIGINT | Y | | | doc_voucher_invoice_e |
| 20 | SDPA8 | PA8 | AddressNumberParent | S | 0 | BIGINT | Y | | | address_number_parent |
| 118 | SDCARS | CARS | Carrier | S | 0 | BIGINT | Y | | | carrier |
| 242 | SDANBY | ANBY | BuyerNumber | S | 0 | BIGINT | Y | | | buyer_number |
| 244 | SDNUMB | NUMB | AssetItemNumber | S | 0 | BIGINT | Y | | | asset_item_number |
| 117 | SDVEND | VEND | PrimaryLastVendorNo | S | 0 | BIGINT | Y | | | primary_last_vendor_no |
| 268 | SDPNS | PNS | ProductionNumberShort | S | 0 | BIGINT | Y | | | production_number_short |
| 225 | SDRSDT | RSDT | PromisedDeliveryTime | S | 0 | BIGINT | Y | | | promised_delivery_time |
| 206 | SDPMDT | PMDT | ScheduledShipmentTime | S | 0 | BIGINT | Y | | | scheduled_shipment_time |
| 33 | SDITM | ITM | IdentifierShortItem | S | 0 | BIGINT | Y | | | identifier_short_item |
| 18 | SDAN8 | AN8 | AddressNumber | S | 0 | BIGINT | Y | | | address_number |
| 183 | SDURAB | URAB | UserReservedNumber | S | 0 | BIGINT | Y | | | user_reserved_number |

## F0101 — Address Book Master
**PK / grain:** natural key `ABAN8` ⚠️ (**not flagged** in metadata — `key_type`/index NULL). Role-playing dim (Ship-To / Carrier / Destination). **95 columns.**

| col_id | column | alias | friendly_label | jde | dec | fabric | null | key | udc | snake_case |
|---|---|---|---|---|---|---|---|---|---|---|
| 85 | ABCLASS04 | CLASS04 | ClassificationCode04 | A | 0 | STRING | Y | | 00/CL | classification_code_04 |
| 86 | ABCLASS05 | CLASS05 | ClassificationCode05 | A | 0 | STRING | Y | | 00/CL | classification_code_05 |
| 82 | ABCLASS01 | CLASS01 | ClassificationCode01 | A | 0 | STRING | Y | | 00/CL | classification_code_01 |
| 84 | ABCLASS03 | CLASS03 | ClassificationCode03 | A | 0 | STRING | Y | | 00/CL | classification_code_03 |
| 83 | ABCLASS02 | CLASS02 | ClassificationCode02 | A | 0 | STRING | Y | | 00/CL | classification_code_02 |
| 30 | ABAC02 | AC02 | ReportCodeAddBook002 | A | 0 | STRING | Y | | 01/02 | report_code_add_book_002 |
| 43 | ABAC15 | AC15 | ReportCodeAddBook015 | A | 0 | STRING | Y | | 01/15 | report_code_add_book_015 |
| 32 | ABAC04 | AC04 | ReportCodeAddBook004 | A | 0 | STRING | Y | | 01/04 | report_code_add_book_004 |
| 35 | ABAC07 | AC07 | ReportCodeAddBook007 | A | 0 | STRING | Y | | 01/07 | report_code_add_book_007 |
| 38 | ABAC10 | AC10 | ReportCodeAddBook010 | A | 0 | STRING | Y | | 01/10 | report_code_add_book_010 |
| 44 | ABAC16 | AC16 | ReportCodeAddBook016 | A | 0 | STRING | Y | | 01/16 | report_code_add_book_016 |
| 45 | ABAC17 | AC17 | ReportCodeAddBook017 | A | 0 | STRING | Y | | 01/17 | report_code_add_book_017 |
| 37 | ABAC09 | AC09 | ReportCodeAddBook009 | A | 0 | STRING | Y | | 01/09 | report_code_add_book_009 |
| 36 | ABAC08 | AC08 | ReportCodeAddBook008 | A | 0 | STRING | Y | | 01/08 | report_code_add_book_008 |
| 57 | ABAC29 | AC29 | CategoryCodeAddressBk29 | A | 0 | STRING | Y | | 01/29 | category_code_address_bk_29 |
| 33 | ABAC05 | AC05 | ReportCodeAddBook005 | A | 0 | STRING | Y | | 01/05 | report_code_add_book_005 |
| 29 | ABAC01 | AC01 | ReportCodeAddBook001 | A | 0 | STRING | Y | | 01/01 | report_code_add_book_001 |
| 55 | ABAC27 | AC27 | CategoryCodeAddressBk27 | A | 0 | STRING | Y | | 01/27 | category_code_address_bk_27 |
| 42 | ABAC14 | AC14 | ReportCodeAddBook014 | A | 0 | STRING | Y | | 01/14 | report_code_add_book_014 |
| 41 | ABAC13 | AC13 | ReportCodeAddBook013 | A | 0 | STRING | Y | | 01/13 | report_code_add_book_013 |
| 31 | ABAC03 | AC03 | ReportCodeAddBook003 | A | 0 | STRING | Y | | 01/03 | report_code_add_book_003 |
| 54 | ABAC26 | AC26 | CategoryCodeAddressBk26 | A | 0 | STRING | Y | | 01/26 | category_code_address_bk_26 |
| 48 | ABAC20 | AC20 | ReportCodeAddBook020 | A | 0 | STRING | Y | | 01/20 | report_code_add_book_020 |
| 56 | ABAC28 | AC28 | CategoryCodeAddressBk28 | A | 0 | STRING | Y | | 01/28 | category_code_address_bk_28 |
| 39 | ABAC11 | AC11 | ReportCodeAddBook011 | A | 0 | STRING | Y | | 01/11 | report_code_add_book_011 |
| 49 | ABAC21 | AC21 | CategoryCodeAddressBook2 | A | 0 | STRING | Y | | 01/21 | category_code_address_bk_02 ⚠️ (should be _21; truncation+pad) |
| 52 | ABAC24 | AC24 | CategoryCodeAddressBk24 | A | 0 | STRING | Y | | 01/24 | category_code_address_bk_24 |
| 40 | ABAC12 | AC12 | ReportCodeAddBook012 | A | 0 | STRING | Y | | 01/12 | report_code_add_book_012 |
| 50 | ABAC22 | AC22 | CategoryCodeAddressBk22 | A | 0 | STRING | Y | | 01/22 | category_code_address_bk_22 |
| 46 | ABAC18 | AC18 | ReportCodeAddBook018 | A | 0 | STRING | Y | | 01/18 | report_code_add_book_018 |
| 9 | ABAT1 | AT1 | AddressType1 | A | 0 | STRING | Y | | 01/ST | address_type_01 |
| 34 | ABAC06 | AC06 | ReportCodeAddBook006 | A | 0 | STRING | Y | | 01/06 | report_code_add_book_006 |
| 47 | ABAC19 | AC19 | ReportCodeAddBook019 | A | 0 | STRING | Y | | 01/19 | report_code_add_book_019 |
| 58 | ABAC30 | AC30 | CategoryCodeAddressBk30 | A | 0 | STRING | Y | | 01/30 | category_code_address_bk_30 |
| 53 | ABAC25 | AC25 | CategoryCodeAddressBk25 | A | 0 | STRING | Y | | 01/25 | category_code_address_bk_25 |
| 51 | ABAC23 | AC23 | CategoryCodeAddressBk23 | A | 0 | STRING | Y | | 01/23 | category_code_address_bk_23 |
| 10 | ABCM | CM | CreditMessage | A | 0 | STRING | Y | | 00/CM | credit_message |
| 8 | ABLNGP | LNGP | LanguagePreference | A | 0 | STRING | Y | | 01/LP | language_preference |
| 21 | ABSBLI | SBLI | SubledgerInactiveCode | A | 0 | STRING | Y | | 00/SI | subledger_inactive_code |
| 7 | ABSIC | SIC | StandardIndustryCode | A | 0 | STRING | Y | | 01/SC | standard_industry_code |
| 63 | ABRMK | RMK | NameRemark | O | 0 | STRING | Y | | | name_remark |
| 4 | ABALPH | ALPH | NameAlpha | O | 0 | STRING | Y | | | name_alpha |
| 64 | ABTXCT | TXCT | CertificateTaxExempt | O | 0 | STRING | Y | | | certificate_tax_exempt |
| 71 | ABURRF | URRF | UserReservedReference | A | 0 | STRING | Y | | | user_reserved_reference |
| 81 | ABDUNS | DUNS | DUNSNumber | A | 0 | STRING | Y | | | duns_number |
| 89 | ABYEARSTAR | YEARSTAR | YearStarted | A | 0 | STRING | Y | | | year_started |
| 66 | ABALP1 | ALP1 | Kanjialpha | A | 0 | STRING | Y | | | kanjialpha ⚠️ (kanji_alpha) |
| 5 | ABDC | DC | DescripCompressed | A | 0 | STRING | Y | | | descrip_compressed |
| 65 | ABTX2 | TX2 | TaxId2 | A | 0 | STRING | Y | | | tax_id_02 |
| 2 | ABALKY | ALKY | AlternateAddressKey | A | 0 | STRING | Y | | | alternate_address_key |
| 3 | ABTAX | TAX | TaxId | A | 0 | STRING | Y | | | tax_id |
| 67 | ABURCD | URCD | UserReservedCode | A | 0 | STRING | Y | | | user_reserved_code |
| 78 | ABSCCLTP | SCCLTP | ShortcutClientType | A | 0 | STRING | Y | | | shortcut_client_type |
| 92 | ABREVRNG | REVRNG | RevenueRange | A | 0 | STRING | Y | | | revenue_range |
| 90 | ABAEMPGP | AEMPGP | EmployeeGroupApprovals | A | 0 | STRING | Y | | | employee_group_approvals |
| 6 | ABMCU | MCU | CostCenter | A | 0 | STRING | Y | | | cost_center |
| 59 | ABGLBA | GLBA | GlBankAccount | A | 0 | STRING | Y | | | gl_bank_account |
| 72 | ABUSER | USER | UserId | A | 0 | STRING | Y | | | user_id |
| 73 | ABPID | PID | ProgramId | A | 0 | STRING | Y | | | program_id |
| 75 | ABJOBN | JOBN | WorkStationId | A | 0 | STRING | Y | | | work_station_id |
| 80 | ABEXCHG | EXCHG | StockExchange | A | 0 | STRING | Y | | | stock_exchange |
| 79 | ABTICKER | TICKER | Ticker | A | 0 | STRING | Y | | | ticker |
| 77 | ABPRGF | PRGF | PurgeFlag | A | 0 | STRING | Y | | | purge_flag |
| 20 | ABATE | ATE | AddressTypeEmployee | A | 0 | STRING | Y | | | address_type_employee |
| 16 | ABATP | ATP | AddressTypePayables | A | 0 | STRING | Y | | | address_type_payables |
| 18 | ABATPR | ATPR | AddTypeCode4Purch | A | 0 | STRING | Y | | | add_type_code_04_purch ⚠️ ("4"=for) |
| 15 | ABAT5 | AT5 | AddressType5 | A | 0 | STRING | Y | | | address_type_05 |
| 14 | ABAT4 | AT4 | AddressType4 | A | 0 | STRING | Y | | | address_type_04 |
| 62 | ABMSGA | MSGA | ActionMessageControl | A | 0 | STRING | Y | | | action_message_control |
| 13 | ABAT3 | AT3 | AddressType3 | A | 0 | STRING | Y | | | address_type_03 |
| 19 | ABAB3 | AB3 | MiscCode3 | A | 0 | STRING | Y | | | misc_code_03 |
| 17 | ABATR | ATR | AddressTypeReceivables | A | 0 | STRING | Y | | | address_type_receivables |
| 12 | ABAT2 | AT2 | AddressType2 | A | 0 | STRING | Y | | | address_type_02 |
| 11 | ABTAXC | TAXC | PersonCorporationCode | A | 0 | STRING | Y | | | person_corporation_code |
| 91 | ABACTIN | ACTIN | IndicatorFlg | A | 0 | STRING | Y | | | indicator_flg |
| 69 | ABURAT | URAT | UserReservedAmount | P | 2 | DOUBLE | Y | | | user_reserved_amount |
| 61 | ABPDI | PDI | DateScheduledIn | S | 0 | INT | Y | | | date_scheduled_in |
| 22 | ABEFTB | EFTB | DateBeginningEffective | S | 0 | INT | Y | | | date_beginning_effective |
| 68 | ABURDT | URDT | UserReservedDate | S | 0 | INT | Y | | | user_reserved_date |
| 74 | ABUPMJ | UPMJ | DateUpdated | S | 0 | INT | Y | | | date_updated |
| 87 | ABNOE | NOE | NumberOfEmployee | P | 0 | BIGINT | Y | | | number_of_employee |
| 88 | ABGROWTHR | GROWTHR | GrowthRate | P | 0 | BIGINT | Y | | | growth_rate |
| 1 | ABAN8 | AN8 | AddressNumber | S | 0 | BIGINT | N | (PK)⚠️ | | address_number |
| 60 | ABPTI | PTI | TimeScheduledIn | S | 0 | BIGINT | Y | | | time_scheduled_in |
| 23 | ABAN81 | AN81 | AddressNumber1st | S | 0 | BIGINT | Y | | | address_number_first |
| 27 | ABAN86 | AN86 | AddressNumber6th | S | 0 | BIGINT | Y | | | address_number_sixth |
| 24 | ABAN82 | AN82 | AddressNumber2nd | S | 0 | BIGINT | Y | | | address_number_second |
| 25 | ABAN83 | AN83 | AddressNumber3rd | S | 0 | BIGINT | Y | | | address_number_third |
| 94 | ABPERRS | PERRS | PreviousErrorStatus | S | 0 | BIGINT | Y | | | previous_error_status |
| 28 | ABAN85 | AN85 | AddressNumber5th | S | 0 | BIGINT | Y | | | address_number_fifth |
| 26 | ABAN84 | AN84 | AddressNumber4th | S | 0 | BIGINT | Y | | | address_number_forth ⚠️ (fourth) |
| 95 | ABCAAD | CAAD | ServerStatus | S | 0 | BIGINT | Y | | | server_status |
| 93 | ABSYNCS | SYNCS | SynchronizationStatus | S | 0 | BIGINT | Y | | | synchronization_status |
| 70 | ABURAB | URAB | UserReservedNumber | S | 0 | BIGINT | Y | | | user_reserved_number |
| 76 | ABUPMT | UPMT | TimeLastUpdated | S | 0 | BIGINT | Y | | | time_last_updated |

## F0116 — Address by Date
**PK / grain:** effective-dated key `ALAN8 + ALEFTB` ⚠️ (**not flagged**; both `nullable=N`). **21 columns.**

| col_id | column | alias | friendly_label | jde | dec | fabric | null | key | udc | snake_case |
|---|---|---|---|---|---|---|---|---|---|---|
| 11 | ALADDS | ADDS | State | A | 0 | STRING | Y | | 00/S | state |
| 14 | ALCTR | CTR | Country | A | 0 | STRING | Y | | 00/CN | country |
| 10 | ALCOUN | COUN | CountyAddress | A | 0 | STRING | Y | | 00/CT | county_address |
| 9 | ALCTY1 | CTY1 | City | O | 0 | STRING | Y | | | city |
| 5 | ALADD2 | ADD2 | AddressLine2 | A | 0 | STRING | Y | | | address_line_02 |
| 4 | ALADD1 | ADD1 | AddressLine1 | A | 0 | STRING | Y | | | address_line_01 |
| 6 | ALADD3 | ADD3 | AddressLine3 | A | 0 | STRING | Y | | | address_line_03 |
| 7 | ALADD4 | ADD4 | AddressLine4 | A | 0 | STRING | Y | | | address_line_04 |
| 12 | ALCRTE | CRTE | CarrierRoute | A | 0 | STRING | Y | | | carrier_route |
| 13 | ALBKML | BKML | BulkMailingCenter | A | 0 | STRING | Y | | | bulk_mailing_center |
| 8 | ALADDZ | ADDZ | ZipCodePostal | A | 0 | STRING | Y | | | zip_code_postal |
| 15 | ALUSER | USER | UserId | A | 0 | STRING | Y | | | user_id |
| 16 | ALPID | PID | ProgramId | A | 0 | STRING | Y | | | program_id |
| 18 | ALJOBN | JOBN | WorkStationId | A | 0 | STRING | Y | | | work_station_id |
| 3 | ALEFTF | EFTF | EffectiveDateExistence10 | A | 0 | STRING | Y | | | effective_date_existence_10 |
| 2 | ALEFTB | EFTB | DateBeginningEffective | S | 0 | INT | N | (PK)⚠️ | | date_beginning_effective |
| 17 | ALUPMJ | UPMJ | DateUpdated | S | 0 | INT | Y | | | date_updated |
| 1 | ALAN8 | AN8 | AddressNumber | S | 0 | BIGINT | N | (PK)⚠️ | | address_number |
| 21 | ALCAAD | CAAD | ServerStatus | S | 0 | BIGINT | Y | | | server_status |
| 20 | ALSYNCS | SYNCS | SynchronizationStatus | S | 0 | BIGINT | Y | | | synchronization_status |
| 19 | ALUPMT | UPMT | TimeLastUpdated | S | 0 | BIGINT | Y | | | time_last_updated |

## F4201 — Sales Order Header
**PK / grain:** `SHKCOO + SHDCTO + SHDOCO` (index `F4201_0`) — header (many lines per header). **133 columns.**

| col_id | column | alias | friendly_label | jde | dec | fabric | null | key | udc | snake_case |
|---|---|---|---|---|---|---|---|---|---|---|
| 54 | SHFRTH | FRTH | FreightHandlingCode | A | 0 | STRING | Y | | 42/FR | freight_handling_code |
| 50 | SHROUT | ROUT | RouteCode | A | 0 | STRING | Y | | 42/RT | route_code |
| 51 | SHSTOP | STOP | StopCode | A | 0 | STRING | Y | | 42/SP | stop_code |
| 59 | SHRCD | RCD | ReasonCode | A | 0 | STRING | Y | | 42/RC | reason_code |
| 52 | SHZON | ZON | ZoneNumber | A | 0 | STRING | Y | | 40/ZN | zone_number |
| 48 | SHMOT | MOT | ModeOfTransport | A | 0 | STRING | Y | | 00/TM | mode_of_transport |
| 49 | SHCOT | COT | ConditionsOfTransport | A | 0 | STRING | Y | | 00/TC | conditions_of_transport |
| 63 | SHWUMD | WUMD | UnitOfMeasureWhtDisp | A | 0 | STRING | Y | | 00/UM | uom_wht_disp |
| 64 | SHVUMD | VUMD | UnitOfMeasureVolDisp | A | 0 | STRING | Y | | 00/UM | uom_vol_disp |
| 12 | SHRCTO | RCTO | RelatedOrderType | A | 0 | STRING | Y | | 00/DT | related_order_type |
| 109 | SHDCT4 | DCT4 | DoctType | A | 0 | STRING | Y | | 00/DT | doct_type |
| 9 | SHOCTO | OCTO | OriginalOrderType | A | 0 | STRING | Y | | 00/DT | original_order_type |
| 36 | SHEXR1 | EXR1 | TaxExplanationCode1 | A | 0 | STRING | Y | | 00/EX | tax_explanation_code_01 |
| 45 | SHNTR | NTR | NatureOfTransaction | A | 0 | STRING | Y | | 00/NT | nature_of_transaction |
| 73 | SHLNGP | LNGP | LanguagePreference | A | 0 | STRING | Y | | 01/LP | language_preference |
| 42 | SHHOLD | HOLD | HoldOrdersCode | A | 0 | STRING | Y | | 42/HC | hold_orders_code |
| 30 | SHRYIN | RYIN | PaymentInstrumentA | A | 0 | STRING | Y | | 00/PY | payment_instrument_a |
| 68 | SHSBLI | SBLI | SubledgerInactiveCode | A | 0 | STRING | Y | | 00/SI | subledger_inactive_code |
| 69 | SHCRMD | CRMD | CorrespondenceMethod | A | 0 | STRING | Y | | 00/SM | correspondence_method |
| 28 | SHINMG | INMG | PrintMessage1 | A | 0 | STRING | Y | | 40/PM | print_message_01 |
| 31 | SHASN | ASN | PriceAdjustmentScheduleN | A | 0 | STRING | Y | | 40/AS | price_adjustment_schedule_n |
| 32 | SHPRGP | PRGP | PricingGroup | A | 0 | STRING | Y | | 40/PC | pricing_group |
| 3 | SHDCTO | DCTO | OrderType | A | 0 | STRING | N | PK | 00/DT | order_type |
| 2 | SHDOCO | DOCO | DocumentOrderInvoiceE | S | 0 | BIGINT | N | PK | | document_order_invoice_e |
| 1 | SHKCOO | KCOO | CompanyKeyOrderNo | A | 0 | STRING | N | PK | | company_key_order_no |
| 26 | SHDEL1 | DEL1 | DeliveryInstructLine1 | O | 0 | STRING | Y | | | delivery_instruct_line_01 |
| 37 | SHTXCT | TXCT | CertificateTaxExempt | O | 0 | STRING | Y | | | certificate_tax_exempt |
| 82 | SHURRF | URRF | UserReservedReference | A | 0 | STRING | Y | | | user_reserved_reference |
| 129 | SHSPATTN | SPATTN | ShipToAttention | A | 0 | STRING | Y | | | ship_to_attention |
| 128 | SHSDATTN | SDATTN | SoldToAttention | A | 0 | STRING | Y | | | sold_to_attention |
| 53 | SHCNID | CNID | ContainerID | A | 0 | STRING | Y | | | container_id |
| 78 | SHURCD | URCD | UserReservedCode | A | 0 | STRING | Y | | | user_reserved_code |
| 91 | SHIR04 | IR04 | IntegrationReference04 | A | 0 | STRING | Y | | | integration_reference_04 |
| 115 | SHOPBO | OPBO | OPBusinessObjective | A | 0 | STRING | Y | | | op_business_objective |
| 88 | SHIR01 | IR01 | IntegrationReference01 | A | 0 | STRING | Y | | | integration_reference_01 |
| 90 | SHIR03 | IR03 | IntegrationReference03 | A | 0 | STRING | Y | | | integration_reference_03 |
| 92 | SHIR05 | IR05 | IntegrationReference05 | A | 0 | STRING | Y | | | integration_reference_05 |
| 27 | SHDEL2 | DEL2 | DeliveryInstructLine2 | A | 0 | STRING | Y | | | delivery_instruct_line_02 |
| 89 | SHIR02 | IR02 | IntegrationReference02 | A | 0 | STRING | Y | | | integration_reference_02 |
| 120 | SHOPPS | OPPS | OPPromisedStatus | A | 0 | STRING | Y | | | op_promised_status |
| 24 | SHVR01 | VR01 | Reference1 | A | 0 | STRING | Y | | | reference_01 |
| 25 | SHVR02 | VR02 | Reference2Vendor | A | 0 | STRING | Y | | | reference_02_vendor |
| 66 | SHCACT | CACT | AcctNoCrBank | A | 0 | STRING | Y | | | acct_no_cr_bank |
| 93 | SHVR03 | VR03 | ReferenceUCISNo | A | 0 | STRING | Y | | | reference_ucis_no |
| 6 | SHCO | CO | Company | A | 0 | STRING | Y | | | company |
| 7 | SHOKCO | OKCO | CompanyKeyOriginal | A | 0 | STRING | Y | | | company_key_original |
| 10 | SHRKCO | RKCO | CompanyKeyRelated | A | 0 | STRING | Y | | | company_key_related |
| 5 | SHMCU | MCU | CostCenter | A | 0 | STRING | Y | | | cost_center |
| 11 | SHRORN | RORN | RelatedPoSoNumber | A | 0 | STRING | Y | | | related_po_so_number |
| 8 | SHOORN | OORN | OriginalPoSoNumber | A | 0 | STRING | Y | | | original_po_so_number |
| 4 | SHSFXO | SFXO | OrderSuffix | A | 0 | STRING | Y | | | order_suffix |
| 29 | SHPTC | PTC | PaymentTermsCode01 | A | 0 | STRING | Y | | | payment_terms_code_01 |
| 112 | SHBCRC | BCRC | CurrencyCodeBase | A | 0 | STRING | Y | | | currency_code_base |
| 71 | SHCRCD | CRCD | CurrencyCodeFrom | A | 0 | STRING | Y | | | currency_code_from |
| 83 | SHUSER | USER | UserId | A | 0 | STRING | Y | | | user_id |
| 84 | SHPID | PID | ProgramId | A | 0 | STRING | Y | | | program_id |
| 85 | SHJOBN | JOBN | WorkStationId | A | 0 | STRING | Y | | | work_station_id |
| 35 | SHTXA1 | TXA1 | TaxArea1 | A | 0 | STRING | Y | | | tax_area_01 |
| 77 | SHTKBY | TKBY | OrderTakenBy | A | 0 | STRING | Y | | | order_taken_by |
| 111 | SHBSC | BSC | BuyingSegmentCode | A | 0 | STRING | Y | | | buying_segment_code |
| 76 | SHORBY | ORBY | OrderedBy | A | 0 | STRING | Y | | | ordered_by |
| 65 | SHAUTN | AUTN | AuthorizationNoCredit | A | 0 | STRING | Y | | | authorization_no_credit |
| 70 | SHCRRM | CRRM | CurrencyMode | A | 0 | STRING | Y | | | currency_mode |
| 56 | SHFUF1 | FUF1 | ApplyFreight | A | 0 | STRING | Y | | | apply_freight |
| 57 | SHFRTC | FRTC | FreightCalculatedYN | A | 0 | STRING | Y | | | freight_calculated_yn |
| 38 | SHATXT | ATXT | AssociatedText | A | 0 | STRING | Y | | | associated_text |
| 55 | SHAFT | AFT | ApplyFreightYN | A | 0 | STRING | Y | | | apply_freight_yn |
| 123 | SHOPSS | OPSS | OPAllowSubstitutes | A | 0 | STRING | Y | | | op_allow_substitutes |
| 122 | SHOPMS | OPMS | OPAllowMultiSource | A | 0 | STRING | Y | | | op_allow_multi_source |
| 41 | SHSBAL | SBAL | SubstitutesAllowedYN | A | 0 | STRING | Y | | | substitutes_allowed_yn |
| 124 | SHOPBA | OPBA | OPAllowBackorders | A | 0 | STRING | Y | | | op_allow_backorders |
| 40 | SHBACK | BACK | BackordersAllowedYN | A | 0 | STRING | Y | | | backorders_allowed_yn |
| 43 | SHPLST | PLST | PricePickListYN | A | 0 | STRING | Y | | | price_pick_list_yn |
| 114 | SHAUFI | AUFI | AddressNumberForTransportation | A | 0 | STRING | Y | | | address_number_for_transportation ⚠️(NCHAR2 flag) |
| 121 | SHOPPL | OPPL | OPPartialOrderShipment | A | 0 | STRING | Y | | | op_partial_order_shipment |
| 125 | SHOPLL | OPLL | OPPartialShipLineItems | A | 0 | STRING | Y | | | op_partial_ship_line_items |
| 39 | SHPRIO | PRIO | PriorityProcessing | A | 0 | STRING | Y | | | priority_processing |
| 113 | SHAUFT | AUFT | AddressNumberForTax | A | 0 | STRING | Y | | | address_number_for_tax ⚠️(NCHAR2 flag) |
| 60 | SHFUF2 | FUF2 | PostQuantities | A | 0 | STRING | Y | | | post_quantities |
| 58 | SHMORD | MORD | MergeOrdersYN | A | 0 | STRING | Y | | | merge_orders_yn |
| 130 | SHOTIND | OTIND | OrderTypeIndicator | A | 0 | STRING | Y | | | order_type_indicator |
| 116 | SHOPTC | OPTC | OPTotalCost | P | 4 | DOUBLE | Y | | | op_total_cost |
| 33 | SHTRDC | TRDC | DiscountTrade | P | 3 | DOUBLE | Y | | | discount_trade |
| 34 | SHPCRT | PCRT | PercentRetainage1 | P | 3 | DOUBLE | Y | | | percent_retainage_01 |
| 80 | SHURAT | URAT | UserReservedAmount | P | 2 | DOUBLE | Y | | | user_reserved_amount |
| 74 | SHFAP | FAP | AmountForeignOpen | P | 2 | DOUBLE | Y | | | amount_foreign_open |
| 75 | SHFCST | FCST | AmountForeignTotalC | P | 2 | DOUBLE | Y | | | amount_foreign_total_c |
| 61 | SHOTOT | OTOT | AmountOrderGross | P | 2 | DOUBLE | Y | | | amount_order_gross |
| 62 | SHTOTC | TOTC | AmountTotalCost | P | 2 | DOUBLE | Y | | | amount_total_cost |
| 17 | SHTRDJ | TRDJ | DateTransactionJulian | S | 0 | INT | Y | | | date_transaction_julian |
| 16 | SHDRQJ | DRQJ | DateRequestedJulian | S | 0 | INT | Y | | | date_requested_julian |
| 20 | SHADDJ | ADDJ | ActualShipDate | S | 0 | INT | Y | | | actual_ship_date |
| 23 | SHPPDJ | PPDJ | DatePromisedShipJu | S | 0 | INT | Y | | | date_promised_ship_julian |
| 117 | SHOPLD | OPLD | OPLatestLineDate | S | 0 | INT | Y | | | op_latest_line_date |
| 21 | SHCNDJ | CNDJ | CancelDate | S | 0 | INT | Y | | | cancel_date |
| 18 | SHPDDJ | PDDJ | ScheduledPickDate | S | 0 | INT | Y | | | scheduled_pick_date |
| 97 | SHRQSJ | RQSJ | DateRequestedShip | S | 0 | INT | Y | | | date_requested_ship |
| 103 | SHADLJ | ADLJ | DateActualDelivery | S | 0 | INT | Y | | | date_actual_delivery |
| 19 | SHOPDJ | OPDJ | DateOriginalPromisde | S | 0 | INT | Y | | | date_original_promisde |
| 22 | SHPEFJ | PEFJ | DatePriceEffectiveDate | S | 0 | INT | Y | | | date_price_effective_date |
| 67 | SHCEXP | CEXP | DateExpired | S | 0 | INT | Y | | | date_expired |
| 79 | SHURDT | URDT | UserReservedDate | S | 0 | INT | Y | | | user_reserved_date |
| 86 | SHUPMJ | UPMJ | DateUpdated | S | 0 | INT | Y | | | date_updated |
| 87 | SHTDAY | TDAY | TimeOfDay | P | 0 | BIGINT | Y | | | time_of_day |
| 72 | SHCRR | CRR | CurrencyConverRateOv | P | 0 | BIGINT | Y | | | currency_conver_rate_ov |
| 107 | SHDVAN | DVAN | AddressNumberDeliveredTo | P | 0 | BIGINT | Y | | | address_number_delivered_to |
| 132 | SHCCIDLN | CCIDLN | CustomerContactLineNumberID | P | 0 | BIGINT | Y | | | customer_contact_line_number_id |
| 131 | SHPRCIDLN | PRCIDLN | PartnerContactLineNumID | P | 0 | BIGINT | Y | | | partner_contact_line_num_id |
| 99 | SHPDTT | PDTT | PromisedPickTime | P | 0 | BIGINT | Y | | | promised_pick_time |
| 98 | SHPSTM | PSTM | TimeFuture2 | P | 0 | BIGINT | Y | | | time_future_02 |
| 118 | SHOPBK | OPBK | OPNumberofBackorders | P | 0 | BIGINT | Y | | | op_numberof_backorders ⚠️ (op_number_of_backorders) |
| 133 | SHSHCCIDLN | SHCCIDLN | ShipToCusContactLineNumID | P | 0 | BIGINT | Y | | | ship_to_cus_contact_line_num_id |
| 104 | SHPBAN | PBAN | AddressNumberPaidBy | P | 0 | BIGINT | Y | | | address_number_paid_by |
| 119 | SHOPSB | OPSB | OPNumberofSubstitutes | P | 0 | BIGINT | Y | | | op_numberof_substitutes ⚠️ (op_number_of_substitutes) |
| 106 | SHFTAN | FTAN | AddressNumberForwardedTo | P | 0 | BIGINT | Y | | | address_number_forwarded_to |
| 105 | SHITAN | ITAN | AddressNumberInvoicedTo | P | 0 | BIGINT | Y | | | address_number_invoiced_to |
| 100 | SHOPTT | OPTT | TimeOriginalPromised | P | 0 | BIGINT | Y | | | time_original_promised |
| 127 | SHOPPID | OPPID | OpportunityId | P | 0 | BIGINT | Y | | | opportunity_id |
| 126 | SHPRAN8 | PRAN8 | PartnerAddressNumber | P | 0 | BIGINT | Y | | | partner_address_number |
| 94 | SHSOOR | SOOR | SourceOfOrder | S | 0 | BIGINT | Y | | | source_of_order |
| 110 | SHCORD | CORD | NumberChangeOrder | S | 0 | BIGINT | Y | | | number_change_order |
| 101 | SHDRQT | DRQT | RequestedDeliveryTime | S | 0 | BIGINT | Y | | | requested_delivery_time |
| 96 | SHRSDT | RSDT | PromisedDeliveryTime | S | 0 | BIGINT | Y | | | promised_delivery_time |
| 46 | SHANBY | ANBY | BuyerNumber | S | 0 | BIGINT | Y | | | buyer_number |
| 14 | SHSHAN | SHAN | AddressNumberShipTo | S | 0 | BIGINT | Y | | | address_number_ship_to |
| 15 | SHPA8 | PA8 | AddressNumberParent | S | 0 | BIGINT | Y | | | address_number_parent |
| 95 | SHPMDT | PMDT | ScheduledShipmentTime | S | 0 | BIGINT | Y | | | scheduled_shipment_time |
| 102 | SHADTM | ADTM | ActualShipmentTime | S | 0 | BIGINT | Y | | | actual_shipment_time |
| 44 | SHINVC | INVC | InvoiceCopies | S | 0 | BIGINT | Y | | | invoice_copies |
| 47 | SHCARS | CARS | Carrier | S | 0 | BIGINT | Y | | | carrier |
| 108 | SHDOC1 | DOC1 | DocumentOrderInvoi | S | 0 | BIGINT | Y | | | document_order_invoi |
| 13 | SHAN8 | AN8 | AddressNumber | S | 0 | BIGINT | Y | | | address_number |
| 81 | SHURAB | URAB | UserReservedNumber | S | 0 | BIGINT | Y | | | user_reserved_number |

## F4101 — Item Master
**PK / grain:** `IMITM` (index `F4101_0`) — item. Alt unique indexes: `IMLITM`→`F4101_2`, `IMAITM`→`F4101_3`. **209 columns.**

| col_id | column | alias | friendly_label | jde | dec | fabric | null | key | udc | snake_case |
|---|---|---|---|---|---|---|---|---|---|---|
| 29 | IMPDGR | PDGR | ProductGroupFrom | A | 0 | STRING | Y | | 41B/PG | product_group_from |
| 30 | IMDSGP | DSGP | DispatchGrp | A | 0 | STRING | Y | | 41B/DG | dispatch_grp |
| 69 | IMFRGD | FRGD | FromGrade | A | 0 | STRING | Y | | 40/LG | from_grade |
| 68 | IMSTDG | STDG | StandardGrade | A | 0 | STRING | Y | | 40/LG | standard_grade |
| 70 | IMTHGD | THGD | ThruGrade | A | 0 | STRING | Y | | 40/LG | thru_grade |
| 9 | IMSRP2 | SRP2 | SalesReportingCode2 | A | 0 | STRING | Y | | 41/S2 | sales_reporting_code_02 |
| 21 | IMPRP4 | PRP4 | PurchasingReportCode4 | A | 0 | STRING | Y | | 41/P4 | purchasing_report_code_04 |
| 12 | IMSRP5 | SRP5 | SalesReportingCode5 | A | 0 | STRING | Y | | 41/S5 | sales_reporting_code_05 |
| 20 | IMPRP3 | PRP3 | PurchasingReportCode3 | A | 0 | STRING | Y | | 41/P3 | purchasing_report_code_03 |
| 10 | IMSRP3 | SRP3 | SalesReportingCode3 | A | 0 | STRING | Y | | 41/S3 | sales_reporting_code_03 |
| 19 | IMPRP2 | PRP2 | PurchasingReportCode2 | A | 0 | STRING | Y | | 41/P2 | purchasing_report_code_02 |
| 18 | IMPRP1 | PRP1 | PurchasingReportCode1 | A | 0 | STRING | Y | | 41/P1 | purchasing_report_code_01 |
| 11 | IMSRP4 | SRP4 | SalesReportingCode4 | A | 0 | STRING | Y | | 41/S4 | sales_reporting_code_04 |
| 54 | IMCYCL | CYCL | CycleCountCategory | A | 0 | STRING | Y | | 41/8 | cycle_count_category |
| 22 | IMPRP5 | PRP5 | PurchasingReportCode5 | A | 0 | STRING | Y | | 41/P5 | purchasing_report_code_05 |
| 41 | IMSHCN | SHCN | ShippingConditionsCode | A | 0 | STRING | Y | | 41/C | shipping_conditions_code |
| 8 | IMSRP1 | SRP1 | SalesReportingCode1 | A | 0 | STRING | Y | | 41/S1 | sales_reporting_code_01 |
| 42 | IMSHCM | SHCM | ShippingCommodityClass | A | 0 | STRING | Y | | 41/E | shipping_commodity_class |
| 52 | IMSUTM | SUTM | UnitOfMeasureStocki | A | 0 | STRING | Y | | 00/UM | uom_stocki |
| 51 | IMUVM1 | UVM1 | UnitOfMeasureVolume | A | 0 | STRING | Y | | 00/UM | uom_volume |
| 46 | IMUOM4 | UOM4 | UnitOfMeasurePricing | A | 0 | STRING | Y | | 00/UM | uom_pricing |
| 44 | IMUOM2 | UOM2 | UnitOfMeasureSecondary | A | 0 | STRING | Y | | 00/UM | uom_secondary |
| 136 | IMUMS1 | UMS1 | UnitofMeasureSCCPI1 | A | 0 | STRING | Y | | 00/UM | uom_sccpi_01 |
| 43 | IMUOM1 | UOM1 | UnitOfMeasurePrimary | A | 0 | STRING | Y | | 00/UM | uom_primary |
| 50 | IMUWUM | UWUM | UnitOfMeasureWeight | A | 0 | STRING | Y | | 00/UM | uom_weight |
| 139 | IMUMS4 | UMS4 | UnitofMeasureSCCPI4 | A | 0 | STRING | Y | | 00/UM | uom_sccpi_04 |
| 47 | IMUOM6 | UOM6 | UnitOfMeasureShipping | A | 0 | STRING | Y | | 00/UM | uom_shipping |
| 143 | IMUMS8 | UMS8 | UnitofMeasureSCCPI8 | A | 0 | STRING | Y | | 00/UM | uom_sccpi_08 |
| 135 | IMUMS0 | UMS0 | UnitofMeasureSCCPI0 | A | 0 | STRING | Y | | 00/UM | uom_sccpi_00 |
| 142 | IMUMS7 | UMS7 | UnitofMeasureSCCPI7 | A | 0 | STRING | Y | | 00/UM | uom_sccpi_07 |
| 141 | IMUMS6 | UMS6 | UnitofMeasureSCCPI6 | A | 0 | STRING | Y | | 00/UM | uom_sccpi_06 |
| 138 | IMUMS3 | UMS3 | UnitofMeasureSCCPI3 | A | 0 | STRING | Y | | 00/UM | uom_sccpi_03 |
| 134 | IMUMDF | UMDF | UnitofMeasureAggregateUPC | A | 0 | STRING | Y | | 00/UM | uom_aggregate_upc |
| 137 | IMUMS2 | UMS2 | UnitofMeasureSCCPI2 | A | 0 | STRING | Y | | 00/UM | uom_sccpi_02 |
| 133 | IMUMUP | UMUP | UnitofMeasureUPC | A | 0 | STRING | Y | | 00/UM | uom_upc |
| 45 | IMUOM3 | UOM3 | UnitOfMeasurePurchas | A | 0 | STRING | Y | | 00/UM | uom_purchas |
| 140 | IMUMS5 | UMS5 | UnitofMeasureSCCPI5 | A | 0 | STRING | Y | | 00/UM | uom_sccpi_05 |
| 49 | IMUOM9 | UOM9 | UnitOfMeasureAllocation | A | 0 | STRING | Y | | 00/UM | uom_allocation |
| 48 | IMUOM8 | UOM8 | UnitOfMeasureProduction | A | 0 | STRING | Y | | 00/UM | uom_production |
| 76 | IMIFLA | IFLA | ItemFlashMessage | A | 0 | STRING | Y | | 40/FL | item_flash_message |
| 94 | IMPTSC | PTSC | PartStatusCode | A | 0 | STRING | Y | | 40/PS | part_status_code |
| 104 | IMMPSP | MPSP | PlanTimeFenceRule | A | 0 | STRING | Y | | 34/TF | plan_time_fence_rule |
| 72 | IMSTKT | STKT | StockingType | A | 0 | STRING | Y | | 41/I | stocking_type |
| 88 | IMLOTS | LOTS | LotStatusCode | A | 0 | STRING | Y | | 41/L | lot_status_code |
| 59 | IMPRPO | PRPO | PotencyPricing | A | 0 | STRING | Y | | 40/LP | potency_pricing |
| 78 | IMINMG | INMG | PrintMessage1 | A | 0 | STRING | Y | | 40/PM | print_message_01 |
| 33 | IMORPR | ORPR | OrderRepriceCategory | A | 0 | STRING | Y | | 40/PI | order_reprice_category |
| 32 | IMRPRC | RPRC | RepriceBasketPriceCat | A | 0 | STRING | Y | | 40/PI | reprice_basket_price_cat |
| 31 | IMPRGR | PRGR | PricingCategory | A | 0 | STRING | Y | | 40/PI | pricing_category |
| 25 | IMPRP8 | PRP8 | PurchReportingCode8 | A | 0 | STRING | Y | | 41/02 | purch_reporting_code_08 |
| 15 | IMSRP8 | SRP8 | SalesReportingCode8 | A | 0 | STRING | Y | | 41/08 | sales_reporting_code_08 |
| 16 | IMSRP9 | SRP9 | SalesReportingCode9 | A | 0 | STRING | Y | | 41/09 | sales_reporting_code_09 |
| 14 | IMSRP7 | SRP7 | SalesReportingCode7 | A | 0 | STRING | Y | | 41/07 | sales_reporting_code_07 |
| 23 | IMPRP6 | PRP6 | PurchReportingCode6 | A | 0 | STRING | Y | | 41/01 | purch_reporting_code_06 |
| 13 | IMSRP6 | SRP6 | SalesReportingCode6 | A | 0 | STRING | Y | | 41/06 | sales_reporting_code_06 |
| 24 | IMPRP7 | PRP7 | PurchReportingCode7 | A | 0 | STRING | Y | | 41/02 | purch_reporting_code_07 |
| 26 | IMPRP9 | PRP9 | PurchReportingCode9 | A | 0 | STRING | Y | | 41/02 | purch_reporting_code_09 |
| 17 | IMSRP0 | SRP0 | SalesReportingCode10 | A | 0 | STRING | Y | | 41/10 | sales_reporting_code_010 ⚠️ (should _10) |
| 27 | IMPRP0 | PRP0 | PurchReportingCode10 | A | 0 | STRING | Y | | 41/05 | purch_reporting_code_10 |
| 55 | IMGLPT | GLPT | GlCategory | A | 0 | STRING | Y | | 41/9 | gl_category |
| 146 | IMEQTY | EQTY | PalletType | A | 0 | STRING | Y | | 46/EQ | pallet_type |
| 1 | IMITM | ITM | IdentifierShortItem | S | 0 | BIGINT | N | PK | | identifier_short_item |
| 3 | IMAITM | AITM | Identifier3rdItem | A | 0 | STRING | Y | F4101_3 | | identifier_3rd_item ⚠️ (vs F4211 identifier_third_item) |
| 2 | IMLITM | LITM | Identifier2ndItem | A | 0 | STRING | Y | F4101_2 | | identifier_2nd_item ⚠️ (vs F4211 identifier_second_item) |
| 5 | IMDSC2 | DSC2 | DescriptionLine2 | O | 0 | STRING | Y | | | description_line_02 |
| 182 | IMAUOM | AUOM | APSPlanningUOM | O | 0 | STRING | Y | | | aps_planning_uom |
| 125 | IMURRF | URRF | UserReservedReference | A | 0 | STRING | Y | | | user_reserved_reference |
| 204 | IMATPRN | ATPRN | ATPRuleName | A | 0 | STRING | Y | | | atp_rule_name |
| 132 | IMSCC0 | SCC0 | AggregateSCCCodePI0 | A | 0 | STRING | Y | | | aggregate_scc_code_pi0 |
| 131 | IMUPCN | UPCN | UPCNumber | A | 0 | STRING | Y | | | upc_number |
| 28 | IMCDCD | CDCD | CommodityCode | A | 0 | STRING | Y | | | commodity_code |
| 148 | IMTMPL | TMPL | Template | A | 0 | STRING | Y | | | template |
| 208 | IMVCPFC | VCPFC | ATOForecastControl | A | 0 | STRING | Y | | | ato_forecast_control |
| 35 | IMDRAW | DRAW | DrawingNumber | A | 0 | STRING | Y | | | drawing_number |
| 121 | IMURCD | URCD | UserReservedCode | A | 0 | STRING | Y | | | user_reserved_code |
| 73 | IMLNTY | LNTY | LineType | A | 0 | STRING | Y | | | line_type |
| 77 | IMTFLA | TFLA | TemporaryItemFlashMessag | A | 0 | STRING | Y | | | temporary_item_flash_messag |
| 36 | IMRVNO | RVNO | RevisionNumber | A | 0 | STRING | Y | | | revision_number |
| 6 | IMSRTX | SRTX | SearchText | A | 0 | STRING | Y | | | search_text |
| 7 | IMALN | ALN | SearchTextCompressed | A | 0 | STRING | Y | | | search_text_compressed |
| 4 | IMDSC1 | DSC1 | DescriptionLine1 | A | 0 | STRING | Y | | | description_line_01 |
| 206 | IMATPAC | ATPAC | ATPComponents | A | 0 | STRING | Y | | | atp_components |
| 195 | IMLINE | LINE | LineIdentifier | A | 0 | STRING | Y | | | line_identifier |
| 83 | IMWARR | WARR | TypeWarranty | A | 0 | STRING | Y | | | type_warranty |
| 84 | IMCMCG | CMCG | CommissionCategory | A | 0 | STRING | Y | | | commission_category |
| 203 | IMCUMTH | CUMTH | CumulativeThresholdUOM | A | 0 | STRING | Y | | | cumulative_threshold_uom |
| 180 | IMLOTC | LOTC | LotStatusCodeExpanded | A | 0 | STRING | Y | | | lot_status_expanded ⚠️(dropped "code") |
| 193 | IMUMTH | UMTH | OperationalThresholdUOM | A | 0 | STRING | Y | | | operational_threshold_uom |
| 126 | IMUSER | USER | UserId | A | 0 | STRING | Y | | | user_id |
| 127 | IMPID | PID | ProgramId | A | 0 | STRING | Y | | | program_id |
| 128 | IMJOBN | JOBN | WorkStationId | A | 0 | STRING | Y | | | work_station_id |
| 150 | IMSEG2 | SEG2 | Segment2 | A | 0 | STRING | Y | | | segment_02 |
| 149 | IMSEG1 | SEG1 | Segment1 | A | 0 | STRING | Y | | | segment_01 |
| 155 | IMSEG7 | SEG7 | Segment7 | A | 0 | STRING | Y | | | segment_07 |
| 157 | IMSEG9 | SEG9 | Segment9 | A | 0 | STRING | Y | | | segment_09 |
| 152 | IMSEG4 | SEG4 | Segment4 | A | 0 | STRING | Y | | | segment_04 |
| 156 | IMSEG8 | SEG8 | Segment8 | A | 0 | STRING | Y | | | segment_08 |
| 151 | IMSEG3 | SEG3 | Segment3 | A | 0 | STRING | Y | | | segment_03 |
| 154 | IMSEG6 | SEG6 | Segment6 | A | 0 | STRING | Y | | | segment_06 |
| 158 | IMSEG0 | SEG0 | Segment10 | A | 0 | STRING | Y | | | segment_010 ⚠️ (should _10) |
| 153 | IMSEG5 | SEG5 | Segment5 | A | 0 | STRING | Y | | | segment_05 |
| 79 | IMABCS | ABCS | AbcCode1SalesInv | A | 0 | STRING | Y | | | abc_code_01_sales_inv |
| 56 | IMPLEV | PLEV | PriceLevel | A | 0 | STRING | Y | | | price_level |
| 57 | IMPPLV | PPLV | LevelPurchasingPrice | A | 0 | STRING | Y | | | level_purchasing_price |
| 62 | IMSRCE | SRCE | LayerCodeSource | A | 0 | STRING | Y | | | layer_code_source |
| 184 | IMGCMP | GCMP | Composition | A | 0 | STRING | Y | | | composition |
| 117 | IMCOBY | COBY | CoproductsByproducts | A | 0 | STRING | Y | | | coproducts_byproducts |
| 106 | IMITC | ITC | IssueTypeCode | A | 0 | STRING | Y | | | issue_type_code |
| 199 | IMKANEXLL | KANEXLL | KanbanExplodeToLowerLevel | A | 0 | STRING | Y | | | kanban_explode_to_lower_level |
| 159 | IMMIC | MIC | MatrixControlled | A | 0 | STRING | Y | | | matrix_controlled |
| 87 | IMFIFO | FIFO | FifoProcessing | A | 0 | STRING | Y | | | fifo_processing |
| 74 | IMCONT | CONT | ContractItem | A | 0 | STRING | Y | | | contract_item |
| 80 | IMABCM | ABCM | AbcCode2MarginInv | A | 0 | STRING | Y | | | abc_code_02_margin_inv |
| 147 | IMWTRQ | WTRQ | ItemWeightRequired | A | 0 | STRING | Y | | | item_weight_required |
| 183 | IMCONB | CONB | Consumable | A | 0 | STRING | Y | | | consumable |
| 91 | IMMPST | MPST | PlanningCode | A | 0 | STRING | Y | | | planning_code |
| 119 | IMCMGL | CMGL | AllocateByLot | A | 0 | STRING | Y | | | allocate_by_lot |
| 162 | IMCMDM | CMDM | CommitmentDateMethod | A | 0 | STRING | Y | | | commitment_date_method |
| 58 | IMCLEV | CLEV | CostLevel | A | 0 | STRING | Y | | | cost_level |
| 61 | IMBPFG | BPFG | BulkPackedFlag | A | 0 | STRING | Y | | | bulk_packed_flag |
| 95 | IMSNS | SNS | StockNonstock | A | 0 | STRING | Y | | | stock_nonstock |
| 189 | IMCMETH | CMETH | MethodOfCostCalcula | A | 0 | STRING | Y | | | method_of_cost_calcula |
| 188 | IMVMINV | VMINV | VendorManagedInventory | A | 0 | STRING | Y | | | vendor_managed_inventory |
| 63 | IMOT1Y | OT1Y | ConstantFutureUse1 | A | 0 | STRING | Y | | | constant_future_use1 ⚠️ (should _01) |
| 85 | IMSRNR | SRNR | SerialNumberRequired | A | 0 | STRING | Y | | | serial_number_required |
| 200 | IMSCPSELL | SCPSELL | Sellable | A | 0 | STRING | Y | | | sellable |
| 99 | IMOPC | OPC | OrderPolicyCode | A | 0 | STRING | Y | | | order_policy_code |
| 207 | IMCOORE | COORE | CountryOfOriginRequired | A | 0 | STRING | Y | | | country_of_origin_required |
| 175 | IMXDCK | XDCK | CrossDockFlag | A | 0 | STRING | Y | | | cross_dock_flag |
| 178 | IMRWLA | RWLA | RestrictWoLotAssignment | A | 0 | STRING | Y | | | restrict_wo_lot_assignment |
| 75 | IMBACK | BACK | BackordersAllowedYN | A | 0 | STRING | Y | | | backorders_allowed_yn |
| 209 | IMPNYN | PNYN | ProductionNumberControlled | A | 0 | STRING | Y | | | production_number_controlled |
| 197 | IMKBIT | KBIT | KanbanItem | A | 0 | STRING | Y | | | kanban_item |
| 160 | IMAING | AING | ActiveIngredientFlag | A | 0 | STRING | Y | | | active_ingredient_flag |
| 198 | IMDFENDITM | DFENDITM | DFEndItemFlag | A | 0 | STRING | Y | | | df_end_item_flag |
| 81 | IMABCI | ABCI | AbcCode3InvestInv | A | 0 | STRING | Y | | | abc_code_03_invest_inv |
| 82 | IMOVR | OVR | AbcCodeOverrideIndica | A | 0 | STRING | Y | | | abc_code_override_indica |
| 205 | IMATPCA | ATPCA | CheckATP | A | 0 | STRING | Y | | | check_atp |
| 173 | IMDPPO | DPPO | DualPickingProcessOption | A | 0 | STRING | Y | | | dual_picking_process_option |
| 174 | IMDUAL | DUAL | DualUnitOfMeasureItem | A | 0 | STRING | Y | | | dual_uom_item |
| 179 | IMLNPA | LNPA | LotNumberPreAsignment | A | 0 | STRING | Y | | | lot_number_pre_asignment |
| 107 | IMORDW | ORDW | OrderWithYN | A | 0 | STRING | Y | | | order_with_yn |
| 105 | IMMRPP | MRPP | FixedVariableLeadtime | A | 0 | STRING | Y | | | fixed_variable_leadtime |
| 37 | IMDSZE | DSZE | DrawingSize | A | 0 | STRING | Y | | | drawing_size |
| 64 | IMOT2Y | OT2Y | ConstantFutureUse2 | A | 0 | STRING | Y | | | constant_future_use_02 |
| 176 | IMLAF | LAF | LotAuditFlag | A | 0 | STRING | Y | | | lot_audit_flag |
| 181 | IMAPSC | APSC | ConstraintsFlag | A | 0 | STRING | Y | | | constraints_flag |
| 86 | IMPMTH | PMTH | MethodOfPriceCalcula | A | 0 | STRING | Y | | | method_of_price_calcula |
| 190 | IMEXPI | EXPI | ExplodeItem10 | A | 0 | STRING | Y | | | explode_item_10 |
| 71 | IMCOTY | COTY | ComponentType | A | 0 | STRING | Y | | | component_type |
| 116 | IMMAKE | MAKE | MakeBuyCode | A | 0 | STRING | Y | | | make_buy_code |
| 177 | IMLTFM | LTFM | SpecialLotFormat | A | 0 | STRING | Y | | | special_lot_format |
| 163 | IMLECM | LECM | LotExpirationDateCalculationMethod | A | 0 | STRING | Y | | | lot_expiration_date_calculation_method |
| 144 | IMPOC | POC | Payonconsumption | A | 0 | STRING | Y | | | pay_on_consumption |
| 187 | IMASHL | ASHL | AllowShippingHeldLots | A | 0 | STRING | Y | | | allow_shipping_held_lots |
| 60 | IMCKAV | CKAV | CheckAvailabilityYN | A | 0 | STRING | Y | | | check_availability_yn |
| 194 | IMLMFG | LMFG | LeanFlag | A | 0 | STRING | Y | | | lean_flag |
| 53 | IMUMVW | UMVW | UnitofMeasureVolumeorWeI | A | 0 | STRING | Y | | | uom_volume_or_we_i ⚠️ (mangled) |
| 103 | IMLTPU | LTPU | LeadtimePerUnit | S | 2 | DOUBLE | Y | | | leadtime_per_unit |
| 196 | IMDFTPCT | DFTPCT | TotalProductCycleTime | P | 5 | DOUBLE | Y | | | total_product_cycle_time |
| 186 | IMPRI2 | PRI2 | PriorityTwoAlertLevel | P | 4 | DOUBLE | Y | | | priority_two_alert_level |
| 201 | IMMOPTH | MOPTH | MaxOperationalThreshold | P | 4 | DOUBLE | Y | | | max_operational_threshold |
| 38 | IMVCUD | VCUD | VolumeCubicDimensions | P | 4 | DOUBLE | Y | | | volume_cubic_dimensions |
| 202 | IMMCUTH | MCUTH | MaxCumulativeThreshold | P | 4 | DOUBLE | Y | | | max_cumulative_threshold |
| 185 | IMPRI1 | PRI1 | PriorityOneAlertLevel | P | 4 | DOUBLE | Y | | | priority_one_alert_level |
| 67 | IMTHRP | THRP | ThruPotency | P | 3 | DOUBLE | Y | | | thru_potency |
| 92 | IMPCTM | PCTM | PercentMargin | P | 3 | DOUBLE | Y | | | percent_margin |
| 65 | IMSTDP | STDP | StandardPotency | P | 3 | DOUBLE | Y | | | standard_potency |
| 101 | IMACQ | ACQ | AcctingCostQty | P | 3 | DOUBLE | Y | | | accting_cost_qty |
| 102 | IMMLQ | MLQ | MfgLeadtimeQty | P | 3 | DOUBLE | Y | | | mfg_leadtime_qty |
| 66 | IMFRMP | FRMP | FromPotency | P | 3 | DOUBLE | Y | | | from_potency |
| 93 | IMMMPC | MMPC | MarginMaintenancePer | P | 3 | DOUBLE | Y | | | margin_maintenance_per |
| 123 | IMURAT | URAT | UserReservedAmount | P | 2 | DOUBLE | Y | | | user_reserved_amount |
| 172 | IMDLTL | DLTL | DualTolerance | P | 2 | DOUBLE | Y | | | dual_tolerance |
| 191 | IMOPTH | OPTH | MinOperationalThreshold | P | 2 | DOUBLE | Y | | | min_operational_threshold |
| 192 | IMCUTH | CUTH | MinCumulativeThreshold | P | 2 | DOUBLE | Y | | | min_cumulative_threshold |
| 145 | IMAVRT | AVRT | AverageQueueTimeHours | P | 2 | DOUBLE | Y | | | average_queue_time_hours |
| 122 | IMURDT | URDT | UserReservedDate | S | 0 | INT | Y | | | user_reserved_date |
| 129 | IMUPMJ | UPMJ | DateUpdated | S | 0 | INT | Y | | | date_updated |
| 130 | IMTDAY | TDAY | TimeOfDay | P | 0 | BIGINT | Y | | | time_of_day |
| 112 | IMMTF5 | MTF5 | TimeFence5 | P | 0 | BIGINT | Y | | | time_fence_05 |
| 89 | IMSLD | SLD | ShelfLifeDays | P | 0 | BIGINT | Y | | | shelf_life_days |
| 120 | IMCOMH | COMH | CommitmentSpecificDays | P | 0 | BIGINT | Y | | | commitment_specific_days |
| 164 | IMLEDD | LEDD | LotEffectiveDefaultDays | P | 0 | BIGINT | Y | | | lot_effective_default_days |
| 111 | IMMTF4 | MTF4 | TimeFence4 | P | 0 | BIGINT | Y | | | time_fence_04 |
| 109 | IMMTF2 | MTF2 | FreezeTimeFenceDays | P | 0 | BIGINT | Y | | | freeze_time_fence_days |
| 118 | IMLLX | LLX | LowLevelCode | P | 0 | BIGINT | Y | | | low_level_code |
| 168 | IMU2DD | U2DD | UserLotDate2DefaultDays | P | 0 | BIGINT | Y | | | user_lot_date_02_default_days |
| 165 | IMPEFD | PEFD | PurchasingEffectiveDays | P | 0 | BIGINT | Y | | | purchasing_effective_days |
| 161 | IMBBDD | BBDD | BestBeforeDefaultDays | P | 0 | BIGINT | Y | | | best_before_default_days |
| 108 | IMMTF1 | MTF1 | PlanningTimeFenceDays | P | 0 | BIGINT | Y | | | planning_time_fence_days |
| 100 | IMOPV | OPV | OrderPolicyValue | P | 0 | BIGINT | Y | | | order_policy_value |
| 166 | IMSBDD | SBDD | SellByDefaultDays | P | 0 | BIGINT | Y | | | sell_by_default_days |
| 171 | IMU5DD | U5DD | UserLotDate5DefaultDays | P | 0 | BIGINT | Y | | | user_lot_date_05_default_days |
| 169 | IMU3DD | U3DD | UserLotDate3DefaultDays | P | 0 | BIGINT | Y | | | user_lot_date_03_default_days |
| 170 | IMU4DD | U4DD | UserLotDate4DefaultDays | P | 0 | BIGINT | Y | | | user_lot_date_04_default_days |
| 110 | IMMTF3 | MTF3 | MsgTimeFenceDays | P | 0 | BIGINT | Y | | | msg_time_fence_days |
| 167 | IMU1DD | U1DD | UserLotDate1DefaultDays | P | 0 | BIGINT | Y | | | user_lot_date_01_default_days |
| 96 | IMLTLV | LTLV | LeadtimeLevel | S | 0 | BIGINT | Y | | | leadtime_level |
| 115 | IMSFLT | SFLT | SafetyLeadtime | S | 0 | BIGINT | Y | | | safety_leadtime |
| 114 | IMDEFD | DEFD | DeferDamperDays | S | 0 | BIGINT | Y | | | defer_damper_days |
| 34 | IMBUYR | BUYR | Buyer | S | 0 | BIGINT | Y | | | buyer |
| 40 | IMCARP | CARP | PreferCarrierPurchasin | S | 0 | BIGINT | Y | | | prefer_carrier_purchasin |
| 39 | IMCARS | CARS | Carrier | S | 0 | BIGINT | Y | | | carrier |
| 98 | IMLTCM | LTCM | LeadtimeCum | S | 0 | BIGINT | Y | | | leadtime_cum |
| 113 | IMEXPD | EXPD | ExpediteDamperDays | S | 0 | BIGINT | Y | | | expedite_damper_days |
| 97 | IMLTMF | LTMF | LeadtimeMfg | S | 0 | BIGINT | Y | | | leadtime_mfg |
| 90 | IMANPL | ANPL | AddressNumberPlanner | S | 0 | BIGINT | Y | | | address_number_planner |
| 124 | IMURAB | URAB | UserReservedNumber | S | 0 | BIGINT | Y | | | user_reserved_number |

## F41002 — Item UoM Conversion Factors
**PK / grain:** `UMMCU + UMITM + UMUM + UMRUM` (index `F41002_0`). `UMCONV` (dec 7) is the tonnage conversion. **16 columns.**

| col_id | column | alias | friendly_label | jde | dec | fabric | null | key | udc | snake_case |
|---|---|---|---|---|---|---|---|---|---|---|
| 4 | UMRUM | RUM | RelatedUnitOfMeasure | A | 0 | STRING | N | PK | 00/UM | related_uom |
| 3 | UMUM | UM | UnitOfMeasure | A | 0 | STRING | N | PK | 00/UM | uom |
| 2 | UMITM | ITM | IdentifierShortItem | S | 0 | BIGINT | N | PK | | identifier_short_item |
| 1 | UMMCU | MCU | CostCenter | A | 0 | STRING | N | PK | | cost_center |
| 8 | UMUSER | USER | UserId | A | 0 | STRING | Y | | | user_id |
| 9 | UMPID | PID | ProgramId | A | 0 | STRING | Y | | | program_id |
| 10 | UMJOBN | JOBN | WorkStationId | A | 0 | STRING | Y | | | work_station_id |
| 14 | UMEXSO | EXSO | ExcludeFromSO | A | 0 | STRING | Y | | | exclude_from_so |
| 13 | UMEXPO | EXPO | ExcludeFromPO | A | 0 | STRING | Y | | | exclude_from_po |
| 5 | UMUSTR | USTR | UnitOfMeasureStructure | A | 0 | STRING | Y | | | uom_structure |
| 6 | UMCONV | CONV | ConversionFactor | P | 7 | DOUBLE | Y | | | conversion_factor |
| 7 | UMCNV1 | CNV1 | ConversionFactorSec | P | 7 | DOUBLE | Y | | | conversion_factor_sec |
| 11 | UMUPMJ | UPMJ | DateUpdated | S | 0 | INT | Y | | | date_updated |
| 12 | UMTDAY | TDAY | TimeOfDay | P | 0 | BIGINT | Y | | | time_of_day |
| 16 | UMSEPC | SEPC | SalesPriceCode | S | 0 | BIGINT | Y | | | sales_price_code |
| 15 | UMPUPC | PUPC | PurchasePriceCode | S | 0 | BIGINT | Y | | | purchase_price_code |

## F4074 — Price Adjustment Detail (Advanced Pricing)
**PK / grain:** 11-col `F4074_0` = `ALKCOO+ALDCTO+ALDOCO+ALSFXO+ALLNID+ALAKID+ALSRCFD+ALOSEQ+ALSUBSEQ+ALTIER+ALPA04` — **line→many adjustments** (pre-aggregate before fact). `ALFVTR` (dec 4) = freight factor. **70 columns.**

| col_id | column | alias | friendly_label | jde | dec | fabric | null | key | udc | snake_case |
|---|---|---|---|---|---|---|---|---|---|---|
| 53 | ALAPRP2 | APRP2 | PricingReportCode2 | A | 0 | STRING | Y | | 45/P2 | pricing_report_code_02 |
| 54 | ALAPRP3 | APRP3 | PricingReportCode3 | A | 0 | STRING | Y | | 45/P3 | pricing_report_code_03 |
| 52 | ALAPRP1 | APRP1 | PricingReportCode1 | A | 0 | STRING | Y | | 45/P1 | pricing_report_code_01 |
| 26 | ALARSN | ARSN | AdjustmentReasonCode | A | 0 | STRING | Y | | 40/AR | adjustment_reason_code |
| 16 | ALUOM | UOM | UnitOfMeasureAsInput | A | 0 | STRING | Y | | 00/UM | uom_as_input |
| 18 | ALLEDG | LEDG | LedgType | A | 0 | STRING | Y | | 40/CM | ledg_type |
| 42 | ALADJSTS | ADJSTS | AdjustmentStatus | A | 0 | STRING | Y | | 45/ST | adjustment_status |
| 20 | ALBSCD | BSCD | BasisCode | A | 0 | STRING | Y | | 40/BC | basis_code |
| 28 | ALSBIF | SBIF | SubledgerInformation | A | 0 | STRING | Y | | 40/SI | subledger_information |
| 41 | ALOLVL | OLVL | OrderLevelAdjustmentYN | A | 0 | STRING | Y | | 40/AL | order_level_adjustment_yn |
| 49 | ALPDCL | PDCL | PromotionDisplayControl | A | 0 | STRING | Y | | 40/CO | promotion_display_control |
| 27 | ALACNT | ACNT | AdjustmentControlCode | A | 0 | STRING | Y | | 40/CO | adjustment_control_code |
| 19 | ALFRMN | FRMN | PriceFormulaName | A | 0 | STRING | Y | | 40/FM | price_formula_name |
| 11 | ALASN | ASN | PriceAdjustmentScheduleN | A | 0 | STRING | Y | | 40/AS | price_adjustment_schedule_n |
| 12 | ALAST | AST | PriceAdjustmentType | A | 0 | STRING | Y | | 40/TY | price_adjustment_type |
| 57 | ALAPRP6 | APRP6 | PricingReportCode6 | A | 0 | STRING | Y | | 45/P6 | pricing_report_code_06 |
| 56 | ALAPRP5 | APRP5 | PricingReportCode5 | A | 0 | STRING | Y | | 45/P5 | pricing_report_code_05 |
| 55 | ALAPRP4 | APRP4 | PricingReportCode4 | A | 0 | STRING | Y | | 45/P4 | pricing_report_code_04 |
| 66 | ALPA04 | PA04 | TargetApplication | A | 0 | STRING | N | PK | 40/TA | target_application |
| 2 | ALDCTO | DCTO | OrderType | A | 0 | STRING | N | PK | 00/DT | order_type |
| 10 | ALTIER | TIER | Tier | S | 0 | BIGINT | N | PK | | tier |
| 8 | ALOSEQ | OSEQ | SequenceNumber | S | 0 | BIGINT | N | PK | | sequence_number |
| 9 | ALSUBSEQ | SUBSEQ | SubSequenceNumber | S | 0 | BIGINT | N | PK | | sub_sequence_number |
| 6 | ALAKID | AKID | PriceHistoryAltKey | P | 0 | BIGINT | N | PK | | price_history_alt_key |
| 5 | ALLNID | LNID | LineNumber | P | 3 | DOUBLE | N | PK | | line_number |
| 1 | ALDOCO | DOCO | DocumentOrderInvoiceE | S | 0 | BIGINT | N | PK | | document_order_invoice_e |
| 4 | ALSFXO | SFXO | OrderSuffix | A | 0 | STRING | N | PK | | order_suffix |
| 3 | ALKCOO | KCOO | CompanyKeyOrderNo | A | 0 | STRING | N | PK | | company_key_order_no |
| 7 | ALSRCFD | SRCFD | PriceHistoryAltKeySource | A | 0 | STRING | N | PK | | price_history_alt_key_source |
| 38 | ALADJCAL | ADJCAL | AdjustmentCalculation | A | 0 | STRING | Y | | | adjustment_calculation |
| 43 | ALADJREF | ADJREF | AdjustmentReference | A | 0 | STRING | Y | | | adjustment_reference |
| 25 | ALGLC | GLC | GlClass | A | 0 | STRING | Y | | | gl_class |
| 40 | ALUOMVID | UOMVID | UOMforVolumneIncentives | A | 0 | STRING | Y | | | uo_mfor_volumne_incentives ⚠️ (mangled) |
| 48 | ALFVUM | FVUM | FactorValueUM | A | 0 | STRING | Y | | | factor_value_um |
| 70 | ALTSTRSNM | TSTRSNM | TestResultName | A | 0 | STRING | Y | | | test_result_name |
| 64 | ALPMTN | PMTN | PromotionID | A | 0 | STRING | Y | | | promotion_id |
| 15 | ALCRCD | CRCD | CurrencyCodeFrom | A | 0 | STRING | Y | | | currency_code_from |
| 59 | ALUSER | USER | UserId | A | 0 | STRING | Y | | | user_id |
| 60 | ALPID | PID | ProgramId | A | 0 | STRING | Y | | | program_id |
| 61 | ALJOBN | JOBN | WorkStationId | A | 0 | STRING | Y | | | work_station_id |
| 65 | ALRULENAME | RULENAME | RULENAME | A | 0 | STRING | Y | | | rulename |
| 46 | ALADJGRP | ADJGRP | AdjustmentGroup | A | 0 | STRING | Y | | | adjustment_group |
| 22 | ALABAS | ABAS | AdjustmentBasedon | A | 0 | STRING | Y | | | adjustment_based_on |
| 29 | ALMDED | MDED | ManualDiscount | A | 0 | STRING | Y | | | manual_discount |
| 37 | ALSRFLAG | SRFLAG | SlidingRateFlag | A | 0 | STRING | Y | | | sliding_rate_flag |
| 69 | ALSTPRCF | STPRCF | NewBasePriceFlag | A | 0 | STRING | Y | | | new_base_price_flag |
| 58 | ALNDPI | NDPI | NetDownPriceIndicator | A | 0 | STRING | Y | | | net_down_price_indicator |
| 47 | ALMEADJ | MEADJ | MutuallyExclusiveAdjustment | A | 0 | STRING | Y | | | mutually_exclusive_adjustment |
| 30 | ALPROV | PROV | PriceOverrideCode | A | 0 | STRING | Y | | | price_override_code |
| 67 | ALADJQTY | ADJQTY | AdjustQuantityToPay | A | 0 | STRING | Y | | | adjust_quantity_to_pay |
| 23 | ALUPRC | UPRC | AmtPricePerUnit2 | P | 6 | DOUBLE | Y | | | amt_price_per_unit_02 |
| 24 | ALFUP | FUP | AmtForPricePerUnit | P | 4 | DOUBLE | Y | | | amt_for_ppu ⚠️ (vs F4211 amt_for_price_per_unit) |
| 21 | ALFVTR | FVTR | FactorValue | P | 4 | DOUBLE | Y | | | factor_value |
| 36 | ALBSDVAL | BSDVAL | BasedOnValue | P | 4 | DOUBLE | Y | | | based_on_value |
| 68 | ALQTYPY | QTYPY | QuantityToPay | P | 3 | DOUBLE | Y | | | quantity_to_pay |
| 17 | ALMNQ | MNQ | QuantityMinimum | P | 3 | DOUBLE | Y | | | quantity_minimum |
| 62 | ALUPMJ | UPMJ | DateUpdated | S | 0 | INT | Y | | | date_updated |
| 63 | ALTDAY | TDAY | TimeOfDay | P | 0 | BIGINT | Y | | | time_of_day |
| 34 | ALOGID | OGID | OrderGroupKeyID | P | 0 | BIGINT | Y | | | order_group_key_id |
| 51 | ALCFGCID | CFGCID | ComponentIDNumber | P | 0 | BIGINT | Y | | | component_id_number |
| 32 | ALIGID | IGID | ItemGroupKeyID | P | 0 | BIGINT | Y | | | item_group_key_id |
| 33 | ALCGID | CGID | CustomerGroupKeyID | P | 0 | BIGINT | Y | | | customer_group_key_id |
| 35 | ALANPS | ANPS | AddressNumberPriceAdjustments | P | 0 | BIGINT | Y | | | address_number_price_adjustments |
| 50 | ALCFGID | CFGID | ConfigurationIDNumber | P | 0 | BIGINT | Y | | | configuration_id_number |
| 39 | ALNBRORD | NBRORD | NumberofOrders | P | 0 | BIGINT | Y | | | numberof_orders ⚠️ (number_of_orders) |
| 31 | ALATID | ATID | PriceAdjustmentKeyID | S | 0 | BIGINT | Y | | | price_adjustment_key_id |
| 45 | ALBNAD | BNAD | BeneficiaryAddress | S | 0 | BIGINT | Y | | | beneficiary_address |
| 44 | ALACCAN8 | ACCAN8 | AccumulateAtAddress | S | 0 | BIGINT | Y | | | accumulate_at_address |
| 13 | ALITM | ITM | IdentifierShortItem | S | 0 | BIGINT | Y | | | identifier_short_item |
| 14 | ALAN8 | AN8 | AddressNumber | S | 0 | BIGINT | Y | | | address_number |

## F4981 — Freight Audit History
**PK / grain:** `FHUK01` (index `F4981_0`) — one freight-audit record; **shipment (`FHSHPN`) is one-to-many** (pre-aggregate to shipment before relating to fact). `FHNAMT` (dec 2) = freight $. Supplies `FHRTN` (route number). **94 columns.**

| col_id | column | alias | friendly_label | jde | dec | fabric | null | key | udc | snake_case |
|---|---|---|---|---|---|---|---|---|---|---|
| 31 | FHCGC1 | CGC1 | ChargeCode1 | A | 0 | STRING | Y | | 49/BL | charge_code_01 |
| 9 | FHDSGP | DSGP | DispatchGrp | A | 0 | STRING | Y | | 41B/DG | dispatch_grp |
| 59 | FHFRTH | FRTH | FreightHandlingCode | A | 0 | STRING | Y | | 42/FR | freight_handling_code |
| 57 | FHZON | ZON | ZoneNumber | A | 0 | STRING | Y | | 40/ZN | zone_number |
| 17 | FHMOT | MOT | ModeOfTransport | A | 0 | STRING | Y | | 00/TM | mode_of_transport |
| 55 | FHADDS | ADDS | State | A | 0 | STRING | Y | | 00/S | state |
| 78 | FHADSO | ADSO | OriginState | A | 0 | STRING | Y | | 00/S | origin_state |
| 80 | FHCTRO | CTRO | OriginCountry | A | 0 | STRING | Y | | 00/CN | origin_country |
| 30 | FHUOM | UOM | UnitOfMeasureAsInput | A | 0 | STRING | Y | | 00/UM | uom_as_input |
| 21 | FHWTUM | WTUM | WeightUnitOfMeasure | A | 0 | STRING | Y | | 00/UM | weight_uom |
| 23 | FHVLUM | VLUM | VolumeUnitOfMeasure | A | 0 | STRING | Y | | 00/UM | volume_uom |
| 43 | FHODCT | ODCT | OriginalDocumentType | A | 0 | STRING | Y | | 00/DT | original_document_type |
| 39 | FHDCTO | DCTO | OrderType | A | 0 | STRING | Y | | 00/DT | order_type |
| 47 | FHDCT | DCT | DocumentType | A | 0 | STRING | Y | | 00/DT | document_type |
| 76 | FHEXR1 | EXR1 | TaxExplanationCode1 | A | 0 | STRING | Y | | 00/EX | tax_explanation_code_01 |
| 51 | FHREFQ | REFQ | RefNumberQualifier | A | 0 | STRING | Y | | 41/X6 | ref_number_qualifier |
| 28 | FHRTGB | RTGB | RateBasisFreight | A | 0 | STRING | Y | | 49/CD | rate_basis_freight |
| 15 | FHOVFG | OVFG | OverrideFlag | A | 0 | STRING | Y | | 49/BT | override_flag |
| 58 | FHCZON | CZON | CarrierZone | A | 0 | STRING | Y | | 49/CF | carrier_zone |
| 25 | FHRTNM | RTNM | RateName | A | 0 | STRING | Y | | 49/CN | rate_name |
| 24 | FHFRSC | FRSC | FreightRateSchedule | A | 0 | STRING | Y | | 49/BK | freight_rate_schedule |
| 8 | FHNMFC | NMFC | FreightClassificationNMF | A | 0 | STRING | Y | | 49/BE | freight_classification_nmf |
| 1 | FHUK01 | UK01 | UniqueKeyID01 | P | 0 | BIGINT | N | PK | | unique_key_id_001 ⚠️ (should _01) |
| 52 | FHREFN | REFN | ReferenceNumber | O | 0 | STRING | Y | | | reference_number |
| 77 | FHCTYO | CTYO | OriginCity | O | 0 | STRING | Y | | | origin_city |
| 54 | FHCTY1 | CTY1 | City | O | 0 | STRING | Y | | | city |
| 67 | FHURRF | URRF | UserReservedReference | A | 0 | STRING | Y | | | user_reserved_reference |
| 88 | FHCNMR | CNMR | ContractNumber9 | A | 0 | STRING | Y | | | contract_number_09 |
| 74 | FHANI | ANI | AcctNoInputMode | A | 0 | STRING | Y | | | acct_no_input_mode |
| 10 | FHFRT1 | FRT1 | FreightCategoryCode1 | A | 0 | STRING | Y | | | freight_category_code_01 |
| 11 | FHFRT2 | FRT2 | FreightCategoryCode2 | A | 0 | STRING | Y | | | freight_category_code_02 |
| 63 | FHURCD | URCD | UserReservedCode | A | 0 | STRING | Y | | | user_reserved_code |
| 50 | FHVINV | VINV | VendorInvoiceNumber | A | 0 | STRING | Y | | | vendor_invoice_number |
| 44 | FHOKCO | OKCO | CompanyKeyOriginal | A | 0 | STRING | Y | | | company_key_original |
| 48 | FHKCO | KCO | CompanyKey | A | 0 | STRING | Y | | | company_key |
| 40 | FHKCOO | KCOO | CompanyKeyOrderNo | A | 0 | STRING | Y | | | company_key_order_no |
| 56 | FHADDZ | ADDZ | ZipCodePostal | A | 0 | STRING | Y | | | zip_code_postal |
| 79 | FHADZO | ADZO | OriginPostalCode | A | 0 | STRING | Y | | | origin_postal_code |
| 4 | FHVMCU | VMCU | CostCenterTrip | A | 0 | STRING | Y | | | cost_center_trip |
| 13 | FHNMCU | NMCU | CostCenterOrigin | A | 0 | STRING | Y | | | cost_center_origin |
| 81 | FHSCT1 | SCT1 | ShipmentCategoryCode1 | A | 0 | STRING | Y | | | shipment_category_code_01 |
| 85 | FHSC2O | SC2O | OriginCategoryCode2 | A | 0 | STRING | Y | | | origin_category_code_02 |
| 73 | FHFRSN | FRSN | FreightAdjustmentReasonCode | A | 0 | STRING | Y | | | freight_adjustment_reason_code |
| 83 | FHSCT3 | SCT3 | ShipmentCategoryCode3 | A | 0 | STRING | Y | | | shipment_category_code_03 |
| 35 | FHCRDC | CRDC | CurrencyCodeTo | A | 0 | STRING | Y | | | currency_code_to |
| 82 | FHSCT2 | SCT2 | ShipmentCategoryCode2 | A | 0 | STRING | Y | | | shipment_category_code_02 |
| 86 | FHSC3O | SC3O | OriginCategoryCode3 | A | 0 | STRING | Y | | | origin_category_code_03 |
| 84 | FHSC1O | SC1O | OriginCategoryCode1 | A | 0 | STRING | Y | | | origin_category_code_01 |
| 37 | FHCRCD | CRCD | CurrencyCodeFrom | A | 0 | STRING | Y | | | currency_code_from |
| 68 | FHUSER | USER | UserId | A | 0 | STRING | Y | | | user_id |
| 69 | FHPID | PID | ProgramId | A | 0 | STRING | Y | | | program_id |
| 70 | FHJOBN | JOBN | WorkStationId | A | 0 | STRING | Y | | | work_station_id |
| 75 | FHTXA1 | TXA1 | TaxArea1 | A | 0 | STRING | Y | | | tax_area_01 |
| 16 | FHOVFH | OVFH | OverrideFlagFrtAuditHist | A | 0 | STRING | Y | | | override_flag_frt_audit_hist |
| 14 | FHBLPB | BLPB | BillablePayable | A | 0 | STRING | Y | | | billable_payable |
| 19 | FHCAMD | CAMD | CarrierAuditMode | A | 0 | STRING | Y | | | carrier_audit_mode |
| 91 | FHTX | TX | TaxableYN1 | A | 0 | STRING | Y | | | taxable_yn_01 |
| 92 | FHOVRTAX | OVRTAX | TaxOverriden | A | 0 | STRING | Y | | | tax_overriden |
| 3 | FHRSSN | RSSN | RoutingStepNumber | S | 1 | DOUBLE | Y | | | routing_step_number |
| 20 | FHWGTS | WGTS | ShipmentWeight | P | 4 | DOUBLE | Y | | | shipment_weight |
| 29 | FHRTDQ | RTDQ | RatedQuantity | P | 4 | DOUBLE | Y | | | rated_quantity |
| 22 | FHSCVL | SCVL | ScheduledLoadVolume | P | 3 | DOUBLE | Y | | | scheduled_load_volume |
| 41 | FHLNID | LNID | LineNumber | P | 3 | DOUBLE | Y | | | line_number |
| 65 | FHURAT | URAT | UserReservedAmount | P | 2 | DOUBLE | Y | | | user_reserved_amount |
| 36 | FHNAMF | NAMF | AmountNetForeign | P | 2 | DOUBLE | Y | | | amount_net_foreign |
| 94 | FHCTXA | CTXA | ForeignTaxableAmount | P | 2 | DOUBLE | Y | | | foreign_taxable_amount |
| 33 | FHFAA | FAA | AmountForeign | P | 2 | DOUBLE | Y | | | amount_foreign |
| 90 | FHCTAM | CTAM | ForeignTaxAmount | P | 2 | DOUBLE | Y | | | foreign_tax_amount |
| 89 | FHSTAM | STAM | AmtTax2 | P | 2 | DOUBLE | Y | | | amt_tax_02 |
| 93 | FHATXA | ATXA | AmountTaxable | P | 2 | DOUBLE | Y | | | amount_taxable |
| 34 | FHNAMT | NAMT | NetAmount | P | 2 | DOUBLE | Y | | | net_amount |
| 32 | FHAG | AG | AmountGross | P | 2 | DOUBLE | Y | | | amount_gross |
| 61 | FHADDJ | ADDJ | ActualShipDate | S | 0 | INT | Y | | | actual_ship_date |
| 49 | FHDGJ | DGJ | DateForGLandVoucherJULIA | S | 0 | INT | Y | | | date_for_g_land_voucher_julian ⚠️ (gl_and) |
| 64 | FHURDT | URDT | UserReservedDate | S | 0 | INT | Y | | | user_reserved_date |
| 71 | FHUPMJ | UPMJ | DateUpdated | S | 0 | INT | Y | | | date_updated |
| 72 | FHTDAY | TDAY | TimeOfDay | P | 0 | BIGINT | Y | | | time_of_day |
| 62 | FHUKID | UKID | UniqueKeyIDInternal | P | 0 | BIGINT | Y | | | unique_key_id_internal |
| 7 | FHOSEQ | OSEQ | SequenceNumber | S | 0 | BIGINT | Y | | | sequence_number |
| 5 | FHLDNM | LDNM | LoadNumber | S | 0 | BIGINT | Y | | | load_number |
| 26 | FHSDSQ | SDSQ | ScheduleSequenceNumber | S | 0 | BIGINT | Y | | | schedule_sequence_number |
| 27 | FHSCSN | SCSN | SecondaryScheduleSequenceNumber | S | 0 | BIGINT | Y | | | secondary_schedule_sequence_number |
| 12 | FHORGN | ORGN | OriginAddressNumber | S | 0 | BIGINT | Y | | | origin_address_number |
| 46 | FHDOC | DOC | DocVoucherInvoiceE | S | 0 | BIGINT | Y | | | doc_voucher_invoice_e |
| 6 | FHDLNO | DLNO | DeliveryNumberA | S | 0 | BIGINT | Y | | | delivery_number_a |
| 87 | FHLNMB | LNMB | LegNumber | S | 0 | BIGINT | Y | | | leg_number |
| 53 | FHSHAN | SHAN | AddressNumberShipTo | S | 0 | BIGINT | Y | | | address_number_ship_to |
| 60 | FHRTN | RTN | RouteNumber | S | 0 | BIGINT | Y | | | route_number |
| 2 | FHSHPN | SHPN | ShipmentNumber | S | 0 | BIGINT | Y | | | shipment_number |
| 45 | FHJELN | JELN | JournalEntryLineNo | S | 0 | BIGINT | Y | | | journal_entry_line_no |
| 38 | FHDOCO | DOCO | DocumentOrderInvoiceE | S | 0 | BIGINT | Y | | | document_order_invoice_e |
| 18 | FHCARS | CARS | Carrier | S | 0 | BIGINT | Y | | | carrier |
| 42 | FHODOC | ODOC | OriginalDocumentNo | S | 0 | BIGINT | Y | | | original_document_no |
| 66 | FHURAB | URAB | UserReservedNumber | S | 0 | BIGINT | Y | | | user_reserved_number |

## F5642B11 — Custom Transportation (shipment line)
**PK / grain:** `AKSHPN + AKDOCO + AKDCTO + AKLNID + AKKCOO` (index `F5642B11_0`) — order-line + shipment. Custom US Silica table; report payload = `AK55SELN` (seal no). **54 columns.**

| col_id | column | alias | friendly_label | jde | dec | fabric | null | key | udc | snake_case |
|---|---|---|---|---|---|---|---|---|---|---|
| 8 | AKLTTR | LTTR | StatusCodeLast | A | 0 | STRING | Y | | 40/AT | status_code_last |
| 10 | AKNXTR | NXTR | StatusCodeNext | A | 0 | STRING | Y | | 40/AT | status_code_next |
| 15 | AKSSTS | SSTS | ShipmentStatus | A | 0 | STRING | Y | | 41/SS | shipment_status |
| 3 | AKDCTO | DCTO | OrderType | A | 0 | STRING | N | PK | 00/DT | order_type |
| 5 | AKLNID | LNID | LineNumber | P | 3 | DOUBLE | N | PK | | line_number |
| 2 | AKDOCO | DOCO | DocumentOrderInvoiceE | S | 0 | BIGINT | N | PK | | document_order_invoice_e |
| 1 | AKSHPN | SHPN | ShipmentNumber | S | 0 | BIGINT | N | PK | | shipment_number |
| 4 | AKKCOO | KCOO | CompanyKeyOrderNo | A | 0 | STRING | N | PK | | company_key_order_no |
| 12 | AK55SELN | 55SELN | SealNo | A | 0 | STRING(NVARCHAR2 2000) | Y | | | seal_no |
| 13 | AK55PDCD | 55PDCD | ProductionCode | A | 0 | STRING(2000) | Y | | | production_code |
| 29 | AKCPASK | CPASK | ask | A | 0 | STRING(2000) | Y | | | ask ⚠️ (placeholder label) |
| 28 | AKCPACT | CPACT | action | A | 0 | STRING(2000) | Y | | | action ⚠️ (placeholder label) |
| 14 | AK55PDSHNT | 55PDSHNT | ProductionShipNotes | A | 0 | STRING(2000) | Y | | | production_ship_notes |
| 27 | AKC75RDSC | C75RDSC | RecordDescription | A | 0 | STRING(2000) | Y | | | record_description |
| 30 | AKURLDATA | URLDATA | URLData | A | 0 | STRING(2000) | Y | | | url_data |
| 41 | AKDSC2 | DSC2 | DescriptionLine2 | O | 0 | STRING | Y | | | description_line_02 |
| 39 | AKDSC | DSC | DescriptionUM | O | 0 | STRING | Y | | | description_um |
| 9 | AKSTDS | STDS | DescriptionStatus | O | 0 | STRING | Y | | | description_status |
| 11 | AKDL0 | DL0 | DescriptionParent | O | 0 | STRING | Y | | | description_parent |
| 44 | AKDL011 | DL011 | Description011 | O | 0 | STRING | Y | | | description_011 |
| 34 | AKMNRA | MNRA | MinorAccount | A | 0 | STRING | Y | | | minor_account |
| 33 | AKMJRA | MJRA | MajorAccount | A | 0 | STRING | Y | | | major_account |
| 43 | AKDL010 | DL010 | Description010 | A | 0 | STRING | Y | | | description_010 |
| 7 | AKCNID | CNID | ContainerID | A | 0 | STRING | Y | | | container_id |
| 6 | AKLOTN | LOTN | Lot | A | 0 | STRING | Y | | | lot |
| 42 | AKDL01 | DL01 | Description001 | A | 0 | STRING | Y | | | description_001 |
| 40 | AKDSC1 | DSC1 | DescriptionLine1 | A | 0 | STRING | Y | | | description_line_01 |
| 19 | AK55VGUM | 55VGUM | VGMUOM | A | 0 | STRING | Y | | | vgmuom ⚠️ (vgm_uom) |
| 17 | AK55EQUM | 55EQUM | EquipmentUOM | A | 0 | STRING | Y | | | equipment_uom |
| 20 | AKUSER | USER | UserId | A | 0 | STRING | Y | | | user_id |
| 21 | AKPID | PID | ProgramId | A | 0 | STRING | Y | | | program_id |
| 22 | AKJOBN | JOBN | WorkStationId | A | 0 | STRING | Y | | | work_station_id |
| 36 | AKEV02 | EV02 | EverestEventPoint02 | A | 0 | STRING | Y | | | everest_event_point_02 |
| 35 | AKEV01 | EV01 | EverestEventPoint01 | A | 0 | STRING | Y | | | everest_event_point_01 |
| 37 | AKEV03 | EV03 | EverestEventPoint03 | A | 0 | STRING | Y | | | everest_event_point_03 |
| 38 | AKEV04 | EV04 | EverestEventPoint04 | A | 0 | STRING | Y | | | everest_event_point_04 |
| 16 | AK55EQWT | 55EQWT | EquipmentWeight(Manual) | S | 4 | DOUBLE | Y | | | equipment_weight_manual |
| 18 | AK55VGM | 55VGM | VGM(Manual) | P | 4 | DOUBLE | Y | | | vgm_manual |
| 26 | AKDFDR | DFDR | DailyRate | P | 4 | DOUBLE | Y | | | daily_rate |
| 47 | AKMATH03 | MATH03 | MathNumeric03 | P | 2 | DOUBLE | Y | | | math_numeric_03 |
| 45 | AKMATH01 | MATH01 | MathNumeric01 | P | 2 | DOUBLE | Y | | | math_numeric_01 |
| 46 | AKMATH02 | MATH02 | MathNumeric02 | P | 2 | DOUBLE | Y | | | math_numeric_02 |
| 48 | AKMATH04 | MATH04 | MathNumeric04 | P | 2 | DOUBLE | Y | | | math_numeric_04 |
| 49 | AKMATH05 | MATH05 | MathNumeric05 | P | 2 | DOUBLE | Y | | | math_numeric_05 |
| 31 | AKTRDJ | TRDJ | DateTransactionJulian | S | 0 | INT | Y | | | date_transaction_julian |
| 54 | AKDATE04 | DATE04 | Date04 | S | 0 | INT | Y | | | date_04 |
| 53 | AKDATE03 | DATE03 | Date03 | S | 0 | INT | Y | | | date_03 |
| 32 | AKIVD | IVD | DateInvoiceJulian | S | 0 | INT | Y | | | date_invoice_julian |
| 52 | AKDATE02 | DATE02 | Date 02 | S | 0 | INT | Y | | | date_02 |
| 51 | AKDATE01 | DATE01 | Date01 | S | 0 | INT | Y | | | date_01 |
| 24 | AKUPMJ | UPMJ | DateUpdated | S | 0 | INT | Y | | | date_updated |
| 50 | AKMATH06 | MATH06 | MathNumeric06 | P | 0 | BIGINT | Y | | | math_numeric_06 |
| 25 | AK79AGSNO | 79AGSNO | SpecialNumber | P | 0 | BIGINT | Y | | | special_number |
| 23 | AKUPMT | UPMT | TimeLastUpdated | S | 0 | BIGINT | Y | | | time_last_updated |

## F5642B01 — Custom Transportation (booking)
**PK / grain:** `BASHPN + BADOCO + BADCTO + BAKCOO` (index `F5642B01_0`) — booking (no line number). Custom US Silica table; report payload = `BA55DSTPT` (destination port → `F0101_1`). Many wide `NVARCHAR2(2000–3000)` text cols → project only what's needed. **92 columns.**

| col_id | column | alias | friendly_label | jde | dec | fabric | null | key | udc | snake_case |
|---|---|---|---|---|---|---|---|---|---|---|
| 26 | BA55INDLT | 55INDLT | InlandDelterms | A | 0 | STRING | Y | | 42/FR | inland_delterms |
| 17 | BA55RLL | 55RLL | Roll | A | 0 | STRING | Y | | 87/S1 | roll |
| 3 | BADCTO | DCTO | OrderType | A | 0 | STRING | N | PK | 00/DT | order_type |
| 2 | BADOCO | DOCO | DocumentOrderInvoiceE | S | 0 | BIGINT | N | PK | | document_order_invoice_e |
| 1 | BASHPN | SHPN | ShipmentNumber | S | 0 | BIGINT | N | PK | | shipment_number |
| 4 | BAKCOO | KCOO | CompanyKeyOrderNo | A | 0 | STRING | N | PK | | company_key_order_no |
| 62 | BAGPTX | GPTX | Text | O | 0 | STRING(NVARCHAR2 3000) | Y | | | text |
| 64 | BAOLST | OLST | ListofOptionNumbers | A | 0 | STRING(2200) | Y | | | listof_option_numbers ⚠️ (list_of) |
| 63 | BAVTXX | VTXX | TextDisplayArray | A | 0 | STRING(2400) | Y | | | text_display_array |
| 61 | BAEXPL | EXPL | ExplanationFull | O | 0 | STRING(2400) | Y | | | explanation_full |
| 36 | BA55REMK | 55REMK | Remarks | A | 0 | STRING(3000) | Y | | | remarks |
| 35 | BA55ALNO | 55ALNO | AlsoNotify | A | 0 | STRING(3000) | Y | | | also_notify |
| 24 | BA55ROUT | 55ROUT | RoutingNotes | A | 0 | STRING(3000) | Y | | | routing_notes |
| 34 | BA55NOTI | 55NOTI | Notify | A | 0 | STRING(3000) | Y | | | notify |
| 39 | BA55CSE | 55CSE | Consignee | A | 0 | STRING(2000) | Y | | | consignee |
| 29 | BA55BOLC | 55BOLC | BoLContact | A | 0 | STRING(2000) | Y | | | bo_l_contact ⚠️ (bol_contact) |
| 42 | BA55INSUP | 55INSUP | InsurancePaidBy | A | 0 | STRING(2000) | Y | | | insurance_paid_by |
| 25 | BA55MKS | 55MKS | Marks | A | 0 | STRING(2000) | Y | | | marks |
| 10 | BA55INCO | 55INCO | Incoterms | A | 0 | STRING(2000) | Y | | | incoterms |
| 38 | BA55SHP | 55SHP | Shipper | A | 0 | STRING(2000) | Y | | | shipper |
| 40 | BA55DORE | 55DORE | DocRequired | A | 0 | STRING(2000) | Y | | | doc_required |
| 79 | BADSC2 | DSC2 | DescriptionLine2 | O | 0 | STRING | Y | | | description_line_02 |
| 77 | BADSC | DSC | DescriptionUM | O | 0 | STRING | Y | | | description_um |
| 82 | BADL011 | DL011 | Description011 | O | 0 | STRING | Y | | | description_011 |
| 50 | BA55REF1 | 55REF1 | Reference1 | A | 0 | STRING | Y | | | reference_01 |
| 53 | BA55REF4 | 55REF4 | Reference4 | A | 0 | STRING | Y | | | reference_04 |
| 70 | BAMJRA | MJRA | MajorAccount | A | 0 | STRING | Y | | | major_account |
| 54 | BA55REF5 | 55REF5 | Reference5 | A | 0 | STRING | Y | | | reference_05 |
| 51 | BA55REF2 | 55REF2 | Reference2 | A | 0 | STRING | Y | | | reference_02 |
| 52 | BA55REF3 | 55REF3 | Reference3 | A | 0 | STRING | Y | | | reference_03 |
| 71 | BAMNRA | MNRA | MinorAccount | A | 0 | STRING | Y | | | minor_account |
| 14 | BA55VLNO | 55VLNO | VesselName | A | 0 | STRING | Y | | | vessel_name |
| 81 | BADL010 | DL010 | Description010 | A | 0 | STRING | Y | | | description_010 |
| 9 | BA55BKSTAT | 55BKSTAT | Bookingstatus | A | 0 | STRING | Y | | | bookingstatus |
| 8 | BA55BKNO | 55BKNO | BookingNo | A | 0 | STRING | Y | | | booking_no |
| 12 | BA55VONO | 55VONO | VoyageNo | A | 0 | STRING | Y | | | voyage_no |
| 80 | BADL01 | DL01 | Description001 | A | 0 | STRING | Y | | | description_001 |
| 78 | BADSC1 | DSC1 | DescriptionLine1 | A | 0 | STRING | Y | | | description_line_01 |
| 83 | BADATE | DATE | SystemDate | A | 0 | STRING | Y | | | system_date ⚠️ (date stored as alpha) |
| 55 | BAUSER | USER | UserId | A | 0 | STRING | Y | | | user_id |
| 56 | BAPID | PID | ProgramId | A | 0 | STRING | Y | | | program_id |
| 57 | BAJOBN | JOBN | WorkStationId | A | 0 | STRING | Y | | | work_station_id |
| 15 | BA55EQTY | 55EQTY | EquipmentType | A | 0 | STRING | Y | | | equipment_type |
| 13 | BA55OCDLT | 55OCDLT | OceanDelTerms | A | 0 | STRING | Y | | | ocean_del_terms |
| 20 | BA55CT | 55CT | CT | A | 0 | STRING | Y | | | ct ⚠️ (placeholder label) |
| 16 | BA55ABBK | 55ABBK | ActiononBuycoBooking | A | 0 | STRING | Y | | | actionon_buyco_booking ⚠️ (action_on) |
| 18 | BA55ADVN | 55ADVN | Advance | A | 0 | STRING | Y | | | advance |
| 72 | BAEV01 | EV01 | EverestEventPoint01 | A | 0 | STRING | Y | | | everest_event_point_01 |
| 75 | BAEV04 | EV04 | EverestEventPoint04 | A | 0 | STRING | Y | | | everest_event_point_04 |
| 31 | BA55CBXOD | 55CBXOD | Checkbox-OrderReference | A | 0 | STRING | Y | | | check_box_order_reference |
| 76 | BAEV05 | EV05 | EverestEventPoint05 | A | 0 | STRING | Y | | | everest_event_point_05 |
| 28 | BA55CBXRV | 55CBXRV | CheckBoxforRevision | A | 0 | STRING | Y | | | check_boxfor_revision ⚠️ (check_box_for) |
| 73 | BAEV02 | EV02 | EverestEventPoint02 | A | 0 | STRING | Y | | | everest_event_point_02 |
| 19 | BA55HLD | 55HLD | Hold | A | 0 | STRING | Y | | | hold |
| 74 | BAEV03 | EV03 | EverestEventPoint03 | A | 0 | STRING | Y | | | everest_event_point_03 |
| 27 | BA55CKBT | 55CKBT | CheckBoxforBT | P | 2 | DOUBLE | Y | | | check_boxfor_bt ⚠️ (check_box_for_bt) |
| 89 | BAMATH03 | MATH03 | MathNumeric03 | P | 2 | DOUBLE | Y | | | math_numeric_03 |
| 87 | BAMATH01 | MATH01 | MathNumeric01 | P | 2 | DOUBLE | Y | | | math_numeric_01 |
| 90 | BAMATH04 | MATH04 | MathNumeric04 | P | 2 | DOUBLE | Y | | | math_numeric_04 |
| 88 | BAMATH02 | MATH02 | MathNumeric02 | P | 2 | DOUBLE | Y | | | math_numeric_02 |
| 91 | BAMATH05 | MATH05 | MathNumeric05 | P | 2 | DOUBLE | Y | | | math_numeric_05 |
| 68 | BATRDJ | TRDJ | DateTransactionJulian | S | 0 | INT | Y | | | date_transaction_julian |
| 32 | BADRQJ | DRQJ | DateRequestedJulian | S | 0 | INT | Y | | | date_requested_julian |
| 46 | BAADDJ | ADDJ | ActualShipDate | S | 0 | INT | Y | | | actual_ship_date |
| 33 | BAPPDJ | PPDJ | DatePromisedShipJu | S | 0 | INT | Y | | | date_promised_ship_julian |
| 48 | BALOAD | LOAD | DateLoaded | S | 0 | INT | Y | | | date_loaded |
| 43 | BADEPU | DEPU | DateEarliestPickup | S | 0 | INT | Y | | | date_earliest_pickup |
| 85 | BADATE02 | DATE02 | Date 02 | S | 0 | INT | Y | | | date_02 |
| 86 | BADATE03 | DATE03 | Date03 | S | 0 | INT | Y | | | date_03 |
| 49 | BARQSJ | RQSJ | DateRequestedShip | S | 0 | INT | Y | | | date_requested_ship |
| 47 | BARSDJ | RSDJ | DateReleaseJulian | S | 0 | INT | Y | | | date_release_julian |
| 45 | BADLDL | DLDL | DateLatestDelivery | S | 0 | INT | Y | | | date_latest_delivery |
| 84 | BADATE01 | DATE01 | Date01 | S | 0 | INT | Y | | | date_01 |
| 69 | BAIVD | IVD | DateInvoiceJulian | S | 0 | INT | Y | | | date_invoice_julian |
| 41 | BADLPU | DLPU | DateLatestPickup | S | 0 | INT | Y | | | date_latest_pickup |
| 44 | BADEDL | DEDL | DateEarliestDelivery | S | 0 | INT | Y | | | date_earliest_delivery |
| 59 | BAUPMJ | UPMJ | DateUpdated | S | 0 | INT | Y | | | date_updated |
| 66 | BALPNN | LPNN | mnLPNextNumber | P | 0 | BIGINT | Y | | | mn_lp_next_number |
| 60 | BALPFN | LPFN | mnLPFromNextNumber | P | 0 | BIGINT | Y | | | mn_lp_from_next_number |
| 22 | BA55HSHPN2 | 55HSHPN2 | HookShipmentNumber2 | P | 0 | BIGINT | Y | | | hook_shipment_number_02 |
| 92 | BAMATH06 | MATH06 | MathNumeric06 | P | 0 | BIGINT | Y | | | math_numeric_06 |
| 65 | BALPINC | LPINC | mnLPIncrement | P | 0 | BIGINT | Y | | | mn_lp_increment |
| 21 | BA55HSHPN1 | 55HSHPN1 | HookShipmentNumber1 | P | 0 | BIGINT | Y | | | hook_shipment_number_01 |
| 67 | BALPTN | LPTN | mnLPToNextNumber | P | 0 | BIGINT | Y | | | mn_lp_to_next_number |
| 7 | BA55INCR | 55INCR | InlandCarrier | S | 0 | BIGINT | Y | | | inland_carrier |
| 30 | BA55ODREF | 55ODREF | OrderReference | S | 0 | BIGINT | Y | | | order_reference |
| 23 | BA55HSHPN3 | 55HSHPN3 | HookShipmentNumber3 | S | 0 | BIGINT | Y | | | hook_shipment_number_03 |
| 5 | BA55LODP | 55LODP | LoadingPort | S | 0 | BIGINT | Y | | | loading_port |
| 37 | BA55NCON | 55NCON | No.ofContainer | S | 0 | BIGINT | Y | | | no_of_container |
| 6 | BA55OCCR | 55OCCR | OceanCarrier | S | 0 | BIGINT | Y | | | ocean_carrier |
| 11 | BA55DSTPT | 55DSTPT | DestinationPort | S | 0 | BIGINT | Y | | | destination_port |
| 58 | BAUPMT | UPMT | TimeLastUpdated | S | 0 | BIGINT | Y | | | time_last_updated |

## F4941 — Shipment Routing Steps
**PK / grain:** natural key `RSSHPN + RSRSSN` ⚠️ (**not flagged**) — shipment + routing step (one shipment → many steps). Spec's "Route Number" = `RSRTN`. Shipment-grain → dedup before relating to fact. **91 columns.**

| col_id | column | alias | friendly_label | jde | dec | fabric | null | key | udc | snake_case |
|---|---|---|---|---|---|---|---|---|---|---|
| 7 | RSROUT | ROUT | RouteCode | A | 0 | STRING | Y | | 42/RT | route_code |
| 3 | RSMOT | MOT | ModeOfTransport | A | 0 | STRING | Y | | 00/TM | mode_of_transport |
| 32 | RSUMD1 | UMD1 | UnitofMeasureDistance | A | 0 | STRING | Y | | 00/UM | uom_distance |
| 22 | RSVLUM | VLUM | VolumeUnitOfMeasure | A | 0 | STRING | Y | | 00/UM | volume_uom |
| 27 | RSLUOM | LUOM | UnitofMeasureLinear | A | 0 | STRING | Y | | 00/UM | uom_linear |
| 20 | RSWTUM | WTUM | WeightUnitOfMeasure | A | 0 | STRING | Y | | 00/UM | weight_uom |
| 35 | RSUM | UM | UnitOfMeasure | A | 0 | STRING | Y | | 00/UM | uom |
| 53 | RSREFQ | REFQ | RefNumberQualifier | A | 0 | STRING | Y | | 41/X6 | ref_number_qualifier |
| 33 | RSDSRC | DSRC | DistanceSource | A | 0 | STRING | Y | | 49/BC | distance_source |
| 11 | RSCZON | CZON | CarrierZone | A | 0 | STRING | Y | | 49/CF | carrier_zone |
| 10 | RSFRSC | FRSC | FreightRateSchedule | A | 0 | STRING | Y | | 49/BK | freight_rate_schedule |
| 54 | RSREFN | REFN | ReferenceNumber | O | 0 | STRING | Y | | | reference_number |
| 86 | RSURRF | URRF | UserReservedReference | A | 0 | STRING | Y | | | user_reserved_reference |
| 81 | RSCNMR | CNMR | ContractNumber9 | A | 0 | STRING | Y | | | contract_number_09 |
| 77 | RSDKID | DKID | DockID | A | 0 | STRING | Y | | | dock_id |
| 82 | RSURCD | URCD | UserReservedCode | A | 0 | STRING | Y | | | user_reserved_code |
| 12 | RSVMCU | VMCU | CostCenterTrip | A | 0 | STRING | Y | | | cost_center_trip |
| 59 | RSCRCP | CRCP | CurrencyCodeAPAmounts | A | 0 | STRING | Y | | | currency_code_ap_amounts |
| 62 | RSCRDC | CRDC | CurrencyCodeTo | A | 0 | STRING | Y | | | currency_code_to |
| 38 | RSCRCD | CRCD | CurrencyCodeFrom | A | 0 | STRING | Y | | | currency_code_from |
| 87 | RSUSER | USER | UserId | A | 0 | STRING | Y | | | user_id |
| 88 | RSPID | PID | ProgramId | A | 0 | STRING | Y | | | program_id |
| 89 | RSJOBN | JOBN | WorkStationId | A | 0 | STRING | Y | | | work_station_id |
| 79 | RSPRTE | PRTE | ParentRoute | A | 0 | STRING | Y | | | parent_route |
| 56 | RSFRTV | FRTV | VendorFreightCalculatedY | A | 0 | STRING | Y | | | vendor_freight_calculated_y |
| 52 | RSDPCR | DPCR | DocumentPrintControlRequ | A | 0 | STRING | Y | | | document_print_control_requ |
| 50 | RSRRTR | RRTR | RoutingRequiredIndicator | A | 0 | STRING | Y | | | routing_required_indicator |
| 51 | RSRATR | RATR | RatingRequiredIndicator | A | 0 | STRING | Y | | | rating_required_indicator |
| 4 | RSOVRM | OVRM | ModeofTransportOverrideC | A | 0 | STRING | Y | | | modeof_transport_override_c ⚠️ (mode_of_) |
| 6 | RSOVRC | OVRC | CarrierOverrideCode | A | 0 | STRING | Y | | | carrier_override_code |
| 64 | RSIBRS | IBRS | InboundRouteSelected | A | 0 | STRING | Y | | | inbound_route_selected |
| 63 | RSINTF | INTF | FlagInTransit | A | 0 | STRING | Y | | | flag_in_transit |
| 55 | RSFRTD | FRTD | CustomerFreightCalculate | A | 0 | STRING | Y | | | customer_freight_calculate |
| 2 | RSRSSN | RSSN | RoutingStepNumber | S | 1 | DOUBLE | N | (PK)⚠️ | | routing_step_number |
| 60 | RSFRCC | FRCC | AmountCustomerFreightCha | P | 4 | DOUBLE | Y | | | amount_customer_freight_charge |
| 19 | RSWGTS | WGTS | ShipmentWeight | P | 4 | DOUBLE | Y | | | shipment_weight |
| 61 | RSFRCF | FRCF | AmountCustomerFreightFor | P | 4 | DOUBLE | Y | | | amount_customer_freight_for |
| 58 | RSFRVF | FRVF | AmountSupplierFreightCha | P | 4 | DOUBLE | Y | | | amount_supplier_freight_charge |
| 57 | RSFRVC | FRVC | AmountVendorFreightCharg | P | 4 | DOUBLE | Y | | | amount_vendor_freight_charge |
| 21 | RSSCVL | SCVL | ScheduledLoadVolume | P | 3 | DOUBLE | Y | | | scheduled_load_volume |
| 84 | RSURAT | URAT | UserReservedAmount | P | 2 | DOUBLE | Y | | | user_reserved_amount |
| 26 | RSGTHS | GTHS | ShipmentGirth | P | 2 | DOUBLE | Y | | | shipment_girth |
| 30 | RSCCUB | CCUB | CubicContainerSpace | P | 2 | DOUBLE | Y | | | cubic_container_space |
| 25 | RSHGTS | HGTS | ShipmentHeight | P | 2 | DOUBLE | Y | | | shipment_height |
| 39 | RSECST | ECST | AmountExtendedCost | P | 2 | DOUBLE | Y | | | amount_extended_cost |
| 36 | RSAEXP | AEXP | AmountExtendedPrice | P | 2 | DOUBLE | Y | | | amount_extended_price |
| 37 | RSFEA | FEA | AmountForeignExtPrice | P | 2 | DOUBLE | Y | | | amount_foreign_ext_price |
| 24 | RSWTHS | WTHS | ShipmentWidth | P | 2 | DOUBLE | Y | | | shipment_width |
| 23 | RSLGTS | LGTS | ShipmentLength | P | 2 | DOUBLE | Y | | | shipment_length |
| 46 | RSADDJ | ADDJ | ActualShipDate | S | 0 | INT | Y | | | actual_ship_date |
| 40 | RSPPDJ | PPDJ | DatePromisedShipJu | S | 0 | INT | Y | | | date_promised_ship_julian |
| 75 | RSDEDL | DEDL | DateEarliestDelivery | S | 0 | INT | Y | | | date_earliest_delivery |
| 76 | RSDLDL | DLDL | DateLatestDelivery | S | 0 | INT | Y | | | date_latest_delivery |
| 73 | RSDEPU | DEPU | DateEarliestPickup | S | 0 | INT | Y | | | date_earliest_pickup |
| 42 | RSRSDJ | RSDJ | DateReleaseJulian | S | 0 | INT | Y | | | date_release_julian |
| 74 | RSDLPU | DLPU | DateLatestPickup | S | 0 | INT | Y | | | date_latest_pickup |
| 48 | RSDLDT | DLDT | DELIVERY_DATE | S | 0 | INT | Y | | | delivery_date |
| 44 | RSLDDT | LDDT | LoadConfirmDate | S | 0 | INT | Y | | | load_confirm_date |
| 83 | RSURDT | URDT | UserReservedDate | S | 0 | INT | Y | | | user_reserved_date |
| 90 | RSUPMJ | UPMJ | DateUpdated | S | 0 | INT | Y | | | date_updated |
| 91 | RSTDAY | TDAY | TimeOfDay | P | 0 | BIGINT | Y | | | time_of_day |
| 31 | RSDSTN | DSTN | Distance | P | 0 | BIGINT | Y | | | distance |
| 29 | RSNCTR | NCTR | NumberOfContainers | P | 0 | BIGINT | Y | | | number_of_containers |
| 28 | RSNPCS | NPCS | NumberofPirces | P | 0 | BIGINT | Y | | | numberof_pirces ⚠️ (number_of_pieces; src typo) |
| 1 | RSSHPN | SHPN | ShipmentNumber | S | 0 | BIGINT | N | (PK)⚠️ | | shipment_number |
| 49 | RSDLTM | DLTM | TimeDelivery | S | 0 | BIGINT | Y | | | time_delivery |
| 18 | RSANCC | ANCC | AddressNumberDeconsolida | S | 0 | BIGINT | Y | | | address_number_deconsolida |
| 41 | RSPMDT | PMDT | ScheduledShipmentTime | S | 0 | BIGINT | Y | | | scheduled_shipment_time |
| 15 | RSSTSQ | STSQ | StopSequence | S | 0 | BIGINT | Y | | | stop_sequence |
| 67 | RSLALT | LALT | ActualLoadingTime | S | 0 | BIGINT | Y | | | actual_loading_time |
| 47 | RSADTM | ADTM | ActualShipmentTime | S | 0 | BIGINT | Y | | | actual_shipment_time |
| 72 | RSTDLT | TDLT | ThruDeliveryTime | S | 0 | BIGINT | Y | | | thru_delivery_time |
| 71 | RSTDLF | TDLF | FromDeliveryTime | S | 0 | BIGINT | Y | | | from_delivery_time |
| 13 | RSLDNM | LDNM | LoadNumber | S | 0 | BIGINT | Y | | | load_number |
| 17 | RSORGN | ORGN | OriginAddressNumber | S | 0 | BIGINT | Y | | | origin_address_number |
| 68 | RSLAUT | LAUT | ActualUnloadingTime | S | 0 | BIGINT | Y | | | actual_unloading_time |
| 45 | RSLDTM | LDTM | TimeLoad | S | 0 | BIGINT | Y | | | time_load |
| 8 | RSRTN | RTN | RouteNumber | S | 0 | BIGINT | Y | | | route_number |
| 34 | RSELTM | ELTM | TimeElapsed | S | 0 | BIGINT | Y | | | time_elapsed |
| 43 | RSRSDT | RSDT | PromisedDeliveryTime | S | 0 | BIGINT | Y | | | promised_delivery_time |
| 9 | RSDLNO | DLNO | DeliveryNumberA | S | 0 | BIGINT | Y | | | delivery_number_a |
| 16 | RSANID | ANID | AddressNumberIntermediat9 | S | 0 | BIGINT | Y | | | address_number_intermedia_t_09 ⚠️ (mangled) |
| 70 | RSTPUT | TPUT | ThruPickupTime | S | 0 | BIGINT | Y | | | thru_pickup_time |
| 14 | RSTRPL | TRPL | TripLegNumber | S | 0 | BIGINT | Y | | | trip_leg_number |
| 65 | RSLSLT | LSLT | ScheduledLoadingTime | S | 0 | BIGINT | Y | | | scheduled_loading_time |
| 78 | RSPRNB | PRNB | ParentRouteNumber | S | 0 | BIGINT | Y | | | parent_route_number |
| 69 | RSTPUF | TPUF | FromPickupTime | S | 0 | BIGINT | Y | | | from_pickup_time |
| 80 | RSLNMB | LNMB | LegNumber | S | 0 | BIGINT | Y | | | leg_number |
| 5 | RSCARS | CARS | Carrier | S | 0 | BIGINT | Y | | | carrier |
| 66 | RSLSUT | LSUT | ScheduledUnloadingTime | S | 0 | BIGINT | Y | | | scheduled_unloading_time |
| 85 | RSURAB | URAB | UserReservedNumber | S | 0 | BIGINT | Y | | | user_reserved_number |

---

## Cross-cutting validation findings (apply as one ruleset before build)
1. **Grain / fan-out (highest priority):** `F4074` (line→many adjustments), `F4981` (shipment-grain freight), `F41002` (branch `UMMCU` dropped from join), `F0116` (effective-dated `AN8+EFTB`), `F5642B01` (shipment dropped). Pre-aggregate/dedup before joining to the line-grain fact, or tons/freight measures inflate.
2. **Tokenizer bugs (missing word boundary):** `lineof_business`, `basedon_date`, `op_numberof_backorders/substitutes`, `uo_mfor_volumne_incentives`, `numberof_orders`, `date_for_g_land_voucher_julian`, `listof_option_numbers`, `bo_l_contact`, `actionon_buyco_booking`, `check_boxfor_revision/bt`, `vgmuom`, `uom_volume_or_we_i`, `numberof_pirces` (+src typo→pieces), `modeof_transport_override_c`, `address_number_intermedia_t_09`. (Splitter fails only on lowercase "of"/concatenations; CamelCase "Of" splits fine, e.g. `number_of_containers`.)
3. **Zero-pad misfires:** `category_code_address_bk_02` (was 21), `sales_reporting_code_010` / `segment_010` / `unique_key_id_001` (over-padded), `constant_future_use1` (un-padded).
4. **Cross-table inconsistency (same data item):** `identifier_2nd_item` (F4101) vs `identifier_second_item` (F4211); `amt_for_ppu` (F4074) vs `amt_for_price_per_unit` (F4211).
5. **`O` "(unknown)" type:** reproducibly on the 2nd/free-text member of pairs (`DSC2`, `DEL1`, `City`, big `NVARCHAR2`). Generator quirk — fix at source; harmless (→`STRING`).
6. **Standing decisions:** `amt`/`amount`, `dt`/`date`, `num`/`number` abbreviation policy; date `_julian` suffix consistency; PK designation missing on `F0101`/`F0116`.
7. **Placeholder custom labels:** `ask`, `action`, `ct` (F5642B11/B01) — need real names (governance: Andrew/Shweta).
8. **Route Number source:** available in **`F4981.FHRTN`** (already joined — preferred) and **`F4941.RSRTN`** (spec's literal mapping, but a dropped table + shipment-step grain → would need a new fan-out join). Recommend `F4981.FHRTN` pending team confirmation of value parity with `RSRTN`.
9. **TRIM discipline:** UDC codes and fixed-CHAR filters are space-padded (`ALAST` 'FRTHIDE ', `FHVINV` literal `'NULL'`); TRIM before compare.
10. **Implied-decimal keys:** qty `÷10^3`, price `÷10^6`, amount `÷10^2`, cost `÷10^4`, UoM conv `÷10^7`; Julian dates (precision 6) → `julian_to_date()`. Exception: `F5642B01.system_date` is alpha text, **not** Julian.






