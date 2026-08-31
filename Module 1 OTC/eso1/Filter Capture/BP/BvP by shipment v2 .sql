/* =====================================================================
   REPORT   : BP Freight and Fuel  (Shipment / OD Summary)
   SOURCE   : JD Edwards E1, Oracle PRODDTA. Replicates Hubble logic.
   GRAIN    : Shipment (OD level: origin plant -> destination ship-to).
              Item detail is a SEPARATE query; freight is not sliced by item.
   WINDOW   : GL Jul 1 - Aug 20 2026 (Julian 126182..126232). VERIFY.

   BILLABLE FREIGHT (Hubble APMTOT):
     BFRTRANS = F4981 B/BFR carrier bill  (NO invoice gate; JDE blank FHVINV
                passes Hubble's "!= NULL", so gating would wrongly drop rows).
     BFRAPM   = SUM over sales lines of (line tons * F4074 ALFVTR rate),
                freight adjustment types only, DEDUPED to one rate per line
                (F4074 replicates across sub-lines; summing raw double-counts).
                Blank when tons conversion is missing (NEWBOV undefined).
     Billable Freight = BFRTRANS + BFRAPM  (additive; in practice a shipment
                has one source or the other).
   PAYABLE FREIGHT = F4981 P/PFR.  FUEL = FSC/FSB billable, FSC payable.
   Variance/CM% per Hubble calc grid.
   ===================================================================== */
WITH
-- Reporting window as JDE Julian (CYYDDD). 126182=01-Jul-2026, 126232=20-Aug-2026.
params AS (SELECT 126182 AS gl_from, 126232 AS gl_to FROM dual),

-- External customers only (F0101 search type kept, internal SIC 'F' dropped).
addr AS (
    SELECT ab.ABAN8
    FROM   PRODDTA.F0101 ab
    WHERE (ab.ABAT1 BETWEEN 'A' AND 'P' OR ab.ABAT1 BETWEEN 'R' AND 'ZZZ')
      AND  ab.ABSIC <> 'F'
),

-- Sales Order Detail (F4211), one row per shipped line, tons + price resolved.
line_fact AS (
    SELECT
        t.SDSHPN AS shipment_no, t.SDKCOO AS order_company,
        t.SDDOCO AS order_no,    t.SDDCTO AS order_type,
        t.SDLNID / 1000 AS line_no,
        t.SDMCU  AS origin_plant, t.SDAN8 AS bp_sold_to, t.SDSHAN AS ship_to,
        t.SDCARS AS carrier,      t.SDMOT AS mode_of_transport,
        t.SDFRTH AS freight_handling, t.SDDGL AS gl_date_jul,
        TRIM(t.SDUOM)  AS volume_uom,
        t.SDSOQS / 1000 AS qty_shipped,
        -- tons-per-unit factor (match on transaction UoM SDUOM). MULTIPLY qty.
        COALESCE(
            CASE WHEN TRIM(t.SDUOM)='TN' THEN 1 END,
            vd.UMCONV/1e7, 1e7/NULLIF(vi.UMCONV,0),
            fd.UCCONV/1e7, 1e7/NULLIF(fi.UCCONV,0), 1) AS volume_factor,
        -- 'Y' = no tons conversion found (factor fell to 1). Blanks APM billable.
        CASE WHEN TRIM(t.SDUOM)<>'TN'
              AND vd.UMCONV IS NULL AND vi.UMCONV IS NULL
              AND fd.UCCONV IS NULL AND fi.UCCONV IS NULL
             THEN 'Y' ELSE 'N' END AS vol_conv_missing
    FROM PRODDTA.F4211 t
    JOIN addr a ON a.ABAN8 = t.SDSHAN
    CROSS JOIN params p
    LEFT JOIN PRODDTA.F41002 vd ON vd.UMITM=t.SDITM AND vd.UMUM=TRIM(t.SDUOM) AND vd.UMRUM='TN'
    LEFT JOIN PRODDTA.F41002 vi ON vi.UMITM=t.SDITM AND vi.UMUM='TN'          AND vi.UMRUM=TRIM(t.SDUOM)
    LEFT JOIN PRODDTA.F41003 fd ON fd.UCUM=TRIM(t.SDUOM) AND fd.UCRUM='TN'
    LEFT JOIN PRODDTA.F41003 fi ON fi.UCUM='TN'          AND fi.UCRUM=TRIM(t.SDUOM)
    WHERE t.SDDGL BETWEEN p.gl_from AND p.gl_to
      AND t.SDFRTH IN ('DLV','PP') AND t.SDLNTY='S'
      AND t.SDLTTR <> 980 AND t.SDNXTR = 999
      AND t.SDSHPN in ( 15862333, 15896633)
),

-- F4981 transportation actuals at shipment grain. Strict Hubble codes.
-- No invoice gate (blank FHVINV passes Hubble's condition).
freight AS (
    SELECT fc.FHSHPN AS shipment_no,
        -- Billable freight: strict B/BFR only (Hubble BFRTRANS). NOT widened.
        ROUND(SUM(CASE WHEN TRIM(fc.FHBLPB)='B' AND TRIM(fc.FHCGC1)='BFR'
                       THEN fc.FHNAMT*0.01 ELSE 0 END),2) AS bfrtrans,          -- billable freight
        ROUND(SUM(CASE WHEN TRIM(fc.FHBLPB)='B' AND TRIM(fc.FHCGC1) IN ('FSC','FSB')
                       THEN fc.FHNAMT*0.01 ELSE 0 END),2) AS fsbtrans,          -- billable fuel
        ROUND(SUM(CASE WHEN TRIM(fc.FHBLPB)='P' AND TRIM(fc.FHCGC1)='PFR'
                       THEN fc.FHNAMT*0.01 ELSE 0 END),2) AS pfrtrans,          -- payable freight
        ROUND(SUM(CASE WHEN TRIM(fc.FHBLPB)='P' AND TRIM(fc.FHCGC1)='FSC'
                       THEN fc.FHNAMT*0.01 ELSE 0 END),2) AS fsctrans,          -- payable fuel
        MAX(fc.FHCTY1) AS freight_city, MAX(fc.FHADDS) AS freight_state,
        MAX(fc.FHADDZ) AS freight_zip,  MAX(fc.FHFRTH) AS freight_audit_handling_code
    FROM PRODDTA.F4981 fc
    WHERE fc.FHSHPN IN (SELECT DISTINCT shipment_no FROM line_fact)
    GROUP BY fc.FHSHPN
),

-- BFRAPM: F4074 advanced-pricing freight estimate = line tons * lane rate.
-- Dedup: ONE rate per sales line (F4074 replicates across sub-lines), then
-- multiply by that line's tons (from line_fact, already one row per line).
apm_rate AS (
    SELECT a.ALDOCO, a.ALKCOO, a.ALDCTO, a.ALLNID,
           MAX(a.ALFVTR/10000) AS fvtr           -- one EPDELFRT rate per line
    FROM   PRODDTA.F4074 a
    WHERE  TRIM(a.ALAST) = 'EPDELFRT'            -- base delivered freight only
    GROUP  BY a.ALDOCO, a.ALKCOO, a.ALDCTO, a.ALLNID
),
billable_apm AS (
    SELECT lf.shipment_no,
        -- blank the APM when tons conversion is missing (Hubble NEWBOV rule)
        ROUND(SUM(CASE WHEN lf.vol_conv_missing='N'
                       THEN lf.qty_shipped * lf.volume_factor * ar.fvtr
                       ELSE 0 END),2) AS bfrapm
    FROM   line_fact lf
    JOIN   apm_rate ar
           ON  ar.ALDOCO = lf.order_no  AND ar.ALKCOO = lf.order_company
           AND ar.ALDCTO = lf.order_type AND ar.ALLNID = lf.line_no*1000
    GROUP  BY lf.shipment_no
),

-- Ship-to geography (destination).
dest AS (
    SELECT ALAN8, ALCTY1 AS city, ALADDS AS state, ALADDZ AS zip, ALCTR AS country
    FROM PRODDTA.F0116
)

-- FINAL: one row per shipment. Tons SUM across lines; freight is shipment-grain
-- so MAX returns the single value. Billable = BFRTRANS + BFRAPM.
SELECT
    shipment_no, order_company, order_no, order_type,
    origin_plant, bp_sold_to, ship_to,
    ship_to_city, ship_to_state, ship_to_zip, ship_to_country,
    carrier, mode_of_transport, freight_handling, gl_date,
    SUM(qty_shipped) AS qty_shipped,
    SUM(volume_tons) AS volume_tons,
    -- Billable Freight (Total) = transportation actual + APM estimate
    MAX(billable_freight) AS billable_freight,
    MAX(bfrtrans)  AS billable_freight_trans,   -- F4981 component
    MAX(bfrapm)    AS billable_freight_apm,      -- F4074 component
    MAX(fsbtrans)  AS billable_fuel,
    MAX(pfrtrans)  AS payable_freight,
    MAX(fsctrans)  AS payable_fuel,
    MAX(total_billable) AS total_billable,
    MAX(total_payable)  AS total_payable,
    MAX(freight_variance) AS freight_variance,
    MAX(total_variance)   AS total_variance,
    ROUND(MAX(total_payable) / NULLIF(SUM(volume_tons),0),2) AS payable_per_ton,
    ROUND(MAX(billable_freight) / NULLIF(SUM(volume_tons),0),2) AS billable_per_ton,
    -- Freight CM% = freight variance / billable freight (Hubble FREIGHTCM%)
    CASE WHEN MAX(billable_freight) <> 0
         THEN ROUND(MAX(freight_variance) / MAX(billable_freight),2) END AS freight_cm_pct,
    -- Total CM% = total variance / total billable (Hubble TOTALCM%)
    CASE WHEN MAX(total_billable) <> 0
         THEN ROUND(MAX(total_variance) / MAX(total_billable),2) END AS total_cm_pct
FROM (
    SELECT
        lf.shipment_no, lf.order_company, lf.order_no, lf.order_type, lf.line_no,
        lf.origin_plant, lf.bp_sold_to, lf.ship_to,
        d.city AS ship_to_city, d.state AS ship_to_state, d.zip AS ship_to_zip, d.country AS ship_to_country,
        lf.carrier, lf.mode_of_transport, lf.freight_handling,
        TO_DATE(TO_CHAR(1900000 + lf.gl_date_jul),'YYYYDDD') AS gl_date,
        lf.qty_shipped,
        lf.qty_shipped * lf.volume_factor AS volume_tons,
        NVL(fr.bfrtrans,0) AS bfrtrans,
        NVL(ba.bfrapm,0)   AS bfrapm,
        -- Billable Freight (Total) = BFRTRANS + BFRAPM
        NVL(fr.bfrtrans,0) + NVL(ba.bfrapm,0)                    AS billable_freight,
        NVL(fr.fsbtrans,0) AS fsbtrans,
        NVL(fr.pfrtrans,0) AS pfrtrans,
        NVL(fr.fsctrans,0) AS fsctrans,
        -- Total Billable = billable freight (total) + billable fuel
        NVL(fr.bfrtrans,0) + NVL(ba.bfrapm,0) + NVL(fr.fsbtrans,0) AS total_billable,
        -- Total Payable = payable freight + payable fuel
        NVL(fr.pfrtrans,0) + NVL(fr.fsctrans,0)                    AS total_payable,
        -- Freight Variance = billable freight (total) - payable freight
        (NVL(fr.bfrtrans,0)+NVL(ba.bfrapm,0)) - NVL(fr.pfrtrans,0) AS freight_variance,
        -- Total Variance = total billable - total payable
        (NVL(fr.bfrtrans,0)+NVL(ba.bfrapm,0)+NVL(fr.fsbtrans,0))
          - (NVL(fr.pfrtrans,0)+NVL(fr.fsctrans,0))               AS total_variance,
        fr.freight_city, fr.freight_state, fr.freight_zip, fr.freight_audit_handling_code
    FROM line_fact lf
    LEFT JOIN freight      fr ON fr.shipment_no = lf.shipment_no
    LEFT JOIN billable_apm ba ON ba.shipment_no = lf.shipment_no
    LEFT JOIN dest         d  ON d.ALAN8        = lf.ship_to
)
GROUP BY
    shipment_no, order_company, order_no, order_type,
    origin_plant, bp_sold_to, ship_to,
    ship_to_city, ship_to_state, ship_to_zip, ship_to_country,
    carrier, mode_of_transport, freight_handling, gl_date
ORDER BY origin_plant, shipment_no;