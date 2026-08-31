/* =====================================================================
   REPORT   : Item Detail — Volume (Tons) and Price per Ton
   SOURCE   : JD Edwards E1, Oracle PRODDTA
   PURPOSE  : Drill target for the BP Freight shipment summary. One row per
              sales line, exposing full conversion traceability: for both
              Volume and Price, the source value, source UoM, conversion
              factor, and converted result are all shown (not just the
              converted number), so every tons/price figure is auditable
              back to F4211 and the F41002/F41003 conversion tables.
   GRAIN    : F4211 sales line (shipment, line, item). Freight is NOT here;
              it lives at shipment grain in the summary query.
   WINDOW   : GL Jul 1 - Aug 20 2026 (Julian 126182..126232). VERIFY.
   SCOPE    : Delivered lines only, Last Status <> 980 AND Next Status = 999,
              which also satisfies the "price on delivered lines only" rule.

   CONVERSION RULES:
     Volume tons  = source volume  * volume factor   (MULTIPLY; match SDUOM).
     Price/ton    = source price    / price factor    (DIVIDE;   match SDUOM4).
     Factor cascade: already-TN, F41002 item-specific, F41003 standard, each
     direct then inverse, else default 1 (flagged *_conv_missing = 'Y').
     Hubble uses direct F41002 only with no fallback, so on items missing the
     direct factor this query is intentionally MORE correct than Hubble.
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

-- Sales Order Detail (F4211), one row per shipped line, with both conversion
-- factors resolved and all source inputs retained for transparency.
line_fact AS (
    SELECT
        t.SDSHPN AS shipment_no,          -- Shipment Number (links to summary)
        t.SDLNID / 1000 AS line_no,       -- Line Number (stored x1000)
        t.SDLITM AS item,                 -- Item Number (long)
        t.SDITM  AS short_item,           -- Item Number (short) = F41002 key
        t.SDSRP2 AS major_prod_code,      -- Sales Reporting Code 2
        t.SDSRP4 AS minor_prod_code,      -- Sales Reporting Code 4
        t.SDMCU  AS origin_plant,         -- Branch/Plant = ORIGIN
        t.SDAN8  AS bp_sold_to,           -- Sold-To = BP
        t.SDSHAN AS ship_to,              -- Ship-To = DESTINATION

        -- VOLUME source inputs (from the transactional table)
        t.SDSOQS / 1000 AS source_volume, -- Quantity Shipped (SDSOQS, stored x1000)
        TRIM(t.SDUOM)   AS source_volume_uom,   -- transaction UoM (SDUOM)
        -- VOLUME conversion factor (tons per source unit); MULTIPLY qty by this.
        COALESCE(
            CASE WHEN TRIM(t.SDUOM)='TN' THEN 1 END,   -- already tons
            vd.UMCONV/1e7,                              -- F41002 item, direct
            1e7/NULLIF(vi.UMCONV,0),                    -- F41002 item, inverse
            fd.UCCONV/1e7,                              -- F41003 std, direct
            1e7/NULLIF(fi.UCCONV,0),                    -- F41003 std, inverse
            1) AS volume_conversion_factor,             -- default 1 (flagged)

        -- PRICE source inputs (F4211 only, per the requirement)
        t.SDUPRC / 1000000 AS source_price,     -- Unit Price (SDUPRC, stored x1,000,000)
        TRIM(t.SDUOM4)     AS source_price_uom,  -- pricing UoM (SDUOM4)
        -- PRICE conversion factor (tons per pricing unit); DIVIDE price by this.
        COALESCE(
            CASE WHEN TRIM(t.SDUOM4)='TN' THEN 1 END,
            pd.UMCONV/1e7, 1e7/NULLIF(pi.UMCONV,0),
            gd.UCCONV/1e7, 1e7/NULLIF(gi.UCCONV,0), 1) AS price_conversion_factor,

        -- Data-quality flags: 'Y' = no conversion found, factor defaulted to 1,
        -- so the converted figure is unreliable and the item needs UoM setup.
        CASE WHEN TRIM(t.SDUOM)<>'TN'
              AND vd.UMCONV IS NULL AND vi.UMCONV IS NULL
              AND fd.UCCONV IS NULL AND fi.UCCONV IS NULL
             THEN 'Y' ELSE 'N' END AS vol_conv_missing,
        CASE WHEN TRIM(t.SDUOM4)<>'TN'
              AND pd.UMCONV IS NULL AND pi.UMCONV IS NULL
              AND gd.UCCONV IS NULL AND gi.UCCONV IS NULL
             THEN 'Y' ELSE 'N' END AS price_conv_missing
    FROM PRODDTA.F4211 t
    JOIN addr a ON a.ABAN8 = t.SDSHAN
    CROSS JOIN params p
    -- Volume factor lookups on transaction UoM (SDUOM):
    LEFT JOIN PRODDTA.F41002 vd ON vd.UMITM=t.SDITM AND vd.UMUM=TRIM(t.SDUOM)  AND vd.UMRUM='TN'
    LEFT JOIN PRODDTA.F41002 vi ON vi.UMITM=t.SDITM AND vi.UMUM='TN'           AND vi.UMRUM=TRIM(t.SDUOM)
    LEFT JOIN PRODDTA.F41003 fd ON fd.UCUM=TRIM(t.SDUOM)  AND fd.UCRUM='TN'
    LEFT JOIN PRODDTA.F41003 fi ON fi.UCUM='TN'           AND fi.UCRUM=TRIM(t.SDUOM)
    -- Price factor lookups on pricing UoM (SDUOM4):
    LEFT JOIN PRODDTA.F41002 pd ON pd.UMITM=t.SDITM AND pd.UMUM=TRIM(t.SDUOM4) AND pd.UMRUM='TN'
    LEFT JOIN PRODDTA.F41002 pi ON pi.UMITM=t.SDITM AND pi.UMUM='TN'           AND pi.UMRUM=TRIM(t.SDUOM4)
    LEFT JOIN PRODDTA.F41003 gd ON gd.UCUM=TRIM(t.SDUOM4) AND gd.UCRUM='TN'
    LEFT JOIN PRODDTA.F41003 gi ON gi.UCUM='TN'           AND gi.UCRUM=TRIM(t.SDUOM4)
    WHERE t.SDDGL BETWEEN p.gl_from AND p.gl_to
      AND t.SDFRTH IN ('DLV','PP') AND t.SDLNTY='S'
      AND t.SDLTTR <> 980 AND t.SDNXTR = 999   -- delivered scope (also gates price)
      AND t.SDSHPN in ( 15862333, 15896633)
)
SELECT
    shipment_no, line_no, item, short_item,
    major_prod_code, minor_prod_code,
    origin_plant, bp_sold_to, ship_to,

    -- VOLUME block: source, UoM, factor, converted tons
    source_volume,
    source_volume_uom,
    volume_conversion_factor,
    ROUND(source_volume * volume_conversion_factor, 3) AS converted_volume_tons,

    -- PRICE block: source, UoM, factor, converted price per ton
    source_price,
    source_price_uom,
    price_conversion_factor,
    ROUND(source_price / NULLIF(price_conversion_factor,0), 4) AS converted_price_per_ton,

    -- conversion quality flags
    vol_conv_missing,
    price_conv_missing
FROM line_fact
ORDER BY shipment_no, line_no;