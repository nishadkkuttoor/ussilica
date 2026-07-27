SELECT
  NEWSHIPPINGISSUESREPORT.ORDERNUMBER NEWSHIPPINGISSUES29cdef56,
  NEWSHIPPINGISSUESREPORT.ORDERTYPE NEWSHIPPINGISSUES4d403de7,
  NEWSHIPPINGISSUESREPORT.MCU NEWSHIPPINGISSUEScfd13eac,
  NEWSHIPPINGISSUESREPORT.SHIPMENTNUMBER NEWSHIPPINGISSUES898c0ad6,
  NEWSHIPPINGISSUESREPORT.LINEID NEWSHIPPINGISSUES3bd48876,
  NEWSHIPPINGISSUESREPORT.SEALNO NEWSHIPPINGISSUES4ec24557,
  NEWSHIPPINGISSUESREPORT.NOOFCONTAINTERS NEWSHIPPINGISSUES1f7e7461,
  NEWSHIPPINGISSUESREPORT.LOCATION NEWSHIPPINGISSUESeb0083e6,
  NEWSHIPPINGISSUESREPORT.LOTNO NEWSHIPPINGISSUES11b07cff,
  NEWSHIPPINGISSUESREPORT.PLANT NEWSHIPPINGISSUES6dd4d1d2,
  NEWSHIPPINGISSUESREPORT.NEXTSTATUS NEWSHIPPINGISSUES8a2e12de,
  NEWSHIPPINGISSUESREPORT.IFSORDERNO NEWSHIPPINGISSUESe5496cb6,
  NEWSHIPPINGISSUESREPORT.VEHICLEID NEWSHIPPINGISSUES84d49b8e,
  NEWSHIPPINGISSUESREPORT.ITEMNUMBER NEWSHIPPINGISSUESc2e45b13,
  NEWSHIPPINGISSUESREPORT.TRANSUOM NEWSHIPPINGISSUES3dbd0778,
  (
    TO_CHAR (NEWSHIPPINGISSUESREPORT.ACTUALSHIPDATE, N'YYYY') - 1900
  ) * 1000 + TO_CHAR (NEWSHIPPINGISSUESREPORT.ACTUALSHIPDATE, N'DDD') NEWSHIPPINGISSUES5978e459,
  NEWSHIPPINGISSUESREPORT.QTYINTRANSUOM NEWSHIPPINGISSUES975027e3,
  NEWSHIPPINGISSUESREPORT.CONVERSIONFACTOR NEWSHIPPINGISSUESf5e1a,
  (
    NEWSHIPPINGISSUESREPORT.CONVERSIONFACTOR * NEWSHIPPINGISSUESREPORT.QTYINTRANSUOM
  ) ID_CUSTOM_807d95c114aee7,
  NEWSHIPPINGISSUESREPORT.AMOUNT NEWSHIPPINGISSUES8ca51d39,
  MAX(FLOOR(TO_NUMBER (NULL))) ReportColumn1
FROM
  (
    SELECT
      sddoco OrderNumber,
      sddcto OrderType,
      sdmcu MCU,
      sdshpn ShipmentNumber,
      lnid LineID,
      sealno SEALNo,
      nocontainer NoOfContainters,
      sdlocn Location,
      sdlotn LotNo,
      Plant,
      sdnxtr NextStatus,
      sdvr03 IFSOrderNo,
      sdcnid VehicleID,
      sdlitm ItemNumber,
      sduom TransUOM,
      ADDJ ActualShipDate,
      Quantity QtyinTransUOM,
      decode (ConversionFactor, NULL, TNCONV1, ConversionFactor) ConversionFactor,
      decode (
        ConversionFactor,
        NULL,
        (Quantity * TNCONV1),
        (Quantity * ConversionFactor)
      ) "QtyinTons",
      OrderAmount Amount
    FROM
      (
        select
          sddoco,
          sddcto,
          sdmcu,
          sdshpn,
          sdlnid / 1000 lnid,
          sealno,
          nocontainer,
          sdlocn,
          sdlotn,
          Plant,
          sdnxtr,
          sdvr03,
          sdcnid,
          sdlitm,
          sduom,
          TO_DATE (TO_CHAR (sdaddj + 1900000), 'YYYYDDD') ADDJ,
          sduorg / 1000 Quantity,
          DECODE (sduom, 'TN', 1, TNCONV) ConversionFactor,
          TNCONV1,
          sdaexp / 100 OrderAmount
        from
          (
            select
              sddoco,
              sddcto,
              sdmcu,
              sdshpn,
              sdlnid,
              (
                select
                  ak55seln
                from
                  proddta.F5642B11
                where
                  akshpn = sdshpn
                  and aklnid = sdlnid
                  and akdoco = sddoco
                  and akdcto = sddcto
              ) sealno,
              (
                select
                  BA55NCON
                from
                  proddta.F5642B01
                where
                  bashpn = sdshpn
                  and badoco = sddoco
                  and badcto = sddcto
                  and bakcoo = sdkcoo
              ) nocontainer,
              sdlocn,
              sdlotn,
              sdnxtr,
              (
                select
                  mcdl01
                from
                  PRODDTA.F0006
                where
                  mcmcu = sdmcu
              ) Plant,
              sdvr03,
              sdcnid,
              sdlitm,
              sduom,
              sdaddj,
              (
                select
                  umconv / 10000000
                from
                  proddta.F41002
                where
                  umitm = sditm
                  and umum = sduom
                  and umrum = 'TN'
              ) TNCONV,
              (
                select
                  (1 / (ucconv / 10000000))
                from
                  proddta.F41003
                where
                  ucum = 'TN'
                  and ucrum = sduom
              ) TNCONV1,
              sduorg,
              sdaexp
            from
              proddta.F4211
            where
              sdaddj > 110000
              and sdco in ('00640', '00645')
              and sdlttr <> 980
              and sdlnty = 'S'
              and sdcnid <> ' '
              and sdcnid <> '.'
              and sdnxtr < 571
              and sddcto in ('S1', 'SE', 'SZ')
          )
      )
  ) NEWSHIPPINGISSUESREPORT
GROUP BY
  NEWSHIPPINGISSUESREPORT.ORDERNUMBER,
  NEWSHIPPINGISSUESREPORT.ORDERTYPE,
  NEWSHIPPINGISSUESREPORT.MCU,
  NEWSHIPPINGISSUESREPORT.SHIPMENTNUMBER,
  NEWSHIPPINGISSUESREPORT.LINEID,
  NEWSHIPPINGISSUESREPORT.SEALNO,
  NEWSHIPPINGISSUESREPORT.NOOFCONTAINTERS,
  NEWSHIPPINGISSUESREPORT.LOCATION,
  NEWSHIPPINGISSUESREPORT.LOTNO,
  NEWSHIPPINGISSUESREPORT.PLANT,
  NEWSHIPPINGISSUESREPORT.NEXTSTATUS,
  NEWSHIPPINGISSUESREPORT.IFSORDERNO,
  NEWSHIPPINGISSUESREPORT.VEHICLEID,
  NEWSHIPPINGISSUESREPORT.ITEMNUMBER,
  NEWSHIPPINGISSUESREPORT.TRANSUOM,
  (
    TO_CHAR (NEWSHIPPINGISSUESREPORT.ACTUALSHIPDATE, N'YYYY') - 1900
  ) * 1000 + TO_CHAR (NEWSHIPPINGISSUESREPORT.ACTUALSHIPDATE, N'DDD'),
  NEWSHIPPINGISSUESREPORT.QTYINTRANSUOM,
  NEWSHIPPINGISSUESREPORT.CONVERSIONFACTOR,
  (
    NEWSHIPPINGISSUESREPORT.CONVERSIONFACTOR * NEWSHIPPINGISSUESREPORT.QTYINTRANSUOM
  ),
  NEWSHIPPINGISSUESREPORT.AMOUNT
ORDER BY
  NEWSHIPPINGISSUES6dd4d1d2 ASC,
  NEWSHIPPINGISSUES4d403de7 ASC,
  NEWSHIPPINGISSUES29cdef56 ASC,
  NEWSHIPPINGISSUES5978e459 ASC,
  NEWSHIPPINGISSUES8ca51d39 ASC,
  NEWSHIPPINGISSUESf5e1a ASC,
  NEWSHIPPINGISSUESe5496cb6 ASC,
  NEWSHIPPINGISSUESc2e45b13 ASC,
  NEWSHIPPINGISSUES3bd48876 ASC,
  NEWSHIPPINGISSUESeb0083e6 ASC,
  NEWSHIPPINGISSUES11b07cff ASC,
  NEWSHIPPINGISSUEScfd13eac ASC,
  NEWSHIPPINGISSUES8a2e12de ASC,
  NEWSHIPPINGISSUES1f7e7461 ASC,
  NEWSHIPPINGISSUES975027e3 ASC,
  NEWSHIPPINGISSUES4ec24557 ASC,
  NEWSHIPPINGISSUES898c0ad6 ASC,
  NEWSHIPPINGISSUES3dbd0778 ASC,
  NEWSHIPPINGISSUES84d49b8e ASC,
  ID_CUSTOM_807d95c114aee7 ASC