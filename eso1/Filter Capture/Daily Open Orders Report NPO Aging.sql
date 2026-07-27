SELECT
  F4211.SDMCU F4211_SDMCU,
  F4211.SDADDJ F4211_SDADDJ,
  F4211.SDDRQJ F4211_SDDRQJ,
  F4201.SHOPDJ F4201_SHOPDJ,
  F4211.SDCNDJ F4211_SDCNDJ,
  F4211.SDSHAN F4211_SDSHAN,
  C.ALCTY1 F0116_ALCTY1,
  F0101.ABALPH F0101_ABALPH,
  F4211.SDLITM F4211_SDLITM,
  F4211.SDAITM F4211_SDAITM,
  F4211.SDDOCO F4211_SDDOCO,
  CAST(F4211.SDLNID AS FLOAT) / 1000 F4211_SDLNID,
  F4211.SDVR01 F4211_SDVR01,
  F4211.SDUOM F4211_SDUOM,
  F4211.SDSRP1 F4211_SDSRP1,
  F0101.ABSIC F0101_ABSIC,
  F4201.SHDEL1 F4201_SHDEL1,
  F4201.SHDEL2 F4201_SHDEL2,
  126191 ID_CUSTOM_8f246e4cdf0ee6,
  SUM(F4211.SDAEXP) ReportColumn1,
  SUM(F4211.SDUORG) ReportColumn3,
  SUM(F4211.SDPQOR) ReportColumn9,
  F0010.CCCRCD DomesticCurrency
FROM
  PRODDTA.F4211 F4211
  INNER JOIN PRODDTA.F4201 F4201 ON (
    (F4201.SHDCTO = F4211.SDDCTO)
    AND (F4201.SHDOCO = F4211.SDDOCO)
  )
  AND (F4201.SHKCOO = F4211.SDKCOO)
  INNER JOIN (
    SELECT
      F0101.ABALPH,
      F0101.ABSIC,
      F0101.ABAN8
    FROM
      PRODDTA.F0101 F0101
    WHERE
      (
        (F0101.ABAT1 BETWEEN N'A  ' AND N'P  ')
        OR (F0101.ABAT1 BETWEEN N'R  ' AND N'ZZZ')
      )
  ) F0101 ON F0101.ABAN8 = F4211.SDSHAN
  INNER JOIN PRODDTA.F0116 C
  INNER JOIN (
    SELECT
      F0116.ALAN8,
      MAX(F0116.ALEFTB) ALEFTB
    FROM
      PRODDTA.F0116 F0116
    GROUP BY
      F0116.ALAN8
  ) D ON (C.ALAN8 = D.ALAN8)
  AND (C.ALEFTB = D.ALEFTB) ON F0101.ABAN8 = D.ALAN8
  INNER JOIN PRODDTA.F0010 F0010 ON F0010.CCCO = F4211.SDKCOO
WHERE
  (
    (
      (
        (
          (
            (
              (
                (
                  (
                    (
                      (
                        ((F4211.SDLTTR < N'980'))
                        AND ((F4211.SDDCTO IN (N'SO')))
                      )
                    )
                    AND (
                      (
                        (
                          NOT (
                            NVL (RTRIM (LTRIM (F4211.SDLITM)), N' ') LIKE N'MISC%'
                          )
                        )
                        AND (
                          NOT (
                            NVL (RTRIM (LTRIM (F4211.SDLITM)), N' ') LIKE N'TR%'
                          )
                        )
                      )
                    )
                  )
                )
                AND ((F4211.SDNXTR IN (N'560')))
              )
            )
            AND ((NOT (F4211.SDLNTY IN (N'F ', N'FT'))))
          )
        )
        AND ((F4211.SDVR01 IN (N'NPO                      ')))
      )
    )
    AND (
      (
        F0101.ABSIC IN (N'F         ', N'FA        ', N'FB        ')
      )
    )
  )
GROUP BY
  F4211.SDMCU,
  F4211.SDADDJ,
  F4211.SDDRQJ,
  F4201.SHOPDJ,
  F4211.SDCNDJ,
  F4211.SDSHAN,
  C.ALCTY1,
  F0101.ABALPH,
  F4211.SDLITM,
  F4211.SDAITM,
  F4211.SDDOCO,
  CAST(F4211.SDLNID AS FLOAT) / 1000,
  F4211.SDVR01,
  F4211.SDUOM,
  F4211.SDSRP1,
  F0101.ABSIC,
  F4201.SHDEL1,
  F4201.SHDEL2,
  F0010.CCCRCD
UNION ALL
SELECT
  F4211.SDMCU F4211_SDMCU,
  F4211.SDADDJ F4211_SDADDJ,
  F4211.SDDRQJ F4211_SDDRQJ,
  F4201.SHOPDJ F4201_SHOPDJ,
  F4211.SDCNDJ F4211_SDCNDJ,
  F4211.SDSHAN F4211_SDSHAN,
  C.ALCTY1 F0116_ALCTY1,
  F0101.ABALPH F0101_ABALPH,
  F4211.SDLITM F4211_SDLITM,
  F4211.SDAITM F4211_SDAITM,
  F4211.SDDOCO F4211_SDDOCO,
  CAST(F4211.SDLNID AS FLOAT) / 1000 F4211_SDLNID,
  F4211.SDVR01 F4211_SDVR01,
  F4211.SDUOM F4211_SDUOM,
  F4211.SDSRP1 F4211_SDSRP1,
  F0101.ABSIC F0101_ABSIC,
  F4201.SHDEL1 F4201_SHDEL1,
  F4201.SHDEL2 F4201_SHDEL2,
  126191 ID_CUSTOM_8f246e4cdf0ee6,
  SUM(F4211.SDAEXP) ReportColumn1,
  SUM(F4211.SDUORG) ReportColumn3,
  SUM(F4211.SDPQOR) ReportColumn9,
  F0010.CCCRCD DomesticCurrency
FROM
  PRODDTA.F42119 F4211
  INNER JOIN PRODDTA.F4201 F4201 ON (
    (F4201.SHDCTO = F4211.SDDCTO)
    AND (F4201.SHDOCO = F4211.SDDOCO)
  )
  AND (F4201.SHKCOO = F4211.SDKCOO)
  INNER JOIN (
    SELECT
      F0101.ABALPH,
      F0101.ABSIC,
      F0101.ABAN8
    FROM
      PRODDTA.F0101 F0101
    WHERE
      (
        (F0101.ABAT1 BETWEEN N'A  ' AND N'P  ')
        OR (F0101.ABAT1 BETWEEN N'R  ' AND N'ZZZ')
      )
  ) F0101 ON F0101.ABAN8 = F4211.SDSHAN
  INNER JOIN PRODDTA.F0116 C
  INNER JOIN (
    SELECT
      F0116.ALAN8,
      MAX(F0116.ALEFTB) ALEFTB
    FROM
      PRODDTA.F0116 F0116
    GROUP BY
      F0116.ALAN8
  ) D ON (C.ALAN8 = D.ALAN8)
  AND (C.ALEFTB = D.ALEFTB) ON F0101.ABAN8 = D.ALAN8
  INNER JOIN PRODDTA.F0010 F0010 ON F0010.CCCO = F4211.SDKCOO
WHERE
  (
    (
      (
        (
          (
            (
              (
                (
                  (
                    (
                      (
                        ((F4211.SDLTTR < N'980'))
                        AND ((F4211.SDDCTO IN (N'SO')))
                      )
                    )
                    AND (
                      (
                        (
                          NOT (
                            NVL (RTRIM (LTRIM (F4211.SDLITM)), N' ') LIKE N'MISC%'
                          )
                        )
                        AND (
                          NOT (
                            NVL (RTRIM (LTRIM (F4211.SDLITM)), N' ') LIKE N'TR%'
                          )
                        )
                      )
                    )
                  )
                )
                AND ((F4211.SDNXTR IN (N'560')))
              )
            )
            AND ((NOT (F4211.SDLNTY IN (N'F ', N'FT'))))
          )
        )
        AND ((F4211.SDVR01 IN (N'NPO                      ')))
      )
    )
    AND (
      (
        F0101.ABSIC IN (N'F         ', N'FA        ', N'FB        ')
      )
    )
  )
GROUP BY
  F4211.SDMCU,
  F4211.SDADDJ,
  F4211.SDDRQJ,
  F4201.SHOPDJ,
  F4211.SDCNDJ,
  F4211.SDSHAN,
  C.ALCTY1,
  F0101.ABALPH,
  F4211.SDLITM,
  F4211.SDAITM,
  F4211.SDDOCO,
  CAST(F4211.SDLNID AS FLOAT) / 1000,
  F4211.SDVR01,
  F4211.SDUOM,
  F4211.SDSRP1,
  F0101.ABSIC,
  F4201.SHDEL1,
  F4201.SHDEL2,
  F0010.CCCRCD
ORDER BY
  F4211_SDLITM ASC,
  F0101_ABALPH ASC,
  F4211_SDCNDJ ASC,
  F4211_SDUOM ASC,
  F4211_SDSRP1 ASC,
  F0101_ABSIC ASC,
  F4211_SDAITM ASC,
  F4201_SHDEL1 ASC,
  F4201_SHDEL2 ASC,
  F4211_SDDRQJ ASC,
  F4201_SHOPDJ ASC,
  F0116_ALCTY1 ASC,
  F4211_SDDOCO ASC,
  F4211_SDLNID ASC,
  F4211_SDMCU ASC,
  F4211_SDADDJ ASC,
  F4211_SDSHAN ASC,
  F4211_SDVR01 ASC,
  ID_CUSTOM_8f246e4cdf0ee6 ASC