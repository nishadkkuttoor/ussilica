SELECT
  F4211.SDDRQJ F4211_SDDRQJ,
  F4211.SDVR01 F4211_SDVR01,
  F4211.SDDOCO F4211_SDDOCO,
  CAST(F4211.SDLNID AS FLOAT) / 1000 F4211_SDLNID,
  F4211.SDSHAN F4211_SDSHAN,
  F0101.ABALPH F0101_ABALPH,
  C.ALCTY1 F0116_ALCTY1,
  C.ALADDS F0116_ALADDS,
  F4211.SDLITM F4211_SDLITM,
  F4211.SDAITM F4211_SDAITM,
  F4211.SDCARS F4211_SDCARS,
  F4201.SHCARS F4201_SHCARS,
  F4211.SDDCTO F4211_SDDCTO,
  SUM(F4211.SDAEXP) ReportColumn1,
  SUM(F4211.SDUORG) ReportColumn3,
  SUM(F4211.SDPQOR) ReportColumn4,
  F0010.CCCRCD DomesticCurrency
FROM
  PRODDTA.F4211 F4211
  INNER JOIN (
    SELECT
      F0101.ABALPH,
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
  INNER JOIN PRODDTA.F4201 F4201 ON (
    (F4201.SHDCTO = F4211.SDDCTO)
    AND (F4201.SHDOCO = F4211.SDDOCO)
  )
  AND (F4201.SHKCOO = F4211.SDKCOO)
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
                    ((F4211.SDDRQJ BETWEEN 116001 AND 8098365))
                    AND ((F4211.SDMCU IN (N'         501')))
                  )
                )
                AND (
                  (
                    NOT (
                      F4211.SDLITM IN (
                        N'50065B00000              ',
                        N'50064B00000              ',
                        N'50063B00000              '
                      )
                    )
                  )
                )
              )
            )
            AND ((F4211.SDNXTR IN (N'560')))
          )
        )
        AND (
          (
            F4211.SDMOT IN (
              N'RC1',
              N'RCP',
              N'RCS',
              N'RCX',
              N'RSP',
              N'RU1',
              N'RU2',
              N'RU7',
              N'RU8',
              N'RUT',
              N'WNF',
              N'RCC',
              N'UTP',
              N'RU3',
              N'UTR',
              N'UTS'
            )
          )
        )
      )
    )
    AND ((F4211.SDCO IN (N'00400')))
  )
GROUP BY
  F4211.SDDRQJ,
  F4211.SDVR01,
  F4211.SDDOCO,
  CAST(F4211.SDLNID AS FLOAT) / 1000,
  F4211.SDSHAN,
  F0101.ABALPH,
  C.ALCTY1,
  C.ALADDS,
  F4211.SDLITM,
  F4211.SDAITM,
  F4211.SDCARS,
  F4201.SHCARS,
  F4211.SDDCTO,
  F0010.CCCRCD
UNION ALL
SELECT
  F4211.SDDRQJ F4211_SDDRQJ,
  F4211.SDVR01 F4211_SDVR01,
  F4211.SDDOCO F4211_SDDOCO,
  CAST(F4211.SDLNID AS FLOAT) / 1000 F4211_SDLNID,
  F4211.SDSHAN F4211_SDSHAN,
  F0101.ABALPH F0101_ABALPH,
  C.ALCTY1 F0116_ALCTY1,
  C.ALADDS F0116_ALADDS,
  F4211.SDLITM F4211_SDLITM,
  F4211.SDAITM F4211_SDAITM,
  F4211.SDCARS F4211_SDCARS,
  F4201.SHCARS F4201_SHCARS,
  F4211.SDDCTO F4211_SDDCTO,
  SUM(F4211.SDAEXP) ReportColumn1,
  SUM(F4211.SDUORG) ReportColumn3,
  SUM(F4211.SDPQOR) ReportColumn4,
  F0010.CCCRCD DomesticCurrency
FROM
  PRODDTA.F42119 F4211
  INNER JOIN (
    SELECT
      F0101.ABALPH,
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
  INNER JOIN PRODDTA.F4201 F4201 ON (
    (F4201.SHDCTO = F4211.SDDCTO)
    AND (F4201.SHDOCO = F4211.SDDOCO)
  )
  AND (F4201.SHKCOO = F4211.SDKCOO)
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
                    ((F4211.SDDRQJ BETWEEN 116001 AND 8098365))
                    AND ((F4211.SDMCU IN (N'         501')))
                  )
                )
                AND (
                  (
                    NOT (
                      F4211.SDLITM IN (
                        N'50065B00000              ',
                        N'50064B00000              ',
                        N'50063B00000              '
                      )
                    )
                  )
                )
              )
            )
            AND ((F4211.SDNXTR IN (N'560')))
          )
        )
        AND (
          (
            F4211.SDMOT IN (
              N'RC1',
              N'RCP',
              N'RCS',
              N'RCX',
              N'RSP',
              N'RU1',
              N'RU2',
              N'RU7',
              N'RU8',
              N'RUT',
              N'WNF',
              N'RCC',
              N'UTP',
              N'RU3',
              N'UTR',
              N'UTS'
            )
          )
        )
      )
    )
    AND ((F4211.SDCO IN (N'00400')))
  )
GROUP BY
  F4211.SDDRQJ,
  F4211.SDVR01,
  F4211.SDDOCO,
  CAST(F4211.SDLNID AS FLOAT) / 1000,
  F4211.SDSHAN,
  F0101.ABALPH,
  C.ALCTY1,
  C.ALADDS,
  F4211.SDLITM,
  F4211.SDAITM,
  F4211.SDCARS,
  F4201.SHCARS,
  F4211.SDDCTO,
  F0010.CCCRCD
ORDER BY
  F4211_SDDOCO ASC,
  F4211_SDLITM ASC,
  F4211_SDDCTO ASC,
  F0101_ABALPH ASC,
  F4211_SDDRQJ ASC,
  F4211_SDLNID ASC,
  F0116_ALCTY1 ASC,
  F0116_ALADDS ASC,
  F4211_SDAITM ASC,
  F4201_SHCARS ASC,
  F4211_SDCARS ASC,
  F4211_SDVR01 ASC,
  F4211_SDSHAN ASC