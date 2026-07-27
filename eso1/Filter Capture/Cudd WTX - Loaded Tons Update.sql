SELECT
  F4211.SDADDJ F4211_SDADDJ,
  F4211.SDMCU F4211_SDMCU,
  F4201.SHPA8 F4201_SHPA8,
  F0101.ABALPH F0101_ABALPH,
  F4211.SDLITM F4211_SDLITM,
  (126189 - F4211.SDADDJ) ID_CUSTOM_8182f1886c2be9,
  F0101.ABSIC F0101_ABSIC,
  SUM(F4211.SDAEXP) ReportColumn1,
  SUM(F4211.SDUORG) ReportColumn3,
  SUM(F4211.SDPQOR) ReportColumn4,
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
                  ((F4211.SDDCTO IN (N'SO')))
                  AND (
                    (
                      F4211.SDMCU IN (
                        N'         321',
                        N'         341',
                        N'        9705',
                        N'        9786',
                        N'         161',
                        N'         181'
                      )
                    )
                  )
                )
              )
              AND (
                (
                  NOT (F4211.SDLITM IN (N'MISC BILLING             '))
                )
              )
            )
          )
          AND ((F4211.SDPA8 IN (10058491)))
        )
      )
      AND ((126189 - F4211.SDADDJ) = 0)
    )
    AND (F0101.ABSIC = N'F         ')
  )
GROUP BY
  F4211.SDADDJ,
  F4211.SDMCU,
  F4201.SHPA8,
  F0101.ABALPH,
  F4211.SDLITM,
  (126189 - F4211.SDADDJ),
  F0101.ABSIC,
  F0010.CCCRCD
UNION ALL
SELECT
  F4211.SDADDJ F4211_SDADDJ,
  F4211.SDMCU F4211_SDMCU,
  F4201.SHPA8 F4201_SHPA8,
  F0101.ABALPH F0101_ABALPH,
  F4211.SDLITM F4211_SDLITM,
  (126189 - F4211.SDADDJ) ID_CUSTOM_8182f1886c2be9,
  F0101.ABSIC F0101_ABSIC,
  SUM(F4211.SDAEXP) ReportColumn1,
  SUM(F4211.SDUORG) ReportColumn3,
  SUM(F4211.SDPQOR) ReportColumn4,
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
                  ((F4211.SDDCTO IN (N'SO')))
                  AND (
                    (
                      F4211.SDMCU IN (
                        N'         321',
                        N'         341',
                        N'        9705',
                        N'        9786',
                        N'         161',
                        N'         181'
                      )
                    )
                  )
                )
              )
              AND (
                (
                  NOT (F4211.SDLITM IN (N'MISC BILLING             '))
                )
              )
            )
          )
          AND ((F4211.SDPA8 IN (10058491)))
        )
      )
      AND ((126189 - F4211.SDADDJ) = 0)
    )
    AND (F0101.ABSIC = N'F         ')
  )
GROUP BY
  F4211.SDADDJ,
  F4211.SDMCU,
  F4201.SHPA8,
  F0101.ABALPH,
  F4211.SDLITM,
  (126189 - F4211.SDADDJ),
  F0101.ABSIC,
  F0010.CCCRCD
ORDER BY
  F4211_SDMCU ASC,
  F4211_SDLITM ASC,
  F4211_SDADDJ ASC,
  F4201_SHPA8 ASC,
  F0101_ABSIC ASC,
  F0101_ABALPH ASC,
  ID_CUSTOM_8182f1886c2be9 ASC