SELECT
  F4211.SDMCU F4211_SDMCU,
  F4211.SDVR01 F4211_SDVR01,
  F4211.SDDOCO F4211_SDDOCO,
  F4211.SDRORN F4211_SDRORN,
  F0101.ABALPH F0101_ABALPH,
  C.ALCTY1 F0116_ALCTY1,
  F4211.SDADDJ F4211_SDADDJ,
  F4211.SDLITM F4211_SDLITM,
  F4211.SDUOM F4211_SDUOM,
  F4211.SDCNID F4211_SDCNID,
  F4211.SDSHAN F4211_SDSHAN,
  F4211.SDURAB F4211_SDURAB,
  F4211.SDDOC F4211_SDDOC,
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
  INNER JOIN PRODDTA.F0010 F0010 ON F0010.CCCO = F4211.SDKCOO
WHERE
  (
    (
      (
        (
          (
            ((F4211.SDADDJ >= 121060))
            AND ((F4211.SDDCTO IN (N'ST')))
          )
        )
        AND (
          (
            F4211.SDSHAN IN (
              9001,
              9002,
              9003,
              9004,
              9005,
              9006,
              9007,
              9008,
              9009,
              9012,
              9013,
              9014,
              9015,
              9016,
              9017,
              9018,
              9019,
              9020,
              9021,
              9022,
              9023,
              9024,
              9025,
              9026,
              9027,
              9028,
              9040,
              9041
            )
          )
        )
      )
    )
    AND ((F4211.SDNXTR > N'560'))
  )
GROUP BY
  F4211.SDMCU,
  F4211.SDVR01,
  F4211.SDDOCO,
  F4211.SDRORN,
  F0101.ABALPH,
  C.ALCTY1,
  F4211.SDADDJ,
  F4211.SDLITM,
  F4211.SDUOM,
  F4211.SDCNID,
  F4211.SDSHAN,
  F4211.SDURAB,
  F4211.SDDOC,
  F0010.CCCRCD
UNION ALL
SELECT
  F4211.SDMCU F4211_SDMCU,
  F4211.SDVR01 F4211_SDVR01,
  F4211.SDDOCO F4211_SDDOCO,
  F4211.SDRORN F4211_SDRORN,
  F0101.ABALPH F0101_ABALPH,
  C.ALCTY1 F0116_ALCTY1,
  F4211.SDADDJ F4211_SDADDJ,
  F4211.SDLITM F4211_SDLITM,
  F4211.SDUOM F4211_SDUOM,
  F4211.SDCNID F4211_SDCNID,
  F4211.SDSHAN F4211_SDSHAN,
  F4211.SDURAB F4211_SDURAB,
  F4211.SDDOC F4211_SDDOC,
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
  INNER JOIN PRODDTA.F0010 F0010 ON F0010.CCCO = F4211.SDKCOO
WHERE
  (
    (
      (
        (
          (
            ((F4211.SDADDJ >= 121060))
            AND ((F4211.SDDCTO IN (N'ST')))
          )
        )
        AND (
          (
            F4211.SDSHAN IN (
              9001,
              9002,
              9003,
              9004,
              9005,
              9006,
              9007,
              9008,
              9009,
              9012,
              9013,
              9014,
              9015,
              9016,
              9017,
              9018,
              9019,
              9020,
              9021,
              9022,
              9023,
              9024,
              9025,
              9026,
              9027,
              9028,
              9040,
              9041
            )
          )
        )
      )
    )
    AND ((F4211.SDNXTR > N'560'))
  )
GROUP BY
  F4211.SDMCU,
  F4211.SDVR01,
  F4211.SDDOCO,
  F4211.SDRORN,
  F0101.ABALPH,
  C.ALCTY1,
  F4211.SDADDJ,
  F4211.SDLITM,
  F4211.SDUOM,
  F4211.SDCNID,
  F4211.SDSHAN,
  F4211.SDURAB,
  F4211.SDDOC,
  F0010.CCCRCD
ORDER BY
  F4211_SDMCU ASC,
  F4211_SDUOM ASC,
  F0101_ABALPH ASC,
  F0116_ALCTY1 ASC,
  F4211_SDDOCO ASC,
  F4211_SDLITM ASC,
  F4211_SDADDJ ASC,
  F4211_SDCNID ASC,
  F4211_SDDOC ASC,
  F4211_SDRORN ASC,
  F4211_SDVR01 ASC,
  F4211_SDSHAN ASC,
  F4211_SDURAB ASC