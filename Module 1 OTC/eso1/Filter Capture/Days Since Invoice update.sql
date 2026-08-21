SELECT F0101.ABAT1       F0101_ABAT1,
       F4211.SDAN8       C_F4211_SDAN8,
       F4211.SDAN8       F4211_SDAN8,
       F4211.SDSHAN      C_F4211_SDSHAN,
       F4211.SDSHAN      F4211_SDSHAN,
       126223            ID_CUSTOM_85e458cc548136,
       MAX(F4211.SDIVD)  ID_CUSTOM_85e45afd40c2327,
       CASE
         WHEN ( 126223 IS NULL )
               OR ( 126223 = 0 ) THEN NULL
         WHEN ( MAX(F4211.SDIVD) IS NULL )
               OR ( MAX(F4211.SDIVD) = 0 ) THEN NULL
         ELSE ( TO_DATE(N'01/01/2026', N'dd/mm/yyyy') + 222 ) - ( TO_DATE(N'01/01/'
                                                                           || TO_CHAR(FLOOR(TO_NUMBER(MAX(F4211.SDIVD) / 1000)) + 1900), N'dd/mm/yyyy') + MAX(F4211.SDIVD) - FLOOR(TO_NUMBER(MAX(F4211.SDIVD) / 1000)) * 1000 - 1 )
       END               ID_CUSTOM_85e4653cf668450,
       SUM(F4211.SDUORG) ReportColumn1
FROM   PRODDTA.F4211 F4211
       INNER JOIN (SELECT F0101.ABAT1,
                          F0101.ABAN8
                   FROM   PRODDTA.F0101 F0101
                   WHERE  ( ( F0101.ABAT1 BETWEEN N'A  ' AND N'P  ' )
                             OR ( F0101.ABAT1 BETWEEN N'R  ' AND N'ZZZ' ) )) F0101
         ON F4211.SDAN8 = F0101.ABAN8
WHERE  ( (( F4211.SDKCOO IN ( N'00750' ) ))
         AND (( F0101.ABAT1 IN ( N'CB ' ) )) )
GROUP  BY F0101.ABAT1,
          F4211.SDAN8,
          F4211.SDAN8,
          F4211.SDSHAN,
          F4211.SDSHAN
ORDER  BY F4211_SDSHAN ASC,
          F4211_SDAN8 ASC,
          F0101_ABAT1 ASC,
          ID_CUSTOM_85e458cc548136 ASC,
          ID_CUSTOM_85e45afd40c2327 ASC,
          ID_CUSTOM_85e4653cf668450 ASC 