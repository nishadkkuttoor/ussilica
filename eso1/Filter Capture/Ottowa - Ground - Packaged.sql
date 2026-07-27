SELECT
  F4211.SDDRQJ F4211_SDDRQJ,
  F4211.SDDOCO F4211_SDDOCO,
  F0101.ABALPH F0101_ABALPH,
  F4211.SDVR01 F4211_SDVR01,
  C.ALCTY1 F0116_ALCTY1,
  C.ALADDS F0116_ALADDS,
  F4211.SDLITM F4211_SDLITM,
  F4211.SDMOT F4211_SDMOT,
  F4211.SDCARS F4211_SDCARS,
  F4211.SDUOM F4211_SDUOM,
  F4211.SDSRP3 F4211_SDSRP3,
  F4201.SHDEL1 F4201_SHDEL1,
  F4201.SHDEL2 F4201_SHDEL2,
  F4211.SDTORG F4211_SDTORG,
  F4201.SHHOLD F4201_SHHOLD,
  F0101.ABSIC F0101_ABSIC,
  F4211.SDMCU F4211_SDMCU,
  CAST(F4211.SDLNID AS FLOAT) / 1000 F4211_SDLNID,
  F4211.SDHOLD F4211_SDHOLD,
  F4211.SDDCTO F4211_SDDCTO,
  F4211.SDTRDJ F4211_SDTRDJ,
  F4211.SDADDJ F4211_SDADDJ,
  F4211.SDFRTH F4211_SDFRTH,
  F4211.SDCNID F4211_SDCNID,
  F4211.SDSHAN F4211_SDSHAN,
  F4211.SDPA8 F4211_SDPA8,
  F4211.SDURAB F4211_SDURAB,
  CAST(F4211.SDUPRC AS FLOAT) / 1000000 F4211_SDUPRC,
  F4211.SDLTTR F4211_SDLTTR,
  F4211.SDNXTR F4211_SDNXTR,
  F4211.SDSRP1 F4211_SDSRP1,
  F4211.SDODCT F4211_SDODCT,
  F4211.SDOORN F4211_SDOORN,
  F4211.SDODOC F4211_SDODOC,
  F4211.SDSRP2 F4211_SDSRP2,
  SUM(F4211.SDAEXP) ReportColumn1,
  SUM(F4211.SDUORG) ReportColumn3,
  SUM(F4211.SDPQOR) ReportColumn4,
  SUM(F4211.SDSOQS) ReportColumn5,
  SUM(F4211.SDUOPN) ReportColumn6,
  F0010.CCCRCD DomesticCurrency
FROM
  PRODDTA.F4211 F4211
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
                    (
                      (
                        ((F4211.SDDRQJ BETWEEN 116001 AND 1099365))
                        AND ((F4211.SDMCU IN (N'         501')))
                      )
                    )
                    AND (
                      (
                        F4211.SDLITM IN (
                          N'06069B00000              ',
                          N'06069P30154              ',
                          N'06069P30160              ',
                          N'06069P84101              ',
                          N'06069P91101              ',
                          N'06069P9B101              ',
                          N'06113B00000              ',
                          N'06113P84101              ',
                          N'06113P91101              ',
                          N'06115B00000              ',
                          N'06115P91101              ',
                          N'06119B00000              ',
                          N'06119P58002              ',
                          N'06119P58102              ',
                          N'06119P80102              ',
                          N'06119P90101              ',
                          N'06119P91101              ',
                          N'06123B00000              ',
                          N'06143B00000              ',
                          N'06143P80102              ',
                          N'06143P84101              ',
                          N'06143P91101              ',
                          N'07063B00000              ',
                          N'07123B00000              ',
                          N'07150R00000              ',
                          N'08101F00000              ',
                          N'08111B00000              ',
                          N'08113B00000              ',
                          N'08117P91101              ',
                          N'08119B00000              ',
                          N'08119P10170              ',
                          N'08122B00000              ',
                          N'08123B00000              ',
                          N'08123P10101              ',
                          N'08131B00000              ',
                          N'08143B00000              ',
                          N'08143P10170              ',
                          N'08144B00000              ',
                          N'15061B00000              ',
                          N'15061F00000              ',
                          N'15063B00000              ',
                          N'15111B00000              ',
                          N'15111F00000              ',
                          N'15111P30149              ',
                          N'15111P30163              ',
                          N'15111P30170              ',
                          N'15111P80101              ',
                          N'15111P80102              ',
                          N'15111P91101              ',
                          N'15111PC7101              ',
                          N'15114F00000              ',
                          N'15114P84101              ',
                          N'15115B00000              ',
                          N'15115F00000              ',
                          N'15115P08101              ',
                          N'15115P30142              ',
                          N'15115P30156              ',
                          N'15115P30163              ',
                          N'15115P30170              ',
                          N'15115P80101              ',
                          N'15115P80102              ',
                          N'15115P83101              ',
                          N'15115P91101              ',
                          N'15119B00000              ',
                          N'15119F00000              ',
                          N'15119P30142              ',
                          N'15119P30149              ',
                          N'15119P30156              ',
                          N'15119P30170              ',
                          N'15119P78102              ',
                          N'15119P80102              ',
                          N'15119P87101              ',
                          N'15119P91101              ',
                          N'15119P93101              ',
                          N'15131B00000              ',
                          N'15131F00000              ',
                          N'15131P30149              ',
                          N'15131P30156              ',
                          N'15131P30163              ',
                          N'15131P30170              ',
                          N'15131P80102              ',
                          N'15131P87101              ',
                          N'15131P91101              ',
                          N'15131PC1101              ',
                          N'15143B00000              ',
                          N'15143P30142              ',
                          N'15143P30149              ',
                          N'15143P30156              ',
                          N'15143P30170              ',
                          N'15143P80102              ',
                          N'15143P84101              ',
                          N'15143P91101              ',
                          N'156745                   ',
                          N'17061F00000              ',
                          N'17063B00000              ',
                          N'17112B00000              ',
                          N'17112F00000              ',
                          N'17114F00000              ',
                          N'17116F00000              ',
                          N'17117F00000              ',
                          N'17117P91101              ',
                          N'17119B00000              ',
                          N'17119F00000              ',
                          N'17119P91101              ',
                          N'17131B00000              ',
                          N'17131F00000              ',
                          N'17142B00000              ',
                          N'17143B00000              ',
                          N'17143F00000              ',
                          N'17144B00000              ',
                          N'17144F00000              ',
                          N'34123B00000              ',
                          N'50061B00000              ',
                          N'50061P30160              ',
                          N'50061P50130              ',
                          N'50063B00000              ',
                          N'50063P30160              ',
                          N'50063P50130              ',
                          N'50064B00000              ',
                          N'50065B00000              ',
                          N'50065P30160              ',
                          N'50065P50130              ',
                          N'50066B00000              ',
                          N'50066P30160              ',
                          N'50066P51130              ',
                          N'50067B00000              ',
                          N'50067P30160              ',
                          N'50067P51130              ',
                          N'50067P52130              ',
                          N'50068B00000              ',
                          N'50068P30160              ',
                          N'50068P52130              ',
                          N'50069B00000              ',
                          N'50069P30160              ',
                          N'50069P52130              ',
                          N'50069P91101              ',
                          N'574772                   ',
                          N'60280                    ',
                          N'75531                    ',
                          N'75572                    ',
                          N'75614                    ',
                          N'75630                    ',
                          N'90061B00000              ',
                          N'93123P10101              ',
                          N'97064B00000              '
                        )
                      )
                    )
                  )
                )
                AND ((F4211.SDNXTR < N'561'))
              )
            )
            AND ((F4211.SDSRP3 IN (N'PKG')))
          )
        )
        AND ((NOT (F4211.SDUOM IN (N'EA'))))
      )
    )
    AND ((F4211.SDCO IN (N'00400', N'00390', N'00330')))
  )
GROUP BY
  F4211.SDDRQJ,
  F4211.SDDOCO,
  F0101.ABALPH,
  F4211.SDVR01,
  C.ALCTY1,
  C.ALADDS,
  F4211.SDLITM,
  F4211.SDMOT,
  F4211.SDCARS,
  F4211.SDUOM,
  F4211.SDSRP3,
  F4201.SHDEL1,
  F4201.SHDEL2,
  F4211.SDTORG,
  F4201.SHHOLD,
  F0101.ABSIC,
  F4211.SDMCU,
  CAST(F4211.SDLNID AS FLOAT) / 1000,
  F4211.SDHOLD,
  F4211.SDDCTO,
  F4211.SDTRDJ,
  F4211.SDADDJ,
  F4211.SDFRTH,
  F4211.SDCNID,
  F4211.SDSHAN,
  F4211.SDPA8,
  F4211.SDURAB,
  CAST(F4211.SDUPRC AS FLOAT) / 1000000,
  F4211.SDLTTR,
  F4211.SDNXTR,
  F4211.SDSRP1,
  F4211.SDODCT,
  F4211.SDOORN,
  F4211.SDODOC,
  F4211.SDSRP2,
  F0010.CCCRCD
UNION ALL
SELECT
  F4211.SDDRQJ F4211_SDDRQJ,
  F4211.SDDOCO F4211_SDDOCO,
  F0101.ABALPH F0101_ABALPH,
  F4211.SDVR01 F4211_SDVR01,
  C.ALCTY1 F0116_ALCTY1,
  C.ALADDS F0116_ALADDS,
  F4211.SDLITM F4211_SDLITM,
  F4211.SDMOT F4211_SDMOT,
  F4211.SDCARS F4211_SDCARS,
  F4211.SDUOM F4211_SDUOM,
  F4211.SDSRP3 F4211_SDSRP3,
  F4201.SHDEL1 F4201_SHDEL1,
  F4201.SHDEL2 F4201_SHDEL2,
  F4211.SDTORG F4211_SDTORG,
  F4201.SHHOLD F4201_SHHOLD,
  F0101.ABSIC F0101_ABSIC,
  F4211.SDMCU F4211_SDMCU,
  CAST(F4211.SDLNID AS FLOAT) / 1000 F4211_SDLNID,
  F4211.SDHOLD F4211_SDHOLD,
  F4211.SDDCTO F4211_SDDCTO,
  F4211.SDTRDJ F4211_SDTRDJ,
  F4211.SDADDJ F4211_SDADDJ,
  F4211.SDFRTH F4211_SDFRTH,
  F4211.SDCNID F4211_SDCNID,
  F4211.SDSHAN F4211_SDSHAN,
  F4211.SDPA8 F4211_SDPA8,
  F4211.SDURAB F4211_SDURAB,
  CAST(F4211.SDUPRC AS FLOAT) / 1000000 F4211_SDUPRC,
  F4211.SDLTTR F4211_SDLTTR,
  F4211.SDNXTR F4211_SDNXTR,
  F4211.SDSRP1 F4211_SDSRP1,
  F4211.SDODCT F4211_SDODCT,
  F4211.SDOORN F4211_SDOORN,
  F4211.SDODOC F4211_SDODOC,
  F4211.SDSRP2 F4211_SDSRP2,
  SUM(F4211.SDAEXP) ReportColumn1,
  SUM(F4211.SDUORG) ReportColumn3,
  SUM(F4211.SDPQOR) ReportColumn4,
  SUM(F4211.SDSOQS) ReportColumn5,
  SUM(F4211.SDUOPN) ReportColumn6,
  F0010.CCCRCD DomesticCurrency
FROM
  PRODDTA.F42119 F4211
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
                    (
                      (
                        ((F4211.SDDRQJ BETWEEN 116001 AND 1099365))
                        AND ((F4211.SDMCU IN (N'         501')))
                      )
                    )
                    AND (
                      (
                        F4211.SDLITM IN (
                          N'06069B00000              ',
                          N'06069P30154              ',
                          N'06069P30160              ',
                          N'06069P84101              ',
                          N'06069P91101              ',
                          N'06069P9B101              ',
                          N'06113B00000              ',
                          N'06113P84101              ',
                          N'06113P91101              ',
                          N'06115B00000              ',
                          N'06115P91101              ',
                          N'06119B00000              ',
                          N'06119P58002              ',
                          N'06119P58102              ',
                          N'06119P80102              ',
                          N'06119P90101              ',
                          N'06119P91101              ',
                          N'06123B00000              ',
                          N'06143B00000              ',
                          N'06143P80102              ',
                          N'06143P84101              ',
                          N'06143P91101              ',
                          N'07063B00000              ',
                          N'07123B00000              ',
                          N'07150R00000              ',
                          N'08101F00000              ',
                          N'08111B00000              ',
                          N'08113B00000              ',
                          N'08117P91101              ',
                          N'08119B00000              ',
                          N'08119P10170              ',
                          N'08122B00000              ',
                          N'08123B00000              ',
                          N'08123P10101              ',
                          N'08131B00000              ',
                          N'08143B00000              ',
                          N'08143P10170              ',
                          N'08144B00000              ',
                          N'15061B00000              ',
                          N'15061F00000              ',
                          N'15063B00000              ',
                          N'15111B00000              ',
                          N'15111F00000              ',
                          N'15111P30149              ',
                          N'15111P30163              ',
                          N'15111P30170              ',
                          N'15111P80101              ',
                          N'15111P80102              ',
                          N'15111P91101              ',
                          N'15111PC7101              ',
                          N'15114F00000              ',
                          N'15114P84101              ',
                          N'15115B00000              ',
                          N'15115F00000              ',
                          N'15115P08101              ',
                          N'15115P30142              ',
                          N'15115P30156              ',
                          N'15115P30163              ',
                          N'15115P30170              ',
                          N'15115P80101              ',
                          N'15115P80102              ',
                          N'15115P83101              ',
                          N'15115P91101              ',
                          N'15119B00000              ',
                          N'15119F00000              ',
                          N'15119P30142              ',
                          N'15119P30149              ',
                          N'15119P30156              ',
                          N'15119P30170              ',
                          N'15119P78102              ',
                          N'15119P80102              ',
                          N'15119P87101              ',
                          N'15119P91101              ',
                          N'15119P93101              ',
                          N'15131B00000              ',
                          N'15131F00000              ',
                          N'15131P30149              ',
                          N'15131P30156              ',
                          N'15131P30163              ',
                          N'15131P30170              ',
                          N'15131P80102              ',
                          N'15131P87101              ',
                          N'15131P91101              ',
                          N'15131PC1101              ',
                          N'15143B00000              ',
                          N'15143P30142              ',
                          N'15143P30149              ',
                          N'15143P30156              ',
                          N'15143P30170              ',
                          N'15143P80102              ',
                          N'15143P84101              ',
                          N'15143P91101              ',
                          N'156745                   ',
                          N'17061F00000              ',
                          N'17063B00000              ',
                          N'17112B00000              ',
                          N'17112F00000              ',
                          N'17114F00000              ',
                          N'17116F00000              ',
                          N'17117F00000              ',
                          N'17117P91101              ',
                          N'17119B00000              ',
                          N'17119F00000              ',
                          N'17119P91101              ',
                          N'17131B00000              ',
                          N'17131F00000              ',
                          N'17142B00000              ',
                          N'17143B00000              ',
                          N'17143F00000              ',
                          N'17144B00000              ',
                          N'17144F00000              ',
                          N'34123B00000              ',
                          N'50061B00000              ',
                          N'50061P30160              ',
                          N'50061P50130              ',
                          N'50063B00000              ',
                          N'50063P30160              ',
                          N'50063P50130              ',
                          N'50064B00000              ',
                          N'50065B00000              ',
                          N'50065P30160              ',
                          N'50065P50130              ',
                          N'50066B00000              ',
                          N'50066P30160              ',
                          N'50066P51130              ',
                          N'50067B00000              ',
                          N'50067P30160              ',
                          N'50067P51130              ',
                          N'50067P52130              ',
                          N'50068B00000              ',
                          N'50068P30160              ',
                          N'50068P52130              ',
                          N'50069B00000              ',
                          N'50069P30160              ',
                          N'50069P52130              ',
                          N'50069P91101              ',
                          N'574772                   ',
                          N'60280                    ',
                          N'75531                    ',
                          N'75572                    ',
                          N'75614                    ',
                          N'75630                    ',
                          N'90061B00000              ',
                          N'93123P10101              ',
                          N'97064B00000              '
                        )
                      )
                    )
                  )
                )
                AND ((F4211.SDNXTR < N'561'))
              )
            )
            AND ((F4211.SDSRP3 IN (N'PKG')))
          )
        )
        AND ((NOT (F4211.SDUOM IN (N'EA'))))
      )
    )
    AND ((F4211.SDCO IN (N'00400', N'00390', N'00330')))
  )
GROUP BY
  F4211.SDDRQJ,
  F4211.SDDOCO,
  F0101.ABALPH,
  F4211.SDVR01,
  C.ALCTY1,
  C.ALADDS,
  F4211.SDLITM,
  F4211.SDMOT,
  F4211.SDCARS,
  F4211.SDUOM,
  F4211.SDSRP3,
  F4201.SHDEL1,
  F4201.SHDEL2,
  F4211.SDTORG,
  F4201.SHHOLD,
  F0101.ABSIC,
  F4211.SDMCU,
  CAST(F4211.SDLNID AS FLOAT) / 1000,
  F4211.SDHOLD,
  F4211.SDDCTO,
  F4211.SDTRDJ,
  F4211.SDADDJ,
  F4211.SDFRTH,
  F4211.SDCNID,
  F4211.SDSHAN,
  F4211.SDPA8,
  F4211.SDURAB,
  CAST(F4211.SDUPRC AS FLOAT) / 1000000,
  F4211.SDLTTR,
  F4211.SDNXTR,
  F4211.SDSRP1,
  F4211.SDODCT,
  F4211.SDOORN,
  F4211.SDODOC,
  F4211.SDSRP2,
  F0010.CCCRCD
ORDER BY
  F4211_SDDOCO ASC,
  F4211_SDLITM ASC,
  F4211_SDDCTO ASC,
  F0101_ABALPH ASC,
  F4211_SDDRQJ ASC,
  F4211_SDTRDJ ASC,
  F4211_SDLTTR ASC,
  F4211_SDNXTR ASC,
  F4211_SDUOM ASC,
  F4211_SDSRP1 ASC,
  F4201_SHDEL1 ASC,
  F4201_SHDEL2 ASC,
  F4211_SDLNID ASC,
  F4211_SDHOLD ASC,
  F4211_SDADDJ ASC,
  F0116_ALCTY1 ASC,
  F0116_ALADDS ASC,
  F4211_SDMOT ASC,
  F4211_SDCNID ASC,
  F4211_SDTORG ASC,
  F4211_SDPA8 ASC,
  F4211_SDUPRC ASC,
  F4211_SDODCT ASC,
  F4211_SDOORN ASC,
  F4211_SDODOC ASC,
  F0101_ABSIC ASC,
  F4211_SDFRTH ASC,
  F4201_SHHOLD ASC,
  F4211_SDSRP3 ASC,
  F4211_SDSRP2 ASC,
  F4211_SDCARS ASC,
  F4211_SDVR01 ASC,
  F4211_SDMCU ASC,
  F4211_SDSHAN ASC,
  F4211_SDURAB ASC