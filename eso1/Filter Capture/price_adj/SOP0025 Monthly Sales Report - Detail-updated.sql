SELECT F4211.SDKCOO                          F4211_SDKCOO,
       F4211.SDDOCO                          F4211_SDDOCO,
       F4211.SDDCTO                          F4211_SDDCTO,
       CAST(F4211.SDLNID AS FLOAT) / 1000    F4211_SDLNID,
       F4211.SDMCU                           F4211_SDMCU,
       F4211.SDDOC                           F4211_SDDOC,
       F4211.SDDGL                           F4211_SDDGL,
       F4211.SDADDJ                          F4211_SDADDJ,
       F4211.SDTRDJ                          F4211_SDTRDJ,
       F4211.SDLITM                          F4211_SDLITM,
       F4101.IMDSC1                          F4101_IMDSC1,
       F4211.SDAN8                           F4211_SDAN8,
       F4211.SDSHAN                          F4211_SDSHAN,
       F4211.SDLNTY                          F4211_SDLNTY,
       F4211.SDNXTR                          F4211_SDNXTR,
       F4211.SDSRP4                          F4211_SDSRP4,
       F4101.IMSEG4                          F4101_IMSEG4,
       F4211.SDURCD                          F4211_SDURCD,
       F4074.ALAST                           F4074_ALAST,
       F4211.SDSRP3                          F4211_SDSRP3,
       F4211.SDSRP2                          F4211_SDSRP2,
       F4211.SDSRP1                          F4211_SDSRP1,
       CAST(F4211.SDUPRC AS FLOAT) / 1000000 F4211_SDUPRC,
       CAST(F4074.ALUPRC AS FLOAT) / 1000000 F4074_ALUPRC,
       F4211.SDUOM                           F4211_SDUOM,
       F4074.ALUOM                           F4074_ALUOM,
       F4211.SDGLC                           F4211_SDGLC,
       SUM(F4211.SDSOQS)                     ReportColumn1,
       SUM(F4211.SDAEXP)                     ReportColumn4,
       SUM(F4211.SDSOQS * CASE
                            WHEN ( ( COALESCE(FromConvTableF41002.UMCNV1, FromConvTableF41003.UMCNV1, CASE
                                                                                                        WHEN F4211.SDUOM = F4101.IMUOM1 THEN 10000000
                                                                                                      END) IS NULL )
                                    OR ( COALESCE(ToConvTableF41002.UMCNV1, ToConvTableF41003.UMCNV1, CASE
                                                                                                        WHEN N'TN' = F4101.IMUOM1 THEN 10000000
                                                                                                      END) IS NULL ) )
                                  OR ( COALESCE(ToConvTableF41002.UMCNV1, ToConvTableF41003.UMCNV1, CASE
                                                                                                      WHEN N'TN' = F4101.IMUOM1 THEN 10000000
                                                                                                    END) = 0 ) THEN 0
                            ELSE COALESCE(FromConvTableF41002.UMCNV1, FromConvTableF41003.UMCNV1, CASE
                                                                                                    WHEN F4211.SDUOM = F4101.IMUOM1 THEN 10000000
                                                                                                  END) / COALESCE(ToConvTableF41002.UMCNV1, ToConvTableF41003.UMCNV1, CASE
                                                                                                                                                                        WHEN N'TN' = F4101.IMUOM1 THEN 10000000
                                                                                                                                                                      END)
                          END)               ReportColumn10,
       SUM(F4211.SDUORG * CASE
                            WHEN ( ( COALESCE(FromConvTableF41002.UMCNV1, FromConvTableF41003.UMCNV1, CASE
                                                                                                        WHEN F4211.SDUOM = F4101.IMUOM1 THEN 10000000
                                                                                                      END) IS NULL )
                                    OR ( COALESCE(ToConvTableF41002.UMCNV1, ToConvTableF41003.UMCNV1, CASE
                                                                                                        WHEN N'TN' = F4101.IMUOM1 THEN 10000000
                                                                                                      END) IS NULL ) )
                                  OR ( COALESCE(ToConvTableF41002.UMCNV1, ToConvTableF41003.UMCNV1, CASE
                                                                                                      WHEN N'TN' = F4101.IMUOM1 THEN 10000000
                                                                                                    END) = 0 ) THEN 0
                            ELSE COALESCE(FromConvTableF41002.UMCNV1, FromConvTableF41003.UMCNV1, CASE
                                                                                                    WHEN F4211.SDUOM = F4101.IMUOM1 THEN 10000000
                                                                                                  END) / COALESCE(ToConvTableF41002.UMCNV1, ToConvTableF41003.UMCNV1, CASE
                                                                                                                                                                        WHEN N'TN' = F4101.IMUOM1 THEN 10000000
                                                                                                                                                                      END)
                          END)               ReportColumn11,
       F0010.CCCRCD                          DomesticCurrency
FROM   PRODDTA.F4211 F4211
       INNER JOIN PRODDTA.F4101 F4101
         ON F4211.SDITM = F4101.IMITM
       LEFT JOIN PRODDTA.F4074 F4074
         ON (( ( ( ( F4074.ALDCTO = F4211.SDDCTO )
                   AND ( F4074.ALDOCO = F4211.SDDOCO ) )
                 AND ( F4074.ALKCOO = F4211.SDKCOO ) )
               AND ( F4074.ALLNID = F4211.SDLNID ) ))
            AND (( ( F4074.ALAST IS NULL )
                    OR ( F4074.ALAST IN ( N'A03     ', N'CASLB   ', N'FRTHIDE ', N'FRTTAXN ',
                                          N'FRTTAXY ', N'PP06    ', N'PP07    ', N'PP08    ',
                                          N'PP13    ', N'PP15    ', N'PP17    ', N'PP26    ',
                                          N'PP37    ', N'PP50    ', N'PP51    ', N'PP56    ',
                                          N'PP57    ', N'PP97    ', N'PP99    ', N'PPSLB   ',
                                          N'COLPALN ', N'COLPALT ', N'ALST    ' ) ) ))
       LEFT JOIN (SELECT UMMCU UMMCU,
                         UMITM UMITM,
                         UMUM  UMUM,
                         CASE
                           WHEN COUNT(UMCNV1) > 1 THEN NULL
                           ELSE MIN(UMCNV1)
                         END   UMCNV1
                  FROM   (SELECT UMMCU  UMMCU,
                                 UMITM  UMITM,
                                 UMUM   UMUM,
                                 UMCNV1 UMCNV1
                          FROM   PRODDTA.F41002 F41002_1
                          UNION
                          SELECT UMMCU UMMCU,
                                 UMITM UMITM,
                                 UMRUM UMUM,
                                 CASE
                                   WHEN UMCONV = 0 THEN NULL
                                   ELSE ROUND(UMCNV1 / UMCONV * 10000000, 0)
                                 END   UMCNV1
                          FROM   PRODDTA.F41002 F41002_2) F41002_3
                  GROUP  BY UMMCU,
                            UMITM,
                            UMUM) FromConvTableF41002
         ON ( ( FromConvTableF41002.UMMCU = N'            ' )
              AND ( F4211.SDITM = FromConvTableF41002.UMITM ) )
            AND ( F4211.SDUOM = FromConvTableF41002.UMUM )
       LEFT JOIN (SELECT UMMCU UMMCU,
                         UMITM UMITM,
                         UMUM  UMUM,
                         CASE
                           WHEN COUNT(UMCNV1) > 1 THEN NULL
                           ELSE MIN(UMCNV1)
                         END   UMCNV1
                  FROM   (SELECT UMMCU  UMMCU,
                                 UMITM  UMITM,
                                 UMUM   UMUM,
                                 UMCNV1 UMCNV1
                          FROM   PRODDTA.F41002 F41002_1
                          UNION
                          SELECT UMMCU UMMCU,
                                 UMITM UMITM,
                                 UMRUM UMUM,
                                 CASE
                                   WHEN UMCONV = 0 THEN NULL
                                   ELSE ROUND(UMCNV1 / UMCONV * 10000000, 0)
                                 END   UMCNV1
                          FROM   PRODDTA.F41002 F41002_2) F41002_3
                  GROUP  BY UMMCU,
                            UMITM,
                            UMUM) ToConvTableF41002
         ON ( ( ToConvTableF41002.UMMCU = N'            ' )
              AND ( F4211.SDITM = ToConvTableF41002.UMITM ) )
            AND ( N'TN' = ToConvTableF41002.UMUM )
       LEFT JOIN (SELECT UMUM  UMUM,
                         UMRUM UMRUM,
                         CASE
                           WHEN COUNT(UMCNV1) > 1 THEN NULL
                           ELSE MIN(UMCNV1)
                         END   UMCNV1
                  FROM   (SELECT UCUM   UMUM,
                                 UCRUM  UMRUM,
                                 UCCONV UMCNV1
                          FROM   PRODDTA.F41003 F41003_1
                          UNION
                          SELECT UCRUM UMUM,
                                 UCUM  UMRUM,
                                 CASE
                                   WHEN UCCONV = 0 THEN NULL
                                   ELSE 10000000 / ( UCCONV / 10000000 )
                                 END   UMCNV1
                          FROM   PRODDTA.F41003 F41003_2) F41003_3
                  GROUP  BY UMUM,
                            UMRUM) FromConvTableF41003
         ON ( F4211.SDUOM = FromConvTableF41003.UMUM )
            AND ( F4101.IMUOM1 = FromConvTableF41003.UMRUM )
       LEFT JOIN (SELECT UMUM  UMUM,
                         UMRUM UMRUM,
                         CASE
                           WHEN COUNT(UMCNV1) > 1 THEN NULL
                           ELSE MIN(UMCNV1)
                         END   UMCNV1
                  FROM   (SELECT UCUM   UMUM,
                                 UCRUM  UMRUM,
                                 UCCONV UMCNV1
                          FROM   PRODDTA.F41003 F41003_1
                          UNION
                          SELECT UCRUM UMUM,
                                 UCUM  UMRUM,
                                 CASE
                                   WHEN UCCONV = 0 THEN NULL
                                   ELSE 10000000 / ( UCCONV / 10000000 )
                                 END   UMCNV1
                          FROM   PRODDTA.F41003 F41003_2) F41003_3
                  GROUP  BY UMUM,
                            UMRUM) ToConvTableF41003
         ON ( N'TN' = ToConvTableF41003.UMUM )
            AND ( F4101.IMUOM1 = ToConvTableF41003.UMRUM )
       INNER JOIN PRODDTA.F0010 F0010
         ON F0010.CCCO = F4211.SDKCOO
WHERE  ( (( (( (( (( (( (( (( (( F4211.SDMCU IN ( N'         061' ) ))
                              AND (( F4211.SDDCTO IN ( N'SO', N'CO' ) )) ))
                           AND (( F4211.SDLTTR < N'980' )) ))
                        AND (( F4211.SDNXTR IN ( N'999' ) )) ))
                     AND (( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) )) ))
                  AND (( F4211.SDDGL BETWEEN 116001 AND 116031 )) ))
               AND (( ( F4074.ALAST IS NULL )
                       OR ( F4074.ALAST IN ( N'A03     ', N'CASLB   ', N'FRTHIDE ', N'FRTTAXN ',
                                             N'FRTTAXY ', N'PP06    ', N'PP07    ', N'PP08    ',
                                             N'PP13    ', N'PP15    ', N'PP17    ', N'PP26    ',
                                             N'PP37    ', N'PP50    ', N'PP51    ', N'PP56    ',
                                             N'PP57    ', N'PP97    ', N'PP99    ', N'PPSLB   ',
                                             N'COLPALN ', N'COLPALT ', N'ALST    ' ) ) )) ))
            AND (( NOT ( F4101.IMSEG4 IN ( N'ZZ        ' ) ) )) ))
         AND ( F4211.SDGLC <> N'26AN' ) )
GROUP  BY F4211.SDKCOO,
          F4211.SDDOCO,
          F4211.SDDCTO,
          CAST(F4211.SDLNID AS FLOAT) / 1000,
          F4211.SDMCU,
          F4211.SDDOC,
          F4211.SDDGL,
          F4211.SDADDJ,
          F4211.SDTRDJ,
          F4211.SDLITM,
          F4101.IMDSC1,
          F4211.SDAN8,
          F4211.SDSHAN,
          F4211.SDLNTY,
          F4211.SDNXTR,
          F4211.SDSRP4,
          F4101.IMSEG4,
          F4211.SDURCD,
          F4074.ALAST,
          F4211.SDSRP3,
          F4211.SDSRP2,
          F4211.SDSRP1,
          CAST(F4211.SDUPRC AS FLOAT) / 1000000,
          CAST(F4074.ALUPRC AS FLOAT) / 1000000,
          F4211.SDUOM,
          F4074.ALUOM,
          F4211.SDGLC,
          FromConvTableF41002.UMCNV1,
          ToConvTableF41002.UMCNV1,
          FromConvTableF41003.UMCNV1,
          ToConvTableF41003.UMCNV1,
          F4101.IMUOM1,
          F0010.CCCRCD
ORDER  BY F4211_SDMCU ASC,
          F4211_SDDOCO ASC,
          F4211_SDLNID ASC,
          F4074_ALAST ASC,
          F4211_SDSHAN ASC,
          F4074_ALUPRC ASC,
          F4211_SDUPRC ASC,
          F4211_SDLITM ASC,
          F4211_SDGLC ASC,
          F4211_SDDCTO ASC,
          F4101_IMDSC1 ASC,
          F4211_SDLNTY ASC,
          F4211_SDDOC ASC,
          F4211_SDDGL ASC,
          F4101_IMSEG4 ASC,
          F4211_SDKCOO ASC,
          F4211_SDTRDJ ASC,
          F4211_SDSRP4 ASC,
          F4211_SDNXTR ASC,
          F4211_SDSRP3 ASC,
          F4211_SDSRP2 ASC,
          F4211_SDSRP1 ASC,
          F4074_ALUOM ASC,
          F4211_SDUOM ASC,
          F4211_SDADDJ ASC,
          F4211_SDURCD ASC,
          F4211_SDAN8 ASC 