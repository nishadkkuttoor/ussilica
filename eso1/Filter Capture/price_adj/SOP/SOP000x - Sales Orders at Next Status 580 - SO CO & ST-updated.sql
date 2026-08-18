SELECT F4211.SDURRF                          F4211_SDURRF,
       F4211.SDTORG                          F4211_SDTORG,
       F4211.SDUSER                          F4211_SDUSER,
       F4211.SDUPMJ                          F4211_SDUPMJ,
       F4211.SDKCOO                          F4211_SDKCOO,
       F4211.SDMCU                           F4211_SDMCU,
       F4211.SDSHAN                          F4211_SDSHAN,
       F4211.SDAN8                           F4211_SDAN8,
       F0101.ABALPH                          F0101_ABALPH,
       C.ALCTY1                              F0116_ALCTY1,
       C.ALADDS                              F0116_ALADDS,
       F4211.SDDCTO                          F4211_SDDCTO,
       F4211.SDDOCO                          F4211_SDDOCO,
       F4201.SHHOLD                          F4201_SHHOLD,
       F49211.UDDEFF                         F49211_UDDEFF,
       F4211.SDGLC                           F4211_SDGLC,
       F4211.SDLNTY                          F4211_SDLNTY,
       CAST(F4211.SDLNID AS FLOAT) / 1000    F4211_SDLNID,
       F4211.SDTRDJ                          F4211_SDTRDJ,
       F4211.SDADDJ                          F4211_SDADDJ,
       F4211.SDDGL                           F4211_SDDGL,
       F4211.SDDRQJ                          F4211_SDDRQJ,
       F4211.SDURAB                          F4211_SDURAB,
       F4211.SDSHPN                          F4211_SDSHPN,
       F4211.SDFRTH                          F4211_SDFRTH,
       F4211.SDCARS                          F4211_SDCARS,
       F4211.SDMOT                           F4211_SDMOT,
       F4211.SDCNID                          F4211_SDCNID,
       F4211.SDVR01                          F4211_SDVR01,
       F4211.SDLITM                          F4211_SDLITM,
       F4101.IMDSC1                          F4101_IMDSC1,
       F4211.SDLTTR                          F4211_SDLTTR,
       F4211.SDNXTR                          F4211_SDNXTR,
       F4211.SDASN                           F4211_SDASN,
       F4211.SDURCD                          F4211_SDURCD,
       F4211.SDPROV                          F4211_SDPROV,
       F4074.ALAST                           F4074_ALAST,
       F4211.SDITM                           F4211_SDITM,
       CAST(F4211.SDUPRC AS FLOAT) / 1000000 F4211_SDUPRC,
       CAST(F4074.ALUPRC AS FLOAT) / 1000000 F4074_ALUPRC,
       F4211.SDUOM                           F4211_SDUOM,
       F4074.ALUOM                           F4074_ALUOM,
       CAST(F4074.ALBSDVAL AS FLOAT) / 10000 F4074_ALBSDVAL,
       SUM(F4211.SDSOQS)                     ReportColumn8,
       SUM(F4211.SDAEXP)                     ReportColumn13,
       SUM(F4211.SDECST)                     ReportColumn17,
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
                          END)               ReportColumn20,
       SUM(F4211.SDPQOR)                     ReportColumn21,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'DRY', N'Dry' ) ) THEN F4211.SDSOQS
           END)                              ReportColumn23,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'DRY', N'Dry' ) ) THEN F4211.SDAEXP
           END)                              ReportColumn24,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'DRY', N'Dry' ) ) THEN F4211.SDECST
           END)                              ReportColumn25,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'DRY', N'Dry' ) ) THEN F4211.SDUORG * CASE
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
                                                                        END
           END)                              ReportColumn26,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'DRY', N'Dry' ) ) THEN F4211.SDPQOR
           END)                              ReportColumn27,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'DRY', N'Dry' ) ) THEN 1
             ELSE 0
           END)                              ReportColumn28,
       SUM(CASE
             WHEN ( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) )
                  AND ( F4211.SDAITM IN ( N'MISC BILLING             ', N'TRANSLOAD CHARGES        ', N'BANKING FEE              ', N'EXPEDITE FEE             ', N'                         ' ) ) THEN F4211.SDAEXP
           END)                              ReportColumn29,
       SUM(CASE
             WHEN ( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) )
                  AND ( F4211.SDAITM IN ( N'MISC BILLING             ', N'TRANSLOAD CHARGES        ', N'BANKING FEE              ', N'EXPEDITE FEE             ', N'                         ' ) ) THEN 1
             ELSE 0
           END)                              ReportColumn30,
       SUM(CASE
             WHEN F4074.ALAST = N'A03     ' THEN F4211.SDSOQS
           END)                              ReportColumn31,
       SUM(CASE
             WHEN F4074.ALAST = N'A03     ' THEN 1
             ELSE 0
           END)                              ReportColumn32,
       SUM(CASE
             WHEN F4211.SDPROV = N'1' THEN F4211.SDUORG * CASE
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
                                                          END
           END)                              ReportColumn33,
       SUM(CASE
             WHEN F4211.SDPROV = N'1' THEN 1
             ELSE 0
           END)                              ReportColumn34,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'FR1' THEN F4211.SDSOQS
           END)                              ReportColumn35,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'FR1' THEN F4211.SDAEXP
           END)                              ReportColumn36,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'FR1' THEN F4211.SDECST
           END)                              ReportColumn37,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'FR1' THEN F4211.SDUORG * CASE
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
                                                             END
           END)                              ReportColumn38,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'FR1' THEN F4211.SDPQOR
           END)                              ReportColumn39,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'FR1' THEN 1
             ELSE 0
           END)                              ReportColumn40,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'BIL', N'Bil', N'FRE', N'Fre',
                                 N'FUE', N'Fue', N'TRA', N'Tra' ) ) THEN F4211.SDSOQS
           END)                              ReportColumn41,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'BIL', N'Bil', N'FRE', N'Fre',
                                 N'FUE', N'Fue', N'TRA', N'Tra' ) ) THEN F4211.SDAEXP
           END)                              ReportColumn42,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'BIL', N'Bil', N'FRE', N'Fre',
                                 N'FUE', N'Fue', N'TRA', N'Tra' ) ) THEN F4211.SDECST
           END)                              ReportColumn43,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'BIL', N'Bil', N'FRE', N'Fre',
                                 N'FUE', N'Fue', N'TRA', N'Tra' ) ) THEN F4211.SDUORG * CASE
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
                                                                                        END
           END)                              ReportColumn44,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'BIL', N'Bil', N'FRE', N'Fre',
                                 N'FUE', N'Fue', N'TRA', N'Tra' ) ) THEN F4211.SDPQOR
           END)                              ReportColumn45,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'BIL', N'Bil', N'FRE', N'Fre',
                                 N'FUE', N'Fue', N'TRA', N'Tra' ) ) THEN 1
             ELSE 0
           END)                              ReportColumn46,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'ALA', N'Ala' ) ) THEN F4211.SDSOQS
           END)                              ReportColumn47,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'ALA', N'Ala' ) ) THEN F4211.SDAEXP
           END)                              ReportColumn48,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'ALA', N'Ala' ) ) THEN F4211.SDECST
           END)                              ReportColumn49,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'ALA', N'Ala' ) ) THEN F4211.SDUORG * CASE
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
                                                                        END
           END)                              ReportColumn50,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'ALA', N'Ala' ) ) THEN F4211.SDPQOR
           END)                              ReportColumn51,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'ALA', N'Ala' ) ) THEN 1
             ELSE 0
           END)                              ReportColumn52,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'ALA' THEN F4211.SDSOQS
           END)                              ReportColumn53,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'ALA' THEN F4211.SDAEXP
           END)                              ReportColumn54,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'ALA' THEN F4211.SDECST
           END)                              ReportColumn55,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'ALA' THEN F4211.SDUORG * CASE
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
                                                             END
           END)                              ReportColumn56,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'ALA' THEN F4211.SDPQOR
           END)                              ReportColumn57,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'ALA' THEN 1
             ELSE 0
           END)                              ReportColumn58,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'CAR' THEN F4211.SDSOQS
           END)                              ReportColumn59,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'CAR' THEN F4211.SDAEXP
           END)                              ReportColumn60,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'CAR' THEN F4211.SDECST
           END)                              ReportColumn61,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'CAR' THEN F4211.SDUORG * CASE
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
                                                             END
           END)                              ReportColumn62,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'CAR' THEN F4211.SDPQOR
           END)                              ReportColumn63,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'CAR' THEN 1
             ELSE 0
           END)                              ReportColumn64,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'RAI', N'Rai' ) ) THEN F4211.SDSOQS
           END)                              ReportColumn65,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'RAI', N'Rai' ) ) THEN F4211.SDAEXP
           END)                              ReportColumn66,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'RAI', N'Rai' ) ) THEN F4211.SDECST
           END)                              ReportColumn67,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'RAI', N'Rai' ) ) THEN F4211.SDUORG * CASE
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
                                                                        END
           END)                              ReportColumn68,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'RAI', N'Rai' ) ) THEN F4211.SDPQOR
           END)                              ReportColumn69,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'RAI', N'Rai' ) ) THEN 1
             ELSE 0
           END)                              ReportColumn70,
       SUM(CASE
             WHEN ( ( ( F4074.ALAST IN ( N'A03     ' ) )
                      AND ( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) ) )
                    AND ( NOT ( F4211.SDLITM IN ( N'MISC BILLING             ', N'EXPEDITE FEE             ', N'BANKING FEE              ', N'TRANSLOAD CHARGES        ',
                                                  N'DRYER TAILINGS           ', N'DRYER TAILING #1         ', N'DRYER TAILING #40        ' ) ) ) )
                  AND ( NOT ( F4211.SDURCD IN ( N'NP', N'N3' ) ) ) THEN F4211.SDUORG * CASE
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
                                                                                       END
           END)                              ReportColumn71,
       SUM(CASE
             WHEN ( ( ( F4074.ALAST IN ( N'A03     ' ) )
                      AND ( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) ) )
                    AND ( NOT ( F4211.SDLITM IN ( N'MISC BILLING             ', N'EXPEDITE FEE             ', N'BANKING FEE              ', N'TRANSLOAD CHARGES        ',
                                                  N'DRYER TAILINGS           ', N'DRYER TAILING #1         ', N'DRYER TAILING #40        ' ) ) ) )
                  AND ( NOT ( F4211.SDURCD IN ( N'NP', N'N3' ) ) ) THEN 1
             ELSE 0
           END)                              ReportColumn72,
       SUM(CASE
             WHEN ( ( F4211.SDURCD IN ( N'NP', N'N3' ) )
                    AND ( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) ) )
                  AND ( NOT ( F4211.SDLITM IN ( N'MISC BILLING             ', N'EXPEDITE FEE             ', N'BANKING FEE              ', N'TRANSLOAD CHARGES        ',
                                                N'DRYER TAILINGS           ', N'DRYER TAILING #1         ', N'DRYER TAILING #40        ' ) ) ) THEN F4211.SDUORG * CASE
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
                                                                                                                                                                   END
           END)                              ReportColumn73,
       SUM(CASE
             WHEN ( ( F4211.SDURCD IN ( N'NP', N'N3' ) )
                    AND ( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) ) )
                  AND ( NOT ( F4211.SDLITM IN ( N'MISC BILLING             ', N'EXPEDITE FEE             ', N'BANKING FEE              ', N'TRANSLOAD CHARGES        ',
                                                N'DRYER TAILINGS           ', N'DRYER TAILING #1         ', N'DRYER TAILING #40        ' ) ) ) THEN 1
             ELSE 0
           END)                              ReportColumn74,
       SUM(CASE
             WHEN ( F4074.ALAPRP1 = N'ACR' )
                   OR ( CASE
                          WHEN F4074.ALAST IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4074.ALAST), 1, 2)
                        END = N'PP' ) THEN F4211.SDSOQS
           END)                              ReportColumn75,
       SUM(CASE
             WHEN ( F4074.ALAPRP1 = N'ACR' )
                   OR ( CASE
                          WHEN F4074.ALAST IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4074.ALAST), 1, 2)
                        END = N'PP' ) THEN F4211.SDAEXP
           END)                              ReportColumn76,
       SUM(CASE
             WHEN ( F4074.ALAPRP1 = N'ACR' )
                   OR ( CASE
                          WHEN F4074.ALAST IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4074.ALAST), 1, 2)
                        END = N'PP' ) THEN F4211.SDECST
           END)                              ReportColumn77,
       SUM(CASE
             WHEN ( F4074.ALAPRP1 = N'ACR' )
                   OR ( CASE
                          WHEN F4074.ALAST IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4074.ALAST), 1, 2)
                        END = N'PP' ) THEN F4211.SDUORG * CASE
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
                                                          END
           END)                              ReportColumn78,
       SUM(CASE
             WHEN ( F4074.ALAPRP1 = N'ACR' )
                   OR ( CASE
                          WHEN F4074.ALAST IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4074.ALAST), 1, 2)
                        END = N'PP' ) THEN F4211.SDPQOR
           END)                              ReportColumn79,
       SUM(CASE
             WHEN ( F4074.ALAPRP1 = N'ACR' )
                   OR ( CASE
                          WHEN F4074.ALAST IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4074.ALAST), 1, 2)
                        END = N'PP' ) THEN 1
             ELSE 0
           END)                              ReportColumn80,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'FR2' THEN F4211.SDSOQS
           END)                              ReportColumn81,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'FR2' THEN F4211.SDAEXP
           END)                              ReportColumn82,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'FR2' THEN F4211.SDECST
           END)                              ReportColumn83,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'FR2' THEN F4211.SDUORG * CASE
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
                                                             END
           END)                              ReportColumn84,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'FR2' THEN F4211.SDPQOR
           END)                              ReportColumn85,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'FR2' THEN 1
             ELSE 0
           END)                              ReportColumn86,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'NON' THEN F4211.SDSOQS
           END)                              ReportColumn87,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'NON' THEN F4211.SDAEXP
           END)                              ReportColumn88,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'NON' THEN F4211.SDECST
           END)                              ReportColumn89,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'NON' THEN F4211.SDUORG * CASE
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
                                                             END
           END)                              ReportColumn90,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'NON' THEN F4211.SDPQOR
           END)                              ReportColumn91,
       SUM(CASE
             WHEN F4074.ALAPRP1 = N'NON' THEN 1
             ELSE 0
           END)                              ReportColumn92,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'HEA', N'Hea', N'MIS', N'Mis',
                                 N'PAL', N'Pal', N'SHA', N'Sha',
                                 N'WIR', N'Wir', N'SHR', N'Shr' ) ) THEN F4211.SDSOQS
           END)                              ReportColumn93,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'HEA', N'Hea', N'MIS', N'Mis',
                                 N'PAL', N'Pal', N'SHA', N'Sha',
                                 N'WIR', N'Wir', N'SHR', N'Shr' ) ) THEN F4211.SDAEXP
           END)                              ReportColumn94,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'HEA', N'Hea', N'MIS', N'Mis',
                                 N'PAL', N'Pal', N'SHA', N'Sha',
                                 N'WIR', N'Wir', N'SHR', N'Shr' ) ) THEN F4211.SDECST
           END)                              ReportColumn95,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'HEA', N'Hea', N'MIS', N'Mis',
                                 N'PAL', N'Pal', N'SHA', N'Sha',
                                 N'WIR', N'Wir', N'SHR', N'Shr' ) ) THEN F4211.SDUORG * CASE
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
                                                                                        END
           END)                              ReportColumn96,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'HEA', N'Hea', N'MIS', N'Mis',
                                 N'PAL', N'Pal', N'SHA', N'Sha',
                                 N'WIR', N'Wir', N'SHR', N'Shr' ) ) THEN F4211.SDPQOR
           END)                              ReportColumn97,
       SUM(CASE
             WHEN ( F4211.SDLNTY IN ( N'F ', N'FT' ) )
                  AND ( CASE
                          WHEN F4211.SDAITM IS NULL THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDAITM), 1, 3)
                        END IN ( N'HEA', N'Hea', N'MIS', N'Mis',
                                 N'PAL', N'Pal', N'SHA', N'Sha',
                                 N'WIR', N'Wir', N'SHR', N'Shr' ) ) THEN 1
             ELSE 0
           END)                              ReportColumn98,
       SUM(CASE
             WHEN ( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) )
                  AND ( F4211.SDLITM IN ( N'DRYER TAILINGS           ', N'DRYER TAILING #1         ', N'DRYER TAILING #40        ', N'DRYER TAILINGS #1        ', N'DRYER TAILINGS #40       ' ) ) THEN F4211.SDSOQS
           END)                              ReportColumn99,
       SUM(CASE
             WHEN ( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) )
                  AND ( F4211.SDLITM IN ( N'DRYER TAILINGS           ', N'DRYER TAILING #1         ', N'DRYER TAILING #40        ', N'DRYER TAILINGS #1        ', N'DRYER TAILINGS #40       ' ) ) THEN F4211.SDAEXP
           END)                              ReportColumn100,
       SUM(CASE
             WHEN ( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) )
                  AND ( F4211.SDLITM IN ( N'DRYER TAILINGS           ', N'DRYER TAILING #1         ', N'DRYER TAILING #40        ', N'DRYER TAILINGS #1        ', N'DRYER TAILINGS #40       ' ) ) THEN F4211.SDECST
           END)                              ReportColumn101,
       SUM(CASE
             WHEN ( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) )
                  AND ( F4211.SDLITM IN ( N'DRYER TAILINGS           ', N'DRYER TAILING #1         ', N'DRYER TAILING #40        ', N'DRYER TAILINGS #1        ', N'DRYER TAILINGS #40       ' ) ) THEN F4211.SDUORG * CASE
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
                                                                                                                                                                                                                       END
           END)                              ReportColumn102,
       SUM(CASE
             WHEN ( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) )
                  AND ( F4211.SDLITM IN ( N'DRYER TAILINGS           ', N'DRYER TAILING #1         ', N'DRYER TAILING #40        ', N'DRYER TAILINGS #1        ', N'DRYER TAILINGS #40       ' ) ) THEN F4211.SDPQOR
           END)                              ReportColumn103,
       SUM(CASE
             WHEN ( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) )
                  AND ( F4211.SDLITM IN ( N'DRYER TAILINGS           ', N'DRYER TAILING #1         ', N'DRYER TAILING #40        ', N'DRYER TAILINGS #1        ', N'DRYER TAILINGS #40       ' ) ) THEN 1
             ELSE 0
           END)                              ReportColumn104,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TM' )
                    AND ( F4211.SDUOM = N'BG' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDSOQS
           END)                              ReportColumn105,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TM' )
                    AND ( F4211.SDUOM = N'BG' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDAEXP
           END)                              ReportColumn106,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TM' )
                    AND ( F4211.SDUOM = N'BG' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDECST
           END)                              ReportColumn107,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TM' )
                    AND ( F4211.SDUOM = N'BG' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDUORG * CASE
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
                                                                        END
           END)                              ReportColumn108,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TM' )
                    AND ( F4211.SDUOM = N'BG' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDPQOR
           END)                              ReportColumn109,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TM' )
                    AND ( F4211.SDUOM = N'BG' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN 1
             ELSE 0
           END)                              ReportColumn110,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TN' )
                    AND ( F4211.SDUOM = N'TN' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDSOQS
           END)                              ReportColumn111,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TN' )
                    AND ( F4211.SDUOM = N'TN' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDAEXP
           END)                              ReportColumn112,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TN' )
                    AND ( F4211.SDUOM = N'TN' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDECST
           END)                              ReportColumn113,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TN' )
                    AND ( F4211.SDUOM = N'TN' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDUORG * CASE
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
                                                                        END
           END)                              ReportColumn114,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TN' )
                    AND ( F4211.SDUOM = N'TN' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDPQOR
           END)                              ReportColumn115,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TN' )
                    AND ( F4211.SDUOM = N'TN' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN 1
             ELSE 0
           END)                              ReportColumn116,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TN' )
                    AND ( F4211.SDUOM = N'BG' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDSOQS
           END)                              ReportColumn117,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TN' )
                    AND ( F4211.SDUOM = N'BG' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDAEXP
           END)                              ReportColumn118,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TN' )
                    AND ( F4211.SDUOM = N'BG' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDECST
           END)                              ReportColumn119,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TN' )
                    AND ( F4211.SDUOM = N'BG' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDUORG * CASE
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
                                                                        END
           END)                              ReportColumn120,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TN' )
                    AND ( F4211.SDUOM = N'BG' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDPQOR
           END)                              ReportColumn121,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TN' )
                    AND ( F4211.SDUOM = N'BG' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN 1
             ELSE 0
           END)                              ReportColumn122,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'BG' )
                    AND ( F4211.SDUOM = N'BG' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDSOQS
           END)                              ReportColumn123,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'BG' )
                    AND ( F4211.SDUOM = N'BG' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDAEXP
           END)                              ReportColumn124,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'BG' )
                    AND ( F4211.SDUOM = N'BG' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDECST
           END)                              ReportColumn125,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'BG' )
                    AND ( F4211.SDUOM = N'BG' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDUORG * CASE
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
                                                                        END
           END)                              ReportColumn126,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'BG' )
                    AND ( F4211.SDUOM = N'BG' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDPQOR
           END)                              ReportColumn127,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'BG' )
                    AND ( F4211.SDUOM = N'BG' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN 1
             ELSE 0
           END)                              ReportColumn128,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TM' )
                    AND ( F4211.SDUOM = N'TM' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDSOQS
           END)                              ReportColumn129,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TM' )
                    AND ( F4211.SDUOM = N'TM' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDAEXP
           END)                              ReportColumn130,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TM' )
                    AND ( F4211.SDUOM = N'TM' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDECST
           END)                              ReportColumn131,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TM' )
                    AND ( F4211.SDUOM = N'TM' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDUORG * CASE
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
                                                                        END
           END)                              ReportColumn132,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TM' )
                    AND ( F4211.SDUOM = N'TM' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN F4211.SDPQOR
           END)                              ReportColumn133,
       SUM(CASE
             WHEN ( ( F4074.ALUOM = N'TM' )
                    AND ( F4211.SDUOM = N'TM' ) )
                  AND ( F4074.ALAST = N'FRTHIDE ' ) THEN 1
             ELSE 0
           END)                              ReportColumn134,
       SUM(CASE
             WHEN F4211.SDLNTY IN ( N'F ', N'FT' ) THEN F4211.SDSOQS
           END)                              ReportColumn135,
       SUM(CASE
             WHEN F4211.SDLNTY IN ( N'F ', N'FT' ) THEN F4211.SDAEXP
           END)                              ReportColumn136,
       SUM(CASE
             WHEN F4211.SDLNTY IN ( N'F ', N'FT' ) THEN F4211.SDECST
           END)                              ReportColumn137,
       SUM(CASE
             WHEN F4211.SDLNTY IN ( N'F ', N'FT' ) THEN F4211.SDUORG * CASE
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
                                                                       END
           END)                              ReportColumn138,
       SUM(CASE
             WHEN F4211.SDLNTY IN ( N'F ', N'FT' ) THEN F4211.SDPQOR
           END)                              ReportColumn139,
       SUM(CASE
             WHEN F4211.SDLNTY IN ( N'F ', N'FT' ) THEN 1
             ELSE 0
           END)                              ReportColumn140,
       SUM(CASE
             WHEN ( F4074.ALAST IN ( N'A03     ' ) )
                  AND ( NOT ( F4211.SDURCD IN ( N'NP', N'N3' ) ) ) THEN F4211.SDSOQS
           END)                              ReportColumn141,
       SUM(CASE
             WHEN ( F4074.ALAST IN ( N'A03     ' ) )
                  AND ( NOT ( F4211.SDURCD IN ( N'NP', N'N3' ) ) ) THEN F4211.SDAEXP
           END)                              ReportColumn142,
       SUM(CASE
             WHEN ( F4074.ALAST IN ( N'A03     ' ) )
                  AND ( NOT ( F4211.SDURCD IN ( N'NP', N'N3' ) ) ) THEN F4211.SDECST
           END)                              ReportColumn143,
       SUM(CASE
             WHEN ( F4074.ALAST IN ( N'A03     ' ) )
                  AND ( NOT ( F4211.SDURCD IN ( N'NP', N'N3' ) ) ) THEN F4211.SDUORG * CASE
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
                                                                                       END
           END)                              ReportColumn144,
       SUM(CASE
             WHEN ( F4074.ALAST IN ( N'A03     ' ) )
                  AND ( NOT ( F4211.SDURCD IN ( N'NP', N'N3' ) ) ) THEN F4211.SDPQOR
           END)                              ReportColumn145,
       SUM(CASE
             WHEN ( F4074.ALAST IN ( N'A03     ' ) )
                  AND ( NOT ( F4211.SDURCD IN ( N'NP', N'N3' ) ) ) THEN 1
             ELSE 0
           END)                              ReportColumn146,
       SUM(CASE
             WHEN ( F4211.SDURCD IN ( N'NP', N'N3' ) )
                  AND ( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) ) THEN F4211.SDSOQS
           END)                              ReportColumn147,
       SUM(CASE
             WHEN ( F4211.SDURCD IN ( N'NP', N'N3' ) )
                  AND ( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) ) THEN F4211.SDAEXP
           END)                              ReportColumn148,
       SUM(CASE
             WHEN ( F4211.SDURCD IN ( N'NP', N'N3' ) )
                  AND ( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) ) THEN F4211.SDECST
           END)                              ReportColumn149,
       SUM(CASE
             WHEN ( F4211.SDURCD IN ( N'NP', N'N3' ) )
                  AND ( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) ) THEN F4211.SDUORG * CASE
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
                                                                                       END
           END)                              ReportColumn150,
       SUM(CASE
             WHEN ( F4211.SDURCD IN ( N'NP', N'N3' ) )
                  AND ( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) ) THEN F4211.SDPQOR
           END)                              ReportColumn151,
       SUM(CASE
             WHEN ( F4211.SDURCD IN ( N'NP', N'N3' ) )
                  AND ( NOT ( F4211.SDLNTY IN ( N'F ', N'FT' ) ) ) THEN 1
             ELSE 0
           END)                              ReportColumn152,
       F0010.CCCRCD                          DomesticCurrency
FROM   PRODDTA.F4211 F4211
       INNER JOIN (SELECT F0101.ABALPH,
                          F0101.ABAN8
                   FROM   PRODDTA.F0101 F0101
                   WHERE  ( ( F0101.ABAT1 BETWEEN N'A  ' AND N'P  ' )
                             OR ( F0101.ABAT1 BETWEEN N'R  ' AND N'ZZZ' ) )) F0101
         ON F0101.ABAN8 = F4211.SDSHAN
       INNER JOIN PRODDTA.F0116 C
                  INNER JOIN (SELECT F0116.ALAN8,
                                     MAX(F0116.ALEFTB) ALEFTB
                              FROM   PRODDTA.F0116 F0116
                              GROUP  BY F0116.ALAN8) D
                    ON ( C.ALAN8 = D.ALAN8 )
                       AND ( C.ALEFTB = D.ALEFTB )
         ON F0101.ABAN8 = D.ALAN8
       INNER JOIN PRODDTA.F4201 F4201
         ON ( ( F4201.SHDCTO = F4211.SDDCTO )
              AND ( F4201.SHDOCO = F4211.SDDOCO ) )
            AND ( F4201.SHKCOO = F4211.SDKCOO )
       INNER JOIN PRODDTA.F49211 F49211
         ON ( ( ( F49211.UDDCTO = F4211.SDDCTO )
                AND ( F49211.UDDOCO = F4211.SDDOCO ) )
              AND ( F49211.UDKCOO = F4211.SDKCOO ) )
            AND ( F49211.UDLNID = F4211.SDLNID )
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
WHERE  ( (( (( (( (( (( NOT ( F4211.SDDCTO IN ( N'SX' ) ) ))
                     AND (( F4211.SDTRDJ >= 115060 )) ))
                  AND (( F4211.SDLTTR < N'980' )) ))
               AND (( F4211.SDNXTR IN ( N'580' ) )) ))
            AND (( F4211.SDADDJ BETWEEN 126229 AND 126229 )) ))
         AND (( ( F4074.ALAST IS NULL )
                 OR ( F4074.ALAST IN ( N'A03     ', N'CASLB   ', N'FRTHIDE ', N'FRTTAXN ',
                                       N'FRTTAXY ', N'PP06    ', N'PP07    ', N'PP08    ',
                                       N'PP13    ', N'PP15    ', N'PP17    ', N'PP26    ',
                                       N'PP37    ', N'PP50    ', N'PP51    ', N'PP56    ',
                                       N'PP57    ', N'PP97    ', N'PP99    ', N'PPSLB   ',
                                       N'COLPALN ', N'COLPALT ', N'ALST    ' ) ) )) )
GROUP  BY F4211.SDURRF,
          F4211.SDTORG,
          F4211.SDUSER,
          F4211.SDUPMJ,
          F4211.SDKCOO,
          F4211.SDMCU,
          F4211.SDSHAN,
          F4211.SDAN8,
          F0101.ABALPH,
          C.ALCTY1,
          C.ALADDS,
          F4211.SDDCTO,
          F4211.SDDOCO,
          F4201.SHHOLD,
          F49211.UDDEFF,
          F4211.SDGLC,
          F4211.SDLNTY,
          CAST(F4211.SDLNID AS FLOAT) / 1000,
          F4211.SDTRDJ,
          F4211.SDADDJ,
          F4211.SDDGL,
          F4211.SDDRQJ,
          F4211.SDURAB,
          F4211.SDSHPN,
          F4211.SDFRTH,
          F4211.SDCARS,
          F4211.SDMOT,
          F4211.SDCNID,
          F4211.SDVR01,
          F4211.SDLITM,
          F4101.IMDSC1,
          F4211.SDLTTR,
          F4211.SDNXTR,
          F4211.SDASN,
          F4211.SDURCD,
          F4211.SDPROV,
          F4074.ALAST,
          F4211.SDITM,
          CAST(F4211.SDUPRC AS FLOAT) / 1000000,
          CAST(F4074.ALUPRC AS FLOAT) / 1000000,
          F4211.SDUOM,
          F4074.ALUOM,
          CAST(F4074.ALBSDVAL AS FLOAT) / 10000,
          FromConvTableF41002.UMCNV1,
          ToConvTableF41002.UMCNV1,
          FromConvTableF41003.UMCNV1,
          ToConvTableF41003.UMCNV1,
          F4101.IMUOM1,
          F0010.CCCRCD
ORDER  BY F4211_SDMCU ASC,
          F4211_SDSHAN ASC,
          F4211_SDDOCO ASC,
          F4211_SDLNID ASC,
          F4211_SDSHPN ASC,
          F4074_ALAST ASC,
          F4211_SDVR01 ASC,
          F4211_SDURAB ASC,
          F4074_ALUPRC ASC,
          F4211_SDUPRC ASC,
          F4211_SDLITM ASC,
          F4211_SDGLC ASC,
          F0101_ABALPH ASC,
          F0116_ALCTY1 ASC,
          F0116_ALADDS ASC,
          F4211_SDDCTO ASC,
          F4101_IMDSC1 ASC,
          F4211_SDLNTY ASC,
          F4211_SDADDJ ASC,
          F4211_SDCARS ASC,
          F4211_SDMOT ASC,
          F4211_SDCNID ASC,
          F4211_SDKCOO ASC,
          F4211_SDTRDJ ASC,
          F4211_SDITM ASC,
          F4211_SDPROV ASC,
          F4211_SDASN ASC,
          F4211_SDURCD ASC,
          F4211_SDUOM ASC,
          F4074_ALUOM ASC,
          F4074_ALBSDVAL ASC,
          F4211_SDLTTR ASC,
          F4211_SDNXTR ASC,
          F4201_SHHOLD ASC,
          F49211_UDDEFF ASC,
          F4211_SDUSER ASC,
          F4211_SDUPMJ ASC,
          F4211_SDURRF ASC,
          F4211_SDFRTH ASC,
          F4211_SDTORG ASC,
          F4211_SDAN8 ASC,
          F4211_SDDGL ASC,
          F4211_SDDRQJ ASC 