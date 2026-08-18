SELECT AggF4211.F4211_SDLITM              F4211_SDLITM,
       AggF4211.F4211_SDIVD               F4211_SDIVD,
       AggF4211.F4211_SDDOC               F4211_SDDOC,
       AggF4211.F4211_SDLNTY              F4211_SDLNTY,
       AggF4211.C_F4211_SDCARS            C_F4211_SDCARS,
       AggF4211.F4211_SDCARS              F4211_SDCARS,
       AggF4211.F4211_SDKCOO              F4211_SDKCOO,
       AggF4211.F4211_SDMCU               F4211_SDMCU,
       AggF4211.F4211_SDDCTO              F4211_SDDCTO,
       AggF4211.F4211_SDDOCO              F4211_SDDOCO,
       AggF4211.F4211_SDFRTH              F4211_SDFRTH,
       AggF4981.F4981_FHFRTH              F4981_FHFRTH,
       AggF4211.F4211_SDADDJ              F4211_SDADDJ,
       AggF4211.F4211_SDDGL               F4211_SDDGL,
       AggF4211.F4211_SDSHPN              F4211_SDSHPN,
       AggF4211.F4211_SDLNID              F4211_SDLNID,
       AggF4211.F4211_SDURAB              F4211_SDURAB,
       AggF4211.C_F4211_SDSHAN            C_F4211_SDSHAN,
       AggF4211.F4211_SDSHAN              F4211_SDSHAN,
       AggF4211.F0116_ALADD1              F0116_ALADD1,
       AggF4211.F0116_ALADD2              F0116_ALADD2,
       AggF4211.F0116_ALCTY1              F0116_ALCTY1,
       AggF4211.F0116_ALCTR               F0116_ALCTR,
       AggF4211.F0116_ALADDZ              F0116_ALADDZ,
       AggF4211.F0116_ALADDS              F0116_ALADDS,
       AggF4211.F4211_SDMOT               F4211_SDMOT,
       AggF4211.F4211_SDUOM               F4211_SDUOM,
       AggF4211.F4211_SDAPUM              F4211_SDAPUM,
       AggF4211.F4211_SDSRP2              F4211_SDSRP2,
       AggF4211.F4211_SDSRP4              F4211_SDSRP4,
       AggF4211.F41002_UMCONV             F41002_UMCONV,
       AggF4211.ID_CUSTOM_88bd843fb29f66f ID_CUSTOM_88bd843fb29f66f,
       AggF4211.F4074_ALFVTR              F4074_ALFVTR,
       AggF4211.F4941_RSRTN               F4941_RSRTN,
       AggF4211.ReportColumn4             ReportColumn4,
       AggF4981.ReportColumn18            ReportColumn18,
       AggF4981.ReportColumn19            ReportColumn19,
       AggF4211.ReportColumn20            ReportColumn20,
       AggF4981.ReportColumn21            ReportColumn21,
       AggF4211.ReportColumn22            ReportColumn22,
       AggF4211.ReportColumn23            ReportColumn23,
       AggF4981.ReportColumn24            ReportColumn24,
       AggF4211.ReportColumn25            ReportColumn25,
       AggF4211.ReportColumn26            ReportColumn26,
       AggF4981.ReportColumn27            ReportColumn27,
       AggF4211.ReportColumn28            ReportColumn28,
       AggF4981.ReportColumn29            ReportColumn29,
       AggF4211.ReportColumn30            ReportColumn30,
       AggF4981.ReportColumn31            ReportColumn31,
       AggF4211.ReportColumn32            ReportColumn32,
       AggF4211.ReportColumn33            ReportColumn33,
       AggF4981.ReportColumn34            ReportColumn34,
       AggF4211.ReportColumn35            ReportColumn35,
       AggF4981.RowIDX_F4981_2_XRowID,
       AggF4981.RowIDX_F4981_1_XRowID,
       AggF4981.RowIDX_F4981_0_XRowID
FROM   (SELECT RawF4211.XF4211_SDLITM             F4211_SDLITM,
               RawF4211.XF4211_SDIVD              F4211_SDIVD,
               RawF4211.XF4211_SDDOC              F4211_SDDOC,
               RawF4211.XF4211_SDLNTY             F4211_SDLNTY,
               RawF4211.XC_F4211_SDCARS           C_F4211_SDCARS,
               RawF4211.XF4211_SDCARS             F4211_SDCARS,
               RawF4211.XF4211_SDKCOO             F4211_SDKCOO,
               RawF4211.XF4211_SDMCU              F4211_SDMCU,
               RawF4211.XF4211_SDDCTO             F4211_SDDCTO,
               RawF4211.XF4211_SDDOCO             F4211_SDDOCO,
               RawF4211.XF4211_SDFRTH             F4211_SDFRTH,
               RawF4211.XF4981_FHFRTH             F4981_FHFRTH,
               RawF4211.XF4211_SDADDJ             F4211_SDADDJ,
               RawF4211.XF4211_SDDGL              F4211_SDDGL,
               RawF4211.XF4211_SDSHPN             F4211_SDSHPN,
               RawF4211.XF4211_SDLNID             F4211_SDLNID,
               RawF4211.XF4211_SDURAB             F4211_SDURAB,
               RawF4211.XC_F4211_SDSHAN           C_F4211_SDSHAN,
               RawF4211.XF4211_SDSHAN             F4211_SDSHAN,
               RawF4211.XF0116_ALADD1             F0116_ALADD1,
               RawF4211.XF0116_ALADD2             F0116_ALADD2,
               RawF4211.XF0116_ALCTY1             F0116_ALCTY1,
               RawF4211.XF0116_ALCTR              F0116_ALCTR,
               RawF4211.XF0116_ALADDZ             F0116_ALADDZ,
               RawF4211.XF0116_ALADDS             F0116_ALADDS,
               RawF4211.XF4211_SDMOT              F4211_SDMOT,
               RawF4211.XF4211_SDUOM              F4211_SDUOM,
               RawF4211.XF4211_SDAPUM             F4211_SDAPUM,
               RawF4211.XF4211_SDSRP2             F4211_SDSRP2,
               RawF4211.XF4211_SDSRP4             F4211_SDSRP4,
               RawF4211.XF41002_UMCONV            F41002_UMCONV,
               RawF4211.XID_CUSTOM_88bd8492b4b7a3 ID_CUSTOM_88bd843fb29f66f,
               RawF4211.XF4074_ALFVTR             F4074_ALFVTR,
               RawF4211.XF4941_RSRTN              F4941_RSRTN,
               SUM(RawF4211.ZReportColumn40001)   ReportColumn4,
               SUM(RawF4211.ZReportColumn200001)  ReportColumn20,
               SUM(RawF4211.ZReportColumn220001)  ReportColumn22,
               SUM(RawF4211.ZReportColumn230001)  ReportColumn23,
               SUM(RawF4211.ZReportColumn250001)  ReportColumn25,
               SUM(RawF4211.ZReportColumn260001)  ReportColumn26,
               SUM(RawF4211.ZReportColumn280001)  ReportColumn28,
               SUM(RawF4211.ZReportColumn300001)  ReportColumn30,
               SUM(RawF4211.ZReportColumn320001)  ReportColumn32,
               SUM(RawF4211.ZReportColumn330001)  ReportColumn33,
               SUM(RawF4211.ZReportColumn350001)  ReportColumn35
        FROM   (SELECT F4211.XF4211_SDLITM             XF4211_SDLITM,
                       F4211.XF4211_SDIVD              XF4211_SDIVD,
                       F4211.XF4211_SDDOC              XF4211_SDDOC,
                       F4211.XF4211_SDLNTY             XF4211_SDLNTY,
                       F4211.XC_F4211_SDCARS           XC_F4211_SDCARS,
                       F4211.XF4211_SDCARS             XF4211_SDCARS,
                       F4211.XF4211_SDKCOO             XF4211_SDKCOO,
                       F4211.XF4211_SDMCU              XF4211_SDMCU,
                       F4211.XF4211_SDDCTO             XF4211_SDDCTO,
                       F4211.XF4211_SDDOCO             XF4211_SDDOCO,
                       F4211.XF4211_SDFRTH             XF4211_SDFRTH,
                       F4211.XF4981_FHFRTH             XF4981_FHFRTH,
                       F4211.XF4211_SDADDJ             XF4211_SDADDJ,
                       F4211.XF4211_SDDGL              XF4211_SDDGL,
                       F4211.XF4211_SDSHPN             XF4211_SDSHPN,
                       F4211.XF4211_SDLNID             XF4211_SDLNID,
                       F4211.XF4211_SDURAB             XF4211_SDURAB,
                       F4211.XC_F4211_SDSHAN           XC_F4211_SDSHAN,
                       F4211.XF4211_SDSHAN             XF4211_SDSHAN,
                       F4211.XF0116_ALADD1             XF0116_ALADD1,
                       F4211.XF0116_ALADD2             XF0116_ALADD2,
                       F4211.XF0116_ALCTY1             XF0116_ALCTY1,
                       F4211.XF0116_ALCTR              XF0116_ALCTR,
                       F4211.XF0116_ALADDZ             XF0116_ALADDZ,
                       F4211.XF0116_ALADDS             XF0116_ALADDS,
                       F4211.XF4211_SDMOT              XF4211_SDMOT,
                       F4211.XF4211_SDUOM              XF4211_SDUOM,
                       F4211.XF4211_SDAPUM             XF4211_SDAPUM,
                       F4211.XF4211_SDSRP2             XF4211_SDSRP2,
                       F4211.XF4211_SDSRP4             XF4211_SDSRP4,
                       F4211.XF41002_UMCONV            XF41002_UMCONV,
                       F4211.XID_CUSTOM_88bd8492b4b7a3 XID_CUSTOM_88bd8492b4b7a3,
                       F4211.XF4074_ALFVTR             XF4074_ALFVTR,
                       F4211.XF4941_RSRTN              XF4941_RSRTN,
                       F4211.ZReportColumn40001        ZReportColumn40001,
                       CASE
                         WHEN SUM(F4211.ZReportColumn200001) = 0 THEN SUM(F4211.ZReportColumn200001)
                         ELSE SUM(F4211.ZReportColumn200001) / SUM(F4211.ZReportColumn200001)
                       END                             ZReportColumn200001,
                       CASE
                         WHEN SUM(F4211.ZReportColumn220001) = 0 THEN SUM(F4211.ZReportColumn220001)
                         ELSE SUM(F4211.ZReportColumn220001) / SUM(F4211.ZReportColumn220001)
                       END                             ZReportColumn220001,
                       CASE
                         WHEN SUM(F4211.ZReportColumn250001) = 0 THEN SUM(F4211.ZReportColumn230001)
                         ELSE SUM(F4211.ZReportColumn230001) / SUM(F4211.ZReportColumn250001)
                       END                             ZReportColumn230001,
                       CASE
                         WHEN SUM(F4211.ZReportColumn250001) = 0 THEN SUM(F4211.ZReportColumn250001)
                         ELSE SUM(F4211.ZReportColumn250001) / SUM(F4211.ZReportColumn250001)
                       END                             ZReportColumn250001,
                       CASE
                         WHEN SUM(F4211.ZReportColumn280001) = 0 THEN SUM(F4211.ZReportColumn260001)
                         ELSE SUM(F4211.ZReportColumn260001) / SUM(F4211.ZReportColumn280001)
                       END                             ZReportColumn260001,
                       CASE
                         WHEN SUM(F4211.ZReportColumn280001) = 0 THEN SUM(F4211.ZReportColumn280001)
                         ELSE SUM(F4211.ZReportColumn280001) / SUM(F4211.ZReportColumn280001)
                       END                             ZReportColumn280001,
                       CASE
                         WHEN SUM(F4211.ZReportColumn300001) = 0 THEN SUM(F4211.ZReportColumn300001)
                         ELSE SUM(F4211.ZReportColumn300001) / SUM(F4211.ZReportColumn300001)
                       END                             ZReportColumn300001,
                       CASE
                         WHEN SUM(F4211.ZReportColumn320001) = 0 THEN SUM(F4211.ZReportColumn320001)
                         ELSE SUM(F4211.ZReportColumn320001) / SUM(F4211.ZReportColumn320001)
                       END                             ZReportColumn320001,
                       CASE
                         WHEN SUM(F4211.ZReportColumn350001) = 0 THEN SUM(F4211.ZReportColumn330001)
                         ELSE SUM(F4211.ZReportColumn330001) / SUM(F4211.ZReportColumn350001)
                       END                             ZReportColumn330001,
                       CASE
                         WHEN SUM(F4211.ZReportColumn350001) = 0 THEN SUM(F4211.ZReportColumn350001)
                         ELSE SUM(F4211.ZReportColumn350001) / SUM(F4211.ZReportColumn350001)
                       END                             ZReportColumn350001,
                       F4211.PK__F4211__SDLNID         PK__F4211__SDLNID
                FROM   (SELECT DISTINCT F4211.SDLITM                            XF4211_SDLITM,
                                        F4211.SDIVD                             XF4211_SDIVD,
                                        F4211.SDDOC                             XF4211_SDDOC,
                                        F4211.SDLNTY                            XF4211_SDLNTY,
                                        F4211.SDCARS                            XC_F4211_SDCARS,
                                        F4211.SDCARS                            XF4211_SDCARS,
                                        F4211.SDKCOO                            XF4211_SDKCOO,
                                        F4211.SDMCU                             XF4211_SDMCU,
                                        F4211.SDDCTO                            XF4211_SDDCTO,
                                        F4211.SDDOCO                            XF4211_SDDOCO,
                                        F4211.SDFRTH                            XF4211_SDFRTH,
                                        F4981.FHFRTH                            XF4981_FHFRTH,
                                        F4211.SDADDJ                            XF4211_SDADDJ,
                                        F4211.SDDGL                             XF4211_SDDGL,
                                        F4211.SDSHPN                            XF4211_SDSHPN,
                                        CAST(F4211.SDLNID AS FLOAT) / 1000      XF4211_SDLNID,
                                        F4211.SDURAB                            XF4211_SDURAB,
                                        F4211.SDSHAN                            XC_F4211_SDSHAN,
                                        F4211.SDSHAN                            XF4211_SDSHAN,
                                        F0116.ALADD1                            XF0116_ALADD1,
                                        F0116.ALADD2                            XF0116_ALADD2,
                                        F0116.ALCTY1                            XF0116_ALCTY1,
                                        F0116.ALCTR                             XF0116_ALCTR,
                                        F0116.ALADDZ                            XF0116_ALADDZ,
                                        F0116.ALADDS                            XF0116_ALADDS,
                                        F4211.SDMOT                             XF4211_SDMOT,
                                        F4211.SDUOM                             XF4211_SDUOM,
                                        F4211.SDAPUM                            XF4211_SDAPUM,
                                        F4211.SDSRP2                            XF4211_SDSRP2,
                                        F4211.SDSRP4                            XF4211_SDSRP4,
                                        CAST(F41002.UMCONV AS FLOAT) / 10000000 XF41002_UMCONV,
                                        CASE
                                          WHEN F4211.SDUOM = N'TN' THEN 1
                                        END                                     XID_CUSTOM_88bd8492b4b7a3,
                                        CAST(F4074.ALFVTR AS FLOAT) / 10000     XF4074_ALFVTR,
                                        F4941.RSRTN                             XF4941_RSRTN,
                                        F4211.SDSOQS                            ZReportColumn40001,
                                        CASE
                                          WHEN ( ( F4981.FHBLPB = N'B' )
                                                 AND ( F4981.FHCGC1 = N'BFR' ) )
                                               AND ( F4981.FHVINV <> N'NULL                     ' ) THEN 1
                                          ELSE 0
                                        END                                     ZReportColumn200001,
                                        CASE
                                          WHEN ( ( F4981.FHBLPB = N'B' )
                                                 AND ( F4981.FHCGC1 IN ( N'FSC', N'FSB' ) ) )
                                               AND ( F4981.FHVINV <> N'NULL                     ' ) THEN 1
                                          ELSE 0
                                        END                                     ZReportColumn220001,
                                        CASE
                                          WHEN F4211.SDUOM = N'TN' THEN F4211.SDSOQS
                                        END                                     ZReportColumn230001,
                                        CASE
                                          WHEN F4211.SDUOM = N'TN' THEN 1
                                          ELSE 0
                                        END                                     ZReportColumn250001,
                                        CASE
                                          WHEN ( F4211.SDAPUM = N'TM' )
                                               AND ( F4211.SDUOM <> N'BG' ) THEN F4211.SDSOQS
                                        END                                     ZReportColumn260001,
                                        CASE
                                          WHEN ( F4211.SDAPUM = N'TM' )
                                               AND ( F4211.SDUOM <> N'BG' ) THEN 1
                                          ELSE 0
                                        END                                     ZReportColumn280001,
                                        CASE
                                          WHEN ( ( F4981.FHBLPB = N'P' )
                                                 AND ( F4981.FHCGC1 = N'FSC' ) )
                                               AND ( F4981.FHVINV <> N'NULL                     ' ) THEN 1
                                          ELSE 0
                                        END                                     ZReportColumn300001,
                                        CASE
                                          WHEN ( ( F4981.FHBLPB = N'P' )
                                                 AND ( F4981.FHCGC1 = N'PFR' ) )
                                               AND ( F4981.FHVINV <> N'NULL                     ' ) THEN 1
                                          ELSE 0
                                        END                                     ZReportColumn320001,
                                        CASE
                                          WHEN F4211.SDUOM <> N'BG' THEN F4211.SDSOQS
                                        END                                     ZReportColumn330001,
                                        CASE
                                          WHEN F4211.SDUOM <> N'BG' THEN 1
                                          ELSE 0
                                        END                                     ZReportColumn350001,
                                        F4211.SDLNID                            PK__F4211__SDLNID
                        FROM   PRODDTA.F4211 F4211
                               INNER JOIN (SELECT F0101.ABSIC,
                                                  F0101.ABAN8
                                           FROM   PRODDTA.F0101 F0101
                                           WHERE  ( ( F0101.ABAT1 BETWEEN N'A  ' AND N'P  ' )
                                                     OR ( F0101.ABAT1 BETWEEN N'R  ' AND N'ZZZ' ) )) F0101
                                 ON F4211.SDSHAN = F0101.ABAN8
                               INNER JOIN PRODDTA.F0116 F0116
                                 ON F4211.SDSHAN = F0116.ALAN8
                               LEFT JOIN PRODDTA.F4981 F4981
                                 ON F4211.SDSHPN = F4981.FHSHPN
                               LEFT JOIN PRODDTA.F41002 F41002
                                 ON ( ( F4211.SDAPUM = F41002.UMRUM )
                                      AND ( F4211.SDITM = F41002.UMITM ) )
                                    AND ( F4211.SDUOM = F41002.UMUM )
                               LEFT JOIN PRODDTA.F4074 F4074
                                 ON ( ( ( ( F4211.SDDCTO = F4074.ALDCTO )
                                          AND ( F4211.SDDOCO = F4074.ALDOCO ) )
                                        AND ( F4211.SDKCOO = F4074.ALKCOO ) )
                                      AND ( F4211.SDLNID = F4074.ALLNID ) )
                                    AND ( F4074.ALAST IN ( N'FRTHIDE ', N'FRTTAXY ', N'FRTTAXN ', N'EPDELFRT',
                                                           N'FRTNBP  ', N'POOLFSC ' ) )
                               LEFT JOIN PRODDTA.F4941 F4941
                                 ON F4211.SDSHPN = F4941.RSSHPN
                               LEFT JOIN dwtemp75510D79EC8FB2_jds CO_F4981_FHKCO
                                 ON F4981.FHKCO = CO_F4981_FHKCO.CO
                        WHERE  ( (( (( (( (( F4211.SDDGL BETWEEN 126182 AND 126196 ))
                                          AND (( F4211.SDFRTH IN ( N'DLV', N'PP ' ) )) ))
                                       AND (( F4211.SDLNTY IN ( N'S ' ) )) ))
                                    AND (( F4211.SDNXTR IN ( N'999' ) )) ))
                                 AND (( NOT ( F0101.ABSIC IN ( N'F         ' ) ) )) )) F4211
                GROUP  BY F4211.XF4211_SDLITM,
                          F4211.XF4211_SDIVD,
                          F4211.XF4211_SDDOC,
                          F4211.XF4211_SDLNTY,
                          F4211.XC_F4211_SDCARS,
                          F4211.XF4211_SDCARS,
                          F4211.XF4211_SDKCOO,
                          F4211.XF4211_SDMCU,
                          F4211.XF4211_SDDCTO,
                          F4211.XF4211_SDDOCO,
                          F4211.XF4211_SDFRTH,
                          F4211.XF4981_FHFRTH,
                          F4211.XF4211_SDADDJ,
                          F4211.XF4211_SDDGL,
                          F4211.XF4211_SDSHPN,
                          F4211.XF4211_SDLNID,
                          F4211.XF4211_SDURAB,
                          F4211.XC_F4211_SDSHAN,
                          F4211.XF4211_SDSHAN,
                          F4211.XF0116_ALADD1,
                          F4211.XF0116_ALADD2,
                          F4211.XF0116_ALCTY1,
                          F4211.XF0116_ALCTR,
                          F4211.XF0116_ALADDZ,
                          F4211.XF0116_ALADDS,
                          F4211.XF4211_SDMOT,
                          F4211.XF4211_SDUOM,
                          F4211.XF4211_SDAPUM,
                          F4211.XF4211_SDSRP2,
                          F4211.XF4211_SDSRP4,
                          F4211.XF41002_UMCONV,
                          F4211.XID_CUSTOM_88bd8492b4b7a3,
                          F4211.XF4074_ALFVTR,
                          F4211.XF4941_RSRTN,
                          F4211.ZReportColumn40001,
                          F4211.PK__F4211__SDLNID) RawF4211
        GROUP  BY RawF4211.XF4211_SDLITM,
                  RawF4211.XF4211_SDIVD,
                  RawF4211.XF4211_SDDOC,
                  RawF4211.XF4211_SDLNTY,
                  RawF4211.XC_F4211_SDCARS,
                  RawF4211.XF4211_SDCARS,
                  RawF4211.XF4211_SDKCOO,
                  RawF4211.XF4211_SDMCU,
                  RawF4211.XF4211_SDDCTO,
                  RawF4211.XF4211_SDDOCO,
                  RawF4211.XF4211_SDFRTH,
                  RawF4211.XF4981_FHFRTH,
                  RawF4211.XF4211_SDADDJ,
                  RawF4211.XF4211_SDDGL,
                  RawF4211.XF4211_SDSHPN,
                  RawF4211.XF4211_SDLNID,
                  RawF4211.XF4211_SDURAB,
                  RawF4211.XC_F4211_SDSHAN,
                  RawF4211.XF4211_SDSHAN,
                  RawF4211.XF0116_ALADD1,
                  RawF4211.XF0116_ALADD2,
                  RawF4211.XF0116_ALCTY1,
                  RawF4211.XF0116_ALCTR,
                  RawF4211.XF0116_ALADDZ,
                  RawF4211.XF0116_ALADDS,
                  RawF4211.XF4211_SDMOT,
                  RawF4211.XF4211_SDUOM,
                  RawF4211.XF4211_SDAPUM,
                  RawF4211.XF4211_SDSRP2,
                  RawF4211.XF4211_SDSRP4,
                  RawF4211.XF41002_UMCONV,
                  RawF4211.XID_CUSTOM_88bd8492b4b7a3,
                  RawF4211.XF4074_ALFVTR,
                  RawF4211.XF4941_RSRTN) AggF4211
       INNER JOIN (SELECT RawF4981.XF4211_SDLITM               F4211_SDLITM,
                          RawF4981.XF4211_SDIVD                F4211_SDIVD,
                          RawF4981.XF4211_SDDOC                F4211_SDDOC,
                          RawF4981.XF4211_SDLNTY               F4211_SDLNTY,
                          RawF4981.XC_F4211_SDCARS             C_F4211_SDCARS,
                          RawF4981.XF4211_SDCARS               F4211_SDCARS,
                          RawF4981.XF4211_SDKCOO               F4211_SDKCOO,
                          RawF4981.XF4211_SDMCU                F4211_SDMCU,
                          RawF4981.XF4211_SDDCTO               F4211_SDDCTO,
                          RawF4981.XF4211_SDDOCO               F4211_SDDOCO,
                          RawF4981.XF4211_SDFRTH               F4211_SDFRTH,
                          RawF4981.XF4981_FHFRTH               F4981_FHFRTH,
                          RawF4981.XF4211_SDADDJ               F4211_SDADDJ,
                          RawF4981.XF4211_SDDGL                F4211_SDDGL,
                          RawF4981.XF4211_SDSHPN               F4211_SDSHPN,
                          RawF4981.XF4211_SDLNID               F4211_SDLNID,
                          RawF4981.XF4211_SDURAB               F4211_SDURAB,
                          RawF4981.XC_F4211_SDSHAN             C_F4211_SDSHAN,
                          RawF4981.XF4211_SDSHAN               F4211_SDSHAN,
                          RawF4981.XF0116_ALADD1               F0116_ALADD1,
                          RawF4981.XF0116_ALADD2               F0116_ALADD2,
                          RawF4981.XF0116_ALCTY1               F0116_ALCTY1,
                          RawF4981.XF0116_ALCTR                F0116_ALCTR,
                          RawF4981.XF0116_ALADDZ               F0116_ALADDZ,
                          RawF4981.XF0116_ALADDS               F0116_ALADDS,
                          RawF4981.XF4211_SDMOT                F4211_SDMOT,
                          RawF4981.XF4211_SDUOM                F4211_SDUOM,
                          RawF4981.XF4211_SDAPUM               F4211_SDAPUM,
                          RawF4981.XF4211_SDSRP2               F4211_SDSRP2,
                          RawF4981.XF4211_SDSRP4               F4211_SDSRP4,
                          RawF4981.XF41002_UMCONV              F41002_UMCONV,
                          RawF4981.XID_CUSTOM_88bd8492b4b7a3   ID_CUSTOM_88bd843fb29f66f,
                          RawF4981.XF4074_ALFVTR               F4074_ALFVTR,
                          RawF4981.XF4941_RSRTN                F4941_RSRTN,
                          SUM(RawF4981.ZReportColumn180001)    ReportColumn18,
                          SUM(RawF4981.ZReportColumn190001)    ReportColumn19,
                          SUM(RawF4981.ZReportColumn200001)    ReportColumn20,
                          SUM(RawF4981.ZReportColumn210001)    ReportColumn21,
                          SUM(RawF4981.ZReportColumn220001)    ReportColumn22,
                          SUM(RawF4981.ZReportColumn240001)    ReportColumn24,
                          SUM(RawF4981.ZReportColumn250001)    ReportColumn25,
                          SUM(RawF4981.ZReportColumn270001)    ReportColumn27,
                          SUM(RawF4981.ZReportColumn280001)    ReportColumn28,
                          SUM(RawF4981.ZReportColumn290001)    ReportColumn29,
                          SUM(RawF4981.ZReportColumn300001)    ReportColumn30,
                          SUM(RawF4981.ZReportColumn310001)    ReportColumn31,
                          SUM(RawF4981.ZReportColumn320001)    ReportColumn32,
                          SUM(RawF4981.ZReportColumn340001)    ReportColumn34,
                          SUM(RawF4981.ZReportColumn350001)    ReportColumn35,
                          SUM(RawF4981.XRowIDX_F4981_0_XRowID) RowIDX_F4981_0_XRowID,
                          SUM(RawF4981.XRowIDX_F4981_1_XRowID) RowIDX_F4981_1_XRowID,
                          SUM(RawF4981.XRowIDX_F4981_2_XRowID) RowIDX_F4981_2_XRowID
                   FROM   (SELECT F4981.XF4211_SDLITM             XF4211_SDLITM,
                                  F4981.XF4211_SDIVD              XF4211_SDIVD,
                                  F4981.XF4211_SDDOC              XF4211_SDDOC,
                                  F4981.XF4211_SDLNTY             XF4211_SDLNTY,
                                  F4981.XC_F4211_SDCARS           XC_F4211_SDCARS,
                                  F4981.XF4211_SDCARS             XF4211_SDCARS,
                                  F4981.XF4211_SDKCOO             XF4211_SDKCOO,
                                  F4981.XF4211_SDMCU              XF4211_SDMCU,
                                  F4981.XF4211_SDDCTO             XF4211_SDDCTO,
                                  F4981.XF4211_SDDOCO             XF4211_SDDOCO,
                                  F4981.XF4211_SDFRTH             XF4211_SDFRTH,
                                  F4981.XF4981_FHFRTH             XF4981_FHFRTH,
                                  F4981.XF4211_SDADDJ             XF4211_SDADDJ,
                                  F4981.XF4211_SDDGL              XF4211_SDDGL,
                                  F4981.XF4211_SDSHPN             XF4211_SDSHPN,
                                  F4981.XF4211_SDLNID             XF4211_SDLNID,
                                  F4981.XF4211_SDURAB             XF4211_SDURAB,
                                  F4981.XC_F4211_SDSHAN           XC_F4211_SDSHAN,
                                  F4981.XF4211_SDSHAN             XF4211_SDSHAN,
                                  F4981.XF0116_ALADD1             XF0116_ALADD1,
                                  F4981.XF0116_ALADD2             XF0116_ALADD2,
                                  F4981.XF0116_ALCTY1             XF0116_ALCTY1,
                                  F4981.XF0116_ALCTR              XF0116_ALCTR,
                                  F4981.XF0116_ALADDZ             XF0116_ALADDZ,
                                  F4981.XF0116_ALADDS             XF0116_ALADDS,
                                  F4981.XF4211_SDMOT              XF4211_SDMOT,
                                  F4981.XF4211_SDUOM              XF4211_SDUOM,
                                  F4981.XF4211_SDAPUM             XF4211_SDAPUM,
                                  F4981.XF4211_SDSRP2             XF4211_SDSRP2,
                                  F4981.XF4211_SDSRP4             XF4211_SDSRP4,
                                  F4981.XF41002_UMCONV            XF41002_UMCONV,
                                  F4981.XID_CUSTOM_88bd8492b4b7a3 XID_CUSTOM_88bd8492b4b7a3,
                                  F4981.XF4074_ALFVTR             XF4074_ALFVTR,
                                  F4981.XF4941_RSRTN              XF4941_RSRTN,
                                  F4981.ZReportColumn180001       ZReportColumn180001,
                                  CASE
                                    WHEN SUM(F4981.ZReportColumn200001) = 0 THEN SUM(F4981.ZReportColumn190001)
                                    ELSE SUM(F4981.ZReportColumn190001) / SUM(F4981.ZReportColumn200001)
                                  END                             ZReportColumn190001,
                                  CASE
                                    WHEN SUM(F4981.ZReportColumn200001) = 0 THEN SUM(F4981.ZReportColumn200001)
                                    ELSE SUM(F4981.ZReportColumn200001) / SUM(F4981.ZReportColumn200001)
                                  END                             ZReportColumn200001,
                                  CASE
                                    WHEN SUM(F4981.ZReportColumn220001) = 0 THEN SUM(F4981.ZReportColumn210001)
                                    ELSE SUM(F4981.ZReportColumn210001) / SUM(F4981.ZReportColumn220001)
                                  END                             ZReportColumn210001,
                                  CASE
                                    WHEN SUM(F4981.ZReportColumn220001) = 0 THEN SUM(F4981.ZReportColumn220001)
                                    ELSE SUM(F4981.ZReportColumn220001) / SUM(F4981.ZReportColumn220001)
                                  END                             ZReportColumn220001,
                                  CASE
                                    WHEN SUM(F4981.ZReportColumn250001) = 0 THEN SUM(F4981.ZReportColumn240001)
                                    ELSE SUM(F4981.ZReportColumn240001) / SUM(F4981.ZReportColumn250001)
                                  END                             ZReportColumn240001,
                                  CASE
                                    WHEN SUM(F4981.ZReportColumn250001) = 0 THEN SUM(F4981.ZReportColumn250001)
                                    ELSE SUM(F4981.ZReportColumn250001) / SUM(F4981.ZReportColumn250001)
                                  END                             ZReportColumn250001,
                                  CASE
                                    WHEN SUM(F4981.ZReportColumn280001) = 0 THEN SUM(F4981.ZReportColumn270001)
                                    ELSE SUM(F4981.ZReportColumn270001) / SUM(F4981.ZReportColumn280001)
                                  END                             ZReportColumn270001,
                                  CASE
                                    WHEN SUM(F4981.ZReportColumn280001) = 0 THEN SUM(F4981.ZReportColumn280001)
                                    ELSE SUM(F4981.ZReportColumn280001) / SUM(F4981.ZReportColumn280001)
                                  END                             ZReportColumn280001,
                                  CASE
                                    WHEN SUM(F4981.ZReportColumn300001) = 0 THEN SUM(F4981.ZReportColumn290001)
                                    ELSE SUM(F4981.ZReportColumn290001) / SUM(F4981.ZReportColumn300001)
                                  END                             ZReportColumn290001,
                                  CASE
                                    WHEN SUM(F4981.ZReportColumn300001) = 0 THEN SUM(F4981.ZReportColumn300001)
                                    ELSE SUM(F4981.ZReportColumn300001) / SUM(F4981.ZReportColumn300001)
                                  END                             ZReportColumn300001,
                                  CASE
                                    WHEN SUM(F4981.ZReportColumn320001) = 0 THEN SUM(F4981.ZReportColumn310001)
                                    ELSE SUM(F4981.ZReportColumn310001) / SUM(F4981.ZReportColumn320001)
                                  END                             ZReportColumn310001,
                                  CASE
                                    WHEN SUM(F4981.ZReportColumn320001) = 0 THEN SUM(F4981.ZReportColumn320001)
                                    ELSE SUM(F4981.ZReportColumn320001) / SUM(F4981.ZReportColumn320001)
                                  END                             ZReportColumn320001,
                                  CASE
                                    WHEN SUM(F4981.ZReportColumn350001) = 0 THEN SUM(F4981.ZReportColumn340001)
                                    ELSE SUM(F4981.ZReportColumn340001) / SUM(F4981.ZReportColumn350001)
                                  END                             ZReportColumn340001,
                                  CASE
                                    WHEN SUM(F4981.ZReportColumn350001) = 0 THEN SUM(F4981.ZReportColumn350001)
                                    ELSE SUM(F4981.ZReportColumn350001) / SUM(F4981.ZReportColumn350001)
                                  END                             ZReportColumn350001,
                                  F4981.PK__F4981__FHUK01         PK__F4981__FHUK01,
                                  F4981.XRowIDX_F4981_0_XRowID    XRowIDX_F4981_0_XRowID,
                                  F4981.XRowIDX_F4981_1_XRowID    XRowIDX_F4981_1_XRowID,
                                  F4981.XRowIDX_F4981_2_XRowID    XRowIDX_F4981_2_XRowID
                           FROM   (SELECT DISTINCT F4211.SDLITM                                                         XF4211_SDLITM,
                                                   F4211.SDIVD                                                          XF4211_SDIVD,
                                                   F4211.SDDOC                                                          XF4211_SDDOC,
                                                   F4211.SDLNTY                                                         XF4211_SDLNTY,
                                                   F4211.SDCARS                                                         XC_F4211_SDCARS,
                                                   F4211.SDCARS                                                         XF4211_SDCARS,
                                                   F4211.SDKCOO                                                         XF4211_SDKCOO,
                                                   F4211.SDMCU                                                          XF4211_SDMCU,
                                                   F4211.SDDCTO                                                         XF4211_SDDCTO,
                                                   F4211.SDDOCO                                                         XF4211_SDDOCO,
                                                   F4211.SDFRTH                                                         XF4211_SDFRTH,
                                                   F4981.FHFRTH                                                         XF4981_FHFRTH,
                                                   F4211.SDADDJ                                                         XF4211_SDADDJ,
                                                   F4211.SDDGL                                                          XF4211_SDDGL,
                                                   F4211.SDSHPN                                                         XF4211_SDSHPN,
                                                   CAST(F4211.SDLNID AS FLOAT) / 1000                                   XF4211_SDLNID,
                                                   F4211.SDURAB                                                         XF4211_SDURAB,
                                                   F4211.SDSHAN                                                         XC_F4211_SDSHAN,
                                                   F4211.SDSHAN                                                         XF4211_SDSHAN,
                                                   F0116.ALADD1                                                         XF0116_ALADD1,
                                                   F0116.ALADD2                                                         XF0116_ALADD2,
                                                   F0116.ALCTY1                                                         XF0116_ALCTY1,
                                                   F0116.ALCTR                                                          XF0116_ALCTR,
                                                   F0116.ALADDZ                                                         XF0116_ALADDZ,
                                                   F0116.ALADDS                                                         XF0116_ALADDS,
                                                   F4211.SDMOT                                                          XF4211_SDMOT,
                                                   F4211.SDUOM                                                          XF4211_SDUOM,
                                                   F4211.SDAPUM                                                         XF4211_SDAPUM,
                                                   F4211.SDSRP2                                                         XF4211_SDSRP2,
                                                   F4211.SDSRP4                                                         XF4211_SDSRP4,
                                                   CAST(F41002.UMCONV AS FLOAT) / 10000000                              XF41002_UMCONV,
                                                   CASE
                                                     WHEN F4211.SDUOM = N'TN' THEN 1
                                                   END                                                                  XID_CUSTOM_88bd8492b4b7a3,
                                                   CAST(F4074.ALFVTR AS FLOAT) / 10000                                  XF4074_ALFVTR,
                                                   F4941.RSRTN                                                          XF4941_RSRTN,
                                                   F4981.FHNAMT * NVL(CO_F4981_FHKCO.ShiftFactor, 0.01)                 ZReportColumn180001,
                                                   CASE
                                                     WHEN ( ( F4981.FHBLPB = N'B' )
                                                            AND ( F4981.FHCGC1 = N'BFR' ) )
                                                          AND ( F4981.FHVINV <> N'NULL                     ' ) THEN F4981.FHNAMT
                                                   END * NVL(CO_F4981_FHKCO.ShiftFactor, 0.01)                          ZReportColumn190001,
                                                   CASE
                                                     WHEN ( ( F4981.FHBLPB = N'B' )
                                                            AND ( F4981.FHCGC1 = N'BFR' ) )
                                                          AND ( F4981.FHVINV <> N'NULL                     ' ) THEN 1
                                                     ELSE 0
                                                   END                                                                  ZReportColumn200001,
                                                   CASE
                                                     WHEN ( ( F4981.FHBLPB = N'B' )
                                                            AND ( F4981.FHCGC1 IN ( N'FSC', N'FSB' ) ) )
                                                          AND ( F4981.FHVINV <> N'NULL                     ' ) THEN F4981.FHNAMT
                                                   END * NVL(CO_F4981_FHKCO.ShiftFactor, 0.01)                          ZReportColumn210001,
                                                   CASE
                                                     WHEN ( ( F4981.FHBLPB = N'B' )
                                                            AND ( F4981.FHCGC1 IN ( N'FSC', N'FSB' ) ) )
                                                          AND ( F4981.FHVINV <> N'NULL                     ' ) THEN 1
                                                     ELSE 0
                                                   END                                                                  ZReportColumn220001,
                                                   CASE
                                                     WHEN F4211.SDUOM = N'TN' THEN F4981.FHNAMT
                                                   END * NVL(CO_F4981_FHKCO.ShiftFactor, 0.01)                          ZReportColumn240001,
                                                   CASE
                                                     WHEN F4211.SDUOM = N'TN' THEN 1
                                                     ELSE 0
                                                   END                                                                  ZReportColumn250001,
                                                   CASE
                                                     WHEN ( F4211.SDAPUM = N'TM' )
                                                          AND ( F4211.SDUOM <> N'BG' ) THEN F4981.FHNAMT
                                                   END * NVL(CO_F4981_FHKCO.ShiftFactor, 0.01)                          ZReportColumn270001,
                                                   CASE
                                                     WHEN ( F4211.SDAPUM = N'TM' )
                                                          AND ( F4211.SDUOM <> N'BG' ) THEN 1
                                                     ELSE 0
                                                   END                                                                  ZReportColumn280001,
                                                   CASE
                                                     WHEN ( ( F4981.FHBLPB = N'P' )
                                                            AND ( F4981.FHCGC1 = N'FSC' ) )
                                                          AND ( F4981.FHVINV <> N'NULL                     ' ) THEN F4981.FHNAMT
                                                   END * NVL(CO_F4981_FHKCO.ShiftFactor, 0.01)                          ZReportColumn290001,
                                                   CASE
                                                     WHEN ( ( F4981.FHBLPB = N'P' )
                                                            AND ( F4981.FHCGC1 = N'FSC' ) )
                                                          AND ( F4981.FHVINV <> N'NULL                     ' ) THEN 1
                                                     ELSE 0
                                                   END                                                                  ZReportColumn300001,
                                                   CASE
                                                     WHEN ( ( F4981.FHBLPB = N'P' )
                                                            AND ( F4981.FHCGC1 = N'PFR' ) )
                                                          AND ( F4981.FHVINV <> N'NULL                     ' ) THEN F4981.FHNAMT
                                                   END * NVL(CO_F4981_FHKCO.ShiftFactor, 0.01)                          ZReportColumn310001,
                                                   CASE
                                                     WHEN ( ( F4981.FHBLPB = N'P' )
                                                            AND ( F4981.FHCGC1 = N'PFR' ) )
                                                          AND ( F4981.FHVINV <> N'NULL                     ' ) THEN 1
                                                     ELSE 0
                                                   END                                                                  ZReportColumn320001,
                                                   CASE
                                                     WHEN F4211.SDUOM <> N'BG' THEN F4981.FHNAMT
                                                   END * NVL(CO_F4981_FHKCO.ShiftFactor, 0.01)                          ZReportColumn340001,
                                                   CASE
                                                     WHEN F4211.SDUOM <> N'BG' THEN 1
                                                     ELSE 0
                                                   END                                                                  ZReportColumn350001,
                                                   F4981.FHUK01                                                         PK__F4981__FHUK01,
                                                   DBMS_UTILITY.GET_HASH_VALUE(TO_CHAR(F4981.FHUK01)
                                                                                || N'_InDeX_0_SaLt', -1048576, 2097152) XRowIDX_F4981_0_XRowID,
                                                   DBMS_UTILITY.GET_HASH_VALUE(TO_CHAR(F4981.FHUK01)
                                                                                || N'_InDeX_1_SaLt', -1048576, 2097152) XRowIDX_F4981_1_XRowID,
                                                   DBMS_UTILITY.GET_HASH_VALUE(TO_CHAR(F4981.FHUK01)
                                                                                || N'_InDeX_2_SaLt', -1048576, 2097152) XRowIDX_F4981_2_XRowID
                                   FROM   PRODDTA.F4211 F4211
                                          INNER JOIN (SELECT F0101.ABSIC,
                                                             F0101.ABAN8
                                                      FROM   PRODDTA.F0101 F0101
                                                      WHERE  ( ( F0101.ABAT1 BETWEEN N'A  ' AND N'P  ' )
                                                                OR ( F0101.ABAT1 BETWEEN N'R  ' AND N'ZZZ' ) )) F0101
                                            ON F4211.SDSHAN = F0101.ABAN8
                                          INNER JOIN PRODDTA.F0116 F0116
                                            ON F4211.SDSHAN = F0116.ALAN8
                                          LEFT JOIN PRODDTA.F4981 F4981
                                            ON F4211.SDSHPN = F4981.FHSHPN
                                          LEFT JOIN PRODDTA.F41002 F41002
                                            ON ( ( F4211.SDAPUM = F41002.UMRUM )
                                                 AND ( F4211.SDITM = F41002.UMITM ) )
                                               AND ( F4211.SDUOM = F41002.UMUM )
                                          LEFT JOIN PRODDTA.F4074 F4074
                                            ON ( ( ( ( F4211.SDDCTO = F4074.ALDCTO )
                                                     AND ( F4211.SDDOCO = F4074.ALDOCO ) )
                                                   AND ( F4211.SDKCOO = F4074.ALKCOO ) )
                                                 AND ( F4211.SDLNID = F4074.ALLNID ) )
                                               AND ( F4074.ALAST IN ( N'FRTHIDE ', N'FRTTAXY ', N'FRTTAXN ', N'EPDELFRT',
                                                                      N'FRTNBP  ', N'POOLFSC ' ) )
                                          LEFT JOIN PRODDTA.F4941 F4941
                                            ON F4211.SDSHPN = F4941.RSSHPN
                                          LEFT JOIN dwtemp75510D79EC8FB2_jds CO_F4981_FHKCO
                                            ON F4981.FHKCO = CO_F4981_FHKCO.CO
                                   WHERE  ( (( (( (( (( F4211.SDDGL BETWEEN 126182 AND 126196 ))
                                                     AND (( F4211.SDFRTH IN ( N'DLV', N'PP ' ) )) ))
                                                  AND (( F4211.SDLNTY IN ( N'S ' ) )) ))
                                               AND (( F4211.SDNXTR IN ( N'999' ) )) ))
                                            AND (( NOT ( F0101.ABSIC IN ( N'F         ' ) ) )) )) F4981
                           GROUP  BY F4981.XF4211_SDLITM,
                                     F4981.XF4211_SDIVD,
                                     F4981.XF4211_SDDOC,
                                     F4981.XF4211_SDLNTY,
                                     F4981.XC_F4211_SDCARS,
                                     F4981.XF4211_SDCARS,
                                     F4981.XF4211_SDKCOO,
                                     F4981.XF4211_SDMCU,
                                     F4981.XF4211_SDDCTO,
                                     F4981.XF4211_SDDOCO,
                                     F4981.XF4211_SDFRTH,
                                     F4981.XF4981_FHFRTH,
                                     F4981.XF4211_SDADDJ,
                                     F4981.XF4211_SDDGL,
                                     F4981.XF4211_SDSHPN,
                                     F4981.XF4211_SDLNID,
                                     F4981.XF4211_SDURAB,
                                     F4981.XC_F4211_SDSHAN,
                                     F4981.XF4211_SDSHAN,
                                     F4981.XF0116_ALADD1,
                                     F4981.XF0116_ALADD2,
                                     F4981.XF0116_ALCTY1,
                                     F4981.XF0116_ALCTR,
                                     F4981.XF0116_ALADDZ,
                                     F4981.XF0116_ALADDS,
                                     F4981.XF4211_SDMOT,
                                     F4981.XF4211_SDUOM,
                                     F4981.XF4211_SDAPUM,
                                     F4981.XF4211_SDSRP2,
                                     F4981.XF4211_SDSRP4,
                                     F4981.XF41002_UMCONV,
                                     F4981.XID_CUSTOM_88bd8492b4b7a3,
                                     F4981.XF4074_ALFVTR,
                                     F4981.XF4941_RSRTN,
                                     F4981.ZReportColumn180001,
                                     F4981.PK__F4981__FHUK01,
                                     F4981.XRowIDX_F4981_0_XRowID,
                                     F4981.XRowIDX_F4981_1_XRowID,
                                     F4981.XRowIDX_F4981_2_XRowID,
                                     F4981.XRowIDX_F4981_0_XRowID,
                                     F4981.XRowIDX_F4981_1_XRowID,
                                     F4981.XRowIDX_F4981_2_XRowID) RawF4981
                   GROUP  BY RawF4981.XF4211_SDLITM,
                             RawF4981.XF4211_SDIVD,
                             RawF4981.XF4211_SDDOC,
                             RawF4981.XF4211_SDLNTY,
                             RawF4981.XC_F4211_SDCARS,
                             RawF4981.XF4211_SDCARS,
                             RawF4981.XF4211_SDKCOO,
                             RawF4981.XF4211_SDMCU,
                             RawF4981.XF4211_SDDCTO,
                             RawF4981.XF4211_SDDOCO,
                             RawF4981.XF4211_SDFRTH,
                             RawF4981.XF4981_FHFRTH,
                             RawF4981.XF4211_SDADDJ,
                             RawF4981.XF4211_SDDGL,
                             RawF4981.XF4211_SDSHPN,
                             RawF4981.XF4211_SDLNID,
                             RawF4981.XF4211_SDURAB,
                             RawF4981.XC_F4211_SDSHAN,
                             RawF4981.XF4211_SDSHAN,
                             RawF4981.XF0116_ALADD1,
                             RawF4981.XF0116_ALADD2,
                             RawF4981.XF0116_ALCTY1,
                             RawF4981.XF0116_ALCTR,
                             RawF4981.XF0116_ALADDZ,
                             RawF4981.XF0116_ALADDS,
                             RawF4981.XF4211_SDMOT,
                             RawF4981.XF4211_SDUOM,
                             RawF4981.XF4211_SDAPUM,
                             RawF4981.XF4211_SDSRP2,
                             RawF4981.XF4211_SDSRP4,
                             RawF4981.XF41002_UMCONV,
                             RawF4981.XID_CUSTOM_88bd8492b4b7a3,
                             RawF4981.XF4074_ALFVTR,
                             RawF4981.XF4941_RSRTN) AggF4981
         ON ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( ( NVL(AggF4211.F4211_SDLITM, N'---COM.D11S---Null--') = NVL(AggF4981.F4211_SDLITM, N'---COM.D11S---Null--') )
                                                                          AND ( NVL(AggF4211.F4211_SDIVD, 1E-05) = NVL(AggF4981.F4211_SDIVD, 1E-05) ) )
                                                                        AND ( NVL(AggF4211.F4211_SDDOC, 1E-05) = NVL(AggF4981.F4211_SDDOC, 1E-05) ) )
                                                                      AND ( NVL(AggF4211.F4211_SDLNTY, N'---COM.D11S---Null--') = NVL(AggF4981.F4211_SDLNTY, N'---COM.D11S---Null--') ) )
                                                                    AND ( NVL(TO_CHAR(AggF4211.C_F4211_SDCARS), N'---COM.D11S---Null--') = NVL(TO_CHAR(AggF4981.C_F4211_SDCARS), N'---COM.D11S---Null--') ) )
                                                                  AND ( NVL(AggF4211.F4211_SDCARS, 1E-05) = NVL(AggF4981.F4211_SDCARS, 1E-05) ) )
                                                                AND ( NVL(AggF4211.F4211_SDKCOO, N'---COM.D11S---Null--') = NVL(AggF4981.F4211_SDKCOO, N'---COM.D11S---Null--') ) )
                                                              AND ( NVL(AggF4211.F4211_SDMCU, N'---COM.D11S---Null--') = NVL(AggF4981.F4211_SDMCU, N'---COM.D11S---Null--') ) )
                                                            AND ( NVL(AggF4211.F4211_SDDCTO, N'---COM.D11S---Null--') = NVL(AggF4981.F4211_SDDCTO, N'---COM.D11S---Null--') ) )
                                                          AND ( NVL(AggF4211.F4211_SDDOCO, 1E-05) = NVL(AggF4981.F4211_SDDOCO, 1E-05) ) )
                                                        AND ( NVL(AggF4211.F4211_SDFRTH, N'---COM.D11S---Null--') = NVL(AggF4981.F4211_SDFRTH, N'---COM.D11S---Null--') ) )
                                                      AND ( NVL(AggF4211.F4981_FHFRTH, N'---COM.D11S---Null--') = NVL(AggF4981.F4981_FHFRTH, N'---COM.D11S---Null--') ) )
                                                    AND ( NVL(AggF4211.F4211_SDADDJ, 1E-05) = NVL(AggF4981.F4211_SDADDJ, 1E-05) ) )
                                                  AND ( NVL(AggF4211.F4211_SDDGL, 1E-05) = NVL(AggF4981.F4211_SDDGL, 1E-05) ) )
                                                AND ( NVL(AggF4211.F4211_SDSHPN, 1E-05) = NVL(AggF4981.F4211_SDSHPN, 1E-05) ) )
                                              AND ( NVL(AggF4211.F4211_SDLNID, 1E-05) = NVL(AggF4981.F4211_SDLNID, 1E-05) ) )
                                            AND ( NVL(AggF4211.F4211_SDURAB, 1E-05) = NVL(AggF4981.F4211_SDURAB, 1E-05) ) )
                                          AND ( NVL(TO_CHAR(AggF4211.C_F4211_SDSHAN), N'---COM.D11S---Null--') = NVL(TO_CHAR(AggF4981.C_F4211_SDSHAN), N'---COM.D11S---Null--') ) )
                                        AND ( NVL(AggF4211.F4211_SDSHAN, 1E-05) = NVL(AggF4981.F4211_SDSHAN, 1E-05) ) )
                                      AND ( NVL(AggF4211.F0116_ALADD1, N'---COM.D11S---Null--') = NVL(AggF4981.F0116_ALADD1, N'---COM.D11S---Null--') ) )
                                    AND ( NVL(AggF4211.F0116_ALADD2, N'---COM.D11S---Null--') = NVL(AggF4981.F0116_ALADD2, N'---COM.D11S---Null--') ) )
                                  AND ( NVL(AggF4211.F0116_ALCTY1, N'---COM.D11S---Null--') = NVL(AggF4981.F0116_ALCTY1, N'---COM.D11S---Null--') ) )
                                AND ( NVL(AggF4211.F0116_ALCTR, N'---COM.D11S---Null--') = NVL(AggF4981.F0116_ALCTR, N'---COM.D11S---Null--') ) )
                              AND ( NVL(AggF4211.F0116_ALADDZ, N'---COM.D11S---Null--') = NVL(AggF4981.F0116_ALADDZ, N'---COM.D11S---Null--') ) )
                            AND ( NVL(AggF4211.F0116_ALADDS, N'---COM.D11S---Null--') = NVL(AggF4981.F0116_ALADDS, N'---COM.D11S---Null--') ) )
                          AND ( NVL(AggF4211.F4211_SDMOT, N'---COM.D11S---Null--') = NVL(AggF4981.F4211_SDMOT, N'---COM.D11S---Null--') ) )
                        AND ( NVL(AggF4211.F4211_SDUOM, N'---COM.D11S---Null--') = NVL(AggF4981.F4211_SDUOM, N'---COM.D11S---Null--') ) )
                      AND ( NVL(AggF4211.F4211_SDAPUM, N'---COM.D11S---Null--') = NVL(AggF4981.F4211_SDAPUM, N'---COM.D11S---Null--') ) )
                    AND ( NVL(AggF4211.F4211_SDSRP2, N'---COM.D11S---Null--') = NVL(AggF4981.F4211_SDSRP2, N'---COM.D11S---Null--') ) )
                  AND ( NVL(AggF4211.F4211_SDSRP4, N'---COM.D11S---Null--') = NVL(AggF4981.F4211_SDSRP4, N'---COM.D11S---Null--') ) )
                AND ( NVL(AggF4211.F41002_UMCONV, 1E-05) = NVL(AggF4981.F41002_UMCONV, 1E-05) ) )
              AND ( NVL(AggF4211.F4074_ALFVTR, 1E-05) = NVL(AggF4981.F4074_ALFVTR, 1E-05) ) )
            AND ( NVL(AggF4211.F4941_RSRTN, 1E-05) = NVL(AggF4981.F4941_RSRTN, 1E-05) )
ORDER  BY F4211_SDMCU ASC,
          F4211_SDDOCO ASC,
          F4211_SDSHPN ASC,
          F4211_SDFRTH ASC,
          F4211_SDAPUM ASC,
          F4211_SDUOM ASC,
          F41002_UMCONV ASC,
          F4074_ALFVTR ASC,
          F4211_SDIVD ASC,
          F4211_SDDOC ASC,
          F4211_SDLNTY ASC,
          F4211_SDCARS ASC,
          F4211_SDKCOO ASC,
          F4211_SDDCTO ASC,
          F4211_SDADDJ ASC,
          F4211_SDDGL ASC,
          F4211_SDURAB ASC,
          F4211_SDSHAN ASC,
          F4211_SDMOT ASC,
          F0116_ALADD1 ASC,
          F0116_ALADD2 ASC,
          F0116_ALCTY1 ASC,
          F0116_ALCTR ASC,
          F0116_ALADDZ ASC,
          F0116_ALADDS ASC,
          F4211_SDSRP2 ASC,
          F4211_SDSRP4 ASC,
          F4941_RSRTN ASC,
          F4981_FHFRTH ASC,
          F4211_SDLITM ASC,
          F4211_SDLNID ASC,
          ID_CUSTOM_88bd843fb29f66f ASC 