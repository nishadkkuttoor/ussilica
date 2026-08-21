SELECT RawF4211.XC_F4211_SDAN8              C_F4211_SDAN8,
       RawF4211.XF4211_SDAN8                F4211_SDAN8,
       RawF4211.XF4211_SDSHAN               F4211_SDSHAN,
       RawF4211.XF0101_ABALPH               F0101_ABALPH,
       RawF4211.XF4211_SDMCU                F4211_SDMCU,
       RawF4211.XF0116_ALADD1               F0116_ALADD1,
       RawF4211.XF0116_ALADD2               F0116_ALADD2,
       RawF4211.XF0116_ALCTY1               F0116_ALCTY1,
       RawF4211.XF0116_ALADDS               F0116_ALADDS,
       RawF4211.XF0116_ALADDZ               F0116_ALADDZ,
       RawF4211.XF4211_SDDOCO               F4211_SDDOCO,
       RawF4211.XF4211_SDDCT                F4211_SDDCT,
       RawF4211.XF4211_SDDOC                F4211_SDDOC,
       RawF4211.XF4211_SDIVD                F4211_SDIVD,
       RawF4211.XF4211_SDLNID               F4211_SDLNID,
       RawF4211.XF4211_SDHOLD               F4211_SDHOLD,
       RawF4211.XF4211_SDDCTO               F4211_SDDCTO,
       RawF4211.XF4211_SDTRDJ               F4211_SDTRDJ,
       RawF4211.XF4211_SDDRQJ               F4211_SDDRQJ,
       RawF4211.XF4211_SDADDJ               F4211_SDADDJ,
       RawF4211.XF4211_SDLITM               F4211_SDLITM,
       RawF4211.XF4211_SDMOT                F4211_SDMOT,
       RawF4211.XF4211_SDFRTH               F4211_SDFRTH,
       RawF4211.XF4211_SDUOM                F4211_SDUOM,
       RawF4211.XF4211_SDCNID               F4211_SDCNID,
       RawF4211.XF4211_SDPA8                F4211_SDPA8,
       RawF4211.XF4211_SDURAB               F4211_SDURAB,
       RawF4211.XF4211_SDVR01               F4211_SDVR01,
       RawF4211.XF4211_SDUPRC               F4211_SDUPRC,
       RawF4211.XF4211_SDLTTR               F4211_SDLTTR,
       RawF4211.XF4211_SDNXTR               F4211_SDNXTR,
       RawF4211.XF4211_SDSRP1               F4211_SDSRP1,
       RawF4211.XF4211_SDTORG               F4211_SDTORG,
       RawF4211.XF4211_SDCARS               F4211_SDCARS,
       RawF4211.XC_F4211_SDMCU              C_F4211_SDMCU,
       RawF4211.XF4211_SDLNTY               F4211_SDLNTY,
       RawF4211.XF4211_SDCNDJ               F4211_SDCNDJ,
       RawF4211.XF4211_SDSHPN               F4211_SDSHPN,
       RawF4211.XF4211_SDURRF               F4211_SDURRF,
       RawF4211.XF4101_IMUWUM               F4101_IMUWUM,
       RawF4211.XF4211_SDSRP2               F4211_SDSRP2,
       RawF4211.XF4211_SDSRP3               F4211_SDSRP3,
       RawF4211.XF4211_SDSRP4               F4211_SDSRP4,
       RawF4211.XF4211_SDGLC                F4211_SDGLC,
       RawF4211.XF41002_UMCONV              F41002_UMCONV,
       RawF4211.XID_CUSTOM_828f49fff76855   ID_CUSTOM_828f49a6c96031,
       RawF4211.XID_CUSTOM_828f61bacf725b   ID_CUSTOM_828f6163f550b7,
       RawF4211.XID_CUSTOM_828f932492fdf3   ID_CUSTOM_828f93f1edae9ef,
       RawF4211.XID_CUSTOM_828fcf74a404bf   ID_CUSTOM_828fcf3f8f09023,
       RawF4211.XID_CUSTOM_828fdfd17fdc1e   ID_CUSTOM_828fdffdcd5b44e,
       RawF4211.XF0010_CCPNC                F0010_CCPNC,
       RawF4211.XF0010_CCDFF                F0010_CCDFF,
       SUM(RawF4211.ZReportColumn10001)     ReportColumn1,
       SUM(RawF4211.ZReportColumn20001)     ReportColumn2,
       SUM(RawF4211.ZReportColumn60001)     ReportColumn6,
       SUM(RawF4211.ZReportColumn70001)     ReportColumn7,
       SUM(RawF4211.XRowIDX_F4211_0_XRowID) RowIDX_F4211_0_XRowID,
       SUM(RawF4211.XRowIDX_F4211_1_XRowID) RowIDX_F4211_1_XRowID,
       SUM(RawF4211.XRowIDX_F4211_2_XRowID) RowIDX_F4211_2_XRowID
FROM   (SELECT DISTINCT F4211.SDAN8                                                          XC_F4211_SDAN8,
                        F4211.SDAN8                                                          XF4211_SDAN8,
                        F4211.SDSHAN                                                         XF4211_SDSHAN,
                        F0101.ABALPH                                                         XF0101_ABALPH,
                        F4211.SDMCU                                                          XF4211_SDMCU,
                        F0116.ALADD1                                                         XF0116_ALADD1,
                        F0116.ALADD2                                                         XF0116_ALADD2,
                        F0116.ALCTY1                                                         XF0116_ALCTY1,
                        F0116.ALADDS                                                         XF0116_ALADDS,
                        F0116.ALADDZ                                                         XF0116_ALADDZ,
                        F4211.SDDOCO                                                         XF4211_SDDOCO,
                        F4211.SDDCT                                                          XF4211_SDDCT,
                        F4211.SDDOC                                                          XF4211_SDDOC,
                        F4211.SDIVD                                                          XF4211_SDIVD,
                        CAST(F4211.SDLNID AS FLOAT) / 1000                                   XF4211_SDLNID,
                        F4211.SDHOLD                                                         XF4211_SDHOLD,
                        F4211.SDDCTO                                                         XF4211_SDDCTO,
                        F4211.SDTRDJ                                                         XF4211_SDTRDJ,
                        F4211.SDDRQJ                                                         XF4211_SDDRQJ,
                        F4211.SDADDJ                                                         XF4211_SDADDJ,
                        F4211.SDLITM                                                         XF4211_SDLITM,
                        F4211.SDMOT                                                          XF4211_SDMOT,
                        F4211.SDFRTH                                                         XF4211_SDFRTH,
                        F4211.SDUOM                                                          XF4211_SDUOM,
                        F4211.SDCNID                                                         XF4211_SDCNID,
                        F4211.SDPA8                                                          XF4211_SDPA8,
                        F4211.SDURAB                                                         XF4211_SDURAB,
                        F4211.SDVR01                                                         XF4211_SDVR01,
                        CAST(F4211.SDUPRC AS FLOAT) / 1000000                                XF4211_SDUPRC,
                        F4211.SDLTTR                                                         XF4211_SDLTTR,
                        F4211.SDNXTR                                                         XF4211_SDNXTR,
                        F4211.SDSRP1                                                         XF4211_SDSRP1,
                        F4211.SDTORG                                                         XF4211_SDTORG,
                        F4211.SDCARS                                                         XF4211_SDCARS,
                        F4211.SDMCU                                                          XC_F4211_SDMCU,
                        F4211.SDLNTY                                                         XF4211_SDLNTY,
                        F4211.SDCNDJ                                                         XF4211_SDCNDJ,
                        F4211.SDSHPN                                                         XF4211_SDSHPN,
                        F4211.SDURRF                                                         XF4211_SDURRF,
                        F4101.IMUWUM                                                         XF4101_IMUWUM,
                        F4211.SDSRP2                                                         XF4211_SDSRP2,
                        F4211.SDSRP3                                                         XF4211_SDSRP3,
                        F4211.SDSRP4                                                         XF4211_SDSRP4,
                        F4211.SDGLC                                                          XF4211_SDGLC,
                        CAST(F41002.UMCONV AS FLOAT) / 10000000                              XF41002_UMCONV,
                        CASE
                          WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                          OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                        OR ( 4 IS NULL ) )
                                      OR ( 4 <= 0 ) )
                                    OR ( 3 <= 0 ) )
                                  OR ( 3 IS NULL ) )
                                OR ( F4211.SDIVD IS NULL ) THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                        END                                                                  XID_CUSTOM_828f49fff76855,
                        CASE
                          WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 2 )
                                          OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 2 + 2 - 1 ) )
                                        OR ( 2 IS NULL ) )
                                      OR ( 2 <= 0 ) )
                                    OR ( 2 <= 0 ) )
                                  OR ( 2 IS NULL ) )
                                OR ( F4211.SDIVD IS NULL ) THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 2, 2)
                        END                                                                  XID_CUSTOM_828f61bacf725b,
                        CASE
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                               END BETWEEN N'000' AND N'031' THEN 1
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                               END BETWEEN N'032' AND N'060' THEN 2
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                               END BETWEEN N'061' AND N'091' THEN 3
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                               END BETWEEN N'092' AND N'121' THEN 4
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                               END BETWEEN N'122' AND N'152' THEN 5
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                               END BETWEEN N'153' AND N'182' THEN 6
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                               END BETWEEN N'183' AND N'213' THEN 7
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                               END BETWEEN N'214' AND N'244' THEN 8
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                               END BETWEEN N'245' AND N'273' THEN 9
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                               END BETWEEN N'274' AND N'305' THEN 10
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                               END BETWEEN N'306' AND N'335' THEN 11
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                               END BETWEEN N'336' AND N'366' THEN 12
                        END                                                                  XID_CUSTOM_828f932492fdf3,
                        ( F0010.CCPNC - CASE
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                               END BETWEEN N'000' AND N'031' THEN 1
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                               END BETWEEN N'032' AND N'060' THEN 2
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                               END BETWEEN N'061' AND N'091' THEN 3
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                               END BETWEEN N'092' AND N'121' THEN 4
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                               END BETWEEN N'122' AND N'152' THEN 5
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                               END BETWEEN N'153' AND N'182' THEN 6
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                               END BETWEEN N'183' AND N'213' THEN 7
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                               END BETWEEN N'214' AND N'244' THEN 8
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                               END BETWEEN N'245' AND N'273' THEN 9
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                               END BETWEEN N'274' AND N'305' THEN 10
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                               END BETWEEN N'306' AND N'335' THEN 11
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                               END BETWEEN N'336' AND N'366' THEN 12
                                        END )                                                XID_CUSTOM_828fcf74a404bf,
                        ( CASE
                            WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 2 )
                                            OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 2 + 2 - 1 ) )
                                          OR ( 2 IS NULL ) )
                                        OR ( 2 <= 0 ) )
                                      OR ( 2 <= 0 ) )
                                    OR ( 2 IS NULL ) )
                                  OR ( F4211.SDIVD IS NULL ) THEN NULL
                            ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 2, 2)
                          END - F0010.CCDFF )                                                XID_CUSTOM_828fdfd17fdc1e,
                        F0010.CCPNC                                                          XF0010_CCPNC,
                        F0010.CCDFF                                                          XF0010_CCDFF,
                        F4211.SDSOQS                                                         ZReportColumn10001,
                        F4211.SDPQOR                                                         ZReportColumn20001,
                        F4211.SDUORG                                                         ZReportColumn60001,
                        F4211.SDAEXP * NVL(CO_F4211_SDCO.ShiftFactor, 0.01)                  ZReportColumn70001,
                        F4211.SDKCOO                                                         PK__F4211__SDKCOO,
                        F4211.SDLNID                                                         PK__F4211__SDLNID,
                        DBMS_UTILITY.GET_HASH_VALUE(TO_CHAR(F4211.SDKCOO)
                                                     || N'_ISS_'
                                                     || TO_CHAR(F4211.SDDOCO)
                                                     || N'_ISS_'
                                                     || TO_CHAR(F4211.SDDCTO)
                                                     || N'_ISS_'
                                                     || TO_CHAR(F4211.SDLNID)
                                                     || N'_InDeX_0_SaLt', -1048576, 2097152) XRowIDX_F4211_0_XRowID,
                        DBMS_UTILITY.GET_HASH_VALUE(TO_CHAR(F4211.SDKCOO)
                                                     || N'_ISS_'
                                                     || TO_CHAR(F4211.SDDOCO)
                                                     || N'_ISS_'
                                                     || TO_CHAR(F4211.SDDCTO)
                                                     || N'_ISS_'
                                                     || TO_CHAR(F4211.SDLNID)
                                                     || N'_InDeX_1_SaLt', -1048576, 2097152) XRowIDX_F4211_1_XRowID,
                        DBMS_UTILITY.GET_HASH_VALUE(TO_CHAR(F4211.SDKCOO)
                                                     || N'_ISS_'
                                                     || TO_CHAR(F4211.SDDOCO)
                                                     || N'_ISS_'
                                                     || TO_CHAR(F4211.SDDCTO)
                                                     || N'_ISS_'
                                                     || TO_CHAR(F4211.SDLNID)
                                                     || N'_InDeX_2_SaLt', -1048576, 2097152) XRowIDX_F4211_2_XRowID
        FROM   PRODDTA.F4211 F4211
               INNER JOIN PRODDTA.F0010 F0010
                 ON F4211.SDKCOO = F0010.CCCO
               INNER JOIN (SELECT F0101.ABALPH,
                                  F0101.ABAN8
                           FROM   PRODDTA.F0101 F0101
                           WHERE  ( ( F0101.ABAT1 BETWEEN N'A  ' AND N'P  ' )
                                     OR ( F0101.ABAT1 BETWEEN N'R  ' AND N'ZZZ' ) )) F0101
                          INNER JOIN PRODDTA.F0116 F0116
                            ON F0101.ABAN8 = F0116.ALAN8
                 ON F0101.ABAN8 = F4211.SDSHAN
               LEFT JOIN PRODDTA.F4101 F4101
                 ON F4211.SDITM = F4101.IMITM
               LEFT JOIN PRODDTA.F41002 F41002
                 ON ( ( F4211.SDITM = F41002.UMITM )
                      AND ( F41002.UMRUM = N'TN' ) )
                    AND ( F4211.SDUOM = F41002.UMUM )
               LEFT JOIN dwtemp736733D926A15B6_jds CO_F4211_SDCO
                 ON F4211.SDCO = CO_F4211_SDCO.CO
        WHERE  ( (( (( (( F4211.SDPA8 IN ( 20021899, 20021943, 20022190, 20022384,
                                           20022427, 20022496, 20022631, 20021987 ) ))
                       AND (( F4211.SDNXTR IN ( N'999' ) )) ))
                    AND (( NOT ( F4211.SDLTTR IN ( N'980' ) ) )) ))
                 AND ( ( F0010.CCPNC - CASE
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                              END BETWEEN N'000' AND N'031' THEN 1
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                              END BETWEEN N'032' AND N'060' THEN 2
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                              END BETWEEN N'061' AND N'091' THEN 3
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                              END BETWEEN N'092' AND N'121' THEN 4
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                              END BETWEEN N'122' AND N'152' THEN 5
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                              END BETWEEN N'153' AND N'182' THEN 6
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                              END BETWEEN N'183' AND N'213' THEN 7
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                              END BETWEEN N'214' AND N'244' THEN 8
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                              END BETWEEN N'245' AND N'273' THEN 9
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                              END BETWEEN N'274' AND N'305' THEN 10
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                              END BETWEEN N'306' AND N'335' THEN 11
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDIVD IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 4, 3)
                                              END BETWEEN N'336' AND N'366' THEN 12
                                       END ) = 1 ) )
               AND ( ( CASE
                         WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDIVD)) < 2 )
                                         OR ( LENGTH(TO_CHAR(F4211.SDIVD)) < 2 + 2 - 1 ) )
                                       OR ( 2 IS NULL ) )
                                     OR ( 2 <= 0 ) )
                                   OR ( 2 <= 0 ) )
                                 OR ( 2 IS NULL ) )
                               OR ( F4211.SDIVD IS NULL ) THEN NULL
                         ELSE SUBSTR(TO_NCHAR(F4211.SDIVD), 2, 2)
                       END - F0010.CCDFF ) = 0 )) RawF4211
GROUP  BY RawF4211.XC_F4211_SDAN8,
          RawF4211.XF4211_SDAN8,
          RawF4211.XF4211_SDSHAN,
          RawF4211.XF0101_ABALPH,
          RawF4211.XF4211_SDMCU,
          RawF4211.XF0116_ALADD1,
          RawF4211.XF0116_ALADD2,
          RawF4211.XF0116_ALCTY1,
          RawF4211.XF0116_ALADDS,
          RawF4211.XF0116_ALADDZ,
          RawF4211.XF4211_SDDOCO,
          RawF4211.XF4211_SDDCT,
          RawF4211.XF4211_SDDOC,
          RawF4211.XF4211_SDIVD,
          RawF4211.XF4211_SDLNID,
          RawF4211.XF4211_SDHOLD,
          RawF4211.XF4211_SDDCTO,
          RawF4211.XF4211_SDTRDJ,
          RawF4211.XF4211_SDDRQJ,
          RawF4211.XF4211_SDADDJ,
          RawF4211.XF4211_SDLITM,
          RawF4211.XF4211_SDMOT,
          RawF4211.XF4211_SDFRTH,
          RawF4211.XF4211_SDUOM,
          RawF4211.XF4211_SDCNID,
          RawF4211.XF4211_SDPA8,
          RawF4211.XF4211_SDURAB,
          RawF4211.XF4211_SDVR01,
          RawF4211.XF4211_SDUPRC,
          RawF4211.XF4211_SDLTTR,
          RawF4211.XF4211_SDNXTR,
          RawF4211.XF4211_SDSRP1,
          RawF4211.XF4211_SDTORG,
          RawF4211.XF4211_SDCARS,
          RawF4211.XC_F4211_SDMCU,
          RawF4211.XF4211_SDLNTY,
          RawF4211.XF4211_SDCNDJ,
          RawF4211.XF4211_SDSHPN,
          RawF4211.XF4211_SDURRF,
          RawF4211.XF4101_IMUWUM,
          RawF4211.XF4211_SDSRP2,
          RawF4211.XF4211_SDSRP3,
          RawF4211.XF4211_SDSRP4,
          RawF4211.XF4211_SDGLC,
          RawF4211.XF41002_UMCONV,
          RawF4211.XID_CUSTOM_828f49fff76855,
          RawF4211.XID_CUSTOM_828f61bacf725b,
          RawF4211.XID_CUSTOM_828f932492fdf3,
          RawF4211.XID_CUSTOM_828fcf74a404bf,
          RawF4211.XID_CUSTOM_828fdfd17fdc1e,
          RawF4211.XF0010_CCPNC,
          RawF4211.XF0010_CCDFF
ORDER  BY F4211_SDMCU ASC,
          F4211_SDSHPN ASC,
          F4211_SDFRTH ASC,
          F4211_SDMOT ASC,
          F4211_SDCARS ASC,
          F4211_SDADDJ ASC,
          F4211_SDSHAN ASC,
          F4211_SDDCTO ASC,
          F4211_SDCNDJ ASC,
          F4211_SDTRDJ ASC,
          F4211_SDIVD ASC,
          F4211_SDDRQJ ASC,
          F4211_SDLNID ASC,
          F4211_SDLNTY ASC,
          F4211_SDDOCO ASC,
          F4211_SDAN8 ASC,
          F4211_SDLITM ASC,
          F4211_SDDOC ASC,
          F4211_SDURAB ASC,
          F4211_SDVR01 ASC,
          F4211_SDCNID ASC,
          F4211_SDURRF ASC,
          F4211_SDUOM ASC,
          F4211_SDLTTR ASC,
          F4211_SDNXTR ASC,
          F4211_SDSRP1 ASC,
          F4211_SDSRP2 ASC,
          F4211_SDSRP3 ASC,
          F4211_SDSRP4 ASC,
          F4211_SDGLC ASC,
          F4101_IMUWUM ASC,
          F41002_UMCONV ASC,
          F0101_ABALPH ASC,
          F0116_ALCTY1 ASC,
          F0116_ALADDS ASC,
          F4211_SDHOLD ASC,
          F4211_SDPA8 ASC,
          F4211_SDUPRC ASC,
          F4211_SDTORG ASC,
          F0116_ALADD1 ASC,
          F0116_ALADD2 ASC,
          F0116_ALADDZ ASC,
          F4211_SDDCT ASC,
          F0010_CCPNC ASC,
          F0010_CCDFF ASC,
          ID_CUSTOM_828f49a6c96031 ASC,
          ID_CUSTOM_828f6163f550b7 ASC,
          ID_CUSTOM_828f93f1edae9ef ASC,
          ID_CUSTOM_828fcf3f8f09023 ASC,
          ID_CUSTOM_828fdffdcd5b44e ASC 