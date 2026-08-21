SELECT RawF4211.XF4211_SDADDJ               F4211_SDADDJ,
       RawF4211.XF0101_ABALPH               F0101_ABALPH,
       RawF4211.XF4211_SDSHAN               F4211_SDSHAN,
       RawF4211.XF4211_SDVR01               F4211_SDVR01,
       RawF4211.XF4211_SDDSC1               F4211_SDDSC1,
       RawF4211.XF4211_SDPSIG               F4211_SDPSIG,
       RawF4211.XF4211_SDDOC                F4211_SDDOC,
       RawF4211.XF4211_SDCNID               F4211_SDCNID,
       RawF4211.XF4211_SDVR02               F4211_SDVR02,
       RawF4211.XF4211_SDUOM                F4211_SDUOM,
       RawF4211.XF5549002_MIGRWT            F5549002_MIGRWT,
       RawF4211.XF5549002_MICTWT            F5549002_MICTWT,
       RawF4211.XF5549002_MIMXWT            F5549002_MIMXWT,
       RawF4211.XF5549002_MILNID            F5549002_MILNID,
       RawF4211.XF4211_SDLNID               F4211_SDLNID,
       RawF4211.XF0010_CCPNC                F0010_CCPNC,
       RawF4211.XID_CUSTOM_81c4205f979fa0   ID_CUSTOM_81c4202d6fe50e0,
       RawF4211.XID_CUSTOM_81c4236233d080   ID_CUSTOM_81c423daffc4f50,
       RawF4211.XID_CUSTOM_81c3fb6a29e45a   ID_CUSTOM_81c3fb76b2de9ba,
       RawF4211.XID_CUSTOM_81cd5be16fa9a9   ID_CUSTOM_81cd5bf840fd33d,
       RawF4211.XID_CUSTOM_81cd5ecacae98d   ID_CUSTOM_81cd5eab5db2569,
       RawF4211.XDPF4211_SDADDJ             DPF4211_SDADDJ,
       RawF4211.XID_CUSTOM_8fd5f6508402df   ID_CUSTOM_8fd5f634ff9b0e3,
       SUM(RawF4211.ZReportColumn10001)     ReportColumn1,
       SUM(RawF4211.ZReportColumn20001)     ReportColumn2,
       SUM(RawF4211.ZReportColumn40001)     ReportColumn4,
       SUM(RawF4211.XRowIDX_F4211_0_XRowID) RowIDX_F4211_0_XRowID,
       SUM(RawF4211.XRowIDX_F4211_1_XRowID) RowIDX_F4211_1_XRowID,
       SUM(RawF4211.XRowIDX_F4211_2_XRowID) RowIDX_F4211_2_XRowID
FROM   (SELECT DISTINCT F4211.SDADDJ                                                         XF4211_SDADDJ,
                        F0101.ABALPH                                                         XF0101_ABALPH,
                        F4211.SDSHAN                                                         XF4211_SDSHAN,
                        F4211.SDVR01                                                         XF4211_SDVR01,
                        F4211.SDDSC1                                                         XF4211_SDDSC1,
                        F4211.SDPSIG                                                         XF4211_SDPSIG,
                        F4211.SDDOC                                                          XF4211_SDDOC,
                        F4211.SDCNID                                                         XF4211_SDCNID,
                        F4211.SDVR02                                                         XF4211_SDVR02,
                        F4211.SDUOM                                                          XF4211_SDUOM,
                        CAST(F5549002.MIGRWT AS FLOAT) / 10000                               XF5549002_MIGRWT,
                        CAST(F5549002.MICTWT AS FLOAT) / 10000                               XF5549002_MICTWT,
                        CAST(F5549002.MIMXWT AS FLOAT) / 100                                 XF5549002_MIMXWT,
                        CAST(F5549002.MILNID AS FLOAT) / 1000                                XF5549002_MILNID,
                        CAST(F4211.SDLNID AS FLOAT) / 1000                                   XF4211_SDLNID,
                        F0010.CCPNC                                                          XF0010_CCPNC,
                        CASE
                          WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                          OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                        OR ( 4 IS NULL ) )
                                      OR ( 4 <= 0 ) )
                                    OR ( 3 <= 0 ) )
                                  OR ( 3 IS NULL ) )
                                OR ( F4211.SDADDJ IS NULL ) THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                        END                                                                  XID_CUSTOM_81c4205f979fa0,
                        CASE
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                               END BETWEEN N'000' AND N'031' THEN 1
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                               END BETWEEN N'032' AND N'059' THEN 2
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                               END BETWEEN N'060' AND N'090' THEN 3
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                               END BETWEEN N'091' AND N'120' THEN 4
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                               END BETWEEN N'121' AND N'151' THEN 5
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                               END BETWEEN N'152' AND N'181' THEN 6
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                               END BETWEEN N'182' AND N'212' THEN 7
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                               END BETWEEN N'213' AND N'243' THEN 8
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                               END BETWEEN N'244' AND N'273' THEN 9
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                               END BETWEEN N'274' AND N'304' THEN 10
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                               END BETWEEN N'305' AND N'334' THEN 11
                          WHEN CASE
                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                               OR ( 4 IS NULL ) )
                                             OR ( 4 <= 0 ) )
                                           OR ( 3 <= 0 ) )
                                         OR ( 3 IS NULL ) )
                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                               END BETWEEN N'335' AND N'365' THEN 12
                        END                                                                  XID_CUSTOM_81c4236233d080,
                        ( F0010.CCPNC - CASE
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                               END BETWEEN N'000' AND N'031' THEN 1
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                               END BETWEEN N'032' AND N'059' THEN 2
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                               END BETWEEN N'060' AND N'090' THEN 3
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                               END BETWEEN N'091' AND N'120' THEN 4
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                               END BETWEEN N'121' AND N'151' THEN 5
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                               END BETWEEN N'152' AND N'181' THEN 6
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                               END BETWEEN N'182' AND N'212' THEN 7
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                               END BETWEEN N'213' AND N'243' THEN 8
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                               END BETWEEN N'244' AND N'273' THEN 9
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                               END BETWEEN N'274' AND N'304' THEN 10
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                               END BETWEEN N'305' AND N'334' THEN 11
                                          WHEN CASE
                                                 WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                 OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                               OR ( 4 IS NULL ) )
                                                             OR ( 4 <= 0 ) )
                                                           OR ( 3 <= 0 ) )
                                                         OR ( 3 IS NULL ) )
                                                       OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                 ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                               END BETWEEN N'335' AND N'365' THEN 12
                                        END )                                                XID_CUSTOM_81c3fb6a29e45a,
                        CASE
                          WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 2 )
                                          OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 2 + 2 - 1 ) )
                                        OR ( 2 IS NULL ) )
                                      OR ( 2 <= 0 ) )
                                    OR ( 2 <= 0 ) )
                                  OR ( 2 IS NULL ) )
                                OR ( F4211.SDADDJ IS NULL ) THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 2, 2)
                        END                                                                  XID_CUSTOM_81cd5be16fa9a9,
                        ( CASE
                            WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 2 )
                                            OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 2 + 2 - 1 ) )
                                          OR ( 2 IS NULL ) )
                                        OR ( 2 <= 0 ) )
                                      OR ( 2 <= 0 ) )
                                    OR ( 2 IS NULL ) )
                                  OR ( F4211.SDADDJ IS NULL ) THEN NULL
                            ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 2, 2)
                          END - CASE
                                  WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F0010.CCARFJ)) < 2 )
                                                  OR ( LENGTH(TO_CHAR(F0010.CCARFJ)) < 2 + 2 - 1 ) )
                                                OR ( 2 IS NULL ) )
                                              OR ( 2 <= 0 ) )
                                            OR ( 2 <= 0 ) )
                                          OR ( 2 IS NULL ) )
                                        OR ( F0010.CCARFJ IS NULL ) THEN NULL
                                  ELSE SUBSTR(TO_NCHAR(F0010.CCARFJ), 2, 2)
                                END )                                                        XID_CUSTOM_81cd5ecacae98d,
                        F4211.SDADDJ                                                         XDPF4211_SDADDJ,
                        CASE
                          WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F0010.CCARFJ)) < 2 )
                                          OR ( LENGTH(TO_CHAR(F0010.CCARFJ)) < 2 + 2 - 1 ) )
                                        OR ( 2 IS NULL ) )
                                      OR ( 2 <= 0 ) )
                                    OR ( 2 <= 0 ) )
                                  OR ( 2 IS NULL ) )
                                OR ( F0010.CCARFJ IS NULL ) THEN NULL
                          ELSE SUBSTR(TO_NCHAR(F0010.CCARFJ), 2, 2)
                        END                                                                  XID_CUSTOM_8fd5f6508402df,
                        F4211.SDAEXP * NVL(CO_F4211_SDCO.ShiftFactor, 0.01)                  ZReportColumn10001,
                        F4211.SDUORG                                                         ZReportColumn20001,
                        F4211.SDPQOR                                                         ZReportColumn40001,
                        F4211.SDKCOO                                                         PK__F4211__SDKCOO,
                        F4211.SDDOCO                                                         PK__F4211__SDDOCO,
                        F4211.SDDCTO                                                         PK__F4211__SDDCTO,
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
                 ON F4211.SDSHAN = F0101.ABAN8
               LEFT JOIN PRODDTA.F5549002 F5549002
                 ON ( ( ( F4211.SDKCOO = F5549002.MIKCOO )
                        AND ( F4211.SDDOCO = F5549002.MIDOCO ) )
                      AND ( F4211.SDDCTO = F5549002.MIDCTO ) )
                    AND ( F4211.SDLNID = F5549002.MILNID )
               LEFT JOIN dwtemp1594A7E3FFECAB_jds CO_F4211_SDCO
                 ON F4211.SDCO = CO_F4211_SDCO.CO
        WHERE  ( (( (( (( F4211.SDPA8 IN ( 10043240 ) ))
                       AND (( F4211.SDDCTO IN ( N'SO' ) )) ))
                    AND (( F0010.CCCO IN ( N'00400' ) )) ))
                 AND ( ( F0010.CCPNC - CASE
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                              END BETWEEN N'000' AND N'031' THEN 1
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                              END BETWEEN N'032' AND N'059' THEN 2
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                              END BETWEEN N'060' AND N'090' THEN 3
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                              END BETWEEN N'091' AND N'120' THEN 4
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                              END BETWEEN N'121' AND N'151' THEN 5
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                              END BETWEEN N'152' AND N'181' THEN 6
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                              END BETWEEN N'182' AND N'212' THEN 7
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                              END BETWEEN N'213' AND N'243' THEN 8
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                              END BETWEEN N'244' AND N'273' THEN 9
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                              END BETWEEN N'274' AND N'304' THEN 10
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                              END BETWEEN N'305' AND N'334' THEN 11
                                         WHEN CASE
                                                WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 )
                                                                OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 4 + 3 - 1 ) )
                                                              OR ( 4 IS NULL ) )
                                                            OR ( 4 <= 0 ) )
                                                          OR ( 3 <= 0 ) )
                                                        OR ( 3 IS NULL ) )
                                                      OR ( F4211.SDADDJ IS NULL ) THEN NULL
                                                ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 4, 3)
                                              END BETWEEN N'335' AND N'365' THEN 12
                                       END ) = 0 ) )
               AND ( ( CASE
                         WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 2 )
                                         OR ( LENGTH(TO_CHAR(F4211.SDADDJ)) < 2 + 2 - 1 ) )
                                       OR ( 2 IS NULL ) )
                                     OR ( 2 <= 0 ) )
                                   OR ( 2 <= 0 ) )
                                 OR ( 2 IS NULL ) )
                               OR ( F4211.SDADDJ IS NULL ) THEN NULL
                         ELSE SUBSTR(TO_NCHAR(F4211.SDADDJ), 2, 2)
                       END - CASE
                               WHEN ( ( ( ( ( ( LENGTH(TO_CHAR(F0010.CCARFJ)) < 2 )
                                               OR ( LENGTH(TO_CHAR(F0010.CCARFJ)) < 2 + 2 - 1 ) )
                                             OR ( 2 IS NULL ) )
                                           OR ( 2 <= 0 ) )
                                         OR ( 2 <= 0 ) )
                                       OR ( 2 IS NULL ) )
                                     OR ( F0010.CCARFJ IS NULL ) THEN NULL
                               ELSE SUBSTR(TO_NCHAR(F0010.CCARFJ), 2, 2)
                             END ) = 0 )) RawF4211
GROUP  BY RawF4211.XF4211_SDADDJ,
          RawF4211.XF0101_ABALPH,
          RawF4211.XF4211_SDSHAN,
          RawF4211.XF4211_SDVR01,
          RawF4211.XF4211_SDDSC1,
          RawF4211.XF4211_SDPSIG,
          RawF4211.XF4211_SDDOC,
          RawF4211.XF4211_SDCNID,
          RawF4211.XF4211_SDVR02,
          RawF4211.XF4211_SDUOM,
          RawF4211.XF5549002_MIGRWT,
          RawF4211.XF5549002_MICTWT,
          RawF4211.XF5549002_MIMXWT,
          RawF4211.XF5549002_MILNID,
          RawF4211.XF4211_SDLNID,
          RawF4211.XF0010_CCPNC,
          RawF4211.XID_CUSTOM_81c4205f979fa0,
          RawF4211.XID_CUSTOM_81c4236233d080,
          RawF4211.XID_CUSTOM_81c3fb6a29e45a,
          RawF4211.XID_CUSTOM_81cd5be16fa9a9,
          RawF4211.XID_CUSTOM_81cd5ecacae98d,
          RawF4211.XDPF4211_SDADDJ,
          RawF4211.XID_CUSTOM_8fd5f6508402df
ORDER  BY F0101_ABALPH ASC,
          F4211_SDADDJ ASC,
          F4211_SDVR01 ASC,
          F4211_SDPSIG ASC,
          F4211_SDCNID ASC,
          F5549002_MIGRWT ASC,
          F5549002_MIMXWT ASC,
          F5549002_MICTWT ASC,
          F5549002_MILNID ASC,
          F4211_SDLNID ASC,
          F0010_CCPNC ASC,
          ID_CUSTOM_81c4202d6fe50e0 ASC,
          ID_CUSTOM_81c423daffc4f50 ASC,
          ID_CUSTOM_81c3fb76b2de9ba ASC,
          ID_CUSTOM_81cd5bf840fd33d ASC,
          ID_CUSTOM_81cd5eab5db2569 ASC,
          F4211_SDVR02 ASC,
          F4211_SDDSC1 ASC,
          F4211_SDDOC ASC,
          F4211_SDSHAN ASC,
          F4211_SDUOM ASC,
          DPF4211_SDADDJ ASC,
          ID_CUSTOM_8fd5f634ff9b0e3 ASC 