SELECT
       RawF0101.XF4211_SDDOCO F4211_SDDOCO,
       RawF0101.XF4211_SDLITM F4211_SDLITM,
       RawF0101.XF4211_SDVR01 F4211_SDVR01,
       RawF0101.XF4211_SDSHAN F4211_SDSHAN,
       RawF0101.XF0101_ABALPH F0101_ABALPH,
       RawF0101.XF4211_SDPDDJ F4211_SDPDDJ,
       SUM(RawF0101.ZReportColumn10001) ReportColumn1,
       SUM(RawF0101.XRowIDX_F0101_0_XRowID) RowIDX_F0101_0_XRowID,
       SUM(RawF0101.XRowIDX_F0101_1_XRowID) RowIDX_F0101_1_XRowID,
       SUM(RawF0101.XRowIDX_F0101_2_XRowID) RowIDX_F0101_2_XRowID
FROM
       (
              SELECT DISTINCT
                     F4211.SDDOCO XF4211_SDDOCO,
                     F4211.SDLITM XF4211_SDLITM,
                     F4211.SDVR01 XF4211_SDVR01,
                     F4211.SDSHAN XF4211_SDSHAN,
                     F0101.ABALPH XF0101_ABALPH,
                     F4211.SDPDDJ XF4211_SDPDDJ,
                     F0101.ABURAT * 0.01 ZReportColumn10001,
                     F0101.ABAN8 PK__F0101__ABAN8,
                     DBMS_UTILITY.GET_HASH_VALUE (
                            TO_CHAR (F0101.ABAN8) || N'_InDeX_0_SaLt',
                            -1048576,
                            2097152
                     ) XRowIDX_F0101_0_XRowID,
                     DBMS_UTILITY.GET_HASH_VALUE (
                            TO_CHAR (F0101.ABAN8) || N'_InDeX_1_SaLt',
                            -1048576,
                            2097152
                     ) XRowIDX_F0101_1_XRowID,
                     DBMS_UTILITY.GET_HASH_VALUE (
                            TO_CHAR (F0101.ABAN8) || N'_InDeX_2_SaLt',
                            -1048576,
                            2097152
                     ) XRowIDX_F0101_2_XRowID
              FROM
                     PRODDTA.F4211 F4211
                     LEFT JOIN (
                            SELECT
                                   F0101.ABALPH,
                                   F0101.ABURAT,
                                   F0101.ABAN8
                            FROM
                                   PRODDTA.F0101 F0101
                            WHERE
                                   (
                                          (F0101.ABAT1 BETWEEN N'A  ' AND N'P  ')
                                          OR (F0101.ABAT1 BETWEEN N'R  ' AND N'ZZZ')
                                   )
                     ) F0101 ON F4211.SDSHAN = F0101.ABAN8
              WHERE
                     (
                            (
                                   (
                                          ((F4211.SDSHAN IN (20011316)))
                                          AND ((F4211.SDNXTR IN (N'529')))
                                   )
                            )
                            AND ((F4211.SDLTTR BETWEEN N'520' AND N'528'))
                     )
       ) RawF0101
GROUP BY
       RawF0101.XF4211_SDDOCO,
       RawF0101.XF4211_SDLITM,
       RawF0101.XF4211_SDVR01,
       RawF0101.XF4211_SDSHAN,
       RawF0101.XF0101_ABALPH,
       RawF0101.XF4211_SDPDDJ
ORDER BY
       F4211_SDPDDJ ASC,
       F4211_SDDOCO ASC,
       F4211_SDLITM ASC,
       F4211_SDVR01 ASC,
       F4211_SDSHAN ASC,
       F0101_ABALPH ASC