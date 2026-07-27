SELECT
   RawF4211.XF03012_AIAC05 F03012_AIAC05,
   RawF4211.XF4211_SDDOCO F4211_SDDOCO,
   RawF4211.XF4211_SDSHPN F4211_SDSHPN,
   RawF4211.XF4211_SDVR01 F4211_SDVR01,
   RawF4211.XF4211_SDLITM F4211_SDLITM,
   RawF4211.XF4211_SDUOM F4211_SDUOM,
   RawF4211.XF4211_SDLTTR F4211_SDLTTR,
   RawF4211.XF4211_SDNXTR F4211_SDNXTR,
   RawF4211.XF4211_SDCNDJ F4211_SDCNDJ,
   RawF4211.XC_F4211_SDSHAN C_F4211_SDSHAN,
   RawF4211.XF4211_SDSHAN F4211_SDSHAN,
   RawF4211.XF4211_SDPA8 F4211_SDPA8,
   RawF4211.XC_F4211_SDMCU C_F4211_SDMCU,
   RawF4211.XF4211_SDMCU F4211_SDMCU,
   RawF4211.XF4201_SHHOLD F4201_SHHOLD,
   RawF4211.XF5642B01_BA55OCCR F5642B01_BA55OCCR,
   RawF4211.XF0101_ABALPH F0101_ABALPH,
   RawF4211.XF4211_SDPDDJ F4211_SDPDDJ,
   RawF4211.XF5642B01_BA55BKNO F5642B01_BA55BKNO,
   RawF4211.XF5642B01_BA55VLNO F5642B01_BA55VLNO,
   RawF4211.XF5642B01_BA55VONO F5642B01_BA55VONO,
   RawF4211.XF5642B01_BADEPU F5642B01_BADEPU,
   RawF4211.XF5642B01_BADEDL F5642B01_BADEDL,
   RawF4211.XF4211_SDLNID F4211_SDLNID,
   SUM(RawF4211.ZReportColumn10001) ReportColumn1,
   SUM(RawF4211.ZReportColumn20001) ReportColumn2,
   SUM(RawF4211.XRowIDX_F4211_0_XRowID) RowIDX_F4211_0_XRowID,
   SUM(RawF4211.XRowIDX_F4211_1_XRowID) RowIDX_F4211_1_XRowID,
   SUM(RawF4211.XRowIDX_F4211_2_XRowID) RowIDX_F4211_2_XRowID
FROM
   (
      SELECT DISTINCT
         F03012.AIAC05 XF03012_AIAC05,
         F4211.SDDOCO XF4211_SDDOCO,
         F4211.SDSHPN XF4211_SDSHPN,
         F4211.SDVR01 XF4211_SDVR01,
         F4211.SDLITM XF4211_SDLITM,
         F4211.SDUOM XF4211_SDUOM,
         F4211.SDLTTR XF4211_SDLTTR,
         F4211.SDNXTR XF4211_SDNXTR,
         F4211.SDCNDJ XF4211_SDCNDJ,
         F4211.SDSHAN XC_F4211_SDSHAN,
         F4211.SDSHAN XF4211_SDSHAN,
         F4211.SDPA8 XF4211_SDPA8,
         F4211.SDMCU XC_F4211_SDMCU,
         F4211.SDMCU XF4211_SDMCU,
         F4201.SHHOLD XF4201_SHHOLD,
         F5642B01.BA55OCCR XF5642B01_BA55OCCR,
         F0101.ABALPH XF0101_ABALPH,
         F4211.SDPDDJ XF4211_SDPDDJ,
         F5642B01.BA55BKNO XF5642B01_BA55BKNO,
         F5642B01.BA55VLNO XF5642B01_BA55VLNO,
         F5642B01.BA55VONO XF5642B01_BA55VONO,
         F5642B01.BADEPU XF5642B01_BADEPU,
         F5642B01.BADEDL XF5642B01_BADEDL,
         CAST(F4211.SDLNID AS FLOAT) / 1000 XF4211_SDLNID,
         F4211.SDUORG ZReportColumn10001,
         F4211.SDSOQS ZReportColumn20001,
         F4211.SDKCOO PK__F4211__SDKCOO,
         F4211.SDDCTO PK__F4211__SDDCTO,
         F4211.SDLNID PK__F4211__SDLNID,
         DBMS_UTILITY.GET_HASH_VALUE (
            TO_CHAR (F4211.SDKCOO) || N'_ISS_' || TO_CHAR (F4211.SDDOCO) || N'_ISS_' || TO_CHAR (F4211.SDDCTO) || N'_ISS_' || TO_CHAR (F4211.SDLNID) || N'_InDeX_0_SaLt',
            -1048576,
            2097152
         ) XRowIDX_F4211_0_XRowID,
         DBMS_UTILITY.GET_HASH_VALUE (
            TO_CHAR (F4211.SDKCOO) || N'_ISS_' || TO_CHAR (F4211.SDDOCO) || N'_ISS_' || TO_CHAR (F4211.SDDCTO) || N'_ISS_' || TO_CHAR (F4211.SDLNID) || N'_InDeX_1_SaLt',
            -1048576,
            2097152
         ) XRowIDX_F4211_1_XRowID,
         DBMS_UTILITY.GET_HASH_VALUE (
            TO_CHAR (F4211.SDKCOO) || N'_ISS_' || TO_CHAR (F4211.SDDOCO) || N'_ISS_' || TO_CHAR (F4211.SDDCTO) || N'_ISS_' || TO_CHAR (F4211.SDLNID) || N'_InDeX_2_SaLt',
            -1048576,
            2097152
         ) XRowIDX_F4211_2_XRowID
      FROM
         PRODDTA.F4211 F4211
         INNER JOIN PRODDTA.F4201 F4201 ON (
            (F4211.SDKCOO = F4201.SHKCOO)
            AND (F4211.SDDOCO = F4201.SHDOCO)
         )
         AND (F4211.SDDCTO = F4201.SHDCTO)
         LEFT JOIN PRODDTA.F03012 F03012 ON F4211.SDAN8 = F03012.AIAN8
         LEFT JOIN PRODDTA.F5642B01 F5642B01
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
         ) F0101 ON F5642B01.BA55OCCR = F0101.ABAN8 ON (F4211.SDDOCO = F5642B01.BADOCO)
         AND (F4211.SDSHPN = F5642B01.BASHPN)
      WHERE
         (
            (
               (
                  (
                     (
                        (
                           (
                              F4211.SDDOCO IN (
                                 1593550,
                                 1593549,
                                 1581179,
                                 1581165,
                                 1581173,
                                 1581184,
                                 1581161,
                                 1596420,
                                 1594410,
                                 1596628,
                                 1593269,
                                 1593272,
                                 1595918,
                                 1557959,
                                 1557961,
                                 1577127,
                                 1593196,
                                 1593200,
                                 1595505,
                                 1593731,
                                 1594581,
                                 1593732,
                                 1594622,
                                 1593733,
                                 1595678,
                                 1593736,
                                 1593734,
                                 1593735,
                                 1570618,
                                 1571523,
                                 1571525,
                                 1570619,
                                 1571520,
                                 1570615,
                                 1571522,
                                 1570617,
                                 1590914,
                                 1583718,
                                 1595704,
                                 1595696,
                                 1594415,
                                 1594414,
                                 1595402,
                                 1596566,
                                 1590559,
                                 1590562,
                                 1590564,
                                 1590569,
                                 1596416,
                                 1596417,
                                 1596418,
                                 1594405,
                                 1594406,
                                 1595342,
                                 1596583,
                                 1593829,
                                 1595675,
                                 1597223,
                                 1596647,
                                 1593945,
                                 1595602,
                                 1595603,
                                 1595604,
                                 1596951,
                                 1561922,
                                 1561921,
                                 1592418,
                                 1582249,
                                 1587441,
                                 1595294
                              )
                           )
                        )
                        AND ((NOT (F4211.SDLTTR IN (N'980'))))
                     )
                  )
                  AND ((F03012.AIAC05 IN (N'E26')))
               )
            )
            AND ((F4211.SDLNTY IN (N'S ')))
         )
   ) RawF4211
GROUP BY
   RawF4211.XF03012_AIAC05,
   RawF4211.XF4211_SDDOCO,
   RawF4211.XF4211_SDSHPN,
   RawF4211.XF4211_SDVR01,
   RawF4211.XF4211_SDLITM,
   RawF4211.XF4211_SDUOM,
   RawF4211.XF4211_SDLTTR,
   RawF4211.XF4211_SDNXTR,
   RawF4211.XF4211_SDCNDJ,
   RawF4211.XC_F4211_SDSHAN,
   RawF4211.XF4211_SDSHAN,
   RawF4211.XF4211_SDPA8,
   RawF4211.XC_F4211_SDMCU,
   RawF4211.XF4211_SDMCU,
   RawF4211.XF4201_SHHOLD,
   RawF4211.XF5642B01_BA55OCCR,
   RawF4211.XF0101_ABALPH,
   RawF4211.XF4211_SDPDDJ,
   RawF4211.XF5642B01_BA55BKNO,
   RawF4211.XF5642B01_BA55VLNO,
   RawF4211.XF5642B01_BA55VONO,
   RawF4211.XF5642B01_BADEPU,
   RawF4211.XF5642B01_BADEDL,
   RawF4211.XF4211_SDLNID
ORDER BY
   F4211_SDDOCO ASC,
   F4211_SDMCU ASC,
   F4201_SHHOLD ASC,
   F4211_SDVR01 ASC,
   F4211_SDLITM ASC,
   F03012_AIAC05 ASC,
   F4211_SDNXTR ASC,
   F4211_SDLTTR ASC,
   F4211_SDUOM ASC,
   F4211_SDSHAN ASC,
   F5642B01_BA55BKNO ASC,
   F5642B01_BADEDL ASC,
   F5642B01_BADEPU ASC,
   F5642B01_BA55VLNO ASC,
   F4211_SDPA8 ASC,
   F4211_SDCNDJ ASC,
   F4211_SDLNID ASC,
   F4211_SDSHPN ASC,
   F5642B01_BA55VONO ASC,
   F4211_SDPDDJ ASC,
   F5642B01_BA55OCCR ASC,
   F0101_ABALPH ASC