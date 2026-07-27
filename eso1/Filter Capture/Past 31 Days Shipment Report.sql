SELECT
  AggF4211.F4211_SDMCU F4211_SDMCU,
  AggF0101.F0006_MCDL01 F0006_MCDL01,
  AggF4211.F4211_SDSHAN F4211_SDSHAN,
  AggF0101.F0101_ABALPH F0101_ABALPH,
  AggF4211.F4211_SDSRP1 F4211_SDSRP1,
  AggF4211.F4211_SDADDJ F4211_SDADDJ,
  AggF4211.F4211_SDHOLD F4211_SDHOLD,
  AggF0101.F0101_ABSIC F0101_ABSIC,
  AggF4211.F4211_SDDOCO F4211_SDDOCO,
  AggF4211.F4211_SDDCTO F4211_SDDCTO,
  AggF4211.F4211_SDLNID F4211_SDLNID,
  AggF4211.F4211_SDDRQJ F4211_SDDRQJ,
  AggF4211.F4211_SDTRDJ F4211_SDTRDJ,
  AggF4211.F4211_SDLITM F4211_SDLITM,
  AggF4211.F4211_SDMOT F4211_SDMOT,
  AggF4211.F4211_SDUOM F4211_SDUOM,
  AggF4211.F4211_SDPA8 F4211_SDPA8,
  AggF4211.F4211_SDVR01 F4211_SDVR01,
  AggF4211.F4211_SDUPRC F4211_SDUPRC,
  AggF4211.ID_CUSTOM_824e90d952e1bfc ID_CUSTOM_824e90d952e1bfc,
  AggF4211.ID_CUSTOM_824e88f7ed39ece ID_CUSTOM_824e88f7ed39ece,
  AggF4211.ID_CUSTOM_824dbee9b61ef15 ID_CUSTOM_824dbee9b61ef15,
  AggF0101.ReportColumn1 ReportColumn1,
  AggF4211.ReportColumn3 ReportColumn3,
  AggF4211.ReportColumn4 ReportColumn4,
  AggF4211.RowIDX_F4211_2_XRowID,
  AggF4211.RowIDX_F4211_1_XRowID,
  AggF4211.RowIDX_F4211_0_XRowID,
  AggF0101.RowIDX_F0101_2_XRowID,
  AggF0101.RowIDX_F0101_1_XRowID,
  AggF0101.RowIDX_F0101_0_XRowID
FROM
  (
    SELECT
      RawF4211.XF4211_SDMCU F4211_SDMCU,
      RawF4211.XF0006_MCDL01 F0006_MCDL01,
      RawF4211.XF4211_SDSHAN F4211_SDSHAN,
      RawF4211.XF0101_ABALPH F0101_ABALPH,
      RawF4211.XF4211_SDSRP1 F4211_SDSRP1,
      RawF4211.XF4211_SDADDJ F4211_SDADDJ,
      RawF4211.XF4211_SDHOLD F4211_SDHOLD,
      RawF4211.XF0101_ABSIC F0101_ABSIC,
      RawF4211.XF4211_SDDOCO F4211_SDDOCO,
      RawF4211.XF4211_SDDCTO F4211_SDDCTO,
      RawF4211.XF4211_SDLNID F4211_SDLNID,
      RawF4211.XF4211_SDDRQJ F4211_SDDRQJ,
      RawF4211.XF4211_SDTRDJ F4211_SDTRDJ,
      RawF4211.XF4211_SDLITM F4211_SDLITM,
      RawF4211.XF4211_SDMOT F4211_SDMOT,
      RawF4211.XF4211_SDUOM F4211_SDUOM,
      RawF4211.XF4211_SDPA8 F4211_SDPA8,
      RawF4211.XF4211_SDVR01 F4211_SDVR01,
      RawF4211.XF4211_SDUPRC F4211_SDUPRC,
      RawF4211.XID_CUSTOM_824e9012d35e34 ID_CUSTOM_824e90d952e1bfc,
      RawF4211.XID_CUSTOM_824e882404ac0e ID_CUSTOM_824e88f7ed39ece,
      RawF4211.XID_CUSTOM_824dbe1c8f8f35 ID_CUSTOM_824dbee9b61ef15,
      SUM(RawF4211.ZReportColumn30001) ReportColumn3,
      SUM(RawF4211.ZReportColumn40001) ReportColumn4,
      SUM(RawF4211.XRowIDX_F4211_0_XRowID) RowIDX_F4211_0_XRowID,
      SUM(RawF4211.XRowIDX_F4211_1_XRowID) RowIDX_F4211_1_XRowID,
      SUM(RawF4211.XRowIDX_F4211_2_XRowID) RowIDX_F4211_2_XRowID
    FROM
      (
        SELECT DISTINCT
          F4211.SDMCU XF4211_SDMCU,
          F0006.MCDL01 XF0006_MCDL01,
          F4211.SDSHAN XF4211_SDSHAN,
          F0101.ABALPH XF0101_ABALPH,
          F4211.SDSRP1 XF4211_SDSRP1,
          F4211.SDADDJ XF4211_SDADDJ,
          F4211.SDHOLD XF4211_SDHOLD,
          F0101.ABSIC XF0101_ABSIC,
          F4211.SDDOCO XF4211_SDDOCO,
          F4211.SDDCTO XF4211_SDDCTO,
          CAST(F4211.SDLNID AS FLOAT) / 1000 XF4211_SDLNID,
          F4211.SDDRQJ XF4211_SDDRQJ,
          F4211.SDTRDJ XF4211_SDTRDJ,
          F4211.SDLITM XF4211_SDLITM,
          F4211.SDMOT XF4211_SDMOT,
          F4211.SDUOM XF4211_SDUOM,
          F4211.SDPA8 XF4211_SDPA8,
          F4211.SDVR01 XF4211_SDVR01,
          CAST(F4211.SDUPRC AS FLOAT) / 1000000 XF4211_SDUPRC,
          (
            CASE
              WHEN (
                (
                  (
                    (
                      (
                        (LENGTH (TO_CHAR (F4211.SDADDJ)) < 2)
                        OR (LENGTH (TO_CHAR (F4211.SDADDJ)) < 2 + 2 - 1)
                      )
                      OR (2 IS NULL)
                    )
                    OR (2 <= 0)
                  )
                  OR (2 <= 0)
                )
                OR (2 IS NULL)
              )
              OR (F4211.SDADDJ IS NULL) THEN NULL
              ELSE SUBSTR (TO_NCHAR (F4211.SDADDJ), 2, 2)
            END - F0010.CCDFF
          ) XID_CUSTOM_824e9012d35e34,
          CASE
            WHEN (
              (
                (
                  (
                    (
                      (LENGTH (TO_CHAR (F4211.SDADDJ)) < 2)
                      OR (LENGTH (TO_CHAR (F4211.SDADDJ)) < 2 + 2 - 1)
                    )
                    OR (2 IS NULL)
                  )
                  OR (2 <= 0)
                )
                OR (2 <= 0)
              )
              OR (2 IS NULL)
            )
            OR (F4211.SDADDJ IS NULL) THEN NULL
            ELSE SUBSTR (TO_NCHAR (F4211.SDADDJ), 2, 2)
          END XID_CUSTOM_824e882404ac0e,
          CASE
            WHEN (126191 IS NULL)
            OR (126191 = 0) THEN NULL
            WHEN (F4211.SDADDJ IS NULL)
            OR (F4211.SDADDJ = 0) THEN NULL
            ELSE (TO_DATE (N'01/01/2026', N'dd/mm/yyyy') + 190) - (
              TO_DATE (
                N'01/01/' || TO_CHAR (FLOOR(TO_NUMBER (F4211.SDADDJ / 1000)) + 1900),
                N'dd/mm/yyyy'
              ) + F4211.SDADDJ - FLOOR(TO_NUMBER (F4211.SDADDJ / 1000)) * 1000 - 1
            )
          END XID_CUSTOM_824dbe1c8f8f35,
          F4211.SDPQOR ZReportColumn30001,
          F4211.SDUORG ZReportColumn40001,
          F4211.SDKCOO PK__F4211__SDKCOO,
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
          INNER JOIN PRODDTA.F0010 F0010 ON F4211.SDCO = F0010.CCCO
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
          ) F0101 ON F4211.SDSHAN = F0101.ABAN8
          INNER JOIN PRODDTA.F0006 F0006 ON F4211.SDMCU = F0006.MCMCU
        WHERE
          (
            (
              (
                ((F4211.SDDCTO IN (N'SO')))
                AND ((F0101.ABSIC IN (N'F         ', N'ISPF      ')))
              )
            )
            AND (F4211.SDHOLD < N'0 ')
          )
          AND (
            CASE
              WHEN (126191 IS NULL)
              OR (126191 = 0) THEN NULL
              WHEN (F4211.SDADDJ IS NULL)
              OR (F4211.SDADDJ = 0) THEN NULL
              ELSE (TO_DATE (N'01/01/2026', N'dd/mm/yyyy') + 190) - (
                TO_DATE (
                  N'01/01/' || TO_CHAR (FLOOR(TO_NUMBER (F4211.SDADDJ / 1000)) + 1900),
                  N'dd/mm/yyyy'
                ) + F4211.SDADDJ - FLOOR(TO_NUMBER (F4211.SDADDJ / 1000)) * 1000 - 1
              )
            END < 40
          )
      ) RawF4211
    GROUP BY
      RawF4211.XF4211_SDMCU,
      RawF4211.XF0006_MCDL01,
      RawF4211.XF4211_SDSHAN,
      RawF4211.XF0101_ABALPH,
      RawF4211.XF4211_SDSRP1,
      RawF4211.XF4211_SDADDJ,
      RawF4211.XF4211_SDHOLD,
      RawF4211.XF0101_ABSIC,
      RawF4211.XF4211_SDDOCO,
      RawF4211.XF4211_SDDCTO,
      RawF4211.XF4211_SDLNID,
      RawF4211.XF4211_SDDRQJ,
      RawF4211.XF4211_SDTRDJ,
      RawF4211.XF4211_SDLITM,
      RawF4211.XF4211_SDMOT,
      RawF4211.XF4211_SDUOM,
      RawF4211.XF4211_SDPA8,
      RawF4211.XF4211_SDVR01,
      RawF4211.XF4211_SDUPRC,
      RawF4211.XID_CUSTOM_824e9012d35e34,
      RawF4211.XID_CUSTOM_824e882404ac0e,
      RawF4211.XID_CUSTOM_824dbe1c8f8f35
  ) AggF4211
  INNER JOIN (
    SELECT
      RawF0101.XF4211_SDMCU F4211_SDMCU,
      RawF0101.XF0006_MCDL01 F0006_MCDL01,
      RawF0101.XF4211_SDSHAN F4211_SDSHAN,
      RawF0101.XF0101_ABALPH F0101_ABALPH,
      RawF0101.XF4211_SDSRP1 F4211_SDSRP1,
      RawF0101.XF4211_SDADDJ F4211_SDADDJ,
      RawF0101.XF4211_SDHOLD F4211_SDHOLD,
      RawF0101.XF0101_ABSIC F0101_ABSIC,
      RawF0101.XF4211_SDDOCO F4211_SDDOCO,
      RawF0101.XF4211_SDDCTO F4211_SDDCTO,
      RawF0101.XF4211_SDLNID F4211_SDLNID,
      RawF0101.XF4211_SDDRQJ F4211_SDDRQJ,
      RawF0101.XF4211_SDTRDJ F4211_SDTRDJ,
      RawF0101.XF4211_SDLITM F4211_SDLITM,
      RawF0101.XF4211_SDMOT F4211_SDMOT,
      RawF0101.XF4211_SDUOM F4211_SDUOM,
      RawF0101.XF4211_SDPA8 F4211_SDPA8,
      RawF0101.XF4211_SDVR01 F4211_SDVR01,
      RawF0101.XF4211_SDUPRC F4211_SDUPRC,
      RawF0101.XID_CUSTOM_824e9012d35e34 ID_CUSTOM_824e90d952e1bfc,
      RawF0101.XID_CUSTOM_824e882404ac0e ID_CUSTOM_824e88f7ed39ece,
      RawF0101.XID_CUSTOM_824dbe1c8f8f35 ID_CUSTOM_824dbee9b61ef15,
      SUM(RawF0101.ZReportColumn10001) ReportColumn1,
      SUM(RawF0101.XRowIDX_F0101_0_XRowID) RowIDX_F0101_0_XRowID,
      SUM(RawF0101.XRowIDX_F0101_1_XRowID) RowIDX_F0101_1_XRowID,
      SUM(RawF0101.XRowIDX_F0101_2_XRowID) RowIDX_F0101_2_XRowID
    FROM
      (
        SELECT DISTINCT
          F4211.SDMCU XF4211_SDMCU,
          F0006.MCDL01 XF0006_MCDL01,
          F4211.SDSHAN XF4211_SDSHAN,
          F0101.ABALPH XF0101_ABALPH,
          F4211.SDSRP1 XF4211_SDSRP1,
          F4211.SDADDJ XF4211_SDADDJ,
          F4211.SDHOLD XF4211_SDHOLD,
          F0101.ABSIC XF0101_ABSIC,
          F4211.SDDOCO XF4211_SDDOCO,
          F4211.SDDCTO XF4211_SDDCTO,
          CAST(F4211.SDLNID AS FLOAT) / 1000 XF4211_SDLNID,
          F4211.SDDRQJ XF4211_SDDRQJ,
          F4211.SDTRDJ XF4211_SDTRDJ,
          F4211.SDLITM XF4211_SDLITM,
          F4211.SDMOT XF4211_SDMOT,
          F4211.SDUOM XF4211_SDUOM,
          F4211.SDPA8 XF4211_SDPA8,
          F4211.SDVR01 XF4211_SDVR01,
          CAST(F4211.SDUPRC AS FLOAT) / 1000000 XF4211_SDUPRC,
          (
            CASE
              WHEN (
                (
                  (
                    (
                      (
                        (LENGTH (TO_CHAR (F4211.SDADDJ)) < 2)
                        OR (LENGTH (TO_CHAR (F4211.SDADDJ)) < 2 + 2 - 1)
                      )
                      OR (2 IS NULL)
                    )
                    OR (2 <= 0)
                  )
                  OR (2 <= 0)
                )
                OR (2 IS NULL)
              )
              OR (F4211.SDADDJ IS NULL) THEN NULL
              ELSE SUBSTR (TO_NCHAR (F4211.SDADDJ), 2, 2)
            END - F0010.CCDFF
          ) XID_CUSTOM_824e9012d35e34,
          CASE
            WHEN (
              (
                (
                  (
                    (
                      (LENGTH (TO_CHAR (F4211.SDADDJ)) < 2)
                      OR (LENGTH (TO_CHAR (F4211.SDADDJ)) < 2 + 2 - 1)
                    )
                    OR (2 IS NULL)
                  )
                  OR (2 <= 0)
                )
                OR (2 <= 0)
              )
              OR (2 IS NULL)
            )
            OR (F4211.SDADDJ IS NULL) THEN NULL
            ELSE SUBSTR (TO_NCHAR (F4211.SDADDJ), 2, 2)
          END XID_CUSTOM_824e882404ac0e,
          CASE
            WHEN (126191 IS NULL)
            OR (126191 = 0) THEN NULL
            WHEN (F4211.SDADDJ IS NULL)
            OR (F4211.SDADDJ = 0) THEN NULL
            ELSE (TO_DATE (N'01/01/2026', N'dd/mm/yyyy') + 190) - (
              TO_DATE (
                N'01/01/' || TO_CHAR (FLOOR(TO_NUMBER (F4211.SDADDJ / 1000)) + 1900),
                N'dd/mm/yyyy'
              ) + F4211.SDADDJ - FLOOR(TO_NUMBER (F4211.SDADDJ / 1000)) * 1000 - 1
            )
          END XID_CUSTOM_824dbe1c8f8f35,
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
          INNER JOIN PRODDTA.F0010 F0010 ON F4211.SDCO = F0010.CCCO
          INNER JOIN (
            SELECT
              F0101.ABALPH,
              F0101.ABSIC,
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
          INNER JOIN PRODDTA.F0006 F0006 ON F4211.SDMCU = F0006.MCMCU
        WHERE
          (
            (
              (
                ((F4211.SDDCTO IN (N'SO')))
                AND ((F0101.ABSIC IN (N'F         ', N'ISPF      ')))
              )
            )
            AND (F4211.SDHOLD < N'0 ')
          )
          AND (
            CASE
              WHEN (126191 IS NULL)
              OR (126191 = 0) THEN NULL
              WHEN (F4211.SDADDJ IS NULL)
              OR (F4211.SDADDJ = 0) THEN NULL
              ELSE (TO_DATE (N'01/01/2026', N'dd/mm/yyyy') + 190) - (
                TO_DATE (
                  N'01/01/' || TO_CHAR (FLOOR(TO_NUMBER (F4211.SDADDJ / 1000)) + 1900),
                  N'dd/mm/yyyy'
                ) + F4211.SDADDJ - FLOOR(TO_NUMBER (F4211.SDADDJ / 1000)) * 1000 - 1
              )
            END < 40
          )
      ) RawF0101
    GROUP BY
      RawF0101.XF4211_SDMCU,
      RawF0101.XF0006_MCDL01,
      RawF0101.XF4211_SDSHAN,
      RawF0101.XF0101_ABALPH,
      RawF0101.XF4211_SDSRP1,
      RawF0101.XF4211_SDADDJ,
      RawF0101.XF4211_SDHOLD,
      RawF0101.XF0101_ABSIC,
      RawF0101.XF4211_SDDOCO,
      RawF0101.XF4211_SDDCTO,
      RawF0101.XF4211_SDLNID,
      RawF0101.XF4211_SDDRQJ,
      RawF0101.XF4211_SDTRDJ,
      RawF0101.XF4211_SDLITM,
      RawF0101.XF4211_SDMOT,
      RawF0101.XF4211_SDUOM,
      RawF0101.XF4211_SDPA8,
      RawF0101.XF4211_SDVR01,
      RawF0101.XF4211_SDUPRC,
      RawF0101.XID_CUSTOM_824e9012d35e34,
      RawF0101.XID_CUSTOM_824e882404ac0e,
      RawF0101.XID_CUSTOM_824dbe1c8f8f35
  ) AggF0101 ON (
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
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            NVL (AggF4211.F4211_SDMCU, N'---COM.D11S---Null--') = NVL (AggF0101.F4211_SDMCU, N'---COM.D11S---Null--')
                                          )
                                          AND (
                                            NVL (AggF4211.F0006_MCDL01, N'---COM.D11S---Null--') = NVL (AggF0101.F0006_MCDL01, N'---COM.D11S---Null--')
                                          )
                                        )
                                        AND (
                                          NVL (AggF4211.F4211_SDSHAN, 1E-05) = NVL (AggF0101.F4211_SDSHAN, 1E-05)
                                        )
                                      )
                                      AND (
                                        NVL (AggF4211.F0101_ABALPH, N'---COM.D11S---Null--') = NVL (AggF0101.F0101_ABALPH, N'---COM.D11S---Null--')
                                      )
                                    )
                                    AND (
                                      NVL (AggF4211.F4211_SDSRP1, N'---COM.D11S---Null--') = NVL (AggF0101.F4211_SDSRP1, N'---COM.D11S---Null--')
                                    )
                                  )
                                  AND (
                                    NVL (AggF4211.F4211_SDADDJ, 1E-05) = NVL (AggF0101.F4211_SDADDJ, 1E-05)
                                  )
                                )
                                AND (
                                  NVL (AggF4211.F4211_SDHOLD, N'---COM.D11S---Null--') = NVL (AggF0101.F4211_SDHOLD, N'---COM.D11S---Null--')
                                )
                              )
                              AND (
                                NVL (AggF4211.F0101_ABSIC, N'---COM.D11S---Null--') = NVL (AggF0101.F0101_ABSIC, N'---COM.D11S---Null--')
                              )
                            )
                            AND (
                              NVL (AggF4211.F4211_SDDOCO, 1E-05) = NVL (AggF0101.F4211_SDDOCO, 1E-05)
                            )
                          )
                          AND (
                            NVL (AggF4211.F4211_SDDCTO, N'---COM.D11S---Null--') = NVL (AggF0101.F4211_SDDCTO, N'---COM.D11S---Null--')
                          )
                        )
                        AND (
                          NVL (AggF4211.F4211_SDLNID, 1E-05) = NVL (AggF0101.F4211_SDLNID, 1E-05)
                        )
                      )
                      AND (
                        NVL (AggF4211.F4211_SDDRQJ, 1E-05) = NVL (AggF0101.F4211_SDDRQJ, 1E-05)
                      )
                    )
                    AND (
                      NVL (AggF4211.F4211_SDTRDJ, 1E-05) = NVL (AggF0101.F4211_SDTRDJ, 1E-05)
                    )
                  )
                  AND (
                    NVL (AggF4211.F4211_SDLITM, N'---COM.D11S---Null--') = NVL (AggF0101.F4211_SDLITM, N'---COM.D11S---Null--')
                  )
                )
                AND (
                  NVL (AggF4211.F4211_SDMOT, N'---COM.D11S---Null--') = NVL (AggF0101.F4211_SDMOT, N'---COM.D11S---Null--')
                )
              )
              AND (
                NVL (AggF4211.F4211_SDUOM, N'---COM.D11S---Null--') = NVL (AggF0101.F4211_SDUOM, N'---COM.D11S---Null--')
              )
            )
            AND (
              NVL (AggF4211.F4211_SDPA8, 1E-05) = NVL (AggF0101.F4211_SDPA8, 1E-05)
            )
          )
          AND (
            NVL (AggF4211.F4211_SDVR01, N'---COM.D11S---Null--') = NVL (AggF0101.F4211_SDVR01, N'---COM.D11S---Null--')
          )
        )
        AND (
          NVL (AggF4211.F4211_SDUPRC, 1E-05) = NVL (AggF0101.F4211_SDUPRC, 1E-05)
        )
      )
      AND (
        NVL (AggF4211.ID_CUSTOM_824e90d952e1bfc, 1E-05) = NVL (AggF0101.ID_CUSTOM_824e90d952e1bfc, 1E-05)
      )
    )
    AND (
      NVL (
        AggF4211.ID_CUSTOM_824e88f7ed39ece,
        N'---COM.D11S---Null--'
      ) = NVL (
        AggF0101.ID_CUSTOM_824e88f7ed39ece,
        N'---COM.D11S---Null--'
      )
    )
  )
  AND (
    NVL (AggF4211.ID_CUSTOM_824dbee9b61ef15, 1E-05) = NVL (AggF0101.ID_CUSTOM_824dbee9b61ef15, 1E-05)
  )
ORDER BY
  F0006_MCDL01 ASC,
  F4211_SDMCU ASC,
  F0101_ABALPH ASC,
  F4211_SDSHAN ASC,
  F4211_SDSRP1 ASC,
  F4211_SDADDJ ASC,
  F0101_ABSIC ASC,
  F4211_SDHOLD ASC,
  F4211_SDDOCO ASC,
  F4211_SDDCTO ASC,
  F4211_SDLNID ASC,
  F4211_SDDRQJ ASC,
  F4211_SDTRDJ ASC,
  F4211_SDLITM ASC,
  F4211_SDMOT ASC,
  F4211_SDUOM ASC,
  F4211_SDPA8 ASC,
  F4211_SDVR01 ASC,
  F4211_SDUPRC ASC,
  ID_CUSTOM_824e90d952e1bfc ASC,
  ID_CUSTOM_824e88f7ed39ece ASC,
  ID_CUSTOM_824dbee9b61ef15 ASC