SELECT
  AggF4211.F4201_SHHOLD F4201_SHHOLD,
  AggF4211.F4211_SDRSDJ F4211_SDRSDJ,
  AggF4211.F4211_SDLNTY F4211_SDLNTY,
  AggF4211.F4211_SDDOCO F4211_SDDOCO,
  AggF4211.F4211_SDMCU F4211_SDMCU,
  AggF4211.F0101_3_ABAC05 F0101_3_ABAC05,
  AggF4211.F4211_SDVR01 F4211_SDVR01,
  AggF4211.F5642B01_BA55ODREF F5642B01_BA55ODREF,
  AggF4211.C_F4211_SDLTTR C_F4211_SDLTTR,
  AggF4211.F4211_SDNXTR F4211_SDNXTR,
  AggF4211.F4211_SDDCTO F4211_SDDCTO,
  AggF4211.C_F4211_SDSHAN C_F4211_SDSHAN,
  AggF4211.F4211_SDSHAN F4211_SDSHAN,
  AggF4211.F5642B01_BA55REF1 F5642B01_BA55REF1,
  AggF4211.F5642B01_BA55REF2 F5642B01_BA55REF2,
  AggF4211.F5642B01_BA55REF3 F5642B01_BA55REF3,
  AggF4211.F4211_SDSHPN F4211_SDSHPN,
  AggF4211.C_F4211_SDMCU C_F4211_SDMCU,
  AggF4211.F4211_SDDRQJ F4211_SDDRQJ,
  AggF4211.F4211_SDPPDJ F4211_SDPPDJ,
  AggF4211.F4211_SDLITM F4211_SDLITM,
  AggF4211.F5642B11_AK55PDCD F5642B11_AK55PDCD,
  AggF4211.F4211_SDFRTH F4211_SDFRTH,
  AggF4211.F5642B01_BA55ROUT F5642B01_BA55ROUT,
  AggF4211.F4211_SDLNID F4211_SDLNID,
  AggF4211.F5642B01_BARQSJ F5642B01_BARQSJ,
  AggF4211.F5642B01_BA55DSTPT F5642B01_BA55DSTPT,
  AggF4211.F0101_2_ABALPH F0101_2_ABALPH,
  AggF4211.F5642B01_BADLDL F5642B01_BADLDL,
  AggF4211.F5642B01_BA55VLNO F5642B01_BA55VLNO,
  AggF4211.C_F4211_SDCARS C_F4211_SDCARS,
  AggF4211.F4211_SDCARS F4211_SDCARS,
  AggF4211.F4211_SDMOT F4211_SDMOT,
  AggF4211.F5642B11_AK55PDSHNT F5642B11_AK55PDSHNT,
  AggF4211.F4211_SDCNID F4211_SDCNID,
  AggF4211.F5642B01_BA55EQTY F5642B01_BA55EQTY,
  AggF4211.F5642B01_BA55LODP F5642B01_BA55LODP,
  AggF4211.F0101_ABALPH F0101_ABALPH,
  AggF4211.F5642B01_BA55OCCR F5642B01_BA55OCCR,
  AggF4211.F0101_1_ABALPH F0101_1_ABALPH,
  AggF4211.F5642B01_BA55OCDLT F5642B01_BA55OCDLT,
  AggF4211.F5642B01_BA55NCON F5642B01_BA55NCON,
  AggF4211.F5642B01_BA55INDLT F5642B01_BA55INDLT,
  AggF4211.F5642B01_BA55INCO F5642B01_BA55INCO,
  AggF4211.ReportColumn1 ReportColumn1,
  AggF4941.ReportColumn2 ReportColumn2,
  AggF4941.RowIDX_F4941_2_XRowID,
  AggF4941.RowIDX_F4941_1_XRowID,
  AggF4941.RowIDX_F4941_0_XRowID,
  AggF4211.RowIDX_F4211_2_XRowID,
  AggF4211.RowIDX_F4211_1_XRowID,
  AggF4211.RowIDX_F4211_0_XRowID
FROM
  (
    SELECT
      RawF4941.XF4201_SHHOLD F4201_SHHOLD,
      RawF4941.XF4211_SDRSDJ F4211_SDRSDJ,
      RawF4941.XF4211_SDLNTY F4211_SDLNTY,
      RawF4941.XF4211_SDDOCO F4211_SDDOCO,
      RawF4941.XF4211_SDMCU F4211_SDMCU,
      RawF4941.XF0101_3_ABAC05 F0101_3_ABAC05,
      RawF4941.XF4211_SDVR01 F4211_SDVR01,
      RawF4941.XF5642B01_BA55ODREF F5642B01_BA55ODREF,
      RawF4941.XC_F4211_SDLTTR C_F4211_SDLTTR,
      RawF4941.XF4211_SDNXTR F4211_SDNXTR,
      RawF4941.XF4211_SDDCTO F4211_SDDCTO,
      RawF4941.XC_F4211_SDSHAN C_F4211_SDSHAN,
      RawF4941.XF4211_SDSHAN F4211_SDSHAN,
      RawF4941.XF5642B01_BA55REF1 F5642B01_BA55REF1,
      RawF4941.XF5642B01_BA55REF2 F5642B01_BA55REF2,
      RawF4941.XF5642B01_BA55REF3 F5642B01_BA55REF3,
      RawF4941.XF4211_SDSHPN F4211_SDSHPN,
      RawF4941.XC_F4211_SDMCU C_F4211_SDMCU,
      RawF4941.XF4211_SDDRQJ F4211_SDDRQJ,
      RawF4941.XF4211_SDPPDJ F4211_SDPPDJ,
      RawF4941.XF4211_SDLITM F4211_SDLITM,
      RawF4941.XF5642B11_AK55PDCD F5642B11_AK55PDCD,
      RawF4941.XF4211_SDFRTH F4211_SDFRTH,
      RawF4941.XF5642B01_BA55ROUT F5642B01_BA55ROUT,
      RawF4941.XF4211_SDLNID F4211_SDLNID,
      RawF4941.XF5642B01_BARQSJ F5642B01_BARQSJ,
      RawF4941.XF5642B01_BA55DSTPT F5642B01_BA55DSTPT,
      RawF4941.XF0101_2_ABALPH F0101_2_ABALPH,
      RawF4941.XF5642B01_BADLDL F5642B01_BADLDL,
      RawF4941.XF5642B01_BA55VLNO F5642B01_BA55VLNO,
      RawF4941.XC_F4211_SDCARS C_F4211_SDCARS,
      RawF4941.XF4211_SDCARS F4211_SDCARS,
      RawF4941.XF4211_SDMOT F4211_SDMOT,
      RawF4941.XF5642B11_AK55PDSHNT F5642B11_AK55PDSHNT,
      RawF4941.XF4211_SDCNID F4211_SDCNID,
      RawF4941.XF5642B01_BA55EQTY F5642B01_BA55EQTY,
      RawF4941.XF5642B01_BA55LODP F5642B01_BA55LODP,
      RawF4941.XF0101_ABALPH F0101_ABALPH,
      RawF4941.XF5642B01_BA55OCCR F5642B01_BA55OCCR,
      RawF4941.XF0101_1_ABALPH F0101_1_ABALPH,
      RawF4941.XF5642B01_BA55OCDLT F5642B01_BA55OCDLT,
      RawF4941.XF5642B01_BA55NCON F5642B01_BA55NCON,
      RawF4941.XF5642B01_BA55INDLT F5642B01_BA55INDLT,
      RawF4941.XF5642B01_BA55INCO F5642B01_BA55INCO,
      SUM(RawF4941.ZReportColumn20001) ReportColumn2,
      SUM(RawF4941.XRowIDX_F4941_0_XRowID) RowIDX_F4941_0_XRowID,
      SUM(RawF4941.XRowIDX_F4941_1_XRowID) RowIDX_F4941_1_XRowID,
      SUM(RawF4941.XRowIDX_F4941_2_XRowID) RowIDX_F4941_2_XRowID
    FROM
      (
        SELECT DISTINCT
          F4201.SHHOLD XF4201_SHHOLD,
          F4211.SDRSDJ XF4211_SDRSDJ,
          F4211.SDLNTY XF4211_SDLNTY,
          F4211.SDDOCO XF4211_SDDOCO,
          F4211.SDMCU XF4211_SDMCU,
          F0101_3.ABAC05 XF0101_3_ABAC05,
          F4211.SDVR01 XF4211_SDVR01,
          F5642B01.BA55ODREF XF5642B01_BA55ODREF,
          F4211.SDLTTR XC_F4211_SDLTTR,
          F4211.SDNXTR XF4211_SDNXTR,
          F4211.SDDCTO XF4211_SDDCTO,
          F4211.SDSHAN XC_F4211_SDSHAN,
          F4211.SDSHAN XF4211_SDSHAN,
          F5642B01.BA55REF1 XF5642B01_BA55REF1,
          F5642B01.BA55REF2 XF5642B01_BA55REF2,
          F5642B01.BA55REF3 XF5642B01_BA55REF3,
          F4211.SDSHPN XF4211_SDSHPN,
          F4211.SDMCU XC_F4211_SDMCU,
          F4211.SDDRQJ XF4211_SDDRQJ,
          F4211.SDPPDJ XF4211_SDPPDJ,
          F4211.SDLITM XF4211_SDLITM,
          F5642B11.AK55PDCD XF5642B11_AK55PDCD,
          F4211.SDFRTH XF4211_SDFRTH,
          F5642B01.BA55ROUT XF5642B01_BA55ROUT,
          CAST(F4211.SDLNID AS FLOAT) / 1000 XF4211_SDLNID,
          F5642B01.BARQSJ XF5642B01_BARQSJ,
          F5642B01.BA55DSTPT XF5642B01_BA55DSTPT,
          F0101_2.ABALPH XF0101_2_ABALPH,
          F5642B01.BADLDL XF5642B01_BADLDL,
          F5642B01.BA55VLNO XF5642B01_BA55VLNO,
          F4211.SDCARS XC_F4211_SDCARS,
          F4211.SDCARS XF4211_SDCARS,
          F4211.SDMOT XF4211_SDMOT,
          F5642B11.AK55PDSHNT XF5642B11_AK55PDSHNT,
          F4211.SDCNID XF4211_SDCNID,
          F5642B01.BA55EQTY XF5642B01_BA55EQTY,
          F5642B01.BA55LODP XF5642B01_BA55LODP,
          F0101.ABALPH XF0101_ABALPH,
          F5642B01.BA55OCCR XF5642B01_BA55OCCR,
          F0101_1.ABALPH XF0101_1_ABALPH,
          F5642B01.BA55OCDLT XF5642B01_BA55OCDLT,
          F5642B01.BA55NCON XF5642B01_BA55NCON,
          F5642B01.BA55INDLT XF5642B01_BA55INDLT,
          F5642B01.BA55INCO XF5642B01_BA55INCO,
          F4941.RSNCTR ZReportColumn20001,
          F4941.RSSHPN PK__F4941__RSSHPN,
          F4941.RSRSSN PK__F4941__RSRSSN,
          DBMS_UTILITY.GET_HASH_VALUE (
            TO_CHAR (F4941.RSSHPN) || N'_ISS_' || TO_CHAR (F4941.RSRSSN) || N'_InDeX_0_SaLt',
            -1048576,
            2097152
          ) XRowIDX_F4941_0_XRowID,
          DBMS_UTILITY.GET_HASH_VALUE (
            TO_CHAR (F4941.RSSHPN) || N'_ISS_' || TO_CHAR (F4941.RSRSSN) || N'_InDeX_1_SaLt',
            -1048576,
            2097152
          ) XRowIDX_F4941_1_XRowID,
          DBMS_UTILITY.GET_HASH_VALUE (
            TO_CHAR (F4941.RSSHPN) || N'_ISS_' || TO_CHAR (F4941.RSRSSN) || N'_InDeX_2_SaLt',
            -1048576,
            2097152
          ) XRowIDX_F4941_2_XRowID
        FROM
          PRODDTA.F4941 F4941
          INNER JOIN PRODDTA.F4211 F4211
          INNER JOIN (
            SELECT
              F0101_3.ABAC05,
              F0101_3.ABAN8
            FROM
              PRODDTA.F0101 F0101_3
            WHERE
              (
                (F0101_3.ABAT1 BETWEEN N'A  ' AND N'P  ')
                OR (F0101_3.ABAT1 BETWEEN N'R  ' AND N'ZZZ')
              )
          ) F0101_3 ON F4211.SDAN8 = F0101_3.ABAN8
          INNER JOIN PRODDTA.F4201 F4201 ON (
            (F4211.SDKCOO = F4201.SHKCOO)
            AND (F4211.SDDOCO = F4201.SHDOCO)
          )
          AND (F4211.SDDCTO = F4201.SHDCTO) ON (F4211.SDSHPN = F4941.RSSHPN)
          AND (F4941.RSMOT = N'OCE')
          LEFT JOIN PRODDTA.F5642B01 F5642B01
          INNER JOIN PRODDTA.F0101 F0101_1 ON F5642B01.BA55OCCR = F0101_1.ABAN8
          INNER JOIN PRODDTA.F0101 F0101 ON F5642B01.BA55LODP = F0101.ABAN8
          INNER JOIN PRODDTA.F0101 F0101_2 ON F5642B01.BA55DSTPT = F0101_2.ABAN8 ON (
            (
              (F4211.SDSHPN = F5642B01.BASHPN)
              AND (F4211.SDKCOO = F5642B01.BAKCOO)
            )
            AND (F4211.SDDCTO = F5642B01.BADCTO)
          )
          AND (F4211.SDDOCO = F5642B01.BADOCO)
          LEFT JOIN PRODDTA.F5642B11 F5642B11 ON (
            (
              (
                (F4211.SDKCOO = F5642B11.AKKCOO)
                AND (F4211.SDDOCO = F5642B11.AKDOCO)
              )
              AND (F4211.SDDCTO = F5642B11.AKDCTO)
            )
            AND (F4211.SDLNID = F5642B11.AKLNID)
          )
          AND (F4211.SDSHPN = F5642B11.AKSHPN)
        WHERE
          (
            (
              (
                (
                  (
                    (
                      (
                        ((F4211.SDDCTO IN (N'SE')))
                        AND ((F4211.SDKCOO IN (N'00640', N'00645')))
                      )
                    )
                    AND ((F4211.SDLNTY IN (N'S ')))
                  )
                )
                AND ((F4211.SDMCU IN (N'         651')))
              )
            )
            AND ((NOT (F4211.SDNXTR IN (N'980', N'999'))))
          )
      ) RawF4941
    GROUP BY
      RawF4941.XF4201_SHHOLD,
      RawF4941.XF4211_SDRSDJ,
      RawF4941.XF4211_SDLNTY,
      RawF4941.XF4211_SDDOCO,
      RawF4941.XF4211_SDMCU,
      RawF4941.XF0101_3_ABAC05,
      RawF4941.XF4211_SDVR01,
      RawF4941.XF5642B01_BA55ODREF,
      RawF4941.XC_F4211_SDLTTR,
      RawF4941.XF4211_SDNXTR,
      RawF4941.XF4211_SDDCTO,
      RawF4941.XC_F4211_SDSHAN,
      RawF4941.XF4211_SDSHAN,
      RawF4941.XF5642B01_BA55REF1,
      RawF4941.XF5642B01_BA55REF2,
      RawF4941.XF5642B01_BA55REF3,
      RawF4941.XF4211_SDSHPN,
      RawF4941.XC_F4211_SDMCU,
      RawF4941.XF4211_SDDRQJ,
      RawF4941.XF4211_SDPPDJ,
      RawF4941.XF4211_SDLITM,
      RawF4941.XF5642B11_AK55PDCD,
      RawF4941.XF4211_SDFRTH,
      RawF4941.XF5642B01_BA55ROUT,
      RawF4941.XF4211_SDLNID,
      RawF4941.XF5642B01_BARQSJ,
      RawF4941.XF5642B01_BA55DSTPT,
      RawF4941.XF0101_2_ABALPH,
      RawF4941.XF5642B01_BADLDL,
      RawF4941.XF5642B01_BA55VLNO,
      RawF4941.XC_F4211_SDCARS,
      RawF4941.XF4211_SDCARS,
      RawF4941.XF4211_SDMOT,
      RawF4941.XF5642B11_AK55PDSHNT,
      RawF4941.XF4211_SDCNID,
      RawF4941.XF5642B01_BA55EQTY,
      RawF4941.XF5642B01_BA55LODP,
      RawF4941.XF0101_ABALPH,
      RawF4941.XF5642B01_BA55OCCR,
      RawF4941.XF0101_1_ABALPH,
      RawF4941.XF5642B01_BA55OCDLT,
      RawF4941.XF5642B01_BA55NCON,
      RawF4941.XF5642B01_BA55INDLT,
      RawF4941.XF5642B01_BA55INCO
  ) AggF4941
  INNER JOIN (
    SELECT
      RawF4211.XF4201_SHHOLD F4201_SHHOLD,
      RawF4211.XF4211_SDRSDJ F4211_SDRSDJ,
      RawF4211.XF4211_SDLNTY F4211_SDLNTY,
      RawF4211.XF4211_SDDOCO F4211_SDDOCO,
      RawF4211.XF4211_SDMCU F4211_SDMCU,
      RawF4211.XF0101_3_ABAC05 F0101_3_ABAC05,
      RawF4211.XF4211_SDVR01 F4211_SDVR01,
      RawF4211.XF5642B01_BA55ODREF F5642B01_BA55ODREF,
      RawF4211.XC_F4211_SDLTTR C_F4211_SDLTTR,
      RawF4211.XF4211_SDNXTR F4211_SDNXTR,
      RawF4211.XF4211_SDDCTO F4211_SDDCTO,
      RawF4211.XC_F4211_SDSHAN C_F4211_SDSHAN,
      RawF4211.XF4211_SDSHAN F4211_SDSHAN,
      RawF4211.XF5642B01_BA55REF1 F5642B01_BA55REF1,
      RawF4211.XF5642B01_BA55REF2 F5642B01_BA55REF2,
      RawF4211.XF5642B01_BA55REF3 F5642B01_BA55REF3,
      RawF4211.XF4211_SDSHPN F4211_SDSHPN,
      RawF4211.XC_F4211_SDMCU C_F4211_SDMCU,
      RawF4211.XF4211_SDDRQJ F4211_SDDRQJ,
      RawF4211.XF4211_SDPPDJ F4211_SDPPDJ,
      RawF4211.XF4211_SDLITM F4211_SDLITM,
      RawF4211.XF5642B11_AK55PDCD F5642B11_AK55PDCD,
      RawF4211.XF4211_SDFRTH F4211_SDFRTH,
      RawF4211.XF5642B01_BA55ROUT F5642B01_BA55ROUT,
      RawF4211.XF4211_SDLNID F4211_SDLNID,
      RawF4211.XF5642B01_BARQSJ F5642B01_BARQSJ,
      RawF4211.XF5642B01_BA55DSTPT F5642B01_BA55DSTPT,
      RawF4211.XF0101_2_ABALPH F0101_2_ABALPH,
      RawF4211.XF5642B01_BADLDL F5642B01_BADLDL,
      RawF4211.XF5642B01_BA55VLNO F5642B01_BA55VLNO,
      RawF4211.XC_F4211_SDCARS C_F4211_SDCARS,
      RawF4211.XF4211_SDCARS F4211_SDCARS,
      RawF4211.XF4211_SDMOT F4211_SDMOT,
      RawF4211.XF5642B11_AK55PDSHNT F5642B11_AK55PDSHNT,
      RawF4211.XF4211_SDCNID F4211_SDCNID,
      RawF4211.XF5642B01_BA55EQTY F5642B01_BA55EQTY,
      RawF4211.XF5642B01_BA55LODP F5642B01_BA55LODP,
      RawF4211.XF0101_ABALPH F0101_ABALPH,
      RawF4211.XF5642B01_BA55OCCR F5642B01_BA55OCCR,
      RawF4211.XF0101_1_ABALPH F0101_1_ABALPH,
      RawF4211.XF5642B01_BA55OCDLT F5642B01_BA55OCDLT,
      RawF4211.XF5642B01_BA55NCON F5642B01_BA55NCON,
      RawF4211.XF5642B01_BA55INDLT F5642B01_BA55INDLT,
      RawF4211.XF5642B01_BA55INCO F5642B01_BA55INCO,
      SUM(RawF4211.ZReportColumn10001) ReportColumn1,
      SUM(RawF4211.XRowIDX_F4211_0_XRowID) RowIDX_F4211_0_XRowID,
      SUM(RawF4211.XRowIDX_F4211_1_XRowID) RowIDX_F4211_1_XRowID,
      SUM(RawF4211.XRowIDX_F4211_2_XRowID) RowIDX_F4211_2_XRowID
    FROM
      (
        SELECT DISTINCT
          F4201.SHHOLD XF4201_SHHOLD,
          F4211.SDRSDJ XF4211_SDRSDJ,
          F4211.SDLNTY XF4211_SDLNTY,
          F4211.SDDOCO XF4211_SDDOCO,
          F4211.SDMCU XF4211_SDMCU,
          F0101_3.ABAC05 XF0101_3_ABAC05,
          F4211.SDVR01 XF4211_SDVR01,
          F5642B01.BA55ODREF XF5642B01_BA55ODREF,
          F4211.SDLTTR XC_F4211_SDLTTR,
          F4211.SDNXTR XF4211_SDNXTR,
          F4211.SDDCTO XF4211_SDDCTO,
          F4211.SDSHAN XC_F4211_SDSHAN,
          F4211.SDSHAN XF4211_SDSHAN,
          F5642B01.BA55REF1 XF5642B01_BA55REF1,
          F5642B01.BA55REF2 XF5642B01_BA55REF2,
          F5642B01.BA55REF3 XF5642B01_BA55REF3,
          F4211.SDSHPN XF4211_SDSHPN,
          F4211.SDMCU XC_F4211_SDMCU,
          F4211.SDDRQJ XF4211_SDDRQJ,
          F4211.SDPPDJ XF4211_SDPPDJ,
          F4211.SDLITM XF4211_SDLITM,
          F5642B11.AK55PDCD XF5642B11_AK55PDCD,
          F4211.SDFRTH XF4211_SDFRTH,
          F5642B01.BA55ROUT XF5642B01_BA55ROUT,
          CAST(F4211.SDLNID AS FLOAT) / 1000 XF4211_SDLNID,
          F5642B01.BARQSJ XF5642B01_BARQSJ,
          F5642B01.BA55DSTPT XF5642B01_BA55DSTPT,
          F0101_2.ABALPH XF0101_2_ABALPH,
          F5642B01.BADLDL XF5642B01_BADLDL,
          F5642B01.BA55VLNO XF5642B01_BA55VLNO,
          F4211.SDCARS XC_F4211_SDCARS,
          F4211.SDCARS XF4211_SDCARS,
          F4211.SDMOT XF4211_SDMOT,
          F5642B11.AK55PDSHNT XF5642B11_AK55PDSHNT,
          F4211.SDCNID XF4211_SDCNID,
          F5642B01.BA55EQTY XF5642B01_BA55EQTY,
          F5642B01.BA55LODP XF5642B01_BA55LODP,
          F0101.ABALPH XF0101_ABALPH,
          F5642B01.BA55OCCR XF5642B01_BA55OCCR,
          F0101_1.ABALPH XF0101_1_ABALPH,
          F5642B01.BA55OCDLT XF5642B01_BA55OCDLT,
          F5642B01.BA55NCON XF5642B01_BA55NCON,
          F5642B01.BA55INDLT XF5642B01_BA55INDLT,
          F5642B01.BA55INCO XF5642B01_BA55INCO,
          F4211.SDUORG ZReportColumn10001,
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
          PRODDTA.F4941 F4941
          INNER JOIN PRODDTA.F4211 F4211
          INNER JOIN (
            SELECT
              F0101_3.ABAC05,
              F0101_3.ABAN8
            FROM
              PRODDTA.F0101 F0101_3
            WHERE
              (
                (F0101_3.ABAT1 BETWEEN N'A  ' AND N'P  ')
                OR (F0101_3.ABAT1 BETWEEN N'R  ' AND N'ZZZ')
              )
          ) F0101_3 ON F4211.SDAN8 = F0101_3.ABAN8
          INNER JOIN PRODDTA.F4201 F4201 ON (
            (F4211.SDKCOO = F4201.SHKCOO)
            AND (F4211.SDDOCO = F4201.SHDOCO)
          )
          AND (F4211.SDDCTO = F4201.SHDCTO) ON (F4211.SDSHPN = F4941.RSSHPN)
          AND (F4941.RSMOT = N'OCE')
          LEFT JOIN PRODDTA.F5642B01 F5642B01
          INNER JOIN PRODDTA.F0101 F0101_1 ON F5642B01.BA55OCCR = F0101_1.ABAN8
          INNER JOIN PRODDTA.F0101 F0101 ON F5642B01.BA55LODP = F0101.ABAN8
          INNER JOIN PRODDTA.F0101 F0101_2 ON F5642B01.BA55DSTPT = F0101_2.ABAN8 ON (
            (
              (F4211.SDSHPN = F5642B01.BASHPN)
              AND (F4211.SDKCOO = F5642B01.BAKCOO)
            )
            AND (F4211.SDDCTO = F5642B01.BADCTO)
          )
          AND (F4211.SDDOCO = F5642B01.BADOCO)
          LEFT JOIN PRODDTA.F5642B11 F5642B11 ON (
            (
              (
                (F4211.SDKCOO = F5642B11.AKKCOO)
                AND (F4211.SDDOCO = F5642B11.AKDOCO)
              )
              AND (F4211.SDDCTO = F5642B11.AKDCTO)
            )
            AND (F4211.SDLNID = F5642B11.AKLNID)
          )
          AND (F4211.SDSHPN = F5642B11.AKSHPN)
        WHERE
          (
            (
              (
                (
                  (
                    (
                      (
                        ((F4211.SDDCTO IN (N'SE')))
                        AND ((F4211.SDKCOO IN (N'00640', N'00645')))
                      )
                    )
                    AND ((F4211.SDLNTY IN (N'S ')))
                  )
                )
                AND ((F4211.SDMCU IN (N'         651')))
              )
            )
            AND ((NOT (F4211.SDNXTR IN (N'980', N'999'))))
          )
      ) RawF4211
    GROUP BY
      RawF4211.XF4201_SHHOLD,
      RawF4211.XF4211_SDRSDJ,
      RawF4211.XF4211_SDLNTY,
      RawF4211.XF4211_SDDOCO,
      RawF4211.XF4211_SDMCU,
      RawF4211.XF0101_3_ABAC05,
      RawF4211.XF4211_SDVR01,
      RawF4211.XF5642B01_BA55ODREF,
      RawF4211.XC_F4211_SDLTTR,
      RawF4211.XF4211_SDNXTR,
      RawF4211.XF4211_SDDCTO,
      RawF4211.XC_F4211_SDSHAN,
      RawF4211.XF4211_SDSHAN,
      RawF4211.XF5642B01_BA55REF1,
      RawF4211.XF5642B01_BA55REF2,
      RawF4211.XF5642B01_BA55REF3,
      RawF4211.XF4211_SDSHPN,
      RawF4211.XC_F4211_SDMCU,
      RawF4211.XF4211_SDDRQJ,
      RawF4211.XF4211_SDPPDJ,
      RawF4211.XF4211_SDLITM,
      RawF4211.XF5642B11_AK55PDCD,
      RawF4211.XF4211_SDFRTH,
      RawF4211.XF5642B01_BA55ROUT,
      RawF4211.XF4211_SDLNID,
      RawF4211.XF5642B01_BARQSJ,
      RawF4211.XF5642B01_BA55DSTPT,
      RawF4211.XF0101_2_ABALPH,
      RawF4211.XF5642B01_BADLDL,
      RawF4211.XF5642B01_BA55VLNO,
      RawF4211.XC_F4211_SDCARS,
      RawF4211.XF4211_SDCARS,
      RawF4211.XF4211_SDMOT,
      RawF4211.XF5642B11_AK55PDSHNT,
      RawF4211.XF4211_SDCNID,
      RawF4211.XF5642B01_BA55EQTY,
      RawF4211.XF5642B01_BA55LODP,
      RawF4211.XF0101_ABALPH,
      RawF4211.XF5642B01_BA55OCCR,
      RawF4211.XF0101_1_ABALPH,
      RawF4211.XF5642B01_BA55OCDLT,
      RawF4211.XF5642B01_BA55NCON,
      RawF4211.XF5642B01_BA55INDLT,
      RawF4211.XF5642B01_BA55INCO
  ) AggF4211 ON (
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
                                                                                    (
                                                                                      (
                                                                                        NVL (AggF4941.F4201_SHHOLD, N'---COM.D11S---Null--') = NVL (AggF4211.F4201_SHHOLD, N'---COM.D11S---Null--')
                                                                                      )
                                                                                      AND (
                                                                                        NVL (AggF4941.F4211_SDRSDJ, 1E-05) = NVL (AggF4211.F4211_SDRSDJ, 1E-05)
                                                                                      )
                                                                                    )
                                                                                    AND (
                                                                                      NVL (AggF4941.F4211_SDLNTY, N'---COM.D11S---Null--') = NVL (AggF4211.F4211_SDLNTY, N'---COM.D11S---Null--')
                                                                                    )
                                                                                  )
                                                                                  AND (
                                                                                    NVL (AggF4941.F4211_SDDOCO, 1E-05) = NVL (AggF4211.F4211_SDDOCO, 1E-05)
                                                                                  )
                                                                                )
                                                                                AND (
                                                                                  NVL (AggF4941.F4211_SDMCU, N'---COM.D11S---Null--') = NVL (AggF4211.F4211_SDMCU, N'---COM.D11S---Null--')
                                                                                )
                                                                              )
                                                                              AND (
                                                                                NVL (
                                                                                  TO_CHAR (AggF4941.F0101_3_ABAC05),
                                                                                  N'---COM.D11S---Null--'
                                                                                ) = NVL (
                                                                                  TO_CHAR (AggF4211.F0101_3_ABAC05),
                                                                                  N'---COM.D11S---Null--'
                                                                                )
                                                                              )
                                                                            )
                                                                            AND (
                                                                              NVL (AggF4941.F4211_SDVR01, N'---COM.D11S---Null--') = NVL (AggF4211.F4211_SDVR01, N'---COM.D11S---Null--')
                                                                            )
                                                                          )
                                                                          AND (
                                                                            NVL (AggF4941.F5642B01_BA55ODREF, 1E-05) = NVL (AggF4211.F5642B01_BA55ODREF, 1E-05)
                                                                          )
                                                                        )
                                                                        AND (
                                                                          NVL (
                                                                            TO_CHAR (AggF4941.C_F4211_SDLTTR),
                                                                            N'---COM.D11S---Null--'
                                                                          ) = NVL (
                                                                            TO_CHAR (AggF4211.C_F4211_SDLTTR),
                                                                            N'---COM.D11S---Null--'
                                                                          )
                                                                        )
                                                                      )
                                                                      AND (
                                                                        NVL (AggF4941.F4211_SDNXTR, N'---COM.D11S---Null--') = NVL (AggF4211.F4211_SDNXTR, N'---COM.D11S---Null--')
                                                                      )
                                                                    )
                                                                    AND (
                                                                      NVL (AggF4941.F4211_SDDCTO, N'---COM.D11S---Null--') = NVL (AggF4211.F4211_SDDCTO, N'---COM.D11S---Null--')
                                                                    )
                                                                  )
                                                                  AND (
                                                                    NVL (
                                                                      TO_CHAR (AggF4941.C_F4211_SDSHAN),
                                                                      N'---COM.D11S---Null--'
                                                                    ) = NVL (
                                                                      TO_CHAR (AggF4211.C_F4211_SDSHAN),
                                                                      N'---COM.D11S---Null--'
                                                                    )
                                                                  )
                                                                )
                                                                AND (
                                                                  NVL (AggF4941.F4211_SDSHAN, 1E-05) = NVL (AggF4211.F4211_SDSHAN, 1E-05)
                                                                )
                                                              )
                                                              AND (
                                                                NVL (
                                                                  AggF4941.F5642B01_BA55REF1,
                                                                  N'---COM.D11S---Null--'
                                                                ) = NVL (
                                                                  AggF4211.F5642B01_BA55REF1,
                                                                  N'---COM.D11S---Null--'
                                                                )
                                                              )
                                                            )
                                                            AND (
                                                              NVL (
                                                                AggF4941.F5642B01_BA55REF2,
                                                                N'---COM.D11S---Null--'
                                                              ) = NVL (
                                                                AggF4211.F5642B01_BA55REF2,
                                                                N'---COM.D11S---Null--'
                                                              )
                                                            )
                                                          )
                                                          AND (
                                                            NVL (
                                                              AggF4941.F5642B01_BA55REF3,
                                                              N'---COM.D11S---Null--'
                                                            ) = NVL (
                                                              AggF4211.F5642B01_BA55REF3,
                                                              N'---COM.D11S---Null--'
                                                            )
                                                          )
                                                        )
                                                        AND (
                                                          NVL (AggF4941.F4211_SDSHPN, 1E-05) = NVL (AggF4211.F4211_SDSHPN, 1E-05)
                                                        )
                                                      )
                                                      AND (
                                                        NVL (
                                                          TO_CHAR (AggF4941.C_F4211_SDMCU),
                                                          N'---COM.D11S---Null--'
                                                        ) = NVL (
                                                          TO_CHAR (AggF4211.C_F4211_SDMCU),
                                                          N'---COM.D11S---Null--'
                                                        )
                                                      )
                                                    )
                                                    AND (
                                                      NVL (AggF4941.F4211_SDDRQJ, 1E-05) = NVL (AggF4211.F4211_SDDRQJ, 1E-05)
                                                    )
                                                  )
                                                  AND (
                                                    NVL (AggF4941.F4211_SDPPDJ, 1E-05) = NVL (AggF4211.F4211_SDPPDJ, 1E-05)
                                                  )
                                                )
                                                AND (
                                                  NVL (AggF4941.F4211_SDLITM, N'---COM.D11S---Null--') = NVL (AggF4211.F4211_SDLITM, N'---COM.D11S---Null--')
                                                )
                                              )
                                              AND (
                                                NVL (
                                                  AggF4941.F5642B11_AK55PDCD,
                                                  N'---COM.D11S---Null--'
                                                ) = NVL (
                                                  AggF4211.F5642B11_AK55PDCD,
                                                  N'---COM.D11S---Null--'
                                                )
                                              )
                                            )
                                            AND (
                                              NVL (AggF4941.F4211_SDFRTH, N'---COM.D11S---Null--') = NVL (AggF4211.F4211_SDFRTH, N'---COM.D11S---Null--')
                                            )
                                          )
                                          AND (
                                            NVL (
                                              AggF4941.F5642B01_BA55ROUT,
                                              N'---COM.D11S---Null--'
                                            ) = NVL (
                                              AggF4211.F5642B01_BA55ROUT,
                                              N'---COM.D11S---Null--'
                                            )
                                          )
                                        )
                                        AND (
                                          NVL (AggF4941.F4211_SDLNID, 1E-05) = NVL (AggF4211.F4211_SDLNID, 1E-05)
                                        )
                                      )
                                      AND (
                                        NVL (AggF4941.F5642B01_BARQSJ, 1E-05) = NVL (AggF4211.F5642B01_BARQSJ, 1E-05)
                                      )
                                    )
                                    AND (
                                      NVL (AggF4941.F5642B01_BA55DSTPT, 1E-05) = NVL (AggF4211.F5642B01_BA55DSTPT, 1E-05)
                                    )
                                  )
                                  AND (
                                    NVL (
                                      TO_CHAR (AggF4941.F0101_2_ABALPH),
                                      N'---COM.D11S---Null--'
                                    ) = NVL (
                                      TO_CHAR (AggF4211.F0101_2_ABALPH),
                                      N'---COM.D11S---Null--'
                                    )
                                  )
                                )
                                AND (
                                  NVL (AggF4941.F5642B01_BADLDL, 1E-05) = NVL (AggF4211.F5642B01_BADLDL, 1E-05)
                                )
                              )
                              AND (
                                NVL (
                                  AggF4941.F5642B01_BA55VLNO,
                                  N'---COM.D11S---Null--'
                                ) = NVL (
                                  AggF4211.F5642B01_BA55VLNO,
                                  N'---COM.D11S---Null--'
                                )
                              )
                            )
                            AND (
                              NVL (
                                TO_CHAR (AggF4941.C_F4211_SDCARS),
                                N'---COM.D11S---Null--'
                              ) = NVL (
                                TO_CHAR (AggF4211.C_F4211_SDCARS),
                                N'---COM.D11S---Null--'
                              )
                            )
                          )
                          AND (
                            NVL (AggF4941.F4211_SDCARS, 1E-05) = NVL (AggF4211.F4211_SDCARS, 1E-05)
                          )
                        )
                        AND (
                          NVL (AggF4941.F4211_SDMOT, N'---COM.D11S---Null--') = NVL (AggF4211.F4211_SDMOT, N'---COM.D11S---Null--')
                        )
                      )
                      AND (
                        NVL (
                          AggF4941.F5642B11_AK55PDSHNT,
                          N'---COM.D11S---Null--'
                        ) = NVL (
                          AggF4211.F5642B11_AK55PDSHNT,
                          N'---COM.D11S---Null--'
                        )
                      )
                    )
                    AND (
                      NVL (AggF4941.F4211_SDCNID, N'---COM.D11S---Null--') = NVL (AggF4211.F4211_SDCNID, N'---COM.D11S---Null--')
                    )
                  )
                  AND (
                    NVL (
                      AggF4941.F5642B01_BA55EQTY,
                      N'---COM.D11S---Null--'
                    ) = NVL (
                      AggF4211.F5642B01_BA55EQTY,
                      N'---COM.D11S---Null--'
                    )
                  )
                )
                AND (
                  NVL (AggF4941.F5642B01_BA55LODP, 1E-05) = NVL (AggF4211.F5642B01_BA55LODP, 1E-05)
                )
              )
              AND (
                NVL (AggF4941.F0101_ABALPH, N'---COM.D11S---Null--') = NVL (AggF4211.F0101_ABALPH, N'---COM.D11S---Null--')
              )
            )
            AND (
              NVL (AggF4941.F5642B01_BA55OCCR, 1E-05) = NVL (AggF4211.F5642B01_BA55OCCR, 1E-05)
            )
          )
          AND (
            NVL (
              TO_CHAR (AggF4941.F0101_1_ABALPH),
              N'---COM.D11S---Null--'
            ) = NVL (
              TO_CHAR (AggF4211.F0101_1_ABALPH),
              N'---COM.D11S---Null--'
            )
          )
        )
        AND (
          NVL (
            AggF4941.F5642B01_BA55OCDLT,
            N'---COM.D11S---Null--'
          ) = NVL (
            AggF4211.F5642B01_BA55OCDLT,
            N'---COM.D11S---Null--'
          )
        )
      )
      AND (
        NVL (AggF4941.F5642B01_BA55NCON, 1E-05) = NVL (AggF4211.F5642B01_BA55NCON, 1E-05)
      )
    )
    AND (
      NVL (
        AggF4941.F5642B01_BA55INDLT,
        N'---COM.D11S---Null--'
      ) = NVL (
        AggF4211.F5642B01_BA55INDLT,
        N'---COM.D11S---Null--'
      )
    )
  )
  AND (
    NVL (
      AggF4941.F5642B01_BA55INCO,
      N'---COM.D11S---Null--'
    ) = NVL (
      AggF4211.F5642B01_BA55INCO,
      N'---COM.D11S---Null--'
    )
  )
ORDER BY
  F4211_SDDCTO ASC,
  F4211_SDDOCO ASC,
  F4211_SDMCU ASC,
  F4211_SDSHAN ASC,
  F4211_SDLITM ASC,
  F4211_SDFRTH ASC,
  F4211_SDPPDJ ASC,
  F4211_SDDRQJ ASC,
  F4211_SDSHPN ASC,
  F4211_SDLNID ASC,
  F4211_SDCNID ASC,
  F4211_SDVR01 ASC,
  F4211_SDMOT ASC,
  F5642B11_AK55PDCD ASC,
  F5642B11_AK55PDSHNT ASC,
  F5642B01_BA55INDLT ASC,
  F5642B01_BA55LODP ASC,
  F5642B01_BA55OCCR ASC,
  F5642B01_BA55OCDLT ASC,
  F5642B01_BA55INCO ASC,
  F5642B01_BA55REF1 ASC,
  F0101_ABALPH ASC,
  F0101_1_ABALPH ASC,
  F5642B01_BA55NCON ASC,
  F4211_SDCARS ASC,
  F5642B01_BA55ROUT ASC,
  F5642B01_BARQSJ ASC,
  F5642B01_BA55DSTPT ASC,
  F0101_2_ABALPH ASC,
  F5642B01_BADLDL ASC,
  F5642B01_BA55VLNO ASC,
  F4211_SDLNTY ASC,
  F4211_SDRSDJ ASC,
  F0101_3_ABAC05 ASC,
  F5642B01_BA55ODREF ASC,
  F4211_SDNXTR ASC,
  F5642B01_BA55REF2 ASC,
  F5642B01_BA55REF3 ASC,
  F5642B01_BA55EQTY ASC,
  F4201_SHHOLD ASC