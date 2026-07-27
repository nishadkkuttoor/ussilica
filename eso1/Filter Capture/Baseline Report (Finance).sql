SELECT
  AggF4211.F4201_SHHOLD F4201_SHHOLD,
  AggF4211.F0101_ABSIC F0101_ABSIC,
  AggF4211.F4211_SDMCU F4211_SDMCU,
  AggF4211.F0101_ABALPH F0101_ABALPH,
  AggF4211.F0116_ALCTY1 F0116_ALCTY1,
  AggF4211.F0116_ALADDS F0116_ALADDS,
  AggF4211.F4211_SDDOCO F4211_SDDOCO,
  AggF4211.F4211_SDLNID F4211_SDLNID,
  AggF4211.F4211_SDHOLD F4211_SDHOLD,
  AggF4211.F4211_SDDCTO F4211_SDDCTO,
  AggF4211.F4211_SDTRDJ F4211_SDTRDJ,
  AggF4211.F4211_SDDRQJ F4211_SDDRQJ,
  AggF4211.F4211_SDADDJ F4211_SDADDJ,
  AggF4211.F4211_SDLITM F4211_SDLITM,
  AggF4211.F4211_SDMOT F4211_SDMOT,
  AggF4211.F4211_SDFRTH F4211_SDFRTH,
  AggF4211.F4211_SDUOM F4211_SDUOM,
  AggF4211.F4211_SDCNID F4211_SDCNID,
  AggF4211.F4211_SDSHAN F4211_SDSHAN,
  AggF4211.F4211_SDPA8 F4211_SDPA8,
  AggF4211.F4211_SDURAB F4211_SDURAB,
  AggF4211.F4211_SDVR01 F4211_SDVR01,
  AggF4211.F4211_SDUPRC F4211_SDUPRC,
  AggF4211.F4211_SDLTTR F4211_SDLTTR,
  AggF4211.F4211_SDNXTR F4211_SDNXTR,
  AggF4211.F4211_SDSRP1 F4211_SDSRP1,
  AggF4211.F4211_SDTORG F4211_SDTORG,
  AggF4211.F4201_SHDEL1 F4201_SHDEL1,
  AggF4211.F4201_SHDEL2 F4201_SHDEL2,
  AggF4211.F4211_SDODCT F4211_SDODCT,
  AggF4211.F4211_SDOORN F4211_SDOORN,
  AggF4211.F4211_SDODOC F4211_SDODOC,
  AggF4211.F4211_SDCARS F4211_SDCARS,
  AggF4211.C_F4211_SDMCU C_F4211_SDMCU,
  AggF4211.F4211_SDLNTY F4211_SDLNTY,
  AggF4211.F4211_SDCNDJ F4211_SDCNDJ,
  AggF4211.F4211_SDAN8 F4211_SDAN8,
  AggF4981.F4981_FHCTY1 F4981_FHCTY1,
  AggF4981.F4981_FHADDS F4981_FHADDS,
  AggF4981.F4981_FHADDZ F4981_FHADDZ,
  AggF4211.F4211_SDSHPN F4211_SDSHPN,
  AggF4211.F4211_SDDOC F4211_SDDOC,
  AggF4211.F4211_SDIVD F4211_SDIVD,
  AggF4211.F4211_SDURRF F4211_SDURRF,
  AggF4211.F4101_IMUWUM F4101_IMUWUM,
  AggF4211.F4211_SDSRP2 F4211_SDSRP2,
  AggF4211.F4211_SDSRP3 F4211_SDSRP3,
  AggF4211.F4211_SDSRP4 F4211_SDSRP4,
  AggF4211.F4211_SDGLC F4211_SDGLC,
  AggF4211.F41002_UMCONV F41002_UMCONV,
  AggF4211.ReportColumn8 ReportColumn8,
  AggF4211.ReportColumn9 ReportColumn9,
  AggF4211.ReportColumn13 ReportColumn13,
  AggF4074.ReportColumn14 ReportColumn14,
  AggF4981.ReportColumn15 ReportColumn15,
  AggF4211.RowIDX_F4211_2_XRowID,
  AggF4211.RowIDX_F4211_1_XRowID,
  AggF4211.RowIDX_F4211_0_XRowID,
  AggF4981.RowIDX_F4981_2_XRowID,
  AggF4981.RowIDX_F4981_1_XRowID,
  AggF4981.RowIDX_F4981_0_XRowID,
  AggF4074.RowIDX_F4074_2_XRowID,
  AggF4074.RowIDX_F4074_1_XRowID,
  AggF4074.RowIDX_F4074_0_XRowID
FROM
  (
    SELECT
      RawF4211.XF4201_SHHOLD F4201_SHHOLD,
      RawF4211.XF0101_ABSIC F0101_ABSIC,
      RawF4211.XF4211_SDMCU F4211_SDMCU,
      RawF4211.XF0101_ABALPH F0101_ABALPH,
      RawF4211.XF0116_ALCTY1 F0116_ALCTY1,
      RawF4211.XF0116_ALADDS F0116_ALADDS,
      RawF4211.XF4211_SDDOCO F4211_SDDOCO,
      RawF4211.XF4211_SDLNID F4211_SDLNID,
      RawF4211.XF4211_SDHOLD F4211_SDHOLD,
      RawF4211.XF4211_SDDCTO F4211_SDDCTO,
      RawF4211.XF4211_SDTRDJ F4211_SDTRDJ,
      RawF4211.XF4211_SDDRQJ F4211_SDDRQJ,
      RawF4211.XF4211_SDADDJ F4211_SDADDJ,
      RawF4211.XF4211_SDLITM F4211_SDLITM,
      RawF4211.XF4211_SDMOT F4211_SDMOT,
      RawF4211.XF4211_SDFRTH F4211_SDFRTH,
      RawF4211.XF4211_SDUOM F4211_SDUOM,
      RawF4211.XF4211_SDCNID F4211_SDCNID,
      RawF4211.XF4211_SDSHAN F4211_SDSHAN,
      RawF4211.XF4211_SDPA8 F4211_SDPA8,
      RawF4211.XF4211_SDURAB F4211_SDURAB,
      RawF4211.XF4211_SDVR01 F4211_SDVR01,
      RawF4211.XF4211_SDUPRC F4211_SDUPRC,
      RawF4211.XF4211_SDLTTR F4211_SDLTTR,
      RawF4211.XF4211_SDNXTR F4211_SDNXTR,
      RawF4211.XF4211_SDSRP1 F4211_SDSRP1,
      RawF4211.XF4211_SDTORG F4211_SDTORG,
      RawF4211.XF4201_SHDEL1 F4201_SHDEL1,
      RawF4211.XF4201_SHDEL2 F4201_SHDEL2,
      RawF4211.XF4211_SDODCT F4211_SDODCT,
      RawF4211.XF4211_SDOORN F4211_SDOORN,
      RawF4211.XF4211_SDODOC F4211_SDODOC,
      RawF4211.XF4211_SDCARS F4211_SDCARS,
      RawF4211.XC_F4211_SDMCU C_F4211_SDMCU,
      RawF4211.XF4211_SDLNTY F4211_SDLNTY,
      RawF4211.XF4211_SDCNDJ F4211_SDCNDJ,
      RawF4211.XF4211_SDAN8 F4211_SDAN8,
      RawF4211.XF4981_FHCTY1 F4981_FHCTY1,
      RawF4211.XF4981_FHADDS F4981_FHADDS,
      RawF4211.XF4981_FHADDZ F4981_FHADDZ,
      RawF4211.XF4211_SDSHPN F4211_SDSHPN,
      RawF4211.XF4211_SDDOC F4211_SDDOC,
      RawF4211.XF4211_SDIVD F4211_SDIVD,
      RawF4211.XF4211_SDURRF F4211_SDURRF,
      RawF4211.XF4101_IMUWUM F4101_IMUWUM,
      RawF4211.XF4211_SDSRP2 F4211_SDSRP2,
      RawF4211.XF4211_SDSRP3 F4211_SDSRP3,
      RawF4211.XF4211_SDSRP4 F4211_SDSRP4,
      RawF4211.XF4211_SDGLC F4211_SDGLC,
      RawF4211.XF41002_UMCONV F41002_UMCONV,
      SUM(RawF4211.ZReportColumn80001) ReportColumn8,
      SUM(RawF4211.ZReportColumn90001) ReportColumn9,
      SUM(RawF4211.ZReportColumn130001) ReportColumn13,
      SUM(RawF4211.XRowIDX_F4211_0_XRowID) RowIDX_F4211_0_XRowID,
      SUM(RawF4211.XRowIDX_F4211_1_XRowID) RowIDX_F4211_1_XRowID,
      SUM(RawF4211.XRowIDX_F4211_2_XRowID) RowIDX_F4211_2_XRowID
    FROM
      (
        SELECT DISTINCT
          F4201.SHHOLD XF4201_SHHOLD,
          F0101.ABSIC XF0101_ABSIC,
          F4211.SDMCU XF4211_SDMCU,
          F0101.ABALPH XF0101_ABALPH,
          F0116.ALCTY1 XF0116_ALCTY1,
          F0116.ALADDS XF0116_ALADDS,
          F4211.SDDOCO XF4211_SDDOCO,
          CAST(F4211.SDLNID AS FLOAT) / 1000 XF4211_SDLNID,
          F4211.SDHOLD XF4211_SDHOLD,
          F4211.SDDCTO XF4211_SDDCTO,
          F4211.SDTRDJ XF4211_SDTRDJ,
          F4211.SDDRQJ XF4211_SDDRQJ,
          F4211.SDADDJ XF4211_SDADDJ,
          F4211.SDLITM XF4211_SDLITM,
          F4211.SDMOT XF4211_SDMOT,
          F4211.SDFRTH XF4211_SDFRTH,
          F4211.SDUOM XF4211_SDUOM,
          F4211.SDCNID XF4211_SDCNID,
          F4211.SDSHAN XF4211_SDSHAN,
          F4211.SDPA8 XF4211_SDPA8,
          F4211.SDURAB XF4211_SDURAB,
          F4211.SDVR01 XF4211_SDVR01,
          CAST(F4211.SDUPRC AS FLOAT) / 1000000 XF4211_SDUPRC,
          F4211.SDLTTR XF4211_SDLTTR,
          F4211.SDNXTR XF4211_SDNXTR,
          F4211.SDSRP1 XF4211_SDSRP1,
          F4211.SDTORG XF4211_SDTORG,
          F4201.SHDEL1 XF4201_SHDEL1,
          F4201.SHDEL2 XF4201_SHDEL2,
          F4211.SDODCT XF4211_SDODCT,
          F4211.SDOORN XF4211_SDOORN,
          F4211.SDODOC XF4211_SDODOC,
          F4211.SDCARS XF4211_SDCARS,
          F4211.SDMCU XC_F4211_SDMCU,
          F4211.SDLNTY XF4211_SDLNTY,
          F4211.SDCNDJ XF4211_SDCNDJ,
          F4211.SDAN8 XF4211_SDAN8,
          F4981.FHCTY1 XF4981_FHCTY1,
          F4981.FHADDS XF4981_FHADDS,
          F4981.FHADDZ XF4981_FHADDZ,
          F4211.SDSHPN XF4211_SDSHPN,
          F4211.SDDOC XF4211_SDDOC,
          F4211.SDIVD XF4211_SDIVD,
          F4211.SDURRF XF4211_SDURRF,
          F4101.IMUWUM XF4101_IMUWUM,
          F4211.SDSRP2 XF4211_SDSRP2,
          F4211.SDSRP3 XF4211_SDSRP3,
          F4211.SDSRP4 XF4211_SDSRP4,
          F4211.SDGLC XF4211_SDGLC,
          CAST(F41002.UMCONV AS FLOAT) / 10000000 XF41002_UMCONV,
          F4211.SDSOQS ZReportColumn80001,
          F4211.SDPQOR ZReportColumn90001,
          F4211.SDUORG ZReportColumn130001,
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
          INNER JOIN (
            SELECT
              F0101.ABSIC,
              F0101.ABALPH,
              F0101.ABAN8
            FROM
              PRODDTA.F0101 F0101
            WHERE
              (
                (F0101.ABAT1 BETWEEN N'A  ' AND N'P  ')
                OR (F0101.ABAT1 BETWEEN N'R  ' AND N'ZZZ')
              )
          ) F0101
          INNER JOIN PRODDTA.F0116 F0116 ON F0101.ABAN8 = F0116.ALAN8 ON F0101.ABAN8 = F4211.SDSHAN
          INNER JOIN PRODDTA.F4201 F4201 ON (
            (F4201.SHKCOO = F4211.SDKCOO)
            AND (F4201.SHDOCO = F4211.SDDOCO)
          )
          AND (F4201.SHDCTO = F4211.SDDCTO)
          LEFT JOIN PRODDTA.F4981 F4981 ON F4211.SDSHPN = F4981.FHSHPN
          LEFT JOIN PRODDTA.F4101 F4101 ON F4211.SDITM = F4101.IMITM
          LEFT JOIN PRODDTA.F41002 F41002 ON (
            (F4211.SDITM = F41002.UMITM)
            AND (F41002.UMRUM = N'TN')
          )
          AND (F4211.SDUOM = F41002.UMUM)
          LEFT JOIN PRODDTA.F4074 F4074 ON (
            (
              (
                (
                  (F4211.SDDOCO = F4074.ALDOCO)
                  AND (F4211.SDDCTO = F4074.ALDCTO)
                )
                AND (F4211.SDKCOO = F4074.ALKCOO)
              )
              AND (F4211.SDLNID = F4074.ALLNID)
            )
          )
          AND (
            (
              (F4074.ALAST IS NULL)
              OR (
                F4074.ALAST IN (
                  N'A03     ',
                  N'FRTHIDE ',
                  N'FRTTAXN ',
                  N'FRTTAXY '
                )
              )
            )
          )
          LEFT JOIN dwtemp239455FC2696701_jds CO_F4981_FHKCO ON F4981.FHKCO = CO_F4981_FHKCO.CO
        WHERE
          (
            (
              (
                (
                  (
                    (F4074.ALAST IS NULL)
                    OR (
                      F4074.ALAST IN (
                        N'A03     ',
                        N'FRTHIDE ',
                        N'FRTTAXN ',
                        N'FRTTAXY '
                      )
                    )
                  )
                )
                AND ((F4211.SDADDJ BETWEEN 124001 AND 124366))
              )
            )
            AND (
              (
                F0101.ABSIC IN (
                  N'A         ',
                  N'AB        ',
                  N'AC        ',
                  N'AD        ',
                  N'AE        ',
                  N'AF        ',
                  N'AG        ',
                  N'B         ',
                  N'BA        ',
                  N'BB        ',
                  N'BD        ',
                  N'C         ',
                  N'CF        ',
                  N'CG        ',
                  N'CH        ',
                  N'CI        ',
                  N'CJ        ',
                  N'CK        ',
                  N'CL        ',
                  N'CM        ',
                  N'CO        ',
                  N'CQ        ',
                  N'D         ',
                  N'DA        ',
                  N'DB        ',
                  N'DC        ',
                  N'DD        ',
                  N'DH        ',
                  N'DK        ',
                  N'DL        ',
                  N'DM        ',
                  N'DO        ',
                  N'DP        ',
                  N'DQ        ',
                  N'DR        ',
                  N'DS        ',
                  N'DT        ',
                  N'DU        ',
                  N'EPAF      ',
                  N'EPAU      ',
                  N'EPBA      ',
                  N'EPBD      ',
                  N'EPBM      ',
                  N'EPBR      ',
                  N'EPCA      ',
                  N'EPCC      ',
                  N'EPCE      ',
                  N'EPCH      ',
                  N'EPCN      ',
                  N'EPCO      ',
                  N'EPCP      ',
                  N'EPCR      ',
                  N'EPCW      ',
                  N'EPCY      ',
                  N'EPDE      ',
                  N'EPDI      ',
                  N'EPDN      ',
                  N'EPDR      ',
                  N'EPDS      ',
                  N'EPDT      ',
                  N'EPED      ',
                  N'EPEN      ',
                  N'EPFC      ',
                  N'EPFM      ',
                  N'EPFO      ',
                  N'EPFR      ',
                  N'EPGS      ',
                  N'EPHI      ',
                  N'EPIC      ',
                  N'EPIN      ',
                  N'EPIO      ',
                  N'EPIS      ',
                  N'EPJA      ',
                  N'EPJU      ',
                  N'EPKT      ',
                  N'EPM       ',
                  N'EPMI      ',
                  N'EPMU      ',
                  N'EPOA      ',
                  N'EPOT      ',
                  N'EPPA      ',
                  N'EPPE      ',
                  N'EPPH      ',
                  N'EPPL      ',
                  N'EPPO      ',
                  N'EPRE      ',
                  N'EPRI      ',
                  N'EPSA      ',
                  N'EPSD      ',
                  N'EPSE      ',
                  N'EPSL      ',
                  N'EPSP      ',
                  N'EPST      ',
                  N'EPTR      ',
                  N'EPWA      ',
                  N'EPWI      ',
                  N'EPWW      ',
                  N'FB        ',
                  N'G         ',
                  N'GA        ',
                  N'GC        ',
                  N'GE        ',
                  N'GG        ',
                  N'GH        ',
                  N'I         ',
                  N'INT       ',
                  N'M         ',
                  N'NEW       ',
                  N'P         ',
                  N'PA        ',
                  N'PB        ',
                  N'PC        ',
                  N'PE        ',
                  N'PF        ',
                  N'PR        ',
                  N'PS        ',
                  N'PW        ',
                  N'Q         ',
                  N'QGA       ',
                  N'QGB       ',
                  N'QGC       ',
                  N'QGD       ',
                  N'R         ',
                  N'RA        ',
                  N'RB        ',
                  N'RC        ',
                  N'RE        ',
                  N'RO        ',
                  N'Z         ',
                  N'ZA        ',
                  N'ZB        ',
                  N'ZC        ',
                  N'ZD        ',
                  N'ZE        ',
                  N'ZH        ',
                  N'ZJ        ',
                  N'ZP        ',
                  N'ZV        ',
                  N'ZZ        '
                )
              )
            )
          )
      ) RawF4211
    GROUP BY
      RawF4211.XF4201_SHHOLD,
      RawF4211.XF0101_ABSIC,
      RawF4211.XF4211_SDMCU,
      RawF4211.XF0101_ABALPH,
      RawF4211.XF0116_ALCTY1,
      RawF4211.XF0116_ALADDS,
      RawF4211.XF4211_SDDOCO,
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
      RawF4211.XF4211_SDSHAN,
      RawF4211.XF4211_SDPA8,
      RawF4211.XF4211_SDURAB,
      RawF4211.XF4211_SDVR01,
      RawF4211.XF4211_SDUPRC,
      RawF4211.XF4211_SDLTTR,
      RawF4211.XF4211_SDNXTR,
      RawF4211.XF4211_SDSRP1,
      RawF4211.XF4211_SDTORG,
      RawF4211.XF4201_SHDEL1,
      RawF4211.XF4201_SHDEL2,
      RawF4211.XF4211_SDODCT,
      RawF4211.XF4211_SDOORN,
      RawF4211.XF4211_SDODOC,
      RawF4211.XF4211_SDCARS,
      RawF4211.XC_F4211_SDMCU,
      RawF4211.XF4211_SDLNTY,
      RawF4211.XF4211_SDCNDJ,
      RawF4211.XF4211_SDAN8,
      RawF4211.XF4981_FHCTY1,
      RawF4211.XF4981_FHADDS,
      RawF4211.XF4981_FHADDZ,
      RawF4211.XF4211_SDSHPN,
      RawF4211.XF4211_SDDOC,
      RawF4211.XF4211_SDIVD,
      RawF4211.XF4211_SDURRF,
      RawF4211.XF4101_IMUWUM,
      RawF4211.XF4211_SDSRP2,
      RawF4211.XF4211_SDSRP3,
      RawF4211.XF4211_SDSRP4,
      RawF4211.XF4211_SDGLC,
      RawF4211.XF41002_UMCONV
  ) AggF4211
  INNER JOIN (
    SELECT
      RawF4981.XF4201_SHHOLD F4201_SHHOLD,
      RawF4981.XF0101_ABSIC F0101_ABSIC,
      RawF4981.XF4211_SDMCU F4211_SDMCU,
      RawF4981.XF0101_ABALPH F0101_ABALPH,
      RawF4981.XF0116_ALCTY1 F0116_ALCTY1,
      RawF4981.XF0116_ALADDS F0116_ALADDS,
      RawF4981.XF4211_SDDOCO F4211_SDDOCO,
      RawF4981.XF4211_SDLNID F4211_SDLNID,
      RawF4981.XF4211_SDHOLD F4211_SDHOLD,
      RawF4981.XF4211_SDDCTO F4211_SDDCTO,
      RawF4981.XF4211_SDTRDJ F4211_SDTRDJ,
      RawF4981.XF4211_SDDRQJ F4211_SDDRQJ,
      RawF4981.XF4211_SDADDJ F4211_SDADDJ,
      RawF4981.XF4211_SDLITM F4211_SDLITM,
      RawF4981.XF4211_SDMOT F4211_SDMOT,
      RawF4981.XF4211_SDFRTH F4211_SDFRTH,
      RawF4981.XF4211_SDUOM F4211_SDUOM,
      RawF4981.XF4211_SDCNID F4211_SDCNID,
      RawF4981.XF4211_SDSHAN F4211_SDSHAN,
      RawF4981.XF4211_SDPA8 F4211_SDPA8,
      RawF4981.XF4211_SDURAB F4211_SDURAB,
      RawF4981.XF4211_SDVR01 F4211_SDVR01,
      RawF4981.XF4211_SDUPRC F4211_SDUPRC,
      RawF4981.XF4211_SDLTTR F4211_SDLTTR,
      RawF4981.XF4211_SDNXTR F4211_SDNXTR,
      RawF4981.XF4211_SDSRP1 F4211_SDSRP1,
      RawF4981.XF4211_SDTORG F4211_SDTORG,
      RawF4981.XF4201_SHDEL1 F4201_SHDEL1,
      RawF4981.XF4201_SHDEL2 F4201_SHDEL2,
      RawF4981.XF4211_SDODCT F4211_SDODCT,
      RawF4981.XF4211_SDOORN F4211_SDOORN,
      RawF4981.XF4211_SDODOC F4211_SDODOC,
      RawF4981.XF4211_SDCARS F4211_SDCARS,
      RawF4981.XC_F4211_SDMCU C_F4211_SDMCU,
      RawF4981.XF4211_SDLNTY F4211_SDLNTY,
      RawF4981.XF4211_SDCNDJ F4211_SDCNDJ,
      RawF4981.XF4211_SDAN8 F4211_SDAN8,
      RawF4981.XF4981_FHCTY1 F4981_FHCTY1,
      RawF4981.XF4981_FHADDS F4981_FHADDS,
      RawF4981.XF4981_FHADDZ F4981_FHADDZ,
      RawF4981.XF4211_SDSHPN F4211_SDSHPN,
      RawF4981.XF4211_SDDOC F4211_SDDOC,
      RawF4981.XF4211_SDIVD F4211_SDIVD,
      RawF4981.XF4211_SDURRF F4211_SDURRF,
      RawF4981.XF4101_IMUWUM F4101_IMUWUM,
      RawF4981.XF4211_SDSRP2 F4211_SDSRP2,
      RawF4981.XF4211_SDSRP3 F4211_SDSRP3,
      RawF4981.XF4211_SDSRP4 F4211_SDSRP4,
      RawF4981.XF4211_SDGLC F4211_SDGLC,
      RawF4981.XF41002_UMCONV F41002_UMCONV,
      SUM(RawF4981.ZReportColumn150001) ReportColumn15,
      SUM(RawF4981.XRowIDX_F4981_0_XRowID) RowIDX_F4981_0_XRowID,
      SUM(RawF4981.XRowIDX_F4981_1_XRowID) RowIDX_F4981_1_XRowID,
      SUM(RawF4981.XRowIDX_F4981_2_XRowID) RowIDX_F4981_2_XRowID
    FROM
      (
        SELECT DISTINCT
          F4201.SHHOLD XF4201_SHHOLD,
          F0101.ABSIC XF0101_ABSIC,
          F4211.SDMCU XF4211_SDMCU,
          F0101.ABALPH XF0101_ABALPH,
          F0116.ALCTY1 XF0116_ALCTY1,
          F0116.ALADDS XF0116_ALADDS,
          F4211.SDDOCO XF4211_SDDOCO,
          CAST(F4211.SDLNID AS FLOAT) / 1000 XF4211_SDLNID,
          F4211.SDHOLD XF4211_SDHOLD,
          F4211.SDDCTO XF4211_SDDCTO,
          F4211.SDTRDJ XF4211_SDTRDJ,
          F4211.SDDRQJ XF4211_SDDRQJ,
          F4211.SDADDJ XF4211_SDADDJ,
          F4211.SDLITM XF4211_SDLITM,
          F4211.SDMOT XF4211_SDMOT,
          F4211.SDFRTH XF4211_SDFRTH,
          F4211.SDUOM XF4211_SDUOM,
          F4211.SDCNID XF4211_SDCNID,
          F4211.SDSHAN XF4211_SDSHAN,
          F4211.SDPA8 XF4211_SDPA8,
          F4211.SDURAB XF4211_SDURAB,
          F4211.SDVR01 XF4211_SDVR01,
          CAST(F4211.SDUPRC AS FLOAT) / 1000000 XF4211_SDUPRC,
          F4211.SDLTTR XF4211_SDLTTR,
          F4211.SDNXTR XF4211_SDNXTR,
          F4211.SDSRP1 XF4211_SDSRP1,
          F4211.SDTORG XF4211_SDTORG,
          F4201.SHDEL1 XF4201_SHDEL1,
          F4201.SHDEL2 XF4201_SHDEL2,
          F4211.SDODCT XF4211_SDODCT,
          F4211.SDOORN XF4211_SDOORN,
          F4211.SDODOC XF4211_SDODOC,
          F4211.SDCARS XF4211_SDCARS,
          F4211.SDMCU XC_F4211_SDMCU,
          F4211.SDLNTY XF4211_SDLNTY,
          F4211.SDCNDJ XF4211_SDCNDJ,
          F4211.SDAN8 XF4211_SDAN8,
          F4981.FHCTY1 XF4981_FHCTY1,
          F4981.FHADDS XF4981_FHADDS,
          F4981.FHADDZ XF4981_FHADDZ,
          F4211.SDSHPN XF4211_SDSHPN,
          F4211.SDDOC XF4211_SDDOC,
          F4211.SDIVD XF4211_SDIVD,
          F4211.SDURRF XF4211_SDURRF,
          F4101.IMUWUM XF4101_IMUWUM,
          F4211.SDSRP2 XF4211_SDSRP2,
          F4211.SDSRP3 XF4211_SDSRP3,
          F4211.SDSRP4 XF4211_SDSRP4,
          F4211.SDGLC XF4211_SDGLC,
          CAST(F41002.UMCONV AS FLOAT) / 10000000 XF41002_UMCONV,
          F4981.FHNAMT * NVL (CO_F4981_FHKCO.ShiftFactor, 0.01) ZReportColumn150001,
          F4981.FHUK01 PK__F4981__FHUK01,
          DBMS_UTILITY.GET_HASH_VALUE (
            TO_CHAR (F4981.FHUK01) || N'_InDeX_0_SaLt',
            -1048576,
            2097152
          ) XRowIDX_F4981_0_XRowID,
          DBMS_UTILITY.GET_HASH_VALUE (
            TO_CHAR (F4981.FHUK01) || N'_InDeX_1_SaLt',
            -1048576,
            2097152
          ) XRowIDX_F4981_1_XRowID,
          DBMS_UTILITY.GET_HASH_VALUE (
            TO_CHAR (F4981.FHUK01) || N'_InDeX_2_SaLt',
            -1048576,
            2097152
          ) XRowIDX_F4981_2_XRowID
        FROM
          PRODDTA.F4211 F4211
          INNER JOIN (
            SELECT
              F0101.ABSIC,
              F0101.ABALPH,
              F0101.ABAN8
            FROM
              PRODDTA.F0101 F0101
            WHERE
              (
                (F0101.ABAT1 BETWEEN N'A  ' AND N'P  ')
                OR (F0101.ABAT1 BETWEEN N'R  ' AND N'ZZZ')
              )
          ) F0101
          INNER JOIN PRODDTA.F0116 F0116 ON F0101.ABAN8 = F0116.ALAN8 ON F0101.ABAN8 = F4211.SDSHAN
          INNER JOIN PRODDTA.F4201 F4201 ON (
            (F4201.SHKCOO = F4211.SDKCOO)
            AND (F4201.SHDOCO = F4211.SDDOCO)
          )
          AND (F4201.SHDCTO = F4211.SDDCTO)
          LEFT JOIN PRODDTA.F4981 F4981 ON F4211.SDSHPN = F4981.FHSHPN
          LEFT JOIN PRODDTA.F4101 F4101 ON F4211.SDITM = F4101.IMITM
          LEFT JOIN PRODDTA.F41002 F41002 ON (
            (F4211.SDITM = F41002.UMITM)
            AND (F41002.UMRUM = N'TN')
          )
          AND (F4211.SDUOM = F41002.UMUM)
          LEFT JOIN PRODDTA.F4074 F4074 ON (
            (
              (
                (
                  (F4211.SDDOCO = F4074.ALDOCO)
                  AND (F4211.SDDCTO = F4074.ALDCTO)
                )
                AND (F4211.SDKCOO = F4074.ALKCOO)
              )
              AND (F4211.SDLNID = F4074.ALLNID)
            )
          )
          AND (
            (
              (F4074.ALAST IS NULL)
              OR (
                F4074.ALAST IN (
                  N'A03     ',
                  N'FRTHIDE ',
                  N'FRTTAXN ',
                  N'FRTTAXY '
                )
              )
            )
          )
          LEFT JOIN dwtemp239455FC2696701_jds CO_F4981_FHKCO ON F4981.FHKCO = CO_F4981_FHKCO.CO
        WHERE
          (
            (
              (
                (
                  (
                    (F4074.ALAST IS NULL)
                    OR (
                      F4074.ALAST IN (
                        N'A03     ',
                        N'FRTHIDE ',
                        N'FRTTAXN ',
                        N'FRTTAXY '
                      )
                    )
                  )
                )
                AND ((F4211.SDADDJ BETWEEN 124001 AND 124366))
              )
            )
            AND (
              (
                F0101.ABSIC IN (
                  N'A         ',
                  N'AB        ',
                  N'AC        ',
                  N'AD        ',
                  N'AE        ',
                  N'AF        ',
                  N'AG        ',
                  N'B         ',
                  N'BA        ',
                  N'BB        ',
                  N'BD        ',
                  N'C         ',
                  N'CF        ',
                  N'CG        ',
                  N'CH        ',
                  N'CI        ',
                  N'CJ        ',
                  N'CK        ',
                  N'CL        ',
                  N'CM        ',
                  N'CO        ',
                  N'CQ        ',
                  N'D         ',
                  N'DA        ',
                  N'DB        ',
                  N'DC        ',
                  N'DD        ',
                  N'DH        ',
                  N'DK        ',
                  N'DL        ',
                  N'DM        ',
                  N'DO        ',
                  N'DP        ',
                  N'DQ        ',
                  N'DR        ',
                  N'DS        ',
                  N'DT        ',
                  N'DU        ',
                  N'EPAF      ',
                  N'EPAU      ',
                  N'EPBA      ',
                  N'EPBD      ',
                  N'EPBM      ',
                  N'EPBR      ',
                  N'EPCA      ',
                  N'EPCC      ',
                  N'EPCE      ',
                  N'EPCH      ',
                  N'EPCN      ',
                  N'EPCO      ',
                  N'EPCP      ',
                  N'EPCR      ',
                  N'EPCW      ',
                  N'EPCY      ',
                  N'EPDE      ',
                  N'EPDI      ',
                  N'EPDN      ',
                  N'EPDR      ',
                  N'EPDS      ',
                  N'EPDT      ',
                  N'EPED      ',
                  N'EPEN      ',
                  N'EPFC      ',
                  N'EPFM      ',
                  N'EPFO      ',
                  N'EPFR      ',
                  N'EPGS      ',
                  N'EPHI      ',
                  N'EPIC      ',
                  N'EPIN      ',
                  N'EPIO      ',
                  N'EPIS      ',
                  N'EPJA      ',
                  N'EPJU      ',
                  N'EPKT      ',
                  N'EPM       ',
                  N'EPMI      ',
                  N'EPMU      ',
                  N'EPOA      ',
                  N'EPOT      ',
                  N'EPPA      ',
                  N'EPPE      ',
                  N'EPPH      ',
                  N'EPPL      ',
                  N'EPPO      ',
                  N'EPRE      ',
                  N'EPRI      ',
                  N'EPSA      ',
                  N'EPSD      ',
                  N'EPSE      ',
                  N'EPSL      ',
                  N'EPSP      ',
                  N'EPST      ',
                  N'EPTR      ',
                  N'EPWA      ',
                  N'EPWI      ',
                  N'EPWW      ',
                  N'FB        ',
                  N'G         ',
                  N'GA        ',
                  N'GC        ',
                  N'GE        ',
                  N'GG        ',
                  N'GH        ',
                  N'I         ',
                  N'INT       ',
                  N'M         ',
                  N'NEW       ',
                  N'P         ',
                  N'PA        ',
                  N'PB        ',
                  N'PC        ',
                  N'PE        ',
                  N'PF        ',
                  N'PR        ',
                  N'PS        ',
                  N'PW        ',
                  N'Q         ',
                  N'QGA       ',
                  N'QGB       ',
                  N'QGC       ',
                  N'QGD       ',
                  N'R         ',
                  N'RA        ',
                  N'RB        ',
                  N'RC        ',
                  N'RE        ',
                  N'RO        ',
                  N'Z         ',
                  N'ZA        ',
                  N'ZB        ',
                  N'ZC        ',
                  N'ZD        ',
                  N'ZE        ',
                  N'ZH        ',
                  N'ZJ        ',
                  N'ZP        ',
                  N'ZV        ',
                  N'ZZ        '
                )
              )
            )
          )
      ) RawF4981
    GROUP BY
      RawF4981.XF4201_SHHOLD,
      RawF4981.XF0101_ABSIC,
      RawF4981.XF4211_SDMCU,
      RawF4981.XF0101_ABALPH,
      RawF4981.XF0116_ALCTY1,
      RawF4981.XF0116_ALADDS,
      RawF4981.XF4211_SDDOCO,
      RawF4981.XF4211_SDLNID,
      RawF4981.XF4211_SDHOLD,
      RawF4981.XF4211_SDDCTO,
      RawF4981.XF4211_SDTRDJ,
      RawF4981.XF4211_SDDRQJ,
      RawF4981.XF4211_SDADDJ,
      RawF4981.XF4211_SDLITM,
      RawF4981.XF4211_SDMOT,
      RawF4981.XF4211_SDFRTH,
      RawF4981.XF4211_SDUOM,
      RawF4981.XF4211_SDCNID,
      RawF4981.XF4211_SDSHAN,
      RawF4981.XF4211_SDPA8,
      RawF4981.XF4211_SDURAB,
      RawF4981.XF4211_SDVR01,
      RawF4981.XF4211_SDUPRC,
      RawF4981.XF4211_SDLTTR,
      RawF4981.XF4211_SDNXTR,
      RawF4981.XF4211_SDSRP1,
      RawF4981.XF4211_SDTORG,
      RawF4981.XF4201_SHDEL1,
      RawF4981.XF4201_SHDEL2,
      RawF4981.XF4211_SDODCT,
      RawF4981.XF4211_SDOORN,
      RawF4981.XF4211_SDODOC,
      RawF4981.XF4211_SDCARS,
      RawF4981.XC_F4211_SDMCU,
      RawF4981.XF4211_SDLNTY,
      RawF4981.XF4211_SDCNDJ,
      RawF4981.XF4211_SDAN8,
      RawF4981.XF4981_FHCTY1,
      RawF4981.XF4981_FHADDS,
      RawF4981.XF4981_FHADDZ,
      RawF4981.XF4211_SDSHPN,
      RawF4981.XF4211_SDDOC,
      RawF4981.XF4211_SDIVD,
      RawF4981.XF4211_SDURRF,
      RawF4981.XF4101_IMUWUM,
      RawF4981.XF4211_SDSRP2,
      RawF4981.XF4211_SDSRP3,
      RawF4981.XF4211_SDSRP4,
      RawF4981.XF4211_SDGLC,
      RawF4981.XF41002_UMCONV
  ) AggF4981 ON (
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
                                                                                        (
                                                                                          (
                                                                                            (
                                                                                              (
                                                                                                (
                                                                                                  (
                                                                                                    NVL (AggF4211.F4201_SHHOLD, N'---COM.D11S---Null--') = NVL (AggF4981.F4201_SHHOLD, N'---COM.D11S---Null--')
                                                                                                  )
                                                                                                  AND (
                                                                                                    NVL (AggF4211.F0101_ABSIC, N'---COM.D11S---Null--') = NVL (AggF4981.F0101_ABSIC, N'---COM.D11S---Null--')
                                                                                                  )
                                                                                                )
                                                                                                AND (
                                                                                                  NVL (AggF4211.F4211_SDMCU, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDMCU, N'---COM.D11S---Null--')
                                                                                                )
                                                                                              )
                                                                                              AND (
                                                                                                NVL (AggF4211.F0101_ABALPH, N'---COM.D11S---Null--') = NVL (AggF4981.F0101_ABALPH, N'---COM.D11S---Null--')
                                                                                              )
                                                                                            )
                                                                                            AND (
                                                                                              NVL (AggF4211.F0116_ALCTY1, N'---COM.D11S---Null--') = NVL (AggF4981.F0116_ALCTY1, N'---COM.D11S---Null--')
                                                                                            )
                                                                                          )
                                                                                          AND (
                                                                                            NVL (AggF4211.F0116_ALADDS, N'---COM.D11S---Null--') = NVL (AggF4981.F0116_ALADDS, N'---COM.D11S---Null--')
                                                                                          )
                                                                                        )
                                                                                        AND (
                                                                                          NVL (AggF4211.F4211_SDDOCO, 1E-05) = NVL (AggF4981.F4211_SDDOCO, 1E-05)
                                                                                        )
                                                                                      )
                                                                                      AND (
                                                                                        NVL (AggF4211.F4211_SDLNID, 1E-05) = NVL (AggF4981.F4211_SDLNID, 1E-05)
                                                                                      )
                                                                                    )
                                                                                    AND (
                                                                                      NVL (AggF4211.F4211_SDHOLD, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDHOLD, N'---COM.D11S---Null--')
                                                                                    )
                                                                                  )
                                                                                  AND (
                                                                                    NVL (AggF4211.F4211_SDDCTO, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDDCTO, N'---COM.D11S---Null--')
                                                                                  )
                                                                                )
                                                                                AND (
                                                                                  NVL (AggF4211.F4211_SDTRDJ, 1E-05) = NVL (AggF4981.F4211_SDTRDJ, 1E-05)
                                                                                )
                                                                              )
                                                                              AND (
                                                                                NVL (AggF4211.F4211_SDDRQJ, 1E-05) = NVL (AggF4981.F4211_SDDRQJ, 1E-05)
                                                                              )
                                                                            )
                                                                            AND (
                                                                              NVL (AggF4211.F4211_SDADDJ, 1E-05) = NVL (AggF4981.F4211_SDADDJ, 1E-05)
                                                                            )
                                                                          )
                                                                          AND (
                                                                            NVL (AggF4211.F4211_SDLITM, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDLITM, N'---COM.D11S---Null--')
                                                                          )
                                                                        )
                                                                        AND (
                                                                          NVL (AggF4211.F4211_SDMOT, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDMOT, N'---COM.D11S---Null--')
                                                                        )
                                                                      )
                                                                      AND (
                                                                        NVL (AggF4211.F4211_SDFRTH, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDFRTH, N'---COM.D11S---Null--')
                                                                      )
                                                                    )
                                                                    AND (
                                                                      NVL (AggF4211.F4211_SDUOM, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDUOM, N'---COM.D11S---Null--')
                                                                    )
                                                                  )
                                                                  AND (
                                                                    NVL (AggF4211.F4211_SDCNID, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDCNID, N'---COM.D11S---Null--')
                                                                  )
                                                                )
                                                                AND (
                                                                  NVL (AggF4211.F4211_SDSHAN, 1E-05) = NVL (AggF4981.F4211_SDSHAN, 1E-05)
                                                                )
                                                              )
                                                              AND (
                                                                NVL (AggF4211.F4211_SDPA8, 1E-05) = NVL (AggF4981.F4211_SDPA8, 1E-05)
                                                              )
                                                            )
                                                            AND (
                                                              NVL (AggF4211.F4211_SDURAB, 1E-05) = NVL (AggF4981.F4211_SDURAB, 1E-05)
                                                            )
                                                          )
                                                          AND (
                                                            NVL (AggF4211.F4211_SDVR01, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDVR01, N'---COM.D11S---Null--')
                                                          )
                                                        )
                                                        AND (
                                                          NVL (AggF4211.F4211_SDUPRC, 1E-05) = NVL (AggF4981.F4211_SDUPRC, 1E-05)
                                                        )
                                                      )
                                                      AND (
                                                        NVL (AggF4211.F4211_SDLTTR, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDLTTR, N'---COM.D11S---Null--')
                                                      )
                                                    )
                                                    AND (
                                                      NVL (AggF4211.F4211_SDNXTR, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDNXTR, N'---COM.D11S---Null--')
                                                    )
                                                  )
                                                  AND (
                                                    NVL (AggF4211.F4211_SDSRP1, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDSRP1, N'---COM.D11S---Null--')
                                                  )
                                                )
                                                AND (
                                                  NVL (AggF4211.F4211_SDTORG, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDTORG, N'---COM.D11S---Null--')
                                                )
                                              )
                                              AND (
                                                NVL (AggF4211.F4201_SHDEL1, N'---COM.D11S---Null--') = NVL (AggF4981.F4201_SHDEL1, N'---COM.D11S---Null--')
                                              )
                                            )
                                            AND (
                                              NVL (AggF4211.F4201_SHDEL2, N'---COM.D11S---Null--') = NVL (AggF4981.F4201_SHDEL2, N'---COM.D11S---Null--')
                                            )
                                          )
                                          AND (
                                            NVL (AggF4211.F4211_SDODCT, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDODCT, N'---COM.D11S---Null--')
                                          )
                                        )
                                        AND (
                                          NVL (AggF4211.F4211_SDOORN, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDOORN, N'---COM.D11S---Null--')
                                        )
                                      )
                                      AND (
                                        NVL (AggF4211.F4211_SDODOC, 1E-05) = NVL (AggF4981.F4211_SDODOC, 1E-05)
                                      )
                                    )
                                    AND (
                                      NVL (AggF4211.F4211_SDCARS, 1E-05) = NVL (AggF4981.F4211_SDCARS, 1E-05)
                                    )
                                  )
                                  AND (
                                    NVL (
                                      TO_CHAR (AggF4211.C_F4211_SDMCU),
                                      N'---COM.D11S---Null--'
                                    ) = NVL (
                                      TO_CHAR (AggF4981.C_F4211_SDMCU),
                                      N'---COM.D11S---Null--'
                                    )
                                  )
                                )
                                AND (
                                  NVL (AggF4211.F4211_SDLNTY, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDLNTY, N'---COM.D11S---Null--')
                                )
                              )
                              AND (
                                NVL (AggF4211.F4211_SDCNDJ, 1E-05) = NVL (AggF4981.F4211_SDCNDJ, 1E-05)
                              )
                            )
                            AND (
                              NVL (AggF4211.F4211_SDAN8, 1E-05) = NVL (AggF4981.F4211_SDAN8, 1E-05)
                            )
                          )
                          AND (
                            NVL (AggF4211.F4981_FHCTY1, N'---COM.D11S---Null--') = NVL (AggF4981.F4981_FHCTY1, N'---COM.D11S---Null--')
                          )
                        )
                        AND (
                          NVL (AggF4211.F4981_FHADDS, N'---COM.D11S---Null--') = NVL (AggF4981.F4981_FHADDS, N'---COM.D11S---Null--')
                        )
                      )
                      AND (
                        NVL (AggF4211.F4981_FHADDZ, N'---COM.D11S---Null--') = NVL (AggF4981.F4981_FHADDZ, N'---COM.D11S---Null--')
                      )
                    )
                    AND (
                      NVL (AggF4211.F4211_SDSHPN, 1E-05) = NVL (AggF4981.F4211_SDSHPN, 1E-05)
                    )
                  )
                  AND (
                    NVL (AggF4211.F4211_SDDOC, 1E-05) = NVL (AggF4981.F4211_SDDOC, 1E-05)
                  )
                )
                AND (
                  NVL (AggF4211.F4211_SDIVD, 1E-05) = NVL (AggF4981.F4211_SDIVD, 1E-05)
                )
              )
              AND (
                NVL (AggF4211.F4211_SDURRF, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDURRF, N'---COM.D11S---Null--')
              )
            )
            AND (
              NVL (AggF4211.F4101_IMUWUM, N'---COM.D11S---Null--') = NVL (AggF4981.F4101_IMUWUM, N'---COM.D11S---Null--')
            )
          )
          AND (
            NVL (AggF4211.F4211_SDSRP2, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDSRP2, N'---COM.D11S---Null--')
          )
        )
        AND (
          NVL (AggF4211.F4211_SDSRP3, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDSRP3, N'---COM.D11S---Null--')
        )
      )
      AND (
        NVL (AggF4211.F4211_SDSRP4, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDSRP4, N'---COM.D11S---Null--')
      )
    )
    AND (
      NVL (AggF4211.F4211_SDGLC, N'---COM.D11S---Null--') = NVL (AggF4981.F4211_SDGLC, N'---COM.D11S---Null--')
    )
  )
  AND (
    NVL (AggF4211.F41002_UMCONV, 1E-05) = NVL (AggF4981.F41002_UMCONV, 1E-05)
  )
  INNER JOIN (
    SELECT
      RawF4074.XF4201_SHHOLD F4201_SHHOLD,
      RawF4074.XF0101_ABSIC F0101_ABSIC,
      RawF4074.XF4211_SDMCU F4211_SDMCU,
      RawF4074.XF0101_ABALPH F0101_ABALPH,
      RawF4074.XF0116_ALCTY1 F0116_ALCTY1,
      RawF4074.XF0116_ALADDS F0116_ALADDS,
      RawF4074.XF4211_SDDOCO F4211_SDDOCO,
      RawF4074.XF4211_SDLNID F4211_SDLNID,
      RawF4074.XF4211_SDHOLD F4211_SDHOLD,
      RawF4074.XF4211_SDDCTO F4211_SDDCTO,
      RawF4074.XF4211_SDTRDJ F4211_SDTRDJ,
      RawF4074.XF4211_SDDRQJ F4211_SDDRQJ,
      RawF4074.XF4211_SDADDJ F4211_SDADDJ,
      RawF4074.XF4211_SDLITM F4211_SDLITM,
      RawF4074.XF4211_SDMOT F4211_SDMOT,
      RawF4074.XF4211_SDFRTH F4211_SDFRTH,
      RawF4074.XF4211_SDUOM F4211_SDUOM,
      RawF4074.XF4211_SDCNID F4211_SDCNID,
      RawF4074.XF4211_SDSHAN F4211_SDSHAN,
      RawF4074.XF4211_SDPA8 F4211_SDPA8,
      RawF4074.XF4211_SDURAB F4211_SDURAB,
      RawF4074.XF4211_SDVR01 F4211_SDVR01,
      RawF4074.XF4211_SDUPRC F4211_SDUPRC,
      RawF4074.XF4211_SDLTTR F4211_SDLTTR,
      RawF4074.XF4211_SDNXTR F4211_SDNXTR,
      RawF4074.XF4211_SDSRP1 F4211_SDSRP1,
      RawF4074.XF4211_SDTORG F4211_SDTORG,
      RawF4074.XF4201_SHDEL1 F4201_SHDEL1,
      RawF4074.XF4201_SHDEL2 F4201_SHDEL2,
      RawF4074.XF4211_SDODCT F4211_SDODCT,
      RawF4074.XF4211_SDOORN F4211_SDOORN,
      RawF4074.XF4211_SDODOC F4211_SDODOC,
      RawF4074.XF4211_SDCARS F4211_SDCARS,
      RawF4074.XC_F4211_SDMCU C_F4211_SDMCU,
      RawF4074.XF4211_SDLNTY F4211_SDLNTY,
      RawF4074.XF4211_SDCNDJ F4211_SDCNDJ,
      RawF4074.XF4211_SDAN8 F4211_SDAN8,
      RawF4074.XF4981_FHCTY1 F4981_FHCTY1,
      RawF4074.XF4981_FHADDS F4981_FHADDS,
      RawF4074.XF4981_FHADDZ F4981_FHADDZ,
      RawF4074.XF4211_SDSHPN F4211_SDSHPN,
      RawF4074.XF4211_SDDOC F4211_SDDOC,
      RawF4074.XF4211_SDIVD F4211_SDIVD,
      RawF4074.XF4211_SDURRF F4211_SDURRF,
      RawF4074.XF4101_IMUWUM F4101_IMUWUM,
      RawF4074.XF4211_SDSRP2 F4211_SDSRP2,
      RawF4074.XF4211_SDSRP3 F4211_SDSRP3,
      RawF4074.XF4211_SDSRP4 F4211_SDSRP4,
      RawF4074.XF4211_SDGLC F4211_SDGLC,
      RawF4074.XF41002_UMCONV F41002_UMCONV,
      SUM(RawF4074.ZReportColumn140001) ReportColumn14,
      SUM(RawF4074.XRowIDX_F4074_0_XRowID) RowIDX_F4074_0_XRowID,
      SUM(RawF4074.XRowIDX_F4074_1_XRowID) RowIDX_F4074_1_XRowID,
      SUM(RawF4074.XRowIDX_F4074_2_XRowID) RowIDX_F4074_2_XRowID
    FROM
      (
        SELECT DISTINCT
          F4201.SHHOLD XF4201_SHHOLD,
          F0101.ABSIC XF0101_ABSIC,
          F4211.SDMCU XF4211_SDMCU,
          F0101.ABALPH XF0101_ABALPH,
          F0116.ALCTY1 XF0116_ALCTY1,
          F0116.ALADDS XF0116_ALADDS,
          F4211.SDDOCO XF4211_SDDOCO,
          CAST(F4211.SDLNID AS FLOAT) / 1000 XF4211_SDLNID,
          F4211.SDHOLD XF4211_SDHOLD,
          F4211.SDDCTO XF4211_SDDCTO,
          F4211.SDTRDJ XF4211_SDTRDJ,
          F4211.SDDRQJ XF4211_SDDRQJ,
          F4211.SDADDJ XF4211_SDADDJ,
          F4211.SDLITM XF4211_SDLITM,
          F4211.SDMOT XF4211_SDMOT,
          F4211.SDFRTH XF4211_SDFRTH,
          F4211.SDUOM XF4211_SDUOM,
          F4211.SDCNID XF4211_SDCNID,
          F4211.SDSHAN XF4211_SDSHAN,
          F4211.SDPA8 XF4211_SDPA8,
          F4211.SDURAB XF4211_SDURAB,
          F4211.SDVR01 XF4211_SDVR01,
          CAST(F4211.SDUPRC AS FLOAT) / 1000000 XF4211_SDUPRC,
          F4211.SDLTTR XF4211_SDLTTR,
          F4211.SDNXTR XF4211_SDNXTR,
          F4211.SDSRP1 XF4211_SDSRP1,
          F4211.SDTORG XF4211_SDTORG,
          F4201.SHDEL1 XF4201_SHDEL1,
          F4201.SHDEL2 XF4201_SHDEL2,
          F4211.SDODCT XF4211_SDODCT,
          F4211.SDOORN XF4211_SDOORN,
          F4211.SDODOC XF4211_SDODOC,
          F4211.SDCARS XF4211_SDCARS,
          F4211.SDMCU XC_F4211_SDMCU,
          F4211.SDLNTY XF4211_SDLNTY,
          F4211.SDCNDJ XF4211_SDCNDJ,
          F4211.SDAN8 XF4211_SDAN8,
          F4981.FHCTY1 XF4981_FHCTY1,
          F4981.FHADDS XF4981_FHADDS,
          F4981.FHADDZ XF4981_FHADDZ,
          F4211.SDSHPN XF4211_SDSHPN,
          F4211.SDDOC XF4211_SDDOC,
          F4211.SDIVD XF4211_SDIVD,
          F4211.SDURRF XF4211_SDURRF,
          F4101.IMUWUM XF4101_IMUWUM,
          F4211.SDSRP2 XF4211_SDSRP2,
          F4211.SDSRP3 XF4211_SDSRP3,
          F4211.SDSRP4 XF4211_SDSRP4,
          F4211.SDGLC XF4211_SDGLC,
          CAST(F41002.UMCONV AS FLOAT) / 10000000 XF41002_UMCONV,
          F4074.ALUPRC ZReportColumn140001,
          F4074.ALDOCO PK__F4074__ALDOCO,
          F4074.ALDCTO PK__F4074__ALDCTO,
          F4074.ALKCOO PK__F4074__ALKCOO,
          F4074.ALSFXO PK__F4074__ALSFXO,
          F4074.ALLNID PK__F4074__ALLNID,
          F4074.ALAKID PK__F4074__ALAKID,
          F4074.ALSRCFD PK__F4074__ALSRCFD,
          F4074.ALOSEQ PK__F4074__ALOSEQ,
          F4074.ALSUBSEQ PK__F4074__ALSUBSEQ,
          F4074.ALTIER PK__F4074__ALTIER,
          F4074.ALPA04 PK__F4074__ALPA04,
          DBMS_UTILITY.GET_HASH_VALUE (
            TO_CHAR (F4074.ALDOCO) || N'_ISS_' || TO_CHAR (F4074.ALDCTO) || N'_ISS_' || TO_CHAR (F4074.ALKCOO) || N'_ISS_' || TO_CHAR (F4074.ALSFXO) || N'_ISS_' || TO_CHAR (F4074.ALLNID) || N'_ISS_' || TO_CHAR (F4074.ALAKID) || N'_ISS_' || TO_CHAR (F4074.ALSRCFD) || N'_ISS_' || TO_CHAR (F4074.ALOSEQ) || N'_ISS_' || TO_CHAR (F4074.ALSUBSEQ) || N'_ISS_' || TO_CHAR (F4074.ALTIER) || N'_ISS_' || TO_CHAR (F4074.ALPA04) || N'_InDeX_0_SaLt',
            -1048576,
            2097152
          ) XRowIDX_F4074_0_XRowID,
          DBMS_UTILITY.GET_HASH_VALUE (
            TO_CHAR (F4074.ALDOCO) || N'_ISS_' || TO_CHAR (F4074.ALDCTO) || N'_ISS_' || TO_CHAR (F4074.ALKCOO) || N'_ISS_' || TO_CHAR (F4074.ALSFXO) || N'_ISS_' || TO_CHAR (F4074.ALLNID) || N'_ISS_' || TO_CHAR (F4074.ALAKID) || N'_ISS_' || TO_CHAR (F4074.ALSRCFD) || N'_ISS_' || TO_CHAR (F4074.ALOSEQ) || N'_ISS_' || TO_CHAR (F4074.ALSUBSEQ) || N'_ISS_' || TO_CHAR (F4074.ALTIER) || N'_ISS_' || TO_CHAR (F4074.ALPA04) || N'_InDeX_1_SaLt',
            -1048576,
            2097152
          ) XRowIDX_F4074_1_XRowID,
          DBMS_UTILITY.GET_HASH_VALUE (
            TO_CHAR (F4074.ALDOCO) || N'_ISS_' || TO_CHAR (F4074.ALDCTO) || N'_ISS_' || TO_CHAR (F4074.ALKCOO) || N'_ISS_' || TO_CHAR (F4074.ALSFXO) || N'_ISS_' || TO_CHAR (F4074.ALLNID) || N'_ISS_' || TO_CHAR (F4074.ALAKID) || N'_ISS_' || TO_CHAR (F4074.ALSRCFD) || N'_ISS_' || TO_CHAR (F4074.ALOSEQ) || N'_ISS_' || TO_CHAR (F4074.ALSUBSEQ) || N'_ISS_' || TO_CHAR (F4074.ALTIER) || N'_ISS_' || TO_CHAR (F4074.ALPA04) || N'_InDeX_2_SaLt',
            -1048576,
            2097152
          ) XRowIDX_F4074_2_XRowID
        FROM
          PRODDTA.F4211 F4211
          INNER JOIN (
            SELECT
              F0101.ABSIC,
              F0101.ABALPH,
              F0101.ABAN8
            FROM
              PRODDTA.F0101 F0101
            WHERE
              (
                (F0101.ABAT1 BETWEEN N'A  ' AND N'P  ')
                OR (F0101.ABAT1 BETWEEN N'R  ' AND N'ZZZ')
              )
          ) F0101
          INNER JOIN PRODDTA.F0116 F0116 ON F0101.ABAN8 = F0116.ALAN8 ON F0101.ABAN8 = F4211.SDSHAN
          INNER JOIN PRODDTA.F4201 F4201 ON (
            (F4201.SHKCOO = F4211.SDKCOO)
            AND (F4201.SHDOCO = F4211.SDDOCO)
          )
          AND (F4201.SHDCTO = F4211.SDDCTO)
          LEFT JOIN PRODDTA.F4981 F4981 ON F4211.SDSHPN = F4981.FHSHPN
          LEFT JOIN PRODDTA.F4101 F4101 ON F4211.SDITM = F4101.IMITM
          LEFT JOIN PRODDTA.F41002 F41002 ON (
            (F4211.SDITM = F41002.UMITM)
            AND (F41002.UMRUM = N'TN')
          )
          AND (F4211.SDUOM = F41002.UMUM)
          LEFT JOIN PRODDTA.F4074 F4074 ON (
            (
              (
                (
                  (F4211.SDDOCO = F4074.ALDOCO)
                  AND (F4211.SDDCTO = F4074.ALDCTO)
                )
                AND (F4211.SDKCOO = F4074.ALKCOO)
              )
              AND (F4211.SDLNID = F4074.ALLNID)
            )
          )
          AND (
            (
              (F4074.ALAST IS NULL)
              OR (
                F4074.ALAST IN (
                  N'A03     ',
                  N'FRTHIDE ',
                  N'FRTTAXN ',
                  N'FRTTAXY '
                )
              )
            )
          )
          LEFT JOIN dwtemp239455FC2696701_jds CO_F4981_FHKCO ON F4981.FHKCO = CO_F4981_FHKCO.CO
        WHERE
          (
            (
              (
                (
                  (
                    (F4074.ALAST IS NULL)
                    OR (
                      F4074.ALAST IN (
                        N'A03     ',
                        N'FRTHIDE ',
                        N'FRTTAXN ',
                        N'FRTTAXY '
                      )
                    )
                  )
                )
                AND ((F4211.SDADDJ BETWEEN 124001 AND 124366))
              )
            )
            AND (
              (
                F0101.ABSIC IN (
                  N'A         ',
                  N'AB        ',
                  N'AC        ',
                  N'AD        ',
                  N'AE        ',
                  N'AF        ',
                  N'AG        ',
                  N'B         ',
                  N'BA        ',
                  N'BB        ',
                  N'BD        ',
                  N'C         ',
                  N'CF        ',
                  N'CG        ',
                  N'CH        ',
                  N'CI        ',
                  N'CJ        ',
                  N'CK        ',
                  N'CL        ',
                  N'CM        ',
                  N'CO        ',
                  N'CQ        ',
                  N'D         ',
                  N'DA        ',
                  N'DB        ',
                  N'DC        ',
                  N'DD        ',
                  N'DH        ',
                  N'DK        ',
                  N'DL        ',
                  N'DM        ',
                  N'DO        ',
                  N'DP        ',
                  N'DQ        ',
                  N'DR        ',
                  N'DS        ',
                  N'DT        ',
                  N'DU        ',
                  N'EPAF      ',
                  N'EPAU      ',
                  N'EPBA      ',
                  N'EPBD      ',
                  N'EPBM      ',
                  N'EPBR      ',
                  N'EPCA      ',
                  N'EPCC      ',
                  N'EPCE      ',
                  N'EPCH      ',
                  N'EPCN      ',
                  N'EPCO      ',
                  N'EPCP      ',
                  N'EPCR      ',
                  N'EPCW      ',
                  N'EPCY      ',
                  N'EPDE      ',
                  N'EPDI      ',
                  N'EPDN      ',
                  N'EPDR      ',
                  N'EPDS      ',
                  N'EPDT      ',
                  N'EPED      ',
                  N'EPEN      ',
                  N'EPFC      ',
                  N'EPFM      ',
                  N'EPFO      ',
                  N'EPFR      ',
                  N'EPGS      ',
                  N'EPHI      ',
                  N'EPIC      ',
                  N'EPIN      ',
                  N'EPIO      ',
                  N'EPIS      ',
                  N'EPJA      ',
                  N'EPJU      ',
                  N'EPKT      ',
                  N'EPM       ',
                  N'EPMI      ',
                  N'EPMU      ',
                  N'EPOA      ',
                  N'EPOT      ',
                  N'EPPA      ',
                  N'EPPE      ',
                  N'EPPH      ',
                  N'EPPL      ',
                  N'EPPO      ',
                  N'EPRE      ',
                  N'EPRI      ',
                  N'EPSA      ',
                  N'EPSD      ',
                  N'EPSE      ',
                  N'EPSL      ',
                  N'EPSP      ',
                  N'EPST      ',
                  N'EPTR      ',
                  N'EPWA      ',
                  N'EPWI      ',
                  N'EPWW      ',
                  N'FB        ',
                  N'G         ',
                  N'GA        ',
                  N'GC        ',
                  N'GE        ',
                  N'GG        ',
                  N'GH        ',
                  N'I         ',
                  N'INT       ',
                  N'M         ',
                  N'NEW       ',
                  N'P         ',
                  N'PA        ',
                  N'PB        ',
                  N'PC        ',
                  N'PE        ',
                  N'PF        ',
                  N'PR        ',
                  N'PS        ',
                  N'PW        ',
                  N'Q         ',
                  N'QGA       ',
                  N'QGB       ',
                  N'QGC       ',
                  N'QGD       ',
                  N'R         ',
                  N'RA        ',
                  N'RB        ',
                  N'RC        ',
                  N'RE        ',
                  N'RO        ',
                  N'Z         ',
                  N'ZA        ',
                  N'ZB        ',
                  N'ZC        ',
                  N'ZD        ',
                  N'ZE        ',
                  N'ZH        ',
                  N'ZJ        ',
                  N'ZP        ',
                  N'ZV        ',
                  N'ZZ        '
                )
              )
            )
          )
      ) RawF4074
    GROUP BY
      RawF4074.XF4201_SHHOLD,
      RawF4074.XF0101_ABSIC,
      RawF4074.XF4211_SDMCU,
      RawF4074.XF0101_ABALPH,
      RawF4074.XF0116_ALCTY1,
      RawF4074.XF0116_ALADDS,
      RawF4074.XF4211_SDDOCO,
      RawF4074.XF4211_SDLNID,
      RawF4074.XF4211_SDHOLD,
      RawF4074.XF4211_SDDCTO,
      RawF4074.XF4211_SDTRDJ,
      RawF4074.XF4211_SDDRQJ,
      RawF4074.XF4211_SDADDJ,
      RawF4074.XF4211_SDLITM,
      RawF4074.XF4211_SDMOT,
      RawF4074.XF4211_SDFRTH,
      RawF4074.XF4211_SDUOM,
      RawF4074.XF4211_SDCNID,
      RawF4074.XF4211_SDSHAN,
      RawF4074.XF4211_SDPA8,
      RawF4074.XF4211_SDURAB,
      RawF4074.XF4211_SDVR01,
      RawF4074.XF4211_SDUPRC,
      RawF4074.XF4211_SDLTTR,
      RawF4074.XF4211_SDNXTR,
      RawF4074.XF4211_SDSRP1,
      RawF4074.XF4211_SDTORG,
      RawF4074.XF4201_SHDEL1,
      RawF4074.XF4201_SHDEL2,
      RawF4074.XF4211_SDODCT,
      RawF4074.XF4211_SDOORN,
      RawF4074.XF4211_SDODOC,
      RawF4074.XF4211_SDCARS,
      RawF4074.XC_F4211_SDMCU,
      RawF4074.XF4211_SDLNTY,
      RawF4074.XF4211_SDCNDJ,
      RawF4074.XF4211_SDAN8,
      RawF4074.XF4981_FHCTY1,
      RawF4074.XF4981_FHADDS,
      RawF4074.XF4981_FHADDZ,
      RawF4074.XF4211_SDSHPN,
      RawF4074.XF4211_SDDOC,
      RawF4074.XF4211_SDIVD,
      RawF4074.XF4211_SDURRF,
      RawF4074.XF4101_IMUWUM,
      RawF4074.XF4211_SDSRP2,
      RawF4074.XF4211_SDSRP3,
      RawF4074.XF4211_SDSRP4,
      RawF4074.XF4211_SDGLC,
      RawF4074.XF41002_UMCONV
  ) AggF4074 ON (
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
                                                                                        (
                                                                                          (
                                                                                            (
                                                                                              (
                                                                                                (
                                                                                                  (
                                                                                                    NVL (AggF4981.F4201_SHHOLD, N'---COM.D11S---Null--') = NVL (AggF4074.F4201_SHHOLD, N'---COM.D11S---Null--')
                                                                                                  )
                                                                                                  AND (
                                                                                                    NVL (AggF4981.F0101_ABSIC, N'---COM.D11S---Null--') = NVL (AggF4074.F0101_ABSIC, N'---COM.D11S---Null--')
                                                                                                  )
                                                                                                )
                                                                                                AND (
                                                                                                  NVL (AggF4981.F4211_SDMCU, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDMCU, N'---COM.D11S---Null--')
                                                                                                )
                                                                                              )
                                                                                              AND (
                                                                                                NVL (AggF4981.F0101_ABALPH, N'---COM.D11S---Null--') = NVL (AggF4074.F0101_ABALPH, N'---COM.D11S---Null--')
                                                                                              )
                                                                                            )
                                                                                            AND (
                                                                                              NVL (AggF4981.F0116_ALCTY1, N'---COM.D11S---Null--') = NVL (AggF4074.F0116_ALCTY1, N'---COM.D11S---Null--')
                                                                                            )
                                                                                          )
                                                                                          AND (
                                                                                            NVL (AggF4981.F0116_ALADDS, N'---COM.D11S---Null--') = NVL (AggF4074.F0116_ALADDS, N'---COM.D11S---Null--')
                                                                                          )
                                                                                        )
                                                                                        AND (
                                                                                          NVL (AggF4981.F4211_SDDOCO, 1E-05) = NVL (AggF4074.F4211_SDDOCO, 1E-05)
                                                                                        )
                                                                                      )
                                                                                      AND (
                                                                                        NVL (AggF4981.F4211_SDLNID, 1E-05) = NVL (AggF4074.F4211_SDLNID, 1E-05)
                                                                                      )
                                                                                    )
                                                                                    AND (
                                                                                      NVL (AggF4981.F4211_SDHOLD, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDHOLD, N'---COM.D11S---Null--')
                                                                                    )
                                                                                  )
                                                                                  AND (
                                                                                    NVL (AggF4981.F4211_SDDCTO, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDDCTO, N'---COM.D11S---Null--')
                                                                                  )
                                                                                )
                                                                                AND (
                                                                                  NVL (AggF4981.F4211_SDTRDJ, 1E-05) = NVL (AggF4074.F4211_SDTRDJ, 1E-05)
                                                                                )
                                                                              )
                                                                              AND (
                                                                                NVL (AggF4981.F4211_SDDRQJ, 1E-05) = NVL (AggF4074.F4211_SDDRQJ, 1E-05)
                                                                              )
                                                                            )
                                                                            AND (
                                                                              NVL (AggF4981.F4211_SDADDJ, 1E-05) = NVL (AggF4074.F4211_SDADDJ, 1E-05)
                                                                            )
                                                                          )
                                                                          AND (
                                                                            NVL (AggF4981.F4211_SDLITM, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDLITM, N'---COM.D11S---Null--')
                                                                          )
                                                                        )
                                                                        AND (
                                                                          NVL (AggF4981.F4211_SDMOT, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDMOT, N'---COM.D11S---Null--')
                                                                        )
                                                                      )
                                                                      AND (
                                                                        NVL (AggF4981.F4211_SDFRTH, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDFRTH, N'---COM.D11S---Null--')
                                                                      )
                                                                    )
                                                                    AND (
                                                                      NVL (AggF4981.F4211_SDUOM, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDUOM, N'---COM.D11S---Null--')
                                                                    )
                                                                  )
                                                                  AND (
                                                                    NVL (AggF4981.F4211_SDCNID, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDCNID, N'---COM.D11S---Null--')
                                                                  )
                                                                )
                                                                AND (
                                                                  NVL (AggF4981.F4211_SDSHAN, 1E-05) = NVL (AggF4074.F4211_SDSHAN, 1E-05)
                                                                )
                                                              )
                                                              AND (
                                                                NVL (AggF4981.F4211_SDPA8, 1E-05) = NVL (AggF4074.F4211_SDPA8, 1E-05)
                                                              )
                                                            )
                                                            AND (
                                                              NVL (AggF4981.F4211_SDURAB, 1E-05) = NVL (AggF4074.F4211_SDURAB, 1E-05)
                                                            )
                                                          )
                                                          AND (
                                                            NVL (AggF4981.F4211_SDVR01, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDVR01, N'---COM.D11S---Null--')
                                                          )
                                                        )
                                                        AND (
                                                          NVL (AggF4981.F4211_SDUPRC, 1E-05) = NVL (AggF4074.F4211_SDUPRC, 1E-05)
                                                        )
                                                      )
                                                      AND (
                                                        NVL (AggF4981.F4211_SDLTTR, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDLTTR, N'---COM.D11S---Null--')
                                                      )
                                                    )
                                                    AND (
                                                      NVL (AggF4981.F4211_SDNXTR, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDNXTR, N'---COM.D11S---Null--')
                                                    )
                                                  )
                                                  AND (
                                                    NVL (AggF4981.F4211_SDSRP1, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDSRP1, N'---COM.D11S---Null--')
                                                  )
                                                )
                                                AND (
                                                  NVL (AggF4981.F4211_SDTORG, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDTORG, N'---COM.D11S---Null--')
                                                )
                                              )
                                              AND (
                                                NVL (AggF4981.F4201_SHDEL1, N'---COM.D11S---Null--') = NVL (AggF4074.F4201_SHDEL1, N'---COM.D11S---Null--')
                                              )
                                            )
                                            AND (
                                              NVL (AggF4981.F4201_SHDEL2, N'---COM.D11S---Null--') = NVL (AggF4074.F4201_SHDEL2, N'---COM.D11S---Null--')
                                            )
                                          )
                                          AND (
                                            NVL (AggF4981.F4211_SDODCT, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDODCT, N'---COM.D11S---Null--')
                                          )
                                        )
                                        AND (
                                          NVL (AggF4981.F4211_SDOORN, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDOORN, N'---COM.D11S---Null--')
                                        )
                                      )
                                      AND (
                                        NVL (AggF4981.F4211_SDODOC, 1E-05) = NVL (AggF4074.F4211_SDODOC, 1E-05)
                                      )
                                    )
                                    AND (
                                      NVL (AggF4981.F4211_SDCARS, 1E-05) = NVL (AggF4074.F4211_SDCARS, 1E-05)
                                    )
                                  )
                                  AND (
                                    NVL (
                                      TO_CHAR (AggF4981.C_F4211_SDMCU),
                                      N'---COM.D11S---Null--'
                                    ) = NVL (
                                      TO_CHAR (AggF4074.C_F4211_SDMCU),
                                      N'---COM.D11S---Null--'
                                    )
                                  )
                                )
                                AND (
                                  NVL (AggF4981.F4211_SDLNTY, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDLNTY, N'---COM.D11S---Null--')
                                )
                              )
                              AND (
                                NVL (AggF4981.F4211_SDCNDJ, 1E-05) = NVL (AggF4074.F4211_SDCNDJ, 1E-05)
                              )
                            )
                            AND (
                              NVL (AggF4981.F4211_SDAN8, 1E-05) = NVL (AggF4074.F4211_SDAN8, 1E-05)
                            )
                          )
                          AND (
                            NVL (AggF4981.F4981_FHCTY1, N'---COM.D11S---Null--') = NVL (AggF4074.F4981_FHCTY1, N'---COM.D11S---Null--')
                          )
                        )
                        AND (
                          NVL (AggF4981.F4981_FHADDS, N'---COM.D11S---Null--') = NVL (AggF4074.F4981_FHADDS, N'---COM.D11S---Null--')
                        )
                      )
                      AND (
                        NVL (AggF4981.F4981_FHADDZ, N'---COM.D11S---Null--') = NVL (AggF4074.F4981_FHADDZ, N'---COM.D11S---Null--')
                      )
                    )
                    AND (
                      NVL (AggF4981.F4211_SDSHPN, 1E-05) = NVL (AggF4074.F4211_SDSHPN, 1E-05)
                    )
                  )
                  AND (
                    NVL (AggF4981.F4211_SDDOC, 1E-05) = NVL (AggF4074.F4211_SDDOC, 1E-05)
                  )
                )
                AND (
                  NVL (AggF4981.F4211_SDIVD, 1E-05) = NVL (AggF4074.F4211_SDIVD, 1E-05)
                )
              )
              AND (
                NVL (AggF4981.F4211_SDURRF, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDURRF, N'---COM.D11S---Null--')
              )
            )
            AND (
              NVL (AggF4981.F4101_IMUWUM, N'---COM.D11S---Null--') = NVL (AggF4074.F4101_IMUWUM, N'---COM.D11S---Null--')
            )
          )
          AND (
            NVL (AggF4981.F4211_SDSRP2, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDSRP2, N'---COM.D11S---Null--')
          )
        )
        AND (
          NVL (AggF4981.F4211_SDSRP3, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDSRP3, N'---COM.D11S---Null--')
        )
      )
      AND (
        NVL (AggF4981.F4211_SDSRP4, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDSRP4, N'---COM.D11S---Null--')
      )
    )
    AND (
      NVL (AggF4981.F4211_SDGLC, N'---COM.D11S---Null--') = NVL (AggF4074.F4211_SDGLC, N'---COM.D11S---Null--')
    )
  )
  AND (
    NVL (AggF4981.F41002_UMCONV, 1E-05) = NVL (AggF4074.F41002_UMCONV, 1E-05)
  )
ORDER BY
  F4211_SDMCU ASC,
  F4211_SDSHPN ASC,
  F4211_SDFRTH ASC,
  F4211_SDMOT ASC,
  F0101_ABSIC ASC,
  F4981_FHCTY1 ASC,
  F4981_FHADDS ASC,
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
  F4981_FHADDZ ASC,
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
  F4201_SHHOLD ASC,
  F4201_SHDEL1 ASC,
  F4201_SHDEL2 ASC,
  F0101_ABALPH ASC,
  F0116_ALCTY1 ASC,
  F0116_ALADDS ASC,
  F4211_SDHOLD ASC,
  F4211_SDPA8 ASC,
  F4211_SDUPRC ASC,
  F4211_SDTORG ASC,
  F4211_SDODCT ASC,
  F4211_SDOORN ASC,
  F4211_SDODOC ASC