SELECT
       RawF0101.XSBXLOADPOVIEW_SDDOCO SBXLOADPOVIEW_SDDOCO,
       RawF0101.XSBXLOADPOVIEW_SDDCTO SBXLOADPOVIEW_SDDCTO,
       RawF0101.XSBXLOADPOVIEW_SDKCOO SBXLOADPOVIEW_SDKCOO,
       RawF0101.XSBXLOADPOVIEW_SDMCU SBXLOADPOVIEW_SDMCU,
       RawF0101.XSBXLOADPOVIEW_SDAN8 SBXLOADPOVIEW_SDAN8,
       RawF0101.XSBXLOADPOVIEW_SDSHAN SBXLOADPOVIEW_SDSHAN,
       RawF0101.XSBXLOADPOVIEW_SDCARS SBXLOADPOVIEW_SDCARS,
       RawF0101.XSBXLOADPOVIEW_SDVR01 SBXLOADPOVIEW_SDVR01,
       RawF0101.XF554201T_QCDS50 F554201T_QCDS50,
       RawF0101.XSBXUSSSAND_SOPONO SBXUSSSAND_SOPONO,
       RawF0101.XSBXLOADPOVIEW_SDLITM SBXLOADPOVIEW_SDLITM,
       RawF0101.XSBXLOADPOVIEW_SDDSC1 SBXLOADPOVIEW_SDDSC1,
       RawF0101.XSBXLOADPOVIEW_ORDATE SBXLOADPOVIEW_ORDATE,
       RawF0101.XSBXLOADPOVIEW_GLDATE SBXLOADPOVIEW_GLDATE,
       RawF0101.XSBXLOADPOVIEW_LOFA SBXLOADPOVIEW_LOFA,
       RawF0101.XF0101_ABALPH F0101_ABALPH,
       RawF0101.XSBXUSSSAND_LOFAPLANTMCU SBXUSSSAND_LOFAPLANTMCU,
       RawF0101.XSBXUSSSAND_PLANT84d27c18 SBXUSSSAND_PLANTTRANSLOAD,
       RawF0101.XSBXUSSSAND_USSSAND SBXUSSSAND_USSSAND,
       RawF0101.XSBXUSSSAND_MATCHFLAG SBXUSSSAND_MATCHFLAG,
       RawF0101.XSBXUSSSAND_SOORDERNO SBXUSSSAND_SOORDERNO,
       RawF0101.XSBXUSSSAND_SOWEIGHT SBXUSSSAND_SOWEIGHT,
       RawF0101.XSBXUSSSAND_SXWEIGHT SBXUSSSAND_SXWEIGHT,
       RawF0101.XSBXUSSSAND_SOALTBOLNO SBXUSSSAND_SOALTBOLNO,
       RawF0101.XSBXLOADPOVIEW_SANDTKT SBXLOADPOVIEW_SANDTKT,
       RawF0101.XSBXLOADPOVIEW_BOL SBXLOADPOVIEW_BOL,
       RawF0101.XSBXLOADPOVIEW_UOM SBXLOADPOVIEW_UOM,
       RawF0101.XSBXLOADPOVIEW_QTY SBXLOADPOVIEW_QTY,
       RawF0101.XSBXLOADPOVIEW_SDUPRC SBXLOADPOVIEW_SDUPRC,
       RawF0101.XSBXLOADPOVIEW_EXTAMT SBXLOADPOVIEW_EXTAMT,
       RawF0101.XSBXLOADPOVIEW_SDLTTR SBXLOADPOVIEW_SDLTTR,
       RawF0101.XSBXLOADPOVIEW_SDNXTR SBXLOADPOVIEW_SDNXTR,
       RawF0101.XSBXLOADPOVIEW_SDDOC SBXLOADPOVIEW_SDDOC,
       RawF0101.XSBXLOADPOVIEW_OXLTTR SBXLOADPOVIEW_OXLTTR,
       RawF0101.XSBXLOADPOVIEW_OXNXTR SBXLOADPOVIEW_OXNXTR,
       RawF0101.XSBXLOADPOVIEW_OXAMT SBXLOADPOVIEW_OXAMT,
       RawF0101.XSBXLOADPOVIEW_GLPOST SBXLOADPOVIEW_GLPOST,
       RawF0101.XSBXLOADPOVIEW_GLDGJ SBXLOADPOVIEW_GLDGJ,
       RawF0101.XSBXLOADPOVIEW_SDLNID SBXLOADPOVIEW_SDLNID,
       SUM(RawF0101.ZReportColumn10001) ReportColumn1,
       SUM(RawF0101.XRowIDX_F0101_0_XRowID) RowIDX_F0101_0_XRowID,
       SUM(RawF0101.XRowIDX_F0101_1_XRowID) RowIDX_F0101_1_XRowID,
       SUM(RawF0101.XRowIDX_F0101_2_XRowID) RowIDX_F0101_2_XRowID
FROM
       (
              SELECT DISTINCT
                     SBXLOADPOVIEW.SDDOCO XSBXLOADPOVIEW_SDDOCO,
                     SBXLOADPOVIEW.SDDCTO XSBXLOADPOVIEW_SDDCTO,
                     SBXLOADPOVIEW.SDKCOO XSBXLOADPOVIEW_SDKCOO,
                     SBXLOADPOVIEW.SDMCU XSBXLOADPOVIEW_SDMCU,
                     SBXLOADPOVIEW.SDAN8 XSBXLOADPOVIEW_SDAN8,
                     SBXLOADPOVIEW.SDSHAN XSBXLOADPOVIEW_SDSHAN,
                     SBXLOADPOVIEW.SDCARS XSBXLOADPOVIEW_SDCARS,
                     SBXLOADPOVIEW.SDVR01 XSBXLOADPOVIEW_SDVR01,
                     F554201T.QCDS50 XF554201T_QCDS50,
                     SBXUSSSAND.SOPONO XSBXUSSSAND_SOPONO,
                     SBXLOADPOVIEW.SDLITM XSBXLOADPOVIEW_SDLITM,
                     SBXLOADPOVIEW.SDDSC1 XSBXLOADPOVIEW_SDDSC1,
                     (TO_CHAR (SBXLOADPOVIEW.ORDATE, N'YYYY') - 1900) * 1000 + TO_CHAR (SBXLOADPOVIEW.ORDATE, N'DDD') XSBXLOADPOVIEW_ORDATE,
                     (TO_CHAR (SBXLOADPOVIEW.GLDATE, N'YYYY') - 1900) * 1000 + TO_CHAR (SBXLOADPOVIEW.GLDATE, N'DDD') XSBXLOADPOVIEW_GLDATE,
                     SBXLOADPOVIEW.LOFA XSBXLOADPOVIEW_LOFA,
                     F0101.ABALPH XF0101_ABALPH,
                     SBXUSSSAND.LOFAPLANTMCU XSBXUSSSAND_LOFAPLANTMCU,
                     SBXUSSSAND.PLANTTRANSLOAD XSBXUSSSAND_PLANT84d27c18,
                     SBXUSSSAND.USSSAND XSBXUSSSAND_USSSAND,
                     SBXUSSSAND.MATCHFLAG XSBXUSSSAND_MATCHFLAG,
                     SBXUSSSAND.SOORDERNO XSBXUSSSAND_SOORDERNO,
                     SBXUSSSAND.SOWEIGHT XSBXUSSSAND_SOWEIGHT,
                     SBXUSSSAND.SXWEIGHT XSBXUSSSAND_SXWEIGHT,
                     SBXUSSSAND.SOALTBOLNO XSBXUSSSAND_SOALTBOLNO,
                     SBXLOADPOVIEW.SANDTKT XSBXLOADPOVIEW_SANDTKT,
                     SBXLOADPOVIEW.BOL XSBXLOADPOVIEW_BOL,
                     SBXLOADPOVIEW.UOM XSBXLOADPOVIEW_UOM,
                     SBXLOADPOVIEW.QTY XSBXLOADPOVIEW_QTY,
                     SBXLOADPOVIEW.SDUPRC XSBXLOADPOVIEW_SDUPRC,
                     SBXLOADPOVIEW.EXTAMT XSBXLOADPOVIEW_EXTAMT,
                     SBXLOADPOVIEW.SDLTTR XSBXLOADPOVIEW_SDLTTR,
                     SBXLOADPOVIEW.SDNXTR XSBXLOADPOVIEW_SDNXTR,
                     SBXLOADPOVIEW.SDDOC XSBXLOADPOVIEW_SDDOC,
                     SBXLOADPOVIEW.OXLTTR XSBXLOADPOVIEW_OXLTTR,
                     SBXLOADPOVIEW.OXNXTR XSBXLOADPOVIEW_OXNXTR,
                     SBXLOADPOVIEW.OXAMT XSBXLOADPOVIEW_OXAMT,
                     SBXLOADPOVIEW.GLPOST XSBXLOADPOVIEW_GLPOST,
                     SBXLOADPOVIEW.GLDGJ XSBXLOADPOVIEW_GLDGJ,
                     SBXLOADPOVIEW.SDLNID XSBXLOADPOVIEW_SDLNID,
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
                     (
                            SELECT
                                   SDKCOO,
                                   SDDOCO,
                                   SDDCTO,
                                   SDVR01,
                                   SDAN8,
                                   SDSHAN,
                                   SDCARS,
                                   SDMCU,
                                   SDLITM,
                                   SDDSC1,
                                   (
                                          select
                                                 MAX(I.sddsc1)
                                          from
                                                 proddta.F4211 I
                                          where
                                                 I.sddoco = F4211.SDDOCO
                                                 and I.sddcto = F4211.SDDCTO
                                                 and I.sdlitm = 'BOL'
                                   ) AS BOL,
                                   (
                                          select
                                                 MAX(I.sddsc1)
                                          from
                                                 proddta.F4211 I
                                          where
                                                 I.sddoco = F4211.SDDOCO
                                                 and I.sddcto = F4211.SDDCTO
                                                 and I.sdlitm = 'SANDTKTNBR'
                                   ) as SANDTKT,
                                   F4211.SDUOM AS UOM,
                                   DECODE (
                                          F4211.SDPRP1,
                                          'COM',
                                          (F4211.SDUORG / 1000) / 2000,
                                          F4211.SDUORG / 1000
                                   ) AS Qty,
                                   TO_DATE (TO_CHAR (SDTRDJ + 1900000), 'YYYYDDD') AS ORDATE,
                                   SDVEND AS LOFA,
                                   sduprc,
                                   F4211.SDAEXP AS EXTAMT,
                                   CASE
                                          WHEN F4211.SDDGL = 0 THEN TO_DATE ('1900001', 'YYYYDDD')
                                          WHEN F4211.SDDGL IS NULL THEN TO_DATE ('1900001', 'YYYYDDD')
                                          ELSE TO_DATE (TO_CHAR (F4211.SDDGL + 1900000), 'YYYYDDD')
                                   END AS GLDATE,
                                   F4211.SDLTTR AS SDLTTR,
                                   F4211.SDNXTR AS SDNXTR,
                                   F4211.SDDOC AS SDDOC,
                                   (
                                          select
                                                 MAX(pdnxtr)
                                          from
                                                 proddta.F4311
                                          where
                                                 pddcto = 'OX'
                                                 and pdkcoo = '00750'
                                                 and pdlitm = F4211.SDLITM
                                                 and pddoco = F4211.sddoco
                                   ) AS OXNXTR,
                                   (
                                          select
                                                 MAX(pdlttr)
                                          from
                                                 proddta.F4311
                                          where
                                                 pddcto = 'OX'
                                                 and pdkcoo = '00750'
                                                 and pdlitm = F4211.SDLITM
                                                 and pddoco = F4211.Sddoco
                                   ) AS OXLTTR,
                                   CASE
                                          WHEN F4211.SDLITM IN ('FRT') THEN (
                                                 select
                                                        SUM(pdAEXP)
                                                 from
                                                        proddta.F4311
                                                 where
                                                        pddcto = 'OX'
                                                        and pdkcoo = '00750'
                                                        and pdlitm = F4211.SDLITM
                                                        and pddoco = F4211.Sddoco
                                          )
                                          ELSE 0
                                   END AS OXAMT,
                                   (
                                          select
                                                 max(glpost)
                                          from
                                                 proddta.F0911
                                          where
                                                 gldct = 'OV'
                                                 and glkco = '00750'
                                                 and gldoc in (
                                                        select
                                                               prdoc
                                                        from
                                                               proddta.F43121
                                                        where
                                                               sdcars = pran8
                                                               and F4211.sdkcoo = prkcoo
                                                               and F4211.sddoco = prdoco
                                                               and F4211.sdlitm = prlitm
                                                               and prdct = 'OV'
                                                 )
                                   ) GLPost,
                                   (
                                          select
                                                 MAX(
                                                        to_char (
                                                               to_date (to_char (1900000 + f43121.PRDGL), 'yyyyddd'),
                                                               'mm/dd/yyyy'
                                                        )
                                                 )
                                          from
                                                 proddta.F43121
                                          where
                                                 F4211.sdcars = F43121.pran8
                                                 and F4211.sdkcoo = F43121.prkcoo
                                                 and F4211.sddoco = F43121.prdoco
                                                 and F4211.sdlitm = F43121.prlitm
                                                 and F43121.prdct = 'OV'
                                                 AND F43121.PRMATC = '1'
                                                 AND F43121.PRDGL > 1
                                   ) GLdgj,
                                   SDLNID
                            FROM
                                   PRODDTA.F4211
                            WHERE
                                   F4211.SDDCTO = 'SX'
                                   and F4211.sdlnty <> 'TL'
                                   and F4211.sdkcoo = '00750'
                                   and F4211.SDLITM <> 'HOLADD'
                     ) SBXLOADPOVIEW
                     LEFT JOIN PRODDTA.F554201T F554201T ON (
                            (
                                   SUBSTR (
                                          N'00000' || TO_CHAR (RTRIM (LTRIM (SBXLOADPOVIEW.SDKCOO))),
                                          LENGTH (TO_CHAR (RTRIM (LTRIM (SBXLOADPOVIEW.SDKCOO)))) + 1,
                                          5
                                   ) = F554201T.QCKCOO
                            )
                            AND (SBXLOADPOVIEW.SDDOCO = F554201T.QCDOCO)
                     )
                     AND (SBXLOADPOVIEW.SDDCTO = F554201T.QCDCTO)
                     LEFT JOIN (
                            select
                                   SDDOCO,
                                   SDDCTO,
                                   SDVEND,
                                   SDAN8,
                                   SDSHAN,
                                   SDMCU,
                                   SANDTKTPO,
                                   SANDTKTNO,
                                   SOORDERNO,
                                   SOALTBOLNO,
                                   SOPONO,
                                   MATCHFLAG,
                                   USSSAND,
                                   LOFAPLANTMCU,
                                   PLANTTRANSLOAD,
                                   SXWEIGHT,
                                   SOWEIGHT
                            FROM
                                   (
                                          SELECT
                                                 SDDOCO,
                                                 SDDCTO,
                                                 SDVEND,
                                                 SDAN8,
                                                 SDSHAN,
                                                 SDMCU,
                                                 SANDTKTPO,
                                                 SANDTKTNO,
                                                 SOORDERNO,
                                                 SOALTBOLNO,
                                                 SOPONO,
                                                 MATCHFLAG,
                                                 USSSAND,
                                                 LOFAPLANTMCU,
                                                 PLANTTRANSLOAD,
                                                 SXWEIGHT,
                                                 SOWEIGHT
                                          FROM
                                                 (
                                                        SELECT
                                                               M.sddoco,
                                                               M.sddcto,
                                                               M.sdvend,
                                                               M.sdan8,
                                                               M.sdshan,
                                                               M.sdmcu,
                                                               F554201T.QCDS50 "SANDTKTPO",
                                                               M.sddsc1 "SANDTKTNO",
                                                               (
                                                                      select distinct
                                                                             L.sddoco
                                                                      from
                                                                             proddta.f4211 L
                                                                      where
                                                                             L.sdpsig = rpad (rtrim (rtrim (M.sddsc1, ' '), '.'), 30, ' ')
                                                                             and L.sdvr01 = rpad (rtrim (F554201T.qcds50, ' '), 25, ' ')
                                                                             and L.sddcto = 'SO'
                                                                             and L.sdco = '00400'
                                                                             and L.SDMCU in (
                                                                                    select
                                                                                           F0005.drsphd
                                                                                    from
                                                                                           prodctl.F0005
                                                                                    where
                                                                                           F0005.drsy = '55'
                                                                                           and F0005.drrt = 'UP'
                                                                                           and TO_Number (rtrim (F0005.drky, ' ')) = M.sdvend
                                                                             )
                                                               ) "SOORDERNO",
                                                               (
                                                                      select distinct
                                                                             L2.sdpsig
                                                                      from
                                                                             proddta.f4211 L2
                                                                      where
                                                                             L2.sdpsig = rpad (rtrim (rtrim (M.sddsc1, ' '), '.'), 30, ' ')
                                                                             and L2.sdvr01 = rpad (rtrim (F554201T.qcds50, ' '), 25, ' ')
                                                                             and L2.sddcto = 'SO'
                                                                             and L2.sdco = '00400'
                                                               ) "SOALTBOLNO",
                                                               (
                                                                      select distinct
                                                                             L3.sdvr01
                                                                      from
                                                                             proddta.f4211 L3
                                                                      where
                                                                             L3.sdpsig = rpad (rtrim (rtrim (M.sddsc1, ' '), '.'), 30, ' ')
                                                                             and L3.sdvr01 = rpad (rtrim (F554201T.qcds50, ' '), 25, ' ')
                                                                             and L3.sddcto = 'SO'
                                                                             and L3.sdco = '00400'
                                                               ) "SOPONO",
                                                               (
                                                                      select distinct
                                                                             'Y'
                                                                      from
                                                                             proddta.f4211 L4
                                                                      where
                                                                             L4.sdpsig = rpad (rtrim (rtrim (M.sddsc1, ' '), '.'), 30, ' ')
                                                                             and L4.sdvr01 = rpad (rtrim (F554201T.qcds50, ' '), 25, ' ')
                                                                             and L4.sddcto = 'SO'
                                                                             and L4.sdco = '00400'
                                                               ) "MATCHFLAG",
                                                               (
                                                                      select
                                                                             case
                                                                                    when (
                                                                                           to_Number (D1.drsphd) > 1
                                                                                           AND to_Number (D1.drsphd) < 9000
                                                                                    ) Then 'Y'
                                                                                    else 'N'
                                                                             end
                                                                      from
                                                                             prodctl.F0005 D1
                                                                      where
                                                                             D1.drsy = '55'
                                                                             and D1.drrt = 'UP'
                                                                             and TO_Number (rtrim (D1.drky, ' ')) = M.sdvend
                                                               ) "USSSAND",
                                                               (
                                                                      select
                                                                             F0005.drsphd
                                                                      from
                                                                             prodctl.F0005
                                                                      where
                                                                             F0005.drsy = '55'
                                                                             and F0005.drrt = 'UP'
                                                                             and TO_Number (rtrim (F0005.drky, ' ')) = M.sdvend
                                                               ) "LOFAPLANTMCU",
                                                               (
                                                                      select
                                                                             case
                                                                                    when to_Number (D1.drsphd) > 9000 Then 'TRANSLOAD'
                                                                                    when (
                                                                                           to_Number (D1.drsphd) > 1
                                                                                           AND to_Number (D1.drsphd) < 9000
                                                                                    ) Then 'PLANT'
                                                                                    else '3RDPARTY'
                                                                             end
                                                                      from
                                                                             prodctl.F0005 D1
                                                                      where
                                                                             D1.drsy = '55'
                                                                             and D1.drrt = 'UP'
                                                                             and TO_Number (rtrim (D1.drky, ' ')) = M.sdvend
                                                               ) "PLANTTRANSLOAD",
                                                               (
                                                                      select
                                                                             sum(S.SDUORG / 1000)
                                                                      from
                                                                             proddta.F4211 S
                                                                      where
                                                                             S.sddcto = 'SX'
                                                                             and S.SDPRP1 = 'COM'
                                                                             and S.SDLTTR <> '980'
                                                                             and S.SDDOCO = M.SDDOCO
                                                               ) "SXWEIGHT",
                                                               (
                                                                      select
                                                                             sum(L1.sdSQOR / 1000)
                                                                      from
                                                                             proddta.f4211 L1
                                                                      where
                                                                             L1.sdpsig = rpad (rtrim (rtrim (M.sddsc1, ' '), '.'), 30, ' ')
                                                                             and L1.sdvr01 = rpad (rtrim (F554201T.qcds50, ' '), 25, ' ')
                                                                             and L1.sdlnty = 'S'
                                                                             and L1.sddcto = 'SO'
                                                                             and L1.sdco = '00400'
                                                               ) "SOWEIGHT"
                                                        from
                                                               proddta.F4211 M
                                                               INNER JOIN proddta.F554201T ON M.SDDOCO = F554201T.QCDOCO
                                                               AND M.SDDCTO = F554201T.QCDCTO
                                                               AND M.SDKCOO = F554201T.QCKCOO
                                                        where
                                                               M.sddcto = 'SX'
                                                               and M.SDLITM = 'SANDTKTNBR'
                                                               and M.SDKCOO = '00750'
                                                 )
                                   )
                     ) SBXUSSSAND ON (SBXLOADPOVIEW.SDDOCO = SBXUSSSAND.SDDOCO)
                     AND (SBXLOADPOVIEW.SDDCTO = SBXUSSSAND.SDDCTO)
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
                     ) F0101 ON SBXLOADPOVIEW.LOFA = F0101.ABAN8
              WHERE
                     (
                            (TO_CHAR (SBXLOADPOVIEW.ORDATE, N'YYYY') - 1900) * 1000 + TO_CHAR (SBXLOADPOVIEW.ORDATE, N'DDD') BETWEEN 126121 AND 126151
                     )
       ) RawF0101
GROUP BY
       RawF0101.XSBXLOADPOVIEW_SDDOCO,
       RawF0101.XSBXLOADPOVIEW_SDDCTO,
       RawF0101.XSBXLOADPOVIEW_SDKCOO,
       RawF0101.XSBXLOADPOVIEW_SDMCU,
       RawF0101.XSBXLOADPOVIEW_SDAN8,
       RawF0101.XSBXLOADPOVIEW_SDSHAN,
       RawF0101.XSBXLOADPOVIEW_SDCARS,
       RawF0101.XSBXLOADPOVIEW_SDVR01,
       RawF0101.XF554201T_QCDS50,
       RawF0101.XSBXUSSSAND_SOPONO,
       RawF0101.XSBXLOADPOVIEW_SDLITM,
       RawF0101.XSBXLOADPOVIEW_SDDSC1,
       RawF0101.XSBXLOADPOVIEW_ORDATE,
       RawF0101.XSBXLOADPOVIEW_GLDATE,
       RawF0101.XSBXLOADPOVIEW_LOFA,
       RawF0101.XF0101_ABALPH,
       RawF0101.XSBXUSSSAND_LOFAPLANTMCU,
       RawF0101.XSBXUSSSAND_PLANT84d27c18,
       RawF0101.XSBXUSSSAND_USSSAND,
       RawF0101.XSBXUSSSAND_MATCHFLAG,
       RawF0101.XSBXUSSSAND_SOORDERNO,
       RawF0101.XSBXUSSSAND_SOWEIGHT,
       RawF0101.XSBXUSSSAND_SXWEIGHT,
       RawF0101.XSBXUSSSAND_SOALTBOLNO,
       RawF0101.XSBXLOADPOVIEW_SANDTKT,
       RawF0101.XSBXLOADPOVIEW_BOL,
       RawF0101.XSBXLOADPOVIEW_UOM,
       RawF0101.XSBXLOADPOVIEW_QTY,
       RawF0101.XSBXLOADPOVIEW_SDUPRC,
       RawF0101.XSBXLOADPOVIEW_EXTAMT,
       RawF0101.XSBXLOADPOVIEW_SDLTTR,
       RawF0101.XSBXLOADPOVIEW_SDNXTR,
       RawF0101.XSBXLOADPOVIEW_SDDOC,
       RawF0101.XSBXLOADPOVIEW_OXLTTR,
       RawF0101.XSBXLOADPOVIEW_OXNXTR,
       RawF0101.XSBXLOADPOVIEW_OXAMT,
       RawF0101.XSBXLOADPOVIEW_GLPOST,
       RawF0101.XSBXLOADPOVIEW_GLDGJ,
       RawF0101.XSBXLOADPOVIEW_SDLNID
ORDER BY
       SBXLOADPOVIEW_ORDATE ASC,
       SBXLOADPOVIEW_LOFA ASC,
       SBXLOADPOVIEW_GLDATE ASC,
       SBXLOADPOVIEW_SANDTKT ASC,
       SBXLOADPOVIEW_BOL ASC,
       SBXLOADPOVIEW_EXTAMT ASC,
       SBXLOADPOVIEW_OXLTTR ASC,
       SBXLOADPOVIEW_OXNXTR ASC,
       SBXLOADPOVIEW_OXAMT ASC,
       SBXLOADPOVIEW_GLPOST ASC,
       SBXLOADPOVIEW_SDDOCO ASC,
       SBXLOADPOVIEW_SDDCTO ASC,
       SBXLOADPOVIEW_SDKCOO ASC,
       SBXLOADPOVIEW_SDMCU ASC,
       SBXLOADPOVIEW_SDAN8 ASC,
       SBXLOADPOVIEW_SDSHAN ASC,
       SBXLOADPOVIEW_SDCARS ASC,
       SBXLOADPOVIEW_SDVR01 ASC,
       SBXLOADPOVIEW_SDLITM ASC,
       SBXLOADPOVIEW_SDLTTR ASC,
       SBXLOADPOVIEW_SDNXTR ASC,
       SBXLOADPOVIEW_SDDOC ASC,
       SBXLOADPOVIEW_SDDSC1 ASC,
       SBXLOADPOVIEW_SDUPRC ASC,
       SBXLOADPOVIEW_QTY ASC,
       SBXLOADPOVIEW_UOM ASC,
       SBXLOADPOVIEW_GLDGJ ASC,
       SBXLOADPOVIEW_SDLNID ASC,
       F554201T_QCDS50 ASC,
       F0101_ABALPH ASC,
       SBXUSSSAND_SOORDERNO ASC,
       SBXUSSSAND_LOFAPLANTMCU ASC,
       SBXUSSSAND_PLANTTRANSLOAD ASC,
       SBXUSSSAND_USSSAND ASC,
       SBXUSSSAND_SOALTBOLNO ASC,
       SBXUSSSAND_SOPONO ASC,
       SBXUSSSAND_SOWEIGHT ASC,
       SBXUSSSAND_SXWEIGHT ASC,
       SBXUSSSAND_MATCHFLAG ASC