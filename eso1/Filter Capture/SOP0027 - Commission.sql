SELECT
    AggF4211.F4211_SDSRP5 F4211_SDSRP5,
    AggF42005.F42005_SCSLSP F42005_SCSLSP,
    AggF4211.F4211_SDMCU F4211_SDMCU,
    AggF4211.F4211_SDKCOO F4211_SDKCOO,
    AggF4211.F4211_SDDCTO F4211_SDDCTO,
    AggF4211.F4211_SDSHAN F4211_SDSHAN,
    AggF42005.F4201_SHAN8 F4201_SHAN8,
    AggF4211.F4211_SDDOCO F4211_SDDOCO,
    AggF4211.F4211_SDLNID F4211_SDLNID,
    AggF4211.F4211_SDDOC F4211_SDDOC,
    AggF4211.F4211_SDADDJ F4211_SDADDJ,
    AggF4211.F4211_SDDGL F4211_SDDGL,
    AggF4211.F4211_SDLITM F4211_SDLITM,
    AggF4211.F4211_SDITM F4211_SDITM,
    AggF4211.F4211_SDLNTY F4211_SDLNTY,
    AggF42005.F42005_SCCPCT F42005_SCCPCT,
    AggF42005.F42005_SCCCTY F42005_SCCCTY,
    AggF4211.F4211_SDUOM4 F4211_SDUOM4,
    AggF4211.F4211_SDUOM1 F4211_SDUOM1,
    AggF42005.F0101_ABAC10 F0101_ABAC10,
    AggF42005.ReportColumn1 ReportColumn1,
    AggF42005.ReportColumn2 ReportColumn2,
    AggF42005.ReportColumn3 ReportColumn3,
    AggF4211.ReportColumn4 ReportColumn4,
    AggF4211.ReportColumn5 ReportColumn5,
    AggF4211.ReportColumn6 ReportColumn6,
    AggF4211.ReportColumn7 ReportColumn7,
    AggF42005.RowIDX_F42005_2_XRowID,
    AggF42005.RowIDX_F42005_1_XRowID,
    AggF42005.RowIDX_F42005_0_XRowID
FROM
    (
        SELECT
            RawF4211.XF4211_SDSRP5 F4211_SDSRP5,
            RawF4211.XF42005_SCSLSP F42005_SCSLSP,
            RawF4211.XF4211_SDMCU F4211_SDMCU,
            RawF4211.XF4211_SDKCOO F4211_SDKCOO,
            RawF4211.XF4211_SDDCTO F4211_SDDCTO,
            RawF4211.XF4211_SDSHAN F4211_SDSHAN,
            RawF4211.XF4201_SHAN8 F4201_SHAN8,
            RawF4211.XF4211_SDDOCO F4211_SDDOCO,
            RawF4211.XF4211_SDLNID F4211_SDLNID,
            RawF4211.XF4211_SDDOC F4211_SDDOC,
            RawF4211.XF4211_SDADDJ F4211_SDADDJ,
            RawF4211.XF4211_SDDGL F4211_SDDGL,
            RawF4211.XF4211_SDLITM F4211_SDLITM,
            RawF4211.XF4211_SDITM F4211_SDITM,
            RawF4211.XF4211_SDLNTY F4211_SDLNTY,
            RawF4211.XF42005_SCCPCT F42005_SCCPCT,
            RawF4211.XF42005_SCCCTY F42005_SCCCTY,
            RawF4211.XF4211_SDUOM4 F4211_SDUOM4,
            RawF4211.XF4211_SDUOM1 F4211_SDUOM1,
            RawF4211.XF0101_ABAC10 F0101_ABAC10,
            SUM(RawF4211.ZReportColumn40001) ReportColumn4,
            SUM(RawF4211.ZReportColumn50001) ReportColumn5,
            SUM(RawF4211.ZReportColumn60001) ReportColumn6,
            SUM(RawF4211.ZReportColumn70001) ReportColumn7
        FROM
            (
                SELECT DISTINCT
                    F4211.SDSRP5 XF4211_SDSRP5,
                    F42005.SCSLSP XF42005_SCSLSP,
                    F4211.SDMCU XF4211_SDMCU,
                    F4211.SDKCOO XF4211_SDKCOO,
                    F4211.SDDCTO XF4211_SDDCTO,
                    F4211.SDSHAN XF4211_SDSHAN,
                    F4201.SHAN8 XF4201_SHAN8,
                    F4211.SDDOCO XF4211_SDDOCO,
                    CAST(F4211.SDLNID AS FLOAT) / 1000 XF4211_SDLNID,
                    F4211.SDDOC XF4211_SDDOC,
                    F4211.SDADDJ XF4211_SDADDJ,
                    F4211.SDDGL XF4211_SDDGL,
                    F4211.SDLITM XF4211_SDLITM,
                    F4211.SDITM XF4211_SDITM,
                    F4211.SDLNTY XF4211_SDLNTY,
                    CAST(F42005.SCCPCT AS FLOAT) / 1000 XF42005_SCCPCT,
                    F42005.SCCCTY XF42005_SCCCTY,
                    F4211.SDUOM4 XF4211_SDUOM4,
                    F4211.SDUOM1 XF4211_SDUOM1,
                    F0101.ABAC10 XF0101_ABAC10,
                    F4211.SDSOQS ZReportColumn40001,
                    F4211.SDAEXP * NVL (CO_F4211_SDCO.ShiftFactor, 0.01) ZReportColumn50001,
                    F4211.SDECST * NVL (CO_F4211_SDCO.ShiftFactor, 0.01) ZReportColumn60001,
                    F4211.SDPQOR ZReportColumn70001,
                    F4211.SDLNID PK__F4211__SDLNID
                FROM
                    PRODDTA.F4211 F4211
                    INNER JOIN PRODDTA.F4201 F4201
                    INNER JOIN (
                        SELECT
                            F0101.ABAC10,
                            F0101.ABAN8
                        FROM
                            PRODDTA.F0101 F0101
                        WHERE
                            (
                                (F0101.ABAT1 BETWEEN N'A  ' AND N'P  ')
                                OR (F0101.ABAT1 BETWEEN N'R  ' AND N'ZZZ')
                            )
                    ) F0101 ON F4201.SHAN8 = F0101.ABAN8 ON (
                        (F4201.SHDOCO = F4211.SDDOCO)
                        AND (F4201.SHDCTO = F4211.SDDCTO)
                    )
                    AND (F4201.SHKCOO = F4211.SDKCOO)
                    LEFT JOIN PRODDTA.F42005 F42005 ON (
                        (
                            (F4211.SDDOCO = F42005.SCDOCO)
                            AND (F4211.SDLNID = F42005.SCLNID)
                        )
                        AND (F4211.SDKCOO = F42005.SCKCOO)
                    )
                    AND (F4211.SDDCTO = F42005.SCDCTO)
                    LEFT JOIN dwtemp10F664AE17E2C55_jds CO_F4211_SDCO ON F4211.SDCO = CO_F4211_SDCO.CO
                WHERE
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
                                                                ((F4211.SDNXTR IN (N'999')))
                                                                AND ((NOT (F4211.SDLTTR IN (N'980'))))
                                                            )
                                                        )
                                                        AND ((NOT (F4211.SDLNTY IN (N'F ', N'FT'))))
                                                    )
                                                )
                                                AND (
                                                    (
                                                        NOT (
                                                            F4211.SDLITM IN (
                                                                N'MISC BILLING             ',
                                                                N'EXPEDITE FEE             '
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                        AND ((F4211.SDDGL BETWEEN 121001 AND 121365))
                                    )
                                )
                                AND ((F4211.SDDCTO IN (N'SO', N'CO')))
                            )
                        )
                        AND ((NOT (F4211.SDCO IN (N'00750'))))
                    )
            ) RawF4211
        GROUP BY
            RawF4211.XF4211_SDSRP5,
            RawF4211.XF42005_SCSLSP,
            RawF4211.XF4211_SDMCU,
            RawF4211.XF4211_SDKCOO,
            RawF4211.XF4211_SDDCTO,
            RawF4211.XF4211_SDSHAN,
            RawF4211.XF4201_SHAN8,
            RawF4211.XF4211_SDDOCO,
            RawF4211.XF4211_SDLNID,
            RawF4211.XF4211_SDDOC,
            RawF4211.XF4211_SDADDJ,
            RawF4211.XF4211_SDDGL,
            RawF4211.XF4211_SDLITM,
            RawF4211.XF4211_SDITM,
            RawF4211.XF4211_SDLNTY,
            RawF4211.XF42005_SCCPCT,
            RawF4211.XF42005_SCCCTY,
            RawF4211.XF4211_SDUOM4,
            RawF4211.XF4211_SDUOM1,
            RawF4211.XF0101_ABAC10
    ) AggF4211
    INNER JOIN (
        SELECT
            RawF42005.XF4211_SDSRP5 F4211_SDSRP5,
            RawF42005.XF42005_SCSLSP F42005_SCSLSP,
            RawF42005.XF4211_SDMCU F4211_SDMCU,
            RawF42005.XF4211_SDKCOO F4211_SDKCOO,
            RawF42005.XF4211_SDDCTO F4211_SDDCTO,
            RawF42005.XF4211_SDSHAN F4211_SDSHAN,
            RawF42005.XF4201_SHAN8 F4201_SHAN8,
            RawF42005.XF4211_SDDOCO F4211_SDDOCO,
            RawF42005.XF4211_SDLNID F4211_SDLNID,
            RawF42005.XF4211_SDDOC F4211_SDDOC,
            RawF42005.XF4211_SDADDJ F4211_SDADDJ,
            RawF42005.XF4211_SDDGL F4211_SDDGL,
            RawF42005.XF4211_SDLITM F4211_SDLITM,
            RawF42005.XF4211_SDITM F4211_SDITM,
            RawF42005.XF4211_SDLNTY F4211_SDLNTY,
            RawF42005.XF42005_SCCPCT F42005_SCCPCT,
            RawF42005.XF42005_SCCCTY F42005_SCCCTY,
            RawF42005.XF4211_SDUOM4 F4211_SDUOM4,
            RawF42005.XF4211_SDUOM1 F4211_SDUOM1,
            RawF42005.XF0101_ABAC10 F0101_ABAC10,
            SUM(RawF42005.ZReportColumn10001) ReportColumn1,
            SUM(RawF42005.ZReportColumn20001) ReportColumn2,
            SUM(RawF42005.ZReportColumn30001) ReportColumn3,
            SUM(RawF42005.XRowIDX_F42005_0_XRowID) RowIDX_F42005_0_XRowID,
            SUM(RawF42005.XRowIDX_F42005_1_XRowID) RowIDX_F42005_1_XRowID,
            SUM(RawF42005.XRowIDX_F42005_2_XRowID) RowIDX_F42005_2_XRowID
        FROM
            (
                SELECT DISTINCT
                    F4211.SDSRP5 XF4211_SDSRP5,
                    F42005.SCSLSP XF42005_SCSLSP,
                    F4211.SDMCU XF4211_SDMCU,
                    F4211.SDKCOO XF4211_SDKCOO,
                    F4211.SDDCTO XF4211_SDDCTO,
                    F4211.SDSHAN XF4211_SDSHAN,
                    F4201.SHAN8 XF4201_SHAN8,
                    F4211.SDDOCO XF4211_SDDOCO,
                    CAST(F4211.SDLNID AS FLOAT) / 1000 XF4211_SDLNID,
                    F4211.SDDOC XF4211_SDDOC,
                    F4211.SDADDJ XF4211_SDADDJ,
                    F4211.SDDGL XF4211_SDDGL,
                    F4211.SDLITM XF4211_SDLITM,
                    F4211.SDITM XF4211_SDITM,
                    F4211.SDLNTY XF4211_SDLNTY,
                    CAST(F42005.SCCPCT AS FLOAT) / 1000 XF42005_SCCPCT,
                    F42005.SCCCTY XF42005_SCCCTY,
                    F4211.SDUOM4 XF4211_SDUOM4,
                    F4211.SDUOM1 XF4211_SDUOM1,
                    F0101.ABAC10 XF0101_ABAC10,
                    F42005.SCTOTL * 0.01 ZReportColumn10001,
                    F42005.SCLRCS * 0.01 ZReportColumn20001,
                    F42005.SCCOMA * 0.01 ZReportColumn30001,
                    F42005.SCKCOO PK__F42005__SCKCOO,
                    F42005.SCDOCO PK__F42005__SCDOCO,
                    F42005.SCDCTO PK__F42005__SCDCTO,
                    F42005.SCLNID PK__F42005__SCLNID,
                    F42005.SCCMLN PK__F42005__SCCMLN,
                    DBMS_UTILITY.GET_HASH_VALUE (
                        TO_CHAR (F42005.SCKCOO) || N'_ISS_' || TO_CHAR (F42005.SCDOCO) || N'_ISS_' || TO_CHAR (F42005.SCSLSP) || N'_ISS_' || TO_CHAR (F42005.SCDCTO) || N'_ISS_' || TO_CHAR (F42005.SCLNID) || N'_ISS_' || TO_CHAR (F42005.SCCMLN) || N'_InDeX_0_SaLt',
                        -1048576,
                        2097152
                    ) XRowIDX_F42005_0_XRowID,
                    DBMS_UTILITY.GET_HASH_VALUE (
                        TO_CHAR (F42005.SCKCOO) || N'_ISS_' || TO_CHAR (F42005.SCDOCO) || N'_ISS_' || TO_CHAR (F42005.SCSLSP) || N'_ISS_' || TO_CHAR (F42005.SCDCTO) || N'_ISS_' || TO_CHAR (F42005.SCLNID) || N'_ISS_' || TO_CHAR (F42005.SCCMLN) || N'_InDeX_1_SaLt',
                        -1048576,
                        2097152
                    ) XRowIDX_F42005_1_XRowID,
                    DBMS_UTILITY.GET_HASH_VALUE (
                        TO_CHAR (F42005.SCKCOO) || N'_ISS_' || TO_CHAR (F42005.SCDOCO) || N'_ISS_' || TO_CHAR (F42005.SCSLSP) || N'_ISS_' || TO_CHAR (F42005.SCDCTO) || N'_ISS_' || TO_CHAR (F42005.SCLNID) || N'_ISS_' || TO_CHAR (F42005.SCCMLN) || N'_InDeX_2_SaLt',
                        -1048576,
                        2097152
                    ) XRowIDX_F42005_2_XRowID
                FROM
                    PRODDTA.F4211 F4211
                    INNER JOIN PRODDTA.F4201 F4201
                    INNER JOIN (
                        SELECT
                            F0101.ABAC10,
                            F0101.ABAN8
                        FROM
                            PRODDTA.F0101 F0101
                        WHERE
                            (
                                (F0101.ABAT1 BETWEEN N'A  ' AND N'P  ')
                                OR (F0101.ABAT1 BETWEEN N'R  ' AND N'ZZZ')
                            )
                    ) F0101 ON F4201.SHAN8 = F0101.ABAN8 ON (
                        (F4201.SHDOCO = F4211.SDDOCO)
                        AND (F4201.SHDCTO = F4211.SDDCTO)
                    )
                    AND (F4201.SHKCOO = F4211.SDKCOO)
                    LEFT JOIN PRODDTA.F42005 F42005 ON (
                        (
                            (F4211.SDDOCO = F42005.SCDOCO)
                            AND (F4211.SDLNID = F42005.SCLNID)
                        )
                        AND (F4211.SDKCOO = F42005.SCKCOO)
                    )
                    AND (F4211.SDDCTO = F42005.SCDCTO)
                    LEFT JOIN dwtemp10F664AE17E2C55_jds CO_F4211_SDCO ON F4211.SDCO = CO_F4211_SDCO.CO
                WHERE
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
                                                                ((F4211.SDNXTR IN (N'999')))
                                                                AND ((NOT (F4211.SDLTTR IN (N'980'))))
                                                            )
                                                        )
                                                        AND ((NOT (F4211.SDLNTY IN (N'F ', N'FT'))))
                                                    )
                                                )
                                                AND (
                                                    (
                                                        NOT (
                                                            F4211.SDLITM IN (
                                                                N'MISC BILLING             ',
                                                                N'EXPEDITE FEE             '
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                        AND ((F4211.SDDGL BETWEEN 121001 AND 121365))
                                    )
                                )
                                AND ((F4211.SDDCTO IN (N'SO', N'CO')))
                            )
                        )
                        AND ((NOT (F4211.SDCO IN (N'00750'))))
                    )
            ) RawF42005
        GROUP BY
            RawF42005.XF4211_SDSRP5,
            RawF42005.XF42005_SCSLSP,
            RawF42005.XF4211_SDMCU,
            RawF42005.XF4211_SDKCOO,
            RawF42005.XF4211_SDDCTO,
            RawF42005.XF4211_SDSHAN,
            RawF42005.XF4201_SHAN8,
            RawF42005.XF4211_SDDOCO,
            RawF42005.XF4211_SDLNID,
            RawF42005.XF4211_SDDOC,
            RawF42005.XF4211_SDADDJ,
            RawF42005.XF4211_SDDGL,
            RawF42005.XF4211_SDLITM,
            RawF42005.XF4211_SDITM,
            RawF42005.XF4211_SDLNTY,
            RawF42005.XF42005_SCCPCT,
            RawF42005.XF42005_SCCCTY,
            RawF42005.XF4211_SDUOM4,
            RawF42005.XF4211_SDUOM1,
            RawF42005.XF0101_ABAC10
    ) AggF42005 ON (
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
                                                                                NVL (AggF4211.F4211_SDSRP5, N'---COM.D11S---Null--') = NVL (AggF42005.F4211_SDSRP5, N'---COM.D11S---Null--')
                                                                            )
                                                                            AND (
                                                                                NVL (AggF4211.F42005_SCSLSP, 1E-05) = NVL (AggF42005.F42005_SCSLSP, 1E-05)
                                                                            )
                                                                        )
                                                                        AND (
                                                                            NVL (AggF4211.F4211_SDMCU, N'---COM.D11S---Null--') = NVL (AggF42005.F4211_SDMCU, N'---COM.D11S---Null--')
                                                                        )
                                                                    )
                                                                    AND (
                                                                        NVL (AggF4211.F4211_SDKCOO, N'---COM.D11S---Null--') = NVL (AggF42005.F4211_SDKCOO, N'---COM.D11S---Null--')
                                                                    )
                                                                )
                                                                AND (
                                                                    NVL (AggF4211.F4211_SDDCTO, N'---COM.D11S---Null--') = NVL (AggF42005.F4211_SDDCTO, N'---COM.D11S---Null--')
                                                                )
                                                            )
                                                            AND (
                                                                NVL (AggF4211.F4211_SDSHAN, 1E-05) = NVL (AggF42005.F4211_SDSHAN, 1E-05)
                                                            )
                                                        )
                                                        AND (
                                                            NVL (AggF4211.F4201_SHAN8, 1E-05) = NVL (AggF42005.F4201_SHAN8, 1E-05)
                                                        )
                                                    )
                                                    AND (
                                                        NVL (AggF4211.F4211_SDDOCO, 1E-05) = NVL (AggF42005.F4211_SDDOCO, 1E-05)
                                                    )
                                                )
                                                AND (
                                                    NVL (AggF4211.F4211_SDLNID, 1E-05) = NVL (AggF42005.F4211_SDLNID, 1E-05)
                                                )
                                            )
                                            AND (
                                                NVL (AggF4211.F4211_SDDOC, 1E-05) = NVL (AggF42005.F4211_SDDOC, 1E-05)
                                            )
                                        )
                                        AND (
                                            NVL (AggF4211.F4211_SDADDJ, 1E-05) = NVL (AggF42005.F4211_SDADDJ, 1E-05)
                                        )
                                    )
                                    AND (
                                        NVL (AggF4211.F4211_SDDGL, 1E-05) = NVL (AggF42005.F4211_SDDGL, 1E-05)
                                    )
                                )
                                AND (
                                    NVL (AggF4211.F4211_SDLITM, N'---COM.D11S---Null--') = NVL (AggF42005.F4211_SDLITM, N'---COM.D11S---Null--')
                                )
                            )
                            AND (
                                NVL (AggF4211.F4211_SDITM, 1E-05) = NVL (AggF42005.F4211_SDITM, 1E-05)
                            )
                        )
                        AND (
                            NVL (AggF4211.F4211_SDLNTY, N'---COM.D11S---Null--') = NVL (AggF42005.F4211_SDLNTY, N'---COM.D11S---Null--')
                        )
                    )
                    AND (
                        NVL (AggF4211.F42005_SCCPCT, 1E-05) = NVL (AggF42005.F42005_SCCPCT, 1E-05)
                    )
                )
                AND (
                    NVL (AggF4211.F42005_SCCCTY, N'---COM.D11S---Null--') = NVL (AggF42005.F42005_SCCCTY, N'---COM.D11S---Null--')
                )
            )
            AND (
                NVL (AggF4211.F4211_SDUOM4, N'---COM.D11S---Null--') = NVL (AggF42005.F4211_SDUOM4, N'---COM.D11S---Null--')
            )
        )
        AND (
            NVL (AggF4211.F4211_SDUOM1, N'---COM.D11S---Null--') = NVL (AggF42005.F4211_SDUOM1, N'---COM.D11S---Null--')
        )
    )
    AND (
        NVL (AggF4211.F0101_ABAC10, N'---COM.D11S---Null--') = NVL (AggF42005.F0101_ABAC10, N'---COM.D11S---Null--')
    )
ORDER BY
    F4211_SDSRP5 ASC,
    F42005_SCCCTY ASC,
    F4211_SDMCU ASC,
    F42005_SCSLSP ASC,
    F4211_SDSHAN ASC,
    F4211_SDDOCO ASC,
    F4211_SDLNID ASC,
    F4211_SDADDJ ASC,
    F4211_SDDGL ASC,
    F4211_SDDOC ASC,
    F4211_SDKCOO ASC,
    F4211_SDDCTO ASC,
    F4201_SHAN8 ASC,
    F4211_SDLITM ASC,
    F4211_SDUOM4 ASC,
    F4211_SDUOM1 ASC,
    F4211_SDITM ASC,
    F42005_SCCPCT ASC,
    F4211_SDLNTY ASC,
    F0101_ABAC10 ASC