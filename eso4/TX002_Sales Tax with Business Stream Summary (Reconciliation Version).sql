SELECT RawF03B11.XF4211_SDKCO                 F4211_SDKCO,
       RawF03B11.XC_F4211_SDMCU               C_F4211_SDMCU,
       RawF03B11.XF4211_SDMCU                 F4211_SDMCU,
       RawF03B11.XID_CUSTOM_84c76537583683    ID_CUSTOM_84c7652ecdb3913,
       RawF03B11.XF4211_SDEXR1                F4211_SDEXR1,
       RawF03B11.XF03B11_RPTXA1               F03B11_RPTXA1,
       RawF03B11.XF4211_SDDGL                 F4211_SDDGL,
       RawF03B11.XF03B11_RPDSVJ               F03B11_RPDSVJ,
       RawF03B11.XF4211_SDDCT                 F4211_SDDCT,
       RawF03B11.XF4211_SDDOC                 F4211_SDDOC,
       RawF03B11.XID_CUSTOM_8501fecff7aa51    ID_CUSTOM_8501fe576da8867,
       RawF03B11.XF0116_ALADDS                F0116_ALADDS,
       RawF03B11.XF0116_ALCOUN                F0116_ALCOUN,
       RawF03B11.XF4211_SDSHAN                F4211_SDSHAN,
       RawF03B11.XF4211_SDAN8                 F4211_SDAN8,
       RawF03B11.XF4211_SDPA8                 F4211_SDPA8,
       RawF03B11.XF4211_SDDOCO                F4211_SDDOCO,
       RawF03B11.XF4211_SDDCTO                F4211_SDDCTO,
       RawF03B11.XC_F0101_ABSIC               C_F0101_ABSIC,
       RawF03B11.XF0101_ABSIC                 F0101_ABSIC,
       RawF03B11.XF0006_MCRP20                F0006_MCRP20,
       RawF03B11.XF03B11_RPOKCO               F03B11_RPOKCO,
       RawF03B11.XF03B11_RPODOC               F03B11_RPODOC,
       RawF03B11.XF03B11_RPODCT               F03B11_RPODCT,
       RawF03B11.XID_CUSTOM_8501fc90c75a1     ID_CUSTOM_8501fca6bf83d39,
       SUM(RawF03B11.ZReportColumn10001)      ReportColumn1,
       SUM(RawF03B11.ZReportColumn20001)      ReportColumn2,
       SUM(RawF03B11.ZReportColumn30001)      ReportColumn3,
       SUM(RawF03B11.ZReportColumn40001)      ReportColumn4,
       SUM(RawF03B11.XRowIDX_F03B11_0_XRowID) RowIDX_F03B11_0_XRowID,
       SUM(RawF03B11.XRowIDX_F03B11_1_XRowID) RowIDX_F03B11_1_XRowID,
       SUM(RawF03B11.XRowIDX_F03B11_2_XRowID) RowIDX_F03B11_2_XRowID
FROM   (SELECT DISTINCT F4211.SDKCO                                                          XF4211_SDKCO,
                        F4211.SDMCU                                                          XC_F4211_SDMCU,
                        F4211.SDMCU                                                          XF4211_SDMCU,
                        CASE
                          WHEN ( F0101.ABSIC = N'F' )
                               AND ( F0006.MCRP20 = N'ENG' ) THEN N'O&G'
                          WHEN ( F0101.ABSIC <> N'F' )
                               AND ( F0006.MCRP20 = N'ENG' ) THEN N'ISP'
                          WHEN ( F0101.ABSIC <> N'F' )
                               AND ( F0006.MCRP20 = N'SHR' ) THEN N'ISP'
                          WHEN ( F0101.ABSIC = N'F' )
                               AND ( F0006.MCRP20 = N'SHR' ) THEN N'O&G'
                          WHEN NOT ( F0006.MCRP20 IN ( N'ENG', N'SHR' ) ) THEN N'ISP'
                        END                                                                  XID_CUSTOM_84c76537583683,
                        F4211.SDEXR1                                                         XF4211_SDEXR1,
                        F03B11.RPTXA1                                                        XF03B11_RPTXA1,
                        F4211.SDDGL                                                          XF4211_SDDGL,
                        F03B11.RPDSVJ                                                        XF03B11_RPDSVJ,
                        F4211.SDDCT                                                          XF4211_SDDCT,
                        F4211.SDDOC                                                          XF4211_SDDOC,
                        RTRIM(LTRIM(NVL(F4211.SDDOC, -999999999)))
                         || RTRIM(LTRIM(NVL(F4211.SDDCT, N'')))
                         || RTRIM(LTRIM(NVL(F4211.SDKCO, N'')))                              XID_CUSTOM_8501fecff7aa51,
                        F0116.ALADDS                                                         XF0116_ALADDS,
                        F0116.ALCOUN                                                         XF0116_ALCOUN,
                        F4211.SDSHAN                                                         XF4211_SDSHAN,
                        F4211.SDAN8                                                          XF4211_SDAN8,
                        F4211.SDPA8                                                          XF4211_SDPA8,
                        F4211.SDDOCO                                                         XF4211_SDDOCO,
                        F4211.SDDCTO                                                         XF4211_SDDCTO,
                        F0101.ABSIC                                                          XC_F0101_ABSIC,
                        F0101.ABSIC                                                          XF0101_ABSIC,
                        F0006.MCRP20                                                         XF0006_MCRP20,
                        F03B11.RPOKCO                                                        XF03B11_RPOKCO,
                        F03B11.RPODOC                                                        XF03B11_RPODOC,
                        F03B11.RPODCT                                                        XF03B11_RPODCT,
                        RTRIM(LTRIM(NVL(F4211.SDDOC, -999999999)))
                         || RTRIM(LTRIM(NVL(F4211.SDDCT, N'')))                              XID_CUSTOM_8501fc90c75a1,
                        F03B11.RPATXA * NVL(CO_F03B11_RPCO.ShiftFactor, 0.01)                ZReportColumn10001,
                        F03B11.RPATXN * NVL(CO_F03B11_RPCO.ShiftFactor, 0.01)                ZReportColumn20001,
                        F03B11.RPSTAM * NVL(CO_F03B11_RPCO.ShiftFactor, 0.01)                ZReportColumn30001,
                        F03B11.RPAG * NVL(CO_F03B11_RPCO.ShiftFactor, 0.01)                  ZReportColumn40001,
                        F03B11.RPDOC                                                         PK__F03B11__RPDOC,
                        F03B11.RPDCT                                                         PK__F03B11__RPDCT,
                        F03B11.RPKCO                                                         PK__F03B11__RPKCO,
                        F03B11.RPSFX                                                         PK__F03B11__RPSFX,
                        DBMS_UTILITY.GET_HASH_VALUE(TO_CHAR(F03B11.RPDOC)
                                                     || N'_ISS_'
                                                     || TO_CHAR(F03B11.RPDCT)
                                                     || N'_ISS_'
                                                     || TO_CHAR(F03B11.RPKCO)
                                                     || N'_ISS_'
                                                     || TO_CHAR(F03B11.RPSFX)
                                                     || N'_InDeX_0_SaLt', -1048576, 2097152) XRowIDX_F03B11_0_XRowID,
                        DBMS_UTILITY.GET_HASH_VALUE(TO_CHAR(F03B11.RPDOC)
                                                     || N'_ISS_'
                                                     || TO_CHAR(F03B11.RPDCT)
                                                     || N'_ISS_'
                                                     || TO_CHAR(F03B11.RPKCO)
                                                     || N'_ISS_'
                                                     || TO_CHAR(F03B11.RPSFX)
                                                     || N'_InDeX_1_SaLt', -1048576, 2097152) XRowIDX_F03B11_1_XRowID,
                        DBMS_UTILITY.GET_HASH_VALUE(TO_CHAR(F03B11.RPDOC)
                                                     || N'_ISS_'
                                                     || TO_CHAR(F03B11.RPDCT)
                                                     || N'_ISS_'
                                                     || TO_CHAR(F03B11.RPKCO)
                                                     || N'_ISS_'
                                                     || TO_CHAR(F03B11.RPSFX)
                                                     || N'_InDeX_2_SaLt', -1048576, 2097152) XRowIDX_F03B11_2_XRowID
        FROM   PRODDTA.F4211 F4211
               INNER JOIN PRODDTA.F03B11 F03B11
                 ON ( ( ( F4211.SDDOC = F03B11.RPODOC )
                        AND ( F4211.SDDCT = F03B11.RPODCT ) )
                      AND ( F4211.SDKCO = F03B11.RPOKCO ) )
                    AND ( F4211.SDLNID = F03B11.RPLNID )
               LEFT JOIN (SELECT F0101.ABSIC,
                                 F0101.ABAN8
                          FROM   PRODDTA.F0101 F0101
                          WHERE  ( ( F0101.ABAT1 BETWEEN N'A  ' AND N'P  ' )
                                    OR ( F0101.ABAT1 BETWEEN N'R  ' AND N'ZZZ' ) )) F0101
                         INNER JOIN PRODDTA.F0116 F0116
                           ON F0101.ABAN8 = F0116.ALAN8
                 ON F4211.SDSHAN = F0101.ABAN8
               LEFT JOIN PRODDTA.F0006 F0006
                 ON F4211.SDMCU = F0006.MCMCU
               LEFT JOIN dwtemp63CAC686302C4E4_jds CO_F03B11_RPCO
                 ON F03B11.RPCO = CO_F03B11_RPCO.CO
        WHERE  ( F4211.SDDGL BETWEEN 126032 AND 126059 )) RawF03B11
GROUP  BY RawF03B11.XF4211_SDKCO,
          RawF03B11.XC_F4211_SDMCU,
          RawF03B11.XF4211_SDMCU,
          RawF03B11.XID_CUSTOM_84c76537583683,
          RawF03B11.XF4211_SDEXR1,
          RawF03B11.XF03B11_RPTXA1,
          RawF03B11.XF4211_SDDGL,
          RawF03B11.XF03B11_RPDSVJ,
          RawF03B11.XF4211_SDDCT,
          RawF03B11.XF4211_SDDOC,
          RawF03B11.XID_CUSTOM_8501fecff7aa51,
          RawF03B11.XF0116_ALADDS,
          RawF03B11.XF0116_ALCOUN,
          RawF03B11.XF4211_SDSHAN,
          RawF03B11.XF4211_SDAN8,
          RawF03B11.XF4211_SDPA8,
          RawF03B11.XF4211_SDDOCO,
          RawF03B11.XF4211_SDDCTO,
          RawF03B11.XC_F0101_ABSIC,
          RawF03B11.XF0101_ABSIC,
          RawF03B11.XF0006_MCRP20,
          RawF03B11.XF03B11_RPOKCO,
          RawF03B11.XF03B11_RPODOC,
          RawF03B11.XF03B11_RPODCT,
          RawF03B11.XID_CUSTOM_8501fc90c75a1
ORDER  BY F4211_SDMCU ASC,
          F4211_SDDOC ASC,
          F4211_SDAN8 ASC,
          F4211_SDPA8 ASC,
          F4211_SDSHAN ASC,
          F4211_SDDOCO ASC,
          F4211_SDKCO ASC,
          F4211_SDDCT ASC,
          F4211_SDDGL ASC,
          F4211_SDDCTO ASC,
          F03B11_RPODOC ASC,
          F03B11_RPODCT ASC,
          F03B11_RPOKCO ASC,
          F4211_SDEXR1 ASC,
          F03B11_RPTXA1 ASC,
          F0006_MCRP20 ASC,
          F0101_ABSIC ASC,
          F03B11_RPDSVJ ASC,
          F0116_ALADDS ASC,
          F0116_ALCOUN ASC,
          ID_CUSTOM_84c7652ecdb3913 ASC,
          ID_CUSTOM_8501fe576da8867 ASC,
          ID_CUSTOM_8501fca6bf83d39 ASC 