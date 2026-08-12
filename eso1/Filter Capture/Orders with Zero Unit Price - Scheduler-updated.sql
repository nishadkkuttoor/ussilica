SELECT PRICINGISSUE.SDDOCO                                                                          PRICINGISSUE_SDDOCO,
       PRICINGISSUE.SDDCTO                                                                          PRICINGISSUE_SDDCTO,
       PRICINGISSUE.SDLNID                                                                          PRICINGISSUE_SDLNID,
       PRICINGISSUE.SDMCU                                                                           PRICINGISSUE_SDMCU,
       PRICINGISSUE.SDAN8                                                                           PRICINGISSUE_SDAN8,
       PRICINGISSUE.CUSTNAME                                                                        PRICINGISSUE_CUSTNAME,
       PRICINGISSUE.SDSHAN                                                                          PRICINGISSUE_SDSHAN,
       PRICINGISSUE.SHIPTONAME                                                                      PRICINGISSUE_SHIPTONAME,
       PRICINGISSUE.SDLITM                                                                          PRICINGISSUE_SDLITM,
       PRICINGISSUE.SDCNID                                                                          PRICINGISSUE_SDCNID,
       PRICINGISSUE.SDNXTR                                                                          PRICINGISSUE_SDNXTR,
       PRICINGISSUE.SDSHPN                                                                          PRICINGISSUE_SDSHPN,
       PRICINGISSUE.SDUOM                                                                           PRICINGISSUE_SDUOM,
       PRICINGISSUE.PRUOM                                                                           PRICINGISSUE_PRUOM,
       PRICINGISSUE.PUOM                                                                            PRICINGISSUE_PUOM,
       ( TO_CHAR(PRICINGISSUE.ODATE, N'YYYY') - 1900 ) * 1000 + TO_CHAR(PRICINGISSUE.ODATE, N'DDD') PRICINGISSUE_ODATE,
       ( TO_CHAR(PRICINGISSUE.RDATE, N'YYYY') - 1900 ) * 1000 + TO_CHAR(PRICINGISSUE.RDATE, N'DDD') PRICINGISSUE_RDATE,
       ( TO_CHAR(PRICINGISSUE.PDATE, N'YYYY') - 1900 ) * 1000 + TO_CHAR(PRICINGISSUE.PDATE, N'DDD') PRICINGISSUE_PDATE,
       ( TO_CHAR(PRICINGISSUE.SDATE, N'YYYY') - 1900 ) * 1000 + TO_CHAR(PRICINGISSUE.SDATE, N'DDD') PRICINGISSUE_SDATE,
       PRICINGISSUE.UORG                                                                            PRICINGISSUE_UORG,
       PRICINGISSUE.REMARK                                                                          PRICINGISSUE_REMARK,
       PRICINGISSUE.FRTHDLCODE                                                                      PRICINGISSUE_FRTHDLCODE,
       PRICINGISSUE.SALESREP                                                                        PRICINGISSUE_SALESREP,
       PRICINGISSUE.SALESPERSON                                                                     PRICINGISSUE_SALESPERSON,
       PRICINGISSUE.PARENTNUM                                                                       PRICINGISSUE_PARENTNUM,
       PRICINGISSUE.PARENTNAME                                                                      PRICINGISSUE_PARENTNAME,
       MAX(FLOOR(TO_NUMBER(NULL)))                                                                  ReportColumn1
FROM   (select sddoco,
               sddcto,
               sdlnid,
               sdmcu,
               sdan8,
               (select abalph
                from   proddta.F0101
                where  aban8 = sdan8)                                     CustName,
               sdshan,
               (select abalph
                from   proddta.F0101
                where  aban8 = sdshan)                                    ShipToName,
               sdnxtr,
               sdshpn,
               sdlitm,
               sdcnid,
               sduom,
               (select IMUOM1
                from   proddta.F4101
                where  imlitm = sdlitm)                                   PUOM,
               (select IMUOM4
                from   proddta.F4101
                where  imlitm = sdlitm)                                   PRUOM,
               sduorg / 1000                                              uorg,
               TO_DATE(TO_CHAR(SdDRQJ + 1900000), 'YYYYDDD')              RDATE,
               TO_DATE(TO_CHAR(SdTRDJ + 1900000), 'YYYYDDD')              ODATE,
               TO_DATE(TO_CHAR(sdpefj + 1900000), 'YYYYDDD')              PDATE,
               CASE
                 WHEN SDADDJ = 0 THEN NULL
                 WHEN SDADDJ IS NULL THEN TO_DATE('1900001', 'YYYYDDD')
                 ELSE TO_DATE(TO_CHAR(SDADDJ + 1900000), 'YYYYDDD')
               END                                                        SDATE,
               sduprc,
               sdaexp,
               'Unit Price Zero'                                          Remark,
               TO_DATE(TO_CHAR(sdppdj + 1900000), 'YYYYDDD')              PSDATE,
               (select SN.abac05
                from   proddta.F0101 SN
                where  SN.aban8 = F4211.sdan8)                            SalesRep,
               (select F0005.drdl01
                from   prodctl.F0005
                where  F0005.drsy = '01'
                       and F0005.drrt = '05'
                       and F0005.drky in (select lpad(AN.abac05, 10, ' ')
                                          from   proddta.F0101 AN
                                          where  AN.aban8 = F4211.sdan8)) SalesPerson,
               F4211.SDFRTH                                               FRTHDLCODE,
               F4211.SDPA8                                                PARENTNUM,
               (select PN.abalph
                from   proddta.F0101 PN
                where  PN.aban8 = F4211.sdpa8)                            PARENTNAME
        from   proddta.F4211
        where  sduprc = 0
               and sdlnty = 'S'
               and sdco in ( '00640', '00645' )
               and sddcto in ( 'S1', 'SE', 'SZ' )
               and sdlttr <> 980
               and sdnxtr < 620
        UNION
        select U1.sddoco,
               U1.sddcto,
               U1.sdlnid,
               U1.sdmcu,
               U1.sdan8,
               (select abalph
                from   proddta.F0101
                where  aban8 = U1.sdan8)                               CustName,
               sdshan,
               (select abalph
                from   proddta.F0101
                where  aban8 = U1.sdshan)                              ShipToName,
               U1.sdnxtr,
               U1.sdshpn,
               U1.sdlitm,
               U1.sdcnid,
               U1.sduom,
               (select IMUOM1
                from   proddta.F4101
                where  imlitm = U1.sdlitm)                             PUOM,
               (select IMUOM4
                from   proddta.F4101
                where  imlitm = U1.sdlitm)                             PRUOM,
               U1.sduorg / 1000                                        uorg,
               TO_DATE(TO_CHAR(U1.SdDRQJ + 1900000), 'YYYYDDD')        RDATE,
               TO_DATE(TO_CHAR(U1.SdTRDJ + 1900000), 'YYYYDDD')        ODATE,
               TO_DATE(TO_CHAR(U1.sdpefj + 1900000), 'YYYYDDD')        PDATE,
               CASE
                 WHEN U1.SDADDJ = 0 THEN null
                 WHEN U1.SDADDJ IS NULL THEN TO_DATE('1900001', 'YYYYDDD')
                 ELSE TO_DATE(TO_CHAR(U1.SDADDJ + 1900000), 'YYYYDDD')
               END                                                     SDATE,
               U1.sduprc,
               U1.sdaexp,
               'No effective price'                                    Remark,
               TO_DATE(TO_CHAR(sdppdj + 1900000), 'YYYYDDD')           PSDATE,
               (select SN.abac05
                from   proddta.F0101 SN
                where  SN.aban8 = U1.sdan8)                            SalesRep,
               (select F0005.drdl01
                from   prodctl.F0005
                where  F0005.drsy = '01'
                       and F0005.drrt = '05'
                       and F0005.drky in (select lpad(AN.abac05, 10, ' ')
                                          from   proddta.F0101 AN
                                          where  AN.aban8 = U1.sdan8)) SalesPerson,
               U1.SDFRTH                                               FRTHDLCODE,
               U1.SDPA8                                                PARENTNUM,
               (select PN.abalph
                from   proddta.F0101 PN
                where  PN.aban8 = U1.sdpa8)                            PARENTNAME
        from   proddta.F4211 U1
        where  U1.sduprc <> 0
               and U1.sdlnty = 'S'
               and U1.sdco in ( '00640', '00645' )
               and U1.sddcto in ( 'S1', 'SE', 'SZ' )
               and U1.sdlttr <> 980
               and U1.sdaddj > 0
               and not exists (select '1'
                               from   proddta.F4106
                               where  bplitm = U1.sdlitm
                                      and bpmcu = U1.sdmcu
                                      and bpan8 = U1.sdshan
                                      and bpeftj <= U1.sdaddj
                                      and bpexdj >= U1.sdaddj
                                      and bpuprc <> 0)) PRICINGISSUE
GROUP  BY PRICINGISSUE.SDDOCO,
          PRICINGISSUE.SDDCTO,
          PRICINGISSUE.SDLNID,
          PRICINGISSUE.SDMCU,
          PRICINGISSUE.SDAN8,
          PRICINGISSUE.CUSTNAME,
          PRICINGISSUE.SDSHAN,
          PRICINGISSUE.SHIPTONAME,
          PRICINGISSUE.SDLITM,
          PRICINGISSUE.SDCNID,
          PRICINGISSUE.SDNXTR,
          PRICINGISSUE.SDSHPN,
          PRICINGISSUE.SDUOM,
          PRICINGISSUE.PRUOM,
          PRICINGISSUE.PUOM,
          ( TO_CHAR(PRICINGISSUE.ODATE, N'YYYY') - 1900 ) * 1000 + TO_CHAR(PRICINGISSUE.ODATE, N'DDD'),
          ( TO_CHAR(PRICINGISSUE.RDATE, N'YYYY') - 1900 ) * 1000 + TO_CHAR(PRICINGISSUE.RDATE, N'DDD'),
          ( TO_CHAR(PRICINGISSUE.PDATE, N'YYYY') - 1900 ) * 1000 + TO_CHAR(PRICINGISSUE.PDATE, N'DDD'),
          ( TO_CHAR(PRICINGISSUE.SDATE, N'YYYY') - 1900 ) * 1000 + TO_CHAR(PRICINGISSUE.SDATE, N'DDD'),
          PRICINGISSUE.UORG,
          PRICINGISSUE.REMARK,
          PRICINGISSUE.FRTHDLCODE,
          PRICINGISSUE.SALESREP,
          PRICINGISSUE.SALESPERSON,
          PRICINGISSUE.PARENTNUM,
          PRICINGISSUE.PARENTNAME
ORDER  BY PRICINGISSUE_ODATE ASC,
          PRICINGISSUE_SDCNID ASC,
          PRICINGISSUE_SDDCTO ASC,
          PRICINGISSUE_SDDOCO ASC,
          PRICINGISSUE_SDLITM ASC,
          PRICINGISSUE_SDMCU ASC,
          PRICINGISSUE_SDNXTR ASC,
          PRICINGISSUE_SDSHPN ASC,
          PRICINGISSUE_SDUOM ASC,
          PRICINGISSUE_PDATE ASC,
          PRICINGISSUE_SDATE ASC,
          PRICINGISSUE_UORG ASC,
          PRICINGISSUE_REMARK ASC,
          PRICINGISSUE_SDAN8 ASC,
          PRICINGISSUE_CUSTNAME ASC,
          PRICINGISSUE_SDSHAN ASC,
          PRICINGISSUE_SHIPTONAME ASC,
          PRICINGISSUE_PRUOM ASC,
          PRICINGISSUE_PUOM ASC,
          PRICINGISSUE_RDATE ASC,
          PRICINGISSUE_SDLNID ASC,
          PRICINGISSUE_FRTHDLCODE ASC,
          PRICINGISSUE_SALESREP ASC,
          PRICINGISSUE_SALESPERSON ASC,
          PRICINGISSUE_PARENTNUM ASC,
          PRICINGISSUE_PARENTNAME ASC 