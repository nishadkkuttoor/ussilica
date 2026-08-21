SELECT
  MULTIPLESHIPMENTS.SDDOCO MULTIPLESHIPMENTS_SDDOCO,
  MULTIPLESHIPMENTS.SDDCTO MULTIPLESHIPMENTS_SDDCTO,
  MULTIPLESHIPMENTS.AN8 MULTIPLESHIPMENTS_AN8,
  MULTIPLESHIPMENTS.BILLTONAME MULTIPLESHIPMENTS90198eff,
  MULTIPLESHIPMENTS.SHAN MULTIPLESHIPMENTS_SHAN,
  MULTIPLESHIPMENTS.SHIPTONAME MULTIPLESHIPMENTS6498ecea,
  MULTIPLESHIPMENTS.SHPN MULTIPLESHIPMENTS_SHPN,
  MAX(FLOOR(TO_NUMBER (NULL))) ReportColumn1
FROM
  (
    select
      sddoco,
      sddcto,
      (
        select
          F4201.shshan
        from
          proddta.F4201
        where
          shdoco = sddoco
          and shdcto = sddcto
      ) SHAN,
      (
        select
          F0101.abalph
        from
          proddta.F0101,
          proddta.F4201
        where
          F4201.shdoco = sddoco
          and F4201.shdcto = sddcto
          and F0101.aban8 = F4201.shshan
      ) ShipToName,
      (
        select
          F4201.shan8
        from
          proddta.F4201
        where
          shdoco = sddoco
          and F4201.shdcto = sddcto
      ) AN8,
      (
        select
          F0101.abalph
        from
          proddta.F0101,
          proddta.F4201
        where
          F4201.shdoco = sddoco
          and F4201.shdcto = sddcto
          and F0101.aban8 = F4201.shan8
      ) BillToName,
      SHPN
    from
      (
        select
          sddoco,
          sddcto,
          count(distinct sdshpn) SHPN
        from
          PRODDTA.F4211
        where
          sdkcoo in ('00640', '00645')
          and sdlttr <> 980
          and sddcto in ('S1', 'SE', 'SZ', 'SM', 'SO')
          and sdnxtr < 620
        group by
          sddoco,
          sddcto
      )
    where
      SHPN > 1
  ) MULTIPLESHIPMENTS
GROUP BY
  MULTIPLESHIPMENTS.SDDOCO,
  MULTIPLESHIPMENTS.SDDCTO,
  MULTIPLESHIPMENTS.AN8,
  MULTIPLESHIPMENTS.BILLTONAME,
  MULTIPLESHIPMENTS.SHAN,
  MULTIPLESHIPMENTS.SHIPTONAME,
  MULTIPLESHIPMENTS.SHPN
ORDER BY
  MULTIPLESHIPMENTS_SDDCTO ASC,
  MULTIPLESHIPMENTS_SDDOCO ASC,
  MULTIPLESHIPMENTS_SHPN ASC,
  MULTIPLESHIPMENTS_AN8 ASC,
  MULTIPLESHIPMENTS90198eff ASC,
  MULTIPLESHIPMENTS_SHAN ASC,
  MULTIPLESHIPMENTS6498ecea ASC