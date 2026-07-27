SELECT
  ONESHIPMENTWITHMU95e816f3.SDDCTO ONESHIPMENTWITHMU4ef2c7bb,
  ONESHIPMENTWITHMU95e816f3.SDSHPN ONESHIPMENTWITHMU33d037c,
  ONESHIPMENTWITHMU95e816f3.DOCO ONESHIPMENTWITHMU753a278f,
  MAX(FLOOR(TO_NUMBER (NULL))) ReportColumn1
FROM
  (
    select
      sdshpn,
      sddcto,
      DOCO
    from
      (
        select
          sdshpn,
          sddcto,
          count(distinct sddoco) DOCO
        from
          PRODDTA.F4211
        where
          sdkcoo in ('00640', '00645')
          and sdlttr <> 980
          and sddcto in ('S1', 'SE', 'SZ', 'SM', 'SO')
          and sdnxtr < 620
        group by
          sdshpn,
          sddcto
      )
    where
      DOCO > 1
  ) ONESHIPMENTWITHMU95e816f3
GROUP BY
  ONESHIPMENTWITHMU95e816f3.SDDCTO,
  ONESHIPMENTWITHMU95e816f3.SDSHPN,
  ONESHIPMENTWITHMU95e816f3.DOCO
ORDER BY
  ONESHIPMENTWITHMU753a278f ASC,
  ONESHIPMENTWITHMU4ef2c7bb ASC,
  ONESHIPMENTWITHMU33d037c ASC