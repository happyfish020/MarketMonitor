WITH x AS (
  SELECT
    b.run_id,
    COUNT(*) AS n_days,
    SUM(b.exposed_flag) AS exposed_days,
    ROUND(SUM(b.exposed_flag)/COUNT(*),4) AS exposure_ratio,
    MAX(b.nav) AS nav_end,
    POWER(MAX(b.nav), 252/COUNT(*)) - 1 AS cagr_252,
    MIN(b.nav) AS nav_min,
    (1 - MIN(b.nav)/MAX(b.nav)) AS mdd_approx
  FROM SECOPR.CN_SECTOR_ROT_BT_DAILY_T b
  WHERE b.run_id LIKE 'SR_XP_SWEEP_V1%'
  GROUP BY b.run_id
),
a AS (
  SELECT
    p.run_id,
    SUM(CASE WHEN p.action='ENTER' THEN 1 ELSE 0 END) AS n_enter,
    SUM(CASE WHEN p.action='EXIT'  THEN 1 ELSE 0 END) AS n_exit,
    SUM(CASE WHEN p.action='KEEP'  THEN 1 ELSE 0 END) AS n_keep
  FROM SECOPR.CN_SECTOR_ROT_POS_DAILY_T p
  WHERE p.run_id LIKE 'SR_XP_SWEEP_V1%'
  GROUP BY p.run_id
)
SELECT
  x.run_id,
  x.exposure_ratio, x.exposed_days, x.nav_end, x.cagr_252,
  ROUND(x.mdd_approx,4) AS mdd_approx,
  a.n_enter, a.n_exit, a.n_keep,
  ROUND(a.n_keep / NULLIF(a.n_enter,0), 2) AS keep_per_enter
FROM x
LEFT JOIN a ON a.run_id = x.run_id
ORDER BY
  x.cagr_252 DESC, x.mdd_approx ASC;
