-- UnifiedRisk V12 / SECOPR
-- Create or replace: Market Rotation Signal builder (事实层)
-- IMPORTANT: This is NOT the backtest/strategy signal.
-- It is a more sensitive overlay designed to reflect sector rotation reality.

CREATE OR REPLACE PROCEDURE SECOPR.SP_BUILD_SECTOR_ROTATION_MKT_SIGNAL_LATEST AS
  v_run_id VARCHAR2(64);
  v_dt     DATE;
BEGIN
  -- 1) active baseline -> run_id
  SELECT run_id INTO v_run_id
    FROM SECOPR.CN_BASELINE_REGISTRY_T
   WHERE baseline_key='DEFAULT_BASELINE'
     AND is_active=1;

  -- 2) latest asof date from BT (rotation execution calendar)
  SELECT MAX(trade_date) INTO v_dt
    FROM SECOPR.CN_SECTOR_ROT_BT_DAILY_T
   WHERE run_id = v_run_id;

  IF v_dt IS NULL THEN
    RETURN;
  END IF;

  -- 3) idempotent: rebuild for the day
  DELETE FROM SECOPR.CN_SECTOR_ROTATION_MKT_SIGNAL_T
   WHERE run_id = v_run_id
     AND signal_date = v_dt;

  /*
    Facts overlay logic (frozen v1):

    ENTER (facts):
      - state in ('CONFIRM','TREND','START_IGNITE','IGNITE')
      - tier in ('S','A','B')
      - up_ma5 >= 0.60  (breadth diffusion)
      - amt_impulse >= 1.05 (liquidity support)
      - score high, take TopN by theme_rank then score

    WATCH:
      - state in ('OBSERVE','CONFIRM','START_IGNITE','IGNITE')
      - tier in ('B','C')
      - up_ma5 >= 0.55

    EXIT (cooling):
      - transition hints weakening, OR tier down to ('C','D','E') with negative/weak momentum proxies

    Notes:
    - This procedure relies on CN_SECTOR_ROTATION_TRANSITION_V as the "current rotation state" view.
    - Thresholds are intentionally softer than strategy signal.
  */

  -- ENTER candidates
  INSERT INTO SECOPR.CN_SECTOR_ROTATION_MKT_SIGNAL_T (
      run_id, signal_date, sector_type, sector_id, sector_name,
      action, signal_score, strength_tag, reasons_json, created_at
  )
  SELECT
      v_run_id,
      v_dt,
      t.sector_type,
      t.sector_id,
      t.sector_name,
      'ENTER' AS action,
      t.score AS signal_score,
      CASE
        WHEN t.tier IN ('S') THEN 'STRONG'
        WHEN t.tier IN ('A') THEN 'GOOD'
        ELSE 'OK'
      END AS strength_tag,
      '{"state":"' || NVL(t.state,'') || '",' ||
      '"tier":"' || NVL(t.tier,'') || '",' ||
      '"theme_group":"' || NVL(t.theme_group,'') || '",' ||
      '"theme_rank":' || NVL(TO_CHAR(t.theme_rank),'null') || ',' ||
      '"score":' || NVL(TO_CHAR(t.score),'null') || ',' ||
      '"confirm_streak":' || NVL(TO_CHAR(t.confirm_streak),'null') || ',' ||
      '"amt_impulse":' || NVL(TO_CHAR(t.amt_impulse),'null') || ',' ||
      '"up_ma5":' || NVL(TO_CHAR(t.up_ma5),'null') ||
      '}' AS reasons_json,
      SYSTIMESTAMP
  FROM (
      SELECT
          x.*,
          ROW_NUMBER() OVER (
              ORDER BY
                CASE WHEN x.theme_rank IS NULL THEN 999 ELSE x.theme_rank END,
                x.score DESC NULLS LAST,
                x.confirm_streak DESC NULLS LAST
          ) AS rn
      FROM SECOPR.CN_SECTOR_ROTATION_TRANSITION_V x
      WHERE x.trade_date = v_dt
        AND x.state IN ('CONFIRM','TREND','START_IGNITE','IGNITE')
        AND x.tier  IN ('S','A','B')
        AND NVL(x.up_ma5, 0) >= 0.60
        AND NVL(x.amt_impulse, 0) >= 1.05
  ) t
  WHERE t.rn <= 10;

  -- WATCH candidates (avoid duplicates with ENTER)
  INSERT INTO SECOPR.CN_SECTOR_ROTATION_MKT_SIGNAL_T (
      run_id, signal_date, sector_type, sector_id, sector_name,
      action, signal_score, strength_tag, reasons_json, created_at
  )
  SELECT
      v_run_id,
      v_dt,
      t.sector_type,
      t.sector_id,
      t.sector_name,
      'WATCH' AS action,
      t.score AS signal_score,
      'WATCH' AS strength_tag,
      '{"state":"' || NVL(t.state,'') || '",' ||
      '"tier":"' || NVL(t.tier,'') || '",' ||
      '"theme_group":"' || NVL(t.theme_group,'') || '",' ||
      '"theme_rank":' || NVL(TO_CHAR(t.theme_rank),'null') || ',' ||
      '"score":' || NVL(TO_CHAR(t.score),'null') || ',' ||
      '"amt_impulse":' || NVL(TO_CHAR(t.amt_impulse),'null') || ',' ||
      '"up_ma5":' || NVL(TO_CHAR(t.up_ma5),'null') ||
      '}' AS reasons_json,
      SYSTIMESTAMP
  FROM (
      SELECT
          x.*,
          ROW_NUMBER() OVER (
              ORDER BY
                CASE WHEN x.theme_rank IS NULL THEN 999 ELSE x.theme_rank END,
                x.score DESC NULLS LAST
          ) AS rn
      FROM SECOPR.CN_SECTOR_ROTATION_TRANSITION_V x
      WHERE x.trade_date = v_dt
        AND x.state IN ('OBSERVE','CONFIRM','START_IGNITE','IGNITE')
        AND x.tier  IN ('B','C')
        AND NVL(x.up_ma5, 0) >= 0.55
  ) t
  WHERE t.rn <= 15
    AND NOT EXISTS (
        SELECT 1
          FROM SECOPR.CN_SECTOR_ROTATION_MKT_SIGNAL_T m
         WHERE m.run_id = v_run_id
           AND m.signal_date = v_dt
           AND m.sector_type = t.sector_type
           AND m.sector_id   = t.sector_id
           AND m.action      = 'ENTER'
    );

  -- EXIT (cooling list): sectors that are weakening today
  INSERT INTO SECOPR.CN_SECTOR_ROTATION_MKT_SIGNAL_T (
      run_id, signal_date, sector_type, sector_id, sector_name,
      action, signal_score, strength_tag, reasons_json, created_at
  )
  SELECT
      v_run_id,
      v_dt,
      t.sector_type,
      t.sector_id,
      t.sector_name,
      'EXIT' AS action,
      t.score AS signal_score,
      'COOLING' AS strength_tag,
      '{"state":"' || NVL(t.state,'') || '",' ||
      '"tier":"' || NVL(t.tier,'') || '",' ||
      '"transition":"' || NVL(t.transition,'') || '",' ||
      '"score":' || NVL(TO_CHAR(t.score),'null') || ',' ||
      '"amt_impulse":' || NVL(TO_CHAR(t.amt_impulse),'null') || ',' ||
      '"up_ma5":' || NVL(TO_CHAR(t.up_ma5),'null') ||
      '}' AS reasons_json,
      SYSTIMESTAMP
  FROM (
      SELECT
          x.*,
          ROW_NUMBER() OVER (ORDER BY x.score ASC NULLS LAST, x.up_ma5 ASC NULLS LAST) AS rn
      FROM SECOPR.CN_SECTOR_ROTATION_TRANSITION_V x
      WHERE x.trade_date = v_dt
        AND (
              x.transition IN ('TREND_TO_WEAK','CONFIRM_TO_WEAK','WEAK_TO_EXIT','EXIT')
           OR (x.tier IN ('C','D','E') AND (NVL(x.up_ma5,0) < 0.50 OR NVL(x.amt_impulse,0) < 0.95))
        )
  ) t
  WHERE t.rn <= 20
    AND NOT EXISTS (
        SELECT 1
          FROM SECOPR.CN_SECTOR_ROTATION_MKT_SIGNAL_T m
         WHERE m.run_id = v_run_id
           AND m.signal_date = v_dt
           AND m.sector_type = t.sector_type
           AND m.sector_id   = t.sector_id
           AND m.action IN ('ENTER','WATCH')
    );

  COMMIT;
END;
/
