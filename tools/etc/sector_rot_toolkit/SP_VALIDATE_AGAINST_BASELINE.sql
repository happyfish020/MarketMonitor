
CREATE OR REPLACE PROCEDURE SECOPR.SP_VALIDATE_AGAINST_BASELINE (
    p_run_id IN VARCHAR2
) IS
    v_baseline_run VARCHAR2(128);
    v_base_nav     NUMBER;
    v_run_nav      NUMBER;
    v_base_exp     NUMBER;
    v_run_exp      NUMBER;
BEGIN
    SELECT run_id
      INTO v_baseline_run
      FROM SECOPR.CN_SECTOR_ROT_BASELINE_T
     WHERE baseline_key = 'DEFAULT_BASELINE';

    SELECT MAX(nav), SUM(exposed_flag)
      INTO v_base_nav, v_base_exp
      FROM SECOPR.CN_SECTOR_ROT_BT_DAILY_T
     WHERE run_id = v_baseline_run;

    SELECT MAX(nav), SUM(exposed_flag)
      INTO v_run_nav, v_run_exp
      FROM SECOPR.CN_SECTOR_ROT_BT_DAILY_T
     WHERE run_id = p_run_id;

    DBMS_OUTPUT.PUT_LINE('[BASELINE] '||v_baseline_run||' nav='||v_base_nav||' exposed='||v_base_exp);
    DBMS_OUTPUT.PUT_LINE('[RUN] '||p_run_id||' nav='||v_run_nav||' exposed='||v_run_exp);

    IF v_run_nav < v_base_nav * 0.9 THEN
        RAISE_APPLICATION_ERROR(-20101, 'Run underperforms baseline NAV');
    END IF;

    DBMS_OUTPUT.PUT_LINE('[VALIDATE_BASELINE] OK');
END;
/
