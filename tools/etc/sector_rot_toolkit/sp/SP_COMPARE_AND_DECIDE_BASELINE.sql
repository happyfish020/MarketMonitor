
CREATE OR REPLACE PROCEDURE SECOPR.SP_COMPARE_AND_DECIDE_BASELINE (
    p_candidate_run_id IN VARCHAR2
) IS
    v_baseline_run_id   VARCHAR2(128);

    v_b_n_days          NUMBER;
    v_c_n_days          NUMBER;

    v_b_exposed_days    NUMBER;
    v_c_exposed_days    NUMBER;

    v_b_exposure_ratio  NUMBER;
    v_c_exposure_ratio  NUMBER;

    v_b_nav_end         NUMBER;
    v_c_nav_end         NUMBER;

    v_b_nav_min         NUMBER;
    v_c_nav_min         NUMBER;

    v_b_cagr_252        NUMBER;
    v_c_cagr_252        NUMBER;

    v_b_mdd_approx      NUMBER;
    v_c_mdd_approx      NUMBER;

    v_decision          VARCHAR2(16);
    v_reason            VARCHAR2(256);
BEGIN
    -- 1) Read pinned baseline (frozen convention)
    SELECT run_id
      INTO v_baseline_run_id
      FROM SECOPR.CN_BASELINE_REGISTRY_T
     WHERE baseline_key = 'DEFAULT_BASELINE';

    -- 2) Compute baseline metrics (SQL facts)
    SELECT
        COUNT(*) AS n_days,
        NVL(SUM(exposed_flag),0) AS exposed_days,
        ROUND(NVL(SUM(exposed_flag),0)/COUNT(*), 6) AS exposure_ratio,
        MAX(nav) AS nav_end,
        MIN(nav) AS nav_min
    INTO
        v_b_n_days, v_b_exposed_days, v_b_exposure_ratio, v_b_nav_end, v_b_nav_min
    FROM SECOPR.CN_SECTOR_ROT_BT_DAILY_T
    WHERE run_id = v_baseline_run_id;

    v_b_cagr_252 := POWER(v_b_nav_end, 252 / v_b_n_days) - 1;
    v_b_mdd_approx := (1 - v_b_nav_min / NULLIF(v_b_nav_end,0));

    -- 3) Compute candidate metrics (SQL facts)
    SELECT
        COUNT(*) AS n_days,
        NVL(SUM(exposed_flag),0) AS exposed_days,
        ROUND(NVL(SUM(exposed_flag),0)/COUNT(*), 6) AS exposure_ratio,
        MAX(nav) AS nav_end,
        MIN(nav) AS nav_min
    INTO
        v_c_n_days, v_c_exposed_days, v_c_exposure_ratio, v_c_nav_end, v_c_nav_min
    FROM SECOPR.CN_SECTOR_ROT_BT_DAILY_T
    WHERE run_id = p_candidate_run_id;

    v_c_cagr_252 := POWER(v_c_nav_end, 252 / v_c_n_days) - 1;
    v_c_mdd_approx := (1 - v_c_nav_min / NULLIF(v_c_nav_end,0));

    -- 4) Decide (P1 conservative rule, centralized here)
    -- PASS if:
    -- - CAGR not materially worse than baseline (>= 98% of baseline)
    -- - MDD not materially worse (<= 105% of baseline)
    -- - Exposure not materially lower (>= baseline - 0.03)
    IF v_c_cagr_252 < v_b_cagr_252 * 0.98 THEN
        v_decision := 'REJECT';
        v_reason := 'CAGR_TOO_LOW';
    ELSIF v_c_mdd_approx > v_b_mdd_approx * 1.05 THEN
        v_decision := 'REJECT';
        v_reason := 'MDD_TOO_HIGH';
    ELSIF v_c_exposure_ratio < v_b_exposure_ratio - 0.03 THEN
        v_decision := 'REJECT';
        v_reason := 'EXPOSURE_TOO_LOW';
    ELSE
        v_decision := 'PASS';
        v_reason := 'OK';
    END IF;

    -- 5) Persist decision
    INSERT INTO SECOPR.CN_BASELINE_DECISION_T (
        baseline_run_id, candidate_run_id, decision, reason,
        baseline_exposure_ratio, cand_exposure_ratio,
        baseline_nav_end, cand_nav_end,
        baseline_cagr_252, cand_cagr_252,
        baseline_mdd_approx, cand_mdd_approx
    ) VALUES (
        v_baseline_run_id, p_candidate_run_id, v_decision, v_reason,
        v_b_exposure_ratio, v_c_exposure_ratio,
        v_b_nav_end, v_c_nav_end,
        v_b_cagr_252, v_c_cagr_252,
        v_b_mdd_approx, v_c_mdd_approx
    );

    COMMIT;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RAISE_APPLICATION_ERROR(-20031, 'Step3b failed: baseline not pinned or run not found. candidate='||p_candidate_run_id);
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/
