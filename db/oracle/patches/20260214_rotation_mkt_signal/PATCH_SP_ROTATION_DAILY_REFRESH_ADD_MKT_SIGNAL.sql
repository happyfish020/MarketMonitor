-- UnifiedRisk V12 / SECOPR
-- Patch: SP_ROTATION_DAILY_REFRESH
-- Add: SP_BUILD_SECTOR_ROTATION_MKT_SIGNAL_LATEST (facts overlay)

CREATE OR REPLACE PROCEDURE SECOPR.SP_ROTATION_DAILY_REFRESH(
    p_run_id         IN VARCHAR2,
    p_trade_date     IN DATE,
    p_force          IN NUMBER DEFAULT 0,
    p_refresh_energy IN NUMBER DEFAULT 1
) AS
    v_dt DATE;
    v_entry NUMBER;
    v_holding NUMBER;
    v_exit NUMBER;
BEGIN
    IF p_trade_date IS NULL THEN
        SELECT MAX(trade_date) INTO v_dt
        FROM SECOPR.CN_STOCK_DAILY_PRICE;
    ELSE
        v_dt := p_trade_date;
    END IF;

    IF v_dt IS NULL THEN
        RAISE_APPLICATION_ERROR(-20001, 'SP_ROTATION_DAILY_REFRESH: trade_date is NULL (CN_STOCK_DAILY_PRICE empty?)');
    END IF;

    IF p_run_id IS NULL THEN
        RAISE_APPLICATION_ERROR(-20002, 'SP_ROTATION_DAILY_REFRESH: p_run_id is NULL');
    END IF;

    -- 1) Ensure energy available
    IF NVL(p_refresh_energy, 1) = 1 THEN
        SECOPR.SP_BACKFILL_SECTOR_ENERGY_SNAP(v_dt, 0);
    END IF;

    -- 2) Build ranked + latest signal (latest-only SPs)
    SECOPR.SP_BUILD_SECTOR_ROTATION_RANKED_LATEST;
    SECOPR.SP_BUILD_SECTOR_ROTATION_SIGNAL_LATEST;

    -- 2b) Build Market Rotation Facts overlay (separated from strategy)
    SECOPR.SP_BUILD_SECTOR_ROTATION_MKT_SIGNAL_LATEST;

    -- 3) Ensure BT day axis exists for run_id
    SECOPR.SP_BACKFILL_ROT_BT_FROM_PRICE(p_run_id, v_dt);

    -- 4) Refresh snapshots
    SECOPR.SP_REFRESH_ROTATION_SNAP_ALL(p_run_id, v_dt, NVL(p_force, 0));

    -- 5) Minimal audit: each snapshot must have >=1 row for the day (summary or details)
    SELECT COUNT(*) INTO v_entry
    FROM SECOPR.CN_ROTATION_ENTRY_SNAP_T
    WHERE run_id = p_run_id AND trade_date = v_dt;

    SELECT COUNT(*) INTO v_holding
    FROM SECOPR.CN_ROTATION_HOLDING_SNAP_T
    WHERE run_id = p_run_id AND trade_date = v_dt;

    SELECT COUNT(*) INTO v_exit
    FROM SECOPR.CN_ROTATION_EXIT_SNAP_T
    WHERE run_id = p_run_id AND trade_date = v_dt;

    IF v_entry = 0 OR v_holding = 0 OR v_exit = 0 THEN
        RAISE_APPLICATION_ERROR(
            -20003,
            'SP_ROTATION_DAILY_REFRESH: snapshot missing rows. entry='||v_entry||', holding='||v_holding||', exit='||v_exit
        );
    END IF;

    COMMIT;
END;
/
