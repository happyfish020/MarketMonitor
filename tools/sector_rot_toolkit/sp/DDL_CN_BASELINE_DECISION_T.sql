
-- Decision table for Step 3b baseline compare & decide
-- Safe to run multiple times (drops are NOT performed).
CREATE TABLE SECOPR.CN_BASELINE_DECISION_T (
    baseline_run_id          VARCHAR2(128)   NOT NULL,
    candidate_run_id         VARCHAR2(128)   NOT NULL,
    decision                VARCHAR2(16)    NOT NULL,
    reason                  VARCHAR2(256)   NOT NULL,

    baseline_exposure_ratio  NUMBER,
    cand_exposure_ratio      NUMBER,
    baseline_nav_end         NUMBER,
    cand_nav_end             NUMBER,
    baseline_cagr_252        NUMBER,
    cand_cagr_252            NUMBER,
    baseline_mdd_approx      NUMBER,
    cand_mdd_approx          NUMBER,

    create_ts                TIMESTAMP(6)   DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE INDEX SECOPR.IX_BASELINE_DECISION_CAND_TS
ON SECOPR.CN_BASELINE_DECISION_T (candidate_run_id, create_ts);
