-- =============================================================================
-- Risk Management System — Report Generator Stored Procedures
-- =============================================================================
-- Contains all report generation stored procedures:
--   1. GENERATE_EXPOSURE_BY_PARTICIPANT(p_run_id)
--   2. GENERATE_CONCENTRATION_REPORT(p_run_id)
--   3. GENERATE_HIGH_VOLUME_CUSIP_REPORT(p_run_id)
--   4. GENERATE_HIGH_EXPOSURE_PARTICIPANT_REPORT(p_run_id)
--   5. GENERATE_TOP_CUSIPS_REPORT(p_run_id)
--   6. GENERATE_TOP_PARTICIPANTS_REPORT(p_run_id)
--   7. GENERATE_REPORTS(p_run_id) — master orchestrator
--
-- Requirements: 5.1–5.3, 6.1–6.3, 7.1–7.2, 8.1–8.2, 9.1–9.2, 10.1–10.2
-- =============================================================================

USE DATABASE RISK_MANAGEMENT;
USE SCHEMA RISK_MANAGEMENT.CORE;

-- =============================================================================
-- 1. GENERATE_EXPOSURE_BY_PARTICIPANT
-- =============================================================================
-- Aggregates VaR by participant for a given run_id. Joins with ENTITY_MASTER
-- (is_current=TRUE) for participant_name. Computes total_var_exposure,
-- open_trade_count, pct_of_total_var. Sorted descending by total_var_exposure
-- with rank assigned.
--
-- Requirements: 5.1, 5.2, 5.3
-- =============================================================================
CREATE OR REPLACE PROCEDURE GENERATE_EXPOSURE_BY_PARTICIPANT(p_run_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Remove any previous results for this run_id (idempotent)
    DELETE FROM REPORT_EXPOSURE_BY_PARTICIPANT WHERE run_id = :p_run_id;

    INSERT INTO REPORT_EXPOSURE_BY_PARTICIPANT (
        run_id,
        participant_id,
        participant_name,
        total_var_exposure,
        open_trade_count,
        pct_of_total_var,
        rank
    )
    WITH participant_agg AS (
        SELECT
            vr.participant_id,
            SUM(vr.var_exposure)  AS total_var_exposure,
            SUM(vr.trade_count)  AS open_trade_count
        FROM VAR_RESULTS vr
        WHERE vr.run_id = :p_run_id
        GROUP BY vr.participant_id
    ),
    grand_total AS (
        SELECT COALESCE(SUM(total_var_exposure), 0) AS portfolio_var
        FROM participant_agg
    )
    SELECT
        :p_run_id                                                       AS run_id,
        pa.participant_id,
        COALESCE(em.participant_name, pa.participant_id)                AS participant_name,
        pa.total_var_exposure,
        pa.open_trade_count,
        CASE
            WHEN gt.portfolio_var > 0
            THEN ROUND(pa.total_var_exposure / gt.portfolio_var * 100, 4)
            ELSE 0
        END                                                             AS pct_of_total_var,
        ROW_NUMBER() OVER (ORDER BY pa.total_var_exposure DESC)         AS rank
    FROM participant_agg pa
    CROSS JOIN grand_total gt
    LEFT JOIN ENTITY_MASTER em
        ON pa.participant_id = em.participant_id AND em.is_current = TRUE
    ORDER BY pa.total_var_exposure DESC;

    RETURN 'GENERATE_EXPOSURE_BY_PARTICIPANT completed for run_id=' || :p_run_id;
END;
$$;


-- =============================================================================
-- 2. GENERATE_CONCENTRATION_REPORT
-- =============================================================================
-- Calculates % of total VaR per CUSIP and per Participant. Flags entries where
-- concentration_pct > 10. Inserts into REPORT_CONCENTRATION with entity_type
-- of 'CUSIP' or 'Participant'.
--
-- Requirements: 6.1, 6.2, 6.3
-- =============================================================================
CREATE OR REPLACE PROCEDURE GENERATE_CONCENTRATION_REPORT(p_run_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Remove any previous results for this run_id (idempotent)
    DELETE FROM REPORT_CONCENTRATION WHERE run_id = :p_run_id;

    -- Insert CUSIP-level concentration
    INSERT INTO REPORT_CONCENTRATION (
        run_id,
        entity_type,
        entity_id,
        entity_name,
        concentration_pct,
        is_flagged
    )
    WITH cusip_agg AS (
        SELECT
            vr.cusip                       AS entity_id,
            SUM(vr.var_exposure)           AS entity_var
        FROM VAR_RESULTS vr
        WHERE vr.run_id = :p_run_id
        GROUP BY vr.cusip
    ),
    grand_total AS (
        SELECT COALESCE(SUM(entity_var), 0) AS portfolio_var
        FROM cusip_agg
    )
    SELECT
        :p_run_id                                                       AS run_id,
        'CUSIP'                                                         AS entity_type,
        ca.entity_id,
        COALESCE(sm.security_name, ca.entity_id)                        AS entity_name,
        CASE
            WHEN gt.portfolio_var > 0
            THEN ROUND(ca.entity_var / gt.portfolio_var * 100, 4)
            ELSE 0
        END                                                             AS concentration_pct,
        CASE
            WHEN gt.portfolio_var > 0
                 AND ROUND(ca.entity_var / gt.portfolio_var * 100, 4) > 10
            THEN TRUE
            ELSE FALSE
        END                                                             AS is_flagged
    FROM cusip_agg ca
    CROSS JOIN grand_total gt
    LEFT JOIN SECURITY_MASTER sm
        ON ca.entity_id = sm.cusip AND sm.is_current = TRUE;

    -- Insert Participant-level concentration
    INSERT INTO REPORT_CONCENTRATION (
        run_id,
        entity_type,
        entity_id,
        entity_name,
        concentration_pct,
        is_flagged
    )
    WITH participant_agg AS (
        SELECT
            vr.participant_id              AS entity_id,
            SUM(vr.var_exposure)           AS entity_var
        FROM VAR_RESULTS vr
        WHERE vr.run_id = :p_run_id
        GROUP BY vr.participant_id
    ),
    grand_total AS (
        SELECT COALESCE(SUM(entity_var), 0) AS portfolio_var
        FROM participant_agg
    )
    SELECT
        :p_run_id                                                       AS run_id,
        'Participant'                                                   AS entity_type,
        pa.entity_id,
        COALESCE(em.participant_name, pa.entity_id)                     AS entity_name,
        CASE
            WHEN gt.portfolio_var > 0
            THEN ROUND(pa.entity_var / gt.portfolio_var * 100, 4)
            ELSE 0
        END                                                             AS concentration_pct,
        CASE
            WHEN gt.portfolio_var > 0
                 AND ROUND(pa.entity_var / gt.portfolio_var * 100, 4) > 10
            THEN TRUE
            ELSE FALSE
        END                                                             AS is_flagged
    FROM participant_agg pa
    CROSS JOIN grand_total gt
    LEFT JOIN ENTITY_MASTER em
        ON pa.entity_id = em.participant_id AND em.is_current = TRUE;

    RETURN 'GENERATE_CONCENTRATION_REPORT completed for run_id=' || :p_run_id;
END;
$$;


-- =============================================================================
-- 3. GENERATE_HIGH_VOLUME_CUSIP_REPORT
-- =============================================================================
-- Compares current trade volume (from TRADES for this run's trades) to the
-- 90th percentile of historical daily volumes. Inserts into
-- REPORT_HIGH_VOLUME_CUSIP.
--
-- Requirements: 7.1, 7.2
-- =============================================================================
CREATE OR REPLACE PROCEDURE GENERATE_HIGH_VOLUME_CUSIP_REPORT(p_run_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Remove any previous results for this run_id (idempotent)
    DELETE FROM REPORT_HIGH_VOLUME_CUSIP WHERE run_id = :p_run_id;

    INSERT INTO REPORT_HIGH_VOLUME_CUSIP (
        run_id,
        cusip,
        current_volume,
        historical_avg_volume,
        percentile_rank
    )
    WITH run_timestamp AS (
        -- Get the run_timestamp for this run to identify the current window
        SELECT MIN(run_timestamp) AS ts
        FROM VAR_RESULTS
        WHERE run_id = :p_run_id
    ),
    -- Current trade volume per CUSIP: trades that are part of this VaR run
    -- (CUSIPs present in VAR_RESULTS for this run_id)
    current_volumes AS (
        SELECT
            t.cusip,
            SUM(t.quantity) AS current_volume
        FROM TRADES t
        WHERE t.cusip IN (
            SELECT DISTINCT cusip FROM VAR_RESULTS WHERE run_id = :p_run_id
        )
        AND t.trade_timestamp::DATE = (SELECT ts::DATE FROM run_timestamp)
        GROUP BY t.cusip
    ),
    -- Historical daily volumes per CUSIP (excluding current day)
    historical_daily AS (
        SELECT
            t.cusip,
            t.trade_timestamp::DATE AS trade_date,
            SUM(t.quantity)         AS daily_volume
        FROM TRADES t
        WHERE t.trade_timestamp::DATE < (SELECT ts::DATE FROM run_timestamp)
        GROUP BY t.cusip, t.trade_timestamp::DATE
    ),
    -- 90th percentile and average of historical daily volumes per CUSIP
    historical_stats AS (
        SELECT
            cusip,
            AVG(daily_volume)                                                    AS avg_volume,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY daily_volume)           AS p90_volume
        FROM historical_daily
        GROUP BY cusip
    ),
    -- Compute percentile rank of current volume within historical distribution
    ranked AS (
        SELECT
            cv.cusip,
            cv.current_volume,
            COALESCE(hs.avg_volume, 0)   AS historical_avg_volume,
            COALESCE(hs.p90_volume, 0)   AS p90_volume,
            -- Percentile rank: fraction of historical days where volume < current
            COALESCE(
                (SELECT COUNT(*)
                 FROM historical_daily hd
                 WHERE hd.cusip = cv.cusip AND hd.daily_volume < cv.current_volume
                ) * 1.0
                / NULLIF(
                    (SELECT COUNT(*) FROM historical_daily hd WHERE hd.cusip = cv.cusip),
                    0
                ) * 100,
                100
            ) AS percentile_rank
        FROM current_volumes cv
        LEFT JOIN historical_stats hs ON cv.cusip = hs.cusip
    )
    SELECT
        :p_run_id           AS run_id,
        r.cusip,
        r.current_volume,
        r.historical_avg_volume,
        r.percentile_rank
    FROM ranked r
    WHERE r.current_volume > r.p90_volume
       OR r.p90_volume = 0;  -- If no history, flag as high-volume

    RETURN 'GENERATE_HIGH_VOLUME_CUSIP_REPORT completed for run_id=' || :p_run_id;
END;
$$;


-- =============================================================================
-- 4. GENERATE_HIGH_EXPOSURE_PARTICIPANT_REPORT
-- =============================================================================
-- Identifies participants exceeding a configurable threshold (default: top 10%
-- by VaR exposure). Inserts into REPORT_HIGH_EXPOSURE_PARTICIPANT.
--
-- Requirements: 8.1, 8.2
-- =============================================================================
CREATE OR REPLACE PROCEDURE GENERATE_HIGH_EXPOSURE_PARTICIPANT_REPORT(p_run_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_threshold_pct NUMBER(5,2) DEFAULT 90;  -- top 10% means >= 90th percentile
BEGIN
    -- Remove any previous results for this run_id (idempotent)
    DELETE FROM REPORT_HIGH_EXPOSURE_PARTICIPANT WHERE run_id = :p_run_id;

    INSERT INTO REPORT_HIGH_EXPOSURE_PARTICIPANT (
        run_id,
        participant_id,
        participant_name,
        total_var_exposure,
        open_trade_count
    )
    WITH participant_agg AS (
        SELECT
            vr.participant_id,
            SUM(vr.var_exposure)  AS total_var_exposure,
            SUM(vr.trade_count)  AS open_trade_count
        FROM VAR_RESULTS vr
        WHERE vr.run_id = :p_run_id
        GROUP BY vr.participant_id
    ),
    threshold AS (
        SELECT
            PERCENTILE_CONT(:v_threshold_pct / 100.0)
                WITHIN GROUP (ORDER BY total_var_exposure) AS exposure_threshold
        FROM participant_agg
    )
    SELECT
        :p_run_id                                                   AS run_id,
        pa.participant_id,
        COALESCE(em.participant_name, pa.participant_id)            AS participant_name,
        pa.total_var_exposure,
        pa.open_trade_count
    FROM participant_agg pa
    CROSS JOIN threshold th
    LEFT JOIN ENTITY_MASTER em
        ON pa.participant_id = em.participant_id AND em.is_current = TRUE
    WHERE pa.total_var_exposure >= th.exposure_threshold;

    RETURN 'GENERATE_HIGH_EXPOSURE_PARTICIPANT_REPORT completed for run_id=' || :p_run_id;
END;
$$;


-- =============================================================================
-- 5. GENERATE_TOP_CUSIPS_REPORT
-- =============================================================================
-- Ranks CUSIPs by total VaR, takes top 3. Joins with SECURITY_MASTER for
-- security_name. Inserts into REPORT_TOP_CUSIPS.
--
-- Requirements: 9.1, 9.2
-- =============================================================================
CREATE OR REPLACE PROCEDURE GENERATE_TOP_CUSIPS_REPORT(p_run_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Remove any previous results for this run_id (idempotent)
    DELETE FROM REPORT_TOP_CUSIPS WHERE run_id = :p_run_id;

    INSERT INTO REPORT_TOP_CUSIPS (
        run_id,
        rank,
        cusip,
        security_name,
        total_var_exposure,
        pct_of_total_var
    )
    WITH cusip_agg AS (
        SELECT
            vr.cusip,
            SUM(vr.var_exposure) AS total_var_exposure
        FROM VAR_RESULTS vr
        WHERE vr.run_id = :p_run_id
        GROUP BY vr.cusip
    ),
    grand_total AS (
        SELECT COALESCE(SUM(total_var_exposure), 0) AS portfolio_var
        FROM cusip_agg
    ),
    ranked AS (
        SELECT
            ca.cusip,
            ca.total_var_exposure,
            ROW_NUMBER() OVER (ORDER BY ca.total_var_exposure DESC) AS rank
        FROM cusip_agg ca
    )
    SELECT
        :p_run_id                                                       AS run_id,
        r.rank,
        r.cusip,
        COALESCE(sm.security_name, r.cusip)                             AS security_name,
        r.total_var_exposure,
        CASE
            WHEN gt.portfolio_var > 0
            THEN ROUND(r.total_var_exposure / gt.portfolio_var * 100, 4)
            ELSE 0
        END                                                             AS pct_of_total_var
    FROM ranked r
    CROSS JOIN grand_total gt
    LEFT JOIN SECURITY_MASTER sm
        ON r.cusip = sm.cusip AND sm.is_current = TRUE
    WHERE r.rank <= 3
    ORDER BY r.rank;

    RETURN 'GENERATE_TOP_CUSIPS_REPORT completed for run_id=' || :p_run_id;
END;
$$;


-- =============================================================================
-- 6. GENERATE_TOP_PARTICIPANTS_REPORT
-- =============================================================================
-- Ranks participants by total VaR, takes top 5. Joins with ENTITY_MASTER for
-- participant_name. Inserts into REPORT_TOP_PARTICIPANTS.
--
-- Requirements: 10.1, 10.2
-- =============================================================================
CREATE OR REPLACE PROCEDURE GENERATE_TOP_PARTICIPANTS_REPORT(p_run_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Remove any previous results for this run_id (idempotent)
    DELETE FROM REPORT_TOP_PARTICIPANTS WHERE run_id = :p_run_id;

    INSERT INTO REPORT_TOP_PARTICIPANTS (
        run_id,
        rank,
        participant_id,
        participant_name,
        total_var_exposure,
        open_trade_count,
        pct_of_total_var
    )
    WITH participant_agg AS (
        SELECT
            vr.participant_id,
            SUM(vr.var_exposure)  AS total_var_exposure,
            SUM(vr.trade_count)  AS open_trade_count
        FROM VAR_RESULTS vr
        WHERE vr.run_id = :p_run_id
        GROUP BY vr.participant_id
    ),
    grand_total AS (
        SELECT COALESCE(SUM(total_var_exposure), 0) AS portfolio_var
        FROM participant_agg
    ),
    ranked AS (
        SELECT
            pa.participant_id,
            pa.total_var_exposure,
            pa.open_trade_count,
            ROW_NUMBER() OVER (ORDER BY pa.total_var_exposure DESC) AS rank
        FROM participant_agg pa
    )
    SELECT
        :p_run_id                                                       AS run_id,
        r.rank,
        r.participant_id,
        COALESCE(em.participant_name, r.participant_id)                 AS participant_name,
        r.total_var_exposure,
        r.open_trade_count,
        CASE
            WHEN gt.portfolio_var > 0
            THEN ROUND(r.total_var_exposure / gt.portfolio_var * 100, 4)
            ELSE 0
        END                                                             AS pct_of_total_var
    FROM ranked r
    CROSS JOIN grand_total gt
    LEFT JOIN ENTITY_MASTER em
        ON r.participant_id = em.participant_id AND em.is_current = TRUE
    WHERE r.rank <= 5
    ORDER BY r.rank;

    RETURN 'GENERATE_TOP_PARTICIPANTS_REPORT completed for run_id=' || :p_run_id;
END;
$$;


-- =============================================================================
-- 7. GENERATE_REPORTS (Master Orchestrator)
-- =============================================================================
-- Calls all 6 report procedures for a given run_id. Handles edge cases: if no
-- VAR_RESULTS exist for the run_id, produces empty reports. Logs execution to
-- COMPUTE_EXECUTION_LOG.
--
-- Requirements: 5.1, 6.1, 7.1, 8.1, 9.1, 10.1
-- =============================================================================
CREATE OR REPLACE PROCEDURE GENERATE_REPORTS(p_run_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_execution_id    VARCHAR;
    v_exec_start      TIMESTAMP_NTZ;
    v_exec_duration   NUMBER(10,2);
    v_result_count    INTEGER DEFAULT 0;
    v_status          VARCHAR DEFAULT 'success';
    v_error_message   TEXT DEFAULT NULL;
    v_error_code      VARCHAR DEFAULT NULL;
BEGIN
    v_execution_id := UUID_STRING();
    v_exec_start   := CURRENT_TIMESTAMP();

    BEGIN
        -- Check if any VAR_RESULTS exist for this run_id
        SELECT COUNT(*) INTO :v_result_count
        FROM VAR_RESULTS
        WHERE run_id = :p_run_id;

        -- Call all report generators (they handle empty results gracefully)
        CALL GENERATE_EXPOSURE_BY_PARTICIPANT(:p_run_id);
        CALL GENERATE_CONCENTRATION_REPORT(:p_run_id);
        CALL GENERATE_HIGH_VOLUME_CUSIP_REPORT(:p_run_id);
        CALL GENERATE_HIGH_EXPOSURE_PARTICIPANT_REPORT(:p_run_id);
        CALL GENERATE_TOP_CUSIPS_REPORT(:p_run_id);
        CALL GENERATE_TOP_PARTICIPANTS_REPORT(:p_run_id);

        v_exec_duration := TIMESTAMPDIFF(
            MILLISECOND, :v_exec_start, CURRENT_TIMESTAMP()
        ) / 1000.0;

    EXCEPTION
        WHEN OTHER THEN
            v_status        := 'failure';
            v_error_message := SQLERRM;
            v_error_code    := SQLCODE;
            v_exec_duration := TIMESTAMPDIFF(
                MILLISECOND, :v_exec_start, CURRENT_TIMESTAMP()
            ) / 1000.0;
    END;

    -- Log execution to COMPUTE_EXECUTION_LOG
    INSERT INTO COMPUTE_EXECUTION_LOG (
        execution_id,
        computation_name,
        execution_start,
        execution_duration_seconds,
        status,
        error_message,
        error_code
    )
    VALUES (
        :v_execution_id,
        'GENERATE_REPORTS',
        :v_exec_start,
        :v_exec_duration,
        :v_status,
        :v_error_message,
        :v_error_code
    );

    RETURN 'GENERATE_REPORTS completed. run_id=' || :p_run_id
        || ', status=' || :v_status
        || ', var_results_count=' || :v_result_count::VARCHAR;
END;
$$;
