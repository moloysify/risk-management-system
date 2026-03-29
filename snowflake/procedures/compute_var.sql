-- =============================================================================
-- Risk Management System — COMPUTE_VAR() Stored Procedure
-- =============================================================================
-- Computes Value-at-Risk (VaR) using historical simulation at 95% confidence
-- over a 1-day time horizon. Enriches trades with Security Master and Entity
-- Master reference data, excludes trades with missing market data (logging
-- warnings), and writes results to VAR_RESULTS.
--
-- After VaR computation, triggers Report Generator and Cortex Narrator:
--   - GENERATE_REPORTS(run_id) — must complete within 60 seconds
--   - GENERATE_NARRATIVE(run_id) — must complete within 120 seconds
-- Each downstream call is independently exception-handled so failures do not
-- break the overall VaR computation.
--
-- Requirements: 4.2, 4.3, 4.4, 4.5, 4.6, 5.1, 6.1, 7.1, 8.1, 9.1, 10.1, 11.1
-- =============================================================================

USE DATABASE RISK_MANAGEMENT;
USE SCHEMA RISK_MANAGEMENT.CORE;

CREATE OR REPLACE PROCEDURE COMPUTE_VAR()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id          VARCHAR;
    v_run_timestamp   TIMESTAMP_NTZ;
    v_execution_id    VARCHAR;
    v_exec_start      TIMESTAMP_NTZ;
    v_exec_duration   NUMBER(10,2);
    v_result_count    INTEGER DEFAULT 0;
    v_warning_count   INTEGER DEFAULT 0;
    v_error_message   TEXT DEFAULT NULL;
    v_error_code      VARCHAR DEFAULT NULL;
    v_status          VARCHAR DEFAULT 'success';

    -- Report Generator tracking
    v_report_exec_id      VARCHAR;
    v_report_exec_start   TIMESTAMP_NTZ;
    v_report_exec_dur     NUMBER(10,2);
    v_report_status       VARCHAR DEFAULT 'success';
    v_report_error_msg    TEXT DEFAULT NULL;
    v_report_error_code   VARCHAR DEFAULT NULL;

    -- Cortex Narrator tracking
    v_narr_exec_id        VARCHAR;
    v_narr_exec_start     TIMESTAMP_NTZ;
    v_narr_exec_dur       NUMBER(10,2);
    v_narr_status         VARCHAR DEFAULT 'success';
    v_narr_error_msg      TEXT DEFAULT NULL;
    v_narr_error_code     VARCHAR DEFAULT NULL;
BEGIN
    -- Generate unique identifiers and capture timestamps
    v_run_id        := UUID_STRING();
    v_execution_id  := UUID_STRING();
    v_run_timestamp := CURRENT_TIMESTAMP();
    v_exec_start    := CURRENT_TIMESTAMP();

    BEGIN
        -- =================================================================
        -- Step 1: Identify and log trades with missing market data
        -- =================================================================
        -- A trade has "missing market data" if its CUSIP has no rows at all
        -- in MARKET_DATA. We log a warning for each such (cusip, participant_id).
        -- =================================================================
        INSERT INTO REJECTION_LOG (
            rejection_id,
            batch_id,
            data_type,
            raw_record,
            rejection_reason,
            rejection_timestamp
        )
        SELECT
            UUID_STRING(),
            :v_run_id,
            'var_warning',
            OBJECT_CONSTRUCT(
                'cusip', t.cusip,
                'participant_id', t.participant_id,
                'trade_id', t.trade_id,
                'ticker', t.ticker
            ),
            'Missing market data for CUSIP ' || t.cusip || ' — trade excluded from VaR computation',
            CURRENT_TIMESTAMP()
        FROM TRADES t
        INNER JOIN SECURITY_MASTER sm
            ON t.cusip = sm.cusip AND sm.is_current = TRUE
        INNER JOIN ENTITY_MASTER em
            ON t.participant_id = em.participant_id AND em.is_current = TRUE
        LEFT JOIN (
            SELECT DISTINCT cusip
            FROM MARKET_DATA
        ) md ON t.cusip = md.cusip
        WHERE md.cusip IS NULL;

        SELECT COUNT(*) INTO :v_warning_count
        FROM TRADES t
        INNER JOIN SECURITY_MASTER sm
            ON t.cusip = sm.cusip AND sm.is_current = TRUE
        INNER JOIN ENTITY_MASTER em
            ON t.participant_id = em.participant_id AND em.is_current = TRUE
        LEFT JOIN (
            SELECT DISTINCT cusip
            FROM MARKET_DATA
        ) md ON t.cusip = md.cusip
        WHERE md.cusip IS NULL;

        -- =================================================================
        -- Step 2: Compute VaR for each (cusip, participant_id) combination
        -- =================================================================
        -- Historical simulation approach:
        --   1. Get the most recent market price per CUSIP (current price)
        --   2. Compute daily returns from MARKET_DATA historical prices
        --   3. VaR at 95% confidence = 5th percentile of daily returns
        --   4. Dollar VaR = |percentile_return| * position_value
        --      where position_value = SUM(quantity) * current_price
        -- =================================================================

        INSERT INTO VAR_RESULTS (
            var_result_id,
            run_id,
            run_timestamp,
            cusip,
            participant_id,
            var_exposure,
            confidence_level,
            time_horizon_days,
            trade_count,
            input_market_data_version,
            input_security_master_version,
            input_entity_master_version
        )
        WITH enriched_trades AS (
            -- Join trades with current reference data, only for CUSIPs that
            -- have market data available
            SELECT
                t.trade_id,
                t.cusip,
                t.participant_id,
                t.quantity,
                t.price AS trade_price,
                sm.version AS sm_version,
                em.version AS em_version
            FROM TRADES t
            INNER JOIN SECURITY_MASTER sm
                ON t.cusip = sm.cusip AND sm.is_current = TRUE
            INNER JOIN ENTITY_MASTER em
                ON t.participant_id = em.participant_id AND em.is_current = TRUE
            WHERE t.cusip IN (SELECT DISTINCT cusip FROM MARKET_DATA)
        ),
        -- Get the most recent price per CUSIP from MARKET_DATA
        current_prices AS (
            SELECT
                cusip,
                price AS current_price,
                ingestion_timestamp AS market_data_timestamp
            FROM MARKET_DATA
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY cusip ORDER BY data_date DESC, ingestion_timestamp DESC
            ) = 1
        ),
        -- Compute daily returns for each CUSIP from historical prices
        daily_returns AS (
            SELECT
                cusip,
                data_date,
                price,
                LAG(price) OVER (PARTITION BY cusip ORDER BY data_date) AS prev_price,
                CASE
                    WHEN LAG(price) OVER (PARTITION BY cusip ORDER BY data_date) IS NOT NULL
                         AND LAG(price) OVER (PARTITION BY cusip ORDER BY data_date) > 0
                    THEN (price - LAG(price) OVER (PARTITION BY cusip ORDER BY data_date))
                         / LAG(price) OVER (PARTITION BY cusip ORDER BY data_date)
                    ELSE NULL
                END AS daily_return
            FROM MARKET_DATA
        ),
        -- Compute the 5th percentile of daily returns per CUSIP (VaR return)
        var_percentiles AS (
            SELECT
                cusip,
                PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY daily_return) AS var_return
            FROM daily_returns
            WHERE daily_return IS NOT NULL
            GROUP BY cusip
        ),
        -- Aggregate positions per (cusip, participant_id)
        positions AS (
            SELECT
                et.cusip,
                et.participant_id,
                SUM(et.quantity) AS total_quantity,
                COUNT(*) AS trade_count,
                MAX(et.sm_version) AS sm_version,
                MAX(et.em_version) AS em_version
            FROM enriched_trades et
            GROUP BY et.cusip, et.participant_id
        )
        SELECT
            UUID_STRING()                                       AS var_result_id,
            :v_run_id                                           AS run_id,
            :v_run_timestamp                                    AS run_timestamp,
            p.cusip,
            p.participant_id,
            -- Dollar VaR = |var_return| * position_value
            -- position_value = total_quantity * current_price
            ABS(COALESCE(vp.var_return, 0))
                * p.total_quantity
                * cp.current_price                              AS var_exposure,
            0.95                                                AS confidence_level,
            1                                                   AS time_horizon_days,
            p.trade_count,
            cp.market_data_timestamp                            AS input_market_data_version,
            p.sm_version                                        AS input_security_master_version,
            p.em_version                                        AS input_entity_master_version
        FROM positions p
        INNER JOIN current_prices cp ON p.cusip = cp.cusip
        LEFT JOIN var_percentiles vp ON p.cusip = vp.cusip;

        -- Capture how many results were inserted
        SELECT COUNT(*) INTO :v_result_count
        FROM VAR_RESULTS
        WHERE run_id = :v_run_id;

        -- Compute execution duration
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

    -- =================================================================
    -- Step 3: Log execution to COMPUTE_EXECUTION_LOG
    -- =================================================================
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
        'COMPUTE_VAR',
        :v_exec_start,
        :v_exec_duration,
        :v_status,
        :v_error_message,
        :v_error_code
    );

    -- =================================================================
    -- Step 4: Trigger Report Generator (must complete within 60s)
    -- =================================================================
    -- Only trigger if VaR computation succeeded and produced results
    IF (v_status = 'success') THEN
        v_report_exec_id    := UUID_STRING();
        v_report_exec_start := CURRENT_TIMESTAMP();

        BEGIN
            CALL GENERATE_REPORTS(:v_run_id);

            v_report_exec_dur := TIMESTAMPDIFF(
                MILLISECOND, :v_report_exec_start, CURRENT_TIMESTAMP()
            ) / 1000.0;

        EXCEPTION
            WHEN OTHER THEN
                v_report_status     := 'failure';
                v_report_error_msg  := SQLERRM;
                v_report_error_code := SQLCODE;
                v_report_exec_dur   := TIMESTAMPDIFF(
                    MILLISECOND, :v_report_exec_start, CURRENT_TIMESTAMP()
                ) / 1000.0;
        END;

        -- Log report generation execution
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
            :v_report_exec_id,
            'GENERATE_REPORTS (triggered by COMPUTE_VAR)',
            :v_report_exec_start,
            :v_report_exec_dur,
            :v_report_status,
            :v_report_error_msg,
            :v_report_error_code
        );

        -- =================================================================
        -- Step 5: Trigger Cortex Narrator (must complete within 120s)
        -- =================================================================
        v_narr_exec_id    := UUID_STRING();
        v_narr_exec_start := CURRENT_TIMESTAMP();

        BEGIN
            CALL GENERATE_NARRATIVE(:v_run_id);

            v_narr_exec_dur := TIMESTAMPDIFF(
                MILLISECOND, :v_narr_exec_start, CURRENT_TIMESTAMP()
            ) / 1000.0;

        EXCEPTION
            WHEN OTHER THEN
                v_narr_status     := 'failure';
                v_narr_error_msg  := SQLERRM;
                v_narr_error_code := SQLCODE;
                v_narr_exec_dur   := TIMESTAMPDIFF(
                    MILLISECOND, :v_narr_exec_start, CURRENT_TIMESTAMP()
                ) / 1000.0;
        END;

        -- Log narrative generation execution
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
            :v_narr_exec_id,
            'GENERATE_NARRATIVE (triggered by COMPUTE_VAR)',
            :v_narr_exec_start,
            :v_narr_exec_dur,
            :v_narr_status,
            :v_narr_error_msg,
            :v_narr_error_code
        );
    END IF;

    -- Return summary
    RETURN 'COMPUTE_VAR completed. run_id=' || :v_run_id
        || ', status=' || :v_status
        || ', results=' || :v_result_count::VARCHAR
        || ', warnings=' || :v_warning_count::VARCHAR
        || ', reports=' || :v_report_status
        || ', narrative=' || :v_narr_status;
END;
$$;
