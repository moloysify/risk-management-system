-- =============================================================================
-- Risk Management System — VaR Tasks
-- =============================================================================
USE DATABASE RISK_MANAGEMENT;
USE SCHEMA RISK_MANAGEMENT.CORE;

-- 1. Stream on TRADES table
CREATE OR REPLACE STREAM TRADES_STREAM
    ON TABLE TRADES
    APPEND_ONLY = TRUE;

-- 2. Event-driven VaR Task
CREATE OR REPLACE TASK VAR_EVENT_DRIVEN_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('TRADES_STREAM')
AS
DECLARE
    v_enabled BOOLEAN;
BEGIN
    SELECT event_driven_enabled INTO :v_enabled
    FROM VAR_CONFIG
    WHERE config_id = '00000000-0000-0000-0000-000000000001';

    IF (v_enabled) THEN
        CALL COMPUTE_VAR();
    END IF;
END;

-- 3. Scheduled VaR Task (default 15 min)
CREATE OR REPLACE TASK VAR_SCHEDULED_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = '15 MINUTE'
AS
BEGIN
    CALL COMPUTE_VAR();
END;

-- 4. Resume both tasks
ALTER TASK VAR_EVENT_DRIVEN_TASK RESUME;
ALTER TASK VAR_SCHEDULED_TASK   RESUME;
