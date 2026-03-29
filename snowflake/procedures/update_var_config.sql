-- =============================================================================
-- Risk Management System — UPDATE_VAR_CONFIG() Stored Procedure
-- =============================================================================
USE DATABASE RISK_MANAGEMENT;
USE SCHEMA RISK_MANAGEMENT.CORE;

CREATE OR REPLACE PROCEDURE UPDATE_VAR_CONFIG(
    p_event_driven_enabled    BOOLEAN,
    p_schedule_interval_minutes INTEGER,
    p_updated_by              VARCHAR,
    p_expected_updated_at     TIMESTAMP_NTZ
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_execution_id        VARCHAR;
    v_exec_start          TIMESTAMP_NTZ;
    v_exec_duration       NUMBER(10,2);
    v_error_message       TEXT DEFAULT NULL;
    v_error_code          VARCHAR DEFAULT NULL;
    v_status              VARCHAR DEFAULT 'success';
    v_current_updated_at  TIMESTAMP_NTZ;
    v_old_event_driven    BOOLEAN;
    v_old_interval        INTEGER;
    v_rows_updated        INTEGER;
    v_schedule_expr       VARCHAR;
    v_validation_err      EXCEPTION (-20001, 'Validation error');
    v_lock_err            EXCEPTION (-20002, 'Optimistic lock conflict');
BEGIN
    v_execution_id := UUID_STRING();
    v_exec_start   := CURRENT_TIMESTAMP();

    BEGIN
        -- Validate schedule_interval_minutes
        IF (:p_schedule_interval_minutes NOT IN (5, 15, 30, 60)) THEN
            RAISE v_validation_err;
        END IF;

        -- Read current config and check optimistic lock
        SELECT event_driven_enabled, schedule_interval_minutes, updated_at
          INTO :v_old_event_driven, :v_old_interval, :v_current_updated_at
          FROM VAR_CONFIG
         WHERE config_id = '00000000-0000-0000-0000-000000000001';

        IF (:v_current_updated_at != :p_expected_updated_at) THEN
            RAISE v_lock_err;
        END IF;

        -- Update VAR_CONFIG row
        UPDATE VAR_CONFIG
           SET event_driven_enabled      = :p_event_driven_enabled,
               schedule_interval_minutes = :p_schedule_interval_minutes,
               updated_by               = :p_updated_by,
               updated_at               = CURRENT_TIMESTAMP()
         WHERE config_id = '00000000-0000-0000-0000-000000000001'
           AND updated_at = :p_expected_updated_at;

        v_rows_updated := SQLROWCOUNT;
        IF (:v_rows_updated = 0) THEN
            RAISE v_lock_err;
        END IF;

        -- Alter event-driven task if toggle changed
        IF (:p_event_driven_enabled != :v_old_event_driven) THEN
            IF (:p_event_driven_enabled) THEN
                ALTER TASK VAR_EVENT_DRIVEN_TASK RESUME;
            ELSE
                ALTER TASK VAR_EVENT_DRIVEN_TASK SUSPEND;
            END IF;
        END IF;

        -- Alter scheduled task if interval changed
        IF (:p_schedule_interval_minutes != :v_old_interval) THEN
            v_schedule_expr := :p_schedule_interval_minutes::VARCHAR || ' MINUTE';
            ALTER TASK VAR_SCHEDULED_TASK SUSPEND;
            EXECUTE IMMEDIATE 'ALTER TASK VAR_SCHEDULED_TASK SET SCHEDULE = ''' || :v_schedule_expr || '''';
            ALTER TASK VAR_SCHEDULED_TASK RESUME;
        END IF;

        v_exec_duration := TIMESTAMPDIFF(MILLISECOND, :v_exec_start, CURRENT_TIMESTAMP()) / 1000.0;

    EXCEPTION
        WHEN v_validation_err THEN
            v_status := 'failure';
            v_error_message := 'Invalid schedule_interval_minutes: ' || :p_schedule_interval_minutes::VARCHAR || '. Must be 5, 15, 30, or 60.';
            v_error_code := '-20001';
            v_exec_duration := TIMESTAMPDIFF(MILLISECOND, :v_exec_start, CURRENT_TIMESTAMP()) / 1000.0;
        WHEN v_lock_err THEN
            v_status := 'failure';
            v_error_message := 'Optimistic lock conflict. Please refresh and retry.';
            v_error_code := '-20002';
            v_exec_duration := TIMESTAMPDIFF(MILLISECOND, :v_exec_start, CURRENT_TIMESTAMP()) / 1000.0;
        WHEN OTHER THEN
            v_status := 'failure';
            v_error_message := SQLERRM;
            v_error_code := SQLCODE;
            v_exec_duration := TIMESTAMPDIFF(MILLISECOND, :v_exec_start, CURRENT_TIMESTAMP()) / 1000.0;
    END;

    INSERT INTO COMPUTE_EXECUTION_LOG (
        execution_id, computation_name, execution_start,
        execution_duration_seconds, status, error_message, error_code
    ) VALUES (
        :v_execution_id, 'UPDATE_VAR_CONFIG', :v_exec_start,
        :v_exec_duration, :v_status, :v_error_message, :v_error_code
    );

    RETURN 'UPDATE_VAR_CONFIG completed. status=' || :v_status
        || ', event_driven=' || :p_event_driven_enabled::VARCHAR
        || ', interval=' || :p_schedule_interval_minutes::VARCHAR;
END;
$$;
