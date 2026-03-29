-- =============================================================================
-- Risk Management System — Seed Data
-- =============================================================================
-- Inserts default configuration rows. Uses MERGE for idempotent execution
-- (safe to run multiple times without creating duplicates).
-- =============================================================================

USE DATABASE RISK_MANAGEMENT;
USE SCHEMA RISK_MANAGEMENT.CORE;

-- -----------------------------------------------------------------------------
-- VAR_CONFIG — Default configuration row
-- Event-driven VaR computation enabled, 15-minute scheduled interval.
-- -----------------------------------------------------------------------------
MERGE INTO VAR_CONFIG AS target
USING (
    SELECT
        '00000000-0000-0000-0000-000000000001' AS config_id,
        TRUE                                    AS event_driven_enabled,
        15                                      AS schedule_interval_minutes,
        'SYSTEM'                                AS updated_by,
        CURRENT_TIMESTAMP()                     AS updated_at
) AS source
ON target.config_id = source.config_id
WHEN NOT MATCHED THEN
    INSERT (config_id, event_driven_enabled, schedule_interval_minutes, updated_by, updated_at)
    VALUES (source.config_id, source.event_driven_enabled, source.schedule_interval_minutes, source.updated_by, source.updated_at);
