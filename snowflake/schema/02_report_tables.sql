-- =============================================================================
-- Risk Management System — Report Tables
-- =============================================================================
-- Creates all report tables used by the Report Generator. Each table is keyed
-- by run_id plus the relevant entity identifier. Uses CREATE OR REPLACE TABLE
-- for idempotent execution.
--
-- Requirements: 5.2, 6.2, 7.2, 8.2, 9.2, 10.2
-- =============================================================================

USE DATABASE RISK_MANAGEMENT;
USE SCHEMA RISK_MANAGEMENT.CORE;

-- -----------------------------------------------------------------------------
-- REPORT_EXPOSURE_BY_PARTICIPANT
-- Aggregated VaR exposure per participant for a given computation run.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE REPORT_EXPOSURE_BY_PARTICIPANT (
    run_id              VARCHAR(36)       NOT NULL,
    participant_id      VARCHAR(36)       NOT NULL,
    participant_name    VARCHAR(255)      NOT NULL,
    total_var_exposure  NUMBER(18,6)      NOT NULL,
    open_trade_count    INTEGER           NOT NULL,
    pct_of_total_var    NUMBER(10,4)      NOT NULL,
    rank                INTEGER           NOT NULL,

    CONSTRAINT pk_exposure_by_participant PRIMARY KEY (run_id, participant_id)
);

-- -----------------------------------------------------------------------------
-- REPORT_CONCENTRATION
-- Concentration of VaR exposure by CUSIP or Participant for a given run.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE REPORT_CONCENTRATION (
    run_id              VARCHAR(36)       NOT NULL,
    entity_type         VARCHAR(20)       NOT NULL,
    entity_id           VARCHAR(36)       NOT NULL,
    entity_name         VARCHAR(255)      NOT NULL,
    concentration_pct   NUMBER(10,4)      NOT NULL,
    is_flagged          BOOLEAN           NOT NULL,

    CONSTRAINT pk_concentration PRIMARY KEY (run_id, entity_type, entity_id)
);

-- -----------------------------------------------------------------------------
-- REPORT_HIGH_VOLUME_CUSIP
-- CUSIPs whose current trade volume exceeds the 90th percentile of historical
-- daily volumes.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE REPORT_HIGH_VOLUME_CUSIP (
    run_id                  VARCHAR(36)       NOT NULL,
    cusip                   VARCHAR(9)        NOT NULL,
    current_volume          NUMBER(18,4)      NOT NULL,
    historical_avg_volume   NUMBER(18,4)      NOT NULL,
    percentile_rank         NUMBER(10,4)      NOT NULL,

    CONSTRAINT pk_high_volume_cusip PRIMARY KEY (run_id, cusip)
);

-- -----------------------------------------------------------------------------
-- REPORT_HIGH_EXPOSURE_PARTICIPANT
-- Participants whose VaR exposure exceeds the configurable threshold.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE REPORT_HIGH_EXPOSURE_PARTICIPANT (
    run_id              VARCHAR(36)       NOT NULL,
    participant_id      VARCHAR(36)       NOT NULL,
    participant_name    VARCHAR(255)      NOT NULL,
    total_var_exposure  NUMBER(18,6)      NOT NULL,
    open_trade_count    INTEGER           NOT NULL,

    CONSTRAINT pk_high_exposure_participant PRIMARY KEY (run_id, participant_id)
);

-- -----------------------------------------------------------------------------
-- REPORT_TOP_CUSIPS
-- Top 3 CUSIPs ranked by total VaR exposure for a given run.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE REPORT_TOP_CUSIPS (
    run_id              VARCHAR(36)       NOT NULL,
    rank                INTEGER           NOT NULL,
    cusip               VARCHAR(9)        NOT NULL,
    security_name       VARCHAR(255)      NOT NULL,
    total_var_exposure  NUMBER(18,6)      NOT NULL,
    pct_of_total_var    NUMBER(10,4)      NOT NULL,

    CONSTRAINT pk_top_cusips PRIMARY KEY (run_id, rank)
);

-- -----------------------------------------------------------------------------
-- REPORT_TOP_PARTICIPANTS
-- Top 5 Participants ranked by total VaR exposure for a given run.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE REPORT_TOP_PARTICIPANTS (
    run_id              VARCHAR(36)       NOT NULL,
    rank                INTEGER           NOT NULL,
    participant_id      VARCHAR(36)       NOT NULL,
    participant_name    VARCHAR(255)      NOT NULL,
    total_var_exposure  NUMBER(18,6)      NOT NULL,
    open_trade_count    INTEGER           NOT NULL,
    pct_of_total_var    NUMBER(10,4)      NOT NULL,

    CONSTRAINT pk_top_participants PRIMARY KEY (run_id, rank)
);
