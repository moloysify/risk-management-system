-- =============================================================================
-- Risk Management System — Core Tables
-- =============================================================================
-- Creates the database, schema, and all core tables for the Risk Management
-- System. Uses CREATE OR REPLACE TABLE for idempotent execution.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS RISK_MANAGEMENT;
USE DATABASE RISK_MANAGEMENT;

CREATE SCHEMA IF NOT EXISTS RISK_MANAGEMENT.CORE;
USE SCHEMA RISK_MANAGEMENT.CORE;

-- -----------------------------------------------------------------------------
-- TRADES
-- Stores all ingested trade records from bulk files, snapshots, and voice.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE TRADES (
    trade_id                VARCHAR(36)       NOT NULL PRIMARY KEY,
    batch_id                VARCHAR(36)       NOT NULL,
    ticker                  VARCHAR(20)       NOT NULL,
    cusip                   VARCHAR(9)        NOT NULL,
    price                   NUMBER(18,6)      NOT NULL,
    quantity                NUMBER(18,4)      NOT NULL,
    participant_id          VARCHAR(36)       NOT NULL,
    trade_timestamp         TIMESTAMP_NTZ     NOT NULL,
    source_format           VARCHAR(10)       NOT NULL,
    ingestion_timestamp     TIMESTAMP_NTZ     NOT NULL
);

-- -----------------------------------------------------------------------------
-- MARKET_DATA
-- Stores market prices and index values for each CUSIP by date.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE MARKET_DATA (
    market_data_id          VARCHAR(36)       NOT NULL PRIMARY KEY,
    cusip                   VARCHAR(9)        NOT NULL,
    price                   NUMBER(18,6)      NOT NULL,
    index_value             NUMBER(18,6),
    data_date               DATE              NOT NULL,
    ingestion_timestamp     TIMESTAMP_NTZ     NOT NULL
);

-- -----------------------------------------------------------------------------
-- SECURITY_MASTER
-- Reference data for securities with SCD Type 2 versioning.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SECURITY_MASTER (
    security_master_id      VARCHAR(36)       NOT NULL PRIMARY KEY,
    cusip                   VARCHAR(9)        NOT NULL,
    security_name           VARCHAR(255)      NOT NULL,
    issuer                  VARCHAR(255)      NOT NULL,
    security_type           VARCHAR(50)       NOT NULL,
    sector                  VARCHAR(100)      NOT NULL,
    valid_from              TIMESTAMP_NTZ     NOT NULL,
    valid_to                TIMESTAMP_NTZ,
    is_current              BOOLEAN           NOT NULL,
    version                 INTEGER           NOT NULL
);

-- -----------------------------------------------------------------------------
-- ENTITY_MASTER
-- Reference data for trading participants with SCD Type 2 versioning.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE ENTITY_MASTER (
    entity_master_id        VARCHAR(36)       NOT NULL PRIMARY KEY,
    participant_id          VARCHAR(36)       NOT NULL,
    participant_name        VARCHAR(255)      NOT NULL,
    entity_type             VARCHAR(50)       NOT NULL,
    risk_limit              NUMBER(18,2)      NOT NULL,
    valid_from              TIMESTAMP_NTZ     NOT NULL,
    valid_to                TIMESTAMP_NTZ,
    is_current              BOOLEAN           NOT NULL,
    version                 INTEGER           NOT NULL
);

-- -----------------------------------------------------------------------------
-- VAR_RESULTS
-- Stores computed VaR exposure results per run, CUSIP, and participant.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE VAR_RESULTS (
    var_result_id                VARCHAR(36)       NOT NULL PRIMARY KEY,
    run_id                       VARCHAR(36)       NOT NULL,
    run_timestamp                TIMESTAMP_NTZ     NOT NULL,
    cusip                        VARCHAR(9)        NOT NULL,
    participant_id               VARCHAR(36)       NOT NULL,
    var_exposure                 NUMBER(18,6)      NOT NULL,
    confidence_level             NUMBER(5,4)       NOT NULL,
    time_horizon_days            INTEGER           NOT NULL,
    trade_count                  INTEGER           NOT NULL,
    input_market_data_version    TIMESTAMP_NTZ     NOT NULL,
    input_security_master_version INTEGER          NOT NULL,
    input_entity_master_version  INTEGER           NOT NULL
);

-- -----------------------------------------------------------------------------
-- REJECTION_LOG
-- Captures records that failed validation during ingestion.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE REJECTION_LOG (
    rejection_id            VARCHAR(36)       NOT NULL PRIMARY KEY,
    batch_id                VARCHAR(36)       NOT NULL,
    data_type               VARCHAR(20)       NOT NULL,
    raw_record              VARIANT           NOT NULL,
    rejection_reason        VARCHAR(1000)     NOT NULL,
    rejection_timestamp     TIMESTAMP_NTZ     NOT NULL
);

-- -----------------------------------------------------------------------------
-- INGESTION_LOG
-- Tracks metadata for each ingestion batch (counts, timing, status).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE INGESTION_LOG (
    log_id                  VARCHAR(36)       NOT NULL PRIMARY KEY,
    batch_id                VARCHAR(36)       NOT NULL,
    kafka_topic             VARCHAR(100)      NOT NULL,
    data_type               VARCHAR(20)       NOT NULL,
    record_count            INTEGER           NOT NULL,
    rejected_count          INTEGER           NOT NULL,
    status                  VARCHAR(10)       NOT NULL,
    processing_duration_ms  INTEGER           NOT NULL,
    started_at              TIMESTAMP_NTZ     NOT NULL,
    completed_at            TIMESTAMP_NTZ     NOT NULL
);

-- -----------------------------------------------------------------------------
-- MARKET_NARRATIVES
-- AI-generated natural-language market condition summaries.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE MARKET_NARRATIVES (
    narrative_id            VARCHAR(36)       NOT NULL PRIMARY KEY,
    run_id                  VARCHAR(36)       NOT NULL,
    narrative_text          TEXT              NOT NULL,
    news_included           BOOLEAN           NOT NULL,
    generated_at            TIMESTAMP_NTZ     NOT NULL
);

-- -----------------------------------------------------------------------------
-- COMPUTE_EXECUTION_LOG
-- Records execution details for Snowflake compute operations.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE COMPUTE_EXECUTION_LOG (
    execution_id                VARCHAR(36)       NOT NULL PRIMARY KEY,
    computation_name            VARCHAR(255)      NOT NULL,
    execution_start             TIMESTAMP_NTZ     NOT NULL,
    execution_duration_seconds  NUMBER(10,2)      NOT NULL,
    status                      VARCHAR(10)       NOT NULL,
    error_message               TEXT,
    error_code                  VARCHAR(50)
);

-- -----------------------------------------------------------------------------
-- VAR_CONFIG
-- Stores VaR computation frequency configuration (event-driven toggle,
-- schedule interval). Expected to contain a single active row.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE VAR_CONFIG (
    config_id                   VARCHAR(36)       NOT NULL PRIMARY KEY,
    event_driven_enabled        BOOLEAN           NOT NULL,
    schedule_interval_minutes   INTEGER           NOT NULL,
    updated_by                  VARCHAR(255)      NOT NULL,
    updated_at                  TIMESTAMP_NTZ     NOT NULL
);

-- -----------------------------------------------------------------------------
-- SIMULATION_LOG
-- Tracks data simulator activity (synthetic data generation runs).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SIMULATION_LOG (
    simulation_log_id       VARCHAR(36)       NOT NULL PRIMARY KEY,
    simulation_run_id       VARCHAR(36)       NOT NULL,
    data_type               VARCHAR(20)       NOT NULL,
    kafka_topic             VARCHAR(100)      NOT NULL,
    record_count            INTEGER           NOT NULL,
    mode                    VARCHAR(10)       NOT NULL,
    dataset_size            VARCHAR(10)       NOT NULL,
    status                  VARCHAR(10)       NOT NULL,
    error_message           TEXT,
    generated_at            TIMESTAMP_NTZ     NOT NULL
);
