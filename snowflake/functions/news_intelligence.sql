-- =============================================================================
-- Risk Management System — News Intelligence Service
-- =============================================================================
-- Provides Google News API integration via Snowflake External Function.
-- Retrieves financial news articles published within the preceding 24 hours
-- and returns structured news data (headlines, summaries, sources, timestamps).
--
-- Components:
--   1. External Function: GOOGLE_NEWS_API (placeholder — requires API integration)
--   2. Wrapper Procedure: FETCH_NEWS() — calls the external function with
--      graceful failure handling so Cortex Narrator can proceed without news.
--
-- Requirements: 12.1, 12.2, 12.3
-- =============================================================================

USE DATABASE RISK_MANAGEMENT;
USE SCHEMA RISK_MANAGEMENT.CORE;

-- =============================================================================
-- API Integration (placeholder)
-- =============================================================================
-- Before the external function can be used, an API Integration must be created
-- by an ACCOUNTADMIN. This connects Snowflake to the API Gateway (e.g., AWS
-- API Gateway, Azure API Management, or GCP Cloud Endpoints) that proxies
-- requests to the Google News API.
--
-- Example (uncomment and configure for your environment):
--
-- CREATE OR REPLACE API INTEGRATION google_news_api_integration
--     API_PROVIDER         = aws_api_gateway          -- or azure_api_management / google_api_gateway
--     API_AWS_ROLE_ARN     = 'arn:aws:iam::<account>:role/<role_name>'
--     API_ALLOWED_PREFIXES = ('https://<api-gateway-id>.execute-api.<region>.amazonaws.com/')
--     ENABLED              = TRUE;
--
-- After creation, run DESCRIBE INTEGRATION google_news_api_integration; to
-- retrieve the API_AWS_IAM_USER_ARN and API_AWS_EXTERNAL_ID needed to
-- configure the trust relationship on the IAM role.
-- =============================================================================

-- =============================================================================
-- External Function: GOOGLE_NEWS_API
-- =============================================================================
-- Placeholder external function definition. This function sends a query string
-- to a Google News API proxy and returns a JSON array of news articles.
--
-- To activate:
--   1. Deploy an API Gateway endpoint that proxies to Google News API
--   2. Create the API Integration above
--   3. Uncomment and run the CREATE EXTERNAL FUNCTION below with the correct
--      API_INTEGRATION name and gateway URL
--
-- Expected response format (VARIANT — JSON array):
-- [
--   {
--     "headline": "Fed Signals Rate Hold Amid Inflation Concerns",
--     "summary":  "The Federal Reserve indicated it would maintain...",
--     "source":   "Reuters",
--     "published_at": "2024-01-15T14:30:00Z",
--     "url":      "https://..."
--   },
--   ...
-- ]
-- =============================================================================

-- Uncomment when API integration is configured:
--
-- CREATE OR REPLACE EXTERNAL FUNCTION GOOGLE_NEWS_API(query VARCHAR)
--     RETURNS VARIANT
--     API_INTEGRATION = google_news_api_integration
--     HEADERS = ('Content-Type' = 'application/json')
--     MAX_BATCH_ROWS = 1
--     AS 'https://<api-gateway-id>.execute-api.<region>.amazonaws.com/prod/news';

-- =============================================================================
-- Wrapper Procedure: FETCH_NEWS
-- =============================================================================
-- Calls the GOOGLE_NEWS_API external function to retrieve financial news from
-- the last 24 hours. Returns a VARIANT (JSON array of articles) on success,
-- or NULL on failure. All failures are logged to COMPUTE_EXECUTION_LOG so
-- Cortex Narrator can fall back to market-data-only narrative.
--
-- Parameters:
--   p_query  VARCHAR  — Search query for news (default: 'financial markets')
--
-- Returns:
--   VARIANT  — JSON array of news articles, or NULL if retrieval failed
--
-- Requirements: 12.1, 12.2, 12.3
-- =============================================================================
CREATE OR REPLACE PROCEDURE FETCH_NEWS(p_query VARCHAR DEFAULT 'financial markets stock bond')
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_news_data     VARIANT DEFAULT NULL;
    v_execution_id  VARCHAR;
    v_exec_start    TIMESTAMP_NTZ;
    v_exec_duration NUMBER(10,2);
    v_status        VARCHAR DEFAULT 'success';
    v_error_message TEXT DEFAULT NULL;
    v_error_code    VARCHAR DEFAULT NULL;
BEGIN
    v_execution_id := UUID_STRING();
    v_exec_start   := CURRENT_TIMESTAMP();

    BEGIN
        -- =================================================================
        -- Attempt to call the external function.
        -- If the external function is not yet configured, this will raise
        -- an exception which we catch and handle gracefully.
        -- =================================================================
        -- Timeout: treat as failure after 30 seconds (handled by Snowflake
        -- external function timeout settings on the API integration).
        -- =================================================================
        CALL GOOGLE_NEWS_API(:p_query) INTO :v_news_data;

        -- Validate that we received a non-empty array
        IF (v_news_data IS NULL OR ARRAY_SIZE(v_news_data) = 0) THEN
            v_news_data     := NULL;
            v_status        := 'failure';
            v_error_message := 'Google News API returned empty or null response';
            v_error_code    := 'NEWS_EMPTY_RESPONSE';
        END IF;

    EXCEPTION
        WHEN OTHER THEN
            -- Log the failure but do NOT re-raise — allow caller to proceed
            v_news_data     := NULL;
            v_status        := 'failure';
            v_error_message := SQLERRM;
            v_error_code    := SQLCODE;
    END;

    -- Compute execution duration
    v_exec_duration := TIMESTAMPDIFF(
        MILLISECOND, :v_exec_start, CURRENT_TIMESTAMP()
    ) / 1000.0;

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
        'FETCH_NEWS',
        :v_exec_start,
        :v_exec_duration,
        :v_status,
        :v_error_message,
        :v_error_code
    );

    RETURN v_news_data;
END;
$$;
