-- =============================================================================
-- Risk Management System — GENERATE_NARRATIVE() Stored Procedure
-- =============================================================================
-- Generates a natural-language market condition summary using Snowflake Cortex.
-- Gathers latest market data (top movers, index values, price changes), calls
-- FETCH_NEWS() for recent news context, builds a structured prompt, and calls
-- SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', prompt) to produce the narrative.
--
-- Falls back to market-data-only narrative if news retrieval fails
-- (news_included = FALSE). Stores result in MARKET_NARRATIVES and logs
-- execution to COMPUTE_EXECUTION_LOG.
--
-- Must complete within 120 seconds of VaR computation completion.
--
-- Requirements: 11.1, 11.2, 11.3, 12.2, 12.3
-- =============================================================================

USE DATABASE RISK_MANAGEMENT;
USE SCHEMA RISK_MANAGEMENT.CORE;

CREATE OR REPLACE PROCEDURE GENERATE_NARRATIVE(p_run_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_narrative_id      VARCHAR;
    v_execution_id      VARCHAR;
    v_exec_start        TIMESTAMP_NTZ;
    v_exec_duration     NUMBER(10,2);
    v_status            VARCHAR DEFAULT 'success';
    v_error_message     TEXT DEFAULT NULL;
    v_error_code        VARCHAR DEFAULT NULL;

    -- Market data context
    v_market_summary    VARCHAR DEFAULT '';
    v_top_movers        VARCHAR DEFAULT '';
    v_index_summary     VARCHAR DEFAULT '';
    v_var_summary       VARCHAR DEFAULT '';
    v_sector_summary    VARCHAR DEFAULT '';

    -- News context
    v_news_data         VARIANT DEFAULT NULL;
    v_news_context      VARCHAR DEFAULT '';
    v_news_included     BOOLEAN DEFAULT FALSE;

    -- Prompt and narrative
    v_prompt            VARCHAR;
    v_narrative_text    TEXT DEFAULT '';
BEGIN
    v_narrative_id := UUID_STRING();
    v_execution_id := UUID_STRING();
    v_exec_start   := CURRENT_TIMESTAMP();

    BEGIN
        -- =================================================================
        -- Step 1: Gather market data context
        -- =================================================================

        -- 1a. Overall market summary — latest prices and daily changes
        SELECT LISTAGG(summary_line, '; ') WITHIN GROUP (ORDER BY abs_change DESC)
        INTO :v_market_summary
        FROM (
            SELECT
                md.cusip
                || ' price=' || md.price::VARCHAR
                || CASE
                       WHEN prev.price IS NOT NULL AND prev.price > 0
                       THEN ' chg=' || ROUND((md.price - prev.price) / prev.price * 100, 2)::VARCHAR || '%'
                       ELSE ''
                   END AS summary_line,
                ABS(CASE
                    WHEN prev.price IS NOT NULL AND prev.price > 0
                    THEN (md.price - prev.price) / prev.price * 100
                    ELSE 0
                END) AS abs_change
            FROM MARKET_DATA md
            LEFT JOIN MARKET_DATA prev
                ON md.cusip = prev.cusip
                AND prev.data_date = DATEADD(DAY, -1, md.data_date)
            WHERE md.data_date = (SELECT MAX(data_date) FROM MARKET_DATA)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY md.cusip ORDER BY md.ingestion_timestamp DESC) = 1
            ORDER BY abs_change DESC
            LIMIT 20
        );

        -- 1b. Top movers — securities with largest absolute price changes
        SELECT LISTAGG(mover_line, '; ') WITHIN GROUP (ORDER BY abs_pct_change DESC)
        INTO :v_top_movers
        FROM (
            SELECT
                COALESCE(sm.security_name, md.cusip) || ': '
                || ROUND((md.price - prev.price) / prev.price * 100, 2)::VARCHAR || '%'
                AS mover_line,
                ABS((md.price - prev.price) / prev.price * 100) AS abs_pct_change
            FROM MARKET_DATA md
            INNER JOIN MARKET_DATA prev
                ON md.cusip = prev.cusip
                AND prev.data_date = DATEADD(DAY, -1, md.data_date)
            LEFT JOIN SECURITY_MASTER sm
                ON md.cusip = sm.cusip AND sm.is_current = TRUE
            WHERE md.data_date = (SELECT MAX(data_date) FROM MARKET_DATA)
              AND prev.price > 0
            QUALIFY ROW_NUMBER() OVER (PARTITION BY md.cusip ORDER BY md.ingestion_timestamp DESC) = 1
            ORDER BY abs_pct_change DESC
            LIMIT 5
        );

        -- 1c. Index values summary
        SELECT LISTAGG(idx_line, '; ') WITHIN GROUP (ORDER BY idx_line)
        INTO :v_index_summary
        FROM (
            SELECT
                md.cusip || ' index=' || md.index_value::VARCHAR AS idx_line
            FROM MARKET_DATA md
            WHERE md.data_date = (SELECT MAX(data_date) FROM MARKET_DATA)
              AND md.index_value IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY md.cusip ORDER BY md.ingestion_timestamp DESC) = 1
            LIMIT 10
        );

        -- 1d. VaR summary for this run — total exposure and top contributors
        SELECT
            'Total portfolio VaR='
            || ROUND(SUM(var_exposure), 2)::VARCHAR
            || ', positions=' || COUNT(*)::VARCHAR
            || ', CUSIPs=' || COUNT(DISTINCT cusip)::VARCHAR
            || ', participants=' || COUNT(DISTINCT participant_id)::VARCHAR
        INTO :v_var_summary
        FROM VAR_RESULTS
        WHERE run_id = :p_run_id;

        -- 1e. Sector-level trends
        SELECT LISTAGG(sector_line, '; ') WITHIN GROUP (ORDER BY sector_var DESC)
        INTO :v_sector_summary
        FROM (
            SELECT
                COALESCE(sm.sector, 'Unknown') || ': VaR='
                || ROUND(SUM(vr.var_exposure), 2)::VARCHAR
                AS sector_line,
                SUM(vr.var_exposure) AS sector_var
            FROM VAR_RESULTS vr
            LEFT JOIN SECURITY_MASTER sm
                ON vr.cusip = sm.cusip AND sm.is_current = TRUE
            WHERE vr.run_id = :p_run_id
            GROUP BY COALESCE(sm.sector, 'Unknown')
            ORDER BY sector_var DESC
            LIMIT 10
        );

        -- =================================================================
        -- Step 2: Fetch news data (graceful failure)
        -- =================================================================
        BEGIN
            CALL FETCH_NEWS('financial markets stock bond economy') INTO :v_news_data;

            IF (v_news_data IS NOT NULL) THEN
                v_news_included := TRUE;

                -- Format news articles into context string
                SELECT LISTAGG(article_text, '\n') WITHIN GROUP (ORDER BY idx)
                INTO :v_news_context
                FROM (
                    SELECT
                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS idx,
                        '- ' || f.value:headline::VARCHAR
                        || ' (' || COALESCE(f.value:source::VARCHAR, 'Unknown') || ', '
                        || COALESCE(f.value:published_at::VARCHAR, '') || ')'
                        || CASE
                               WHEN f.value:summary IS NOT NULL
                               THEN ': ' || LEFT(f.value:summary::VARCHAR, 200)
                               ELSE ''
                           END AS article_text
                    FROM TABLE(FLATTEN(input => :v_news_data)) f
                    LIMIT 10
                );
            END IF;

        EXCEPTION
            WHEN OTHER THEN
                -- News retrieval failed — proceed without news
                v_news_included := FALSE;
                v_news_context  := '';
        END;

        -- =================================================================
        -- Step 3: Build structured prompt for Cortex
        -- =================================================================
        v_prompt := 'You are a senior financial risk analyst. Generate a concise market condition narrative based on the following data. '
            || 'Include: (1) an overall market volatility assessment, (2) primary factors driving market movement, '
            || '(3) notable sector-level trends, and (4) specific index and price references from the data provided.\n\n'
            || '## Market Data\n'
            || 'Latest prices and daily changes: ' || COALESCE(:v_market_summary, 'No market data available') || '\n\n'
            || '## Top Movers\n'
            || COALESCE(:v_top_movers, 'No significant movers identified') || '\n\n'
            || '## Index Values\n'
            || COALESCE(:v_index_summary, 'No index data available') || '\n\n'
            || '## VaR Summary (Run ID: ' || :p_run_id || ')\n'
            || COALESCE(:v_var_summary, 'No VaR data available for this run') || '\n\n'
            || '## Sector Trends\n'
            || COALESCE(:v_sector_summary, 'No sector data available') || '\n\n';

        -- Append news context if available
        IF (v_news_included = TRUE AND LENGTH(:v_news_context) > 0) THEN
            v_prompt := :v_prompt
                || '## Recent Financial News (last 24 hours)\n'
                || :v_news_context || '\n\n'
                || 'Incorporate relevant news headlines into the narrative where they relate to observed market movements.\n';
        ELSE
            v_prompt := :v_prompt
                || '## News Context\n'
                || 'No recent news data available. Base the narrative solely on market data and VaR results.\n';
        END IF;

        v_prompt := :v_prompt
            || '\nProvide the narrative in 3-5 paragraphs. Be specific with numbers and percentages. '
            || 'Do not use bullet points. Write in a professional tone suitable for a risk management report.';

        -- =================================================================
        -- Step 4: Call Snowflake Cortex to generate narrative
        -- =================================================================
        SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', :v_prompt)
        INTO :v_narrative_text;

        -- Handle potential null or empty response
        IF (v_narrative_text IS NULL OR LENGTH(v_narrative_text) = 0) THEN
            v_narrative_text := 'Narrative generation returned empty response. '
                || 'Market data summary: ' || COALESCE(:v_market_summary, 'N/A');
            v_status := 'failure';
            v_error_message := 'Cortex COMPLETE returned empty response';
            v_error_code := 'CORTEX_EMPTY_RESPONSE';
        END IF;

        -- =================================================================
        -- Step 5: Store narrative in MARKET_NARRATIVES
        -- =================================================================
        INSERT INTO MARKET_NARRATIVES (
            narrative_id,
            run_id,
            narrative_text,
            news_included,
            generated_at
        )
        VALUES (
            :v_narrative_id,
            :p_run_id,
            :v_narrative_text,
            :v_news_included,
            CURRENT_TIMESTAMP()
        );

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
    -- Step 6: Log execution to COMPUTE_EXECUTION_LOG
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
        'GENERATE_NARRATIVE',
        :v_exec_start,
        :v_exec_duration,
        :v_status,
        :v_error_message,
        :v_error_code
    );

    RETURN 'GENERATE_NARRATIVE completed. run_id=' || :p_run_id
        || ', status=' || :v_status
        || ', news_included=' || :v_news_included::VARCHAR;
END;
$$;
