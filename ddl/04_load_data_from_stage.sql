-- ============================================================================
-- Snowflake Stored Procedure: Load Data from Stage to Tables
-- ============================================================================
-- This stored procedure loads specific CSV files from the stage into the table.
-- File names, database, schema, stage, and table names are hardcoded.
-- Process: TRUNCATE table, then INSERT all data from files (DELETE INSERT pattern).
-- Logs all operations to EUROPEAN_CLUB_CUPS_LOAD_LOG table.
-- ============================================================================

USE DATABASE UCL_APUESTA_DB;
USE SCHEMA UCL_APUESTA_SCHEMA;

-- ----------------------------------------------------------------------------
-- Stored Procedure: LOAD_MATCHES_FROM_STAGE
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE LOAD_MATCHES_FROM_STAGE()
RETURNS STRING
LANGUAGE SQL
AS
$$
    DECLARE
        v_result_message STRING := 'Starting data load from stage...\n\n';
        v_rows_total INTEGER;
        v_error_message STRING;
        
    BEGIN
        -- Step 1: Truncate the main table to start fresh
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_MATCHES';
            v_result_message := v_result_message || '✓ Truncated table: EUROPEAN_CLUB_CUPS_MATCHES\n\n';
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := SQLERRM;
                v_result_message := v_result_message || '✗ Failed to truncate table: ' || v_error_message || '\n';
        END;
        
        -- File 1: UCL_champions_league_matches.csv
        BEGIN
            EXECUTE IMMEDIATE 'COPY INTO UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_MATCHES (
                MATCH_ID,
                COMPETITION,
                SEASON,
                PHASE,
                MATCH_DATE,
                HOME_TEAM,
                AWAY_TEAM,
                HOME_GOALS,
                AWAY_GOALS,
                LOAD_DATETIME
            )
            FROM (
                SELECT 
                    $1 AS MATCH_ID,
                    $2 AS COMPETITION,
                    $3 AS SEASON,
                    $4 AS PHASE,
                    $5::DATE AS MATCH_DATE,
                    $6 AS HOME_TEAM,
                    $7 AS AWAY_TEAM,
                    $8::INTEGER AS HOME_GOALS,
                    $9::INTEGER AS AWAY_GOALS,
                    CURRENT_TIMESTAMP() AS LOAD_DATETIME
                FROM @UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CUPS_STAGE
            )
            FILE_FORMAT = (
                TYPE = ''CSV'',
                FIELD_DELIMITER = '','',
                SKIP_HEADER = 1,
                FIELD_OPTIONALLY_ENCLOSED_BY = ''"'',
                TRIM_SPACE = TRUE,
                ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE,
                REPLACE_INVALID_CHARACTERS = TRUE,
                DATE_FORMAT = ''AUTO''
            )
            FILES = (''UCL_champions_league_matches.csv.gz'')
            ON_ERROR = ''CONTINUE''
            FORCE = TRUE';
            
            -- Get row count for this file (approximate from table)
            -- We'll get final count after all files are loaded
            
            -- Log success
            INSERT INTO UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_LOAD_LOG (
                FILE_NAME,
                ROWS_INSERTED,
                STATUS
            ) VALUES (
                'UCL_champions_league_matches.csv',
                0,
                'SUCCESS'
            );
            
            v_result_message := v_result_message || '✓ Processed: UCL_champions_league_matches.csv\n';
            
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := SQLERRM;
                
                -- Log error
                BEGIN
                    INSERT INTO UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_LOAD_LOG (
                        FILE_NAME,
                        ROWS_INSERTED,
                        STATUS
                    ) VALUES (
                        'UCL_champions_league_matches.csv',
                        0,
                        'ERROR: ' || v_error_message
                    );
                EXCEPTION
                    WHEN OTHER THEN
                        NULL;
                END;
                
                v_result_message := v_result_message || '✗ Failed: UCL_champions_league_matches.csv - ' || v_error_message || '\n';
        END;
        
        -- File 2: UEL_europa_league_matches.csv
        BEGIN
            EXECUTE IMMEDIATE 'COPY INTO UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_MATCHES (
                MATCH_ID,
                COMPETITION,
                SEASON,
                PHASE,
                MATCH_DATE,
                HOME_TEAM,
                AWAY_TEAM,
                HOME_GOALS,
                AWAY_GOALS,
                LOAD_DATETIME
            )
            FROM (
                SELECT 
                    $1 AS MATCH_ID,
                    $2 AS COMPETITION,
                    $3 AS SEASON,
                    $4 AS PHASE,
                    $5::DATE AS MATCH_DATE,
                    $6 AS HOME_TEAM,
                    $7 AS AWAY_TEAM,
                    $8::INTEGER AS HOME_GOALS,
                    $9::INTEGER AS AWAY_GOALS,
                    CURRENT_TIMESTAMP() AS LOAD_DATETIME
                FROM @UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CUPS_STAGE
            )
            FILE_FORMAT = (
                TYPE = ''CSV'',
                FIELD_DELIMITER = '','',
                SKIP_HEADER = 1,
                FIELD_OPTIONALLY_ENCLOSED_BY = ''"'',
                TRIM_SPACE = TRUE,
                ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE,
                REPLACE_INVALID_CHARACTERS = TRUE,
                DATE_FORMAT = ''AUTO''
            )
            FILES = (''UEL_europa_league_matches.csv.gz'')
            ON_ERROR = ''CONTINUE''
            FORCE = TRUE';
            
            -- Log success
            INSERT INTO UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_LOAD_LOG (
                FILE_NAME,
                ROWS_INSERTED,
                STATUS
            ) VALUES (
                'UEL_europa_league_matches.csv',
                0,
                'SUCCESS'
            );
            
            v_result_message := v_result_message || '✓ Processed: UEL_europa_league_matches.csv\n';
            
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := SQLERRM;
                
                -- Log error
                BEGIN
                    INSERT INTO UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_LOAD_LOG (
                        FILE_NAME,
                        ROWS_INSERTED,
                        STATUS
                    ) VALUES (
                        'UEL_europa_league_matches.csv',
                        0,
                        'ERROR: ' || v_error_message
                    );
                EXCEPTION
                    WHEN OTHER THEN
                        NULL;
                END;
                
                v_result_message := v_result_message || '✗ Failed: UEL_europa_league_matches.csv - ' || v_error_message || '\n';
        END;
        
        -- File 3: UECL_conference_league_matches.csv
        BEGIN
            EXECUTE IMMEDIATE 'COPY INTO UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_MATCHES (
                MATCH_ID,
                COMPETITION,
                SEASON,
                PHASE,
                MATCH_DATE,
                HOME_TEAM,
                AWAY_TEAM,
                HOME_GOALS,
                AWAY_GOALS,
                LOAD_DATETIME
            )
            FROM (
                SELECT 
                    $1 AS MATCH_ID,
                    $2 AS COMPETITION,
                    $3 AS SEASON,
                    $4 AS PHASE,
                    $5::DATE AS MATCH_DATE,
                    $6 AS HOME_TEAM,
                    $7 AS AWAY_TEAM,
                    $8::INTEGER AS HOME_GOALS,
                    $9::INTEGER AS AWAY_GOALS,
                    CURRENT_TIMESTAMP() AS LOAD_DATETIME
                FROM @UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CUPS_STAGE
            )
            FILE_FORMAT = (
                TYPE = ''CSV'',
                FIELD_DELIMITER = '','',
                SKIP_HEADER = 1,
                FIELD_OPTIONALLY_ENCLOSED_BY = ''"'',
                TRIM_SPACE = TRUE,
                ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE,
                REPLACE_INVALID_CHARACTERS = TRUE,
                DATE_FORMAT = ''AUTO''
            )
            FILES = (''UECL_conference_league_matches.csv.gz'')
            ON_ERROR = ''CONTINUE''
            FORCE = TRUE';
            
            -- Log success
            INSERT INTO UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_LOAD_LOG (
                FILE_NAME,
                ROWS_INSERTED,
                STATUS
            ) VALUES (
                'UECL_conference_league_matches.csv',
                0,
                'SUCCESS'
            );
            
            v_result_message := v_result_message || '✓ Processed: UECL_conference_league_matches.csv\n';
            
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := SQLERRM;
                
                -- Log error
                BEGIN
                    INSERT INTO UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_LOAD_LOG (
                        FILE_NAME,
                        ROWS_INSERTED,
                        STATUS
                    ) VALUES (
                        'UECL_conference_league_matches.csv',
                        0,
                        'ERROR: ' || v_error_message
                    );
                EXCEPTION
                    WHEN OTHER THEN
                        NULL;
                END;
                
                v_result_message := v_result_message || '✗ Failed: UECL_conference_league_matches.csv - ' || v_error_message || '\n';
        END;
        
        -- Get total rows after all inserts
        SELECT COUNT(*) INTO v_rows_total 
        FROM UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_MATCHES;
        
        v_result_message := v_result_message || '\n--- Summary ---\n';
        v_result_message := v_result_message || 'Total rows in table: ' || v_rows_total || '\n';
        v_result_message := v_result_message || '\nCheck EUROPEAN_CLUB_CUPS_LOAD_LOG for detailed operation logs.\n';
        
        RETURN v_result_message;
        
    END;
$$;

-- ----------------------------------------------------------------------------
-- Grant Execute Permission
-- ----------------------------------------------------------------------------
GRANT USAGE ON PROCEDURE UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.LOAD_MATCHES_FROM_STAGE() 
TO ROLE UCL_APUESTA_ROLE;
