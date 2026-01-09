-- ============================================================================
-- Snowflake Stored Procedure: Load Data from Stage to Tables
-- ============================================================================
-- This stored procedure loads CSV files from EUROPEAN_CUPS_STAGE into the
-- EUROPEAN_CLUB_CUPS_MATCHES table and logs the operations.
--
-- Usage:
--   CALL UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.LOAD_MATCHES_FROM_STAGE();
-- ============================================================================

USE DATABASE UCL_APUESTA_DB;
USE SCHEMA UCL_APUESTA_SCHEMA;

-- ----------------------------------------------------------------------------
-- Stored Procedure: LOAD_MATCHES_FROM_STAGE
-- ----------------------------------------------------------------------------
-- Loads all CSV files from EUROPEAN_CUPS_STAGE into EUROPEAN_CLUB_CUPS_MATCHES
-- Uses MERGE to handle duplicates (upsert logic)
-- Logs all operations to EUROPEAN_CLUB_CUPS_LOAD_LOG
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE LOAD_MATCHES_FROM_STAGE()
RETURNS STRING
LANGUAGE SQL
AS
$$
    DECLARE
        v_file_name STRING;
        v_rows_affected INTEGER;
        v_status STRING;
        v_files_processed INTEGER := 0;
        v_files_failed INTEGER := 0;
        v_total_rows INTEGER := 0;
        v_result_message STRING := 'Starting data load from stage...\n';
        v_error_message STRING;
        v_temp_table_name STRING;
        
        -- Cursor to iterate through files in the stage
        file_cursor CURSOR FOR
            SELECT 
                RELATIVE_PATH as file_name,
                FILE_SIZE,
                LAST_MODIFIED
            FROM TABLE(
                LIST_STAGE(
                    '@UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CUPS_STAGE',
                    PATTERN => '.*_matches\\.csv'
                )
            )
            ORDER BY LAST_MODIFIED DESC;
    
    BEGIN
        -- Process each CSV file in the stage
        FOR file_record IN file_cursor DO
            v_file_name := file_record.file_name;
            v_rows_affected := 0;
            v_status := 'ERROR';
            
            BEGIN
                -- Generate unique temporary table name
                v_temp_table_name := 'TEMP_MATCHES_' || REPLACE(TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISSFF9'), '.', '');
                
                -- Create temporary table
                EXECUTE IMMEDIATE '
                    CREATE TEMPORARY TABLE ' || v_temp_table_name || ' (
                        MATCH_ID STRING,
                        COMPETITION STRING,
                        SEASON STRING,
                        PHASE STRING,
                        MATCH_DATE DATE,
                        HOME_TEAM STRING,
                        AWAY_TEAM STRING,
                        HOME_GOALS INTEGER,
                        AWAY_GOALS INTEGER
                    )';
                
                -- Copy data from stage to temporary table
                EXECUTE IMMEDIATE '
                    COPY INTO ' || v_temp_table_name || '
                    FROM @UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CUPS_STAGE
                    FILE_FORMAT = (
                        TYPE = ''CSV'',
                        FIELD_DELIMITER = '','',
                        SKIP_HEADER = 1,
                        FIELD_OPTIONALLY_ENCLOSED_BY = ''"'',
                        TRIM_SPACE = TRUE,
                        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE,
                        REPLACE_INVALID_CHARACTERS = TRUE,
                        DATE_FORMAT = ''AUTO'',
                        TIMESTAMP_FORMAT = ''AUTO''
                    )
                    PATTERN = ''' || REPLACE(v_file_name, '''', '''''') || '''
                    ON_ERROR = ''CONTINUE''';
                
                -- Get number of rows loaded from COPY INTO result
                -- Note: We'll use row count from MERGE instead
                
                -- Merge data from temp table to main table
                EXECUTE IMMEDIATE '
                    MERGE INTO UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_MATCHES AS target
                    USING ' || v_temp_table_name || ' AS source
                    ON target.MATCH_ID = source.MATCH_ID
                    WHEN MATCHED THEN
                        UPDATE SET
                            COMPETITION = source.COMPETITION,
                            SEASON = source.SEASON,
                            PHASE = source.PHASE,
                            MATCH_DATE = source.MATCH_DATE,
                            HOME_TEAM = source.HOME_TEAM,
                            AWAY_TEAM = source.AWAY_TEAM,
                            HOME_GOALS = source.HOME_GOALS,
                            AWAY_GOALS = source.AWAY_GOALS,
                            LOAD_DATETIME = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN
                        INSERT (
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
                        VALUES (
                            source.MATCH_ID,
                            source.COMPETITION,
                            source.SEASON,
                            source.PHASE,
                            source.MATCH_DATE,
                            source.HOME_TEAM,
                            source.AWAY_TEAM,
                            source.HOME_GOALS,
                            source.AWAY_GOALS,
                            CURRENT_TIMESTAMP()
                        )';
                
                -- Get number of rows affected (this is approximate as MERGE doesn't return exact counts in SQL)
                -- We'll use SQLROWCOUNT or count from the temp table
                LET v_temp_count INTEGER;
                EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_temp_table_name INTO v_temp_count;
                v_rows_affected := v_temp_count;
                
                -- Log successful operation
                INSERT INTO UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_LOAD_LOG (
                    FILE_NAME,
                    ROWS_INSERTED,
                    STATUS
                ) VALUES (
                    v_file_name,
                    v_rows_affected,
                    'SUCCESS'
                );
                
                v_status := 'SUCCESS';
                v_files_processed := v_files_processed + 1;
                v_total_rows := v_total_rows + v_rows_affected;
                v_result_message := v_result_message || '✓ Processed: ' || v_file_name || ' (' || v_rows_affected || ' rows)\n';
                
            EXCEPTION
                WHEN OTHER THEN
                    v_status := 'ERROR';
                    v_error_message := SQLERRM;
                    v_files_failed := v_files_failed + 1;
                    
                    -- Log the error
                    BEGIN
                        INSERT INTO UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_LOAD_LOG (
                            FILE_NAME,
                            ROWS_INSERTED,
                            STATUS
                        ) VALUES (
                            v_file_name,
                            0,
                            'ERROR: ' || v_error_message
                        );
                    EXCEPTION
                        WHEN OTHER THEN
                            -- If logging fails, continue anyway
                            NULL;
                    END;
                    
                    v_result_message := v_result_message || '✗ Failed: ' || v_file_name || ' - ' || v_error_message || '\n';
            END;
            
        END FOR;
        
        -- Final summary
        v_result_message := v_result_message || '\n--- Summary ---\n';
        v_result_message := v_result_message || 'Files processed: ' || v_files_processed || '\n';
        v_result_message := v_result_message || 'Files failed: ' || v_files_failed || '\n';
        v_result_message := v_result_message || 'Total rows affected: ' || v_total_rows || '\n';
        
        RETURN v_result_message;
    END;
$$;

-- ----------------------------------------------------------------------------
-- Grant Execute Permission
-- ----------------------------------------------------------------------------
GRANT USAGE ON PROCEDURE UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.LOAD_MATCHES_FROM_STAGE() 
TO ROLE UCL_APUESTA_ROLE;

-- ============================================================================
-- Alternative: Simple COPY INTO command (if you prefer a simpler approach)
-- ============================================================================
-- You can also use COPY INTO directly without a stored procedure:
--
-- COPY INTO UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_MATCHES
-- FROM @UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CUPS_STAGE
-- FILE_FORMAT = (
--     TYPE = 'CSV',
--     FIELD_DELIMITER = ',',
--     SKIP_HEADER = 1,
--     FIELD_OPTIONALLY_ENCLOSED_BY = '"',
--     TRIM_SPACE = TRUE,
--     ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE,
--     REPLACE_INVALID_CHARACTERS = TRUE,
--     DATE_FORMAT = 'AUTO',
--     TIMESTAMP_FORMAT = 'AUTO'
-- )
-- PATTERN = '.*_matches\\.csv'
-- ON_ERROR = 'CONTINUE'
-- FORCE = FALSE;  -- Set to TRUE to reload files even if already loaded
--
-- Note: This will INSERT only. To handle duplicates, use the stored procedure above.
--
-- ============================================================================
-- End of Script
-- ============================================================================
