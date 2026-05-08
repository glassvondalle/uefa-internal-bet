-- ============================================================================
-- Snowflake DDL Script: European Club Cups Data Objects
-- ============================================================================
-- This script creates the necessary objects for storing European club cup
-- match data and load logging information.
--
-- IMPORTANT: If the table EUROPEAN_CLUB_CUPS_MATCHES already exists,
--            it must be dropped manually before running this script.
--            Example: DROP TABLE IF EXISTS UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_MATCHES;
--
-- PREREQUISITES:
--   - Run this script as a user with ACCOUNTADMIN or SECURITYADMIN role
--   - Adjust warehouse size and auto-suspend settings as needed
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. Warehouse and Role Setup
-- ----------------------------------------------------------------------------
-- Following Snowflake best practices for role-based access control (RBAC)
-- ----------------------------------------------------------------------------

-- Create warehouse for UCL_APUESTA operations
CREATE WAREHOUSE IF NOT EXISTS UCL_APUESTA_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for UCL_APUESTA European club cups data operations.';

-- Create role for UCL_APUESTA application
CREATE ROLE IF NOT EXISTS UCL_APUESTA_ROLE
    COMMENT = 'Role for UCL_APUESTA application with access to European club cups data objects.';

-- Grant warehouse usage to the role
GRANT USAGE ON WAREHOUSE UCL_APUESTA_WH TO ROLE UCL_APUESTA_ROLE;

-- ----------------------------------------------------------------------------
-- 1. Database and Schema Creation
-- ----------------------------------------------------------------------------
-- Create database and schema with UCL_APUESTA naming convention
-- ----------------------------------------------------------------------------

-- Create database
CREATE DATABASE IF NOT EXISTS UCL_APUESTA_DB
    COMMENT = 'Database for UCL_APUESTA European club cups data.';

-- Use the database
USE DATABASE UCL_APUESTA_DB;

-- Create schema
CREATE SCHEMA IF NOT EXISTS UCL_APUESTA_SCHEMA
    COMMENT = 'Schema for UCL_APUESTA European club cups data objects.';

-- Use the schema
USE SCHEMA UCL_APUESTA_DB.UCL_APUESTA_SCHEMA;

-- Grant database usage to the role
GRANT USAGE ON DATABASE UCL_APUESTA_DB TO ROLE UCL_APUESTA_ROLE;

-- Grant schema usage to the role
GRANT USAGE ON SCHEMA UCL_APUESTA_DB.UCL_APUESTA_SCHEMA TO ROLE UCL_APUESTA_ROLE;

-- ----------------------------------------------------------------------------
-- 2. Main Table: EUROPEAN_CLUB_CUPS_MATCHES
-- ----------------------------------------------------------------------------
-- Stores match data for European club competitions (Champions League, 
-- Europa League, etc.)
-- MATCH_ID is the natural unique key (combination of competition, season, 
-- phase, and teams should ensure uniqueness)
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TABLE UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_MATCHES (
    MATCH_ID STRING,
    COMPETITION STRING,
    SEASON STRING,
    PHASE STRING,
    MATCH_DATE DATE,
    HOME_TEAM STRING,
    AWAY_TEAM STRING,
    HOME_GOALS INTEGER,
    AWAY_GOALS INTEGER,
    LOAD_DATETIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Primary key constraint on MATCH_ID (natural unique key)
    CONSTRAINT PK_EUROPEAN_CLUB_CUPS_MATCHES PRIMARY KEY (MATCH_ID)
)
COMMENT = 'Main table storing European club cup match results. MATCH_ID is the natural unique key.';

-- ----------------------------------------------------------------------------
-- 3. Log Table: EUROPEAN_CLUB_CUPS_LOAD_LOG
-- ----------------------------------------------------------------------------
-- Tracks data loading operations including file name, row count, and status
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TABLE UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_LOAD_LOG (
    LOAD_DATETIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    FILE_NAME STRING,
    ROWS_INSERTED INTEGER,
    STATUS STRING
)
COMMENT = 'Log table tracking data load operations for European club cups data.';

CREATE OR REPLACE TABLE PARTICIPANTES
    (JUGADOR VARCHAR,
    TEAM VARCHAR)
COMMENT = 'Table with the tuple players and picked teams';

-- ----------------------------------------------------------------------------
-- 4. Internal Stage: EUROPEAN_CUPS_STAGE
-- ----------------------------------------------------------------------------
-- Internal Snowflake stage for loading data files
-- ----------------------------------------------------------------------------

CREATE OR REPLACE STAGE UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CUPS_STAGE
    COMMENT = 'Internal stage for loading European club cups match data files.';

-- ----------------------------------------------------------------------------
-- 5. Grant Privileges to UCL_APUESTA_ROLE
-- ----------------------------------------------------------------------------
-- Following principle of least privilege: grant only necessary permissions
-- ----------------------------------------------------------------------------

-- Grant privileges on main table (using fully qualified name)
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_MATCHES TO ROLE UCL_APUESTA_ROLE;

-- Grant privileges on log table (using fully qualified name)
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CLUB_CUPS_LOAD_LOG TO ROLE UCL_APUESTA_ROLE;

-- Grant privileges on stage (using fully qualified name)
-- READ for downloading, WRITE for uploading
GRANT READ, WRITE ON STAGE UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.EUROPEAN_CUPS_STAGE TO ROLE UCL_APUESTA_ROLE;

-- ----------------------------------------------------------------------------
-- 6. Optional: Create Service User (Best Practice)
-- ----------------------------------------------------------------------------
-- Uncomment and customize the following to create a dedicated service user
-- ----------------------------------------------------------------------------

/*
-- Create service user for UCL_APUESTA application
CREATE USER IF NOT EXISTS UCL_APUESTA_SVC_USER
    PASSWORD = 'CHANGE_ME_STRONG_PASSWORD'
    DEFAULT_ROLE = UCL_APUESTA_ROLE
    DEFAULT_WAREHOUSE = UCL_APUESTA_WH
    DEFAULT_DATABASE = UCL_APUESTA_DB
    DEFAULT_SCHEMA = UCL_APUESTA_SCHEMA
    COMMENT = 'Service user for UCL_APUESTA application.';

-- Grant role to service user
GRANT ROLE UCL_APUESTA_ROLE TO USER UCL_APUESTA_SVC_USER;
*/

-- ----------------------------------------------------------------------------
-- 7. Optional: Grant Role to Existing Users
-- ----------------------------------------------------------------------------
-- Uncomment and customize to grant the role to specific users
-- ----------------------------------------------------------------------------

/*
-- Example: Grant role to a specific user
-- GRANT ROLE UCL_APUESTA_ROLE TO USER your_username;
*/

-- ============================================================================
-- End of Script
-- ============================================================================

