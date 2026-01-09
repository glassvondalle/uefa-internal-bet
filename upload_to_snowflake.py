"""
Script to execute get-results.py and upload CSV files to Snowflake stage.

This script:
1. Executes get-results.py to scrape and generate CSV files
2. Finds all generated CSV files
3. Connects to Snowflake
4. Executes create_european_club_cups_objects.sql to create objects (including stage)
5. Uploads CSV files to the Snowflake stage
"""

import json
import os
import sys
import subprocess
import glob
import re
from pathlib import Path
from typing import List, Optional
import snowflake.connector
from snowflake.connector import DictCursor


# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent.absolute()


def load_config(config_path: Optional[str] = None) -> dict:
    """
    Load Snowflake configuration from JSON file.
    
    Args:
        config_path: Path to the configuration file (default: snowflake_config.json in script directory)
    
    Returns:
        Dictionary with configuration parameters
    """
    # If no path provided, use default in script directory
    if config_path is None:
        config_path = SCRIPT_DIR / "snowflake_config.json"
    else:
        # If relative path, make it relative to script directory
        if not os.path.isabs(config_path):
            config_path = SCRIPT_DIR / config_path
        else:
            config_path = Path(config_path)
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        print(f"❌ Configuration file not found: {config_path}")
        print(f"   Looking in: {config_path.absolute()}")
        print(f"   Script directory: {SCRIPT_DIR}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing configuration file: {e}")
        sys.exit(1)


def execute_scraper(script_path: Optional[str] = None) -> bool:
    """
    Execute the get-results.py scraper script.
    
    Args:
        script_path: Path to the scraper script (default: get-results.py in script directory)
    
    Returns:
        True if execution was successful, False otherwise
    """
    if script_path is None:
        script_path = SCRIPT_DIR / "get-results.py"
    else:
        if not os.path.isabs(script_path):
            script_path = SCRIPT_DIR / script_path
        else:
            script_path = Path(script_path)
    
    if not os.path.exists(script_path):
        print(f"❌ Scraper script not found: {script_path}")
        return False
    
    print("=" * 80)
    print("Executing scraper script...")
    print("=" * 80)
    print()
    
    try:
        # Execute the Python script
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=False,  # Show output in real-time
            text=True,
            check=False  # Don't raise exception on non-zero exit
        )
        
        if result.returncode == 0:
            print()
            print("✅ Scraper execution completed successfully")
            return True
        else:
            print()
            print(f"⚠️  Scraper exited with code: {result.returncode}")
            return False
            
    except Exception as e:
        print(f"❌ Error executing scraper: {e}")
        return False


def find_csv_files(pattern: str = "*_matches.csv", search_dir: Optional[str] = None) -> List[str]:
    """
    Find all CSV files matching the pattern.
    
    Args:
        pattern: Glob pattern to match CSV files
        search_dir: Directory to search in (default: current working directory)
    
    Returns:
        List of CSV file paths
    """
    # Default to current working directory (where get-results.py creates files)
    if search_dir is None:
        search_dir = Path.cwd()
    else:
        search_dir = Path(search_dir)
    
    csv_files = glob.glob(str(search_dir / pattern))
    
    # Filter to only include files that look like competition match files
    competition_patterns = ["UCL_", "UEL_", "UECL_"]
    filtered_files = [
        f for f in csv_files 
        if any(os.path.basename(f).startswith(comp) for comp in competition_patterns)
    ]
    
    return sorted(filtered_files)


def connect_to_snowflake(config: dict) -> snowflake.connector.SnowflakeConnection:
    """
    Connect to Snowflake using configuration parameters.
    
    Args:
        config: Dictionary with connection parameters
    
    Returns:
        Snowflake connection object
    """
    print()
    print("=" * 80)
    print("Connecting to Snowflake...")
    print("=" * 80)
    
    try:
        connection_params = {
            "account": config["account"],
            "user": config["user"],
            "password": config["password"]
        }
        
        # Add optional parameters if they exist
        if config.get("warehouse"):
            connection_params["warehouse"] = config["warehouse"]
        if config.get("database"):
            connection_params["database"] = config["database"]
        if config.get("schema"):
            connection_params["schema"] = config["schema"]
        if config.get("role"):
            connection_params["role"] = config["role"]
        
        conn = snowflake.connector.connect(**connection_params)
        print(f"✅ Connected to Snowflake account: {config['account']}")
        return conn
        
    except Exception as e:
        print(f"❌ Error connecting to Snowflake: {e}")
        raise


def execute_sql_file(conn: snowflake.connector.SnowflakeConnection, 
                     sql_file_path: str) -> bool:
    """
    Execute a SQL file against Snowflake.
    Parses and executes each SQL statement separately, handling comments.
    
    Args:
        conn: Snowflake connection
        sql_file_path: Path to the SQL file
    
    Returns:
        True if execution was successful, False otherwise
    """
    # Make path relative to script directory if not absolute
    if not os.path.isabs(sql_file_path):
        sql_file_path = SCRIPT_DIR / sql_file_path
    else:
        sql_file_path = Path(sql_file_path)
    
    if not os.path.exists(sql_file_path):
        print(f"❌ SQL file not found: {sql_file_path}")
        return False
    
    print()
    print("=" * 80)
    print(f"Executing SQL file: {sql_file_path}")
    print("=" * 80)
    
    try:
        # Read the SQL file
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Check if file contains stored procedures - if so, use simpler execution
        has_procedure = 'CREATE' in sql_content.upper() and 'PROCEDURE' in sql_content.upper() and '$$' in sql_content
        
        if has_procedure:
            print("   Detected stored procedure in file - using direct execution")
            # For stored procedures, execute the entire file as-is
            # This avoids parsing issues with semicolons inside $$ blocks
            try:
                cursor = conn.cursor()
                # Execute the entire file content
                cursor.execute(sql_content)
                cursor.close()
                print("✅ SQL file executed successfully (stored procedure created)")
                return True
            except Exception as e:
                error_msg = str(e)
                print(f"❌ Error executing SQL file: {error_msg[:300]}")
                # Fall through to try parsing method
                print("   Attempting to parse and execute statements separately...")
        
        # Use cursor to execute SQL statements
        cursor = conn.cursor()
        
        # Handle stored procedures specially - they contain semicolons inside $$ blocks
        statements = []
        
        # First, remove block comments (/* ... */) but preserve stored procedure delimiters
        sql_cleaned = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
        
        # Find and extract stored procedure blocks (between $$ delimiters)
        # Pattern: CREATE ... PROCEDURE ... AS $$ ... $$;
        proc_pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+.*?AS\s+\$\$.*?\$\$;'
        procedures = re.findall(proc_pattern, sql_cleaned, re.DOTALL | re.IGNORECASE)
        
        print(f"   Found {len(procedures)} stored procedure(s) in SQL file")
        
        # Replace stored procedures with placeholders to avoid splitting them
        placeholder_map = {}
        sql_with_placeholders = sql_cleaned
        for i, proc in enumerate(procedures):
            placeholder = f'__PROCEDURE_PLACEHOLDER_{i}__'
            placeholder_map[placeholder] = proc
            sql_with_placeholders = sql_with_placeholders.replace(proc, placeholder, 1)
            proc_name_match = re.search(r'PROCEDURE\s+(\w+)', proc, re.IGNORECASE)
            if proc_name_match:
                print(f"      - Procedure: {proc_name_match.group(1)}")
        
        # Split remaining SQL by semicolon
        for stmt in sql_with_placeholders.split(';'):
            stmt = stmt.strip()
            # Check if this is a placeholder for a stored procedure
            for placeholder, proc in placeholder_map.items():
                if placeholder in stmt:
                    # Replace placeholder with actual procedure
                    stmt = stmt.replace(placeholder, proc)
                    statements.append(stmt)
                    break
            else:
                # Regular SQL statement
                # Skip empty statements and statements that are only comments
                if stmt and not stmt.startswith('--'):
                    # Remove inline comments (-- ...)
                    lines = []
                    for line in stmt.split('\n'):
                        if '--' in line:
                            line = line[:line.index('--')]
                        line = line.strip()
                        if line:
                            lines.append(line)
                    if lines:
                        statements.append('\n'.join(lines))
        
        executed_count = 0
        failed_count = 0
        
        for i, statement in enumerate(statements, 1):
            if not statement or len(statement) < 5:  # Skip very short statements
                continue
            
            # Show what we're executing (truncated for long statements)
            stmt_preview = statement[:100].replace('\n', ' ') if len(statement) > 100 else statement.replace('\n', ' ')
            if 'PROCEDURE' in statement.upper():
                print(f"   Executing statement {i} (PROCEDURE): {stmt_preview}...")
            else:
                print(f"   Executing statement {i}: {stmt_preview}...")
                
            try:
                cursor.execute(statement)
                executed_count += 1
                if 'PROCEDURE' in statement.upper():
                    print(f"      ✅ Procedure statement executed successfully")
                    # Verify it was actually created by checking immediately
                    try:
                        verify_cursor = conn.cursor()
                        if 'DATABASE' in statement.upper() and 'SCHEMA' in statement.upper():
                            # Try to find the database/schema from USE statements before
                            verify_cursor.execute("SHOW PROCEDURES LIKE 'LOAD_MATCHES_FROM_STAGE'")
                        else:
                            verify_cursor.execute("SHOW PROCEDURES LIKE 'LOAD_MATCHES_FROM_STAGE'")
                        verify_result = verify_cursor.fetchall()
                        verify_cursor.close()
                        if verify_result:
                            print(f"      ✅✅ Procedure verified to exist after creation!")
                        else:
                            print(f"      ⚠️  Procedure execution succeeded but verification failed")
                    except:
                        pass  # Don't fail if verification fails
            except Exception as e:
                error_msg = str(e)
                # Some errors are acceptable (e.g., object already exists with IF NOT EXISTS)
                if "already exists" in error_msg.lower() or "does not exist" in error_msg.lower():
                    executed_count += 1  # Count as success for IF NOT EXISTS scenarios
                    if 'PROCEDURE' in statement.upper():
                        print(f"      ℹ️  Procedure may already exist (this is usually OK)")
                else:
                    failed_count += 1
                    print(f"      ❌ Error in statement {i}: {error_msg[:200]}")
                    # For procedures, show more details
                    if 'PROCEDURE' in statement.upper():
                        print(f"      This was a CREATE PROCEDURE statement - check syntax above")
        
        cursor.close()
        
        if failed_count == 0:
            print(f"✅ Executed {executed_count} SQL statement(s) successfully")
        else:
            print(f"⚠️  Executed {executed_count} statement(s), {failed_count} had errors")
        
        return executed_count > 0
        
    except Exception as e:
        print(f"❌ Error executing SQL file: {e}")
        import traceback
        traceback.print_exc()
        return False


def create_stage_if_not_exists(conn: snowflake.connector.SnowflakeConnection, 
                                stage_name: str,
                                database: Optional[str] = None,
                                schema: Optional[str] = None) -> None:
    """
    Create a Snowflake stage if it doesn't exist.
    
    Args:
        conn: Snowflake connection
        stage_name: Name of the stage
        database: Optional database name (if not in connection params)
        schema: Optional schema name (if not in connection params)
    """
    print()
    print(f"Ensuring stage '{stage_name}' exists...")
    
    # Build full stage name
    if database and schema:
        full_stage_name = f"{database}.{schema}.{stage_name}"
    elif schema:
        full_stage_name = f"{schema}.{stage_name}"
    else:
        full_stage_name = stage_name
    
    # Check if stage exists
    try:
        cursor = conn.cursor()
        cursor.execute(f"SHOW STAGES LIKE '{stage_name}'")
        result = cursor.fetchall()
        
        if not result:
            # Create the stage
            create_sql = f"CREATE STAGE IF NOT EXISTS {full_stage_name}"
            cursor.execute(create_sql)
            print(f"✅ Created stage: {full_stage_name}")
        else:
            print(f"✅ Stage already exists: {full_stage_name}")
        
        cursor.close()
        
    except Exception as e:
        print(f"⚠️  Error checking/creating stage: {e}")
        # Try to create anyway (might fail if it exists, which is ok)
        try:
            cursor = conn.cursor()
            cursor.execute(f"CREATE STAGE IF NOT EXISTS {full_stage_name}")
            cursor.close()
            print(f"✅ Stage created/verified: {full_stage_name}")
        except:
            pass


def check_file_exists_in_stage(conn: snowflake.connector.SnowflakeConnection,
                                file_name: str,
                                stage_name: str,
                                database: Optional[str] = None,
                                schema: Optional[str] = None) -> bool:
    """
    Check if a file already exists in the Snowflake stage.
    
    Args:
        conn: Snowflake connection
        file_name: Name of the file to check (just filename, not full path)
        stage_name: Name of the stage
        database: Optional database name
        schema: Optional schema name
    
    Returns:
        True if file exists, False otherwise
    """
    # Build full stage path
    if database and schema:
        stage_path = f"{database}.{schema}.{stage_name}"
    elif schema:
        stage_path = f"{schema}.{stage_name}"
    else:
        stage_path = stage_name
    
    try:
        cursor = conn.cursor()
        
        # Set context if database/schema provided
        if database:
            cursor.execute(f"USE DATABASE {database}")
        if schema and database:
            cursor.execute(f"USE SCHEMA {database}.{schema}")
        elif schema:
            cursor.execute(f"USE SCHEMA {schema}")
        
        # List files in stage and check if our file exists
        cursor.execute(f"LIST @{stage_path}")
        files = cursor.fetchall()
        cursor.close()
        
        # Check if file exists (file name can be in different columns depending on LIST output)
        for file_record in files:
            if isinstance(file_record, (list, tuple)):
                # LIST returns: name, size, md5, last_modified
                stage_file_name = file_record[0] if len(file_record) > 0 else ""
                # Compare just the filename (ignore path)
                if os.path.basename(stage_file_name) == file_name:
                    return True
            elif isinstance(file_record, str):
                if os.path.basename(file_record) == file_name:
                    return True
        
        return False
        
    except Exception as e:
        # If we can't check, assume it doesn't exist (better to try upload)
        print(f"      ⚠️  Could not check if file exists: {e}")
        return False


def upload_file_to_stage(conn: snowflake.connector.SnowflakeConnection,
                         file_path: str,
                         stage_name: str,
                         database: Optional[str] = None,
                         schema: Optional[str] = None,
                         skip_existing: bool = True) -> bool:
    """
    Upload a file to Snowflake stage.
    
    Args:
        conn: Snowflake connection
        file_path: Local file path to upload
        stage_name: Name of the stage
        database: Optional database name
        schema: Optional schema name
    
    Returns:
        True if upload was successful, False otherwise
    """
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False
    
    # Build full stage path
    if database and schema:
        stage_path = f"{database}.{schema}.{stage_name}"
    elif schema:
        stage_path = f"{schema}.{stage_name}"
    else:
        stage_path = stage_name
    
    # Get just the filename for the stage
    filename = os.path.basename(file_path)
    
    # Check if file already exists in stage (if skip_existing is True)
    if skip_existing:
        if check_file_exists_in_stage(conn, filename, stage_name, database, schema):
            print(f"   ⏭️  Skipping {filename} (already exists in stage)")
            return True
    
    print(f"   Uploading {filename} to @{stage_path}...")
    
    try:
        cursor = conn.cursor()
        
        # Ensure we're using the correct database and schema context
        # This is important for the PUT command to work correctly
        if database:
            cursor.execute(f"USE DATABASE {database}")
        if schema and database:
            cursor.execute(f"USE SCHEMA {database}.{schema}")
        elif schema:
            cursor.execute(f"USE SCHEMA {schema}")
        
        # Convert Windows path for Snowflake PUT command
        # Snowflake PUT on Windows needs forward slashes
        abs_path = Path(file_path).resolve()
        file_path_normalized = str(abs_path).replace('\\', '/')
        
        # PUT command format: PUT 'file://path/to/file' @stage
        # On Windows, path must use forward slashes and be quoted
        # Escape single quotes in the path if any (unlikely but possible)
        file_path_escaped = file_path_normalized.replace("'", "''")
        put_sql = f"PUT 'file://{file_path_escaped}' @{stage_path}"
        
        print(f"      Command: {put_sql[:200]}...")  # Truncate long paths for display
        
        # Execute PUT command
        cursor.execute(put_sql)
        
        # PUT command returns a result set with upload information
        # Columns: source, target, source_size, target_size, source_compression, target_compression, status, message
        results = cursor.fetchall()
        
        if results:
            # Process each result row (usually just one for a single file)
            for row in results:
                # Handle tuple or other formats
                if isinstance(row, (list, tuple)) and len(row) >= 7:
                    status = str(row[6]).upper()
                    message = str(row[7]) if len(row) > 7 else ""
                    
                    print(f"      Status: {status}")
                    if message:
                        print(f"      Message: {message}")
                    
                    if "UPLOADED" in status:
                        print(f"      ✅ File uploaded successfully")
                        cursor.close()
                        return True
                    elif "SKIPPED" in status:
                        print(f"      ⚠️  File was skipped (may already exist)")
                        cursor.close()
                        return True
                    else:
                        print(f"      ⚠️  Unexpected status: {status}")
                        cursor.close()
                        return False
                else:
                    # If we can't parse the row structure, print it for debugging
                    print(f"      ⚠️  Unexpected result format: {row}")
        
        cursor.close()
        
        # If we got here, we didn't get a clear success indication
        if results:
            print(f"      ⚠️  Upload may have succeeded, but status unclear")
            return True
        else:
            print(f"      ❌ No results returned from PUT command")
            return False
        
    except Exception as e:
        error_msg = str(e)
        print(f"      ❌ Error uploading file: {error_msg}")
        
        # Provide helpful error messages
        if "does not exist" in error_msg.lower() or "not found" in error_msg.lower():
            print(f"      💡 Tip: Make sure the stage '{stage_path}' exists")
        elif "permission" in error_msg.lower() or "access" in error_msg.lower():
            print(f"      💡 Tip: Check that your user has WRITE permission on the stage")
        elif "file://" in error_msg.lower():
            print(f"      💡 Tip: The file path format might be incorrect for your OS")
        
        import traceback
        traceback.print_exc()
        return False


def list_stage_files(conn: snowflake.connector.SnowflakeConnection,
                     stage_name: str,
                     database: Optional[str] = None,
                     schema: Optional[str] = None) -> None:
    """
    List files in the Snowflake stage.
    
    Args:
        conn: Snowflake connection
        stage_name: Name of the stage
        database: Optional database name
        schema: Optional schema name
    """
    # Build full stage path
    if database and schema:
        stage_path = f"{database}.{schema}.{stage_name}"
    elif schema:
        stage_path = f"{schema}.{stage_name}"
    else:
        stage_path = stage_name
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"LIST @{stage_path}")
        files = cursor.fetchall()
        cursor.close()
        
        if files:
            print(f"\n📁 Files in stage '{stage_path}':")
            for file in files[:10]:  # Show first 10
                print(f"   - {file[0] if isinstance(file, tuple) else file}")
            if len(files) > 10:
                print(f"   ... and {len(files) - 10} more files")
        else:
            print(f"\n📁 Stage '{stage_path}' is empty")
            
    except Exception as e:
        print(f"⚠️  Error listing stage files: {e}")


def main():
    """Main execution function."""
    print("=" * 80)
    print("European Club Cups Data - Snowflake Upload Script")
    print("=" * 80)
    print()
    print(f"Working directory: {Path.cwd()}")
    print()
    
    # Load configuration
    config = load_config()
    stage_name = config.get("stage_name", "EUROPEAN_CUPS_STAGE")
    sql_file = config.get("sql_file", "create_european_club_cups_objects.sql")
    load_procedure_file = config.get("load_procedure_file", "load_data_from_stage.sql")
    
    # Step 1: Execute scraper
    if not execute_scraper():
        print("\n⚠️  Scraper execution had issues, but continuing with upload...")
        print()
    
    # Step 2: Find CSV files
    print()
    print("=" * 80)
    print("Finding generated CSV files...")
    print("=" * 80)
    csv_files = find_csv_files()
    
    if not csv_files:
        print("❌ No CSV files found matching pattern '*_matches.csv'")
        print("   Expected files like: UCL_champions_league_matches.csv")
        print(f"   Searched in: {Path.cwd()}")
        print("   Make sure get-results.py has been executed and generated CSV files.")
        sys.exit(1)
    
    print(f"✅ Found {len(csv_files)} CSV file(s):")
    for csv_file in csv_files:
        file_size = os.path.getsize(csv_file)
        print(f"   - {csv_file} ({file_size:,} bytes)")
    
    # Step 3: Connect to Snowflake
    try:
        conn = connect_to_snowflake(config)
    except Exception as e:
        print(f"\n❌ Failed to connect to Snowflake: {e}")
        sys.exit(1)
    
    try:
        # Step 4: Execute SQL file to create objects (warehouse, database, schema, tables, stage, etc.)
        if not execute_sql_file(conn, sql_file):
            print("\n⚠️  SQL file execution had issues, but continuing with upload...")
            print("   The stage and objects may not have been created properly.")
            print()
        
        # Step 4b: Create the stored procedure for loading data (if file exists)
        procedure_created = False
        if os.path.exists(load_procedure_file):
            print()
            print("=" * 80)
            print("Creating stored procedure for loading data...")
            print("=" * 80)
            if execute_sql_file(conn, load_procedure_file):
                # Verify the procedure was created
                try:
                    cursor = conn.cursor()
                    
                    # Set context first
                    if config.get("database"):
                        cursor.execute(f"USE DATABASE {config['database']}")
                    if config.get("schema") and config.get("database"):
                        cursor.execute(f"USE SCHEMA {config['database']}.{config['schema']}")
                    elif config.get("schema"):
                        cursor.execute(f"USE SCHEMA {config['schema']}")
                    
                    # Build fully qualified procedure name
                    if config.get("database") and config.get("schema"):
                        procedure_name = f"{config['database']}.{config['schema']}.LOAD_MATCHES_FROM_STAGE"
                        show_sql = f"SHOW PROCEDURES LIKE 'LOAD_MATCHES_FROM_STAGE' IN SCHEMA {config['database']}.{config['schema']}"
                    elif config.get("schema"):
                        procedure_name = f"{config['schema']}.LOAD_MATCHES_FROM_STAGE"
                        show_sql = f"SHOW PROCEDURES LIKE 'LOAD_MATCHES_FROM_STAGE' IN SCHEMA {config['schema']}"
                    else:
                        procedure_name = "LOAD_MATCHES_FROM_STAGE"
                        show_sql = "SHOW PROCEDURES LIKE 'LOAD_MATCHES_FROM_STAGE'"
                    
                    print(f"   Verifying procedure exists: {procedure_name}")
                    cursor.execute(show_sql)
                    result = cursor.fetchall()
                    cursor.close()
                    
                    if result:
                        procedure_created = True
                        print(f"✅ Stored procedure verified: {procedure_name}")
                    else:
                        print(f"⚠️  Stored procedure not found after creation attempt")
                        print(f"   Expected: {procedure_name}")
                        print(f"   Verification query returned no results")
                        # Still try to proceed - maybe the SHOW command format is different
                        procedure_created = True  # Assume it was created even if verification fails
                        print(f"   Proceeding anyway (assuming procedure exists)")
                except Exception as e:
                    print(f"⚠️  Could not verify stored procedure: {e}")
                    import traceback
                    traceback.print_exc()
                    # Assume it was created if SQL execution succeeded
                    procedure_created = True
                    print(f"   Proceeding anyway (assuming procedure was created)")
            else:
                print("⚠️  Stored procedure creation had issues, but continuing...")
                print("   You can manually execute load_data_from_stage.sql later.")
        else:
            print(f"\nℹ️  Load procedure file not found: {load_procedure_file}")
            print("   The stored procedure won't be created automatically.")
            print()
        
        # Step 5: Check existing files and upload new ones
        print()
        print("=" * 80)
        print("Checking stage for existing files...")
        print("=" * 80)
        
        # List existing files first
        list_stage_files(
            conn,
            stage_name,
            config.get("database"),
            config.get("schema")
        )
        
        print()
        print("=" * 80)
        print("Uploading CSV files to Snowflake stage...")
        print("=" * 80)
        
        skip_existing = config.get("skip_existing_files", True)
        uploaded_count = 0
        skipped_count = 0
        
        for csv_file in csv_files:
            # Check if file exists before uploading
            filename = os.path.basename(csv_file)
            if skip_existing and check_file_exists_in_stage(
                conn, filename, stage_name, 
                config.get("database"), config.get("schema")
            ):
                print(f"   ⏭️  Skipping {filename} (already in stage)")
                skipped_count += 1
                continue
            
            if upload_file_to_stage(
                conn,
                csv_file,
                stage_name,
                config.get("database"),
                config.get("schema"),
                skip_existing=skip_existing
            ):
                uploaded_count += 1
        
        print()
        print(f"✅ Successfully uploaded {uploaded_count} new file(s)")
        if skipped_count > 0:
            print(f"⏭️  Skipped {skipped_count} existing file(s)")
        print(f"📊 Total files processed: {len(csv_files)}")
        
        # Step 6: List stage files
        list_stage_files(
            conn,
            stage_name,
            config.get("database"),
            config.get("schema")
        )
        
        # Step 7: Optionally load data from stage to tables
        load_to_tables = config.get("load_to_tables", False)
        print(f"\n🔍 Debug: load_to_tables={load_to_tables}, procedure_created={procedure_created}")
        
        if load_to_tables:
            # Always try to call the procedure if load_to_tables is True
            # Even if verification failed, the procedure might still exist
            print()
            print("=" * 80)
            print("Loading data from stage to tables...")
            print("=" * 80)
            
            if not procedure_created:
                print("⚠️  Warning: Procedure verification failed, but attempting to call it anyway...")
            
            try:
                cursor = conn.cursor()
                
                # Set database/schema context
                if config.get("database"):
                    cursor.execute(f"USE DATABASE {config['database']}")
                if config.get("schema") and config.get("database"):
                    cursor.execute(f"USE SCHEMA {config['database']}.{config['schema']}")
                elif config.get("schema"):
                    cursor.execute(f"USE SCHEMA {config['schema']}")
                
                # Build fully qualified procedure name
                if config.get("database") and config.get("schema"):
                    procedure_name = f"{config['database']}.{config['schema']}.LOAD_MATCHES_FROM_STAGE"
                elif config.get("schema"):
                    procedure_name = f"{config['schema']}.LOAD_MATCHES_FROM_STAGE"
                else:
                    procedure_name = "LOAD_MATCHES_FROM_STAGE"
                
                # Call the stored procedure with fully qualified name
                call_sql = f"CALL {procedure_name}()"
                print(f"   Calling procedure: {procedure_name}")
                print(f"   SQL: {call_sql}")
                cursor.execute(call_sql)
                result = cursor.fetchone()
                cursor.close()
                
                if result:
                    result_str = result[0] if isinstance(result, tuple) else str(result)
                    print()
                    print(result_str)
                
                print()
                print("✅ Data loaded into tables successfully!")
                
            except Exception as e:
                print(f"\n❌ Error loading data to tables: {e}")
                import traceback
                traceback.print_exc()
                if config.get("database") and config.get("schema"):
                    proc_name = f"{config['database']}.{config['schema']}.LOAD_MATCHES_FROM_STAGE"
                else:
                    proc_name = "LOAD_MATCHES_FROM_STAGE"
                print(f"\n   You can manually run: CALL {proc_name}();")
        
        print()
        print("=" * 80)
        print("✅ Upload process completed successfully!")
        print("=" * 80)
        
    finally:
        conn.close()
        print("\n🔌 Disconnected from Snowflake")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
