"""
Script to upload CSV files to Snowflake stage.

This script provides functions for:
- Connecting to Snowflake
- Finding CSV files
- Uploading CSV files to Snowflake stage
- Checking existing files in stage
- Listing stage files

Note: This script is typically called by orchestrator.py, which handles the complete workflow.
You can also use the functions directly if needed.
"""

import json
import os
import sys
import glob
import tempfile
import shutil
from pathlib import Path
from typing import List, Optional, Tuple
import snowflake.connector


# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent.absolute()


def load_config(config_path: Optional[str] = None) -> dict:
    """
    Load Snowflake configuration from JSON file.
    
    Args:
        config_path: Path to the configuration file (default: snowflake_config.json in PARAMS directory at root level)
    
    Returns:
        Dictionary with configuration parameters
    """
    # If no path provided, use default in PARAMS directory at root level
    if config_path is None:
        # Script is in DML directory, config is in PARAMS directory at same root level
        # Get parent directory (root) and then go to PARAMS
        root_dir = SCRIPT_DIR.parent
        config_path = root_dir / "PARAMS" / "snowflake_config.json"
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
        print(f"   Root directory: {SCRIPT_DIR.parent}")
        print(f"   Expected PARAMS directory: {SCRIPT_DIR.parent / 'PARAMS'}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing configuration file: {e}")
        sys.exit(1)


def find_csv_files(pattern: str = "*_matches.csv", search_dir: Optional[str] = None) -> List[str]:
    """
    Find all CSV files matching the pattern in the files directory at same level as script folder.
    
    Args:
        pattern: Glob pattern to match CSV files
        search_dir: Directory to search in (default: files directory at same level as script folder)
    
    Returns:
        List of CSV file paths
    """
    # Default to files directory at same level as script folder
    if search_dir is None:
        # files folder should be at the same level as the script's folder
        # e.g., if script is in DML/, files should be in files/ at same level
        parent_dir = SCRIPT_DIR.parent
        search_dir = parent_dir / "files"
    else:
        search_dir = Path(search_dir)
    
    # Create files directory if it doesn't exist (should already exist, but just in case)
    search_dir.mkdir(parents=True, exist_ok=True)
    
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


def get_file_path_in_stage(conn: snowflake.connector.SnowflakeConnection,
                           file_name: str,
                           stage_name: str,
                           database: Optional[str] = None,
                           schema: Optional[str] = None) -> Optional[str]:
    """
    Get the full path of a file in the Snowflake stage if it exists.
    
    Args:
        conn: Snowflake connection
        file_name: Name of the file to check (just filename, not full path)
        stage_name: Name of the stage
        database: Optional database name
        schema: Optional schema name
    
    Returns:
        Full stage path to the file if it exists, None otherwise
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
                stage_file_path = file_record[0] if len(file_record) > 0 else ""
                # Compare just the filename (ignore path)
                if os.path.basename(stage_file_path) == file_name:
                    return stage_file_path
            elif isinstance(file_record, str):
                if os.path.basename(file_record) == file_name:
                    return file_record
        
        return None
        
    except Exception as e:
        print(f"      ⚠️  Could not check file in stage: {e}")
        return None


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
    
    file_path = get_file_path_in_stage(conn, file_name, stage_name, database, schema)
    return file_path is not None


def rename_all_files_in_stage(conn: snowflake.connector.SnowflakeConnection,
                              stage_name: str,
                              database: Optional[str] = None,
                              schema: Optional[str] = None) -> bool:
    """
    Rename all files in the Snowflake stage by adding _OLD suffix.
    
    Args:
        conn: Snowflake connection
        stage_name: Name of the stage
        database: Optional database name
        schema: Optional schema name
    
    Returns:
        True if all renames were successful, False otherwise
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
        
        # List all files in stage
        cursor.execute(f"LIST @{stage_path}")
        files = cursor.fetchall()
        
        if not files:
            print("      No files in stage to rename")
            cursor.close()
            return True
        
        print(f"      Found {len(files)} file(s) to rename...")
        
        renamed_count = 0
        failed_count = 0
        
        # Process each file
        for file_record in files:
            if isinstance(file_record, (list, tuple)) and len(file_record) > 0:
                stage_file_path = file_record[0]
                file_name = os.path.basename(stage_file_path)
                
                # Skip files that already have _OLD suffix
                if "_OLD" in file_name:
                    continue
                
                # Create new filename with _OLD suffix
                name_parts = os.path.splitext(file_name)
                new_file_name = f"{name_parts[0]}_OLD{name_parts[1]}"
                
                # Rename this file
                if rename_file_in_stage(conn, file_name, new_file_name, stage_name, database, schema):
                    renamed_count += 1
                else:
                    failed_count += 1
        
        cursor.close()
        
        if failed_count == 0:
            print(f"      ✅ Successfully renamed {renamed_count} file(s) to _OLD")
            return True
        else:
            print(f"      ⚠️  Renamed {renamed_count} file(s), {failed_count} failed")
            return False
            
    except Exception as e:
        error_msg = str(e)
        print(f"      ❌ Error renaming files in stage: {error_msg}")
        import traceback
        traceback.print_exc()
        return False


def rename_file_in_stage(conn: snowflake.connector.SnowflakeConnection,
                         file_name: str,
                         new_file_name: str,
                         stage_name: str,
                         database: Optional[str] = None,
                         schema: Optional[str] = None) -> bool:
    """
    Rename a file in Snowflake stage by downloading it, uploading with new name, then removing old one.
    
    Args:
        conn: Snowflake connection
        file_name: Current filename in stage
        new_file_name: New filename for the file
        stage_name: Name of the stage
        database: Optional database name
        schema: Optional schema name
    
    Returns:
        True if rename was successful, False otherwise
    """
    # Build full stage path
    if database and schema:
        stage_path = f"{database}.{schema}.{stage_name}"
    elif schema:
        stage_path = f"{schema}.{stage_name}"
    else:
        stage_path = stage_name
    
    # Get the full path of the file in stage
    stage_file_path = get_file_path_in_stage(conn, file_name, stage_name, database, schema)
    if not stage_file_path:
        print(f"      ⚠️  File {file_name} not found in stage, cannot rename")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Set context if database/schema provided
        if database:
            cursor.execute(f"USE DATABASE {database}")
        if schema and database:
            cursor.execute(f"USE SCHEMA {database}.{schema}")
        elif schema:
            cursor.execute(f"USE SCHEMA {schema}")
        
        # Create a temporary directory for downloading the file
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file_path = Path(temp_dir) / file_name
            
            # Step 1: Download the file from stage (GET)
            print(f"      Downloading {file_name} from stage for rename...")
            # GET command downloads to a directory, creating subdirectories matching stage path
            # We need to use a path that works on Windows (forward slashes for GET)
            temp_dir_path = str(temp_dir).replace('\\', '/')
            get_sql = f"GET @{stage_path}/{file_name} 'file://{temp_dir_path}'"
            cursor.execute(get_sql)
            get_results = cursor.fetchall()
            
            # Check if GET was successful
            if not get_results:
                print(f"      ⚠️  Failed to download file {file_name}")
                cursor.close()
                return False
            
            # GET creates subdirectories matching the stage structure
            # Find the downloaded file by walking the temp directory
            downloaded_file = None
            for root, dirs, files in os.walk(temp_dir):
                # Look for the file (may be in a subdirectory)
                if file_name in files:
                    downloaded_file = Path(root) / file_name
                    break
            
            if not downloaded_file or not downloaded_file.exists():
                print(f"      ⚠️  Downloaded file not found at expected location")
                print(f"      Searched in: {temp_dir}")
                cursor.close()
                return False
            
            # Step 2: Create a copy with the new name and upload it
            print(f"      Uploading as {new_file_name}...")
            new_temp_file = downloaded_file.parent / new_file_name
            shutil.copy2(downloaded_file, new_temp_file)
            
            # Upload the file with new name
            new_abs_path = new_temp_file.resolve()
            new_file_path_normalized = str(new_abs_path).replace('\\', '/')
            new_file_path_escaped = new_file_path_normalized.replace("'", "''")
            put_sql = f"PUT 'file://{new_file_path_escaped}' @{stage_path}"
            
            cursor.execute(put_sql)
            put_results = cursor.fetchall()
            
            # Check PUT result
            upload_success = False
            if put_results:
                for row in put_results:
                    if isinstance(row, (list, tuple)) and len(row) >= 7:
                        status = str(row[6]).upper()
                        if "UPLOADED" in status or "SKIPPED" in status:
                            upload_success = True
                            break
            
            if not upload_success:
                print(f"      ⚠️  Failed to upload renamed file {new_file_name}")
                cursor.close()
                return False
            
            # Step 3: Remove the original file from stage
            print(f"      Removing original file {file_name} from stage...")
            remove_sql = f"REMOVE @{stage_path}/{file_name}"
            cursor.execute(remove_sql)
            remove_results = cursor.fetchall()
            
            cursor.close()
            
            print(f"      ✅ Renamed {file_name} to {new_file_name} in stage")
            return True
            
    except Exception as e:
        error_msg = str(e)
        print(f"      ❌ Error renaming file in stage: {error_msg}")
        import traceback
        traceback.print_exc()
        return False


def upload_file_to_stage(conn: snowflake.connector.SnowflakeConnection,
                         file_path: str,
                         stage_name: str,
                         database: Optional[str] = None,
                         schema: Optional[str] = None,
                         skip_existing: bool = False) -> bool:
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
    """
    Main execution function for standalone use.
    
    Note: This function is typically called by orchestrator.py, which handles
    the complete workflow. You can also run this script directly if you only
    want to upload files that already exist.
    """
    print("=" * 80)
    print("European Club Cups Data - Snowflake Upload Script")
    print("=" * 80)
    print()
    print(f"Working directory: {Path.cwd()}")
    print()
    
    # Load configuration
    config = load_config()
    stage_name = config.get("stage_name", "EUROPEAN_CUPS_STAGE")
    
    # Find CSV files
    print("=" * 80)
    print("Finding CSV files...")
    print("=" * 80)
    csv_files = find_csv_files()
    
    if not csv_files:
        # Determine files directory path for error message
        parent_dir = SCRIPT_DIR.parent
        files_dir = parent_dir / "files"
        
        print("❌ No CSV files found matching pattern '*_matches.csv'")
        print("   Expected files like: UCL_champions_league_matches.csv")
        print(f"   Searched in: {files_dir}")
        print("   Make sure get-results.py has been executed and generated CSV files.")
        sys.exit(1)
    
    print(f"✅ Found {len(csv_files)} CSV file(s):")
    for csv_file in csv_files:
        file_size = os.path.getsize(csv_file)
        print(f"   - {csv_file} ({file_size:,} bytes)")
    
    # Connect to Snowflake
    try:
        conn = connect_to_snowflake(config)
    except Exception as e:
        print(f"\n❌ Failed to connect to Snowflake: {e}")
        sys.exit(1)
    
    try:
        # Step 1: Rename all existing files in stage to _OLD
        print()
        print("=" * 80)
        print("Step 1: Renaming existing files in stage to _OLD...")
        print("=" * 80)
        
        rename_all_files_in_stage(
            conn,
            stage_name,
            config.get("database"),
            config.get("schema")
        )
        
        # Step 2: Upload new CSV files
        print()
        print("=" * 80)
        print("Step 2: Uploading CSV files to Snowflake stage...")
        print("=" * 80)
        
        uploaded_count = 0
        
        for csv_file in csv_files:
            if upload_file_to_stage(
                conn,
                csv_file,
                stage_name,
                config.get("database"),
                config.get("schema"),
                skip_existing=False
            ):
                uploaded_count += 1
        
        print()
        print(f"✅ Successfully uploaded {uploaded_count} file(s)")
        print(f"📊 Total files processed: {len(csv_files)}")
        
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
