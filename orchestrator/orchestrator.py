"""
Orchestrator Script - Coordinates the entire data pipeline.

This script orchestrates the complete workflow:
1. Executes get-results.py to scrape and generate CSV files
2. Calls upload_to_snowflake.py functions to upload CSV files to Snowflake stage
3. Optionally loads data from stage to tables using the stored procedure

Note: Run create_objects.py first to create the database objects and stored procedure.
"""

import json
import os
import sys
import subprocess
from pathlib import Path
from typing import Optional
import snowflake.connector

# Import functions from upload_to_snowflake.py
try:
    from upload_to_snowflake import (
        load_config,
        connect_to_snowflake,
        find_csv_files,
        upload_file_to_stage,
        check_file_exists_in_stage,
        list_stage_files
    )
except ImportError as e:
    print(f"❌ Error importing from upload_to_snowflake.py: {e}")
    print("   Make sure upload_to_snowflake.py is in the same directory.")
    sys.exit(1)


# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent.absolute()


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
    print("Step 1: Executing scraper script...")
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


def load_data_to_tables(conn: snowflake.connector.SnowflakeConnection, config: dict) -> bool:
    """
    Call the stored procedure to load data from stage to tables.
    
    Args:
        conn: Snowflake connection
        config: Configuration dictionary
    
    Returns:
        True if loading was successful, False otherwise
    """
    print()
    print("=" * 80)
    print("Step 3: Loading data from stage to tables...")
    print("=" * 80)
    
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
        return True
        
    except Exception as e:
        print(f"\n❌ Error loading data to tables: {e}")
        import traceback
        traceback.print_exc()
        if config.get("database") and config.get("schema"):
            proc_name = f"{config['database']}.{config['schema']}.LOAD_MATCHES_FROM_STAGE"
        else:
            proc_name = "LOAD_MATCHES_FROM_STAGE"
        print(f"\n   You can manually run: CALL {proc_name}();")
        return False


def main():
    """Main execution function."""
    print("=" * 80)
    print("European Club Cups Data Pipeline - Orchestrator")
    print("=" * 80)
    print()
    print(f"Working directory: {Path.cwd()}")
    print()
    
    # Load configuration
    config = load_config()
    stage_name = config.get("stage_name", "EUROPEAN_CUPS_STAGE")
    load_to_tables = config.get("load_to_tables", False)
    
    # Step 1: Execute scraper
    if not execute_scraper():
        print("\n⚠️  Scraper execution had issues, but continuing with upload...")
        print()
    
    # Step 2: Find CSV files and upload to Snowflake
    print()
    print("=" * 80)
    print("Step 2: Finding and uploading CSV files to Snowflake...")
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
    
    # Connect to Snowflake
    try:
        conn = connect_to_snowflake(config)
    except Exception as e:
        print(f"\n❌ Failed to connect to Snowflake: {e}")
        sys.exit(1)
    
    try:
        # Check existing files and upload
        print()
        print("Checking stage for existing files...")
        list_stage_files(
            conn,
            stage_name,
            config.get("database"),
            config.get("schema")
        )
        
        print()
        print("Uploading CSV files to Snowflake stage...")
        
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
        
        # List stage files after upload
        list_stage_files(
            conn,
            stage_name,
            config.get("database"),
            config.get("schema")
        )
        
        # Step 3: Optionally load data from stage to tables
        if load_to_tables:
            load_data_to_tables(conn, config)
        else:
            print()
            print("ℹ️  Skipping data load to tables (load_to_tables=false in config)")
            print("   Set 'load_to_tables': true in snowflake_config.json to enable automatic loading")
        
        print()
        print("=" * 80)
        print("✅ Pipeline completed successfully!")
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
