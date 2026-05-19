"""
One-time database setup script.
Creates all tables, views, and seeds participant data in Neon.

Usage:
    pip install psycopg2-binary
    python setup_db.py
"""

import json
import sys
from pathlib import Path

import psycopg2

ROOT_DIR = Path(__file__).parent.absolute()
SQL_DIR = ROOT_DIR / "sql"

SQL_FILES = [
    SQL_DIR / "01_schema.sql",
    SQL_DIR / "03_views.sql",
    SQL_DIR / "04_participants.sql",
]


def get_db_url() -> str:
    config_path = ROOT_DIR / "params" / "db_config.json"
    try:
        with open(config_path) as f:
            return json.load(f)["database_url"]
    except FileNotFoundError:
        print(f"❌ Config file not found: {config_path}")
        sys.exit(1)


def run_sql_file(cur, path: Path):
    print(f"   Running {path.name}...")
    sql = path.read_text(encoding="utf-8")
    cur.execute(sql)
    print(f"   ✅ {path.name} done")


def main():
    print("=" * 60)
    print("European Club Cups — Database Setup")
    print("=" * 60)

    db_url = get_db_url()

    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        print("✅ Connected to Neon\n")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        sys.exit(1)

    with conn.cursor() as cur:
        for sql_file in SQL_FILES:
            try:
                run_sql_file(cur, sql_file)
            except Exception as e:
                print(f"   ❌ Error in {sql_file.name}: {e}")
                conn.close()
                sys.exit(1)

    conn.close()
    print("\n✅ Database setup complete.")
    print("   Next steps:")
    print("   1. Run: python scraper/get_results.py")
    print("   2. Run: python scraper/load_to_db.py")


if __name__ == "__main__":
    main()
