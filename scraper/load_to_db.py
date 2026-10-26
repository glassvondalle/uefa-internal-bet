"""
Loads CSV match files from output/ into the Neon PostgreSQL database.
Run this after get_results.py has generated the CSV files.

Usage:
    python scraper/load_to_db.py
"""

import csv
import json
import sys
from pathlib import Path

import psycopg2

SCRIPT_DIR = Path(__file__).parent.absolute()
ROOT_DIR = SCRIPT_DIR.parent
OUTPUT_DIR = ROOT_DIR / "output"
PARAMS_PATH = ROOT_DIR / "params" / "scraper_params.json"
COMPETITION_PREFIXES = ["UCL_", "UEL_", "UECL_"]
COMPETITIONS = ["UCL", "UEL", "UECL"]
# Phase order matters: team_status uses phase_order + 1 to find the next round.
PHASES = ["LEAGUE_PHASE", "PLAYOFF", "ROUND_OF_16", "QUARTER_FINAL", "SEMI_FINAL", "FINAL"]


def get_db_url() -> str:
    config_path = ROOT_DIR / "params" / "db_config.json"
    try:
        with open(config_path) as f:
            return json.load(f)["database_url"]
    except FileNotFoundError:
        print(f"❌ Config file not found: {config_path}")
        sys.exit(1)


def connect(db_url: str):
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = False
        print("✅ Connected to Neon")
        return conn
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        sys.exit(1)


def load_phases(conn) -> int:
    """Replaces competition_phases with the phase calendar from scraper_params.json."""
    with open(PARAMS_PATH, encoding="utf-8") as f:
        params = json.load(f)

    rows = []
    for comp in COMPETITIONS:
        for order, phase in enumerate(PHASES, start=1):
            if phase == "LEAGUE_PHASE":
                matches_per_team = params[f"{comp}_LEAGUE_PHASE_MATCHES_PER_TEAM"]
            elif phase == "FINAL":
                matches_per_team = 1
            else:
                matches_per_team = 2
            rows.append((
                comp, phase, params.get("SEASON"), order,
                params[f"{comp}_{phase}_INITIAL_DATE"],
                params[f"{comp}_{phase}_END_DATE"],
                matches_per_team,
            ))

    with conn.cursor() as cur:
        cur.execute("DELETE FROM competition_phases")
        cur.executemany(
            """
            INSERT INTO competition_phases
                (competition, phase, season, phase_order, start_date, end_date, matches_per_team)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def find_csv_files():
    files = [
        f for f in OUTPUT_DIR.glob("*_matches.csv")
        if any(f.name.startswith(p) for p in COMPETITION_PREFIXES)
    ]
    return sorted(files)


def load_csv(conn, csv_path: Path) -> int:
    competition = csv_path.stem.split("_")[0]

    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print(f"   ⚠️  {csv_path.name} is empty — skipping")
        return 0

    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM european_club_cups_matches WHERE competition = %s",
            (competition,)
        )

        cur.executemany(
            """
            INSERT INTO european_club_cups_matches
                (match_id, competition, season, phase, match_date,
                 home_team, away_team, home_goals, away_goals)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (match_id) DO UPDATE SET
                season        = EXCLUDED.season,
                phase         = EXCLUDED.phase,
                match_date    = EXCLUDED.match_date,
                home_team     = EXCLUDED.home_team,
                away_team     = EXCLUDED.away_team,
                home_goals    = EXCLUDED.home_goals,
                away_goals    = EXCLUDED.away_goals,
                load_datetime = CURRENT_TIMESTAMP
            """,
            [
                (
                    r["MATCH_ID"], r["COMPETITION"], r["SEASON"], r["PHASE"],
                    r["MATCH_DATE"], r["HOME_TEAM"], r["AWAY_TEAM"],
                    int(r["HOME_GOALS"]), int(r["AWAY_GOALS"]),
                )
                for r in rows
            ],
        )

        cur.execute(
            "INSERT INTO european_club_cups_load_log (file_name, rows_inserted, status) VALUES (%s, %s, %s)",
            (csv_path.name, len(rows), "SUCCESS"),
        )

    conn.commit()
    return len(rows)


def main():
    print("=" * 60)
    print("European Club Cups — Load CSV to Neon")
    print("=" * 60)

    db_url = get_db_url()
    conn = connect(db_url)

    print(f"Loading phase calendar from {PARAMS_PATH.name}...")
    try:
        print(f"   ✅ {load_phases(conn)} phases loaded")
    except Exception as e:
        conn.rollback()
        print(f"   ❌ Failed: {e}")
        conn.close()
        sys.exit(1)

    csv_files = find_csv_files()
    if not csv_files:
        print(f"❌ No CSV files found in {OUTPUT_DIR}")
        print("   Run get_results.py first.")
        conn.close()
        sys.exit(1)

    print(f"\nFound {len(csv_files)} file(s):")
    for f in csv_files:
        print(f"   - {f.name}")
    print()

    total = 0
    for csv_path in csv_files:
        print(f"Loading {csv_path.name}...")
        try:
            rows = load_csv(conn, csv_path)
            print(f"   ✅ {rows} rows loaded")
            total += rows
        except Exception as e:
            conn.rollback()
            print(f"   ❌ Failed: {e}")

    conn.close()
    print(f"\n✅ Done. Total rows loaded: {total}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
