# UEFA BET — European Club Cups

Internal betting game over the Champions League (UCL), Europa League (UEL) and
Conference League (UECL). Each player owns a set of teams; the app scores players
by how their teams do across all three competitions.

```
FlashScore ──(1) scraper/get_results.py──▶ output/*.csv ──(2) scraper/load_to_db.py──▶ Neon Postgres ──(3) app.py──▶ Streamlit
                     ▲                                                                       ▲
          params/scraper_params.json                                     setup_db.py (tables, views, participants)
```

| Step | What | Why |
|---|---|---|
| 0 | `setup_db.py` | Creates tables + views and loads the players/teams. Safe to re-run. |
| 1 | `scraper/get_results.py` | Opens FlashScore in headless Chrome, scrapes results, keeps only matches inside the phase dates of `params/scraper_params.json`, writes one CSV per cup to `output/`. Qualifiers are dropped because they fall outside those dates. |
| 2 | `scraper/load_to_db.py` | Replaces each cup's rows in the DB with the contents of its CSV. |
| 3 | `streamlit run app.py` | The dashboard. Reads the views (`apuesta_table`, `reclasificacion`, ...). |

> `ddl/`, `dml/`, `files/`, `orchestrator/` and `params/snowflake_config.json` are the
> **old v2 Snowflake version**. Nothing in the current flow uses them.

---

## One-time setup (per machine)

You need Python 3.10+ and Google Chrome. Selenium downloads the matching ChromeDriver by itself.

```powershell
cd uefa-internal-bet
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create **`params/db_config.json`** (it is git-ignored, never commit it):

```json
{ "database_url": "postgresql://USER:PASSWORD@HOST.neon.tech/DBNAME?sslmode=require" }
```

Get the connection string from the Neon console → your project → **Connect**.

---

## Routine: refresh results

```powershell
.\.venv\Scripts\Activate.ps1
python scraper/get_results.py     # ~1–3 min, writes output/UCL_… UEL_… UECL_…_matches.csv
python scraper/load_to_db.py      # pushes those CSVs to Neon
streamlit run app.py              # opens http://localhost:8501
```

Before loading, open the CSVs and check the `SEASON` column. If the scraper finds
nothing for a cup it **does not overwrite** that cup's CSV, so the old file would be loaded.

---

## Starting a new season (do this once, e.g. 2026/27)

**The DB holds one season at a time.** The views don't filter by season, and
`load_to_db.py` deletes *all* rows of a competition before inserting. Loading the new season
therefore wipes the old one.

1. **Archive last season.** In the Neon console go to **Branches → Create branch** from
   `main`, named e.g. `season-2025-26`. It's a free snapshot you can query any time.
2. **Dates.** Update `params/scraper_params.json`: set `SEASON` and every phase's
   `*_INITIAL_DATE` / `*_END_DATE` from uefa.com's "teams, dates, draws, format, final" page
   for each cup. A phase runs from its first matchday to its last; a match outside every range is ignored.
3. **Participants.** Replace the rows in `sql/04_participants.sql` with this year's draft.
   Team names must match FlashScore exactly (e.g. `Atl. Madrid`, `Bodo/Glimt`), so check
   them against the CSV `HOME_TEAM` / `AWAY_TEAM` values.
4. **Clear the old data** in the Neon SQL Editor. The participants file only *inserts*,
   so old picks stay unless you truncate:
   ```sql
   TRUNCATE european_club_cups_matches, european_club_cups_load_log, participantes;
   ```
5. `python setup_db.py` → re-creates views and inserts the new participants.
6. Run the routine above (scrape → load → app).
7. **Hard-coded leftovers from 2025/26.** Update these when they matter:
   - `app.py`: titles `UEFA BET 2025/26` (lines 2, 886, 893).
   - The UCL final was decided on penalties, and the winner is hard-coded by match id
     `UCL_2025_2026_FINAL_A299097D` in `sql/03_views.sql` (~line 203) and `app.py` (~line 250).
     For a new final that goes to penalties, put the new match id/winner there.

---

## Checking the DB

**Easiest:** Neon console → **SQL Editor**. Useful queries:

```sql
-- matches per cup / phase, and that they're the right season
SELECT competition, season, phase, COUNT(*), MIN(match_date), MAX(match_date)
FROM european_club_cups_matches
GROUP BY 1,2,3 ORDER BY 1,2,3;

-- latest results
SELECT match_date, competition, home_team, home_goals, away_goals, away_team
FROM european_club_cups_matches ORDER BY match_date DESC LIMIT 20;

-- load history
SELECT * FROM european_club_cups_load_log ORDER BY load_datetime DESC LIMIT 10;

-- participants' teams that have no matches (usually a name mismatch)
SELECT p.* FROM participantes p
WHERE NOT EXISTS (SELECT 1 FROM european_club_cups_matches m
                  WHERE p.team IN (m.home_team, m.away_team));

-- the standings the app shows
SELECT * FROM reclasificacion;
SELECT * FROM apuesta_table;
```

**From the terminal** (no extra installs, uses `params/db_config.json`):

```powershell
python -c "import json,psycopg2; c=psycopg2.connect(json.load(open('params/db_config.json'))['database_url']).cursor(); c.execute('SELECT competition, season, COUNT(*) FROM european_club_cups_matches GROUP BY 1,2'); print(*c.fetchall(), sep='\n')"
```

---

## Deploying the app (Streamlit Cloud)

`app.py` reads `st.secrets["DATABASE_URL"]` first and falls back to `params/db_config.json`.
On Streamlit Cloud, set the secret in the app settings:

```toml
DATABASE_URL = "postgresql://..."
```

For local use you can put the same line in `.streamlit/secrets.toml`, which is git-ignored.

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `Config file not found: params/db_config.json` | Create it, see setup. |
| Scraper finds 0 matches for a cup | Usually FlashScore changed a URL or the page layout. The URLs are in `COMPETITIONS` at the top of `scraper/get_results.py`. Also check that the phase dates cover the matches already played. |
| `N outside date range` is high in scraper output | The dates in `scraper_params.json` are wrong, or those matches were qualifiers (expected). |
| A player's team shows 0 points | Name in `participantes` ≠ FlashScore name. Run the "no matches" query above. |
| App shows old data | Streamlit caches queries. Use the **Rerun** / **Clear cache** menu (top-right). |

## Repo layout

```
app.py                     Streamlit dashboard
setup_db.py                runs sql/01 → 03 → 04 against Neon
scraper/get_results.py     FlashScore → output/*.csv
scraper/load_to_db.py      output/*.csv → Neon
sql/01_schema.sql          tables
sql/03_views.sql           scoring logic (league table, bet table, reclasificación)
sql/04_participants.sql    player → team picks
params/scraper_params.json season + phase date windows
params/db_config.json      DB connection (you create it, git-ignored)
output/                    scraped CSVs
```
