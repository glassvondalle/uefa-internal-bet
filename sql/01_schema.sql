-- European Club Cups - Database Schema
-- PostgreSQL / Neon

CREATE TABLE IF NOT EXISTS european_club_cups_matches (
    match_id      TEXT PRIMARY KEY,
    competition   TEXT NOT NULL,
    season        TEXT,
    phase         TEXT,
    match_date    DATE,
    home_team     TEXT,
    away_team     TEXT,
    home_goals    INTEGER,
    away_goals    INTEGER,
    load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS european_club_cups_load_log (
    id            SERIAL PRIMARY KEY,
    load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name     TEXT,
    rows_inserted INTEGER,
    status        TEXT
);

-- Calendario de cada fase. Se reemplaza en cada carga desde params/scraper_params.json
-- (scraper/load_to_db.py). Lo usa la vista team_status para saber si una fase terminó.
CREATE TABLE IF NOT EXISTS competition_phases (
    competition      TEXT    NOT NULL,
    phase            TEXT    NOT NULL,
    season           TEXT,
    phase_order      INTEGER NOT NULL,
    start_date       DATE    NOT NULL,
    end_date         DATE    NOT NULL,
    matches_per_team INTEGER NOT NULL,   -- liga: 8 (UCL/UEL) o 6 (UECL); ida y vuelta: 2; final: 1
    PRIMARY KEY (competition, phase)
);

CREATE TABLE IF NOT EXISTS participantes (
    jugador TEXT,
    team    TEXT,
    PRIMARY KEY (jugador, team)
);
