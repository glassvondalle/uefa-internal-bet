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

CREATE TABLE IF NOT EXISTS participantes (
    jugador TEXT,
    team    TEXT,
    PRIMARY KEY (jugador, team)
);
