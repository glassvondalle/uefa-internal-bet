-- European Club Cups - Views
-- PostgreSQL / Neon

CREATE OR REPLACE VIEW european_club_cups_matches_consolidated AS
SELECT
    competition, phase, home_team, away_team, home_goals, away_goals,
    CASE WHEN home_goals > away_goals THEN 3 WHEN home_goals = away_goals THEN 1 ELSE 0 END AS home_points,
    CASE WHEN home_goals < away_goals THEN 3 WHEN home_goals = away_goals THEN 1 ELSE 0 END AS away_points,
    CASE WHEN home_goals > away_goals THEN 1 ELSE 0 END AS home_win_games,
    CASE WHEN home_goals = away_goals THEN 1 ELSE 0 END AS draw_games,
    CASE WHEN away_goals > home_goals THEN 1 ELSE 0 END AS away_win_games,
    match_id
FROM european_club_cups_matches;

-- -------------------------------------------------------------------------

CREATE OR REPLACE VIEW home_matches AS
SELECT
    home_team, competition, phase,
    COUNT(1)              AS pj,
    SUM(home_win_games)   AS wins,
    SUM(draw_games)       AS draws,
    SUM(away_win_games)   AS loses,
    SUM(home_goals)       AS gf,
    SUM(away_goals)       AS ga,
    SUM(home_points)      AS home_pts
FROM european_club_cups_matches_consolidated
GROUP BY home_team, competition, phase;

CREATE OR REPLACE VIEW away_matches AS
SELECT
    away_team, competition, phase,
    COUNT(1)              AS pj,
    SUM(away_win_games)   AS wins,
    SUM(draw_games)       AS draws,
    SUM(home_win_games)   AS loses,
    SUM(away_goals)       AS gf,
    SUM(home_goals)       AS ga,
    SUM(away_points)      AS away_pts
FROM european_club_cups_matches_consolidated
GROUP BY away_team, competition, phase;

CREATE OR REPLACE VIEW total_matches AS
    SELECT home_team AS team, competition, phase, pj, wins, draws, loses, gf, ga, gf - ga AS gd, home_pts AS pts, 'HOME' AS home_away
    FROM home_matches
    UNION ALL
    SELECT away_team, competition, phase, pj, wins, draws, loses, gf, ga, gf - ga, away_pts, 'AWAY'
    FROM away_matches;

-- -------------------------------------------------------------------------
-- Real-world table (3 pts/win, 1 pt/draw) — league phase only
-- -------------------------------------------------------------------------

CREATE OR REPLACE VIEW real_table_league_phase AS
WITH a AS (
    SELECT
        competition, phase,
        team,
        SUM(pj)   AS mp,
        SUM(wins) AS w,
        SUM(draws) AS d,
        SUM(loses) AS l,
        SUM(gf)   AS gf,
        SUM(ga)   AS ga,
        SUM(gd)   AS gd,
        SUM(pts)  AS pts,
        SUM(CASE WHEN home_away = 'AWAY' THEN gf   ELSE 0 END) AS away_gf,
        SUM(CASE WHEN home_away = 'AWAY' THEN wins ELSE 0 END) AS away_w
    FROM total_matches
    WHERE phase = 'LEAGUE_PHASE'
    GROUP BY competition, phase, team
)
SELECT
    a.*,
    RANK() OVER (PARTITION BY competition ORDER BY pts DESC, gd DESC, ga DESC, away_gf DESC, w DESC, away_w DESC) AS pos
FROM a
ORDER BY competition, pos;

-- -------------------------------------------------------------------------
-- Real-world table — all phases combined
-- -------------------------------------------------------------------------

CREATE OR REPLACE VIEW real_table AS
WITH a AS (
    SELECT
        competition,
        team,
        SUM(pj)   AS mp,
        SUM(wins) AS w,
        SUM(draws) AS d,
        SUM(loses) AS l,
        SUM(gf)   AS gf,
        SUM(ga)   AS ga,
        SUM(gd)   AS gd,
        SUM(pts)  AS pts,
        SUM(CASE WHEN home_away = 'AWAY' THEN gf   ELSE 0 END) AS away_gf,
        SUM(CASE WHEN home_away = 'AWAY' THEN wins ELSE 0 END) AS away_w
    FROM total_matches
    GROUP BY competition, team
)
SELECT
    a.*,
    RANK() OVER (PARTITION BY competition ORDER BY pts DESC, gd DESC, ga DESC, away_gf DESC, w DESC, away_w DESC) AS pos
FROM a
ORDER BY competition, pos;

-- -------------------------------------------------------------------------
-- Betting table (custom scoring: 2 pts/win, 1 pt/draw)
-- -------------------------------------------------------------------------

CREATE OR REPLACE VIEW apuesta_table AS
SELECT competition, team, mp, w, d, l, (w * 2) + d AS pts, pos
FROM real_table;

-- -------------------------------------------------------------------------
-- Player rankings
-- -------------------------------------------------------------------------

CREATE OR REPLACE VIEW reclasificacion AS
SELECT
    p.jugador,
    SUM(a.pts)                                          AS pts,
    ROUND(SUM(a.pts)::numeric / NULLIF(SUM(a.mp), 0), 3) AS avg
FROM participantes p
LEFT JOIN apuesta_table a ON p.team = a.team
GROUP BY p.jugador;
