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
-- Betting table
--
-- Points per match:  win=2, draw=1, loss=0  (all phases)
-- Top-8 bonus:       UCL=4, UEL=3, UECL=2  (finished top 8 in league phase)
-- Advancement bonus: +1 per knockout round advanced FROM (R16, QF, SF, Final)
--                    playoff advancement gives NO bonus
-- pos: league-phase position (determines CLASIFICADO / PLAYOFFS / ELIMINADO)
-- -------------------------------------------------------------------------

CREATE OR REPLACE VIEW apuesta_table AS
WITH

-- 2/1/0 match points from ALL phases
bet_match_pts AS (
    SELECT competition, team,
        SUM(mp)        AS mp,
        SUM(wins)      AS w,
        SUM(draws)     AS d,
        SUM(loses)     AS l,
        SUM(match_pts) AS match_pts
    FROM (
        SELECT competition, home_team AS team,
            COUNT(*)                                                                AS mp,
            SUM(CASE WHEN home_goals > away_goals THEN 1 ELSE 0 END)               AS wins,
            SUM(CASE WHEN home_goals = away_goals THEN 1 ELSE 0 END)               AS draws,
            SUM(CASE WHEN home_goals < away_goals THEN 1 ELSE 0 END)               AS loses,
            SUM(CASE WHEN home_goals > away_goals THEN 2
                     WHEN home_goals = away_goals THEN 1 ELSE 0 END)               AS match_pts
        FROM european_club_cups_matches
        GROUP BY competition, home_team
        UNION ALL
        SELECT competition, away_team,
            COUNT(*),
            SUM(CASE WHEN away_goals > home_goals THEN 1 ELSE 0 END),
            SUM(CASE WHEN home_goals = away_goals THEN 1 ELSE 0 END),
            SUM(CASE WHEN away_goals < home_goals THEN 1 ELSE 0 END),
            SUM(CASE WHEN away_goals > home_goals THEN 2
                     WHEN home_goals = away_goals THEN 1 ELSE 0 END)
        FROM european_club_cups_matches
        GROUP BY competition, away_team
    ) t
    GROUP BY competition, team
),

-- Top-8 bonus: finished 1-8 in league phase (direct qualification to R16)
top8_bonus AS (
    SELECT competition, team,
        CASE competition
            WHEN 'UCL'  THEN 4
            WHEN 'UEL'  THEN 3
            WHEN 'UECL' THEN 2
        END AS bonus
    FROM real_table_league_phase
    WHERE pos <= 8
),

-- Advancement bonus: +1 per knockout round advanced FROM (excluding playoff)
-- Logic: a team "won round X" if they appear in round X+1.
-- Final winner is detected from the match result (single-leg).
-- Parentheses are required: UNION and UNION ALL have equal precedence in PostgreSQL
-- and evaluate left-to-right, so without them the trailing UNION would deduplicate
-- across all blocks, counting each team only once regardless of rounds advanced.
advancement AS (
    SELECT competition, team, COUNT(*) AS bonus
    FROM (
        -- Won R16 → appears in QF
        (
            SELECT DISTINCT competition, home_team AS team FROM european_club_cups_matches WHERE phase = 'QUARTER_FINAL'
            UNION
            SELECT DISTINCT competition, away_team           FROM european_club_cups_matches WHERE phase = 'QUARTER_FINAL'
        )
        UNION ALL
        -- Won QF → appears in SF
        (
            SELECT DISTINCT competition, home_team FROM european_club_cups_matches WHERE phase = 'SEMI_FINAL'
            UNION
            SELECT DISTINCT competition, away_team  FROM european_club_cups_matches WHERE phase = 'SEMI_FINAL'
        )
        UNION ALL
        -- Won SF → appears in Final
        (
            SELECT DISTINCT competition, home_team FROM european_club_cups_matches WHERE phase = 'FINAL'
            UNION
            SELECT DISTINCT competition, away_team  FROM european_club_cups_matches WHERE phase = 'FINAL'
        )
        UNION ALL
        -- Won Final (single-leg — higher goals wins)
        (
            SELECT competition,
                   CASE WHEN home_goals > away_goals THEN home_team ELSE away_team END AS team
            FROM european_club_cups_matches
            WHERE phase = 'FINAL' AND home_goals <> away_goals
        )
    ) adv
    GROUP BY competition, team
)

SELECT
    lp.competition,
    lp.team,
    COALESCE(b.mp,        0)                                                      AS mp,
    COALESCE(b.w,         0)                                                      AS w,
    COALESCE(b.d,         0)                                                      AS d,
    COALESCE(b.l,         0)                                                      AS l,
    COALESCE(b.match_pts, 0) + COALESCE(t.bonus, 0) + COALESCE(a.bonus, 0)      AS pts,
    lp.pos
FROM real_table_league_phase lp
LEFT JOIN bet_match_pts b ON lp.competition = b.competition AND lp.team = b.team
LEFT JOIN top8_bonus    t ON lp.competition = t.competition AND lp.team = t.team
LEFT JOIN advancement   a ON lp.competition = a.competition AND lp.team = a.team;

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
