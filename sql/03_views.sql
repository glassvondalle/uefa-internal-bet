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
-- League phase status per competition (shared by apuesta_table and team_status)
--   finished: the end date passed AND all 36 teams played every match,
--             or knockout matches already exist (the league phase is over)
-- -------------------------------------------------------------------------

CREATE OR REPLACE VIEW league_phase_status AS
SELECT
    cp.competition,
    cp.end_date,
    COUNT(lp.team) = 36                                            AS all_teams,
    COUNT(lp.team) = 36 AND BOOL_AND(lp.mp = cp.matches_per_team)  AS all_played,
    (COUNT(lp.team) = 36 AND BOOL_AND(lp.mp = cp.matches_per_team) AND CURRENT_DATE >= cp.end_date)
        OR EXISTS (SELECT 1 FROM european_club_cups_matches m
                   WHERE m.competition = cp.competition AND m.phase <> 'LEAGUE_PHASE') AS finished
FROM competition_phases cp
LEFT JOIN real_table_league_phase lp ON lp.competition = cp.competition
WHERE cp.phase = 'LEAGUE_PHASE'
GROUP BY cp.competition, cp.matches_per_team, cp.end_date;

-- -------------------------------------------------------------------------
-- Knockout ties (shared by apuesta_table and team_status)
--   One row per tie, aggregate goals over its legs.
--   leader:  team ahead on aggregate (provisional while not decided)
--   decided: all legs played with a winner on aggregate, or one team already
--            appears in the next round (resolves penalties and missing legs)
-- -------------------------------------------------------------------------

CREATE OR REPLACE VIEW knockout_ties AS
WITH
ties AS (
    SELECT m.competition, m.phase, cp.phase_order, cp.matches_per_team AS legs_needed, cp.end_date,
        LEAST(m.home_team, m.away_team)    AS t1,
        GREATEST(m.home_team, m.away_team) AS t2,
        COUNT(*) AS legs,
        SUM(CASE WHEN m.home_team < m.away_team THEN m.home_goals ELSE m.away_goals END) AS g1,
        SUM(CASE WHEN m.home_team < m.away_team THEN m.away_goals ELSE m.home_goals END) AS g2
    FROM european_club_cups_matches m
    JOIN competition_phases cp ON cp.competition = m.competition AND cp.phase = m.phase
    WHERE m.phase <> 'LEAGUE_PHASE'
    GROUP BY m.competition, m.phase, cp.phase_order, cp.matches_per_team, cp.end_date,
             LEAST(m.home_team, m.away_team), GREATEST(m.home_team, m.away_team)
),
appearances AS (
    SELECT DISTINCT m.competition, cp.phase_order, t.team
    FROM european_club_cups_matches m
    JOIN competition_phases cp ON cp.competition = m.competition AND cp.phase = m.phase
    CROSS JOIN LATERAL (VALUES (m.home_team), (m.away_team)) t(team)
),
nxt AS (
    SELECT k.*,
        EXISTS (SELECT 1 FROM appearances a
                WHERE a.competition = k.competition AND a.phase_order = k.phase_order + 1 AND a.team = k.t1) AS t1_next,
        EXISTS (SELECT 1 FROM appearances a
                WHERE a.competition = k.competition AND a.phase_order = k.phase_order + 1 AND a.team = k.t2) AS t2_next
    FROM ties k
)
SELECT competition, phase, phase_order, end_date, t1, t2, legs, legs_needed, g1, g2,
    CASE WHEN t1_next THEN t1 WHEN t2_next THEN t2
         WHEN g1 > g2 THEN t1 WHEN g2 > g1 THEN t2 END        AS leader,
    t1_next OR t2_next OR (legs >= legs_needed AND g1 <> g2)  AS decided,
    legs >= legs_needed                                       AS complete
FROM nxt;

-- -------------------------------------------------------------------------
-- Betting table
--
-- Points per match:  win=2, draw=1, loss=0  (all phases)
-- Top-8 bonus:       UCL=4, UEL=3, UECL=2  (top 8 once the league phase is over)
-- Advancement bonus: +1 per knockout tie won (R16, QF, SF, Final), once the tie is decided
--                    playoff advancement gives NO bonus
-- Bonuses are paid when the round ends, whether or not the team plays the next one.
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

-- Top-8 bonus: finished 1-8 in league phase (direct qualification to R16).
-- Paid once the whole league phase is over (league_phase_status.finished); a provisional
-- top 8 during the league phase earns nothing.
top8_bonus AS (
    SELECT lp.competition, lp.team,
        CASE lp.competition
            WHEN 'UCL'  THEN 4
            WHEN 'UEL'  THEN 3
            WHEN 'UECL' THEN 2
        END AS bonus
    FROM real_table_league_phase lp
    JOIN league_phase_status s ON s.competition = lp.competition
    WHERE lp.pos <= 8 AND s.finished
),

-- Advancement bonus: +1 per knockout tie won, excluding the play-off.
-- Paid as soon as the tie is decided (knockout_ties.decided).
advancement AS (
    SELECT competition, leader AS team, COUNT(*) AS bonus
    FROM knockout_ties
    WHERE decided AND phase IN ('ROUND_OF_16', 'QUARTER_FINAL', 'SEMI_FINAL', 'FINAL')
    GROUP BY competition, leader
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

-- -------------------------------------------------------------------------
-- Team status: una fila por equipo y competición, con flags de estado
--
--   fase_actual        fase más avanzada que jugó el equipo
--   resultado          liga:          TOP8 / PLAYOFF / ELIMINADO
--                      eliminatorias: AVANZA / ELIMINADO / EMPATE / PENALES
--                      final:         CAMPEON / FINALISTA / PENALES
--   es_definitivo      false = momentáneo, true = ya no puede cambiar
--   motivo             FECHA / MATEMATICO / ELIMINATORIA (por qué es definitivo)
--   esta_vivo          sigue en competición (los estados momentáneos cuentan como vivos)
--   datos_incompletos  la fecha de la fase ya pasó pero faltan partidos cargados
--
-- Fase de liga: 1-8 TOP8, 9-24 PLAYOFF, 25-36 ELIMINADO según la tabla actual.
--   Definitivo por FECHA: la fase terminó (ver league_phase_status).
--   Definitivo por MATEMATICO antes de eso (solo puntos, empate cuenta en contra):
--     pts_max = pts + 3 * partidos restantes
--     TOP8      como mucho 7 rivales pueden llegar a sus puntos actuales
--     ELIMINADO al menos 24 rivales ya superan su pts_max
--     PLAYOFF   como mucho 23 pueden llegar a sus puntos y al menos 8 ya superan su pts_max
-- Eliminatorias: global de ida y vuelta (ver knockout_ties); con una sola pierna es momentáneo.
-- -------------------------------------------------------------------------

CREATE OR REPLACE VIEW team_status AS
WITH
league AS (
    SELECT lp.competition, lp.team, lp.pos, lp.pts,
        cp.matches_per_team - lp.mp                AS remaining,
        lp.pts + 3 * (cp.matches_per_team - lp.mp) AS pts_max,
        cp.end_date
    FROM real_table_league_phase lp
    JOIN competition_phases cp
      ON cp.competition = lp.competition AND cp.phase = 'LEAGUE_PHASE'
),
league_math AS (
    SELECT t.competition, t.team,
        COUNT(*) FILTER (WHERE x.pts_max >= t.pts) AS rivals_can_reach,
        COUNT(*) FILTER (WHERE x.pts > t.pts_max)  AS rivals_out_of_reach
    FROM league t
    JOIN league x ON x.competition = t.competition AND x.team <> t.team
    GROUP BY t.competition, t.team
),
league_status AS (
    SELECT l.competition, l.team, l.pos, l.pts, l.pts_max, l.remaining,
        lps.finished                                  AS finished,
        lps.all_teams AND (
               m.rivals_can_reach <= 7
            OR m.rivals_out_of_reach >= 24
            OR (m.rivals_can_reach <= 23 AND m.rivals_out_of_reach >= 8)
        )                                             AS clinched,
        NOT lps.all_played AND CURRENT_DATE > lps.end_date AS datos_incompletos
    FROM league l
    JOIN league_phase_status lps ON lps.competition = l.competition
    LEFT JOIN league_math m  ON m.competition = l.competition AND m.team = l.team
),
-- La matemática siempre coincide con la posición actual, así que el resultado sale
-- de la posición y la matemática solo decide si ya es definitivo.
league_rows AS (
    SELECT competition, team,
        'LEAGUE_PHASE' AS fase_actual,
        CASE WHEN pos <= 8 THEN 'TOP8' WHEN pos <= 24 THEN 'PLAYOFF' ELSE 'ELIMINADO' END AS resultado,
        finished OR COALESCE(clinched, FALSE) AS es_definitivo,
        CASE WHEN finished THEN 'FECHA' WHEN clinched THEN 'MATEMATICO' END AS motivo,
        datos_incompletos
    FROM league_status
),
ko_latest AS (
    SELECT DISTINCT ON (d.competition, s.team)
        d.competition, s.team, d.phase, d.leader, d.decided, d.complete,
        NOT d.decided AND NOT d.complete AND CURRENT_DATE > d.end_date AS datos_incompletos
    FROM knockout_ties d
    CROSS JOIN LATERAL (VALUES (d.t1), (d.t2)) s(team)
    ORDER BY d.competition, s.team, d.phase_order DESC
),
ko_rows AS (
    SELECT competition, team,
        phase AS fase_actual,
        CASE
            WHEN leader IS NULL THEN CASE WHEN complete THEN 'PENALES' ELSE 'EMPATE' END
            WHEN phase = 'FINAL' AND decided THEN CASE WHEN leader = team THEN 'CAMPEON' ELSE 'FINALISTA' END
            WHEN leader = team THEN 'AVANZA'
            ELSE 'ELIMINADO'
        END AS resultado,
        decided AS es_definitivo,
        CASE WHEN decided THEN 'ELIMINATORIA' END AS motivo,
        datos_incompletos
    FROM ko_latest
),
combined AS (
    SELECT * FROM ko_rows
    UNION ALL
    SELECT lr.* FROM league_rows lr
    WHERE NOT EXISTS (SELECT 1 FROM ko_rows k WHERE k.competition = lr.competition AND k.team = lr.team)
)
SELECT
    c.competition,
    c.team,
    c.fase_actual,
    c.resultado,
    c.es_definitivo,
    NOT (c.es_definitivo AND c.resultado IN ('ELIMINADO', 'FINALISTA')) AS esta_vivo,
    c.motivo,
    c.datos_incompletos,
    ls.pos       AS pos_liga,
    ls.pts       AS pts_liga,
    ls.pts_max   AS pts_max_liga,
    ls.remaining AS partidos_restantes_liga
FROM combined c
LEFT JOIN league_status ls ON ls.competition = c.competition AND ls.team = c.team;
