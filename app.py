"""
UEFA BET 2025/26 — Streamlit App
"""

import json
from collections import Counter
import streamlit as st
import psycopg2
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.absolute()

COMPETITION_COLORS = {
    "UCL":  {"primary": "#0E1E5B", "secondary": "#3562A6", "light": "#5b8dd9", "icon": "🔵"},
    "UEL":  {"primary": "#8B3A00", "secondary": "#D85C00", "light": "#FAC40B", "icon": "🟠"},
    "UECL": {"primary": "#004d00", "secondary": "#007c00", "light": "#26d26d", "icon": "🟢"},
}

MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}


# ---------------------------------------------------------------------------
# Conexión
# ---------------------------------------------------------------------------

def _get_db_url() -> str:
    try:
        return st.secrets["DATABASE_URL"]
    except Exception:
        config_path = SCRIPT_DIR / "params" / "db_config.json"
        with open(config_path) as f:
            return json.load(f)["database_url"]


@st.cache_resource
def get_connection():
    conn = psycopg2.connect(_get_db_url())
    conn.autocommit = True
    return conn


def _fetch(query: str, params=None) -> pd.DataFrame:
    # Neon suspends idle connections; retry once with a fresh connection if needed.
    for attempt in range(2):
        try:
            conn = get_connection()
            cur  = conn.cursor()
            cur.execute(query, params)
            cols = [d[0].upper() for d in cur.description]
            rows = cur.fetchall()
            cur.close()
            return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
        except Exception as e:
            if attempt == 0:
                get_connection.clear()   # drop stale connection, reconnect next loop
            else:
                st.error(f"❌ Error en consulta: {e}")
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Consultas
# ---------------------------------------------------------------------------

def query_reclasificacion() -> pd.DataFrame:
    return _fetch(
        "SELECT jugador, pts, avg FROM reclasificacion ORDER BY avg DESC NULLS LAST, pts DESC"
    )


def query_jugador_details(jugador: str) -> pd.DataFrame:
    return _fetch(
        """
        SELECT a.competition, a.team, a.mp, a.w, a.d, a.l, a.pts, a.pos
        FROM apuesta_table a
        INNER JOIN participantes p ON a.team = p.team
        WHERE p.jugador = %s
        ORDER BY a.competition, a.pos
        """,
        (jugador,),
    )


def query_team_statuses() -> pd.DataFrame:
    """
    Devuelve (competition, team, is_alive, status_label) para todos los equipos.

    Para rondas de ida y vuelta (Playoff, Octavos, Cuartos, Semis) calcula el
    ganador por marcador agregado, lo que permite distinguir al eliminado del
    superviviente incluso antes de que la siguiente ronda se haya jugado.
    """
    return _fetch("""
        WITH
        -- Marcador agregado por eliminatoria (ida + vuelta)
        -- LEAST/GREATEST ordena los equipos alfabéticamente para agrupar ambas piernas.
        -- t1 = equipo con nombre menor, t2 = mayor.
        tie_agg AS (
            SELECT competition, phase,
                LEAST(home_team, away_team)    AS t1,
                GREATEST(home_team, away_team) AS t2,
                SUM(CASE WHEN home_team < away_team THEN home_goals ELSE away_goals END) AS t1_goals,
                SUM(CASE WHEN home_team < away_team THEN away_goals ELSE home_goals END) AS t2_goals
            FROM european_club_cups_matches
            WHERE phase IN ('PLAYOFF','ROUND_OF_16','QUARTER_FINAL','SEMI_FINAL')
            GROUP BY competition, phase,
                     LEAST(home_team, away_team), GREATEST(home_team, away_team)
        ),
        tie_outcomes AS (
            SELECT ta.competition, ta.phase,
                CASE
                    WHEN ta.t1_goals > ta.t2_goals THEN ta.t1
                    WHEN ta.t2_goals > ta.t1_goals THEN ta.t2
                    -- Aggregate draw (settled by penalties): winner is whichever team
                    -- appears in the next phase (works once that phase has data).
                    WHEN ta.t1_goals = ta.t2_goals AND EXISTS (
                        SELECT 1 FROM european_club_cups_matches m
                        WHERE m.competition = ta.competition
                          AND m.phase = CASE ta.phase
                              WHEN 'PLAYOFF'       THEN 'ROUND_OF_16'
                              WHEN 'ROUND_OF_16'   THEN 'QUARTER_FINAL'
                              WHEN 'QUARTER_FINAL' THEN 'SEMI_FINAL'
                              WHEN 'SEMI_FINAL'    THEN 'FINAL' END
                          AND (m.home_team = ta.t1 OR m.away_team = ta.t1)
                    ) THEN ta.t1
                    WHEN ta.t1_goals = ta.t2_goals AND EXISTS (
                        SELECT 1 FROM european_club_cups_matches m
                        WHERE m.competition = ta.competition
                          AND m.phase = CASE ta.phase
                              WHEN 'PLAYOFF'       THEN 'ROUND_OF_16'
                              WHEN 'ROUND_OF_16'   THEN 'QUARTER_FINAL'
                              WHEN 'QUARTER_FINAL' THEN 'SEMI_FINAL'
                              WHEN 'SEMI_FINAL'    THEN 'FINAL' END
                          AND (m.home_team = ta.t2 OR m.away_team = ta.t2)
                    ) THEN ta.t2
                END AS winner,
                CASE
                    WHEN ta.t1_goals > ta.t2_goals THEN ta.t2
                    WHEN ta.t2_goals > ta.t1_goals THEN ta.t1
                    WHEN ta.t1_goals = ta.t2_goals AND EXISTS (
                        SELECT 1 FROM european_club_cups_matches m
                        WHERE m.competition = ta.competition
                          AND m.phase = CASE ta.phase
                              WHEN 'PLAYOFF'       THEN 'ROUND_OF_16'
                              WHEN 'ROUND_OF_16'   THEN 'QUARTER_FINAL'
                              WHEN 'QUARTER_FINAL' THEN 'SEMI_FINAL'
                              WHEN 'SEMI_FINAL'    THEN 'FINAL' END
                          AND (m.home_team = ta.t1 OR m.away_team = ta.t1)
                    ) THEN ta.t2
                    WHEN ta.t1_goals = ta.t2_goals AND EXISTS (
                        SELECT 1 FROM european_club_cups_matches m
                        WHERE m.competition = ta.competition
                          AND m.phase = CASE ta.phase
                              WHEN 'PLAYOFF'       THEN 'ROUND_OF_16'
                              WHEN 'ROUND_OF_16'   THEN 'QUARTER_FINAL'
                              WHEN 'QUARTER_FINAL' THEN 'SEMI_FINAL'
                              WHEN 'SEMI_FINAL'    THEN 'FINAL' END
                          AND (m.home_team = ta.t2 OR m.away_team = ta.t2)
                    ) THEN ta.t1
                END AS loser
            FROM tie_agg ta
        ),
        -- Final: partido único
        final_outcome AS (
            SELECT competition,
                CASE WHEN home_goals > away_goals THEN home_team ELSE away_team END AS winner,
                CASE WHEN home_goals > away_goals THEN away_team ELSE home_team END AS loser
            FROM european_club_cups_matches
            WHERE phase = 'FINAL' AND home_goals <> away_goals
        ),
        -- Todos los eliminados con su fase de eliminación
        eliminated AS (
            SELECT competition, phase AS elim_phase, loser AS team
            FROM   tie_outcomes WHERE loser IS NOT NULL
            UNION ALL
            SELECT competition, 'FINAL', loser FROM final_outcome WHERE loser IS NOT NULL
        ),
        -- Fase máxima alcanzada por cada equipo
        team_phases AS (
            SELECT competition, home_team AS team, phase FROM european_club_cups_matches
            UNION
            SELECT competition, away_team, phase FROM european_club_cups_matches
        ),
        team_max AS (
            SELECT competition, team,
                MAX(CASE phase
                    WHEN 'LEAGUE_PHASE'  THEN 1
                    WHEN 'PLAYOFF'       THEN 2
                    WHEN 'ROUND_OF_16'   THEN 3
                    WHEN 'QUARTER_FINAL' THEN 4
                    WHEN 'SEMI_FINAL'    THEN 5
                    WHEN 'FINAL'         THEN 6
                    ELSE 0 END) AS max_ord
            FROM team_phases GROUP BY competition, team
        ),
        final_champ AS (
            SELECT competition, winner AS team FROM final_outcome WHERE winner IS NOT NULL
        )
        SELECT
            tm.competition,
            tm.team,
            -- Vivo = no está en la lista de eliminados Y jugó al menos una ronda
            (el.team IS NULL AND tm.max_ord > 1) AS is_alive,
            CASE
                WHEN fc.team  IS NOT NULL              THEN '🏆 Campeón'
                WHEN el.elim_phase = 'FINAL'           THEN '🥈 Finalista'
                WHEN el.team IS NULL AND tm.max_ord>=5 THEN '⚽ En la Final'
                WHEN el.elim_phase = 'SEMI_FINAL'      THEN 'Eliminado en Semifinales'
                WHEN el.team IS NULL AND tm.max_ord=4  THEN '🔥 En Cuartos de Final'
                WHEN el.elim_phase = 'QUARTER_FINAL'   THEN 'Eliminado en Cuartos de Final'
                WHEN el.team IS NULL AND tm.max_ord=3  THEN '🔥 En Octavos de Final'
                WHEN el.elim_phase = 'ROUND_OF_16'     THEN 'Eliminado en Octavos de Final'
                WHEN el.team IS NULL AND tm.max_ord=2  THEN '🔥 En Play-off'
                WHEN el.elim_phase = 'PLAYOFF'         THEN 'Eliminado en Play-off'
                ELSE                                        'Eliminado en Fase de Liga'
            END AS status_label
        FROM      team_max    tm
        LEFT JOIN eliminated  el ON tm.competition = el.competition AND tm.team = el.team
        LEFT JOIN final_champ fc ON tm.competition = fc.competition AND tm.team = fc.team
    """)


def query_real_table(competition: str) -> pd.DataFrame:
    return _fetch(
        """
        SELECT team, mp, w, d, l, gf, ga, gd, pts, pos
        FROM real_table WHERE competition = %s ORDER BY pos
        """,
        (competition,),
    )


def query_all_participants() -> pd.DataFrame:
    """Returns (jugador, competition, team) for every participant-team pair."""
    return _fetch("""
        SELECT p.jugador, a.competition, p.team
        FROM participantes p
        JOIN apuesta_table a ON p.team = a.team
        ORDER BY p.jugador, a.competition
    """)


def query_finals() -> pd.DataFrame:
    """One row per competition: both finalist teams; winner/runnerup populated once played."""
    return _fetch("""
        SELECT competition, home_team, away_team, home_goals, away_goals,
            CASE WHEN home_goals > away_goals THEN home_team
                 WHEN away_goals > home_goals THEN away_team END AS winner,
            CASE WHEN home_goals > away_goals THEN away_team
                 WHEN away_goals > home_goals THEN home_team END AS runnerup
        FROM european_club_cups_matches
        WHERE phase = 'FINAL'
    """)


def query_participants_map() -> pd.DataFrame:
    return _fetch("SELECT jugador, team FROM participantes ORDER BY jugador")


# ---------------------------------------------------------------------------
# Prize helpers
# ---------------------------------------------------------------------------

def compute_shirt_winners(
    finals_df: pd.DataFrame,
    parts_df:  pd.DataFrame,
    status_df: pd.DataFrame,
) -> dict:
    """
    Assigns one shirt per competition. Priority: UCL → UEL → UECL.
    The player whose cup-winning team they hold gets the shirt.
    If that player already holds a higher-priority shirt, the shirt goes
    to the player who holds the runner-up team instead.
    Returns {comp: info_dict} where info_dict has keys:
        state       : 'decided' | 'pending' | 'no_data'
        (decided)   : winner_team, winner_player, runnerup_team, runnerup_player,
                      shirt_player, note, home_team, away_team, home_goals, away_goals
        (pending)   : team1, player1, team2, player2
    """
    team_to_player = dict(zip(parts_df['TEAM'], parts_df['JUGADOR']))
    assigned = {}   # player -> comp of their shirt
    result   = {}

    for comp in ['UCL', 'UEL', 'UECL']:
        row = finals_df[finals_df['COMPETITION'] == comp]
        if row.empty:
            # Final not recorded yet — derive finalists from status (⚽ En la Final)
            mask  = (status_df['COMPETITION'] == comp) & status_df['STATUS_LABEL'].str.contains('⚽', na=False)
            teams = status_df.loc[mask, 'TEAM'].tolist()
            if len(teams) >= 2:
                result[comp] = {
                    'state':   'pending',
                    'team1':   teams[0], 'player1': team_to_player.get(teams[0]),
                    'team2':   teams[1], 'player2': team_to_player.get(teams[1]),
                }
            else:
                result[comp] = {'state': 'no_data'}
            continue

        r = row.iloc[0]
        home_team = str(r['HOME_TEAM'])
        away_team = str(r['AWAY_TEAM'])
        winner_team   = r['WINNER']   if pd.notna(r.get('WINNER'))   else None
        runnerup_team = r['RUNNERUP'] if pd.notna(r.get('RUNNERUP')) else None

        if winner_team is None:
            result[comp] = {
                'state':   'pending',
                'team1':   home_team, 'player1': team_to_player.get(home_team),
                'team2':   away_team, 'player2': team_to_player.get(away_team),
            }
            continue

        home_goals = int(r['HOME_GOALS']) if pd.notna(r.get('HOME_GOALS')) else None
        away_goals = int(r['AWAY_GOALS']) if pd.notna(r.get('AWAY_GOALS')) else None

        winner_player   = team_to_player.get(str(winner_team))
        runnerup_player = team_to_player.get(str(runnerup_team))

        note = None
        if winner_player not in assigned:
            shirt_player = winner_player
        elif winner_player == runnerup_player:
            # Same player owns both finalist teams — they already have a shirt,
            # no one else to pass it to.
            shirt_player = None
            note = f"{winner_player} ya ganó Copa {assigned[winner_player]} y tiene ambos finalistas"
        else:
            note = f"{winner_player} ya ganó Copa {assigned[winner_player]}"
            shirt_player = runnerup_player

        if shirt_player:
            assigned[shirt_player] = comp

        result[comp] = {
            'state':          'decided',
            'home_team':      home_team,     'away_team':      away_team,
            'home_goals':     home_goals,    'away_goals':     away_goals,
            'winner_team':    str(winner_team),   'winner_player':   winner_player,
            'runnerup_team':  str(runnerup_team),  'runnerup_player': runnerup_player,
            'shirt_player':   shirt_player,
            'note':           note,
        }

    return result


def render_prize_section(shirt_winners: dict):
    st.markdown('<p class="section-header">🎽 Camisetas</p>', unsafe_allow_html=True)
    cols = st.columns(3)

    for i, comp in enumerate(['UCL', 'UEL', 'UECL']):
        c    = COMPETITION_COLORS[comp]
        info = shirt_winners.get(comp, {'state': 'no_data'})
        state = info.get('state', 'no_data')

        header = f'<div class="pc-comp" style="color:{c["light"]}">{c["icon"]} {comp}</div>'

        if state == 'no_data':
            html = f"""<div class="prize-card">
                {header}
                <div class="pc-pend" style="color:#555">Sin datos de final</div>
            </div>"""

        elif state == 'pending':
            t1, p1 = info['team1'], info.get('player1') or '—'
            t2, p2 = info['team2'], info.get('player2') or '—'
            html = f"""<div class="prize-card">
                {header}
                <div class="pc-pend">⏳ Final por jugarse</div>
                <div class="pc-team">⚽ {t1}</div>
                <div class="pc-pl">{p1}</div>
                <div class="pc-team">⚽ {t2}</div>
                <div class="pc-pl">{p2}</div>
            </div>"""

        else:  # decided
            wt  = info['winner_team'];   wp  = info['winner_player']  or '—'
            rt  = info['runnerup_team']; rp  = info['runnerup_player'] or '—'
            sp  = info['shirt_player']  or '—'
            hg, ag = info.get('home_goals'), info.get('away_goals')
            note = info.get('note') or ''

            score_html = (f'<div class="pc-score">{info["home_team"]} {hg}–{ag} {info["away_team"]}</div>'
                          if hg is not None else '')
            note_html  = f'<div class="pc-note">⚠️ {note}</div>' if note else ''

            html = f"""<div class="prize-card" style="border-color:{c['secondary']}88">
                {header}
                {score_html}
                <div class="pc-row">🏆 {wt} → <b>{wp}</b></div>
                <div class="pc-row">🥈 {rt} → <b>{rp}</b></div>
                <div class="pc-shirt" style="color:{c['light']}">🎽 {sp}</div>
                {note_html}
            </div>"""

        cols[i].markdown(html, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Estado helpers
# ---------------------------------------------------------------------------

def format_comp_cell(statuses: list) -> str:
    """Summarises a player's 3 teams in one competition as a short text string."""
    alive_cnt = Counter()
    elim_cnt  = Counter()

    for s in (str(x) for x in statuses):
        if   '🏆' in s:                  alive_cnt['🏆 Campeón'] += 1
        elif '⚽' in s:                  alive_cnt['⚽ Final']   += 1
        elif '🔥 En Cuartos'  in s:      alive_cnt['🔥 Cuartos'] += 1
        elif '🔥 En Octavos'  in s:      alive_cnt['🔥 Octavos'] += 1
        elif '🔥 En Play-off' in s:      alive_cnt['🔥 Play-off'] += 1
        elif '🥈' in s:                  elim_cnt['Final']    += 1
        elif 'Semifinal'   in s:         elim_cnt['Semis']    += 1
        elif 'Cuartos'     in s:         elim_cnt['Cuartos']  += 1
        elif 'Octavos'     in s:         elim_cnt['Octavos']  += 1
        elif 'Play-off'    in s:         elim_cnt['Play-off'] += 1
        else:                            elim_cnt['Liga']     += 1

    parts = []
    for phase in ['🏆 Campeón', '⚽ Final', '🔥 Cuartos', '🔥 Octavos', '🔥 Play-off']:
        n = alive_cnt.get(phase, 0)
        if n:
            parts.append(f"{n} {phase}" if n > 1 else phase)

    for phase in ['Final', 'Semis', 'Cuartos', 'Octavos', 'Play-off', 'Liga']:
        n = elim_cnt.get(phase, 0)
        if n:
            label = '🥈 Finalista' if phase == 'Final' else f'Elim. {phase}'
            parts.append(f"{n} {label}" if n > 1 else label)

    return ' · '.join(parts) if parts else '—'


def get_sanity_check(df_status: pd.DataFrame) -> dict:
    """
    For each competition, returns how many teams are alive and the expected count
    based on the most advanced alive phase.

    Expected alive per competition:
      ⚽ Final        → 2   (UCL/UEL/UECL have 1 final with 2 teams)
      🔥 Cuartos      → 8   (4 QF ties × 2 teams)
      🔥 Octavos      → 16  (8 R16 ties × 2 teams)
      🏆 Campeón only → 1   (tournament complete)
      🔥 Play-off     → None (complex: 8 direct + up to 16 in playoff)
    """
    result = {}
    for comp in ['UCL', 'UEL', 'UECL']:
        labels = df_status.loc[df_status['COMPETITION'] == comp, 'STATUS_LABEL'].fillna('').tolist()

        n_champ   = sum(1 for s in labels if '🏆' in s)
        n_final   = sum(1 for s in labels if '⚽' in s)
        n_cuartos = sum(1 for s in labels if '🔥 En Cuartos'  in s)
        n_octavos = sum(1 for s in labels if '🔥 En Octavos'  in s)
        n_playoff = sum(1 for s in labels if '🔥 En Play-off' in s)
        n_alive   = n_champ + n_final + n_cuartos + n_octavos + n_playoff

        if n_champ > 0 and n_final == 0:
            expected = 1
        elif n_final > 0:
            expected = 2
        elif n_cuartos > 0:
            expected = 8
        elif n_octavos > 0:
            expected = 16
        else:
            expected = None

        result[comp] = {'alive': n_alive, 'expected': expected}
    return result


# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

def inject_css():
    st.markdown("""
    <style>
    .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }

    .metric-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid #0f3460;
        border-radius: 12px;
        padding: 1.2rem 1rem;
        text-align: center;
    }
    .metric-card .lbl { color: #7a8fa6; font-size: 0.72rem; text-transform: uppercase; letter-spacing: 1.5px; }
    .metric-card .val { color: #FFD700; font-size: 1.9rem; font-weight: 800; line-height: 1.2; }
    .metric-card .sub { color: #99aabb; font-size: 0.8rem; margin-top: 0.15rem; }

    .podium-card {
        border-radius: 14px; padding: 1.4rem 1rem;
        text-align: center; height: 100%;
    }
    .gold   { background: linear-gradient(135deg, #b8860b, #ffd700); color: #1a0a00; }
    .silver { background: linear-gradient(135deg, #606878, #c0c0c0); color: #111; }
    .bronze { background: linear-gradient(135deg, #7d4e2c, #cd7f32); color: #fff; }
    .podium-card .p-rank { font-size: 2.4rem; }
    .podium-card .p-name { font-size: 1.35rem; font-weight: 800; margin-top: 0.4rem; }
    .podium-card .p-avg  { font-size: 1.1rem;  font-weight: 700; margin-top: 0.25rem; }
    .podium-card .p-pts  { font-size: 0.85rem; opacity: 0.8;     margin-top: 0.1rem; }

    .section-header {
        font-size: 0.78rem; color: #7a9fc2;
        text-transform: uppercase; letter-spacing: 2px;
        border-bottom: 1px solid #1e2d3d;
        padding-bottom: 0.4rem; margin: 1.5rem 0 1rem 0;
    }

    .prize-card {
        background: linear-gradient(135deg, #0d0d1a 0%, #1a1a2e 100%);
        border: 1px solid #2a2a3a;
        border-radius: 12px;
        padding: 1.1rem 1rem;
        min-height: 155px;
    }
    .prize-card .pc-comp  { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 2px; font-weight: 700; margin-bottom: 0.5rem; }
    .prize-card .pc-score { font-size: 0.75rem; color: #666; margin-bottom: 0.5rem; }
    .prize-card .pc-row   { font-size: 0.84rem; color: #ccc; margin: 0.2rem 0; }
    .prize-card .pc-shirt { font-size: 1.05rem; font-weight: 800; margin-top: 0.75rem; }
    .prize-card .pc-note  { font-size: 0.7rem; color: #888; font-style: italic; margin-top: 0.25rem; }
    .prize-card .pc-pend  { font-size: 0.8rem; color: #666; margin: 0.4rem 0 0.6rem; }
    .prize-card .pc-team  { font-size: 0.84rem; color: #aaa; margin: 0.15rem 0; }
    .prize-card .pc-pl    { font-size: 0.78rem; color: #777; padding-left: 1.2rem; margin-bottom: 0.2rem; }
    </style>
    """, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Tab 1: Clasificación
# ---------------------------------------------------------------------------

def tab_ranking(df: pd.DataFrame, shirt_winners: dict):
    # --- Prize section ---
    render_prize_section(shirt_winners)

    # Which players won a shirt (only from decided finals)?
    shirt_players = {
        info['shirt_player']
        for info in shirt_winners.values()
        if info.get('state') == 'decided' and info.get('shirt_player')
    }
    player_to_cup = {
        info['shirt_player']: comp
        for comp, info in shirt_winners.items()
        if info.get('state') == 'decided' and info.get('shirt_player')
    }

    # Regular ranking excludes shirt winners
    df_regular = df[~df['JUGADOR'].isin(shirt_players)].copy().reset_index(drop=True)
    df_cups    = df[ df['JUGADOR'].isin(shirt_players)].copy()

    # --- Podio (regular ranking only) ---
    st.markdown('<p class="section-header">Podio</p>', unsafe_allow_html=True)

    top3_cols  = st.columns(3)
    pod_styles = ["gold", "silver", "bronze"]
    medals     = ["🥇", "🥈", "🥉"]

    for i, (col, (_, row)) in enumerate(zip(top3_cols, df_regular.head(3).iterrows())):
        col.markdown(f"""
        <div class="podium-card {pod_styles[i]}">
            <div class="p-rank">{medals[i]}</div>
            <div class="p-name">{row['JUGADOR']}</div>
            <div class="p-avg">{float(row['AVG']):.3f} media</div>
            <div class="p-pts">{int(row['PTS'])} pts totales</div>
        </div>""", unsafe_allow_html=True)

    # --- Bar chart (regular ranking only) ---
    st.markdown('<p class="section-header">Puntos por partido (media)</p>', unsafe_allow_html=True)

    chart = df_regular.copy()
    chart["AVG"] = chart["AVG"].astype(float)
    chart = chart.sort_values("AVG")
    n = len(chart)
    colors = [
        "#FFD700" if i == n - 1 else
        "#C0C0C0" if i == n - 2 else
        "#CD7F32" if i == n - 3 else
        "#3562A6"
        for i in range(n)
    ]

    fig = go.Figure(go.Bar(
        y=chart["JUGADOR"],
        x=chart["AVG"],
        orientation="h",
        marker_color=colors,
        text=chart["AVG"].apply(lambda v: f"{v:.3f}"),
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>Media: %{x:.3f}<br>Pts: %{customdata}<extra></extra>",
        customdata=chart["PTS"],
    ))
    fig.update_layout(
        template="plotly_dark", height=max(280, 50 * n),
        margin=dict(l=0, r=70, t=0, b=0),
        xaxis=dict(title="pts / partido", range=[0, chart["AVG"].max() * 1.22], gridcolor="#1e2d3d"),
        yaxis=dict(title=""),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- Clasificación completa ---
    st.markdown('<p class="section-header">Clasificación completa</p>', unsafe_allow_html=True)

    disp = df_regular.copy()
    disp.index = range(1, len(disp) + 1)
    disp.columns = ["Jugador", "Puntos Totales", "Media pts/partido"]
    st.dataframe(disp, use_container_width=True)

    # Cup winners pinned at the bottom with gold styling
    if not df_cups.empty:
        for _, row in df_cups.iterrows():
            cup  = player_to_cup.get(row['JUGADOR'], '?')
            c    = COMPETITION_COLORS.get(cup, {})
            st.markdown(f"""
            <div style="background:linear-gradient(90deg,#2a1800,#1a1000);
                        border:1px solid #b8860b55; border-radius:8px;
                        padding:0.6rem 1.1rem; margin:0.3rem 0;
                        display:flex; justify-content:space-between; align-items:center;">
                <span style="color:#FFD700;font-weight:700;font-size:0.95rem">{row['JUGADOR']}</span>
                <span style="color:#888;font-size:0.82rem">
                    {float(row['AVG']):.3f} media &nbsp;·&nbsp; {int(row['PTS'])} pts
                </span>
                <span style="color:#b8860b;font-size:0.85rem">
                    🎽 Ganador Copa {cup} {c.get('icon','')}
                </span>
            </div>""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Tab 2: Por Jugador
# ---------------------------------------------------------------------------

def tab_player(df_ranking: pd.DataFrame, df_statuses: pd.DataFrame):
    jugadores = df_ranking["JUGADOR"].tolist()
    selected  = st.selectbox("Selecciona jugador:", jugadores)

    row  = df_ranking[df_ranking["JUGADOR"] == selected].iloc[0]
    rank = jugadores.index(selected) + 1

    st.markdown('<p class="section-header">Resumen</p>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    c1.markdown(f"""<div class="metric-card">
        <div class="lbl">Posición</div>
        <div class="val">{MEDALS.get(rank, f"#{rank}")}</div>
        <div class="sub">Puesto {rank} de {len(jugadores)}</div>
    </div>""", unsafe_allow_html=True)
    c2.markdown(f"""<div class="metric-card">
        <div class="lbl">Puntos Totales</div>
        <div class="val">{int(row['PTS'])}</div>
        <div class="sub">Pts acumulados apuesta</div>
    </div>""", unsafe_allow_html=True)
    c3.markdown(f"""<div class="metric-card">
        <div class="lbl">Media</div>
        <div class="val">{float(row['AVG']):.3f}</div>
        <div class="sub">Puntos por partido</div>
    </div>""", unsafe_allow_html=True)

    st.markdown("")

    with st.spinner("Cargando equipos..."):
        df_details = query_jugador_details(selected)

    if df_details.empty:
        st.info("No se encontraron datos de equipos.")
        return

    # Merge dynamic status
    df = df_details.merge(
        df_statuses[["COMPETITION", "TEAM", "IS_ALIVE", "STATUS_LABEL"]],
        on=["COMPETITION", "TEAM"],
        how="left",
    )
    df["IS_ALIVE"]    = df["IS_ALIVE"].fillna(False)
    df["STATUS_LABEL"] = df["STATUS_LABEL"].fillna("Sin datos")

    df["POS"] = pd.to_numeric(df["POS"], errors="coerce")
    comp_order = {"UCL": 1, "UEL": 2, "UECL": 3}
    df["_s"]   = df["COMPETITION"].map(comp_order).fillna(9)
    df         = df.sort_values(["_s", "POS"]).drop("_s", axis=1)

    # Points by competition chart
    st.markdown('<p class="section-header">Puntos de apuesta por competición</p>', unsafe_allow_html=True)
    comp_pts   = df.groupby("COMPETITION")["PTS"].sum().reset_index()
    bar_colors = [COMPETITION_COLORS.get(c, {}).get("secondary", "#555") for c in comp_pts["COMPETITION"]]

    fig2 = go.Figure(go.Bar(
        x=comp_pts["COMPETITION"], y=comp_pts["PTS"],
        marker_color=bar_colors,
        text=comp_pts["PTS"], textposition="outside", width=0.4,
        hovertemplate="<b>%{x}</b><br>Pts: %{y}<extra></extra>",
    ))
    fig2.update_layout(
        template="plotly_dark", height=260,
        margin=dict(l=0, r=0, t=10, b=0),
        xaxis=dict(title=""),
        yaxis=dict(title="Puntos apuesta", gridcolor="#1e2d3d"),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig2, use_container_width=True)

    # Per-competition team tables
    for comp, grp in df.groupby("COMPETITION", sort=False):
        c = COMPETITION_COLORS.get(comp, {})
        st.markdown(
            f'<p class="section-header" style="color:{c.get("light","#FFD700")}">'
            f'{c.get("icon","⚽")} {comp}</p>',
            unsafe_allow_html=True,
        )

        disp = grp[["TEAM", "MP", "W", "D", "L", "PTS", "STATUS_LABEL"]].copy()
        disp.columns = ["Equipo", "PJ", "V", "E", "D", "Pts", "Estado"]

        def style_row(row, _c=c):
            estado = str(row["Estado"])
            # Alive teams have a live emoji in their label
            vivo = any(e in estado for e in ("🏆", "⚽", "🔥"))
            if "Campeón"   in estado:
                return ["background-color:#7d6000; color:#FFD700; font-weight:bold"] * len(row)
            if "Finalista" in estado:
                return ["background-color:#3a3a4a; color:#C0C0C0; font-style:italic"] * len(row)
            if vivo:
                return [f"background-color:{_c['primary']}; color:white"] * len(row)
            if "Fase de Liga" in estado:
                return ["background-color:#111; color:#333; text-decoration:line-through"] * len(row)
            return ["background-color:#1a1a1a; color:#555"] * len(row)

        st.dataframe(
            disp.style.apply(style_row, axis=1),
            hide_index=True, use_container_width=True,
        )

    # Summary by competition
    st.markdown('<p class="section-header">Resumen por competición</p>', unsafe_allow_html=True)
    summary = (
        df.groupby("COMPETITION")
        .agg(PJ=("MP", "sum"), V=("W", "sum"), E=("D", "sum"),
             D=("L", "sum"), Pts=("PTS", "sum"))
        .reset_index()
    )
    summary.columns = ["Competición", "PJ", "V", "E", "D", "Pts Apuesta"]
    st.dataframe(summary, hide_index=True, use_container_width=True)


# ---------------------------------------------------------------------------
# Tab 3: Estado
# ---------------------------------------------------------------------------

def tab_estado(df_status: pd.DataFrame):
    # --- Sanity check ---
    sanity = get_sanity_check(df_status)
    st.markdown('<p class="section-header">Verificación de equipos vivos</p>', unsafe_allow_html=True)
    cols = st.columns(3)
    for i, comp in enumerate(['UCL', 'UEL', 'UECL']):
        c         = COMPETITION_COLORS[comp]
        n_alive   = sanity[comp]['alive']
        expected  = sanity[comp]['expected']

        if expected is None:
            icon = "ℹ️"; sub = "fase en curso"
        elif n_alive == expected:
            icon = "✅"; sub = f"esperados: {expected}"
        else:
            icon = "⚠️"; sub = f"esperados: {expected} — revisar datos"

        cols[i].markdown(f"""<div class="metric-card">
            <div class="lbl">{c['icon']} {comp}</div>
            <div class="val">{n_alive} {icon}</div>
            <div class="sub">{sub}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("")

    # --- Matrix ---
    with st.spinner("Cargando estado..."):
        df_parts = query_all_participants()

    if df_parts.empty:
        st.warning("Sin datos de participantes.")
        return

    df = df_parts.merge(
        df_status[['COMPETITION', 'TEAM', 'STATUS_LABEL']],
        on=['COMPETITION', 'TEAM'],
        how='left',
    )
    df['STATUS_LABEL'] = df['STATUS_LABEL'].fillna('Sin datos')

    jugadores = sorted(df['JUGADOR'].unique())
    rows = []
    for jugador in jugadores:
        row = {'Jugador': jugador}
        for comp in ['UCL', 'UEL', 'UECL']:
            mask     = (df['JUGADOR'] == jugador) & (df['COMPETITION'] == comp)
            statuses = df.loc[mask, 'STATUS_LABEL'].tolist()
            row[comp] = format_comp_cell(statuses)
        rows.append(row)

    matrix = pd.DataFrame(rows)

    st.markdown('<p class="section-header">Estado de equipos por jugador</p>', unsafe_allow_html=True)
    st.dataframe(matrix, hide_index=True, use_container_width=True)


# ---------------------------------------------------------------------------
# Tab 4: Tabla Real
# ---------------------------------------------------------------------------

def tab_real_table():
    st.markdown('<p class="section-header">Tabla real — 3 pts / victoria</p>', unsafe_allow_html=True)

    comp = st.radio("Competición:", ["UCL", "UEL", "UECL"], horizontal=True)
    c    = COMPETITION_COLORS[comp]

    with st.spinner(f"Cargando {comp}..."):
        df = query_real_table(comp)

    if df.empty:
        st.warning("Sin datos.")
        return

    df["POS"] = pd.to_numeric(df["POS"], errors="coerce")

    bar_colors = [
        c["primary"]   if p <=  8 else
        c["secondary"] if p <= 24 else
        "#2a2a2a"
        for p in df["POS"]
    ]

    fig = go.Figure(go.Bar(
        x=df["TEAM"], y=df["PTS"],
        marker_color=bar_colors,
        text=df["POS"].apply(lambda p: f"#{int(p)}"),
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>Pts: %{y}<br>Pos: %{customdata}<extra></extra>",
        customdata=df["POS"],
    ))
    fig.update_layout(
        template="plotly_dark", height=360,
        margin=dict(l=0, r=0, t=10, b=80),
        xaxis=dict(tickangle=-45, tickfont=dict(size=10)),
        yaxis=dict(title="Puntos", gridcolor="#1e2d3d"),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)

    lc1, lc2, lc3 = st.columns(3)
    lc1.markdown(f'<span style="background:{c["primary"]};padding:3px 10px;border-radius:6px;color:white;font-size:0.8rem">■ Top 8 — Directo a Octavos</span>', unsafe_allow_html=True)
    lc2.markdown(f'<span style="background:{c["secondary"]};padding:3px 10px;border-radius:6px;color:white;font-size:0.8rem">■ 9-24 — Play-off</span>', unsafe_allow_html=True)
    lc3.markdown('<span style="background:#2a2a2a;padding:3px 10px;border-radius:6px;color:#777;font-size:0.8rem">■ 25-36 — Eliminado</span>', unsafe_allow_html=True)
    st.markdown("")

    disp = df.copy()
    disp.columns = ["Equipo", "PJ", "V", "E", "D", "GF", "GC", "DG", "Pts", "Pos"]

    def style_real_row(row, _c=c):
        try:
            pos = int(row["Pos"])
        except Exception:
            pos = 99
        if pos <=  8:
            return [f"background-color:{_c['primary']}; color:white"] * len(row)
        if pos <= 24:
            return ["background-color:#1a2a3a; color:#aac4e0"] * len(row)
        return ["background-color:#111; color:#444"] * len(row)

    st.dataframe(
        disp.style.apply(style_real_row, axis=1),
        hide_index=True, use_container_width=True, height=650,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    st.set_page_config(
        page_title="UEFA BET 2025/26",
        page_icon="⚽",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    inject_css()
    st.markdown("## ⚽ UEFA BET — 2025/26")
    st.markdown("---")

    with st.spinner("Cargando datos..."):
        df_ranking  = query_reclasificacion()
        df_status   = query_team_statuses()
        df_finals   = query_finals()
        df_parts_map = query_participants_map()

    if df_ranking.empty:
        st.warning("Sin datos. Ejecuta el scraper primero.")
        return

    shirt_winners = compute_shirt_winners(df_finals, df_parts_map, df_status)

    t1, t2, t3, t4 = st.tabs(["🏆 Clasificación", "👤 Por Jugador", "📋 Estado", "📊 Tabla Real"])

    with t1:
        tab_ranking(df_ranking, shirt_winners)
    with t2:
        tab_player(df_ranking, df_status)
    with t3:
        tab_estado(df_status)
    with t4:
        tab_real_table()


if __name__ == "__main__":
    main()
