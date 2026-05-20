"""
UEFA BET 2025/26 — Streamlit App
"""

import json
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
# Connection
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
    try:
        conn = psycopg2.connect(_get_db_url())
        conn.autocommit = True
        return conn
    except Exception as e:
        st.error(f"❌ Could not connect to database: {e}")
        st.stop()


def _fetch(query: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        cols = [d[0].upper() for d in cur.description]
        rows = cur.fetchall()
        cur.close()
        return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
    except Exception as e:
        st.error(f"❌ Query error: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Queries
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


def query_real_table(competition: str) -> pd.DataFrame:
    return _fetch(
        """
        SELECT team, mp, w, d, l, gf, ga, gd, pts, pos
        FROM real_table WHERE competition = %s ORDER BY pos
        """,
        (competition,),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_status(pos) -> str:
    try:
        pos = int(float(pos))
    except (ValueError, TypeError):
        return "UNKNOWN"
    if pos <= 8:
        return "CLASIFICADO"
    if pos <= 24:
        return "PLAYOFFS"
    return "ELIMINADO"


def css() -> str:
    return """
    <style>
    .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }

    .metric-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid #0f3460;
        border-radius: 12px;
        padding: 1.2rem 1rem;
        text-align: center;
    }
    .metric-card .lbl  { color: #7a8fa6; font-size: 0.72rem; text-transform: uppercase; letter-spacing: 1.5px; }
    .metric-card .val  { color: #FFD700; font-size: 1.9rem; font-weight: 800; line-height: 1.2; }
    .metric-card .sub  { color: #99aabb; font-size: 0.8rem; margin-top: 0.15rem; }

    .podium-card {
        border-radius: 14px;
        padding: 1.4rem 1rem;
        text-align: center;
        height: 100%;
    }
    .gold   { background: linear-gradient(135deg, #b8860b, #ffd700); color: #1a0a00; }
    .silver { background: linear-gradient(135deg, #606878, #c0c0c0); color: #111; }
    .bronze { background: linear-gradient(135deg, #7d4e2c, #cd7f32); color: #fff; }
    .podium-card .p-rank { font-size: 2.4rem; }
    .podium-card .p-name { font-size: 1.35rem; font-weight: 800; margin-top: 0.4rem; }
    .podium-card .p-avg  { font-size: 1.1rem; font-weight: 700; margin-top: 0.25rem; }
    .podium-card .p-pts  { font-size: 0.85rem; opacity: 0.8; margin-top: 0.1rem; }

    .section-header {
        font-size: 0.78rem;
        color: #7a9fc2;
        text-transform: uppercase;
        letter-spacing: 2px;
        border-bottom: 1px solid #1e2d3d;
        padding-bottom: 0.4rem;
        margin: 1.5rem 0 1rem 0;
    }
    </style>
    """


# ---------------------------------------------------------------------------
# Tab 1: Ranking
# ---------------------------------------------------------------------------

def tab_ranking(df: pd.DataFrame):
    # Podium
    st.markdown('<p class="section-header">Podium</p>', unsafe_allow_html=True)
    top3_cols = st.columns(3)
    styles = ["gold", "silver", "bronze"]
    ranks  = ["🥇", "🥈", "🥉"]
    for i, (col, (_, row)) in enumerate(zip(top3_cols, df.head(3).iterrows())):
        col.markdown(f"""
        <div class="podium-card {styles[i]}">
            <div class="p-rank">{ranks[i]}</div>
            <div class="p-name">{row['JUGADOR']}</div>
            <div class="p-avg">{float(row['AVG']):.3f} avg</div>
            <div class="p-pts">{int(row['PTS'])} pts total</div>
        </div>""", unsafe_allow_html=True)

    # Bar chart — avg
    st.markdown('<p class="section-header">Points per match (avg)</p>', unsafe_allow_html=True)
    chart = df.copy()
    chart["AVG"] = chart["AVG"].astype(float)
    chart = chart.sort_values("AVG")

    max_avg = chart["AVG"].max()
    colors  = [
        "#FFD700" if i == len(chart) - 1
        else "#C0C0C0" if i == len(chart) - 2
        else "#CD7F32" if i == len(chart) - 3
        else "#3562A6"
        for i in range(len(chart))
    ]

    fig = go.Figure(go.Bar(
        y=chart["JUGADOR"],
        x=chart["AVG"],
        orientation="h",
        marker_color=colors,
        text=chart["AVG"].apply(lambda v: f"{v:.3f}"),
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>Avg: %{x:.3f}<br>Pts: %{customdata}<extra></extra>",
        customdata=chart["PTS"],
    ))
    fig.update_layout(
        template="plotly_dark",
        height=420,
        margin=dict(l=0, r=60, t=0, b=0),
        xaxis=dict(title="avg pts / match", range=[0, max_avg * 1.22], gridcolor="#1e2d3d"),
        yaxis=dict(title=""),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Full table
    st.markdown('<p class="section-header">Full standings</p>', unsafe_allow_html=True)
    display = df.copy().reset_index(drop=True)
    display.index = range(1, len(display) + 1)
    display.columns = ["Player", "Total Pts", "Avg pts/match"]
    st.dataframe(display, use_container_width=True)


# ---------------------------------------------------------------------------
# Tab 2: Player Detail
# ---------------------------------------------------------------------------

def tab_player(df_ranking: pd.DataFrame):
    jugadores = df_ranking["JUGADOR"].tolist()
    selected  = st.selectbox("Select player:", jugadores)

    row  = df_ranking[df_ranking["JUGADOR"] == selected].iloc[0]
    rank = jugadores.index(selected) + 1

    # Metric cards
    st.markdown('<p class="section-header">Overview</p>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    c1.markdown(f"""<div class="metric-card">
        <div class="lbl">Ranking</div>
        <div class="val">{MEDALS.get(rank, f"#{rank}")}</div>
        <div class="sub">Position {rank} of {len(jugadores)}</div>
    </div>""", unsafe_allow_html=True)
    c2.markdown(f"""<div class="metric-card">
        <div class="lbl">Total Points</div>
        <div class="val">{int(row['PTS'])}</div>
        <div class="sub">Accumulated bet pts</div>
    </div>""", unsafe_allow_html=True)
    c3.markdown(f"""<div class="metric-card">
        <div class="lbl">Average</div>
        <div class="val">{float(row['AVG']):.3f}</div>
        <div class="sub">Points per match</div>
    </div>""", unsafe_allow_html=True)

    st.markdown("")

    with st.spinner("Loading teams..."):
        df = query_jugador_details(selected)

    if df.empty:
        st.info("No team data found.")
        return

    df["STATUS"] = df["POS"].apply(get_status)
    df["POS"]    = pd.to_numeric(df["POS"], errors="coerce")
    comp_order   = {"UCL": 1, "UEL": 2, "UECL": 3}
    df["_s"]     = df["COMPETITION"].map(comp_order).fillna(9)
    df           = df.sort_values(["_s", "POS"]).drop("_s", axis=1)

    # Points by competition — bar chart
    st.markdown('<p class="section-header">Betting points by competition</p>', unsafe_allow_html=True)
    comp_pts = df.groupby("COMPETITION")["PTS"].sum().reset_index()
    bar_colors = [COMPETITION_COLORS.get(c, {}).get("secondary", "#555") for c in comp_pts["COMPETITION"]]

    fig2 = go.Figure(go.Bar(
        x=comp_pts["COMPETITION"],
        y=comp_pts["PTS"],
        marker_color=bar_colors,
        text=comp_pts["PTS"],
        textposition="outside",
        width=0.4,
        hovertemplate="<b>%{x}</b><br>Pts: %{y}<extra></extra>",
    ))
    fig2.update_layout(
        template="plotly_dark",
        height=260,
        margin=dict(l=0, r=0, t=10, b=0),
        xaxis=dict(title=""),
        yaxis=dict(title="Bet pts", gridcolor="#1e2d3d"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
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

        disp = grp[["TEAM", "MP", "W", "D", "L", "PTS", "POS", "STATUS"]].copy()
        disp.columns = ["Team", "MP", "W", "D", "L", "Pts", "Pos", "Status"]

        def style_row(row, _c=c):
            s = row["Status"]
            if s == "CLASIFICADO":
                return [f"background-color:{_c['primary']}; color:white"] * len(row)
            if s == "PLAYOFFS":
                return [f"background-color:{_c['secondary']}; color:white"] * len(row)
            return ["background-color:#111; color:#444; text-decoration:line-through"] * len(row)

        st.dataframe(
            disp.style.apply(style_row, axis=1),
            hide_index=True,
            use_container_width=True,
        )

    # Competition summary
    st.markdown('<p class="section-header">Summary by competition</p>', unsafe_allow_html=True)
    summary = (
        df.groupby("COMPETITION")
        .agg(MP=("MP", "sum"), W=("W", "sum"), D=("D", "sum"), L=("L", "sum"), Pts=("PTS", "sum"))
        .reset_index()
    )
    summary.columns = ["Competition", "MP", "W", "D", "L", "Bet Pts"]
    st.dataframe(summary, hide_index=True, use_container_width=True)


# ---------------------------------------------------------------------------
# Tab 3: Real Table
# ---------------------------------------------------------------------------

def tab_real_table():
    st.markdown('<p class="section-header">Real-world standings — 3 pts / win</p>', unsafe_allow_html=True)

    comp = st.radio("Competition:", ["UCL", "UEL", "UECL"], horizontal=True)
    c    = COMPETITION_COLORS[comp]

    with st.spinner(f"Loading {comp}..."):
        df = query_real_table(comp)

    if df.empty:
        st.warning("No data.")
        return

    df["POS"] = pd.to_numeric(df["POS"], errors="coerce")

    # Bar chart — pts per team
    bar_colors = [
        c["primary"]   if p <=  8 else
        c["secondary"] if p <= 24 else
        "#2a2a2a"
        for p in df["POS"]
    ]
    fig = go.Figure(go.Bar(
        x=df["TEAM"],
        y=df["PTS"],
        marker_color=bar_colors,
        text=df["POS"].apply(lambda p: f"#{int(p)}"),
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>Pts: %{y}<br>Pos: %{customdata}<extra></extra>",
        customdata=df["POS"],
    ))
    fig.update_layout(
        template="plotly_dark",
        height=360,
        margin=dict(l=0, r=0, t=10, b=80),
        xaxis=dict(tickangle=-45, tickfont=dict(size=10)),
        yaxis=dict(title="Points", gridcolor="#1e2d3d"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Legend
    lc1, lc2, lc3 = st.columns(3)
    lc1.markdown(f'<span style="background:{c["primary"]};padding:3px 10px;border-radius:6px;color:white;font-size:0.8rem">■ Top 8 — Direct to R16</span>', unsafe_allow_html=True)
    lc2.markdown(f'<span style="background:{c["secondary"]};padding:3px 10px;border-radius:6px;color:white;font-size:0.8rem">■ 9-24 — Playoff</span>', unsafe_allow_html=True)
    lc3.markdown('<span style="background:#2a2a2a;padding:3px 10px;border-radius:6px;color:#777;font-size:0.8rem">■ 25-36 — Eliminated</span>', unsafe_allow_html=True)
    st.markdown("")

    # Full table with row styling
    disp = df.copy()
    disp.columns = ["Team", "MP", "W", "D", "L", "GF", "GA", "GD", "Pts", "Pos"]

    def style_real_row(row, _c=c):
        pos = row["Pos"]
        try:
            pos = int(pos)
        except Exception:
            pos = 99
        if pos <=  8:
            return [f"background-color:{_c['primary']}; color:white"] * len(row)
        if pos <= 24:
            return [f"background-color:#1a2a3a; color:#aac4e0"] * len(row)
        return ["background-color:#111; color:#444"] * len(row)

    st.dataframe(
        disp.style.apply(style_real_row, axis=1),
        hide_index=True,
        use_container_width=True,
        height=650,
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

    st.markdown(css(), unsafe_allow_html=True)
    st.markdown("## ⚽ UEFA BET — 2025/26")
    st.markdown("---")

    with st.spinner("Loading data..."):
        df_ranking = query_reclasificacion()

    if df_ranking.empty:
        st.warning("No data found. Run the scraper and loader first.")
        return

    t1, t2, t3 = st.tabs(["🏆 Ranking", "👤 Player Detail", "📊 Real Table"])

    with t1:
        tab_ranking(df_ranking)
    with t2:
        tab_player(df_ranking)
    with t3:
        tab_real_table()


if __name__ == "__main__":
    main()
