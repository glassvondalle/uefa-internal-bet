"""
Streamlit App — UEFA Betting Game
Displays player rankings and per-player team breakdown.
"""

import json
import streamlit as st
import psycopg2
import pandas as pd
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.absolute()


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
    url = _get_db_url()
    try:
        conn = psycopg2.connect(url)
        conn.autocommit = True
        return conn
    except Exception as e:
        st.error(f"❌ Could not connect to database: {e}")
        st.stop()


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

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


def query_reclasificacion() -> pd.DataFrame:
    return _fetch(
        "SELECT jugador, pts, avg FROM reclasificacion ORDER BY pts DESC, avg DESC NULLS LAST"
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


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------

def get_row_style(competition: str, pos):
    try:
        pos = int(float(pos)) if pd.notna(pos) else 0
    except (ValueError, TypeError):
        pos = 0

    if pos > 24:
        return "#FFFFFF", "black", True   # ELIMINADO

    if competition == "UCL":
        if pos < 9:
            return "#0E1E5B", "white", False   # CLASIFICADO
        return "#3562A6", "white", False       # PLAYOFFS

    if competition == "UEL":
        if pos < 9:
            return "#D85C00", "black", False
        return "#FAC40B", "black", False

    if competition == "UECL":
        if pos < 9:
            return "#007c00", "white", False
        return "#26d26d", "white", False

    return "transparent", "black", False


def add_status(df: pd.DataFrame) -> pd.DataFrame:
    def _status(pos):
        try:
            pos = int(float(pos))
        except (ValueError, TypeError):
            return "UNKNOWN"
        if pos < 9:
            return "CLASIFICADO"
        if pos <= 24:
            return "PLAYOFFS"
        return "ELIMINADO"

    df = df.copy()
    df["STATUS"] = df["POS"].apply(_status)
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    st.set_page_config(page_title="UEFA BET - Reclasificación", page_icon="⚽", layout="wide")
    st.title("⚽ UEFA BET - Reclasificación")
    st.markdown("---")

    with st.spinner("Loading data..."):
        df_ranking = query_reclasificacion()

    if df_ranking.empty:
        st.warning("No data found. Run the scraper and loader first.")
        return

    jugadores = sorted(df_ranking["JUGADOR"].dropna().unique().tolist())

    st.sidebar.header("🔍 Filters")
    selected = st.sidebar.selectbox("Select player:", ["All"] + jugadores)

    # ------------------------------------------------------------------
    # All-players view
    # ------------------------------------------------------------------
    if selected == "All":
        st.header("📊 Reclasificación — All Players")

        display = df_ranking.rename(columns={"JUGADOR": "Player", "PTS": "Points", "AVG": "Avg"})
        st.dataframe(display, hide_index=True, use_container_width=True)

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Players", len(display))
        col2.metric("Total Points", int(display["Points"].sum()))
        col3.metric("Avg Points / Player", f"{display['Points'].mean():.2f}")

    # ------------------------------------------------------------------
    # Single-player view
    # ------------------------------------------------------------------
    else:
        st.header(f"👤 Player Details: {selected}")

        summary = df_ranking[df_ranking["JUGADOR"] == selected].iloc[0]
        col1, col2 = st.columns(2)
        col1.metric("Total Points", int(summary["PTS"]))
        col2.metric("Average Points", f"{summary['AVG']:.3f}" if summary["AVG"] is not None else "—")

        st.markdown("---")

        with st.spinner(f"Loading details for {selected}..."):
            df_details = query_jugador_details(selected)

        if df_details.empty:
            st.info(f"No team details found for {selected}.")
            return

        df_details = add_status(df_details)
        df_details["POS"] = pd.to_numeric(df_details["POS"], errors="coerce")

        competition_order = {"UCL": 1, "UEL": 2, "UECL": 3}
        df_details["_sort"] = df_details["COMPETITION"].map(competition_order).fillna(99)
        df_details = df_details.sort_values(["_sort", "POS"]).drop("_sort", axis=1)

        display = df_details.rename(columns={
            "COMPETITION": "Competition", "TEAM": "Team",
            "MP": "Matches Played", "W": "Wins", "D": "Draws",
            "L": "Losses", "PTS": "Points", "POS": "Position", "STATUS": "Status",
        })[["Competition", "Team", "Matches Played", "Wins", "Draws", "Losses", "Points", "Position", "Status"]]

        def style_row(row):
            bg, color, strikethrough = get_row_style(row["Competition"], row["Position"])
            parts = [f"background-color: {bg}", f"color: {color}"]
            if strikethrough:
                parts.append("text-decoration: line-through")
            style = "; ".join(parts)
            return [style] * len(row)

        st.subheader("📋 Teams by Competition")
        st.dataframe(display.style.apply(style_row, axis=1), hide_index=True, use_container_width=True)

        st.subheader("📊 Summary by Competition")
        summary_df = (
            df_details.groupby("COMPETITION")
            .agg(MP=("MP", "sum"), W=("W", "sum"), D=("D", "sum"), L=("L", "sum"), PTS=("PTS", "sum"))
            .reset_index()
            .rename(columns={"COMPETITION": "Competition", "MP": "Total MP", "W": "W", "D": "D", "L": "L", "PTS": "Points"})
        )
        st.dataframe(summary_df, hide_index=True, use_container_width=True)


if __name__ == "__main__":
    main()
