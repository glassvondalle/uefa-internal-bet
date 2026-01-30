"""
Streamlit App for European Club Cups Betting Data
Displays RECLASIFICACION view with filter by JUGADOR
"""

import streamlit as st
import snowflake.connector
import json
import pandas as pd
from pathlib import Path
from typing import Optional

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent.absolute()


@st.cache_resource
def load_config() -> dict:
    """Load Snowflake configuration from JSON file in params folder."""
    config_path = SCRIPT_DIR.parent / "params" / "snowflake_config.json"
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        st.error(f"❌ Configuration file not found: {config_path}")
        st.stop()
    except json.JSONDecodeError as e:
        st.error(f"❌ Error parsing configuration file: {e}")
        st.stop()


@st.cache_resource
def connect_to_snowflake(config: dict) -> snowflake.connector.SnowflakeConnection:
    """Connect to Snowflake using configuration parameters."""
    try:
        connection_params = {
            "account": config["account"],
            "user": config["user"],
            "password": config["password"]
        }
        
        # Add optional parameters if they exist
        if config.get("warehouse"):
            connection_params["warehouse"] = config["warehouse"]
        if config.get("database"):
            connection_params["database"] = config["database"]
        if config.get("schema"):
            connection_params["schema"] = config["schema"]
        if config.get("role"):
            connection_params["role"] = config["role"]
        
        conn = snowflake.connector.connect(**connection_params)
        return conn
        
    except Exception as e:
        st.error(f"❌ Error connecting to Snowflake: {e}")
        st.stop()


def query_reclasificacion(conn: snowflake.connector.SnowflakeConnection) -> pd.DataFrame:
    """Query the RECLASIFICACION view to get all players' stats."""
    query = """
    SELECT 
        JUGADOR,
        PTS,
        ROUND(AVG, 3) AS AVG
    FROM UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.RECLASIFICACION
    ORDER BY PTS DESC, AVG DESC
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        cursor.close()
        
        if data:
            df = pd.DataFrame(data, columns=columns)
            return df
        else:
            return pd.DataFrame(columns=columns)
    except Exception as e:
        st.error(f"❌ Error querying RECLASIFICACION: {e}")
        return pd.DataFrame()


def query_jugador_details(conn: snowflake.connector.SnowflakeConnection, jugador: str) -> pd.DataFrame:
    """Query detailed data for a specific JUGADOR from apuesta_table."""
    query = """
    SELECT 
        a.COMPETITION,
        a.TEAM,
        a.MP,
        a.W,
        a.D,
        a.L,
        a.PTS,
        a.POS
    FROM UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.APUESTA_TABLE a
    INNER JOIN UCL_APUESTA_DB.UCL_APUESTA_SCHEMA.PARTICIPANTES p
        ON a.TEAM = p.TEAM
    WHERE p.JUGADOR = %s
    ORDER BY a.COMPETITION, a.POS
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(query, (jugador,))
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        cursor.close()
        
        if data:
            df = pd.DataFrame(data, columns=columns)
            
            # Add STATUS column based on POS
            def get_status(pos):
                if pos < 9:
                    return "CLASIFICADO"
                elif pos >= 9 and pos <= 24:
                    return "PLAYOFFS"
                else:
                    return "ELIMINADO"
            
            df['STATUS'] = df['POS'].apply(get_status)
            
            return df
        else:
            return pd.DataFrame(columns=columns)
    except Exception as e:
        st.error(f"❌ Error querying jugador details: {e}")
        st.info("Note: Make sure the PARTICIPANTES table exists in your Snowflake database.")
        return pd.DataFrame()


def main():
    """Main Streamlit app."""
    st.set_page_config(
        page_title="UEFA BET - Reclasificación",
        page_icon="⚽",
        layout="wide"
    )
    
    st.title("⚽ UEFA BET - Reclasificación")
    st.markdown("---")
    
    # Load configuration and connect to Snowflake
    config = load_config()
    conn = connect_to_snowflake(config)
    
    # Query RECLASIFICACION view
    with st.spinner("Loading data from Snowflake..."):
        df_reclasificacion = query_reclasificacion(conn)
    
    if df_reclasificacion.empty:
        st.warning("No data found in RECLASIFICACION view.")
        return
    
    # Get list of jugadores for the filter
    jugadores = sorted(df_reclasificacion['JUGADOR'].dropna().unique().tolist())
    
    # Sidebar with filter
    st.sidebar.header("🔍 Filters")
    selected_jugador = st.sidebar.selectbox(
        "Select JUGADOR:",
        options=["All"] + jugadores,
        index=0
    )
    
    # Main content area
    if selected_jugador == "All":
        # Show all players table
        st.header("📊 Reclasificación - All Players")
        
        # Format the dataframe for display
        display_df = df_reclasificacion.copy()
        display_df.columns = ['Jugador', 'Points', 'AVG']
        
        # Display table with styling
        st.dataframe(
            display_df,
            width='stretch',
            hide_index=True
        )
        
        # Summary statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Players", len(display_df))
        with col2:
            st.metric("Total Points", int(display_df['Points'].sum()))
        with col3:
            st.metric("Average Points per Player", f"{display_df['Points'].mean():.2f}")
    
    else:
        # Show selected player details
        st.header(f"👤 Player Details: {selected_jugador}")
        
        # Get player summary from reclasificacion
        player_summary = df_reclasificacion[df_reclasificacion['JUGADOR'] == selected_jugador].iloc[0]
        
        # Display summary metrics
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Points", int(player_summary['PTS']))
        with col2:
            st.metric("Average Points", f"{player_summary['AVG']:.3f}")
        
        st.markdown("---")
        
        # Get detailed team data for this player
        with st.spinner(f"Loading details for {selected_jugador}..."):
            df_details = query_jugador_details(conn, selected_jugador)
        
        if not df_details.empty:
            st.subheader("📋 Teams by Competition")
            
            # Format the dataframe for display
            display_details = df_details.copy()
            display_details.columns = ['Competition', 'Team', 'Matches Played', 'Wins', 'Draws', 'Losses', 'Points', 'Position', 'Status']
            
            # Order by competition: UCL, UEL, UECL
            competition_order = {'UCL': 1, 'UEL': 2, 'UECL': 3}
            display_details['_sort_order'] = display_details['Competition'].map(competition_order).fillna(99)
            display_details = display_details.sort_values(['_sort_order', 'Position']).drop('_sort_order', axis=1)
            
            # Reorder columns to put Status after Position
            column_order = ['Competition', 'Team', 'Matches Played', 'Wins', 'Draws', 'Losses', 'Points', 'Position', 'Status']
            display_details = display_details[column_order]
            
            # Define color schemes by competition
            def get_row_style(competition, pos):
                """
                Get styling based on competition and position.
                Returns tuple: (bg_color, text_color, is_strikethrough)
                """
                # Convert pos to integer if it's not already
                try:
                    pos = int(float(pos)) if pd.notna(pos) else 0
                except (ValueError, TypeError):
                    pos = 0
                
                # ELIMINADO is the same for all competitions
                if pos > 24:
                    return '#FFFFFF', 'black', True  # ELIMINADO
                
                # UCL color scheme
                if competition == 'UCL':
                    if pos < 9:
                        return '#0E1E5B', 'white', False  # CLASIFICADO
                    elif pos >= 9 and pos < 24:
                        return '#3562A6', 'white', False  # PLAYOFFS
                
                # UEL color scheme
                elif competition == 'UEL':
                    if pos < 9:
                        return '#D85C00', 'black', False  # CLASIFICADO
                    elif pos >= 9 and pos <= 24:
                        return '#FAC40B', 'black', False  # PLAYOFFS
                
                # UECL color scheme (CLASIFICADO and PLAYOFFS to be defined)
                elif competition == 'UECL':
                    if pos < 9:
                        return '#007c00', 'white', False  # Placeholder - CLASIFICADO
                    elif pos >= 9 and pos < 24:
                        return '#26d26d', 'white', False  # Placeholder - PLAYOFFS
                
                # Default (no styling)
                return 'transparent', 'black', False
            
            # Convert Position to numeric for comparison
            display_details['Position'] = pd.to_numeric(display_details['Position'], errors='coerce')
            
            # Apply conditional row styling based on Competition and Position
            def style_row(row):
                competition = row['Competition']
                pos = row['Position']
                bg_color, text_color, _ = get_row_style(competition, pos)
                
                # Build CSS style string
                style_parts = [f'background-color: {bg_color}', f'color: {text_color}']
                style_str = '; '.join(style_parts)
                
                # Return style for entire row
                return [style_str] * len(row)
            
            # Apply styling using pandas Styler
            styled_df = display_details.style.apply(style_row, axis=1)
            
            # Display styled table - Streamlit supports pandas Styler objects
            st.dataframe(
                styled_df,
                hide_index=True,
                use_container_width=True
            )
            
            # Group by competition for summary
            st.subheader("📊 Summary by Competition")
            competition_summary = df_details.groupby('COMPETITION').agg({
                'MP': 'sum',
                'W': 'sum',
                'D': 'sum',
                'L': 'sum',
                'PTS': 'sum'
            }).reset_index()
            competition_summary.columns = ['Competition', 'Total MP', 'Total W', 'Total D', 'Total L', 'Total Points']
            
            st.dataframe(
                competition_summary,
                width='stretch',
                hide_index=True
            )
        else:
            st.info(f"No team details found for {selected_jugador}.")


if __name__ == "__main__":
    main()
