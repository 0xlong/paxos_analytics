import streamlit as st
import duckdb
from pathlib import Path
import sys
import pandas as pd
import traceback
sys.path.append(str(Path(__file__).parent.parent))
from components.kpi_card import kpi_card_row
from components.charts import create_area_chart, create_bar_chart, create_line_chart, create_pie_chart, COLORS
import plotly.express as px

st.set_page_config(page_title="Supply & Adoption | PYUSD", page_icon="📈", layout="wide")

@st.cache_resource
def get_db_connection():
    db_path = Path("data/duckdb/pyusd_analytics.duckdb")
    if not db_path.exists():
        return None
    return duckdb.connect(str(db_path), read_only=True)

conn = get_db_connection()
if conn is None:
    st.error("Database connection failed. Please ensure the pipeline has run and duckdb exists.")
    st.stop()

st.title("Supply & Adoption Dynamics")
st.markdown("""
Analyzing PYUSD's growth trajectory post-OCC trust charter - Is PYUSD's post-OCC growth real and accelerating?
""")
st.markdown("---")

try:
    df_supply = conn.execute("SELECT * FROM fct_daily_supply_metrics ORDER BY transfer_date").df()
    df_wallets = conn.execute("SELECT * FROM dim_wallets").df()

    # Ensure transfer_date is proper datetime (Plotly needs this for vline annotations)
    df_supply['transfer_date'] = pd.to_datetime(df_supply['transfer_date'])

    # Defensive type casting — ensure numeric columns are numeric
    for col in ['running_total_supply', 'daily_minted_amount', 'daily_burned_amount', 
                'daily_net_change', 'rolling_7d_avg_net_change', 'supply_growth_wow_pct']:
        if col in df_supply.columns:
            df_supply[col] = pd.to_numeric(df_supply[col], errors='coerce')

    for col in ['current_balance', 'starting_balance', 'total_sent_amount', 'total_received_amount']:
        if col in df_wallets.columns:
            df_wallets[col] = pd.to_numeric(df_wallets[col], errors='coerce')

    # Derive holder stats
    total_holders = len(df_wallets[df_wallets['current_balance'] > 0])

    # --- Row 1: KPIs ---
    latest = df_supply.iloc[-1] if not df_supply.empty else None

    if latest is not None:
        val1 = float(latest['running_total_supply'])
        wow = latest['supply_growth_wow_pct']
        delta_str = f"{float(wow):.2f}% Week-over-Week" if pd.notna(wow) else "N/A"
        
        val2 = float(latest['rolling_7d_avg_net_change']) / 1e6 if pd.notna(latest['rolling_7d_avg_net_change']) else 0
        
        weekly_minted = float(df_supply.tail(7)['daily_minted_amount'].sum()) / 1e6
        weekly_burned = float(df_supply.tail(7)['daily_burned_amount'].sum()) / 1e6
        avg_7d_minted = weekly_minted / 7
        avg_7d_burned = weekly_burned / 7
        
        # Format supply: use B for billions, M for millions
        if val1 >= 1e9:
            supply_str = f"${val1 / 1e9:,.2f}B"
        else:
            supply_str = f"${val1 / 1e6:,.1f}M"

        kpi_card_row([
            {"title": "Total Supply", "value": supply_str, "subvalue": delta_str},
            {"title": "7D Avg Net Change", "value": f"${val2:,.2f}M", "subvalue": "Rolling 7-day average"},
            {"title": "Active Holders", "value": f"{total_holders:,}", "subvalue": "Wallets with balance > 0"},
            {"title": "7D Avg Mints / Burns", "value": f"${avg_7d_minted:,.1f}M / ${avg_7d_burned:,.1f}M", "subvalue": f"Total 7D: ${weekly_minted:,.0f}M / ${weekly_burned:,.0f}M"}
        ])

    st.markdown("---")

    # --- Row 2: Supply Charts ---
    if not df_supply.empty:
        fig_supply = create_area_chart(df_supply, 'transfer_date', 'running_total_supply', "PYUSD Total Supply Over Time")
        # OCC Charter annotation (avoid add_vline annotation_text bug)
        fig_supply.add_shape(type="line", x0="2025-12-15", x1="2025-12-15", y0=0, y1=1, yref="paper",
                             line=dict(color=COLORS["warning"], width=2, dash="dash"))
        fig_supply.add_annotation(x="2025-12-15", y=1, yref="paper", text="OCC Charter",
                                  showarrow=False, yanchor="bottom", font=dict(color=COLORS["warning"]))
        st.plotly_chart(fig_supply, width="stretch")

    if not df_supply.empty:
        st.markdown("---")
        df_supply['color'] = df_supply['daily_net_change'].apply(lambda x: 'Positive' if x >= 0 else 'Negative')
        fig_mints = create_bar_chart(df_supply, x='transfer_date', y='daily_net_change', title="Daily Net Change (Mints − Burns)", color='color',
                           color_discrete_map={'Positive': COLORS['tertiary'], 'Negative': COLORS['danger']})
        fig_mints.update_layout(showlegend=False)
        st.plotly_chart(fig_mints, width="stretch")

    st.markdown("---")
    
    # Supply growth WoW line
    #st.subheader("Supply Growth (Week-over-Week %)")
    df_wow = df_supply[df_supply['supply_growth_wow_pct'].notna()]
    if not df_wow.empty:
        fig_wow = create_line_chart(df_wow, 'transfer_date', 'supply_growth_wow_pct', "Week-over-Week Supply Growth %", color_discrete_sequence=[COLORS["secondary"]])
        fig_wow.add_hline(y=0, line_dash="dot", line_color="gray")
        st.plotly_chart(fig_wow, width="stretch")

    st.markdown("---")

    # --- Row 3: Holder Distribution ---

    # Top labeled wallets table
    st.markdown('<div style="font-family: Roboto, sans-serif; color: #334155; font-size: 15px; font-weight: bold; margin-bottom: 12px;">Top 20 Labeled Wallets</div>', unsafe_allow_html=True)
    top_wallets = df_wallets.sort_values('current_balance', ascending=False).head(20)
    display_df = top_wallets[['wallet_address', 'wallet_label', 'wallet_tier', 'current_balance', 'total_tx_count', 'account_age_days']].copy()
    display_df['current_balance'] = display_df['current_balance'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else x)

    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True
    )

except Exception as e:
    st.error(f"Error querying data: {e}")
    st.code(traceback.format_exc())
