import streamlit as st
import duckdb
from pathlib import Path
import sys
import pandas as pd
sys.path.append(str(Path(__file__).parent.parent))
from components.kpi_card import kpi_card_row
from components.charts import create_line_chart, create_bar_chart, create_area_chart, COLORS
import plotly.express as px

st.set_page_config(page_title="Health & Compliance", page_icon="🛡️", layout="wide")

@st.cache_resource
def get_db_connection():
    db_path = Path("data/duckdb/pyusd_analytics.duckdb")
    if not db_path.exists():
        return None
    return duckdb.connect(str(db_path), read_only=True)

conn = get_db_connection()
if conn is None:
    st.error("Database connection failed.")
    st.stop()

st.title("Health & Regulatory Compliance")
st.markdown("""
Surveillance of large transactions and aggregate behavior monitoring.

- *What does PYUSD's large transaction activity look like for regulatory monitoring?*
- *Who is using PYUSD — institutions or retail? When is it most active?*
""")

try:
    # =========================================================================
    # fct_large_transactions columns:
    #   transfer_id, block_number, block_timestamp, transfer_date, tx_hash,
    #   log_index, from_address, to_address, amount_pyusd, transfer_type,
    #   from_label, to_label, size_tier
    # =========================================================================
    df_large = conn.execute("SELECT * FROM fct_large_transactions ORDER BY block_timestamp DESC").df()

    # =========================================================================
    # fct_daily_transfer_metrics columns:
    #   transfer_date, total_tx_count, total_volume_pyusd,
    #   regular_tx_count, mint_tx_count, burn_tx_count,
    #   regular_volume, mint_volume, burn_volume,
    #   unique_senders, unique_receivers,
    #   avg_transfer_size, running_total_supply, velocity
    # =========================================================================
    df_metrics = conn.execute("SELECT * FROM fct_daily_transfer_metrics ORDER BY transfer_date").df()

    st.markdown("---")

    # --- KPIs ---
    count_100k = len(df_large[df_large['amount_pyusd'] >= 100000])
    count_1m = len(df_large[df_large['amount_pyusd'] >= 1000000])
    count_10m = len(df_large[df_large['amount_pyusd'] >= 10000000])
    
    kpi_card_row([
        {"title": "> $100K Transfers", "value": f"{count_100k:,}", "subvalue": "In analyzed period"},
        {"title": "> $1M Transfers", "value": f"{count_1m:,}", "subvalue": "In analyzed period"},
        {"title": "> $10M Transfers", "value": f"{count_10m:,}", "subvalue": "Mega transactions"}
    ])

    st.markdown("---")
    # --- Daily large tx count bar chart ---
    if not df_large.empty:
        df_daily_large = df_large.groupby('transfer_date').agg(
            tx_count=('transfer_id', 'count'),
            total_volume=('amount_pyusd', 'sum')
        ).reset_index()

        fig_lg_count = create_bar_chart(df_daily_large, x='transfer_date', y='tx_count', title="Daily Large Transaction Count (>$100K)",
                              color_discrete_sequence=[COLORS['warning']])
        st.plotly_chart(fig_lg_count, width="stretch")

        st.markdown("---")

        # By size tier
        df_tier_daily = df_large.groupby(['transfer_date', 'size_tier']).size().reset_index(name='count')
        fig_tier = create_bar_chart(df_tier_daily, x='transfer_date', y='count', color='size_tier',
                          title="Large Tx by Size Tier", barmode='stack',
                          color_discrete_sequence=[COLORS['tertiary'], COLORS['warning'], COLORS['danger']])
        st.plotly_chart(fig_tier, width="stretch")

    st.markdown("---")

    # --- Top 20 largest transfers table ---
    st.markdown('<div style="font-family: Roboto, sans-serif; color: #334155; font-size: 15px; font-weight: bold; margin-bottom: 15px;">Top 20 Largest Transfers</div>', unsafe_allow_html=True)

    top_transfers = df_large.sort_values(by='amount_pyusd', ascending=False).head(20)
    st.dataframe(
        top_transfers[['transfer_date', 'tx_hash', 'from_label', 'to_label', 'amount_pyusd', 'size_tier']],
        width="stretch"
    )

    st.markdown("---")

    # --- KPIs ---
    if not df_metrics.empty:
        latest_m = df_metrics.iloc[-1]
        
        kpi_card_row([
            {"title": "Avg Transfer Size", "value": f"${latest_m['avg_transfer_size']:,.0f}", "subvalue": "Latest day"},
            {"title": "Velocity", "value": f"{latest_m['velocity']:.4f}" if pd.notna(latest_m['velocity']) else "N/A", "subvalue": "Volume / Supply"},
            {"title": "Unique Senders", "value": f"{int(latest_m['unique_senders']):,}", "subvalue": "Latest day"},
            {"title": "Unique Receivers", "value": f"{int(latest_m['unique_receivers']):,}", "subvalue": "Latest day"}
        ])
    
    st.markdown("---")
    # --- Volume & Count Charts ---
    if not df_metrics.empty:
        fig_vol = create_bar_chart(df_metrics, 'transfer_date', 'regular_volume', "Daily Transfer Volume (PYUSD)")
        st.plotly_chart(fig_vol, width="stretch")

    if not df_metrics.empty:
        st.markdown("---")
        fig_count = create_bar_chart(df_metrics, 'transfer_date', 'total_tx_count', "Daily Transfer Count",
                                      color_discrete_sequence=[COLORS["tertiary"]])
        st.plotly_chart(fig_count, width="stretch")

except Exception as e:
    st.error(f"Error executing sql queries: {e}")
