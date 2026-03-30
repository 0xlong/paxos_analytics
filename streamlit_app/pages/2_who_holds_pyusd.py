import streamlit as st
import duckdb
from pathlib import Path
import sys
import pandas as pd
import numpy as np
sys.path.append(str(Path(__file__).parent.parent))
from components.kpi_card import kpi_card_row
from components.charts import create_line_chart, create_bar_chart, create_pie_chart, COLORS
import plotly.express as px

st.set_page_config(page_title="Who Holds PYUSD", page_icon="🏦", layout="wide")

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

st.title("Wallet Concentration & Ecosystem Engagement")
st.markdown("Investigating PYUSD holder concentration and whale dominance.")

try:
    # =========================================================================
    # fct_wallet_concentration columns:
    #   wallet_address, balance, total_tx_count, total_supply,
    #   balance_rank, share_pct, wallet_tier
    # =========================================================================
    df_conc = conn.execute("SELECT * FROM fct_wallet_concentration ORDER BY balance_rank").df()

    # =========================================================================
    # dim_wallets columns:
    #   wallet_address, wallet_label, wallet_tier, starting_balance,
    #   tx_sent_count, total_sent_amount, tx_received_count, total_received_amount,
    #   total_tx_count, current_balance, first_active_at, last_active_at, account_age_days
    # =========================================================================
    df_wallets = conn.execute("SELECT * FROM dim_wallets WHERE current_balance > 0 ORDER BY current_balance DESC").df()

    st.markdown("---")
    st.subheader("Analysis 3: Wallet Concentration & Whale Tracking")
    st.caption("*\"How concentrated is PYUSD, and is distribution improving or worsening?\"*")

    # --- KPIs ---
    if not df_conc.empty:
        total_supply = df_conc['total_supply'].iloc[0]
        top10_balance = df_conc.head(10)['balance'].sum()
        top50_balance = df_conc.head(50)['balance'].sum()
        top10_pct = (top10_balance / total_supply * 100) if total_supply > 0 else 0
        top50_pct = (top50_balance / total_supply * 100) if total_supply > 0 else 0

        # Gini coefficient calculation
        balances = df_conc['balance'].values
        n = len(balances)
        if n > 0:
            sorted_bal = np.sort(balances)
            index = np.arange(1, n + 1)
            gini = (2 * np.sum(index * sorted_bal) / (n * np.sum(sorted_bal))) - (n + 1) / n
        else:
            gini = 0

        # HHI — sum of squared market shares (in percentage points)
        shares = df_conc['share_pct'].values
        hhi = np.sum(shares ** 2)

        kpi_card_row([
            {"title": "Top 10 Share", "value": f"{top10_pct:.1f}%", "subvalue": "Of total supply"},
            {"title": "Top 50 Share", "value": f"{top50_pct:.1f}%", "subvalue": "Of total supply"},
            {"title": "Gini Coefficient", "value": f"{gini:.4f}", "subvalue": "0 = equal, 1 = concentrated"},
            {"title": "HHI Index", "value": f"{hhi:.0f}", "subvalue": "Market concentration index"}
        ])

    st.markdown("---")

    # --- Concentration bar chart (top 20 wallets) ---
    st.subheader("Top 20 Wallets by Balance")
    top20 = df_conc.head(20).copy()
    # Merge labels
    labels = conn.execute("SELECT wallet_address, wallet_label FROM dim_wallets").df()
    top20 = top20.merge(labels, on='wallet_address', how='left')
    top20['display_name'] = top20.apply(
        lambda r: r['wallet_label'] if r['wallet_label'] != 'Unknown' else r['wallet_address'][:10] + '...', axis=1
    )
    fig_top20 = create_bar_chart(top20, x='display_name', y='share_pct', title="Top 20 Wallet Share (%)",
                       color='wallet_tier', color_discrete_sequence=px.colors.qualitative.Set2)
    fig_top20.update_layout(xaxis_title="Wallet", yaxis_title="Share %", xaxis_tickangle=-45)
    st.plotly_chart(fig_top20, width="stretch")

    # --- Tier distribution ---
    st.subheader("Wallet Tier Distribution")
    tcol1, tcol2 = st.columns(2)

    df_tiers = df_conc.groupby('wallet_tier').agg(
        wallet_count=('wallet_address', 'count'),
        total_balance=('balance', 'sum')
    ).reset_index()

    with tcol1:
        fig_tier_count = create_pie_chart(df_tiers, values='wallet_count', names='wallet_tier',
                                title="Wallets by Tier (Count)",
                                color_discrete_sequence=px.colors.sequential.Teal)
        st.plotly_chart(fig_tier_count, width="stretch")

    with tcol2:
        fig_tier_bal = create_pie_chart(df_tiers, values='total_balance', names='wallet_tier',
                              title="Balance by Tier (PYUSD)",
                              color_discrete_sequence=px.colors.sequential.Sunset)
        st.plotly_chart(fig_tier_bal, width="stretch")

    # --- Full ranked table ---
    st.subheader("Full Concentration Table")
    display_df = df_conc.merge(labels, on='wallet_address', how='left')
    st.dataframe(
        display_df[['balance_rank', 'wallet_address', 'wallet_label', 'balance', 'share_pct', 'wallet_tier', 'total_tx_count']].head(100),
        width="stretch"
    )

except Exception as e:
    st.error(f"Error executing analysis queries: {e}")
