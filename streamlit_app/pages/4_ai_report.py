import streamlit as st
import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
import os
import sys
import time

sys.path.append(str(Path(__file__).parent.parent))
from components.kpi_card import kpi_card_row

# Load environment variables
load_dotenv(Path(__file__).parent.parent.parent / ".env")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------
@st.cache_resource
def get_db_connection():
    db_path = Path("data/duckdb/pyusd_analytics.duckdb")
    if not db_path.exists():
        return None
    return duckdb.connect(str(db_path), read_only=True)


# ---------------------------------------------------------------------------
# Data collection — one efficient pass over DuckDB
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def collect_report_data() -> dict:
    """Run all key queries and return a compact dict with report data."""
    conn = get_db_connection()
    if conn is None:
        return None

    data = {}

    # 1. Supply metrics (latest + 7-day history for trends)
    df_supply = conn.execute("""
        SELECT transfer_date, running_total_supply, daily_minted_amount,
               daily_burned_amount, daily_net_change,
               rolling_7d_avg_net_change, supply_growth_wow_pct
        FROM fct_daily_supply_metrics
        ORDER BY transfer_date DESC LIMIT 7
    """).df()
    data["supply_latest"] = df_supply.iloc[0].to_dict() if not df_supply.empty else {}
    data["supply_7d_mint_total"] = float(df_supply["daily_minted_amount"].sum())
    data["supply_7d_burn_total"] = float(df_supply["daily_burned_amount"].sum())
    data["supply_7d_net_avg"] = float(df_supply["daily_net_change"].mean())

    # 2. Active holders
    data["active_holders"] = conn.execute(
        "SELECT COUNT(*) FROM dim_wallets WHERE current_balance > 0"
    ).fetchone()[0]

    # 3. Wallet concentration (top 50)
    df_conc = conn.execute("""
        SELECT balance_rank, wallet_address, balance, share_pct, total_supply
        FROM fct_wallet_concentration ORDER BY balance_rank LIMIT 50
    """).df()
    data["top10_share"] = float(df_conc.head(10)["share_pct"].sum())
    data["top50_share"] = float(df_conc.head(50)["share_pct"].sum())
    data["total_supply_conc"] = float(df_conc["total_supply"].iloc[0]) if not df_conc.empty else 0

    # Gini coefficient
    balances = df_conc["balance"].values
    n = len(balances)
    if n > 0:
        sorted_bal = np.sort(balances)
        index = np.arange(1, n + 1)
        gini = (2 * np.sum(index * sorted_bal) / (n * np.sum(sorted_bal))) - (n + 1) / n
    else:
        gini = 0
    data["gini"] = float(gini)

    # HHI
    all_conc = conn.execute("SELECT share_pct FROM fct_wallet_concentration").df()
    data["hhi"] = float(np.sum(all_conc["share_pct"].values ** 2))

    # 4. Transfer metrics (latest 7 days)
    df_metrics = conn.execute("""
        SELECT transfer_date, total_tx_count, total_volume_pyusd,
               regular_tx_count, mint_tx_count, burn_tx_count,
               regular_volume, mint_volume, burn_volume,
               unique_senders, unique_receivers,
               avg_transfer_size, velocity
        FROM fct_daily_transfer_metrics
        ORDER BY transfer_date DESC LIMIT 7
    """).df()
    data["metrics_latest"] = df_metrics.iloc[0].to_dict() if not df_metrics.empty else {}
    data["avg_daily_volume_7d"] = float(df_metrics["total_volume_pyusd"].mean()) if not df_metrics.empty else 0
    data["avg_daily_tx_count_7d"] = float(df_metrics["total_tx_count"].mean()) if not df_metrics.empty else 0
    data["avg_velocity_7d"] = float(df_metrics["velocity"].mean()) if not df_metrics.empty else 0
    data["avg_unique_senders_7d"] = float(df_metrics["unique_senders"].mean()) if not df_metrics.empty else 0
    data["avg_unique_receivers_7d"] = float(df_metrics["unique_receivers"].mean()) if not df_metrics.empty else 0

    # 5. Large transaction summary
    df_large = conn.execute("""
        SELECT
            COUNT(*) FILTER (WHERE amount_pyusd >= 100000)   AS gt_100k,
            COUNT(*) FILTER (WHERE amount_pyusd >= 1000000)  AS gt_1m,
            COUNT(*) FILTER (WHERE amount_pyusd >= 10000000) AS gt_10m
        FROM fct_large_transactions
    """).df()
    data["large_gt_100k"] = int(df_large["gt_100k"].iloc[0])
    data["large_gt_1m"] = int(df_large["gt_1m"].iloc[0])
    data["large_gt_10m"] = int(df_large["gt_10m"].iloc[0])

    # 6. Top 5 wallets with labels
    df_top5 = conn.execute("""
        SELECT c.balance_rank, c.wallet_address, c.balance, c.share_pct,
               w.wallet_label
        FROM fct_wallet_concentration c
        LEFT JOIN dim_wallets w ON c.wallet_address = w.wallet_address
        ORDER BY c.balance_rank LIMIT 5
    """).df()
    data["top5_wallets"] = df_top5.to_dict("records")

    return data


def build_data_snapshot(data: dict) -> str:
    """Format the collected data into a compact text block for the LLM."""
    s = data.get("supply_latest", {})
    m = data.get("metrics_latest", {})

    top5_lines = ""
    for w in data.get("top5_wallets", []):
        label = w.get("wallet_label", "Unknown")
        addr = w.get("wallet_address", "")[:12] + "..."
        name = label if label != "Unknown" else addr
        top5_lines += f"  #{w['balance_rank']}: {name} — {w['balance']:,.0f} PYUSD ({w['share_pct']:.1f}%)\n"

    snapshot = f"""
PYUSD ON-CHAIN DATA SNAPSHOT (as of {s.get('transfer_date', 'N/A')}):

SUPPLY:
- Total Supply: {s.get('running_total_supply', 0):,.0f} PYUSD
- Week-over-Week Growth: {s.get('supply_growth_wow_pct', 0):.2f}%
- 7D Avg Daily Net Change: {data['supply_7d_net_avg']:,.0f} PYUSD
- 7D Total Minted: {data['supply_7d_mint_total']:,.0f} PYUSD
- 7D Total Burned: {data['supply_7d_burn_total']:,.0f} PYUSD

TRANSFER ACTIVITY (7-day averages):
- Avg Daily Volume: {data['avg_daily_volume_7d']:,.0f} PYUSD
- Avg Daily Tx Count: {data['avg_daily_tx_count_7d']:,.0f}
- Avg Daily Unique Senders: {data['avg_unique_senders_7d']:,.0f}
- Avg Daily Unique Receivers: {data['avg_unique_receivers_7d']:,.0f}
- Avg Transfer Size (latest): {m.get('avg_transfer_size', 0):,.0f} PYUSD
- Avg Velocity (Volume/Supply): {data['avg_velocity_7d']:.4f}

WALLET CONCENTRATION:
- Active Holders: {data['active_holders']:,}
- Top 10 Wallet Share: {data['top10_share']:.1f}%
- Top 50 Wallet Share: {data['top50_share']:.1f}%
- Gini Coefficient: {data['gini']:.4f} (0=equal, 1=concentrated)
- HHI Index: {data['hhi']:.0f}

TOP 5 HOLDERS:
{top5_lines}
LARGE TRANSACTIONS (full period):
- Transfers > $100K: {data['large_gt_100k']:,}
- Transfers > $1M: {data['large_gt_1m']:,}
- Transfers > $10M: {data['large_gt_10m']:,}
"""
    return snapshot


# ---------------------------------------------------------------------------
# LLM report generation
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """You are a senior blockchain analyst at Paxos writing a weekly business intelligence report on PYUSD (PayPal USD stablecoin).

Write a concise, professional weekly report using the data snapshot provided. Use markdown formatting.

Structure your report with these sections:
1. **Executive Summary** — 2-3 sentence overview of PYUSD health & trajectory
2. **Supply Dynamics** — supply growth, mint/burn activity, trends
3. **Transfer Activity & Utilization** — volume, velocity, user activity
4. **Concentration & Distribution Risk** — whale dominance, Gini, HHI interpretation
5. **Compliance & Large Transaction Monitoring** — large tx patterns, flags
6. **Outlook & Recommendations** — forward-looking assessment

Guidelines:
- Be data-driven: reference specific numbers from the snapshot
- Interpret the Gini coefficient and HHI in business terms
- Flag any concerning patterns (e.g., high concentration, unusual large tx spikes)
- Keep the tone professional but accessible to a business audience
- Use bullet points and bold for readability
- Do NOT fabricate data — only use what is provided"""


@st.cache_data(ttl=3600, show_spinner=False)
def generate_report_with_gemini(snapshot: str) -> str:
    """Call Gemini API and return the generated markdown report."""
    from google import genai

    client = genai.Client(api_key=GEMINI_API_KEY)

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=f"{SYSTEM_PROMPT}\n\n---\n\n{snapshot}",
    )
    return response.text


# ---------------------------------------------------------------------------
# Page layout
# ---------------------------------------------------------------------------
st.title("🤖 AI PAXOS PyUSD Report")
st.caption("Auto-generated business intelligence report powered byAI, based on live PYUSD on-chain data.")
st.markdown("---")

# Collect data
data = collect_report_data()

if data is None:
    st.error("Database connection failed. Please ensure the pipeline has run and DuckDB exists.")
    st.stop()

# --- KPI row (quick glance before the full report) ---
s = data.get("supply_latest", {})
supply_val = s.get("running_total_supply", 0)
if supply_val >= 1e9:
    supply_str = f"${supply_val / 1e9:,.2f}B"
else:
    supply_str = f"${supply_val / 1e6:,.1f}M"

wow = s.get("supply_growth_wow_pct", 0)
wow_str = f"{wow:.2f}% WoW" if wow else "N/A"

kpi_card_row([
    {"title": "Total Supply", "value": supply_str, "subvalue": wow_str},
    {"title": "Avg Daily Volume (7D)", "value": f"${data['avg_daily_volume_7d'] / 1e6:,.1f}M", "subvalue": f"{data['avg_daily_tx_count_7d']:,.0f} txs/day"},
    {"title": "Active Holders", "value": f"{data['active_holders']:,}", "subvalue": f"Top 10 hold {data['top10_share']:.1f}%"},
    {"title": "Token Velocity (7D)", "value": f"{data['avg_velocity_7d']:.4f}", "subvalue": "Volume / Supply ratio"},
])

st.markdown("---")

# --- Generate or display report ---
if not GEMINI_API_KEY:
    st.warning("⚠️ `GEMINI_API_KEY` not found in `.env` file. Showing data snapshot only.")
    st.markdown("### 📋 Data Snapshot (LLM input)")
    snapshot = build_data_snapshot(data)
    st.code(snapshot, language="text")
    st.info("Add `GEMINI_API_KEY=your_key_here` to your `.env` file to enable AI-generated reports.")
else:
    snapshot = build_data_snapshot(data)

    if "ai_report_md" not in st.session_state:
        st.session_state["ai_report_md"] = None

    col_btn, col_info = st.columns([1, 3])
    with col_btn:
        button_label = "🔄 Regenerate Report" if st.session_state["ai_report_md"] else "✨ Generate Report"
        generate_clicked = st.button(button_label, type="primary")
    with col_info:
        st.caption("Click to generate a fresh AI analysis of the current data.")

    if generate_clicked:
        # Clear the cache to force a fresh generation if clicked
        generate_report_with_gemini.clear()
        try:
            with st.spinner("🧠 AI is analyzing your data and writing the report..."):
                st.session_state["ai_report_md"] = generate_report_with_gemini(snapshot)
        except Exception as e:
            st.error(f"Failed to generate report: {e}")
            st.session_state["ai_report_md"] = None

    if st.session_state["ai_report_md"]:
        st.markdown(st.session_state["ai_report_md"])

        st.markdown("---")

        # Expandable: show raw data snapshot sent to LLM
        with st.expander("📋 View raw data snapshot sent to AI"):
            st.code(snapshot, language="text")
    else:
        st.info("👈 Click the button above to generate the AI report.")
