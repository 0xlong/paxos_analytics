# 💸 PYUSD On-Chain Analytics

End-to-end analytics platform for **PYUSD (PayPal USD)** — a regulated stablecoin issued by Paxos on Ethereum.

Extracts raw blockchain data, transforms it through a modeled warehouse layer, and surfaces insights in an interactive dashboard with AI-generated intelligence reports.

![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.7+-orange?logo=dbt&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-ready-29B5E8?logo=snowflake&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-0.10+-yellow?logo=duckdb&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-1.30+-FF4B4B?logo=streamlit&logoColor=white)

---


https://github.com/user-attachments/assets/1e6ac87e-d6f7-40c2-a064-17a2af2a0141


---

## What This Project Does

PYUSD is a dollar-backed stablecoin that sits at the center of PayPal's crypto strategy. Understanding its on-chain behavior — who holds it, how supply changes over time, and what large-value transfers look like — is critical for product teams, compliance functions, and business stakeholders working in the stablecoin space.

This project builds a **complete analytics workflow** from raw Ethereum event logs to stakeholder-ready dashboards and AI-written reports:

| Layer | What happens | Tools |
|-------|-------------|-------|
| **Extract** | Pull ERC-20 Transfer event logs from Etherscan API | Python, Requests |
| **Transform** | Decode hex logs into typed, human-readable Parquet files | Python, Pandas, PyArrow |
| **Load** | Ingest decoded data into an analytical warehouse | DuckDB (local) / Snowflake (cloud) |
| **Model** | Build a trusted, tested, documented analytics layer | dbt (staging → intermediate → marts) |
| **Dashboard** | Interactive visualizations across four focused views | Streamlit, Plotly |
| **AI Reports** | One-click intelligence reports from live data | Snowflake Cortex, Google Gemini |

---

## Dashboard Pages

The Streamlit app is organized into four purpose-built pages, each answering a specific business question:

### 📈 Supply & Adoption

> *"Is PYUSD growth real and accelerating?"*

- Total circulating supply over time with OCC charter annotation
- Daily net mint/burn activity (positive vs. negative visualization)
- Week-over-week supply growth trend line
- Rolling 7-day average net change
- Top 20 labeled wallets table

### 🏦 Ecosystem & Wallets

> *"How concentrated is PYUSD, and is distribution improving or worsening?"*

- Top-10 and top-50 wallet share of total supply
- Gini coefficient and HHI index for concentration measurement
- Wallet tier distribution by count and balance (donut charts + tables)
- Full ranked concentration table with labels (top 100 wallets)

### 🛡️ Health & Compliance

> *"What do large transactions look like, and who is using PYUSD — institutions or retail?"*

- Large transaction monitoring: >$100K, >$1M, >$10M thresholds
- Daily large transaction counts with size-tier breakdown
- Top 20 largest transfers table with sender/receiver labels
- Transfer velocity (volume / circulating supply)
- Daily unique sender and receiver counts

### 🤖 AI Intelligence Report

> *"Generate a weekly business intelligence report from live on-chain data."*

- Collects all key metrics in a single pass over the warehouse
- Feeds a structured data snapshot to an LLM with a professional analyst prompt
- Two AI engines available:
  - **Snowflake Cortex** — AI runs natively inside the data warehouse (data never leaves Snowflake)
  - **Google Gemini** — external API alternative for local development
- Report structure: Executive Summary → Supply Dynamics → Transfer Activity → Concentration Risk → Compliance Monitoring → Outlook

---

## Data Modeling (dbt)

The project uses a clean **staging → intermediate → marts** modeling pattern, with each layer serving a distinct purpose:

```
sources.yml
│
├── staging/
│   └── stg_pyusd_transfers          (clean, rename, type-cast, surrogate key)
│
├── intermediate/
│   ├── int_supply_changes           (daily mint/burn with running total supply)
│   ├── int_daily_transfer_summary   (daily aggregates by transfer type)
│   └── int_wallet_activity          (lifetime wallet stats with starting balances)
│
└── marts/
    ├── fct_daily_supply_metrics     (7d rolling averages, WoW growth %)
    ├── fct_daily_transfer_metrics   (volume, velocity, participant counts)
    ├── fct_large_transactions       (>$100K transfers with labels for compliance)
    ├── fct_wallet_concentration     (ranked wallet balances, share %, tier)
    └── dim_wallets                  (master wallet dimension with labels)
```

### Key modeling decisions

- **Starting balances**: A seed file captures the top 10K holder balances at the start of the analysis window (Oct 1 2025), so `current_balance = starting_balance + received - sent` is accurate even though the raw data doesn't go back to PYUSD's genesis, chosen due to API data extraction limitations
- **Wallet labels**: A curated CSV seed maps known addresses to human-readable names (PayPal, Paxos Treasury, exchanges, etc.), labels have been extracted using Etehrscan and Arkham
- **Dual-target profiles**: The same dbt models run on both DuckDB (local, zero-infrastructure) and Snowflake (production cloud warehouse) with no code changes — just `dbt run --target dev` vs `dbt run --target snowflake` making it easy to switch between local and cloud development (universal adapter)
- **Materialization strategy**: Staging and intermediate models are views (fast, no storage cost); marts are tables (materialized for dashboard query speed)

### Data quality tests

The project includes **schema-level tests** (unique, not_null, accepted_values on every column that matters) and **6 custom SQL tests** covering business logic:

| Test | What it checks |
|------|---------------|
| `assert_no_negative_amounts` | No transfer has a negative PYUSD value |
| `assert_mint_from_zero_address` | Every mint originates from 0x000...0000 |
| `assert_burn_to_zero_address` | Every burn is sent to 0x000...0000 |
| `assert_not_both_mint_and_burn` | No single transfer is flagged as both mint and burn |
| `assert_valid_address_format` | All addresses are 42-char, 0x-prefixed hex |
| `assert_valid_tx_hash_format` | All tx hashes are 66-char, 0x-prefixed hex |

Run all tests:

```bash
cd dbt_project && dbt test
```

---

## ETL Pipeline

The extraction pipeline handles the reality of working with blockchain APIs at scale. The Etherscan API has page-size limits, and dense block ranges (like popular DeFi activity) can overflow a single request. The pipeline uses **adaptive chunk splitting** to handle this automatically:

1. **Extract** (`ETL/extract/`) — Walk through block ranges in 5,000-block chunks (due to Etherscan API rate limitations). If a chunk returns too many results, split it in half and recurse. This guarantees gap-free data even during high-activity periods.

2. **Transform** (`ETL/transform/`) — Decode raw hex fields (topics, data, timestamps) into typed columns: addresses, amounts (6-decimal PYUSD precision), block timestamps, mint/burn flags.

3. **Load** (`ETL/load/`) — Read the decoded Parquet file directly into DuckDB using `CREATE TABLE AS SELECT * FROM read_parquet(...)`. Idempotent — safe to re-run at any time.

```
Etherscan API → raw hex Parquet → decoded Parquet → DuckDB → dbt models → dashboards
```

---

## Tech Stack

| Category | Technology | Why |
|----------|-----------|-----|
| **Warehouse (local)** | DuckDB | Zero-config analytical database, runs anywhere, reads Parquet natively |
| **Warehouse (cloud)** | Snowflake | Production-grade warehouse with native AI capabilities (Cortex) |
| **Transformation** | dbt-core + dbt-duckdb + dbt-snowflake | Version-controlled SQL models, testing, documentation, lineage |
| **Dashboard** | Streamlit + Plotly | Python-native interactive dashboards with a clean, minimal design system |
| **AI / LLM** | Snowflake Cortex (Llama 3.1 70B), Google Gemini 2.5 Flash | Automated report generation from structured data snapshots |
| **Data format** | Parquet (via PyArrow) | Columnar, compressed, typed — the right format for analytical workloads |
| **Blockchain data** | Etherscan API v2 | Reliable source for Ethereum event logs |

---

## Project Structure

```
analytics_paxos/
│
├── ETL/
│   ├── extract/
│   │   ├── config.py                       # API keys, contract address, date range
│   │   └── extract_logs_from_etherscan.py  # Adaptive chunk extraction
│   ├── transform/
│   │   └── decode_raw_logs.py              # Hex → typed columns
│   └── load/
│       └── load_to_duckdb.py               # Parquet → DuckDB
│
├── dbt_project/
│   ├── dbt_project.yml                     # Project config, materialization strategy
│   ├── profiles.yml                        # DuckDB (dev) + Snowflake (prod) targets
│   ├── models/
│   │   ├── sources.yml                     # Raw table declaration
│   │   ├── staging/                        # Clean + rename + type-cast
│   │   ├── intermediate/                   # Business logic aggregations
│   │   └── marts/                          # Dashboard-ready fact + dimension tables
│   ├── seeds/
│   │   ├── pyusd_users_labels.csv          # Curated wallet → name mapping
│   │   └── starting_balances.csv           # Top 10K holder snapshot (Oct 1 2025)
│   └── tests/staging/                      # 6 custom data quality assertions
│
├── streamlit_app/
│   ├── app.py                              # Entry point, navigation, global styles
│   ├── components/
│   │   ├── kpi_card.py                     # Reusable KPI card grid component
│   │   └── charts.py                       # Chart factory with consistent styling
│   └── pages/
│       ├── 1_supply_adoption.py            # Supply & growth analysis
│       ├── 2_who_holds_pyusd.py            # Wallet concentration
│       ├── 3_health_compliance.py          # Large tx monitoring
│       └── 4_ai_report.py                  # AI-generated intelligence reports
│
├── data/
│   ├── raw/                                # Raw Parquet from extraction
│   ├── transformed/                        # Decoded Parquet
│   └── duckdb/                             # DuckDB warehouse file
│
├── .streamlit/config.toml                  # Streamlit theme configuration
├── .gitignore
├── requirements.txt
└── .env                                    # API keys (not committed)
```

---

## Getting Started

### Prerequisites

- Python 3.11+
- An Etherscan API key
- Snowflake account for Cortex AI features
- Google Gemini API key for local AI reports

### Setup

```bash
git clone https://github.com/your-username/pyusd-analytics.git && cd pyusd-analytics && python -m venv venv && venv\Scripts\activate && pip install -r requirements.txt
```

Configure API keys:

```bash
cp .env.example .env
# Edit .env with your ETHERSCAN_API_KEY (and optionally GEMINI_API_KEY, Snowflake credentials)
```

### Run the full pipeline

```bash
python ETL/extract/extract_logs_from_etherscan.py && python ETL/transform/decode_raw_logs.py && python ETL/load/load_to_duckdb.py && cd dbt_project && dbt seed && dbt run && dbt test && cd .. && streamlit run streamlit_app/app.py
```

### Run against Snowflake (optional)

```bash
cd dbt_project && dbt seed --target snowflake && dbt run --target snowflake && dbt test --target snowflake
```

---

## Design Choices Worth Noting

**Why DuckDB + Snowflake dual-target?** DuckDB lets anyone clone the repo and run the full pipeline locally in under a minute with no infrastructure. Snowflake is the production environemnt (not free for personal projects) and enables native AI (Cortex) — the same dbt models work against both with zero code changes.

**Why Streamlit for dashboards?** Full control over layout, styling, and interactivity while keeping everything in Python and version-controlled. The reusable component system (`kpi_card.py`, `charts.py`) keeps the codebase DRY and the visual language consistent across all pages.

**Why Parquet as the intermediate format?** Columnar, compressed, fully typed. One decoded Parquet file holds ~6 months of PYUSD transfer data in under 50 MB and loads into DuckDB in seconds. It's also directly readable by dbt, Pandas, and Snowflake.

**Why adaptive chunk splitting in extraction?** The Etherscan API limits results per page. High-activity block ranges silently drop data if you don't handle overflow. The recursive splitting approach guarantees zero data gaps — a subtle but important data quality concern when working with blockchain APIs.

---

## What I'd Add Next
- **Snowflake Cortex for natural-language queries** — let stakeholders ask questions in plain English against the warehouse directly


---

## License

MIT
