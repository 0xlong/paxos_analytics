# PYUSD Intelligence Report — Project Plan & Architecture

> **Purpose:** This document is the single source of truth for the PYUSD analytics project.
> It is designed to be used as context by AI coding agents in the Antigravity IDE.
> All architectural decisions, specifications, and build steps are final unless explicitly changed.

---

## 1. Project Goal

Build a portfolio analytics project analyzing **PayPal USD (PYUSD)** on-chain data to demonstrate readiness for the **Data Analyst role at Paxos**. The project must showcase: advanced SQL, dbt modeling, Streamlit dashboarding, Git workflows, and regulatory awareness — all requirements from the Paxos JD.

### Token
- **Name:** PayPal USD (PYUSD)
- **Contract:** `0x6c3ea9036406852006290770BEdFcAbA0e23A0e8`
- **Chain:** Ethereum (ERC-20)
- **Decimals:** 6

### Timeframe
- **Start:** October 1, 2025 (start block: look up exact block on Etherscan by date)
- **End:** March 15, 2026 (end block: look up exact block on Etherscan by date)
- **Rationale:** Captures OCC trust charter completion (Dec 2025) and post-charter growth — a business inflection point for PYUSD.

---

## 2. Data Sources

### Primary: Etherscan `getLogs` API (FREE)
- **Endpoint:** `https://api.etherscan.io/api?module=logs&action=getLogs`
- **Parameters:**
  - `address`: `0x6c3ea9036406852006290770BEdFcAbA0e23A0e8`
  - `topic0`: `0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef` (keccak256 of `Transfer(address,address,uint256)`)
  - `fromBlock` / `toBlock`: split into ~50,000 block chunks (~1 week)
  - `page` / `offset`: paginate with `offset=1000` per request
- **Rate limit:** 5 calls/sec — use `time.sleep(0.25)` between calls
- **Max results per call:** 1,000 records
- **API key:** required (free account at etherscan.io)

### Secondary: NONE
- CoinGecko peg/price analysis was **explicitly dropped** from scope.
- All analytics are derived from Transfer event logs only.

### What each Transfer event log contains (raw hex):
| Field | Contains | Decoding |
|---|---|---|
| `topic0` | Event signature (always the same) | Ignore — used for filtering only |
| `topic1` | `from` address | Strip leading `0x000000000000000000000000` → 20-byte address |
| `topic2` | `to` address | Same as above |
| `data` | Transfer amount (uint256) | Convert hex → int, divide by `10^6` (PYUSD has 6 decimals) |
| `blockNumber` | Block number (hex) | Convert hex → int |
| `timeStamp` | Unix timestamp (hex) | Convert hex → int → datetime |
| `transactionHash` | Tx hash | Use as-is |

### Special address meanings:
- `from_address == 0x0000000000000000000000000000000000000000` → **MINT** (new PYUSD created)
- `to_address == 0x0000000000000000000000000000000000000000` → **BURN** (PYUSD destroyed)

---

## 3. Technology Stack

| Layer | Tool | Version | Install |
|---|---|---|---|
| Runtime | Python | 3.10+ | Pre-installed |
| Virtual env | venv | — | `python -m venv paxos_analytics_env` (DONE) |
| Extract | requests | latest | `pip install requests` |
| Data format | Parquet via pyarrow | latest | `pip install pandas pyarrow` |
| Database | DuckDB | latest | `pip install duckdb` |
| Transform | dbt Core + dbt-duckdb | latest | `pip install dbt-duckdb` |
| Dashboard | Streamlit + Plotly | latest | `pip install streamlit plotly` |
| Version control | Git + GitHub | — | Pre-installed |

### One-liner install (after activating venv):
```powershell
.\paxos_analytics_env\Scripts\activate; pip install requests pandas pyarrow duckdb dbt-duckdb streamlit plotly
```

---

## 4. Project Folder Structure

```
analytics_paxos/
│
├── PROJECT_PLAN.md                       ← THIS FILE (context for AI agents)
├── README.md                             ← Executive business memo (Step 7)
├── requirements.txt
├── .gitignore
│
├── extract/                              ── DATA EXTRACTION
│   ├── extract_pyusd_transfers.py        Main extraction script
│   ├── config.py                         API key, contract addr, block ranges
│   └── utils.py                          Hex decoding, pagination helpers
│
├── data/                                 ── DATA STORAGE
│   ├── raw/
│   │   └── pyusd_transfers.parquet       Decoded Transfer events
│   └── pyusd_analytics.duckdb           DuckDB database (created by dbt)
│
├── dbt_project/                          ── TRANSFORMATION (dbt)
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml                      (if using dbt_utils)
│   ├── models/
│   │   ├── sources.yml                   Source definition for raw data
│   │   ├── staging/
│   │   │   ├── _staging__models.yml      Schema + tests for staging
│   │   │   ├── stg_pyusd_transfers.sql
│   │   │   ├── stg_pyusd_mints.sql
│   │   │   └── stg_pyusd_burns.sql
│   │   ├── intermediate/
│   │   │   ├── _intermediate__models.yml
│   │   │   ├── int_daily_supply.sql
│   │   │   ├── int_wallet_daily_balances.sql
│   │   │   ├── int_wallet_first_seen.sql
│   │   │   ├── int_wallet_segments.sql
│   │   │   └── int_defi_protocol_balances.sql
│   │   └── marts/
│   │       ├── _marts__models.yml
│   │       ├── fct_daily_supply_metrics.sql
│   │       ├── fct_daily_holder_metrics.sql
│   │       ├── fct_daily_transfer_metrics.sql
│   │       ├── fct_daily_concentration.sql
│   │       ├── fct_daily_defi_tvl.sql
│   │       ├── fct_large_transactions.sql
│   │       ├── dim_wallets.sql
│   │       └── dim_defi_protocols.sql
│   ├── seeds/
│   │   ├── defi_protocols.csv            Known DeFi contract addresses
│   │   └── known_wallets.csv             Labeled wallets (exchanges, treasury)
│   ├── tests/
│   │   ├── assert_supply_non_negative.sql
│   │   ├── assert_no_null_addresses.sql
│   │   └── assert_balance_matches_supply.sql
│   └── macros/
│       └── hex_to_decimal.sql            (if needed for DuckDB)
│
├── streamlit_app/                        ── DASHBOARD
│   ├── app.py                            Main entry point
│   ├── pages/
│   │   ├── 1_supply_adoption.py
│   │   ├── 2_who_holds_pyusd.py
│   │   └── 3_health_compliance.py
│   └── components/
│       ├── kpi_card.py
│       └── charts.py
│
├── .github/
│   └── workflows/
│       └── dbt_ci.yml
│
├── paxos_data_analyst_job_description.txt  ← Reference (existing)
├── paxos_overview.txt                      ← Reference (existing)
└── paxos_portfolio_strategy.md             ← Reference (existing)
```

---

## 5. Analyses to Build (6 total, 3 dashboard pages)

### Page 1: "Supply & Adoption"

#### Analysis 1: Supply Dynamics
- **Source models:** `fct_daily_supply_metrics`
- **Metrics:** total supply, daily mints, daily burns, net mints, 7d rolling net mint rate, supply growth velocity (WoW %)
- **Visuals:** area chart (supply over time), bar chart (daily net mints — green/red), annotated line for OCC charter date (Dec 2025)
- **Business question:** *"Is PYUSD's post-OCC growth real and accelerating?"*

#### Analysis 2: Holder Growth & Retention
- **Source models:** `fct_daily_holder_metrics`
- **Metrics:** unique holders (balance > 0), new wallets/day, churned wallets/day (balance → 0), net new holders, stickiness ratio (active in last 7d ÷ total holders)
- **Visuals:** line chart (holders), stacked bar (new vs churned), KPI card (stickiness)
- **Business question:** *"Is adoption broadening, or are the same wallets recycling tokens?"*

### Page 2: "Who Holds PYUSD"

#### Analysis 3: Wallet Concentration & Whale Tracking
- **Source models:** `fct_daily_concentration`, `dim_wallets`
- **Metrics:** top 10/50 holder share %, Gini coefficient, HHI index, wallet tier distribution (micro <$1K / small $1K-$100K / medium $100K-$1M / large $1M-$10M / mega $10M+)
- **Visuals:** stacked area (top10/50 share over time), line (Gini over time), table (tier snapshot)
- **Business question:** *"How concentrated is PYUSD, and is distribution improving or worsening?"*

#### Analysis 4: DeFi Protocol Distribution
- **Source models:** `fct_daily_defi_tvl`, `dim_defi_protocols`
- **Metrics:** PYUSD held per DeFi protocol (Aave, Curve, Uniswap, others), DeFi share (DeFi holdings ÷ total supply), daily inflow/outflow per protocol
- **Visuals:** stacked area (DeFi TVL by protocol), treemap (protocol breakdown), KPI (DeFi share %)
- **Business question:** *"Where is PYUSD deployed in DeFi, and is there protocol concentration risk?"*

### Page 3: "Health & Compliance"

#### Analysis 5: Large Transaction Surveillance
- **Source models:** `fct_large_transactions`, `dim_wallets`
- **Metrics:** daily count of transfers > $100K, daily count > $1M, large tx volume share (% of total), velocity anomaly days (>2σ from 30d rolling avg), top 20 largest individual transfers
- **Visuals:** bar chart (large tx count), KPI (large tx share %), sortable table (top transfers with wallet labels)
- **Business question:** *"What does PYUSD's large transaction activity look like for regulatory monitoring?"*

#### Analysis 6: Transaction Behavior Patterns
- **Source models:** `fct_daily_transfer_metrics`
- **Metrics:** daily transfer volume, daily tx count, average transfer size, velocity (volume ÷ supply), transfer size distribution (histogram with log-scale buckets), activity heatmap (day-of-week × hour-of-day)
- **Visuals:** dual-axis line (volume + count), histogram (size distribution), heatmap (activity patterns)
- **Business question:** *"Who is using PYUSD — institutions or retail? When is it most active?"*

---

## 6. dbt Model Specifications

### Staging Layer
| Model | Input | Logic | Output Columns |
|---|---|---|---|
| `stg_pyusd_transfers` | `raw_pyusd_transfers` | Clean types, add `transfer_date`, cast amount to DECIMAL | transfer_date, block_number, timestamp, tx_hash, from_address, to_address, amount, is_mint, is_burn |
| `stg_pyusd_mints` | `stg_pyusd_transfers` | WHERE is_mint = true | Same columns |
| `stg_pyusd_burns` | `stg_pyusd_transfers` | WHERE is_burn = true | Same columns |

### Intermediate Layer
| Model | Input | Logic | Output Columns |
|---|---|---|---|
| `int_daily_supply` | `stg_pyusd_mints`, `stg_pyusd_burns` | Aggregate daily mints/burns, compute cumulative supply via window function | date, daily_mints, daily_burns, daily_mint_count, daily_burn_count, net_mints, cumulative_supply |
| `int_wallet_daily_balances` | `stg_pyusd_transfers` | For each wallet, sum(received) - sum(sent) as running balance, partitioned by wallet | date, wallet_address, daily_inflow, daily_outflow, daily_net, running_balance |
| `int_wallet_first_seen` | `stg_pyusd_transfers` | MIN(transfer_date) per unique wallet (both from and to) | wallet_address, first_seen_date |
| `int_wallet_segments` | `int_wallet_daily_balances` | Classify latest balance into tiers: micro/small/medium/large/mega | wallet_address, balance, segment |
| `int_defi_protocol_balances` | `int_wallet_daily_balances`, `seed: defi_protocols` | Join balances with known DeFi addresses | date, protocol_name, protocol_type, balance |

### Mart Layer
| Model | Input | Output Columns |
|---|---|---|
| `fct_daily_supply_metrics` | `int_daily_supply` | date, total_supply, daily_mints, daily_burns, net_mints, mint_count, burn_count, supply_growth_wow_pct, rolling_7d_net_mints |
| `fct_daily_holder_metrics` | `int_wallet_daily_balances`, `int_wallet_first_seen` | date, total_holders, new_wallets, churned_wallets, net_new, active_wallets_7d, stickiness_ratio |
| `fct_daily_transfer_metrics` | `stg_pyusd_transfers`, `int_daily_supply` | date, transfer_volume, transfer_count, avg_transfer_size, velocity, hour_of_day (for heatmap) |
| `fct_daily_concentration` | `int_wallet_daily_balances`, `int_daily_supply` | date, top10_share_pct, top50_share_pct, gini_coefficient, hhi_index, count_by_segment |
| `fct_daily_defi_tvl` | `int_defi_protocol_balances` | date, protocol_name, protocol_type, pyusd_balance, share_of_supply |
| `fct_large_transactions` | `stg_pyusd_transfers`, `seed: known_wallets` | date, tx_hash, from_address, from_label, to_address, to_label, amount (WHERE amount > 100000) |
| `dim_wallets` | `int_wallet_segments`, `int_wallet_first_seen`, `seed: known_wallets` | wallet_address, label, wallet_type, segment, first_seen_date, current_balance |
| `dim_defi_protocols` | `seed: defi_protocols` | address, protocol_name, protocol_type |

### dbt Tests
| Test | What It Validates |
|---|---|
| `assert_supply_non_negative` | `cumulative_supply >= 0` in `int_daily_supply` |
| `assert_no_null_addresses` | No NULL in from_address or to_address in `stg_pyusd_transfers` |
| `assert_balance_matches_supply` | `SUM(all wallet balances) == total supply` on final day |

### dbt Seeds
| Seed File | Purpose | Columns |
|---|---|---|
| `defi_protocols.csv` | Map DeFi contract addresses to protocol names | address, protocol_name, protocol_type |
| `known_wallets.csv` | Label known wallets (exchanges, Paxos, etc.) | address, label, wallet_type |

---

## 7. Build Sequence

| Step | Description | Depends On | Est. Time |
|---|---|---|---|
| **1. Setup** | Create folder structure, activate venv, `pip install` all dependencies | Nothing | 30 min |
| **2. Extract** | Write `extract/extract_pyusd_transfers.py` — query Etherscan getLogs, decode hex, save Parquet | Step 1 + Etherscan API key | 2-4 hrs |
| **3. Seeds** | Create `defi_protocols.csv` and `known_wallets.csv` by looking up top PYUSD holders on Etherscan | Step 2 (need to see which addresses appear) | 1 hr |
| **4. dbt init** | `dbt init dbt_project`, configure `profiles.yml` for DuckDB, load Parquet as source table, `dbt seed` | Step 1 + Step 3 | 1-2 hrs |
| **5. dbt models** | Build staging → intermediate → marts, run `dbt build` and `dbt test` | Step 2 + Step 4 | 1-2 days |
| **6. Streamlit** | Build 3-page dashboard reading from DuckDB marts | Step 5 | 1-1.5 days |
| **7. README** | Write executive memo with findings and recommendations | Step 6 (need dashboard insights) | 0.5 day |
| **8. Polish** | Clean code, add dbt docs, create GitHub CI, push to GitHub with feature branches | Step 7 | 0.5 day |

---

## 8. Key Design Decisions (Reference)

1. **DuckDB over Snowflake** — Local, free, zero setup. dbt-duckdb adapter means SQL is transferable to Snowflake. Hiring team evaluates dbt skills, not which warehouse.

2. **Etherscan getLogs over tokentx** — `tokentx` requires a wallet address. `getLogs` queries all Transfer events for the contract directly. 1,000 results/page with block-range pagination.

3. **Parquet as intermediate format** — Columnar, compressed, fast reads. DuckDB reads Parquet natively via `read_parquet()`.

4. **No CoinGecko / price analysis** — Scope intentionally limited to on-chain Transfer events only. Peg stability analysis was dropped to keep the data pipeline clean and single-source.

5. **Gini coefficient and HHI** — Finance-standard concentration metrics. Differentiates this project from typical crypto dashboards that only show "top 10 holders."

6. **README as business memo** — Not a technical tutorial. Written as if presenting to a VP of Product at Paxos. Findings + recommendations, not just charts.

7. **Feature branches in Git** — Show Git workflow competence per the JD: `feat/extraction`, `feat/dbt-staging`, `feat/dbt-marts`, `feat/streamlit-dashboard`.

---

## 9. Paxos JD Alignment Map

| JD Requirement | Project Proof |
|---|---|
| *"Own analytics for Paxos stablecoins"* | 6 analyses covering supply, adoption, risk, compliance for PYUSD |
| *"Defining core metrics, building dashboards"* | 20+ metrics across 3 Streamlit pages |
| *"Translate ambiguous questions into scoped data problems"* | README: question → scope → finding → recommendation |
| *"Shape and improve dbt models in Snowflake/dbt"* | Full dbt project: staging → intermediate → marts + tests |
| *"Git-based workflows, CI/CD"* | GitHub repo with feature branches, PR history, CI workflow |
| *"AI-powered tooling"* | Document Claude usage in development process |
| *"Regulatory reporting and audits"* | Large tx surveillance + concentration risk + supply reconciliation test |
| *"Not just editing SQL — reasoning about model design"* | Each model has documented design rationale |
| *"Excellent communication skills"* | Executive memo README for non-technical stakeholders |

---

## 10. Paxos Context (for AI Agent Reference)

- **Paxos** is a regulated blockchain infrastructure company (OCC trust charter, NYDFS regulated)
- **PYUSD** is PayPal's stablecoin, issued by Paxos, launched Aug 2023
- **OCC trust charter** completed Dec 2025 — made PYUSD the largest federally-regulated stablecoin
- **Partner Rewards Engine** launched Mar 2, 2026 (closed alpha) — daily on-chain rewards for enterprise partners
- **Total Paxos assets** grew from $1.2B to $8B (500% YoY) by Mar 2026
- **PYUSD market cap:** ~$4B as of Mar 2026
- **PYUSD holders on Ethereum:** ~68,000 wallets
- **Whale concentration:** ~91% held by top wallets
- **Key DeFi presence:** Aave V3 (~$480M TVL), Curve (~$28M), limited Uniswap
- **Paxos core values:** Search for the Truth, Shared Commitment to Excellence, Real-Time Candor, Be an Owner

---

## 11. File Naming Conventions

- **dbt models:** snake_case, prefixed by layer (`stg_`, `int_`, `fct_`, `dim_`)
- **dbt schema files:** `_<layer>__models.yml` (e.g., `_staging__models.yml`)
- **Python files:** snake_case
- **Streamlit pages:** numbered prefix for ordering (`1_supply_adoption.py`)
- **Seeds:** descriptive snake_case CSV files
