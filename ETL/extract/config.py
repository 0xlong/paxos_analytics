"""
Config for pyUSD Transfer Log Extraction
=========================================
Loads API keys from .env and defines all constants.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# Etherscan API
# ---------------------------------------------------------------------------
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
ETHERSCAN_URL = "https://api.etherscan.io/v2/api"
ETHEREUM_CHAIN_ID = 1

# ---------------------------------------------------------------------------
# pyUSD Contract
# ---------------------------------------------------------------------------
PYUSD_CONTRACT = "0x6c3ea9036406852006290770BEdFcAbA0e23A0e8"

# ERC-20 Transfer(address,address,uint256) event signature
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# ---------------------------------------------------------------------------
# Extraction Date Range
# ---------------------------------------------------------------------------
EXTRACTION_START_DATE = "2025-10-01" 
EXTRACTION_END_DATE = "2026-03-15"

# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
RAW_OUTPUT_FILE = "data/raw/pyusd_raw_logs.csv"

# ---------------------------------------------------------------------------
# Rate Limiting
# ---------------------------------------------------------------------------
REQUEST_DELAY = 0.25   # seconds between API calls (Etherscan free: 5/sec)
