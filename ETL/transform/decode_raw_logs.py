"""
Decode Raw PYUSD Transfer Logs
===============================
Reads raw hex Parquet from extraction step, decodes all fields
into clean typed columns, and writes to data/transformed/.

Input:  data/raw/pyusd_raw_logs.parquet  (raw hex from Etherscan)
Output: data/transformed/pyusd_raw_logs_decoded.parquet  (decoded, typed, analytics-ready)
"""

import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent.parent  # analytics_paxos/
RAW_INPUT = PROJECT_ROOT / "data" / "raw" / "pyusd_raw_logs.parquet"
TRANSFORMED_OUTPUT = PROJECT_ROOT / "data" / "transformed" / "pyusd_raw_logs_decoded.parquet"

# PYUSD has 6 decimals
PYUSD_DECIMALS = 6
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


# =============================================================================
# DECODERS
# =============================================================================

def hex_to_int(hex_str: str) -> int:
    """Convert hex string (with or without 0x prefix) to integer."""
    if hex_str in ("0x", "0x0", "", None):
        return 0
    return int(hex_str, 16)


def decode_address(padded_hex: str) -> str:
    """
    Extract 20-byte address from 32-byte zero-padded topic.
    '0x00000000000000000000000049ba491...' → '0x49ba491...'
    """
    return "0x" + padded_hex[-40:]


def decode_amount(data_hex: str, decimals: int = PYUSD_DECIMALS) -> float:
    """
    Decode uint256 hex data field to human-readable token amount.
    '0x0000...05f5e100' → 100.0 (for 6 decimals)
    """
    raw = hex_to_int(data_hex)
    return raw / (10 ** decimals)


def hex_to_datetime(hex_timestamp: str) -> datetime:
    """Convert hex Unix timestamp to UTC datetime."""
    return datetime.fromtimestamp(hex_to_int(hex_timestamp), tz=timezone.utc)


# =============================================================================
# MAIN TRANSFORM
# =============================================================================

def decode_logs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Decode raw Etherscan log DataFrame into clean typed columns.

    Raw schema (each row):
        address, topics (list of 3), data, blockNumber, blockHash,
        timeStamp, gasPrice, gasUsed, logIndex, transactionHash, transactionIndex

    Output schema:
        block_number     int64     — Ethereum block number
        timestamp        datetime  — UTC datetime of the block
        tx_hash          str       — Transaction hash
        log_index        int64     — Log position within the block
        from_address     str       — Sender (0x... 20-byte)
        to_address       str       — Receiver (0x... 20-byte)
        amount           float64   — PYUSD amount (human-readable, 6 decimals)
        is_mint          bool      — True if from_address is zero address
        is_burn          bool      — True if to_address is zero address
        transfer_date    date      — Date extracted from timestamp (for daily aggregations)
    """
    print(f"🔄 Decoding {len(df):,} raw logs...")

    # --- Extract from/to from topics list ---
    df["from_address"] = df["topics"].apply(lambda t: decode_address(t[1]))
    df["to_address"] = df["topics"].apply(lambda t: decode_address(t[2]))

    # --- Decode amount ---
    df["amount"] = df["data"].apply(decode_amount)

    # --- Decode block number ---
    df["block_number"] = df["blockNumber"].apply(hex_to_int)

    # --- Decode timestamp → datetime ---
    df["timestamp"] = df["timeStamp"].apply(hex_to_datetime)

    # --- Transfer date (for daily grouping in dbt) ---
    df["transfer_date"] = df["timestamp"].dt.date

    # --- Transaction identifiers ---
    df["tx_hash"] = df["transactionHash"]
    df["log_index"] = df["logIndex"].apply(hex_to_int)

    # --- Mint / Burn flags ---
    df["is_mint"] = df["from_address"] == ZERO_ADDRESS
    df["is_burn"] = df["to_address"] == ZERO_ADDRESS

    # --- Select & order final columns ---
    output_cols = [
        "block_number",
        "timestamp",
        "transfer_date",
        "tx_hash",
        "log_index",
        "from_address",
        "to_address",
        "amount",
        "is_mint",
        "is_burn",
    ]

    result = df[output_cols].copy()

    # --- Cast types explicitly for Parquet schema consistency ---
    result["block_number"] = result["block_number"].astype("int64")
    result["log_index"] = result["log_index"].astype("int64")
    result["timestamp"] = pd.to_datetime(result["timestamp"], utc=True)
    result["transfer_date"] = pd.to_datetime(result["transfer_date"])
    result["amount"] = result["amount"].astype("float64")

    return result


def print_summary(df: pd.DataFrame) -> None:
    """Print a quick data quality summary after decoding."""
    total = len(df)
    mints = df["is_mint"].sum()
    burns = df["is_burn"].sum()
    regular = total - mints - burns
    total_volume = df["amount"].sum()
    unique_senders = df["from_address"].nunique()
    unique_receivers = df["to_address"].nunique()
    date_range = f"{df['transfer_date'].min()} → {df['transfer_date'].max()}"

    print(f"\n📊 Decoded Transfer Summary")
    print(f"   {'Total transfers:':<25} {total:>12,}")
    print(f"   {'Mints:':<25} {mints:>12,}")
    print(f"   {'Burns:':<25} {burns:>12,}")
    print(f"   {'Regular transfers:':<25} {regular:>12,}")
    print(f"   {'Total volume (PYUSD):':<25} {total_volume:>18,.2f}")
    print(f"   {'Unique senders:':<25} {unique_senders:>12,}")
    print(f"   {'Unique receivers:':<25} {unique_receivers:>12,}")
    print(f"   {'Date range:':<25} {date_range}")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    print("🚀 PYUSD Raw Log Decoder")
    print("=" * 50)

    # --- Read raw Parquet ---
    if not RAW_INPUT.exists():
        print(f"❌ Raw file not found: {RAW_INPUT}")
        exit(1)

    print(f"📂 Reading: {RAW_INPUT}")
    raw_df = pd.read_parquet(RAW_INPUT)
    print(f"   {len(raw_df):,} rows loaded")

    # --- Decode ---
    decoded_df = decode_logs(raw_df)

    # --- Save ---
    TRANSFORMED_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    decoded_df.to_parquet(TRANSFORMED_OUTPUT, index=False, engine="pyarrow")
    print(f"\n💾 Saved: {TRANSFORMED_OUTPUT}")
    print(f"   {len(decoded_df):,} rows, {TRANSFORMED_OUTPUT.stat().st_size / 1e6:.1f} MB")

    # --- Summary ---
    print_summary(decoded_df)

    # --- Sample ---
    print(f"\n📄 Sample decoded row:")
    print(decoded_df.iloc[0].to_dict())
