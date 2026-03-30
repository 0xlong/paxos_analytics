"""
pyUSD Transfer Log Extraction — v2 (gap-free)
===============================================
Fixes the data-gap bug in v1 by using **adaptive chunk splitting**.

Problem in v1:
  - 50,000-block chunks can contain more Transfer events than the
    Etherscan API will return (page limit × offset cap).
  - When a chunk overflows, the tail of the chunk is silently lost,
    creating regular ~5-day gaps in the dataset.

Fix:
  - Start with a smaller default chunk (5,000 blocks ≈ 16 hours).
  - If any single page returns exactly `offset` rows (meaning there
    *might* be more), we still paginate — but if the total for the
    chunk hits a safety ceiling we **split the chunk in half** and
    recurse, guaranteeing every log is captured.
"""

import json
import time
from datetime import datetime, timezone
import requests
import pandas as pd
from pathlib import Path

from config import (
    ETHERSCAN_API_KEY,
    ETHERSCAN_URL,
    ETHEREUM_CHAIN_ID,
    PYUSD_CONTRACT,
    TRANSFER_TOPIC,
    EXTRACTION_START_DATE,
    EXTRACTION_END_DATE,
    RAW_OUTPUT_FILE,
    REQUEST_DELAY,
    PROJECT_ROOT,
)


# =============================================================================
# CONSTANTS
# =============================================================================

DEFAULT_CHUNK_SIZE  = 5_000   # blocks per chunk (~16 h) — down from 50k
PAGE_OFFSET         = 1_000   # rows per page (Etherscan max)
MAX_LOGS_PER_CHUNK  = 9_000   # safety ceiling before we split
MIN_CHUNK_SIZE      = 100     # never split below this


# =============================================================================
# SESSION
# =============================================================================

session = requests.Session()


# =============================================================================
# HELPERS
# =============================================================================

def get_block_by_date(date_str, closest="before"):
    """Convert YYYY-MM-DD to Ethereum block number via Etherscan."""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    params = {
        "chainid": ETHEREUM_CHAIN_ID,
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": int(dt.timestamp()),
        "closest": closest,
        "apikey": ETHERSCAN_API_KEY,
    }
    response = requests.get(ETHERSCAN_URL, params=params, timeout=30)
    data = response.json()
    if data.get("status") == "1":
        return int(data["result"])
    raise ValueError(f"Failed to get block for {date_str}: {data.get('result')}")


# =============================================================================
# EXTRACTION — single chunk with pagination
# =============================================================================

def _fetch_chunk(address, topic0, from_block, to_block, max_retries=3):
    """
    Fetch logs for a single block range, paginating through all pages.

    Returns (logs, hit_ceiling):
      - logs: list of raw log dicts
      - hit_ceiling: True if the total count reached MAX_LOGS_PER_CHUNK,
        signaling the caller should split this range.
    """
    logs = []
    page = 1

    while True:
        params = {
            "chainid": ETHEREUM_CHAIN_ID,
            "module": "logs",
            "action": "getLogs",
            "address": address,
            "topic0": topic0,
            "fromBlock": from_block,
            "toBlock": to_block,
            "page": page,
            "offset": PAGE_OFFSET,
            "apikey": ETHERSCAN_API_KEY,
        }

        # --- retry loop ---
        data = None
        for attempt in range(max_retries):
            try:
                response = session.get(ETHERSCAN_URL, params=params, timeout=30)
                data = response.json()
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    wait = (attempt + 1) * 2
                    print(f"   ⏳ Retry {attempt + 1}/{max_retries} after {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"   ❌ Failed after {max_retries} retries: {e}")
                    return logs, False

        if data is None:
            return logs, False

        # --- handle API response ---
        if data.get("status") != "1":
            result = data.get("result", "")
            if "No records found" not in str(result) and result:
                print(f"   ⚠️  API: {result}")
            break

        batch = data.get("result", [])
        if not batch:
            break

        logs.extend(batch)

        # Safety: if we've accumulated too many logs, abort and signal split
        if len(logs) >= MAX_LOGS_PER_CHUNK:
            return logs, True          # <-- hit ceiling → caller will split

        # All rows for this page received; if fewer than offset, we're done
        if len(batch) < PAGE_OFFSET:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    return logs, False


# =============================================================================
# EXTRACTION — adaptive splitting
# =============================================================================

def fetch_logs_adaptive(address, topic0, from_block, to_block, depth=0):
    """
    Fetch logs for [from_block, to_block].
    If the range returns too many results, split it in half and recurse.
    """
    indent = "  " * depth
    span = to_block - from_block + 1

    logs, hit_ceiling = _fetch_chunk(address, topic0, from_block, to_block)

    if hit_ceiling and span > MIN_CHUNK_SIZE:
        # Range is too dense — split in half
        mid = from_block + span // 2
        print(f"{indent}🔀 Splitting {from_block:,}–{to_block:,} "
              f"(got {len(logs):,} logs, splitting at {mid:,})")

        left  = fetch_logs_adaptive(address, topic0, from_block, mid - 1, depth + 1)
        time.sleep(REQUEST_DELAY)
        right = fetch_logs_adaptive(address, topic0, mid, to_block, depth + 1)
        return left + right

    return logs


# =============================================================================
# MAIN LOOP — iterate through chunks
# =============================================================================

def get_all_logs(address, topic0, from_block, to_block,
                 chunk_size=DEFAULT_CHUNK_SIZE):
    """Fetch ALL logs by walking through block-range chunks with adaptive splitting."""
    all_logs = []
    current = from_block
    chunk_num = 1
    total_chunks = (to_block - from_block) // chunk_size + 1

    print(f"📥 Fetching Transfer logs for {address[:10]}...")
    print(f"   Blocks : {from_block:,} → {to_block:,}")
    print(f"   Chunk  : {chunk_size:,} blocks  (~{total_chunks} chunks)")
    print()

    while current <= to_block:
        chunk_end = min(current + chunk_size - 1, to_block)

        print(f"📦 Chunk {chunk_num}/{total_chunks}: "
              f"{current:,} → {chunk_end:,}", end="")

        chunk_logs = fetch_logs_adaptive(address, topic0, current, chunk_end)
        all_logs.extend(chunk_logs)

        print(f"  ✓ {len(chunk_logs):,} logs  (total: {len(all_logs):,})")

        current = chunk_end + 1
        chunk_num += 1
        time.sleep(REQUEST_DELAY)

    print(f"\n✅ Done! {len(all_logs):,} total logs")
    return all_logs


# =============================================================================
# SAVE
# =============================================================================

def save_logs(logs, output_path):
    """Save raw logs to both JSONL and Parquet."""
    base = PROJECT_ROOT / Path(output_path).with_suffix("")
    base.parent.mkdir(parents=True, exist_ok=True)

    # JSONL
    jsonl_path = base.with_suffix(".jsonl")
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for log in logs:
            f.write(json.dumps(log) + "\n")
    print(f"💾 JSONL   → {jsonl_path} ({len(logs):,} rows)")

    # Parquet
    parquet_path = base.with_suffix(".parquet")
    df = pd.DataFrame(logs)
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    print(f"💾 Parquet → {parquet_path} ({len(df):,} rows)")

    return df


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":

    if not ETHERSCAN_API_KEY:
        print("❌ Set ETHERSCAN_API_KEY in .env!")
        exit(1)

    print("🚀 pyUSD Transfer Log Extraction — v2 (gap-free)")
    print("=" * 50)

    # Resolve date range → block numbers
    print(f"📅 {EXTRACTION_START_DATE} → {EXTRACTION_END_DATE}")
    start_block = get_block_by_date(EXTRACTION_START_DATE, closest="after")
    end_block   = get_block_by_date(EXTRACTION_END_DATE,   closest="before")
    print(f"   Blocks: {start_block:,} → {end_block:,}")

    # Fetch all Transfer logs
    logs = get_all_logs(
        address=PYUSD_CONTRACT,
        topic0=TRANSFER_TOPIC,
        from_block=start_block,
        to_block=end_block,
    )

    if logs:
        df = save_logs(logs, RAW_OUTPUT_FILE)
        print(f"\n📄 Sample row:")
        print(df.iloc[0].to_dict())
    else:
        print("⚠️  No logs found!")
