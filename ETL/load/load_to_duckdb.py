"""
Load decoded PYUSD transfers into DuckDB.
"""
import duckdb
from pathlib import Path

# Resolve absolute paths dynamically so the script works from any terminal folder
PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "duckdb" / "pyusd_analytics.duckdb"
PARQUET_PATH = PROJECT_ROOT / "data" / "transformed" / "pyusd_raw_logs_decoded.parquet"

def load():
    # Fail-fast: Stop execution immediately if the source data is missing
    if not PARQUET_PATH.exists():
        raise FileNotFoundError(f"Parquet file not found: {PARQUET_PATH}")

    # Ensure the target folder exists (DuckDB will error if its parent folder is missing)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    print(f"🔌 Connecting to DuckDB: {DB_PATH}")
    # Connects to the database file (DuckDB creates the file if it doesn't exist yet)
    con = duckdb.connect(str(DB_PATH))

    # Idempotency: Drop the old table first so we can safely re-run this script endlessly
    con.execute("DROP TABLE IF EXISTS raw_pyusd_transfers")
    
    # Use forward slashes: DuckDB's SQL engine will crash on Windows backslashes (\)
    parquet_posix = PARQUET_PATH.as_posix()
    
    # Core ETL logic: Read the parquet file directly into a brand new database table
    con.execute(f"CREATE TABLE raw_pyusd_transfers AS SELECT * FROM read_parquet('{parquet_posix}')")

    # Data Validation: Query the db we just created to confirm the rows actually made it in 
    count = con.execute("SELECT COUNT(*) FROM raw_pyusd_transfers").fetchone()[0]
    sample = con.execute("SELECT * FROM raw_pyusd_transfers LIMIT 1").fetchdf()
    
    print(f"✅ Loaded {count:,} rows into raw_pyusd_transfers")
    print(f"📄 Columns: {list(sample.columns)}")
    
    # Close connection to release the lock on the database file
    con.close()

if __name__ == "__main__":
    load()
