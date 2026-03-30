-- TEST: Transaction hash format validation.
-- Every Ethereum tx hash is exactly 66 characters: '0x' + 64 hex digits (32 bytes).
-- If this fails, the hash was truncated or corrupted during ETL ingestion.
-- A bad tx_hash also means transfer_id (which is built from it) is unreliable.

select
    transfer_id,
    tx_hash,
    length(tx_hash) as actual_length
from {{ ref('stg_pyusd_transfers') }}
where length(tx_hash) != 66
   or left(tx_hash, 2) != '0x'
