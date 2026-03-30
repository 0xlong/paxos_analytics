-- TEST: Ethereum address format validation.
-- Every valid Ethereum address is exactly 42 characters: '0x' + 40 hex digits.
-- Shorter = truncated (ETL bug). Longer = padding not stripped. Wrong prefix = wrong encoding.
--
-- We check both from_address and to_address in one query using UNION ALL.

select
    transfer_id,
    tx_hash,
    'from_address' as column_name,
    from_address   as bad_value
from {{ ref('stg_pyusd_transfers') }}
where length(from_address) != 42
   or left(from_address, 2) != '0x'

union all

select
    transfer_id,
    tx_hash,
    'to_address'  as column_name,
    to_address    as bad_value
from {{ ref('stg_pyusd_transfers') }}
where length(to_address) != 42
   or left(to_address, 2) != '0x'
