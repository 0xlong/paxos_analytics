-- TEST: Mint flag consistency.
-- is_mint = true means the transfer came FROM the zero address (new tokens printed).
-- If is_mint is true but from_address is NOT the zero address, the flag was set wrongly.
--
-- This validates the ETL decode logic, not just the dbt model.

select
    transfer_id,
    tx_hash,
    from_address,
    is_mint
from {{ ref('stg_pyusd_transfers') }}
where is_mint = true
  and from_address != '0x0000000000000000000000000000000000000000'
