-- TEST: Burn flag consistency.
-- is_burn = true means tokens were sent TO the zero address (destroyed / removed from supply).
-- If is_burn is true but to_address is NOT the zero address, the flag was derived incorrectly.

select
    transfer_id,
    tx_hash,
    to_address,
    is_burn
from {{ ref('stg_pyusd_transfers') }}
where is_burn = true
  and to_address != '0x0000000000000000000000000000000000000000'
