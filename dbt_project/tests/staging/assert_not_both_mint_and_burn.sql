-- TEST: A transfer cannot be both a mint and a burn at the same time.
-- Mint  = from zero address (0x000...0)
-- Burn  = to   zero address (0x000...0)
-- Both at once = 0x0 sending to 0x0 = economically meaningless / likely ETL corruption.
--
-- In practice this should never exist on-chain; if it appears, something is wrong.

select
    transfer_id,
    tx_hash,
    from_address,
    to_address,
    is_mint,
    is_burn
from {{ ref('stg_pyusd_transfers') }}
where is_mint = true
  and is_burn = true
