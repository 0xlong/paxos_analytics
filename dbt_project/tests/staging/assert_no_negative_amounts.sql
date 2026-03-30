-- TEST: No transfer should have a negative amount.
-- A negative token amount is physically impossible on Ethereum (uint256 in the ABI).
-- If this fires, something went wrong in the ETL decode step.
--
-- Pattern: return the OFFENDING rows. 0 rows = pass.

select
    transfer_id,
    tx_hash,
    log_index,
    amount_pyusd
from {{ ref('stg_pyusd_transfers') }}
where amount_pyusd < 0
