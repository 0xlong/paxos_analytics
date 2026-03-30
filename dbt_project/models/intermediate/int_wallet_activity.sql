-- =============================================================================
-- int_wallet_activity.sql
-- =============================================================================
-- PURPOSE: Roll up all historical activity at the wallet level.
-- Every address that ever sent or received PYUSD gets one row with their
-- lifetime stats.
--
-- CRITICAL: Our transfer data starts Oct 1, 2025. Many wallets held PYUSD
-- before that date. We use the starting_balances seed (top 10K holders at
-- Oct 1) to calculate TRUE current balances:
--
--   True Balance = Starting Balance + Received (since Oct 1) - Sent (since Oct 1)
--
-- Wallets not in the seed are assumed to have had a starting balance of 0.
-- This is accurate for wallets that first appeared AFTER Oct 1, but
-- underestimates balances for small holders active before Oct 1 who didn't
-- make the top 10K. This is an acceptable trade-off documented here.

with sender_stats as (

    select 
        from_address as wallet_address,
        count(transfer_id) as tx_sent_count,
        sum(amount_pyusd)  as total_sent_amount,
        min(block_timestamp) as first_sent_at,
        max(block_timestamp) as last_sent_at
    from {{ ref('stg_pyusd_transfers') }}
    group by 1

),

receiver_stats as (

    select 
        to_address as wallet_address,
        count(transfer_id) as tx_received_count,
        sum(amount_pyusd)  as total_received_amount,
        min(block_timestamp) as first_received_at,
        max(block_timestamp) as last_received_at
    from {{ ref('stg_pyusd_transfers') }}
    group by 1

),

-- Starting balances from the seed (top 10K holders at Oct 1, 2025)
initial_balances as (

    select
        lower(address) as wallet_address,
        balance as starting_balance
    from {{ ref('starting_balances') }}

),

combined as (

    -- Full outer join: a wallet might only send, only receive, or only
    -- exist in the starting balance seed (if it held PYUSD but never
    -- transacted during our window)
    select
        coalesce(s.wallet_address, r.wallet_address, ib.wallet_address) as wallet_address,
        
        coalesce(s.tx_sent_count, 0)         as tx_sent_count,
        coalesce(s.total_sent_amount, 0)     as total_sent_amount,
        
        coalesce(r.tx_received_count, 0)     as tx_received_count,
        coalesce(r.total_received_amount, 0) as total_received_amount,

        -- Starting balance (0 if not in the seed)
        coalesce(ib.starting_balance, 0)     as starting_balance,

        -- First/last active timestamps
        case 
            when s.first_sent_at is null then r.first_received_at
            when r.first_received_at is null then s.first_sent_at
            when s.first_sent_at < r.first_received_at then s.first_sent_at
            else r.first_received_at
        end as first_active_at,

        case 
            when s.last_sent_at is null then r.last_received_at
            when r.last_received_at is null then s.last_sent_at
            when s.last_sent_at > r.last_received_at then s.last_sent_at
            else r.last_received_at
        end as last_active_at

    from sender_stats s
    full outer join receiver_stats r 
        on s.wallet_address = r.wallet_address
    full outer join initial_balances ib
        on coalesce(s.wallet_address, r.wallet_address) = ib.wallet_address

)

select 
    wallet_address,
    starting_balance,
    tx_sent_count,
    total_sent_amount,
    tx_received_count,
    total_received_amount,
    (tx_sent_count + tx_received_count) as total_tx_count,

    -- TRUE current balance = what they started with + what they received - what they sent
    (starting_balance + total_received_amount - total_sent_amount) as current_balance,

    first_active_at,
    last_active_at,
    -- Account age in days (null for wallets that only exist in starting balances)
    case 
        when first_active_at is not null and last_active_at is not null
        then date_diff('day', cast(first_active_at as date), cast(last_active_at as date))
        else null
    end as account_age_days
from combined
