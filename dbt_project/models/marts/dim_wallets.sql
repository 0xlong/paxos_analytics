-- =============================================================================
-- dim_wallets
-- =============================================================================
-- PURPOSE: The single source of truth for "who is this wallet?"
--
-- Merges lifetime activity stats from int_wallet_activity (now with true
-- balances via starting_balances seed) with human-readable labels from
-- the pyusd_users_labels seed.

with activity as (

    select * from {{ ref('int_wallet_activity') }}

),

labeled as (

    select
        a.wallet_address,

        coalesce(labels.label, 'Unknown') as wallet_label,

        case
            when a.current_balance >= 10000000 then 'mega (>$10M)'
            when a.current_balance >=  1000000 then 'large ($1M-$10M)'
            when a.current_balance >=   100000 then 'medium ($100K-$1M)'
            when a.current_balance >=     1000 then 'small ($1K-$100K)'
            when a.current_balance >        0  then 'micro (<$1K)'
            else                                    'empty (0)'
        end as wallet_tier,

        a.starting_balance,
        a.tx_sent_count,
        a.total_sent_amount,
        a.tx_received_count,
        a.total_received_amount,
        a.total_tx_count,
        a.current_balance,
        a.first_active_at,
        a.last_active_at,
        a.account_age_days

    from activity a
    left join {{ ref('pyusd_users_labels') }} labels
        on lower(a.wallet_address) = lower(labels.address)

)

select * from labeled
