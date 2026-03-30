-- =============================================================================
-- fct_wallet_concentration
-- =============================================================================
-- ANSWERS: "How concentrated is PYUSD, and is distribution improving?"
--
-- Uses int_wallet_activity which now includes starting balances from the
-- Oct 1, 2025 snapshot seed. This means balance rankings are accurate
-- for the top 10K holders.

with wallet_balances as (

    select
        wallet_address,
        current_balance as balance,
        total_tx_count
    from {{ ref('int_wallet_activity') }}
    where current_balance > 0

),

total as (

    select sum(balance) as total_supply from wallet_balances

),

ranked as (

    select
        w.wallet_address,
        w.balance,
        w.total_tx_count,
        t.total_supply,

        row_number() over (order by w.balance desc) as balance_rank,

        round((w.balance / t.total_supply) * 100, 4) as share_pct,

        case
            when w.balance >= 10000000 then 'mega (>$10M)'
            when w.balance >=  1000000 then 'large ($1M-$10M)'
            when w.balance >=   100000 then 'medium ($100K-$1M)'
            when w.balance >=     1000 then 'small ($1K-$100K)'
            else                            'micro (<$1K)'
        end as wallet_tier

    from wallet_balances w
    cross join total t

)

select * from ranked
order by balance_rank
