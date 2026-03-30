-- =============================================================================
-- fct_large_transactions
-- =============================================================================
-- ANSWERS: "What does PYUSD's large transaction activity look like for
--           regulatory monitoring?"
--
-- Filters stg_pyusd_transfers for any transfer > $100K PYUSD and enriches
-- with wallet labels from the seed. This is the table a compliance team
-- would review daily.

with large_txs as (

    select
        transfer_id,
        block_number,
        block_timestamp,
        transfer_date,
        tx_hash,
        log_index,
        from_address,
        to_address,
        amount_pyusd,
        transfer_type

    from {{ ref('stg_pyusd_transfers') }}
    where amount_pyusd >= 100000

),

labeled as (

    select
        lt.*,

        -- Join sender label
        coalesce(sender.label, 'Unknown') as from_label,

        -- Join receiver label
        coalesce(receiver.label, 'Unknown') as to_label,

        -- Size tier for filtering in dashboards
        case
            when lt.amount_pyusd >= 10000000 then 'mega (>$10M)'
            when lt.amount_pyusd >=  1000000 then 'large ($1M-$10M)'
            else                                  'notable ($100K-$1M)'
        end as size_tier

    from large_txs lt
    left join {{ ref('pyusd_users_labels') }} sender
        on lower(lt.from_address) = lower(sender.address)
    left join {{ ref('pyusd_users_labels') }} receiver
        on lower(lt.to_address) = lower(receiver.address)

)

select * from labeled
order by block_timestamp desc
