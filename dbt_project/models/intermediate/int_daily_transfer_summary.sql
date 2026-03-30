-- =============================================================================
-- int_daily_transfer_summary.sql
-- =============================================================================
-- PURPOSE: Aggregate transfer volume and counts at the daily level.
-- This serves as a quick lookup for daily volume charts and reduces the
-- billions of rows to just ~3 rows per day (mint, burn, transfer).

with daily_aggregates as (

    select
        transfer_date,
        transfer_type,
        
        -- Business logic: calculate core daily metrics
        count(transfer_id)      as daily_tx_count,
        sum(amount_pyusd)       as daily_volume_pyusd,
        avg(amount_pyusd)       as avg_tx_size_pyusd,
        count(distinct from_address) as unique_senders,
        count(distinct to_address)   as unique_receivers

    from {{ ref('stg_pyusd_transfers') }}
    group by 1, 2

)

select * from daily_aggregates
order by transfer_date desc, transfer_type
