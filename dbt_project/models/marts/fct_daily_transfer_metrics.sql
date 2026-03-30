-- =============================================================================
-- fct_daily_transfer_metrics
-- =============================================================================
-- ANSWERS: "Who is using PYUSD — institutions or retail? When is it most active?"
--
-- Combines the daily aggregates from int_daily_transfer_summary with supply
-- data from int_supply_changes to compute velocity (volume / supply).
-- Also adds hour-of-day distribution from the raw staging model for heatmaps.

-- Part 1: Daily summary with velocity
with daily_summary as (

    select
        t.transfer_date,
        sum(t.daily_tx_count)       as total_tx_count,
        sum(t.daily_volume_pyusd)   as total_volume_pyusd,

        -- Break out by type for stacked charts
        sum(case when t.transfer_type = 'transfer' then t.daily_tx_count else 0 end) as regular_tx_count,
        sum(case when t.transfer_type = 'mint'     then t.daily_tx_count else 0 end) as mint_tx_count,
        sum(case when t.transfer_type = 'burn'     then t.daily_tx_count else 0 end) as burn_tx_count,

        sum(case when t.transfer_type = 'transfer' then t.daily_volume_pyusd else 0 end) as regular_volume,
        sum(case when t.transfer_type = 'mint'     then t.daily_volume_pyusd else 0 end) as mint_volume,
        sum(case when t.transfer_type = 'burn'     then t.daily_volume_pyusd else 0 end) as burn_volume,

        -- Unique participants
        sum(t.unique_senders)   as unique_senders,
        sum(t.unique_receivers) as unique_receivers

    from {{ ref('int_daily_transfer_summary') }} t
    group by 1

),

with_supply as (

    select
        ds.*,

        -- Average transfer size
        case when ds.total_tx_count > 0
            then ds.total_volume_pyusd / ds.total_tx_count
            else 0
        end as avg_transfer_size,

        -- Velocity = daily volume / circulating supply
        -- Tells you how many times the entire supply "turned over" that day
        s.running_total_supply,
        case when s.running_total_supply > 0
            then round(ds.total_volume_pyusd / s.running_total_supply, 4)
            else null
        end as velocity

    from daily_summary ds
    left join {{ ref('int_supply_changes') }} s
        on ds.transfer_date = s.transfer_date

)

select * from with_supply
order by transfer_date
