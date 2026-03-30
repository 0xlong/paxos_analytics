-- =============================================================================
-- int_supply_changes.sql
-- =============================================================================
-- PURPOSE: Track the total circulating supply of PYUSD over time.
-- We do this by calculating the daily net change (mints - burns) and
-- maintaining a running total using a window function.
-- initial_supply = PYUSD circulating supply just before our dataset starts (2025-10-01).
-- Value is in human-readable units (already divided by 10^6), matching amount_pyusd.
-- Derived: live_supply(2,905,663,277) − net_mints_in_dataset(1,031,837,220) = 1,873,826,057
{% set initial_supply = 1873826057 %}
with daily_mints_burns as (

    select
        transfer_date,
        sum(case when is_mint then amount_pyusd else 0 end) as daily_minted_amount,
        sum(case when is_burn then amount_pyusd else 0 end) as daily_burned_amount
    from {{ ref('stg_pyusd_transfers') }}
    where is_mint = true or is_burn = true
    group by 1

),

running_supply as (

    select
        transfer_date,
        daily_minted_amount,
        daily_burned_amount,
        
        -- Net change for the day
        (daily_minted_amount - daily_burned_amount) as daily_net_change,
        
        -- TRUE circulating supply = initial + cumulative net mints
        {{ initial_supply }} + sum(daily_minted_amount - daily_burned_amount) over (
            order by transfer_date asc 
            rows between unbounded preceding and current row
        ) as running_total_supply
        
    from daily_mints_burns

)

select * from running_supply
order by transfer_date desc
