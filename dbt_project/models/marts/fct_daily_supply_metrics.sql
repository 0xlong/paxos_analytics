-- =============================================================================
-- fct_daily_supply_metrics
-- =============================================================================
-- ANSWERS: "Is PYUSD's post-OCC growth real and accelerating?"
--
-- Takes the raw daily mint/burn data from int_supply_changes and layers on
-- analytical window functions: 7-day rolling averages and week-over-week
-- growth percentages. These are the numbers a VP of Product looks at.
--
-- Materialized as TABLE (configured in dbt_project.yml) for dashboard speed.

with supply as (

    select * from {{ ref('int_supply_changes') }}

),

enriched as (

    select
        transfer_date,
        daily_minted_amount,
        daily_burned_amount,
        daily_net_change,
        running_total_supply,

        -- 7-day rolling average of net mints: smooths out daily noise
        avg(daily_net_change) over (
            order by transfer_date
            rows between 6 preceding and current row
        ) as rolling_7d_avg_net_change,

        -- Supply 7 days ago (for week-over-week growth %)
        lag(running_total_supply, 7) over (
            order by transfer_date
        ) as supply_7d_ago

    from supply

)

select
    transfer_date,
    daily_minted_amount,
    daily_burned_amount,
    daily_net_change,
    running_total_supply,
    rolling_7d_avg_net_change,

    -- Week-over-week supply growth as a percentage
    case
        when supply_7d_ago is not null and supply_7d_ago > 0
        then round(((running_total_supply - supply_7d_ago) / supply_7d_ago) * 100, 4)
        else null
    end as supply_growth_wow_pct

from enriched
order by transfer_date
