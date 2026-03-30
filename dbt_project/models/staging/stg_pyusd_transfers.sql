-- =============================================================================
-- stg_pyusd_transfers
-- =============================================================================
-- PURPOSE: The "bronze → silver" boundary.
--   1. Select only the columns analytics actually needs
--   2. Assert/enforce correct data types via CAST
--   3. Rename anything non-obvious to self-documenting names
--   4. Add a surrogate key (unique row ID) so downstream models can join safely
--   5. NO aggregation, NO business logic — that belongs in intermediate/
--
-- Source: {{ source('pyusd', 'raw_pyusd_transfers') }}
--   → the DuckDB table loaded by our Python ETL script
-- =============================================================================

with source as (

    -- Pull every row from the raw ETL-loaded table.
    -- Using the source macro instead of a bare table name lets dbt:
    --   a) build the DAG (lineage graph) correctly
    --   b) run source freshness checks against this table
    select * from {{ source('pyusd', 'raw_pyusd_transfers') }}

),

renamed as (

    select

        -- ── Surrogate key ────────────────────────────────────────────────────
        -- A transfer is uniquely identified by its tx_hash + log_index.
        -- We hash them together into one stable, short ID.
        -- md5() is available in DuckDB and deterministic across runs.
        md5(cast(tx_hash as varchar) || '-' || cast(log_index as varchar))
            as transfer_id,

        -- ── Block / Chain position ───────────────────────────────────────────
        cast(block_number as bigint)                    as block_number,
        cast(timestamp    as timestamp_tz)               as block_timestamp,
        cast(transfer_date as date)                     as transfer_date,

        -- ── Transaction identifiers ──────────────────────────────────────────
        cast(tx_hash   as varchar)                      as tx_hash,
        cast(log_index as integer)                      as log_index,

        -- ── Addresses ───────────────────────────────────────────────────────
        -- lower() normalises casing: Ethereum addresses are case-insensitive
        -- but raw data sometimes mixes cases. Consistency prevents bad joins.
        lower(cast(from_address as varchar))            as from_address,
        lower(cast(to_address   as varchar))            as to_address,

        -- ── Token amount ─────────────────────────────────────────────────────
        -- The raw amount is in base units, so we apply 6 decimals here.
        -- Double precision is fine for analytics; use numeric/decimal only if
        -- you need exact financial arithmetic.
        cast(amount as double) / 1000000.0              as amount_pyusd,

        -- ── Transfer type flags ──────────────────────────────────────────────
        cast(is_mint as boolean)                        as is_mint,
        cast(is_burn as boolean)                        as is_burn,

        -- ── Derived: transfer type label ─────────────────────────────────────
        -- A readable enum from the two boolean flags — convenient for GROUP BY.
        case
            when is_mint then 'mint'
            when is_burn then 'burn'
            else              'transfer'
        end                                             as transfer_type

    from source

)

select * from renamed
