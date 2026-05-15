with source as (
    select * from {{ source('crypto_api', 'RAW_MARKET_DATA') }}
),

renamed as (
    select
        src_json:id::string as coin_id,
        src_json:symbol::string as ticker,
        src_json:current_price::float as price_aud,
        src_json:market_cap::float as market_cap,
        ingested_at as observation_timestamp
    from source
)

select * from renamed