SELECT
    coin_id,
    symbol,
    name,
    CAST(price_usd AS NUMERIC)        AS price_usd,
    CAST(market_cap AS INT64)         AS market_cap,
    CAST(volume_24h AS NUMERIC)       AS volume_24h,
    CAST(price_change_24h AS NUMERIC) AS price_change_24h,
    TIMESTAMP(fetched_at)             AS fetched_at,
    DATE(fetched_at)                  AS price_date
FROM {{ source('crypto_raw', 'raw_prices') }}
WHERE price_usd IS NOT NULL
