SELECT
    price_date,
    symbol,
    name,
    ROUND(AVG(price_usd), 2)                    AS avg_price,
    ROUND(MIN(price_usd), 2)                    AS low_price,
    ROUND(MAX(price_usd), 2)                    AS high_price,
    ROUND(MAX(price_usd) - MIN(price_usd), 2)   AS daily_range,
    ROUND(AVG(price_change_24h), 2)             AS avg_change_24h,
    COUNT(*)                                     AS data_points
FROM {{ ref('stg_crypto_prices') }}
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2
