{{ config(
    materialized='table',
    alias='countries_silver'
) }}


WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY common_name
            ORDER BY created_at DESC
        ) AS rn
    FROM {{ source('country_pipeline', 'countries_raw') }}
)

SELECT
    common_name,
    official_name,
    capital_city,
    currency,
    created_at
FROM ranked
WHERE rn = 1
