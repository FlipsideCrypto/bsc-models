{{ config(
    materialized = 'incremental',
    unique_key = ['token_address', 'hour'],
    tags = ['non_realtime']
) }}

SELECT
    p.hour,
    p.token_address,
    p.price,
    p.is_imputed,
    p._inserted_timestamp,
    m.symbol AS symbol,
    NULL AS decimals
FROM
    {{ ref('bronze__hourly_prices_priority') }}
    p
    LEFT JOIN {{ ref('silver__asset_metadata_priority') }}
    m
    ON p.token_address = m.token_address
WHERE
    1 = 1
{% if is_incremental() %}
AND p._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
