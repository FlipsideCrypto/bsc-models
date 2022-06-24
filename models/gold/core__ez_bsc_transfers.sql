{{ config(
    materialized = 'view'
) }}

WITH bnb_base AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address,
        to_address,
        bnb_value,
        identifier,
        _call_id,
        ingested_at,
        input
    FROM
        {{ ref('silver__traces') }}
    WHERE
        bnb_value > 0
        AND tx_status = 'SUCCESS'
        and gas_used is not null
),
eth_price AS (
    SELECT
        HOUR,
        AVG(price) AS bnb_price
    FROM
        {{ source(
            'ethereum',
            'fact_hourly_token_prices'
        ) }}
    WHERE
        token_address IS NULL
        AND symbol IS NULL
    GROUP BY
        HOUR
)
SELECT
    A.tx_hash AS tx_hash,
    A.block_number AS block_number,
    A.block_timestamp AS block_timestamp,
    A.identifier AS identifier,
    tx.from_address AS origin_from_address,
    tx.to_address AS origin_to_address,
    tx.origin_function_signature AS origin_function_signature,
    A.from_address AS bnb_from_address,
    A.to_address AS bnb_to_address,
    A.bnb_value AS amount,
    ROUND(
        A.bnb_value * eth_price,
        2
    ) AS amount_usd
FROM
    bnb_base A
    LEFT JOIN bnb_price
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = HOUR
    JOIN {{ ref('silver__transactions') }}
    tx
    ON A.tx_hash = tx.tx_hash