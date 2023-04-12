{{ config(
    materialized = 'view'
) }}

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
        A.bnb_value * price,
        2
    ) AS amount_usd
FROM
    {{ ref('silver__traces') }} A
    LEFT JOIN {{ source(
        'ethereum',
        'fact_hourly_token_prices'
    ) }}
    ON DATE_TRUNC(
        'hour',
        A.block_timestamp
    ) = HOUR
    AND token_address = LOWER('0x418D75f65a02b3D53B2418FB8E1fe493759c7605')
    JOIN {{ ref('silver__transactions') }}
    tx
    ON A.block_timestamp :: DATE = tx.block_timestamp :: DATE
    AND A.tx_hash = tx.tx_hash
WHERE
    A.bnb_value > 0
    AND A.tx_status = 'SUCCESS'
    AND A.gas_used IS NOT NULL
