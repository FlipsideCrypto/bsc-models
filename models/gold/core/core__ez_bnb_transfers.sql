{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core','non_realtime'],
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH bnb_base AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        identifier,
        from_address,
        to_address,
        bnb_value,
        _call_id,
        _inserted_timestamp,
        to_varchar(
            TO_NUMBER(REPLACE(DATA :value :: STRING, '0x'), REPEAT('X', LENGTH(REPLACE(DATA :value :: STRING, '0x'))))
        ) AS bnb_value_precise_raw,
        IFF(LENGTH(bnb_value_precise_raw) > 18, LEFT(bnb_value_precise_raw, LENGTH(bnb_value_precise_raw) - 18) || '.' || RIGHT(bnb_value_precise_raw, 18), '0.' || LPAD(bnb_value_precise_raw, 18, '0')) AS rough_conversion,
        IFF(
            POSITION(
                '.000000000000000000' IN rough_conversion
            ) > 0,
            LEFT(rough_conversion, LENGTH(rough_conversion) - 19),
            REGEXP_REPLACE(
                rough_conversion,
                '0*$',
                ''
            )
        ) AS bnb_value_precise,
        tx_position,
        trace_index
    FROM
        {{ ref('silver__traces') }}
    WHERE
        bnb_value > 0
        AND tx_status = 'SUCCESS'
        AND trace_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '72 hours'
    FROM
        {{ this }}
)
{% endif %}
),
tx_table AS (
    SELECT
        block_number,
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                bnb_base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '72 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    tx_hash AS tx_hash,
    block_number AS block_number,
    block_timestamp AS block_timestamp,
    identifier AS identifier,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    from_address AS bnb_from_address,
    to_address AS bnb_to_address,
    bnb_value AS amount,
    bnb_value_precise_raw AS amount_precise_raw,
    bnb_value_precise AS amount_precise,
    ROUND(
        bnb_value * price,
        2
    ) AS amount_usd,
    _call_id,
    _inserted_timestamp,
    tx_position,
    trace_index
FROM
    bnb_base A
    LEFT JOIN {{ ref('silver__prices') }}
    ON DATE_TRUNC(
        'hour',
        A.block_timestamp
    ) = HOUR
    AND token_address = LOWER('0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c')
    JOIN tx_table USING (
        tx_hash,
        block_number
    )
