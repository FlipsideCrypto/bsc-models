{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['core','non_realtime','reorg']
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
        bnb_value_precise_raw,
        bnb_value_precise,
        tx_position,
        trace_index
    FROM
        {{ ref('silver__traces') }}
    WHERE
        bnb_value > 0
        AND tx_status = 'SUCCESS'
        AND trace_status = 'SUCCESS'
        AND TYPE NOT IN (
            'DELEGATECALL',
            'STATICCALL'
        )

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
    from_address,
    to_address,
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
    trace_index,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'trace_index']
    ) }} AS native_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    bnb_base A
    LEFT JOIN {{ ref('silver__hourly_prices_all_providers') }}
    ON DATE_TRUNC(
        'hour',
        A.block_timestamp
    ) = HOUR
    AND token_address = LOWER('0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c')
    JOIN tx_table USING (
        tx_hash,
        block_number
    )
