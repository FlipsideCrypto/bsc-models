{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = "block_timestamp::date, _inserted_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['overflow']
) }}

WITH overflowed_receipts AS (

    SELECT
        block_number,
        block_hash,
        blockNumber,
        cumulative_gas_used,
        effective_gas_price,
        from_address,
        gas_used,
        logs_bloom,
        status,
        tx_success,
        tx_status,
        to_address1,
        to_address,
        tx_hash,
        POSITION,
        TYPE,
        _inserted_timestamp
    FROM
        {{ ref("silver__overflowed_receipts") }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% endif %}
),
relevant_txs AS (
    SELECT
        block_number,
        block_hash,
        blockNumber,
        from_address,
        gas,
        gas_price,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        tx_hash,
        input_data,
        origin_function_signature,
        nonce,
        r,
        s,
        to_address1,
        to_address,
        POSITION,
        TYPE,
        v,
        value_precise_raw,
        value_precise,
        VALUE,
        DATA
    FROM
        {{ ref('silver__transactions') }}
        JOIN overflowed_receipts USING (
            block_number,
            tx_hash
        )
    WHERE
        is_pending
),
new_records AS (
    SELECT
        t.block_number,
        t.block_hash,
        t.from_address,
        t.gas,
        t.gas_price,
        t.tx_hash,
        t.input_data,
        t.origin_function_signature,
        t.max_fee_per_gas,
        t.max_priority_fee_per_gas,
        t.nonce,
        t.r,
        t.s,
        t.to_address,
        t.position,
        t.type,
        t.v,
        t.value_precise_raw,
        t.value_precise,
        t.value,
        block_timestamp,
        CASE
            WHEN block_timestamp IS NULL
            OR tx_status IS NULL THEN TRUE
            ELSE FALSE
        END AS is_pending,
        r.gas_used,
        tx_success,
        tx_status,
        cumulative_gas_used,
        effective_gas_price,
        utils.udf_decimal_adjust (
            utils.udf_hex_to_int(
                t.data :gasPrice :: STRING
            ) :: bigint * r.gas_used,
            18
        ) AS tx_fee_precise,
        COALESCE(
            tx_fee_precise :: FLOAT,
            0
        ) AS tx_fee,
        r.type AS tx_type,
        r._inserted_timestamp,
        t.data
    FROM
        relevant_txs t
        LEFT OUTER JOIN {{ ref('silver__blocks') }}
        b
        ON t.block_number = b.block_number
        LEFT OUTER JOIN overflowed_receipts r
        ON t.block_number = r.block_number
        AND t.tx_hash = r.tx_hash
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    new_records qualify(ROW_NUMBER() over (PARTITION BY block_number, POSITION
ORDER BY
    _inserted_timestamp DESC, is_pending ASC)) = 1
