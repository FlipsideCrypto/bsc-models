{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    block_hash,
    tx_hash,
    nonce,
    POSITION,
    origin_function_signature,
    from_address,
    to_address,
    VALUE,
    value_precise_raw,
    value_precise,
    tx_fee,
    tx_fee_precise,
    gas_price,
    gas AS gas_limit,
    gas_used,
    cumulative_gas_used,
    input_data,
    tx_status AS status,
    effective_gas_price,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    r,
    s,
    v,
    tx_type,
    COALESCE (
        transactions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash']
        ) }}
    ) AS fact_transactions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__transactions') }}
WHERE
    (
        NOT is_pending
        AND tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                bsc_dev.silver.overflowed_transactions -- update to a source table for circular dependency
        )
    )
UNION ALL
SELECT
    block_number,
    block_timestamp :: timestamp_ntz AS block_timestamp,
    block_hash,
    tx_hash,
    nonce,
    POSITION,
    origin_function_signature,
    from_address,
    to_address,
    VALUE,
    value_precise_raw,
    value_precise,
    tx_fee,
    tx_fee_precise,
    gas_price,
    gas AS gas_limit,
    gas_used,
    cumulative_gas_used,
    input_data,
    tx_status AS status,
    effective_gas_price,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    r,
    s,
    v,
    tx_type,
    transactions_id AS fact_transactions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    bsc_dev.silver.overflowed_transactions
