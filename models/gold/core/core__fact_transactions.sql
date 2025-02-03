{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    from_address,
    to_address,
    origin_function_signature,
    VALUE,
    value_precise_raw,
    value_precise,
    tx_fee,
    tx_fee_precise,
    CASE 
        WHEN tx_status = 'SUCCESS' THEN TRUE 
        ELSE FALSE 
    END AS tx_succeeded, --new column
    tx_type,
    nonce,
    POSITION AS tx_position, --new column
    input_data,
    gas_price,
    gas_used,
    gas AS gas_limit,
    cumulative_gas_used,
    effective_gas_price,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    l1_gas_price,
    l1_gas_used,
    l1_fee_scalar,
    l1_fee_precise,
    l1_fee,
    y_parity, --new column
    r,
    s,
    v,
    transactions_id AS fact_transactions_id,
    inserted_timestamp,
    modified_timestamp,
    block_hash, --deprecate
    tx_status AS status, --deprecate
    POSITION, --deprecate
    deposit_nonce, --deprecate, may be separate table
    deposit_receipt_version --deprecate, may be separate table
FROM
    {{ ref('silver__transactions') }}