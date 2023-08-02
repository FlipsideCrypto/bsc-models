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
    VALUE AS bnb_value,
    bnb_value_precise_raw,
    bnb_value_precise,
    tx_fee,
    tx_fee_precise,
    gas_price,
    gas AS gas_limit,
    gas_used,
    cumulative_gas_used,
    input_data,
    tx_status AS status,
    effective_gas_price,
    r,
    s,
    v,
    tx_type
FROM
    {{ ref('silver__transactions') }}
