{{ config(
    materialized = 'view'
) }}

SELECT
    block_number,
    block_hash,
    blockNumber,
    cumulative_gas_used,
    effective_gas_price,
    from_address,
    gas_used,
    logs,
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
    {{ ref('silver__receipts') }}
UNION ALL
SELECT
    block_number,
    block_hash,
    blockNumber,
    cumulative_gas_used,
    effective_gas_price,
    from_address,
    gas_used,
    NULL AS logs,
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
    bsc_dev.silver.overflowed_receipts -- update to a source table for circular dependency
