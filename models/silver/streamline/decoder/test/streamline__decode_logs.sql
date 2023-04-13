{{ config (
    materialized = "incremental",
    unique_key = "_log_id",
    cluster_by = "round(block_number,-3)"
) }}

WITH base AS (

    SELECT
        l.block_number,
        l.tx_hash,
        l.event_index,
        l.contract_address,
        LOWER('0xb6D7bbdE7c46a8B784F4a19C7FDA0De34b9577DB') AS proxy_address,
        l.topics,
        l.data,
        l._log_id,
        l._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        contract_address = LOWER('0x4DB5a66E937A9F4473fA95b1cAF1d1E1D62E29EA')
        AND block_timestamp :: DATE > '2023-03-01'

{% if is_incremental() %}
AND l._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    b.block_number,
    b._log_id,
    contract_address,
    proxy_address,
    COALESCE(
        proxy_address,
        contract_address
    ) AS abi_address,
    OBJECT_CONSTRUCT(
        'topics',
        b.topics,
        'data',
        b.data,
        'address',
        b.contract_address
    ) AS DATA,
    _inserted_timestamp
FROM
    base b
