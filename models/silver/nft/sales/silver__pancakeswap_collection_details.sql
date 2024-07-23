{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

SELECT
    block_number,
    block_timestamp,
    event_index,
    tx_hash,
    event_name,
    decoded_flat,
    decoded_flat :collection :: STRING AS nft_address,
    decoded_flat :creator :: STRING AS nft_creator_address,
    decoded_flat :creatorFee / pow(
        10,
        4
    ) AS creator_fee,
    decoded_flat :tradingFee / pow(
        10,
        4
    ) AS trading_fee,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__decoded_logs') }}
WHERE
    contract_address = '0x17539cca21c7933df5c980172d22659b8c345c5a'
    AND block_timestamp >= '2021-09-30'
    AND event_name IN (
        'CollectionNew',
        'CollectionUpdate'
    )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
