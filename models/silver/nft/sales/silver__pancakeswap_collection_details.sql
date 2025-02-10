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
    decoded_log,
    decoded_log :collection :: STRING AS nft_address,
    decoded_log :creator :: STRING AS nft_creator_address,
    decoded_log :creatorFee / pow(
        10,
        4
    ) AS creator_fee,
    decoded_log :tradingFee / pow(
        10,
        4
    ) AS trading_fee,
    CONCAT(
        tx_hash :: STRING,
        '-',
        event_index :: STRING
    ) AS _log_id,
    modified_timestamp AS _inserted_timestamp
FROM
    {{ ref('core__ez_decoded_event_logs') }}
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
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
AND block_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
