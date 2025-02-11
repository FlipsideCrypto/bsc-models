{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base_evt AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'symbiosis' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_log :"amount" :: STRING
        ) AS amount,
        TRY_TO_NUMBER(
            decoded_log :"chainID" :: STRING
        ) AS chainID,
        decoded_log :"from" :: STRING AS from_address,
        decoded_log :"id" :: STRING AS id,
        decoded_log :"revertableAddress" :: STRING AS revertableAddress,
        decoded_log :"to" :: STRING AS to_address,
        decoded_log :"token" :: STRING AS token,
        decoded_log,
        event_removed,
        IFF(
            tx_succeeded,
            'SUCCESS',
            'FAIL'
        ) AS tx_status,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x31325fe0a1a2e6a5b1e41572156ba5b4e94f0fae7e7f63ec21e9b5ce1e4b3eab'
        AND contract_address IN (
            '0xb91d3060c90aac7c4c706aef2b37997b3b2a1dcf',
            '0x5aa5f7f84ed0e5db0a4a85c3947ea16b53352fd4'
        )
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'

{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    topic_0,
    event_name,
    event_removed,
    tx_status,
    contract_address AS bridge_address,
    NAME AS platform,
    from_address AS sender,
    to_address AS receiver,
    receiver AS destination_chain_receiver,
    amount,
    chainID AS destination_chain_id,
    id,
    revertableAddress AS revertable_address,
    token AS token_address,
    _log_id,
    _inserted_timestamp
FROM
    base_evt
