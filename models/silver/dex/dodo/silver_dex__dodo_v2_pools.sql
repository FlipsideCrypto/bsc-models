{{ config(
    materialized = 'incremental',
    unique_key = "pool_address"
) }}

WITH pools AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS baseToken,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS quoteToken,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40)) AS creator,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS pool_address,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref ('silver__logs') }}
    WHERE
        contract_address IN (
            '0xafe0a75dffb395eaabd0a7e1bbbd0b11f8609eef',
            --dpp - factory,
            '0xd9cac3d964327e47399aebd8e1e6dcc4c251daae',
            --dpp advanced - private pool factory
            '0x0fb9815938ad069bf90e14fe6c596c514bede767',
            --dsp - factory
            '0x790b4a80fb1094589a3c0efc8740aa9b0c1733fb' --dvm - factory
        )
        AND topics [0] :: STRING IN (
            '0x8494fe594cd5087021d4b11758a2bbc7be28a430e94f2b268d668e5991ed3b8a',
            --NewDPP
            '0xbc1083a2c1c5ef31e13fb436953d22b47880cf7db279c2c5666b16083afd6b9d',
            --NewDSP
            '0xaf5c5f12a80fc937520df6fcaed66262a4cc775e0f3fceaf7a7cfe476d9a751d' --NewDVM
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
AND pool_address NOT IN (
    SELECT
        DISTINCT pool_address
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    baseToken AS base_token,
    quoteToken AS quote_token,
    creator,
    pool_address,
    _log_id,
    _inserted_timestamp
FROM
    pools qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    _inserted_timestamp DESC)) = 1
