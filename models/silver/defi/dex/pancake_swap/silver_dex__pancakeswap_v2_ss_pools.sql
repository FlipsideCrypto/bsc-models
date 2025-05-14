{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'pool_address',
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH pools AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS pool_address,
        CASE
            WHEN contract_address = '0x36bbb126e75351c0dfb651e39b38fe0bc436ffd2' THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
            WHEN contract_address = '0x25a55f9f2279a54951133d503490342b50e5cd15' THEN CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40))
        END AS tokenA,
        CASE
            WHEN contract_address = '0x36bbb126e75351c0dfb651e39b38fe0bc436ffd2' THEN CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40))
            WHEN contract_address = '0x25a55f9f2279a54951133d503490342b50e5cd15' THEN CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40))
        END AS tokenB,
        CASE
            WHEN contract_address = '0x36bbb126e75351c0dfb651e39b38fe0bc436ffd2' THEN NULL
            WHEN contract_address = '0x25a55f9f2279a54951133d503490342b50e5cd15' THEN CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40))
        END AS tokenC,
        CASE
            WHEN contract_address = '0x36bbb126e75351c0dfb651e39b38fe0bc436ffd2' THEN NULL
            WHEN contract_address = '0x25a55f9f2279a54951133d503490342b50e5cd15' THEN CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40))
        END AS lp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref ('core__fact_event_logs') }}
    WHERE
        contract_address IN (
            '0x25a55f9f2279a54951133d503490342b50e5cd15',
            '0x36bbb126e75351c0dfb651e39b38fe0bc436ffd2'
        ) --factory
        AND topics [0] :: STRING IN (
            '0xa9551fb056fc743efe2a0a34e39f9769ad10166520df7843c09a66f82e148b97',
            '0x48dc7a1b156fe3e70ed5ed0afcb307661905edf536f15bb5786e327ea1933532'
        ) -- swap
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
    tx_hash,
    event_index,
    contract_address,
    pool_address,
    tokenA,
    tokenB,
    tokenC,
    lp,
    _log_id,
    _inserted_timestamp
FROM
    pools qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    _inserted_timestamp DESC)) = 1
