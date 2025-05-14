{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH swaps_base AS (

    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS user_address,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 40)) AS mm_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS nonce,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS mmTreasury,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40)) AS baseToken1,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS quoteToken1,
        CASE
            WHEN baseToken1 = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
            ELSE baseToken1
        END AS baseToken,
        CASE
            WHEN quoteToken1 = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
            ELSE quoteToken1
        END AS quoteToken,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [4] :: STRING
            )
        ) AS baseTokenAmount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [5] :: STRING
            )
        ) AS quoteTokenAmount,
        baseToken AS token_in,
        quoteToken AS token_out,
        baseTokenAmount AS token_in_amount,
        quoteTokenAmount AS token_out_amount,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
    WHERE
        contract_address = '0xfeacb05b373f1a08e68235ba7fc92636b92ced01' --router
        AND topics [0] :: STRING = '0xe7d6f812e1a54298ddef0b881cd08a4d452d9de35eb18b5145aa580fdda18b26' --swap
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
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    user_address AS sender,
    user_address AS tx_to,
    mm_address,
    nonce,
    mmTreasury,
    baseToken,
    quoteToken,
    baseTokenAmount,
    quoteTokenAmount,
    token_in,
    token_out,
    token_in_amount AS amount_in_unadj,
    token_out_amount AS amount_out_unadj,
    'Swap' AS event_name,
    'pancakeswap-v2' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
