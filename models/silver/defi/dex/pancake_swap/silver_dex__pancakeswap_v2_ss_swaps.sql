{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH pools AS (

    SELECT
        pool_address,
        tokenA,
        tokenB
    FROM
        {{ ref('silver_dex__pancakeswap_v2_ss_pools') }}
),
swaps_base AS (
    SELECT
        block_number,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS buyer_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS sold_id,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS tokens_sold,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS bought_id,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            )
        ) AS tokens_bought,
        tokens_sold AS amountIn,
        tokens_bought AS amountOut,
        CASE
            WHEN bought_id = 0 THEN tokenA
            ELSE tokenB
        END AS tokenOut,
        CASE
            WHEN bought_id = 0 THEN tokenB
            ELSE tokenA
        END AS tokenIn,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        INNER JOIN pools p
        ON p.pool_address = contract_address
    WHERE
        topics [0] :: STRING = '0xb2e76ae99761dc136e598d4a629bb347eccb9532a5f8bbd72e18467c3c34cc98' --TokenExchange
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
    contract_address,
    buyer_address AS sender,
    buyer_address AS tx_to,
    sold_id,
    tokens_sold,
    bought_id,
    tokens_bought,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    tokenIn AS token_in,
    tokenOut AS token_out,
    'TokenExchange' AS event_name,
    'pancakeswap-v2' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
