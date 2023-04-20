{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH pools AS (

    SELECT
        lb_pair,
        tokenX,
        tokenY,
        version
    FROM
        {{ ref('silver_dex__trader_joe_v2_pools') }}
),
swaps_base AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.origin_function_signature,
        l.origin_from_address,
        l.origin_to_address,
        l.event_index,
        l.contract_address,
        l.topics,
        l.data,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS l_segmented_data,
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS sender_address,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 40)) AS recipient_address,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l.topics [3] :: STRING
            )
        ) AS id,
        CASE
            WHEN ethereum.public.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            ) = 0 THEN FALSE
            ELSE TRUE
        END AS swapForY,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [1] :: STRING
            )
        ) AS amountIn,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [2] :: STRING
            )
        ) AS amountOut,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [3] :: STRING
            )
        ) AS volatilityAccumulated,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [4] :: STRING
            )
        ) AS fees,
        CASE
            WHEN swapForY THEN tokenY
            ELSE tokenX
        END AS token_out_address,
        CASE
            WHEN swapForY THEN tokenX
            ELSE tokenY
        END AS token_in_address,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        INNER JOIN pools p
        ON lb_pair = l.contract_address
    WHERE
        topics [0] :: STRING = '0xc528cda9e500228b16ce84fadae290d9a49aecb17483110004c5af0a07f6fd73' --Swap
        AND version = 'v2'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
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
    sender_address AS sender,
    recipient_address AS tx_to,
    id,
    swapForY AS swap_for_y,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    volatilityAccumulated AS volatility_accumulated,
    fees,
    token_in_address AS token_in,
    token_out_address AS token_out,
    'Swap' AS event_name,
    'trader-joe-v2' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
