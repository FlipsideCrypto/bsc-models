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
        ARRAY_SIZE(segmented_data) AS data_count,
        CASE
            WHEN data_count = 6 THEN CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40))
        END AS sender_address,
        CASE
            WHEN data_count = 6 THEN CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40))
            WHEN data_count = 5 THEN CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40))
        END AS tokenIn,
        CASE
            WHEN data_count = 6 THEN CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40))
            WHEN data_count = 5 THEN CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40))
        END AS tokenOut,
        CASE
            WHEN data_count = 6 THEN TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    segmented_data [3] :: STRING
                )
            )
            WHEN data_count = 5 THEN TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    segmented_data [2] :: STRING
                )
            )
        END AS amountIn,
        CASE
            WHEN data_count = 6 THEN TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    segmented_data [4] :: STRING
                )
            )
            WHEN data_count = 5 THEN TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    segmented_data [3] :: STRING
                )
            )
        END AS amountOut,
        CASE
            WHEN data_count = 6 THEN TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    segmented_data [5] :: STRING
                )
            )
            WHEN data_count = 5 THEN TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    segmented_data [4] :: STRING
                )
            )
        END AS fee,
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
        contract_address = '0xa5abfb56a78d2bd4689b25b8a77fd49bb0675874' --router
        AND topics [0] :: STRING = '0xd6d34547c69c5ee3d2667625c188acf1006abb93e0ee7cf03925c67cf7760413' --swap
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
    COALESCE(
        sender_address,
        origin_from_address
    ) AS sender,
    origin_from_address AS tx_to,
    tokenIn AS token_in,
    tokenOut AS token_out,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    fee,
    'Swap' AS event_name,
    'level-finance' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
