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
        base_token,
        quote_token
    FROM
        {{ ref('silver_dex__dodo_v1_pools') }}
),
proxies AS (
    SELECT
        '0x8e4842d0570c85ba3805a9508dce7c6a458359d0' AS proxy_address
    UNION ALL
    SELECT
        '0x0596908263ef2724fbfbcafa1c983fcd7a629038'
    UNION ALL
    SELECT
        '0x165ba87e882208100672b6c56f477ee42502c820'
    UNION ALL
    SELECT
        '0xab623fbcaeb522046185051911209f5b2c2a2e1f'
),
sell_base_token AS (
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
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS seller_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS payBase,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS receiveQuote,
        base_token,
        quote_token,
        quote_token AS tokenIn,
        base_token AS tokenOut,
        receiveQuote AS amountIn,
        payBase AS amountOut,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN pools p
        ON p.pool_address = l.contract_address
    WHERE
        topics [0] :: STRING = '0xd8648b6ac54162763c86fd54bf2005af8ecd2f9cb273a5775921fd7f91e17b2d' --sellBaseToken
        AND seller_address NOT IN (
            SELECT
                proxy_address
            FROM
                proxies
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
),
buy_base_token AS (
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
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS buyer_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS receiveBase,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS payQuote,
        base_token,
        quote_token,
        quote_token AS tokenIn,
        base_token AS tokenOut,
        payQuote AS amountIn,
        receiveBase AS amountOut,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN pools p
        ON p.pool_address = l.contract_address
    WHERE
        topics [0] :: STRING = '0xe93ad76094f247c0dafc1c61adc2187de1ac2738f7a3b49cb20b2263420251a3' --buyBaseToken
        AND buyer_address NOT IN (
            SELECT
                proxy_address
            FROM
                proxies
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
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    seller_address AS sender,
    origin_from_address AS tx_to,
    tokenIn AS token_in,
    tokenOut AS token_out,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    'SellBaseToken' AS event_name,
    'dodo-v1' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    sell_base_token
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    buyer_address AS sender,
    origin_from_address AS tx_to,
    tokenIn AS token_in,
    tokenOut AS token_out,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    'BuyBaseToken' AS event_name,
    'dodo-v1' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    buy_base_token
