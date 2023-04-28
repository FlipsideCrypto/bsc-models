{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH pools AS (

    SELECT
        '0x327134de48fcdd75320f4c32498d1980470249ae' AS pool_address,
        'WBNB' AS base_token_symbol,
        'BUSD' AS quote_token_symbol,
        '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' AS base_token,
        '0xe9e7cea3dedca5984780bafc599bd69add087d56' AS quote_token
    UNION ALL
    SELECT
        '0x5bdcf4962fded6b7156e710400f4c4c031f600dc',
        'KOGE',
        'WBNB',
        '0xe6df05ce8c8301223373cf5b969afcb1498c5528',
        '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
    UNION ALL
    SELECT
        '0xbe60d4c4250438344bec816ec2dec99925deb4c7',
        'BUSD',
        'USDT',
        '0xe9e7cea3dedca5984780bafc599bd69add087d56',
        '0x55d398326f99059ff775485246999027b3197955'
    UNION ALL
    SELECT
        '0xc64a1d5c819b3c9113ce3db32b66d5d2b05b4cef',
        'BTCB',
        'BUSD',
        '0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c',
        '0xe9e7cea3dedca5984780bafc599bd69add087d56'
    UNION ALL
    SELECT
        '0x89e5015ff12e4536691abfe5f115b1cb37a35465',
        'ETH',
        'BUSD',
        '0x2170ed0880ac9a755fd29b2688956bd959f933f8',
        '0xe9e7cea3dedca5984780bafc599bd69add087d56'
    UNION ALL
    SELECT
        '0x6064dbd0ff10bfed5a797807042e9f63f18cfe10',
        'USDC',
        'BUSD',
        '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d',
        '0xe9e7cea3dedca5984780bafc599bd69add087d56'
    UNION ALL
    SELECT
        '0xb1327b6402ddb34584ab59fbe8ac7cbf43f6353',
        'DOT',
        'BUSD',
        '0x7083609fce4d1d8dc0c979aab8c869ea2c873402',
        '0xe9e7cea3dedca5984780bafc599bd69add087d56'
    UNION ALL
    SELECT
        '0x8d078451a63d118bacc9cc46698cc416f81c93e2',
        'LINK',
        'BUSD',
        '0xf8a0bf9cf54bb92f17374d9e9a321e6a111a51bd',
        '0xe9e7cea3dedca5984780bafc599bd69add087d56'
    UNION ALL
    SELECT
        '0x82aff931d74f0645ce80e8f419b94c8f93952686',
        'WBNB',
        'USDT',
        '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c',
        '0x55d398326f99059ff775485246999027b3197955'
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
        regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS l_segmented_data,
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS seller_address,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            )
        ) AS payBase,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [1] :: STRING
            )
        ) AS receiveQuote,
        base_token,
        quote_token,
        quote_token AS tokenIn,
        base_token AS tokenOut,
        receiveQuote AS amountIn,
        payBase AS amountOut,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
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

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
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
        regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS l_segmented_data,
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS buyer_address,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            )
        ) AS receiveBase,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [1] :: STRING
            )
        ) AS payQuote,
        base_token,
        quote_token,
        quote_token AS tokenIn,
        base_token AS tokenOut,
        payQuote AS amountIn,
        receiveBase AS amountOut,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
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
