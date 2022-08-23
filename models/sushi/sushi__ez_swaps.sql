{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH swap_events AS (

    SELECT
        block_number,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        block_timestamp,
        tx_hash,
        contract_address,
        event_name,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            public.udf_hex_to_int(segmented_data[0]::string)::integer
        ) AS amount0In,
        TRY_TO_NUMBER(
            public.udf_hex_to_int(segmented_data[1]::string)::integer
        ) AS amount1In,
        TRY_TO_NUMBER(
            public.udf_hex_to_int(segmented_data[2]::string)::integer
        ) AS amount0Out,
        TRY_TO_NUMBER(
            public.udf_hex_to_int(segmented_data[3]::string)::integer 
        ) AS amount1Out,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS tx_to,
        event_index,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics[0]::string = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'
        AND tx_status = 'SUCCESS'
        AND contract_address IN (
            SELECT
                DISTINCT pool_address
            FROM
                {{ ref('sushi__dim_dex_pools') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        contract_address,
        event_name,
        CASE
            WHEN amount0In <> 0
            AND amount1In <> 0
            AND token1_decimals IS NOT NULL THEN amount1In / power(
                10,
                token1_decimals
            ) :: FLOAT
            WHEN amount0In <> 0
            AND token0_decimals IS NOT NULL THEN amount0In / power(
                10,
                token0_decimals
            ) :: FLOAT
            WHEN amount1In <> 0
            AND token1_decimals IS NOT NULL THEN amount1In / power(
                10,
                token1_decimals
            ) :: FLOAT
            WHEN amount0In <> 0
            AND token0_decimals IS NULL THEN amount0In
            WHEN amount1In <> 0
            AND token1_decimals IS NULL THEN amount1In
        END AS amount_in,
        CASE
            WHEN amount0Out <> 0
            AND token0_decimals IS NOT NULL THEN amount0Out / power(
                10,
                token0_decimals
            ) :: FLOAT
            WHEN amount1Out <> 0
            AND token1_decimals IS NOT NULL THEN amount1Out / power(
                10,
                token1_decimals
            ) :: FLOAT
            WHEN amount0Out <> 0
            AND token0_decimals IS NULL THEN amount0Out
            WHEN amount1Out <> 0
            AND token1_decimals IS NULL THEN amount1Out
        END AS amount_out,
        sender,
        tx_to,
        event_index,
        _log_id,
        CASE
            WHEN amount0In <> 0
            AND amount1In <> 0 THEN token1_address
            WHEN amount0In <> 0 THEN token0_address
            WHEN amount1In <> 0 THEN token1_address
        END AS token_in,
        CASE
            WHEN amount0Out <> 0 THEN token0_address
            WHEN amount1Out <> 0 THEN token1_address
        END AS token_out,
        CASE
            WHEN amount0In <> 0
            AND amount1In <> 0 THEN token1_symbol
            WHEN amount0In <> 0 THEN token0_symbol
            WHEN amount1In <> 0 THEN token1_symbol
        END AS symbol_in,
        CASE
            WHEN amount0Out <> 0 THEN token0_symbol
            WHEN amount1Out <> 0 THEN token1_symbol
        END AS symbol_out,
        CASE
            WHEN amount0In <> 0
            AND amount1In <> 0 THEN token1_decimals
            WHEN amount0In <> 0 THEN token0_decimals
            WHEN amount1In <> 0 THEN token1_decimals
        END AS decimals_in,
        CASE
            WHEN amount0Out <> 0 THEN token0_decimals
            WHEN amount1Out <> 0 THEN token1_decimals
        END AS decimals_out,
        token0_decimals,
        token1_decimals,
        token0_symbol,
        token1_symbol,
        pool_name,
        _inserted_timestamp
    FROM
        swap_events
        LEFT JOIN {{ ref('sushi__dim_dex_pools') }}
        bb
        ON swap_events.contract_address = bb.pool_address
),

 bsc_prices AS (
    select 
        symbol,
        date_trunc('hour',recorded_at) as hour, 
        avg(price) as price 
    from 
        {{ source('prices','prices_v2') }} a 
    join {{ ref('sushi__dim_dex_pools') }} b
    on a.symbol = b.token0_symbol
    WHERE
        1 = 1

{% if is_incremental() %}
AND hour :: DATE IN (
    SELECT
        DISTINCT block_timestamp :: DATE
    FROM
        swap_events
)
{% else %}
    AND hour :: DATE >= '2021-09-01'
{% endif %}
    group by 1,2
)

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    'sushiswap' AS platform,
    pool_name,
    event_name,
    amount_in,
    amount_in * pIn.price as amount_in_usd,
    amount_out,
    amount_out * pOut.price as amount_out_usd,
    sender,
    tx_to,
    event_index,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    _log_id,
    _inserted_timestamp
FROM
    FINAL wp
    LEFT JOIN bsc_prices pIn
    ON lower(wp.symbol_in) = lower(pIn.symbol)
    AND DATE_TRUNC(
        'hour',
        wp.block_timestamp
    ) = pIn.hour
    LEFT JOIN bsc_prices pOut
    ON lower(wp.symbol_out) = lower(pOut.symbol)
    AND DATE_TRUNC(
        'hour',
        wp.block_timestamp
    ) = pOut.hour
