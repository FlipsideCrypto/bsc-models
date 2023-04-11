{{ config(
    materialized = 'table',
    enabled = false,
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'SUSHI',
                'PURPOSE': 'DEFI, DEX'
            }
        }
    }
) }}

    SELECT
        lower(swap_pair_address) as pool_address,
        lower(token_0_address) as token0_address,
        swap_pair as pool_name,
        token_0_symbol as token0_symbol,
        lower(token_1_address) as token1_address,
        token_1_symbol as token1_symbol,
        token_0_decimals as token0_decimals,
        token_1_decimals as token1_decimals 
    FROM
         {{ source(
            'bsc_pools',
            'SUSHI_DIM_DEX_POOLS'
        ) }} 