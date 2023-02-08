{{ config(
    materialized = 'table',
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
        lower(kashi_pair_address) as pair_address, 
        kashi_pair as pair_name,
        asset_symbol,
        lower(asset_address) as asset_address,
        collateral_symbol,
        lower(collateral_address) as collateral_address,
        asset_decimals,
        collateral_decimals  
    FROM
         {{ source(
            'bsc_pools',
            'SUSHI_DIM_KASHI_PAIRS'
        ) }} 