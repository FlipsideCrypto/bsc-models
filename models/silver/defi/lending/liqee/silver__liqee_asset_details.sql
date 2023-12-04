{{ config(
    materialized = 'table',
    tags = ['curated']
) }}

with log_pull as (

    select
        TX_HASH,
        BLOCK_NUMBER,
        BLOCK_TIMESTAMP,
        CONTRACT_ADDRESS
    from
        {{ ref('silver__logs') }}
    where
        topics[0] :: STRING = '0x70aea8d848e8a90fb7661b227dc522eb6395c3dac71b63cb59edd5c9899b2364'
    AND
        origin_from_address = lower('0x2929F07fF145a21b6784fE923b24F3ED38C3a5c3')
        
),
contracts as (
    select
        *
    from
        {{ ref('silver__contracts') }}
),
contract_pull as (
    select 
        l.TX_HASH,
        l.BLOCK_NUMBER,
        l.BLOCK_TIMESTAMP,
        l.CONTRACT_ADDRESS,
        c.token_name,
        c.token_symbol,
        c.token_decimals,
        CASE
            WHEN L.contract_address = '0x450e09a303aa4bcc518b5f74dd00433bd9555a77' THEN '0xb5102cee1528ce2c760893034a4603663495fd72'
            WHEN L.contract_address = '0x09d0d2c90d09dd817559425479a573faa354c9d2' THEN '0x1dab2a526c8ac1ddea86838a7b968626988d33de'
            WHEN L.contract_address = '0xadcf9619c404de591766b33e696c737ebe341a87' THEN '0x0eb3a705fc54725037cc9e008bdede697f62f335'
            WHEN L.contract_address = '0x89934cf95c8ffa4d748b3a9963fad13dba52c52f' THEN '0x0d8ce2a99bb6e3b7db580ed848240e4a0f9ae153'
            WHEN L.contract_address = '0xf51422c47c6c3e40cfca4a7b04232aedb7f49948' THEN '0x7083609fce4d1d8dc0c979aab8c869ea2c873402'
            WHEN L.contract_address = '0x88131dd9f6a78d3d23abcf4960d91913d2dc2307' THEN '0x2170ed0880ac9a755fd29b2688956bd959f933f8'
            ELSE NULL
        END AS underlying_asset
    from 
        log_pull l
    left join
        contracts c
    ON
        c.contract_address = l.CONTRACT_ADDRESS qualify(ROW_NUMBER() over(PARTITION BY l.contract_address
    ORDER BY
    block_timestamp asc)) = 1
)
SELECT
    l.TX_HASH,
    l.BLOCK_NUMBER,
    l.BLOCK_TIMESTAMP,
    l.CONTRACT_ADDRESS as itoken_address,
    l.token_name as itoken_name,
    l.token_symbol as itoken_symbol,
    l.token_decimals as itoken_decimals,
    l.underlying_asset as underlying_asset_address,
    c.token_name as underlying_name,
    c.token_symbol as underlying_symbol,
    c.token_decimals as underlying_decimals
FROM
    CONTRACT_PULL l 
left join
    contracts c
ON
    c.contract_address = l.underlying_asset 
WHERE 
    underlying_asset is not null