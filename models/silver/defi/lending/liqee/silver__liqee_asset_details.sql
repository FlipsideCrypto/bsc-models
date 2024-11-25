{{ config(
    materialized = 'table',
    tags = ['stale']
) }}

WITH log_pull AS (

    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.contract_address,
        CASE
            WHEN l.contract_address = '0x450e09a303aa4bcc518b5f74dd00433bd9555a77' THEN '0xb5102cee1528ce2c760893034a4603663495fd72'
            WHEN l.contract_address = '0x09d0d2c90d09dd817559425479a573faa354c9d2' THEN '0x1dab2a526c8ac1ddea86838a7b968626988d33de'
            WHEN l.contract_address = '0xadcf9619c404de591766b33e696c737ebe341a87' THEN '0x0eb3a705fc54725037cc9e008bdede697f62f335'
            WHEN l.contract_address = '0x89934cf95c8ffa4d748b3a9963fad13dba52c52f' THEN '0x0d8ce2a99bb6e3b7db580ed848240e4a0f9ae153'
            WHEN l.contract_address = '0xf51422c47c6c3e40cfca4a7b04232aedb7f49948' THEN '0x7083609fce4d1d8dc0c979aab8c869ea2c873402'
            WHEN l.contract_address = '0x88131dd9f6a78d3d23abcf4960d91913d2dc2307' THEN '0x2170ed0880ac9a755fd29b2688956bd959f933f8'
            WHEN l.contract_address = '0x4e673bed356912077c718cbab286bc135faa5fb6' THEN '0x1e5f6d5355ae5f1c5c687d3041c55f0aeec57eab'
            ELSE NULL
        END AS underlying_asset
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        topics [0] :: STRING = '0x70aea8d848e8a90fb7661b227dc522eb6395c3dac71b63cb59edd5c9899b2364'
        AND origin_from_address = LOWER('0x2929F07fF145a21b6784fE923b24F3ED38C3a5c3')

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
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.contract_address as itoken_address,
    CASE
        WHEN l.contract_address = '0x4e673bed356912077c718cbab286bc135faa5fb6' THEN 'Liqee StaFi'
        ELSE c1.token_name
    END AS itoken_name,
    CASE
        WHEN l.contract_address = '0x4e673bed356912077c718cbab286bc135faa5fb6' THEN 'qrATOM'
        ELSE c1.token_symbol
    END AS itoken_symbol,
    CASE
        WHEN l.contract_address = '0x4e673bed356912077c718cbab286bc135faa5fb6' THEN 18
        ELSE c1.token_decimals
    END AS itoken_decimals,
    l.underlying_asset AS underlying_asset_address,
    CASE
        WHEN l.contract_address = '0x4e673bed356912077c718cbab286bc135faa5fb6' THEN 'StaFi'
        ELSE c2.token_name
    END AS underlying_name,
    CASE
        WHEN l.contract_address = '0x4e673bed356912077c718cbab286bc135faa5fb6' THEN 'rATOM'
        ELSE c2.token_symbol
    END AS underlying_symbol,
    CASE
        WHEN l.contract_address = '0x4e673bed356912077c718cbab286bc135faa5fb6' THEN 18
        ELSE c2.token_decimals
    END AS underlying_decimals
FROM
    log_pull l
    LEFT JOIN {{ ref('silver__contracts') }}
    c1
    ON c1.contract_address = l.contract_address
    LEFT JOIN {{ ref('silver__contracts') }}
    c2
    ON c2.contract_address = l.underlying_asset
WHERE
    underlying_asset IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY l.contract_address
ORDER BY
    block_timestamp ASC)) = 1
