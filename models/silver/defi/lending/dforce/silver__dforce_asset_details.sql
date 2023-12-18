WITH log_pull AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        contract_address,
        CASE
            WHEN contract_address = '0x55012ad2f0a50195aef44f403536df2465009ef7' THEN '0x0eb3a705fc54725037cc9e008bdede697f62f335'
            WHEN contract_address = '0xec3fd540a2dee6f479be539d64da593a59e12d08' THEN '0x4a9a2b2b04549c3927dd2c9668a5ef3fca473623'
            WHEN contract_address = '0xad5ec11426970c32da48f58c92b1039bc50e5492' THEN '0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3'
            WHEN contract_address = '0x9747e26c5ad01d3594ea49ccf00790f564193c15' THEN '0x8ff795a6f4d97e7887c79bea79aba5cc76444adf'
            WHEN contract_address = '0xefae8f7af4bada590d4e707d900258fc72194d73' THEN '0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82'
            WHEN contract_address = '0x8be8cd81737b282c909f1911f3f0ade630c335aa' THEN '0x16939ef78684453bfdfb47825f8a5f714f12623a'
            WHEN contract_address = '0xaf9c10b341f55465e8785f0f81dbb52a9bfe005d' THEN '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d'
            WHEN contract_address = '0xd739a569ec254d6a20ecf029f024816be58fb810' THEN '0x0d8ce2a99bb6e3b7db580ed848240e4a0f9ae153'
            WHEN contract_address = '0xfc5bb1e8c29b100ef8f12773f972477bcab68862' THEN '0x3ee2200efb3400fabb9aacf31297cbdd1d435d47'
            WHEN contract_address = '0x0b66a250dadf3237ddb38d485082a7bfe400356e' THEN '0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c'
            WHEN contract_address = '0x0bf8c72d618b5d46b055165e21d661400008fa0f' THEN '0x55d398326f99059ff775485246999027b3197955'
            WHEN contract_address = '0x50e894894809f642de1e11b4076451734c963087' THEN '0xf8a0bf9cf54bb92f17374d9e9a321e6a111a51bd'
            WHEN contract_address = '0x983a727aa3491ab251780a13acb5e876d3f2b1d8' THEN '0x367c17d19fcd0f7746764455497d63c8e8b2bba3'
            WHEN contract_address = '0x5511b64ae77452c7130670c79298dec978204a47' THEN '0xe9e7cea3dedca5984780bafc599bd69add087d56'
            WHEN contract_address = '0xd957bea67aadb8a72061ce94d033c631d1c1e6ac' THEN '0x4338665cbb7b2485a8855a139b75d5e34ab0db94'
            WHEN contract_address = '0x9ab060ba568b86848bf19577226184db6192725b' THEN '0x7083609fce4d1d8dc0c979aab8c869ea2c873402'
            WHEN contract_address = '0xee9099c1318cf960651b3196747640eb84b8806b' THEN '0xbf5140a22578168fd562dccf235e5d43a02ce9b1'
            WHEN contract_address IN (
                '0x7b933e1c1f44be9fb111d87501baada7c8518abe',
                '0x911f90e98d5c5c3a3b0c6c37bf6ea46d15ea6466'
            ) THEN '0xb5102cee1528ce2c760893034a4603663495fd72'
            WHEN contract_address = '0x390bf37355e9df6ea2e16eed5686886da6f47669' THEN '0x2170ed0880ac9a755fd29b2688956bd959f933f8'
            WHEN contract_address = '0x6d64effe3af8697336fc57efd5a7517ad526dd6d' THEN '0x1d2f0da169ceb9fc7b3144628db156f3f6c60dbe'
            WHEN contract_address = '0xd57e1425837567f74a35d07669b23bfb67aa4a93' THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
            ELSE NULL
        END AS underlying_asset
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x70aea8d848e8a90fb7661b227dc522eb6395c3dac71b63cb59edd5c9899b2364'
        AND origin_from_address = LOWER('0x4375c89AF5b4aF46791b05810C4B795A0470207F')
)
SELECT
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.contract_address as token_address,
    c1.token_name,
    c1.token_symbol,
    c1.token_decimals,
    l.underlying_asset AS underlying_asset_address,
    c2.token_name AS underlying_name,
    c2.token_symbol AS underlying_symbol,
    c2.token_decimals AS underlying_decimals
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
