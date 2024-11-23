{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH log_pull AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        contract_address,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        (topics [0] :: STRING = '0x70aea8d848e8a90fb7661b227dc522eb6395c3dac71b63cb59edd5c9899b2364'
        AND origin_from_address = LOWER('0x4375c89AF5b4aF46791b05810C4B795A0470207F'))
        OR tx_hash = '0xcf0ea37207cc7f54c90b6dcb8208cdcb247768836ad1d33ccbb6cbe5d13dee80' --edge case where Liqee token is token collateral in liquidations table

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
traces_pull AS (
    SELECT
        from_address AS token_address,
        to_address AS underlying_asset
    FROM
        {{ ref('silver__traces') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                log_pull
        )
        AND TYPE = 'STATICCALL'
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
contracts AS (
    SELECT
        *
    FROM
        {{ ref('silver__contracts') }}
),
contract_pull AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.contract_address,
        C.token_name,
        C.token_symbol,
        C.token_decimals,
        CASE
            WHEN l.contract_address = '0x390bf37355e9df6ea2e16eed5686886da6f47669' THEN '0x2170ed0880ac9a755fd29b2688956bd959f933f8' --WETH
            ELSE t.underlying_asset
        END AS underlying_asset,
        l._inserted_timestamp,
        l._log_id
    FROM
        log_pull l
        LEFT JOIN traces_pull t
        ON l.contract_address = t.token_address
        LEFT JOIN contracts C
        ON C.contract_address = l.contract_address qualify(ROW_NUMBER() over(PARTITION BY l.contract_address
    ORDER BY
        block_timestamp ASC)) = 1
)
SELECT
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.contract_address AS token_address,
    l.token_name,
    l.token_symbol,
    l.token_decimals,
    l.underlying_asset AS underlying_asset_address,
    C.token_name AS underlying_name,
    C.token_symbol AS underlying_symbol,
    C.token_decimals AS underlying_decimals,
    l._inserted_timestamp,
    l._log_id
FROM
    contract_pull l
    LEFT JOIN contracts C
    ON C.contract_address = l.underlying_asset
WHERE
    underlying_asset IS NOT NULL
    AND l.token_name IS NOT NULL
UNION ALL
--manually adding iBNB, no token creation log 
SELECT
    '0x593fd69bcd788afd9a19adae0783f5c429f821d6c46d20c742e5443b8a067d73' AS tx_hash,
    6580133 AS block_number,
    '2021-04-15 06:36:40.000' as block_timestamp,
    lower('0xd57e1425837567f74a35d07669b23bfb67aa4a93') AS token_address,
    'dForce BNB' AS token_name,
    'iBNB' AS token_symbol,
    18 AS token_decimals,
    lower('0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c') AS underlying_asset_address,
    'Wrapped BNB' AS underlying_name,
    'WBNB' AS underlying_symbol,
    18 AS underlying_decimals,
    '2023-05-22 19:26:31.000 +0000' AS _inserted_timestamp,
    NULL AS _log_id
