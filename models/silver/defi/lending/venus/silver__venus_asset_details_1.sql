{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH contracts as (
    select
        *
    from
        bsc_dev.silver.contracts
),
log_pull AS (

    SELECT 
        tx_hash,
        block_number,
        block_timestamp,
        origin_from_address,
        l.contract_address,
        token_name,
        token_symbol,
        token_decimals,
        l._inserted_timestamp,
        l._log_id
    FROM
        BSC_DEV.silver.logs l 
    LEFT JOIN 
        contracts c
    ON
        l.contract_address = c.contract_address
    WHERE
        topics [0] :: STRING = '0x7ac369dbd14fa5ea3f473ed67cc9d598964a77501540ba6751eb0b3decf5870d'
    AND 
        TOKEN_NAME LIKE '%Venus%'
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}    

),
traces_pull AS (
    SELECT
        from_address AS token_address,
        to_address AS underlying_asset
    FROM
        BSC_DEV.silver.traces
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                log_pull
        )
        AND TYPE = 'STATICCALL'
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
    l.contract_address AS itoken_address,
    l.token_name as itoken_name,
    l.token_symbol as itoken_symbol,
    l.token_decimals itoken_decimals,
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