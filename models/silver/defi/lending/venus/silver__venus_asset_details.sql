{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH contracts AS (

    SELECT
        *
    FROM
        {{ ref('silver__contracts') }}
),
log_pull AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.contract_address,
        C.token_name,
        C.token_symbol,
        C.token_decimals,
        l._inserted_timestamp,
        l._log_id
    FROM
        {{ ref('silver__logs') }}
        l
        LEFT JOIN contracts C
        ON C.contract_address = l.contract_address
    WHERE
        topics [0] :: STRING = '0x7ac369dbd14fa5ea3f473ed67cc9d598964a77501540ba6751eb0b3decf5870d'
        AND C.token_name LIKE 'Venus%'
        AND origin_from_address IN (
            '0x55a9f5374af30e3045fb491f1da3c2e8a74d168d',
            '0x1ca3ac3686071be692be7f1fbecd668641476d7e',
            '0x12bdf8ae9fe2047809080412d7341044b910ef10',
            '0x03862dfa5d0be8f64509c001cb8c6188194469df',
            '0xa05f990d647287e4e84715b813bc000aea970467',
            '0x8bda9f9e1fef0dfd404fef338d9fe4c543d172e1',
            '0xa6575f1d5bd6545fbd34be05259d9d6ae60641f2',
            '0x92054edbb53ecc4f2a1787e92f479ce10392a658',
            '0xc9e3eb04aae820a1aa77789e699e7c433f75e216'
        )

{% if is_incremental() %}
AND l._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND l.contract_address NOT IN (
    SELECT
        itoken_address
    FROM
        {{ this }}
)
AND l._inserted_timestamp >= SYSDATE() - INTERVAL '7 days'
{% endif %}
),
traces_pull AS (
    SELECT
        t.from_address AS token_address,
        t.to_address AS underlying_asset,
        CASE
            WHEN identifier = 'STATICCALL_0_2' THEN 1
            ELSE NULL
        END AS asset_identifier
    FROM
        {{ ref('core__fact_traces') }}
        t
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                log_pull
        )
        and block_number in (
            select block_number from log_pull
        )
),
underlying_details AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.contract_address,
        l.token_name,
        l.token_symbol,
        l.token_decimals,
        t.underlying_asset,
        l._inserted_timestamp,
        l._log_id
    FROM
        log_pull l
        LEFT JOIN traces_pull t
        ON l.contract_address = t.token_address
    WHERE
        t.asset_identifier = 1 qualify(ROW_NUMBER() over(PARTITION BY l.contract_address
    ORDER BY
        block_timestamp ASC)) = 1
)
SELECT
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.contract_address AS itoken_address,
    l.token_name AS itoken_name,
    l.token_symbol AS itoken_symbol,
    l.token_decimals AS itoken_decimals,
    l.underlying_asset AS underlying_asset_address,
    C.token_name AS underlying_name,
    C.token_symbol AS underlying_symbol,
    C.token_decimals AS underlying_decimals,
    l._inserted_timestamp,
    l._log_id
FROM
    underlying_details l
    LEFT JOIN contracts C
    ON C.contract_address = l.underlying_asset
WHERE
    l.token_name IS NOT NULL
