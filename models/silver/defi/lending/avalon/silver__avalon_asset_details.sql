{{ config(
    materialized = 'incremental',
    tags = ['curated']
) }}

WITH DECODE AS (

    SELECT
        block_number AS atoken_created_block,
        contract_address AS a_token_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS underlying_asset,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS pool,
        utils.udf_hex_to_int(
            SUBSTR(
                segmented_data [2] :: STRING,
                27,
                40
            )
        ) :: INTEGER AS atoken_decimals,
        utils.udf_hex_to_string (
            segmented_data [7] :: STRING
        ) :: STRING AS atoken_name,
        utils.udf_hex_to_string (
            segmented_data [9] :: STRING
        ) :: STRING AS atoken_symbol,
        modified_timestamp,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
        l
    WHERE
        block_timestamp >= '2024-05-01'
        AND topic_0 = '0xb19e051f8af41150ccccb3fc2c2d8d15f4a4cf434f32a559ba75fe73d6eea20b'
        AND origin_from_address = '0xd955f0c167adbf7d553fc4d59a964a1b115cc093'
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(
            modified_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND contract_address NOT IN (
    SELECT
        atoken_address
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
a_token_step_1 AS (
    SELECT
        atoken_created_block,
        a_token_address,
        segmented_data,
        underlying_asset,
        pool,
        atoken_decimals,
        atoken_name,
        atoken_symbol,
        modified_timestamp,
        _log_id
    FROM
        DECODE
    WHERE
        atoken_name LIKE '%Avalon%'
),
debt_tokens AS (
    SELECT
        block_number AS atoken_created_block,
        contract_address AS a_token_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS underlying_asset,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS atoken_address,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) :: STRING AS atoken_stable_debt_address,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) :: STRING AS atoken_variable_debt_address,
        modified_timestamp,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp >= '2024-05-01'
        AND topic_0 = '0x3a0ca721fc364424566385a1aa271ed508cc2c0949c2272575fb3013a163a45f'
        AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) IN (
            SELECT
                a_token_address
            FROM
                a_token_step_1
        )
        AND tx_succeeded
),
a_token_step_2 AS (
    SELECT
        atoken_created_block,
        a_token_address,
        segmented_data,
        underlying_asset,
        pool,
        atoken_decimals,
        atoken_name,
        atoken_symbol,
        modified_timestamp,
        _log_id,
        'avalon' AS protocol
    FROM
        a_token_step_1
    WHERE
        pool IN (
            '0x35b3f1bfe7cbe1e95a3dc2ad054eb6f0d4c879b6',
            -- wbtc market
            '0xe1ee45db12ac98d16f1342a03c93673d74527b55',
            -- solv market
            '0xf9278c7c4aefac4ddfd0d496f7a1c39ca6bca6d4' -- pendle market
        )
)
SELECT
    A.atoken_created_block,
    A.atoken_symbol AS atoken_symbol,
    A.a_token_address AS atoken_address,
    b.atoken_stable_debt_address,
    b.atoken_variable_debt_address,
    A.atoken_decimals AS atoken_decimals,
    A.protocol AS atoken_version,
    atoken_name AS atoken_name,
    C.token_symbol AS underlying_symbol,
    A.underlying_asset AS underlying_address,
    C.token_decimals AS underlying_decimals,
    C.token_name AS underlying_name,
    A.modified_timestamp,
    A._log_id
FROM
    a_token_step_2 A
    INNER JOIN debt_tokens b
    ON A.a_token_address = b.atoken_address
    INNER JOIN bsc.silver.contracts C
    ON contract_address = A.underlying_asset qualify(ROW_NUMBER() over(PARTITION BY atoken_address
ORDER BY
    A.atoken_created_block DESC)) = 1
