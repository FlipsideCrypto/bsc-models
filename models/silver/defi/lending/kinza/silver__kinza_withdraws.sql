{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated']
) }}

WITH withdraw AS(

    SELECT
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS kinza_market,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS useraddress,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS depositor,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS withdraw_amount,
        tx_hash,
        COALESCE(
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        modified_timestamp AS _inserted_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7'
        AND contract_address = LOWER('0xcB0620b181140e57D1C0D8b724cde623cA963c8C')
        AND tx_succeeded --excludes failed txs
        AND kinza_market NOT IN (
            '0x2dd73dcc565761b684c56908fa01ac270a03f70f',
            '0xf0daf89f387d9d4ac5e3326eadb20e7bec0ffc7c',
            '0x45b817b36cadba2c3b6c2427db5b22e2e65400dd'
        ) --labeled as protected tokens, markets not relevent

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
atoken_meta AS (
    SELECT
        atoken_address,
        atoken_symbol,
        atoken_name,
        atoken_decimals,
        underlying_address,
        underlying_symbol,
        underlying_name,
        underlying_decimals,
        atoken_version,
        atoken_created_block,
        atoken_stable_debt_address,
        atoken_variable_debt_address
    FROM
        {{ ref('silver__kinza_tokens') }}
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    kinza_market,
    atoken_meta.atoken_address AS kinza_token,
    withdraw_amount AS amount_unadj,
    withdraw_amount / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS amount,
    depositor AS depositor_address,
    'Kinza' AS platform,
    atoken_meta.underlying_symbol AS symbol,
    'bsc' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    withdraw
    LEFT JOIN atoken_meta
    ON withdraw.kinza_market = atoken_meta.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
