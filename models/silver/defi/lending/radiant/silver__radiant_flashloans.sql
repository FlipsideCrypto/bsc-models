{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH flashloan AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS target_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS initiator_address,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS radiant_market,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS flashloan_quantity,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS premium_quantity,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS refferalCode,
        COALESCE(
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x631042c832b07452973831137f2d73e395028b44b250dedc5abb0ee766e168ac'

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
AND contract_address = LOWER('0xd50Cf00b6e600Dd036Ba8eF475677d816d6c4281')
AND tx_status = 'SUCCESS' --excludes failed txs
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
        {{ ref('silver__radiant_tokens') }}
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
    radiant_market,
    atoken_meta.atoken_address AS radiant_token,
    flashloan_quantity AS amount_unadj,
    flashloan_quantity / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS flashloan_amount,
    premium_quantity as premium_amount_unadj,
    premium_quantity / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS premium_amount,
    initiator_address AS initiator_address,
    target_address AS target_address,
'Radiant V2' AS platform,
    atoken_meta.underlying_symbol AS symbol,
    'bsc' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    flashloan
    LEFT JOIN atoken_meta
    ON flashloan.radiant_market = atoken_meta.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
