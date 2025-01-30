{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH --borrows from radiant LendingPool contracts
borrow AS (

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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS radiant_market,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS onBehalfOf,
        utils.udf_hex_to_int(
            topics [3] :: STRING
        ) :: INTEGER AS refferal,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS userAddress,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS borrow_quantity,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS borrow_rate_mode,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS borrowrate,
        origin_from_address AS borrower_address,
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
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xc6a898309e823ee50bac64e45ca8adba6690e99e7841c45d754e2a38e9019d9b'
        AND contract_address = LOWER('0xd50Cf00b6e600Dd036Ba8eF475677d816d6c4281')
        AND tx_status = 'SUCCESS' --excludes failed txs

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
    borrow_quantity AS amount_unadj,
    borrow_quantity / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS amount,
    borrower_address,
    CASE
        WHEN borrow_rate_mode = 2 THEN 'Variable Rate'
        ELSE 'Stable Rate'
    END AS borrow_rate_mode,
    lending_pool_contract,
    atoken_meta.underlying_symbol AS symbol,
    atoken_meta.underlying_decimals AS underlying_decimals,
    'Radiant V2' AS platform,
    'bsc' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    borrow
    LEFT JOIN atoken_meta
    ON borrow.radiant_market = atoken_meta.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
