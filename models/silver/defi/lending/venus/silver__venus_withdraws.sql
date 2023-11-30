{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}
-- pull all token addresses and corresponding name
WITH asset_details AS (

    SELECT
        token_address,
        token_symbol,
        token_name,
        token_decimals,
        underlying_asset_address,
        underlying_name,
        underlying_symbol,
        underlying_decimals
    FROM
        {{ ref('silver__venus_asset_details') }}
),
venus_redemptions AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        contract_address AS token,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS received_amount_raw,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS redeemed_token_raw,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS redeemer,
        'Venus' AS platform,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            SELECT
                token_address
            FROM
                asset_details
        )
        AND topics [0] :: STRING = '0xbd5034ffbd47e4e72a94baa2cdb74c6fad73cb3bcdc13036b72ec8306f5a7646'

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
venus_combine AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        token,
        redeemer,
        received_amount_raw,
        redeemed_token_raw,
        C.underlying_asset_address AS received_contract_address,
        C.underlying_symbol AS received_contract_symbol,
        C.token_symbol,
        C.token_decimals,
        C.underlying_decimals,
        b.platform,
        b._log_id,
        b._inserted_timestamp
    FROM
        venus_redemptions b
        LEFT JOIN {{ ref('silver__venus_asset_details') }} C
        ON b.token = C.token_address
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    token,
    token_symbol,
    received_amount_raw AS amount_unadj,
    received_amount_raw / pow(
        10,
        underlying_decimals
    ) AS received_amount,
    received_contract_address,
    received_contract_symbol,
    redeemed_token_raw / pow(
        10,
        token_decimals
    ) AS redeemed_token,
    redeemer,
    platform,
    _inserted_timestamp,
    _log_id
FROM
    venus_combine ee qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1