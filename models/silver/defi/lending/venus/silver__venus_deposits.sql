{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "block_number",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['reorg','curated']
) }}
-- pull all itoken addresses and corresponding name
WITH asset_details AS (

  SELECT
    itoken_address,
    itoken_symbol,
    itoken_name,
    itoken_decimals,
    underlying_asset_address,
    underlying_name,
    underlying_symbol,
    underlying_decimals
  FROM
    {{ ref('silver__venus_asset_details') }}
),
venus_deposits AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    contract_address AS itoken_address,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER AS minttokens_raw,
    utils.udf_hex_to_int(
      segmented_data [1] :: STRING
    ) :: INTEGER AS mintAmount_raw,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS supplier,
    'Venus' AS platform,
    modified_timestamp AS _inserted_timestamp,
    CONCAT(
      tx_hash :: STRING,
      '-',
      event_index :: STRING
    ) AS _log_id
  FROM
    {{ ref('core__fact_event_logs') }}
  WHERE
    contract_address IN (
      SELECT
        itoken_address
      FROM
        asset_details
    )
    AND topics [0] :: STRING = '0xb4c03061fb5b7fed76389d5af8f2e0ddb09f8c70d1333abbb62582835e10accb'
    AND tx_succeeded

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
    supplier,
    minttokens_raw,
    mintAmount_raw,
    C.underlying_asset_address AS supplied_contract_addr,
    C.underlying_symbol AS supplied_symbol,
    C.itoken_address,
    C.itoken_symbol,
    C.itoken_decimals,
    C.underlying_decimals,
    b.platform,
    b._log_id,
    b._inserted_timestamp
  FROM
    venus_deposits b
    LEFT JOIN asset_details C
    ON b.itoken_address = C.itoken_address
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
  itoken_address AS itoken,
  itoken_symbol,
  minttokens_raw / pow(
    10,
    itoken_decimals
  ) AS issued_tokens,
  mintAmount_raw AS amount_unadj,
  mintAmount_raw / pow(
    10,
    underlying_decimals
  ) AS amount,
  supplied_contract_addr,
  supplied_symbol,
  supplier,
  platform,
  _inserted_timestamp,
  _log_id
FROM
  venus_combine qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
