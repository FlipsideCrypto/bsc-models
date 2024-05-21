-- depends_on: {{ ref('silver__complete_token_prices') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated','heal']
) }}

WITH venus AS (

  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    liquidator,
    borrower,
    amount_unadj,
    amount,
    itoken AS protocol_collateral_asset,
    liquidation_contract_address AS debt_asset,
    liquidation_contract_symbol AS debt_asset_symbol,
    collateral_token AS collateral_asset,
    collateral_symbol AS collateral_asset_symbol,
    platform,
    'bsc' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__venus_liquidations') }}
    l

{% if is_incremental() and 'venus' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
dforce as (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    liquidator,
    borrower,
    amount_unadj,
    amount,
    token AS protocol_collateral_asset,
    liquidation_contract_address AS debt_asset,
    liquidation_contract_symbol AS debt_asset_symbol,
    collateral_token AS collateral_asset,
    collateral_symbol AS collateral_asset_symbol,
    platform,
    'bsc' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__dforce_liquidations') }}
    l

{% if is_incremental() and 'dforce' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
kinza as (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    liquidator,
    borrower,
    amount_unadj,
    amount,
    collateral_kinza_token AS protocol_collateral_asset,
    collateral_asset,
    collateral_token_symbol AS collateral_asset_symbol,
    debt_asset,
    debt_token_symbol AS debt_asset_symbol,
    platform,
    'bsc' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__kinza_liquidations') }}

{% if is_incremental() and 'kinza' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
radiant as (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    liquidator,
    borrower,
    amount_unadj,
    amount,
    collateral_radiant_token AS protocol_collateral_asset,
    collateral_asset,
    collateral_token_symbol AS collateral_asset_symbol,
    debt_asset,
    debt_token_symbol AS debt_asset_symbol,
    platform,
    'bsc' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__radiant_liquidations') }}

{% if is_incremental() and 'radaint' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
venus as (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    liquidator,
    borrower,
    amount_unadj,
    amount,
    itoken AS protocol_collateral_asset,
    liquidation_contract_address AS debt_asset,
    liquidation_contract_symbol AS debt_asset_symbol,
    collateral_token AS collateral_asset,
    collateral_symbol AS collateral_asset_symbol,
    platform,
    'bsc' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__venus_liquidations') }}
    l

{% if is_incremental() and 'venus' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
liqee as (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    liquidator,
    borrower,
    amount_unadj,
    amount,
    itoken AS protocol_collateral_asset,
    liquidation_contract_address AS collateral_asset,
    liquidation_contract_symbol AS collateral_asset_symbol,
    collateral_token AS debt_asset,
    collateral_symbol AS debt_asset_symbol,
    platform,
    'bsc' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__liqee_liquidations') }}
    

{% if is_incremental() and 'liqee' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
liquidation_union as (
    SELECT
        *
    FROM
        venus
    UNION ALL
    SELECT
        *
    FROM
        liqee
    UNION ALL
    SELECT
        *
    FROM
        kinza
    UNION ALL
    SELECT
        *
    FROM
        dforce
    UNION ALL
    SELECT
        *
    FROM
        radiant
),
FINAL AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    A.contract_address,
    CASE
      WHEN platform in ('dForce','Liqee','Venus')  THEN 'LiquidateBorrow'
      ELSE 'LiquidationCall'
    END AS event_name,
    liquidator,
    borrower,
    protocol_collateral_asset AS protocol_market,
    collateral_asset AS collateral_token,
    collateral_asset_symbol AS collateral_token_symbol,
    amount_unadj,
    amount,
    ROUND(
      amount * p.price,
      2
    ) AS amount_usd,
    debt_asset AS debt_token,
    debt_asset_symbol AS debt_token_symbol,
    platform,
    A.blockchain,
    A._LOG_ID,
    A._INSERTED_TIMESTAMP
  FROM
    liquidation_union A
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} P
    ON collateral_asset = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON collateral_asset = C.contract_address
)
SELECT
  *,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash','event_index']
  ) }} AS complete_lending_liquidations_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
