{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['reorg','curated']
) }}

WITH kinza AS (

  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    depositor_address,
    kinza_token AS protocol_market,
    kinza_market AS token_address,
    symbol AS token_symbol,
    amount_unadj,
    amount,
    platform,
    'bsc' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__kinza_deposits') }}

{% if is_incremental() and 'kinza' not in var('HEAL_CURATED_MODEL') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    depositor_address,
    radiant_token AS protocol_market,
    radiant_market AS token_address,
    symbol AS token_symbol,
    amount_unadj,
    amount,
    platform,
    'bsc' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__radiant_deposits') }}

{% if is_incremental() and 'radiant' not in var('HEAL_CURATED_MODEL') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    supplier AS depositor_address,
    itoken AS protocol_market,
    supplied_contract_addr AS token_address,
    supplied_symbol AS token_symbol,
    amount_unadj,
    amount,
    platform,
    'bsc' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__venus_deposits') }}

{% if is_incremental() and 'venus' not in var('HEAL_CURATED_MODEL') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    supplier AS depositor_address,
    itoken AS protocol_market,
    supplied_contract_addr AS token_address,
    supplied_symbol AS token_symbol,
    amount_unadj,
    amount,
    platform,
    'bsc' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__liqee_deposits') }}

{% if is_incremental() and 'liqee' not in var('HEAL_CURATED_MODEL') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    supplier AS depositor_address,
    token_address AS protocol_market,
    supplied_contract_addr AS token_address,
    supplied_symbol AS token_symbol,
    amount_unadj,
    amount,
    platform,
    'bsc' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__dforce_deposits') }}

{% if is_incremental() and 'dforce' not in var('HEAL_CURATED_MODEL') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
),
deposits as (
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
      WHEN platform in ('dForce','Liqee','Venus') THEN 'Mint'
      WHEN platform in ('Radiant','Kinza') THEN 'Supply'
      ELSE 'Deposit'
    END AS event_name,
    protocol_market,
    depositor_address AS depositor,
    A.token_address,
    A.token_symbol,
    amount_unadj,
    amount,
    ROUND(
      amount * price,
      2
    ) AS amount_usd,
    platform,
    A.blockchain,
    A._LOG_ID,
    A._INSERTED_TIMESTAMP
  FROM
    deposits A
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON A.token_address = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON A.token_address = C.contract_address
)
SELECT
      *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS complete_lending_deposits_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
