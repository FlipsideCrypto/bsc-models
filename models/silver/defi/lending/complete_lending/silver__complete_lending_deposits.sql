-- depends_on: {{ ref('silver__complete_token_prices') }}
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE','platform'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_from_address, origin_to_address, origin_function_signature, contract_address, event_name, token_address, token_symbol, depositor, protocol_market), SUBSTRING(tx_hash, origin_from_address, origin_to_address, origin_function_signature, contract_address, event_name, token_address, token_symbol, depositor, protocol_market)",
  tags = ['reorg','curated','heal']
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
radiant AS (
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

{% if is_incremental() and 'radiant' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
venus AS (
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
liqee AS (
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
dforce AS (
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
deposits AS (
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
complete_lending_deposits AS (
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
      WHEN platform IN (
        'dForce',
        'Liqee',
        'Venus'
      ) THEN 'Mint'
      WHEN platform IN (
        'Radiant',
        'Kinza'
      ) THEN 'Supply'
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
),

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
heal_model AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    t0.contract_address,
    event_name,
    protocol_market,
    depositor,
    t0.token_address,
    t0.token_symbol,
    amount_unadj,
    amount,
    ROUND(
      amount * p.price,
      2
    ) AS amount_usd_heal,
    platform,
    t0.blockchain,
    t0._LOG_ID,
    t0._INSERTED_TIMESTAMP
  FROM
    {{ this }}
    t0
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON t0.token_address = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
  WHERE
    CONCAT(
      t0.block_number,
      '-',
      t0.platform
    ) IN (
      SELECT
        CONCAT(
          t1.block_number,
          '-',
          t1.platform
        )
      FROM
        {{ this }}
        t1
      WHERE
        t1.amount_usd IS NULL
        AND t1._inserted_timestamp < (
          SELECT
            MAX(
              _inserted_timestamp
            ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
          FROM
            {{ this }}
        )
        AND EXISTS (
          SELECT
            1
          FROM
            {{ ref('silver__complete_token_prices') }}
            p
          WHERE
            p._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
            AND p.price IS NOT NULL
            AND p.token_address = t1.token_address
            AND p.hour = DATE_TRUNC(
              'hour',
              t1.block_timestamp
            )
        )
      GROUP BY
        1
    )
),
{% endif %}

FINAL AS (
  SELECT
    *
  FROM
    complete_lending_deposits

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
UNION ALL
SELECT
  tx_hash,
  block_number,
  block_timestamp,
  event_index,
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  event_name,
  protocol_market,
  depositor,
  token_address,
  token_symbol,
  amount_unadj,
  amount,
  amount_usd_heal AS amount_usd,
  platform,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  heal_model
{% endif %}
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
