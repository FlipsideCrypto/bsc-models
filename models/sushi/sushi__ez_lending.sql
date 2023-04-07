{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['block_timestamp::DATE'],
  enabled = false,
  meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'SUSHI',
  'PURPOSE': 'DEFI, DEX' } } }
) }}

WITH borrow_txns AS (

  SELECT
    DISTINCT tx_hash,
    contract_address
  FROM
    {{ ref('silver__logs') }}
  WHERE
    topics [0] :: STRING = '0x30a8c4f9ab5af7e1309ca87c32377d1a83366c5990472dbf9d262450eae14e38'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
repay_txns AS (
  SELECT
    DISTINCT tx_hash,
    contract_address
  FROM
    {{ ref('silver__logs') }}
  WHERE
    topics [0] :: STRING = '0x6e853a5fd6b51d773691f542ebac8513c9992a51380d4c342031056a64114228'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
lending AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'Deposit' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    event_index,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS lender,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS lender2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN lender = lender2 THEN 'no'
        ELSE 'yes'
      END AS lender_is_a_contract,
      _log_id,
      _inserted_timestamp
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash IN (
          SELECT
            tx_hash
          FROM
            borrow_txns
        )
        AND CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) IN (
          SELECT
            pair_address
          FROM
            {{ ref('sushi__dim_kashi_pairs') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
withdraw AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'Withdraw' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    event_index,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS lender,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS lender2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN lender = lender2 THEN 'no'
        ELSE 'yes'
      END AS lender_is_a_contract,
      _log_id,
      _inserted_timestamp
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash IN (
          SELECT
            tx_hash
          FROM
            repay_txns
        )
        AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) IN (
          SELECT
            pair_address
          FROM
            {{ ref('sushi__dim_kashi_pairs') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
FINAL AS (
  SELECT
    *
  FROM
    lending
  UNION ALL
  SELECT
    *
  FROM
    withdraw
),
prices AS (
  SELECT
    symbol,
    DATE_TRUNC(
      'hour',
      recorded_at
    ) AS HOUR,
    AVG(price) AS price
  FROM
    {{ source(
      'prices',
      'prices_v2'
    ) }} A
    JOIN {{ ref('sushi__dim_kashi_pairs') }}
    b
    ON A.symbol = b.asset_symbol
  WHERE
    1 = 1

{% if is_incremental() %}
AND HOUR :: DATE IN (
  SELECT
    DISTINCT block_timestamp :: DATE
  FROM
    lending
)
{% else %}
  AND HOUR :: DATE >= '2021-09-01'
{% endif %}
GROUP BY
  1,
  2
),
labels AS (
  SELECT
    *
  FROM
    {{ ref('sushi__dim_kashi_pairs') }}
),
labled_wo_prices AS (
  SELECT
    A.block_timestamp,
    A.block_number,
    A.tx_hash,
    A.action,
    A.origin_from_address,
    A.origin_to_address,
    A.origin_function_signature,
    A.asset,
    A.lender2 AS depositor,
    A.lender_is_a_contract,
    A.lending_pool_address,
    A.event_index,
    b.asset_decimals,
    CASE
      WHEN b.asset_decimals IS NULL THEN A.amount
      ELSE (A.amount / pow(10, b.asset_decimals))
    END AS asset_amount,
    b.pair_name AS lending_pool,
    b.asset_symbol AS symbol,
    A._log_id,
    _inserted_timestamp
  FROM
    FINAL A
    LEFT JOIN labels b
    ON A.lending_pool_address = b.pair_address
)
SELECT
  A.block_timestamp,
  A.block_number,
  A.tx_hash,
  A.action,
  A.origin_from_address,
  A.origin_to_address,
  A.origin_function_signature,
  A.asset,
  A.depositor,
  A.lender_is_a_contract,
  A.lending_pool_address,
  A.event_index,
  A.asset_amount,
  A.lending_pool,
  A.symbol,
  A._log_id,
  CASE
    --when c.price is null then null
    WHEN A.asset_Decimals IS NULL THEN NULL
    ELSE (
      A.asset_amount * C.price
    )
  END AS asset_amount_USD,
  _inserted_timestamp
FROM
  labled_wo_prices A
  LEFT JOIN prices C
  ON A.symbol = C.symbol
  AND DATE_TRUNC(
    'hour',
    A.block_timestamp
  ) = C.hour
