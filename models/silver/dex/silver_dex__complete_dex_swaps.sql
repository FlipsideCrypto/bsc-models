{# {{ config(
    materialized = 'incremental',
    unique_key = "_log_id",
    cluster_by = ['block_timestamp::DATE']
) }}

WITH contracts AS (

    SELECT
        address,
        symbol,
        NAME,
        decimals
    FROM
        {{ ref('core__dim_contracts') }}
),

prices AS (

    SELECT
        hour,
        token_address,
        price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address IN (
            SELECT
                DISTINCT address
            FROM
                contracts
        )
{% if is_incremental() %}
AND hour >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 2
    FROM
      {{ this }}
  )  
{% endif %}
), 

trader_joe_v1_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CONCAT(c1.symbol,'-',c2.symbol) AS pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__trader_joe_v1_swaps') }} s
  LEFT JOIN contracts c1
    ON s.token_in = c1.address
  LEFT JOIN contracts c2
    ON s.token_out = c2.address
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

trader_joe_v2_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CONCAT(c1.symbol,'-',c2.symbol) AS pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__trader_joe_v2_swaps') }} s
  LEFT JOIN contracts c1
    ON s.token_in = c1.address
  LEFT JOIN contracts c2
    ON s.token_out = c2.address
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

trader_joe_v2_1_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CONCAT(c1.symbol,'-',c2.symbol) AS pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__trader_joe_v2_1_swaps') }} s
  LEFT JOIN contracts c1
    ON s.token_in = c1.address
  LEFT JOIN contracts c2
    ON s.token_out = c2.address
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

woofi_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CONCAT(c1.symbol,'-',c2.symbol) AS pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__woofi_swaps') }} s
  LEFT JOIN contracts c1
    ON s.token_in = c1.address
  LEFT JOIN contracts c2
    ON s.token_out = c2.address
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

gmx_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CONCAT(c1.symbol,'-',c2.symbol) AS pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__gmx_swaps') }} s
  LEFT JOIN contracts c1
    ON s.token_in = c1.address
  LEFT JOIN contracts c2
    ON s.token_out = c2.address
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

kyberswap_v1_dynamic AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CONCAT(c1.symbol,'-',c2.symbol) AS pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__kyberswap_v1_dynamic_swaps') }} s
  LEFT JOIN contracts c1
    ON s.token_in = c1.address
  LEFT JOIN contracts c2
    ON s.token_out = c2.address
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

kyberswap_v1_static AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CONCAT(c1.symbol,'-',c2.symbol) AS pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__kyberswap_v1_static_swaps') }} s
  LEFT JOIN contracts c1
    ON s.token_in = c1.address
  LEFT JOIN contracts c2
    ON s.token_out = c2.address
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

kyberswap_v2_elastic AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CONCAT(c1.symbol,'-',c2.symbol) AS pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__kyberswap_v2_elastic_swaps') }} s
  LEFT JOIN contracts c1
    ON s.token_in = c1.address
  LEFT JOIN contracts c2
    ON s.token_out = c2.address
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

pangolin_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CONCAT(c1.symbol,'-',c2.symbol) AS pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__pangolin_swaps') }} s
  LEFT JOIN contracts c1
    ON s.token_in = c1.address
  LEFT JOIN contracts c2
    ON s.token_out = c2.address
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

platypus_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CONCAT(c1.symbol,'-',c2.symbol) AS pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__platypus_swaps') }} s
  LEFT JOIN contracts c1
    ON s.token_in = c1.address
  LEFT JOIN contracts c2
    ON s.token_out = c2.address
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

fraxswap_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CONCAT(c1.symbol,'-',c2.symbol) AS pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__fraxswap_swaps') }} s
  LEFT JOIN contracts c1
    ON s.token_in = c1.address
  LEFT JOIN contracts c2
    ON s.token_out = c2.address
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

hashflow_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CONCAT(c1.symbol,'-',c2.symbol) AS pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__hashflow_swaps') }} s
  LEFT JOIN contracts c1
    ON s.token_in = c1.address
  LEFT JOIN contracts c2
    ON s.token_out = c2.address
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

sushi_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CONCAT(c1.symbol,'-',c2.symbol) AS pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__sushi_swaps') }} s
  LEFT JOIN contracts c1
    ON s.token_in = c1.address
  LEFT JOIN contracts c2
    ON s.token_out = c2.address
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

curve_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    CASE 
      WHEN event_name IS NULL THEN 'TokenExchange'
      ELSE event_name
    END AS event_name,
    s.tokens_sold AS amount_in_unadj,
    s.tokens_bought AS amount_out_unadj,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    COALESCE(c1.symbol,s.symbol_in) AS token_symbol_in,
    COALESCE(c2.symbol,s.symbol_out) AS token_symbol_out,
    c1.decimals AS decimals_in,
    CASE
        WHEN decimals_in IS NOT NULL THEN s.tokens_sold / pow(
            10,
            decimals_in
        )
        ELSE s.tokens_sold
    END AS amount_in,
    c2.decimals AS decimals_out,
    CASE
        WHEN decimals_out IS NOT NULL THEN s.tokens_bought / pow(
            10,
            decimals_out
        )
        ELSE s.tokens_bought
    END AS amount_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__curve_swaps') }} s
  LEFT JOIN contracts c1
    ON c1.address = s.token_in
  LEFT JOIN contracts c2
    ON c2.address = s.token_out
  WHERE amount_out <> 0
    AND COALESCE(token_symbol_in,'null') <> COALESCE(token_symbol_out,'null')
{% if is_incremental() %}
AND
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 1
    FROM
      {{ this }}
  )
{% endif %}
),

--union all standard dex CTEs here (excludes amount_usd)
all_dex_standard AS (
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  trader_joe_v1_swaps
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  trader_joe_v2_swaps
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  trader_joe_v2_1_swaps
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  woofi_swaps
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  gmx_swaps
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  kyberswap_v1_dynamic
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  kyberswap_v1_static
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  kyberswap_v2_elastic
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  pangolin_swaps
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  platypus_swaps
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  fraxswap_swaps
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  sushi_swaps
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  token_symbol_in AS symbol_in,
  token_symbol_in AS symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  curve_swaps
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  hashflow_swaps
),

FINAL AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in,
    CASE
        WHEN s.decimals_in IS NOT NULL THEN ROUND(
            amount_in * p1.price, 2)
        ELSE NULL
    END AS amount_in_usd,
    amount_out,
    CASE
        WHEN s.decimals_out IS NOT NULL THEN ROUND(
            amount_out * p2.price, 2)
        ELSE NULL
    END AS amount_out_usd,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    _log_id,
    _inserted_timestamp
  FROM all_dex_standard s
  LEFT JOIN prices p1
    ON s.token_in = p1.token_address
      AND DATE_TRUNC('hour', block_timestamp) = p1.hour
  LEFT JOIN prices p2
    ON s.token_out = p2.token_address
      AND DATE_TRUNC('hour', block_timestamp) = p2.hour
)

SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  CASE
    WHEN ABS((amount_in_usd - amount_out_usd) / NULLIF(amount_out_usd, 0)) > 0.5
      OR ABS((amount_in_usd - amount_out_usd) / NULLIF(amount_in_usd, 0)) > 0.5 THEN NULL
    ELSE amount_in_usd
  END AS amount_in_usd,
  amount_out,
  CASE
    WHEN ABS((amount_out_usd - amount_in_usd) / NULLIF(amount_in_usd, 0)) > 0.5
      OR ABS((amount_out_usd - amount_in_usd) / NULLIF(amount_out_usd, 0)) > 0.5 THEN NULL
    ELSE amount_out_usd
  END AS amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  _log_id,
  _inserted_timestamp
FROM FINAL #}