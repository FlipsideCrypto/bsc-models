{{ config(
  materialized = 'incremental',
  unique_key = "_id",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime']
) }}

WITH contracts AS (

  SELECT
    contract_address AS address,
    token_symbol AS symbol,
    token_decimals AS decimals,
    _inserted_timestamp
  FROM
    {{ ref('silver__contracts') }}
),

biswap AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    token0,
    token1,
    'biswap' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__biswap_pools') }}
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

dodo_v1 AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    base_token AS token0,
    quote_token AS token1,
    'dodo-v1' AS platform,
    _id,
    _inserted_timestamp
FROM 
    {{ ref('silver_dex__dodo_v1_pools') }}
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

dodo_v2 AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    base_token AS token0,
    quote_token AS token1,
    'dodo-v2' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM 
    {{ ref('silver_dex__dodo_v2_pools') }}
WHERE token0 IS NOT NULL
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

frax AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    factory_address AS contract_address,
    pool_address,
    NULL AS pool_name,
    token0,
    token1,
    'fraxswap' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__fraxswap_pools') }}
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
    contract_address,
    pool_address,
    NULL AS pool_name,
    token0,
    token1,
    'kyberswap-v1' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__kyberswap_v1_dynamic_pools') }}
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
    contract_address,
    pool_address,
    NULL AS pool_name,
    token0,
    token1,
    'kyberswap-v1' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__kyberswap_v1_static_pools') }}
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
    contract_address,
    pool_address,
    swap_fee_units AS fee,
    tick_distance AS tick_spacing,
    token0,
    token1,
    'kyberswap-v2' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__kyberswap_v2_elastic_pools') }}
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

pancakeswap_v1 AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    token0,
    token1,
    'pancakeswap-v1' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__pancakeswap_v1_pools') }}
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

pancakeswap_v2_amm AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    token0,
    token1,
    'pancakeswap-v2' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__pancakeswap_v2_amm_pools') }}
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

pancakeswap_v2_ss AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    'pancakeswap-v2' AS platform,
    _log_id AS _id,
    _inserted_timestamp,
    tokenA AS token0,
    tokenB AS token1,
    tokenC AS token2
FROM
    {{ ref('silver_dex__pancakeswap_v2_ss_pools') }}
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

pancakeswap_v3 AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    fee,
    tick_spacing,
    token0_address AS token0,
    token1_address AS token1,
    'pancakeswap-v3' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__pancakeswap_v3_pools') }} 
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

sushi AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    token0,
    token1,
    'sushiswap' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__sushi_pools') }}
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

trader_joe_v1 AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    NULL AS pool_name,
    token0,
    token1,
    'trader-joe-v1' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__trader_joe_v1_pools') }}
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

trader_joe_v2 AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    lb_pair AS pool_address,
    NULL AS pool_name,
    tokenX AS token0,
    tokenY AS token1,
    'trader-joe-v2' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__trader_joe_v2_pools') }}
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

uni_v3 AS (

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    pool_address,
    fee,
    tick_spacing,
    token0_address AS token0,
    token1_address AS token1,
    'uniswap-v3' AS platform,
    _log_id AS _id,
    _inserted_timestamp
FROM
    {{ ref('silver_dex__univ3_pools') }}
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

all_pools_standard AS (
    SELECT *
    FROM biswap
    UNION ALL
    SELECT *
    FROM dodo_v1
    UNION ALL
    SELECT *
    FROM dodo_v2
    UNION ALL
    SELECT *
    FROM frax
    UNION ALL
    SELECT *
    FROM kyberswap_v1_dynamic
    UNION ALL
    SELECT *
    FROM kyberswap_v1_static
    UNION ALL
    SELECT *
    FROM pancakeswap_v1
    UNION ALL
    SELECT *
    FROM pancakeswap_v2_amm
    UNION ALL
    SELECT *
    FROM sushi
    UNION ALL
    SELECT *
    FROM trader_joe_v1
    UNION ALL
    SELECT *
    FROM trader_joe_v2
),

all_pools_v3 AS (
    SELECT *
    FROM uni_v3
    UNION ALL
    SELECT *
    FROM pancakeswap_v3
    UNION ALL
    SELECT *
    FROM kyberswap_v2_elastic
),

all_pools_other AS (
    SELECT *
    FROM pancakeswap_v2_ss
),

FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        CASE
          WHEN pool_name IS NULL 
            THEN CONCAT(  
                    COALESCE(c0.symbol,CONCAT(SUBSTRING(token0, 1, 5),'...',SUBSTRING(token0, 39, 42))),
                    '-',
                    COALESCE(c1.symbol,CONCAT(SUBSTRING(token1, 1, 5),'...',SUBSTRING(token1, 39, 42)))
                  ) 
          ELSE pool_name
        END AS pool_name,
        OBJECT_CONSTRUCT('token0',token0,'token1',token1) AS tokens,
        OBJECT_CONSTRUCT('token0',c0.symbol,'token1',c1.symbol) AS symbols,
        OBJECT_CONSTRUCT('token0',c0.decimals,'token1',c1.decimals) AS decimals,
        platform,
        _id,
        p._inserted_timestamp
    FROM all_pools_standard p 
    LEFT JOIN contracts c0
        ON c0.address = p.token0
    LEFT JOIN contracts c1
        ON c1.address = p.token1
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        CASE
            WHEN platform = 'kyberswap-v2' 
                THEN CONCAT(COALESCE(c0.symbol,CONCAT(SUBSTRING(token0, 1, 5),'...',SUBSTRING(token0, 39, 42))),'-',COALESCE(c1.symbol,CONCAT(SUBSTRING(token1, 1, 5),'...',SUBSTRING(token1, 39, 42))),' ',COALESCE(fee,0),' ',COALESCE(tick_spacing,0))
            WHEN platform = 'uniswap-v3' 
                THEN CONCAT(COALESCE(c0.symbol,CONCAT(SUBSTRING(token0, 1, 5),'...',SUBSTRING(token0, 39, 42))),'-',COALESCE(c1.symbol,CONCAT(SUBSTRING(token1, 1, 5),'...',SUBSTRING(token1, 39, 42))),' ',COALESCE(fee,0),' ',COALESCE(tick_spacing,0),' UNI-V3 LP')
            WHEN platform = 'pancakeswap-v3'
                THEN CONCAT(COALESCE(c0.symbol,CONCAT(SUBSTRING(token0, 1, 5),'...',SUBSTRING(token0, 39, 42))),'-',COALESCE(c1.symbol,CONCAT(SUBSTRING(token1, 1, 5),'...',SUBSTRING(token1, 39, 42))),' ',COALESCE(fee,0),' ',COALESCE(tick_spacing,0),' PCS-V3 LP')
        END AS pool_name,
        OBJECT_CONSTRUCT('token0',token0,'token1',token1) AS tokens,
        OBJECT_CONSTRUCT('token0',c0.symbol,'token1',c1.symbol) AS symbols,
        OBJECT_CONSTRUCT('token0',c0.decimals,'token1',c1.decimals) AS decimals,
        platform,
        _id,
        p._inserted_timestamp
    FROM all_pools_v3 p 
    LEFT JOIN contracts c0
        ON c0.address = p.token0
    LEFT JOIN contracts c1
        ON c1.address = p.token1
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        pool_address,
        CASE 
          WHEN pool_name IS NULL 
            THEN CONCAT(
                  COALESCE(c0.symbol, SUBSTRING(token0, 1, 5) || '...' || SUBSTRING(token0, 39, 42)),
                  CASE WHEN token1 IS NOT NULL THEN '-' || COALESCE(c1.symbol, SUBSTRING(token1, 1, 5) || '...' || SUBSTRING(token1, 39, 42)) ELSE '' END,
                  CASE WHEN token2 IS NOT NULL THEN '-' || COALESCE(c2.symbol, SUBSTRING(token2, 1, 5) || '...' || SUBSTRING(token2, 39, 42)) ELSE '' END
              ) 
            ELSE pool_name
        END AS pool_name,
        OBJECT_CONSTRUCT('token0', token0, 'token1', token1, 'token2', token2) AS tokens,
        OBJECT_CONSTRUCT('token0', c0.symbol, 'token1', c1.symbol, 'token2', c2.symbol) AS symbols,
        OBJECT_CONSTRUCT('token0', c0.decimals, 'token1', c1.decimals, 'token2', c2.decimals) AS decimals,
        platform,
        _id,
        p._inserted_timestamp
    FROM all_pools_other p
    LEFT JOIN contracts c0
        ON c0.address = p.token0
    LEFT JOIN contracts c1
        ON c1.address = p.token1
    LEFT JOIN contracts c2
        ON c2.address = p.token2
)

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    platform,
    contract_address,
    pool_address,
    pool_name,
    tokens,
    symbols,
    decimals,
    _id,
    _inserted_timestamp
FROM FINAL 