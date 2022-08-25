
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['block_timestamp::DATE']
) }}

with borrow_txns as (
select distinct tx_hash,contract_address
from {{ ref('silver__logs') }}
where topics[0]::string = '0x30a8c4f9ab5af7e1309ca87c32377d1a83366c5990472dbf9d262450eae14e38'
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),

Repay_txns as (
select distinct tx_hash,contract_address
from {{ ref('silver__logs') }}
where topics[0]::string = '0x6e853a5fd6b51d773691f542ebac8513c9992a51380d4c342031056a64114228'
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),

Lending as (
select  block_timestamp,
        block_number,
        tx_hash, 
        'Deposit' as action, 
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        event_index,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS Lending_pool_address,
        origin_from_address as Lender, 
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) as Lender2, 
        TRY_TO_NUMBER(
            public.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA)))::integer
        ) as amount,
        case when Lender = Lender2 then 'no' 
        else 'yes' end as Lender_is_a_contract,
        _log_id,
        _inserted_timestamp
from {{ ref('silver__logs') }}
where topics [0]::string = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a' and tx_hash in (select tx_hash from borrow_txns)
and CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) in (select pair_address from {{ ref('sushi__dim_kashi_pairs') }} )
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}

),

Withdraw as (
select  block_timestamp, 
        block_number,
        tx_hash, 
        'Withdraw' as action,
        origin_from_address,
        origin_to_address,
        origin_function_signature, 
        event_index,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) as asset, 
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) as Lending_pool_address, 
        origin_from_address as Lender, 
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) as Lender2, 
                TRY_TO_NUMBER(
            public.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA)))::integer
        )  as amount,
        case when Lender = Lender2 then 'no' 
        else 'yes' end as Lender_is_a_contract,
        _log_id,
        _inserted_timestamp
from {{ ref('silver__logs') }}
where topics [0]::string = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a' and tx_hash in (select tx_hash from Repay_txns)
and CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) in (select pair_address from {{ ref('sushi__dim_kashi_pairs') }} ) 
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),

Final as (
select * from Lending
union all
select * from Withdraw
),


 prices AS (
    select 
        symbol,
        date_trunc('hour',recorded_at) as hour, 
        avg(price) as price 
    from 
        {{ source('prices','prices_v2') }} a 
    join {{ ref('sushi__dim_kashi_pairs') }} b
    on a.symbol = b.asset_symbol
    WHERE
        1 = 1

{% if is_incremental() %}
AND hour :: DATE IN (
    SELECT
        DISTINCT block_timestamp :: DATE
    FROM
        Lending
)
{% else %}
    AND hour :: DATE >= '2021-09-01'
{% endif %}
    group by 1,2
),




labels as (
select *
from {{ ref('sushi__dim_kashi_pairs') }}
),

Labled_WO_prices as (
select 
a.block_timestamp,
a.block_number,
a.tx_hash,
a.action,
a.origin_from_address,
a.origin_to_address,
a.origin_function_signature,
a.asset,
a.Lender2 as depositor,
a.Lender_is_a_contract,
a.lending_pool_address,
a.event_index,
b.asset_decimals,
case when b.asset_decimals is null then a.amount else (a.amount/pow(10,b.asset_decimals)) end as asset_amount,
b.pair_name as lending_pool,
b.asset_symbol as symbol,
a._log_id,
_inserted_timestamp
from FINAL a
left join labels b 
on a.Lending_pool_address = b.pair_address
)



select 
a.block_timestamp,
a.block_number,
a.tx_hash,
a.action,
a.origin_from_address,
a.origin_to_address,
a.origin_function_signature,
a.asset,
a.depositor,
a.Lender_is_a_contract,
a.lending_pool_address,
a.event_index,
a.asset_amount,
a.lending_pool,
a.symbol,
a._log_id,
case --when c.price is null then null
    when a.asset_Decimals is null then null 
    else (a.asset_amount* c.price) 
    end as asset_amount_USD ,
_inserted_timestamp
from Labled_WO_prices a
LEFT JOIN prices c
ON a.symbol = c.symbol
AND DATE_TRUNC(
    'hour',
    a.block_timestamp
) = c.hour