
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
where topics [0]::string = '0x3a5151e57d3bc9798e7853034ac52293d1a0e12a2b44725e75b03b21f86477a6'
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
where topics [0]::string = '0xc8e512d8f188ca059984b5853d2bf653da902696b8512785b182b2c813789a6e'
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),

Borrow0 as (
select  block_timestamp,
        block_number,
        tx_hash, 
        'Borrow' as action, 
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        event_index,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS Lending_pool_address,
        origin_from_address as Borrower, 
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) as Borrower2, 
        TRY_TO_NUMBER(
            public.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA)))::integer
        ) as amount,
        case when Borrower = Borrower2 then 'no' 
        else 'yes' end as Borrower_is_a_contract,
        _log_id,
        _inserted_timestamp
from {{ ref('silver__logs') }}
where topics [0]::string = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a' and tx_hash in (select tx_hash from borrow_txns)
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

pay_coll as (
select  tx_hash, 
        concat ('0x', SUBSTR(topics [1] :: STRING, 27, 40))  as collateral, 
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) as Lending_pool_address, 
        TRY_TO_NUMBER(
            public.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA)))::integer
        ) as collateral_amount,
        _inserted_timestamp
from {{ ref('silver__logs') }}
where topics [0]::string = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'  and tx_hash in (select tx_hash from borrow_txns)
and  CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) in (select pair_address from {{ ref('sushi__dim_kashi_pairs') }} )
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}

),

Borrow as 
(
select a.*, b.collateral_amount, b.collateral as collateral_address 
from Borrow0 a
left join pay_coll b
on a.tx_hash = b.tx_hash and a.lending_pool_address = b.lending_pool_address
),


Repay0 as (
select  block_timestamp, 
        block_number,
        tx_hash, 
        'Repay' as action,
        origin_from_address,
        origin_to_address,
        origin_function_signature, 
        event_index,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) as asset, 
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) as Lending_pool_address, 
        origin_from_address as Borrower, 
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) as Borrower2, 
                TRY_TO_NUMBER(
            public.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA)))::integer
        )  as amount,
        case when Borrower = Borrower2 then 'no' 
        else 'yes' end as Lender_is_a_contract,
        _log_id,
        _inserted_timestamp
from {{ ref('silver__logs') }}
where topics [0]::string = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a' and tx_hash in (select tx_hash from Repay_txns)
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

receive_coll as (
select  tx_hash, 
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))  as collateral, 
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) as Lending_pool_address, 
        TRY_TO_NUMBER(
            public.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA)))::integer
        ) as collateral_amount,
        _inserted_timestamp
from {{ ref('silver__logs') }}
where topics [0]::string = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'  and tx_hash in (select tx_hash from Repay_txns)
and  CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) in (select pair_address from {{ ref('sushi__dim_kashi_pairs') }} )
{% if is_incremental() %}
AND _inserted_timestamp::DATE >= (
  SELECT
    MAX(_inserted_timestamp) ::DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),

Repay as 
(
select a.*, b.collateral_amount, b.collateral as collateral_address 
from Repay0 a
left join receive_coll b
on a.tx_hash = b.tx_hash and a.lending_pool_address = b.lending_pool_address
),

Total as (
select * from Borrow
union all
select * from Repay
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
        borrow
)
{% else %}
    AND hour :: DATE >= '2021-09-01'
{% endif %}
    group by 1,2
),


 collateral_prices AS (
    select 
        symbol,
        date_trunc('hour',recorded_at) as hour, 
        avg(price) as collateral_price 
    from 
        {{ source('prices','prices_v2') }} a 
    join {{ ref('sushi__dim_kashi_pairs') }} b
    on a.symbol = b.collateral_symbol
    WHERE
        1 = 1

{% if is_incremental() %}
AND hour :: DATE IN (
    SELECT
        DISTINCT block_timestamp :: DATE
    FROM
        borrow
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
a.Borrower2 as Borrower,
a.Borrower_is_a_contract,
a.lending_pool_address,
a.event_index,
b.asset_decimals,
case when b.asset_decimals is null then a.amount else (a.amount/pow(10,b.asset_decimals)) end as asset_amount,
case when b.collateral_decimals is null then a.collateral_amount else (a.collateral_amount/pow(10,b.collateral_decimals)) end as collateral_amount,
b.pair_name as lending_pool,
b.asset_symbol as symbol,
a._log_id,
substring(b.pair_name,3,charindex('/',b.pair_name)-3) as collateral_symbol,
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
a.Borrower,
a.Borrower_is_a_contract,
a.lending_pool_address,
a.event_index,
a.lending_pool,
a.asset,
a.symbol,
a.asset_amount,
(a.asset_amount* c.price) as asset_amount_USD ,
a.collateral_address,
a.collateral_symbol,
a.collateral_amount,
(a.collateral_amount* d.collateral_price) as collateral_amount_USD,
a._log_id,
_inserted_timestamp
from Labled_WO_prices a
LEFT JOIN prices c
ON a.symbol = c.symbol
AND DATE_TRUNC(
    'hour',
    a.block_timestamp
) = c.hour

LEFT JOIN collateral_prices d
ON a.symbol = d.symbol
AND DATE_TRUNC(
    'hour',
    a.block_timestamp
) = d.hour