{{ config(
    materialized = 'incremental',
    unique_key = 'aggregator_identifier',
    merge_update_columns = ['aggregator_identifier', 'aggregator', 'aggregator_type'],
    full_refresh = false,
    tags = ['silver','nft','curated']
) }}


WITH calldata_aggregators AS (
    SELECT
        *
    FROM
        (
            VALUES
                ('0', '0', 'calldata', '2020-01-01')
        ) t (aggregator_identifier, aggregator, aggregator_type, _inserted_timestamp)
),

platform_routers as (
SELECT
        *
    FROM
        (
            VALUES
                ('0x46a03313fa8ef8ac8798f502bb38d35e5e1acbfc', 'element', 'router', '2024-03-07'),
                ('0xa7fd99748ce527eadc0bdac60cba8a4ef4090f7c', 'okx', 'router', '2024-04-03')

        ) t (aggregator_identifier, aggregator, aggregator_type, _inserted_timestamp)
),

combined as (
SELECT * 
FROM
    calldata_aggregators

UNION ALL 

SELECT *
FROM
    platform_routers
)

SELECT 
    aggregator_identifier,
    aggregator, 
    aggregator_type,
    _inserted_timestamp
FROM combined

qualify row_number() over (partition by aggregator_identifier order by _inserted_timestamp desc ) = 1 