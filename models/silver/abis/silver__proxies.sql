{{ config (
    materialized = "table"
) }}

WITH base AS (

    SELECT
        from_address,
        to_address,
        MIN(block_number) AS start_block
    FROM
        {{ ref('silver__traces') }}
    WHERE
        TYPE = 'DELEGATECALL'
    GROUP BY
        from_address,
        to_address
)
SELECT
    from_address AS contract_address,
    to_address AS proxy_address,
    start_block,
    COALESCE(
        (LAG(start_block) over(PARTITION BY from_address
        ORDER BY
            start_block DESC)) - 1,
            100000000000
    ) AS end_block,
    CONCAT(
        from_address,
        '-',
        to_address
    ) AS _id
FROM
    base
