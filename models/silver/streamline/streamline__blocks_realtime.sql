{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'traces', 'output_exploded','nested', 'method', 'debug_traceTransaction', 'producer_limit_size', 10000, 'producer_batch_size',5000, 'worker_batch_size',500))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 3
)
SELECT
    block_number,
    'eth_getBlockByNumber' AS method,
    tx_id AS params
FROM
    {{ ref("streamline__blocks") }}
WHERE
    (
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
    )
    AND block_number IS NOT NULL
EXCEPT
SELECT
    block_number,
    'eth_getBlockByNumber' AS method,
    tx_id AS params
FROM
    {{ ref("streamline__complete_traces") }}
WHERE
    block_number >= (
        SELECT
            block_number
        FROM
            last_3_days
    )
