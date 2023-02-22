{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'transactions', 'exploded_key','data_-_result_-_transactions', 'method', 'eth_getBlockByNumber', 'producer_limit_size', 40000000, 'producer_batch_size',10000, 'worker_batch_size',1000))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_3_days AS (
    {% if var('STREAMLINE_RUN_HISTORY')%}
        SELECT 
            0 AS block_number
    {% else %}
        SELECT
            block_number
        FROM
            {{ ref("_max_block_by_date") }}
            qualify ROW_NUMBER() over (
                ORDER BY
                    block_number DESC
            ) = 3
    {% endif %}
),
tbl AS (
    SELECT
        block_number,
        REPLACE(
            concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
            ' ',
            ''
        ) AS block_number_hex
    FROM
        {{ ref("streamline__transactions") }}
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
        REPLACE(
            concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
            ' ',
            ''
        ) AS block_number_hex
    FROM
        {{ ref("streamline__complete_transactions") }}
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
)
SELECT
    block_number,
    'eth_getBlockByNumber' AS method,
    CONCAT(
        block_number_hex,
        '_-_',
        'true'
    ) AS params
FROM
    tbl