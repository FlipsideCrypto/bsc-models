{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('batch_call','non_batch_call', 'sql_source', '{{this.identifier}}', 'external_table', 'traces', 'exploded_key','[\"result\"]', 'method', 'debug_traceBlockByNumber', 'producer_batch_size',2000, 'producer_limit_size', 2000000, 'worker_batch_size',200))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_3_days AS (

    {% if var('STREAMLINE_RUN_HISTORY')%}
        SELECT 
            0 AS block_number
    {% else %}
        SELECT
            MAX(block_number) - 100000 AS block_number --aprox 3 days
        FROM
            {{ ref("streamline__blocks") }}
    {% endif %}

)
SELECT
    block_number,
    'debug_traceBlockByNumber' AS method,
    CONCAT(
        block_number_hex,
        '_-_',
        '{"tracer": "callTracer"}'
    ) AS params
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
    'debug_traceBlockByNumber' AS method,
    CONCAT(
        REPLACE(
            concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
            ' ',
            ''
        ),
        '_-_',
        '{"tracer": "callTracer"}'
    ) AS params
FROM
    {{ ref("streamline__complete_traces") }}
WHERE
    block_number >= (
        SELECT
            block_number
        FROM
            last_3_days
    )
LIMIT 1000