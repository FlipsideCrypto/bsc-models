{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('batch_call','non_batch_call', 'sql_source', '{{this.identifier}}', 'external_table', 'traces', 'recursive', 'true', 'exploded_key','[\"result\", \"calls\"]', 'method', 'debug_traceBlockByNumber', 'producer_batch_size',2000, 'producer_limit_size', 2000000, 'worker_batch_size',200))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_core_realtime']
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_block_lookback") }}
),
tbl AS (
    SELECT
        block_number,
        block_number_hex
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        block_number IS NOT NULL
        AND (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
    EXCEPT
    SELECT
        block_number,
        REPLACE(
            concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
            ' ',
            ''
        ) AS block_number_hex
    FROM
        {{ ref("streamline__complete_flat_traces") }}
    WHERE
        block_number IS NOT NULL
        AND _inserted_timestamp >= DATEADD(
            'day',
            -4,
            SYSDATE()
        )
        AND (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
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
    tbl
ORDER BY
    block_number ASC
LIMIT
    25000
