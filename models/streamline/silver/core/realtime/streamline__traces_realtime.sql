{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('batch_call','non_batch_call', 'sql_source', '{{this.identifier}}', 'external_table', 'traces', 'exploded_key','[\"result\"]', 'method', 'debug_traceBlockByNumber', 'producer_batch_size',2000, 'producer_limit_size', 2000000, 'worker_batch_size',200))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_core_realtime']
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
),
tbl AS (
    SELECT
        block_number,
        block_number_hex,
        'debug_traceBlockByNumber' AS method,
        CONCAT(
            block_number_hex,
            '_-_',
            '{"tracer": "callTracer"}'
        ) AS params
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
        ) AS block_number_hex,
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
),
retry_blocks AS (
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
        (
            SELECT
                block_number
            FROM
                {{ ref("_missing_traces") }}
            UNION
            SELECT
                block_number
            FROM
                {{ ref("_unconfirmed_blocks") }}
        )
)
SELECT
    block_number,
    method,
    params
FROM
    tbl
UNION
SELECT
    block_number,
    method,
    params
FROM
    retry_blocks
ORDER BY
    block_number ASC
LIMIT
    1200
