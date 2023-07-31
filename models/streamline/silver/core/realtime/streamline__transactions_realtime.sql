{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'transactions', 'exploded_key','[\"result\", \"transactions\"]', 'method', 'eth_getBlockByNumber', 'producer_batch_size',5000, 'producer_limit_size', 5000000, 'worker_batch_size',500))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_core_realtime']
) }}

WITH tbl AS (

    SELECT
        block_number,
        block_number_hex
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        block_number IS NOT NULL
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
),
retry_blocks AS (
    SELECT
        block_number,
        REPLACE(
            concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
            ' ',
            ''
        ) AS block_number_hex
    FROM
        (
            SELECT
                block_number
            FROM
                {{ ref("_missing_txs") }}
            UNION
            SELECT
                block_number
            FROM
                {{ ref("_unconfirmed_blocks") }}
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
UNION
SELECT
    block_number,
    'eth_getBlockByNumber' AS method,
    CONCAT(
        block_number_hex,
        '_-_',
        'true'
    ) AS params
FROM
    retry_blocks
