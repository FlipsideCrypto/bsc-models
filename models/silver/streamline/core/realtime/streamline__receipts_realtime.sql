{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'receipts', 'exploded_key','[\"result\"]', 'method', 'eth_getBlockReceipts', 'producer_batch_size',1000, 'producer_limit_size', 2000000, 'worker_batch_size',100))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
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
        {{ ref("streamline__complete_blocks") }}
    WHERE
        block_number IS NOT NULL
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
        {{ ref("silver__retry_blocks") }}
)
SELECT
    block_number,
    'eth_getBlockReceipts' AS method,
    CONCAT(
        block_number_hex,
        '_-_',
        'false'
    ) AS params
FROM
    tbl
UNION
SELECT
    block_number,
    'eth_getBlockReceipts' AS method,
    CONCAT(
        block_number_hex,
        '_-_',
        'false'
    ) AS params
FROM
    retry_blocks
