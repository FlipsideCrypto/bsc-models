{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'receipts', 'exploded_key','[\"result\"]', 'method', 'eth_getBlockReceipts', 'producer_batch_size',2400, 'producer_limit_size',4800, 'worker_batch_size',1200))",
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
        {{ ref("streamline__complete_receipts") }}
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
                {{ ref("_missing_receipts") }}
            UNION
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
    'eth_getBlockReceipts' AS method,
    block_number_hex AS params
FROM
    tbl
UNION
SELECT
    block_number,
    'eth_getBlockReceipts' AS method,
    block_number_hex AS params
FROM
    retry_blocks
ORDER BY
    block_number ASC
LIMIT
    4800