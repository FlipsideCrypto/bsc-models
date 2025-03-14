{{ config (
    materialized = "view",
    tags = ['overflowed_receipts']
) }}

WITH impacted_blocks AS (

    SELECT
        VALUE :: INT AS block_number
    FROM
        (
            SELECT
                blocks_impacted_array
            FROM
                {{ ref("silver_observability__receipts_completeness") }}
            ORDER BY
                test_timestamp DESC
            LIMIT
                1
        ), LATERAL FLATTEN (
            input => blocks_impacted_array
        )
),
all_txs AS (
    SELECT
        t.block_number,
        t.position,
        t.tx_hash
    FROM
        {{ ref("silver__transactions") }}
        t
        JOIN impacted_blocks USING (block_number)
),
missing_txs AS (
    SELECT
        DISTINCT block_number,
        POSITION,
        file_name
    FROM
        all_txs
        LEFT JOIN {{ ref("silver__receipts") }}
        tr USING (
            block_number,
            tx_hash
        )
        JOIN {{ ref("streamline__receipts_complete") }} USING (block_number)
    WHERE
        tr.tx_hash IS NULL
)
SELECT
    block_number,
    POSITION,
    file_name,
    build_scoped_file_url(
        @streamline.bronze.BSC_SERVERLESS_PROD,
        file_name
    ) AS file_url,
    ['block_number', 'array_index'] AS index_cols,
    ROW_NUMBER() over (
        ORDER BY
            block_number ASC,
            POSITION ASC
    ) AS row_no
FROM
    missing_txs
ORDER BY
    block_number ASC,
    POSITION ASC
