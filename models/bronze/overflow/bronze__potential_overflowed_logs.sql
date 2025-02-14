{{ config (
    materialized = "view",
    tags = ['overflowed_receipts']
) }}

WITH missing_txs AS (

    SELECT
        r.block_number,
        r.position,
        r.tx_hash,
        cr.file_name
    FROM
        {{ ref("silver__receipts") }}
        r
        JOIN {{ ref("streamline__receipts_complete") }}
        cr
        ON r.block_number = cr.block_number
        LEFT JOIN {{ ref("silver__logs") }}
        l
        ON r.block_number = l.block_number
        AND r.tx_hash = l.tx_hash
    WHERE
        overflowed
        AND l.tx_hash IS NULL
)
SELECT
    block_number,
    POSITION,
    tx_hash,
    file_name,
    build_scoped_file_url(
        @streamline.bronze.external_tables,
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
