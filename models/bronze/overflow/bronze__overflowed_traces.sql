{{ config (
    materialized = "view"
) }}

WITH potential_overflows AS (

    SELECT
        block_number,
        POSITION,
        file_name,
        file_url,
        index_cols
    FROM
        {{ ref("bronze__potential_overflowed_traces") }}
    ORDER BY
        block_number ASC,
        POSITION ASC
    LIMIT
        1
), overflowed_responses AS (
    SELECT
        block_number,
        POSITION,
        file_name,
        file_url,
        index_cols,
        utils.udf_detect_overflowed_responses(
            file_url,
            index_cols
        ) AS index_vals
    FROM
        potential_overflows
),
overflowed_txs AS (
    SELECT
        block_number,
        POSITION,
        file_name,
        file_url,
        index_cols,
        VALUE [0] AS overflowed_block,
        VALUE [1] AS overflowed_tx,
        block_number = overflowed_block
        AND POSITION = overflowed_tx AS missing
    FROM
        overflowed_responses,
        LATERAL FLATTEN (
            input => index_vals
        )
),
overflowed_files AS (
    SELECT
        file_name,
        file_url,
        index_cols,
        [overflowed_block, overflowed_tx] AS index_vals
    FROM
        overflowed_txs
    WHERE
        missing = TRUE
)
SELECT
    o.file_name,
    f.*
FROM
    overflowed_files o,
    TABLE(
        utils.udtf_flatten_overflowed_responses(
            o.file_url,
            o.index_cols,
            [o.index_vals]
        )
    ) f
WHERE
    NOT IS_OBJECT(
        f.value_
    )
    AND NOT IS_ARRAY(
        f.value_
    )
    AND NOT IS_NULL_VALUE(
        f.value_
    )
