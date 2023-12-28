{{ config (
    materialized = "view"
) }}

WITH block_range AS (

    SELECT
        COALESCE(
            b.block_number,
            bl.block_number
        ) AS block_number
    FROM
        (
            SELECT
                MIN(
                    VALUE :: INT
                ) AS block_number
            FROM
                (
                    SELECT
                        blocks_impacted_array
                    FROM
                        {{ ref("silver_observability__traces_completeness") }}
                    ORDER BY
                        test_timestamp DESC
                    LIMIT
                        1
                ), LATERAL FLATTEN (
                    input => blocks_impacted_array
                )
        ) b
        JOIN {{ ref("_block_lookback") }}
        bl
),
missing_blocks AS (
    SELECT
        DISTINCT tx.block_number
    FROM
        {{ ref("silver__transactions") }}
        tx
        LEFT JOIN {{ ref("core__fact_traces") }}
        tr USING (
            block_number,
            tx_hash
        )
        JOIN block_range br
        ON tx.block_number >= br.block_number
    WHERE
        tr.tx_hash IS NULL
),
missing_files AS (
    SELECT
        DISTINCT file_name
    FROM
        missing_blocks
        INNER JOIN {{ ref("streamline__complete_traces") }} USING (block_number)
    WHERE
        file_name IS NOT NULL
    ORDER BY
        file_name ASC
    LIMIT
        2
), overflowed_files AS (
    SELECT
        file_name,
        build_scoped_file_url(
            @streamline.bronze.external_tables,
            file_name
        ) AS file_url,
        ['block_number', 'array_index'] AS index_cols,
        utils.udf_detect_overflowed_responses(
            file_url,
            index_cols
        ) AS index_vals
    FROM
        missing_files
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
            o.index_vals
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
