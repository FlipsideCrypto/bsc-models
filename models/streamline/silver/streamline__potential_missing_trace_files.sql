{{ config (
    materialized = "table"
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ ref("silver_observability__traces_completeness") }}
    ORDER BY
        test_timestamp DESC
    LIMIT
        1
), missing_blocks AS (
    SELECT
        VALUE :: INT AS block_number,
        ROUND(
            block_number,
            -3
        ) AS _partition_by_block_id
    FROM
        base,
        LATERAL FLATTEN (
            input => blocks_impacted_array
        )
)
SELECT
    DISTINCT file_name
FROM
    missing_blocks m
    JOIN {{ ref("bronze__streamline_FR_traces") }}
    t
    ON m._partition_by_block_id = t._partition_by_block_id
    AND m.block_number = t.block_number
    AND t._partition_by_block_id > 33000000
