{{ config (
    materialized = "view"
) }}

WITH missing_blocks AS (

    SELECT
        VALUE :: INT AS block_number
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
)
SELECT
    DISTINCT file_name
FROM
    missing_blocks
    INNER JOIN {{ ref("streamline__complete_traces") }} USING (block_number)
WHERE
    file_name IS NOT NULL
