{{ config (
    materialized = "view"
) }}

WITH base AS (

    SELECT
        file_name
    FROM
        {{ ref("streamline__potential_missing_trace_files") }}
    LIMIT
        100
)
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
    base
