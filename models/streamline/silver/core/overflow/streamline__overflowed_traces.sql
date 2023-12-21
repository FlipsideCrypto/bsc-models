{{ config (
    materialized = "view"
) }}

SELECT
    o.file_name,
    f.*
FROM
    {{ ref("streamline__overflowed_files") }}
    o,
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
