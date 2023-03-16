{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)"
) }}

WITH meta AS (
    SELECT
        registered_on,
        last_modified,
        CAST(
            SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER
        ) AS _partition_by_block_number
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "receipts") }}'
            )
        ) A
{% if is_incremental() %}
WHERE
    LEAST(
        registered_on,
        last_modified
    ) >= (
        SELECT
            MAX(max_INSERTED_TIMESTAMP)
        FROM
            max_date
    )
)
{% else %}
)
{% endif %}
SELECT
    MD5(
        CAST(COALESCE(CAST(block_number AS text), '') AS text)
    ) AS id,
    block_number,
    registered_on AS _inserted_timestamp
FROM
    {{ source(
        "bronze_streamline",
        "receipts"
    ) }}
    t
    JOIN meta b
    ON b._partition_by_block_number = t._partition_by_block_id
WHERE
    b._partition_by_block_number = t._partition_by_block_id
    AND DATA :error :code IS NULL
    OR DATA :error :code NOT IN (
        '-32000',
        '-32001',
        '-32002',
        '-32003',
        '-32004',
        '-32005',
        '-32006',
        '-32007',
        '-32008',
        '-32009',
        '-32010'
    ) qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
