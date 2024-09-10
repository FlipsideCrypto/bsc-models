{{ config (
    materialized = 'view'
) }}

WITH meta AS (

    SELECT
        last_modified AS _inserted_timestamp,
        file_name,
        CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER) AS _partition_by_block_id
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "flat_traces") }}'
            )
        ) A
)
SELECT
    block_number,
    MD5(
        CAST(
            COALESCE(CAST(block_number AS text), '' :: STRING) AS text
        )
    ) AS id,
    _inserted_timestamp,
    s._partition_by_block_id,
    s.value AS VALUE,
    s.seq AS seq,
    s.index AS INDEX,
    s.key AS key,
    s.path AS path,
    s.value_ AS value_
FROM
    {{ source(
        "bronze_streamline",
        "flat_traces"
    ) }}
    s
    JOIN meta b
    ON b.file_name = metadata$filename
    AND b._partition_by_block_id = s._partition_by_block_id
WHERE
    b._partition_by_block_id = s._partition_by_block_id
    AND (
        VALUE :DATA :error :code IS NULL
        OR VALUE: DATA :error :code NOT IN (
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
            '-32010',
            '-32608'
        )
    )
