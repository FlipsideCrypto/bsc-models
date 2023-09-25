{{ config (
    materialized = 'view'
) }}

WITH meta AS (

    SELECT
        registered_on AS _inserted_timestamp,
        file_name,
        TO_NUMBER(SPLIT_PART(file_name, '/', 3)) AS _partition_by_block_id
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "token_balances") }}'
            )
        ) A
)
SELECT
    s.block_number :: INTEGER AS block_number,
    address :: STRING AS address,
    contract_address :: STRING AS contract_address,
    b._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['s.block_number','contract_address', 'address']
    ) }} AS id,
    s.data
FROM
    {{ source(
        'bronze_streamline',
        'token_balances'
    ) }}
    s
    JOIN meta b
    ON b.file_name = metadata$filename
    AND b._partition_by_block_id = s._partition_by_block_id
WHERE
    b._partition_by_block_id = s._partition_by_block_id
    AND (
        DATA :error :code IS NULL
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
        )
    )
