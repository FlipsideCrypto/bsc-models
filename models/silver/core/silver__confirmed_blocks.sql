-- depends_on: {{ ref('bronze__streamline_confirm_blocks') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = "round(block_number,-3)"
) }}

WITH base AS (

    SELECT
        block_number,
        DATA :result :hash :: STRING AS block_hash,
        DATA :result :transactions txs,
        _inserted_timestamp
    FROM

{# Remove comments after fill 
{% if is_incremental() %}
{{ ref('bronze__streamline_confirm_blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            IFNULL(
                MAX(
                    _inserted_timestamp
                ),
                '1970-01-01' :: TIMESTAMP
            ) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_FR_confirm_blocks') }}
{% endif %}
#}

{% if is_incremental() %}
{{ ref('bronze__streamline_confirm_blocks') }}
WHERE
    _partition_by_block_id BETWEEN (
        SELECT
            ROUND(MAX(block_number), -4)
        FROM
            {{ this }})
            AND (
                SELECT
                    ROUND(MAX(block_number), -4) + 500000
                FROM
                    {{ this }})
{% else %}
{{ ref('bronze__streamline_FR_confirm_blocks') }}
where _partition_by_block_id <= 2500000
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
)
SELECT
    block_number,
    block_hash,
    VALUE :: STRING AS tx_hash,
    _inserted_timestamp
FROM
    base,
    LATERAL FLATTEN (
        input => txs
    )
