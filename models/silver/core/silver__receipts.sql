-- depends_on: {{ ref('bronze__streamline_receipts') }}
{% set warehouse = 'DBT_SNOWPARK' if var('OVERFLOWED_RECEIPTS') else target.warehouse %}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(block_hash, tx_hash, from_address, to_address)",
    tags = ['core','non_realtime','overflowed_receipts'],
    full_refresh = false,
    snowflake_warehouse = warehouse
) }}

WITH base AS (

    SELECT
        block_number,
        DATA,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_receipts') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND IS_OBJECT(DATA)
{% else %}
    {{ ref('bronze__streamline_FR_receipts') }}
WHERE
    IS_OBJECT(DATA)
{% endif %}
),
FINAL AS (
    SELECT
        block_number,
        DATA :blockHash :: STRING AS block_hash,
        utils.udf_hex_to_int(
            DATA :blockNumber :: STRING
        ) :: INT AS blockNumber,
        utils.udf_hex_to_int(
            DATA :cumulativeGasUsed :: STRING
        ) :: INT AS cumulative_gas_used,
        utils.udf_hex_to_int(
            DATA :effectiveGasPrice :: STRING
        ) :: INT / pow(
            10,
            9
        ) AS effective_gas_price,
        DATA :from :: STRING AS from_address,
        utils.udf_hex_to_int(
            DATA :gasUsed :: STRING
        ) :: INT AS gas_used,
        DATA :logs AS logs,
        DATA :logsBloom :: STRING AS logs_bloom,
        utils.udf_hex_to_int(
            DATA :status :: STRING
        ) :: INT AS status,
        CASE
            WHEN status = 1 THEN TRUE
            ELSE FALSE
        END AS tx_success,
        CASE
            WHEN status = 1 THEN 'SUCCESS'
            ELSE 'FAIL'
        END AS tx_status,
        DATA :to :: STRING AS to_address1,
        CASE
            WHEN to_address1 = '' THEN NULL
            ELSE to_address1
        END AS to_address,
        DATA :transactionHash :: STRING AS tx_hash,
        utils.udf_hex_to_int(
            DATA :transactionIndex :: STRING
        ) :: INT AS POSITION,
        utils.udf_hex_to_int(
            DATA :type :: STRING
        ) :: INT AS TYPE,
        _inserted_timestamp,
        FALSE AS overflowed
    FROM
        base
)

{% if is_incremental() and var(
    'OVERFLOWED_RECEIPTS',
) %},
overflowed_receipts AS (
    SELECT
        block_number,
        block_hash,
        blockNumber,
        cumulative_gas_used,
        effective_gas_price,
        from_address,
        gas_used,
        [] :: variant AS logs,
        logs_bloom,
        status,
        tx_success,
        tx_status,
        to_address1,
        to_address,
        tx_hash,
        POSITION,
        TYPE
    FROM
        {{ source(
            'bsc_silver',
            'overflowed_receipts'
        ) }}
        -- source works around circular dependency
),
existing_blocks AS (
    SELECT
        block_number,
        block_hash,
        blockNumber,
        cumulative_gas_used,
        effective_gas_price,
        from_address,
        gas_used,
        logs,
        logs_bloom,
        status,
        tx_success,
        tx_status,
        to_address1,
        to_address,
        tx_hash,
        POSITION,
        TYPE,
        _inserted_timestamp
    FROM
        {{ this }}
        INNER JOIN (
            SELECT
                DISTINCT block_number
            FROM
                overflowed_receipts
        ) USING(block_number)
),
final_overflowed AS (
    SELECT
        block_number,
        block_hash,
        blockNumber,
        cumulative_gas_used,
        effective_gas_price,
        from_address,
        gas_used,
        logs,
        logs_bloom,
        status,
        tx_success,
        tx_status,
        to_address1,
        to_address,
        tx_hash,
        POSITION,
        TYPE,
        _inserted_timestamp,
        FALSE AS overflowed
    FROM
        FINAL
    UNION ALL
    SELECT
        block_number,
        block_hash,
        blockNumber,
        cumulative_gas_used,
        effective_gas_price,
        from_address,
        gas_used,
        logs,
        logs_bloom,
        status,
        tx_success,
        tx_status,
        to_address1,
        to_address,
        tx_hash,
        POSITION,
        TYPE,
        _inserted_timestamp,
        FALSE AS overflowed
    FROM
        existing_blocks
    UNION ALL
    SELECT
        block_number,
        block_hash,
        blockNumber,
        cumulative_gas_used,
        effective_gas_price,
        from_address,
        gas_used,
        logs,
        logs_bloom,
        status,
        tx_success,
        tx_status,
        to_address1,
        to_address,
        tx_hash,
        POSITION,
        TYPE,
        _inserted_timestamp,
        TRUE AS overflowed
    FROM
        overflowed_receipts
        INNER JOIN (
            SELECT
                block_number,
                MAX(_inserted_timestamp) AS _inserted_timestamp
            FROM
                existing_blocks
            GROUP BY
                block_number
        ) USING(
            block_number
        )
)
{% endif %}
SELECT
    block_number,
    block_hash,
    blockNumber,
    cumulative_gas_used,
    effective_gas_price,
    from_address,
    gas_used,
    logs,
    logs_bloom,
    status,
    tx_success,
    tx_status,
    to_address1,
    to_address,
    tx_hash,
    POSITION,
    TYPE,
    _inserted_timestamp,
    overflowed
FROM

{% if is_incremental() and var(
    'OVERFLOWED_RECEIPTS',
) %}
final_overflowed
{% else %}
    FINAL
{% endif %}
WHERE
    tx_hash IS NOT NULL qualify(ROW_NUMBER() over (PARTITION BY block_number, POSITION
ORDER BY
    _inserted_timestamp DESC)) = 1
