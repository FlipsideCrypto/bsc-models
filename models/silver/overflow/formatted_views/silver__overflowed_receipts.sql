{{ config (
    materialized = 'view',
    tags = ['overflowed_receipts']
) }}

WITH base AS (

    SELECT
        block_number,
        index_vals [1] :: INT AS tx_position,
        OBJECT_AGG(
            key,
            value_
        ) AS DATA
    FROM
        {{ ref("bronze__overflowed_receipts") }}
    WHERE
        path NOT LIKE '%[%'
    GROUP BY
        ALL
)
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
    ) :: INT AS TYPE
FROM
    base
