{{ config(
    materialized = 'view',
    tags = ['overflowed_receipts']
) }}

WITH base AS (

    SELECT
        block_number,
        index_vals [1] :: INT AS tx_position,
        REPLACE(
            REPLACE(SPLIT(IFF(path LIKE '%[%', path, 'logs[-1]'), '.') [0] :: STRING, 'logs['),
            ']'
        ) :: INT AS id_,
        SYSDATE() AS _inserted_timestamp,
        OBJECT_AGG(IFNULL(key, SPLIT(path, '.') [1]), value_) AS json_data
    FROM
        {{ ref("bronze__overflowed_logs") }}
    GROUP BY
        ALL
),
receipt_info AS (
    SELECT
        block_number,
        tx_position,
        json_data :blockHash :: STRING AS block_hash,
        utils.udf_hex_to_int(
            json_data :blockNumber :: STRING
        ) :: INT AS blockNumber,
        utils.udf_hex_to_int(
            json_data :cumulativeGasUsed :: STRING
        ) :: INT AS cumulative_gas_used,
        utils.udf_hex_to_int(
            json_data :effectiveGasPrice :: STRING
        ) :: INT / pow(
            10,
            9
        ) AS effective_gas_price,
        json_data :from :: STRING AS from_address,
        utils.udf_hex_to_int(
            json_data :gasUsed :: STRING
        ) :: INT AS gas_used,
        json_data :logsBloom :: STRING AS logs_bloom,
        utils.udf_hex_to_int(
            json_data :status :: STRING
        ) :: INT AS status,
        CASE
            WHEN status = 1 THEN TRUE
            ELSE FALSE
        END AS tx_success,
        CASE
            WHEN status = 1 THEN 'SUCCESS'
            ELSE 'FAIL'
        END AS tx_status,
        json_data :to :: STRING AS to_address1,
        CASE
            WHEN to_address1 = '' THEN NULL
            ELSE to_address1
        END AS to_address,
        json_data :transactionHash :: STRING AS tx_hash,
        utils.udf_hex_to_int(
            json_data :type :: STRING
        ) :: INT AS TYPE
    FROM
        base
    WHERE
        id_ = -1
),
flat_logs AS (
    SELECT
        block_number,
        tx_position,
        id_,
        json_data :address :: STRING AS contract_address,
        json_data :blockHash :: STRING AS block_hash,
        json_data :data :: STRING AS DATA,
        utils.udf_hex_to_int(
            json_data :logIndex :: STRING
        ) :: INT AS event_index,
        json_data :removed :: BOOLEAN AS event_removed,
        json_data :transactionHash :: STRING AS tx_hash,
        json_data :transactionIndex :: STRING AS tx_position,
        ARRAY_CONSTRUCT(
            json_data :"topics[0]",
            json_data :"topics[1]",
            json_data :"topics[2]",
            json_data :"topics[3]"
        ) AS topics,
        _inserted_timestamp,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        tx_status
    FROM
        base
        JOIN receipt_info USING (
            block_number,
            tx_position
        )
    WHERE
        id_ >= 0
)
SELECT
    block_number,
    tx_hash,
    origin_from_address,
    origin_to_address,
    tx_status,
    contract_address,
    block_hash,
    DATA,
    event_index,
    event_removed,
    topics
FROM
    flat_logs
