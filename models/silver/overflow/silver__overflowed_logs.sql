{{ config(
    materialized = 'view',
    tags = ['overflow']
) }}

WITH base AS (

    SELECT
        block_number,
        index_vals [1] :: INT AS tx_position,
        REPLACE(REPLACE(SPLIT(path, '.') [0] :: STRING, 'logs['), ']') :: INT AS id_,
        SYSDATE() AS _inserted_timestamp,
        OBJECT_AGG(IFNULL(key, SPLIT(path, '.') [1]), value_) AS json_data
    FROM
        {{ ref("bronze__overflowed_receipts") }}
    WHERE
        path LIKE '%[%'
    GROUP BY
        ALL
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
        _inserted_timestamp
    FROM
        base
)
SELECT
    block_number,
    tx_hash,
    from_address AS origin_from_address,
    to_address AS origin_to_address,
    tx_status,
    contract_address,
    block_hash,
    DATA,
    event_index,
    event_removed,
    topics
FROM
    flat_logs
    JOIN {{ ref('silver__receipts') }} USING (
        block_number,
        tx_hash
    ) qualify(ROW_NUMBER() over (PARTITION BY block_number, event_index
ORDER BY
    _inserted_timestamp DESC, is_pending ASC)) = 1
