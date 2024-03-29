{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = "block_timestamp::date, _inserted_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
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
),
new_records AS (
    SELECT
        l.block_number,
        txs.block_timestamp,
        l.tx_hash,
        txs.from_address AS origin_from_address,
        txs.to_address AS origin_to_address,
        txs.origin_function_signature AS origin_function_signature,
        txs.tx_status,
        l.contract_address,
        l.block_hash,
        l.data,
        l.event_index,
        l.event_removed,
        l.topics,
        l._inserted_timestamp,
        CASE
            WHEN txs.block_timestamp IS NULL
            OR txs.origin_function_signature IS NULL THEN TRUE
            ELSE FALSE
        END AS is_pending,
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            l.event_index :: STRING
        ) AS _log_id
    FROM
        flat_logs l
        LEFT OUTER JOIN {{ ref('silver__overflowed_transactions') }}
        txs
        ON l.block_number = txs.block_number
        AND l.tx_hash = txs.tx_hash
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_status,
    contract_address,
    block_hash,
    DATA,
    event_index,
    event_removed,
    topics,
    _inserted_timestamp,
    _log_id,
    is_pending,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS logs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    new_records qualify(ROW_NUMBER() over (PARTITION BY block_number, event_index
ORDER BY
    _inserted_timestamp DESC, is_pending ASC)) = 1
