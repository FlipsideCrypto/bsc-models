{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number'],
    cluster_by = "block_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    full_refresh = false
) }}

WITH silver_traces AS (

    SELECT
        block_number,
        tx_position,
        trace_address,
        parent_trace_address,
        trace_address_array,
        trace_json,
        traces_id,
        'regular' AS source
    FROM
        {{ ref('silver__traces2') }}
    WHERE
        1 = 1

{% if is_incremental() and not var(
    'RELOAD_TRACES',
) %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
) {% elif is_incremental() and var(
    'RELOAD_TRACES',
) %}
AND block_number BETWEEN (
    SELECT
        MAX(
            block_number
        )
    FROM
        {{ this }}
)
AND (
    SELECT
        MAX(
            block_number
        ) + 1000000
    FROM
        {{ this }}
)
{% else %}
    AND block_number <= 5000000
{% endif %}
UNION ALL
SELECT
    block_number,
    tx_position,
    trace_address,
    parent_trace_address,
    trace_address_array,
    trace_json,
    traces_id,
    'overflow' AS source
FROM
    {{ ref('silver__overflowed_traces2') }}
WHERE
    1 = 1

{% if is_incremental() and not var(
    'RELOAD_TRACES',
) %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
) {% elif is_incremental() and var(
    'RELOAD_TRACES',
) %}
AND block_number BETWEEN (
    SELECT
        MAX(
            block_number
        )
    FROM
        {{ this }}
)
AND (
    SELECT
        MAX(
            block_number
        ) + 1000000
    FROM
        {{ this }}
)
{% else %}
    AND block_number <= 5000000
{% endif %}
),
sub_traces AS (
    SELECT
        block_number,
        tx_position,
        parent_trace_address,
        COUNT(*) AS sub_traces
    FROM
        silver_traces
    GROUP BY
        block_number,
        tx_position,
        parent_trace_address
),
trace_index_array AS (
    SELECT
        block_number,
        tx_position,
        trace_address,
        ARRAY_AGG(flat_value) AS number_array
    FROM
        (
            SELECT
                block_number,
                tx_position,
                trace_address,
                IFF(
                    VALUE :: STRING = 'ORIGIN',
                    -1,
                    VALUE :: INT
                ) AS flat_value
            FROM
                silver_traces,
                LATERAL FLATTEN (
                    input => trace_address_array
                )
        )
    GROUP BY
        block_number,
        tx_position,
        trace_address
),
trace_index_sub_traces AS (
    SELECT
        b.block_number,
        b.tx_position,
        b.trace_address,
        IFNULL(
            sub_traces,
            0
        ) AS sub_traces,
        number_array,
        ROW_NUMBER() over (
            PARTITION BY b.block_number,
            b.tx_position
            ORDER BY
                number_array ASC
        ) - 1 AS trace_index,
        b.trace_json,
        b.traces_id
    FROM
        silver_traces b
        LEFT JOIN sub_traces s
        ON b.block_number = s.block_number
        AND b.tx_position = s.tx_position
        AND b.trace_address = s.parent_trace_address
        JOIN trace_index_array n
        ON b.block_number = n.block_number
        AND b.tx_position = n.tx_position
        AND b.trace_address = n.trace_address
),
errored_traces AS (
    SELECT
        block_number,
        tx_position,
        trace_address,
        trace_json
    FROM
        trace_index_sub_traces
    WHERE
        trace_json :error :: STRING IS NOT NULL
),
error_logic AS (
    SELECT
        b0.block_number,
        b0.tx_position,
        b0.trace_address,
        b0.trace_json :error :: STRING AS error,
        b1.trace_json :error :: STRING AS any_error,
        b2.trace_json :error :: STRING AS origin_error
    FROM
        trace_index_sub_traces b0
        LEFT JOIN errored_traces b1
        ON b0.block_number = b1.block_number
        AND b0.tx_position = b1.tx_position
        AND b0.trace_address LIKE CONCAT(
            b1.trace_address,
            '_%'
        )
        LEFT JOIN errored_traces b2
        ON b0.block_number = b2.block_number
        AND b0.tx_position = b2.tx_position
        AND b2.trace_address = 'ORIGIN'
),
aggregated_errors AS (
    SELECT
        block_number,
        tx_position,
        trace_address,
        error,
        IFF(MAX(any_error) IS NULL
        AND error IS NULL
        AND origin_error IS NULL, TRUE, FALSE) AS trace_succeeded
    FROM
        error_logic
    GROUP BY
        block_number,
        tx_position,
        trace_address,
        error,
        origin_error),
        json_traces AS (
            SELECT
                block_number,
                tx_position,
                trace_address,
                sub_traces,
                number_array,
                trace_index,
                trace_json AS DATA,
                trace_succeeded,
                trace_json :error :: STRING AS error_reason,
                trace_json :revertReason :: STRING AS revert_reason,
                trace_json :from :: STRING AS from_address,
                trace_json :to :: STRING AS to_address,
                IFNULL(
                    utils.udf_hex_to_int(
                        trace_json :value :: STRING
                    ),
                    '0'
                ) AS value_precise_raw,
                utils.udf_decimal_adjust(
                    value_precise_raw,
                    18
                ) AS value_precise,
                value_precise :: FLOAT AS VALUE,
                utils.udf_hex_to_int(
                    trace_json :gas :: STRING
                ) :: INT AS gas,
                utils.udf_hex_to_int(
                    trace_json :gasUsed :: STRING
                ) :: INT AS gas_used,
                trace_json :input :: STRING AS input,
                trace_json :output :: STRING AS output,
                trace_json :type :: STRING AS TYPE,
                concat_ws(
                    '_',
                    TYPE,
                    trace_address
                ) AS identifier,
                IFF(
                    trace_succeeded,
                    'SUCCESS',
                    'FAIL'
                ) AS trace_status,
                traces_id
            FROM
                trace_index_sub_traces
                JOIN aggregated_errors USING (
                    block_number,
                    tx_position,
                    trace_address
                )
        ),
        incremental_traces AS (
            SELECT
                f.block_number,
                t.tx_hash,
                t.block_timestamp,
                t.tx_status,
                f.tx_position,
                f.trace_index,
                f.from_address,
                f.to_address,
                f.value_precise_raw,
                f.value_precise,
                f.value,
                f.gas,
                f.gas_used,
                f.input,
                f.output,
                f.type,
                f.identifier,
                f.sub_traces,
                f.error_reason,
                f.revert_reason,
                f.trace_status,
                f.data,
                f.traces_id,
                f.trace_succeeded,
                f.trace_address
            FROM
                json_traces f
                LEFT OUTER JOIN {{ ref('silver__transactions') }}
                t
                ON f.tx_position = t.position
                AND f.block_number = t.block_number

{% if is_incremental() and not var(
    'RELOAD_TRACES',
) %}
AND t.modified_timestamp >= (
    SELECT
        DATEADD('hour', -24, MAX(modified_timestamp))
    FROM
        {{ this }})
    {% endif %}
)

{% if is_incremental() %},
heal_missing_data AS (
    SELECT
        t.block_number,
        txs.tx_hash,
        txs.block_timestamp,
        txs.tx_status,
        t.tx_position,
        t.trace_index,
        t.from_address,
        t.to_address,
        t.value_precise_raw,
        t.value_precise,
        t.value,
        t.gas,
        t.gas_used,
        t.input,
        t.output,
        t.type,
        t.identifier,
        t.sub_traces,
        t.error_reason,
        t.revert_reason,
        t.trace_status,
        t.data,
        t.fact_traces_id AS traces_id,
        t.trace_succeeded,
        t.trace_address
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('silver__transactions') }}
        txs
        ON t.tx_position = txs.position
        AND t.block_number = txs.block_number
    WHERE
        t.tx_hash IS NULL
        OR t.block_timestamp IS NULL
        OR t.tx_status IS NULL
        OR t.block_number IN (
            SELECT
                DISTINCT block_number
            FROM
                silver_traces
            WHERE
                source = 'overflow'
        )
)
{% endif %},
all_traces AS (
    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        tx_status,
        tx_position,
        trace_index,
        from_address,
        to_address,
        value_precise_raw,
        value_precise,
        VALUE,
        gas,
        gas_used,
        input,
        output,
        TYPE,
        identifier,
        sub_traces,
        error_reason,
        revert_reason,
        trace_status,
        DATA,
        traces_id,
        trace_succeeded,
        trace_address
    FROM
        incremental_traces

{% if is_incremental() %}
UNION ALL
SELECT
    block_number,
    tx_hash,
    block_timestamp,
    tx_status,
    tx_position,
    trace_index,
    from_address,
    to_address,
    value_precise_raw,
    value_precise,
    VALUE,
    gas,
    gas_used,
    input,
    output,
    TYPE,
    identifier,
    sub_traces,
    error_reason,
    revert_reason,
    trace_status,
    DATA,
    traces_id,
    trace_succeeded,
    trace_address
FROM
    heal_missing_data
{% endif %}
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    tx_position,
    trace_index,
    from_address,
    to_address,
    VALUE,
    value_precise_raw,
    value_precise,
    gas,
    gas_used,
    input,
    output,
    TYPE,
    trace_address,
    identifier,
    DATA,
    sub_traces,
    tx_status,
    trace_status,
    IFF(
        tx_status = 'SUCCESS',
        TRUE,
        FALSE
    ) AS tx_succeeded,
    trace_succeeded,
    error_reason,
    revert_reason,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'trace_index']
    ) }} AS fact_traces_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    all_traces qualify(ROW_NUMBER() over(PARTITION BY block_number, tx_position, trace_index
ORDER BY
    modified_timestamp DESC, block_timestamp DESC nulls last)) = 1
