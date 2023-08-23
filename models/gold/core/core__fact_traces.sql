{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    from_address,
    to_address,
    bnb_value,
    CASE
        WHEN tx_hash <> '0xb0db99da6d39abd0bb6c169a4ec472438340a011fd646bdce0c7c99232502301' THEN IFNULL(
            bnb_value_precise_raw,
            '0'
        )
        ELSE NULL
    END AS bnb_value_precise_raw,
    CASE
        WHEN tx_hash <> '0xb0db99da6d39abd0bb6c169a4ec472438340a011fd646bdce0c7c99232502301' THEN IFNULL(
            bnb_value_precise,
            '0'
        )
        ELSE NULL
    END AS bnb_value_precise,
    gas,
    gas_used,
    input,
    output,
    TYPE,
    identifier,
    DATA,
    tx_status,
    sub_traces,
    trace_status,
    error_reason,
    trace_index
FROM
    (
        SELECT
            tx_hash,
            block_number,
            block_timestamp,
            from_address,
            to_address,
            bnb_value,
            gas,
            gas_used,
            input,
            output,
            TYPE,
            identifier,
            DATA,
            tx_status,
            sub_traces,
            trace_status,
            error_reason,
            trace_index,
            REPLACE(
                COALESCE(
                    DATA :value :: STRING,
                    DATA :action :value :: STRING
                ),
                '0x'
            ) AS hex,
            CASE
                WHEN tx_hash <> '0xb0db99da6d39abd0bb6c169a4ec472438340a011fd646bdce0c7c99232502301' THEN to_varchar(TO_NUMBER(hex, REPEAT('X', LENGTH(hex))))
                ELSE null end AS bnb_value_precise_raw,
                IFF(LENGTH(bnb_value_precise_raw) > 18, LEFT(bnb_value_precise_raw, LENGTH(bnb_value_precise_raw) - 18) || '.' || RIGHT(bnb_value_precise_raw, 18), '0.' || LPAD(bnb_value_precise_raw, 18, '0')) AS rough_conversion,
                IFF(
                    POSITION(
                        '.000000000000000000' IN rough_conversion
                    ) > 0,
                    LEFT(rough_conversion, LENGTH(rough_conversion) - 19),
                    REGEXP_REPLACE(
                        rough_conversion,
                        '0*$',
                        ''
                    )
                ) AS bnb_value_precise
                FROM
                    {{ ref('silver__traces') }}
            )
