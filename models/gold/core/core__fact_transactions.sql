{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    block_hash,
    tx_hash,
    nonce,
    POSITION,
    origin_function_signature,
    from_address,
    to_address,
    bnb_value,
    bnb_value_precise_raw,
    bnb_value_precise,
    tx_fee,
    tx_fee_precise,
    gas_price,
    gas_limit,
    gas_used,
    cumulative_gas_used,
    input_data,
    status,
    effective_gas_price,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    r,
    s,
    v,
    tx_type
FROM
    (
        SELECT
            block_number,
            block_timestamp,
            block_hash,
            tx_hash,
            nonce,
            POSITION,
            origin_function_signature,
            from_address,
            to_address,
            VALUE AS bnb_value,
            tx_fee,
            tx_fee_precise,
            gas_price,
            gas AS gas_limit,
            gas_used,
            cumulative_gas_used,
            input_data,
            tx_status AS status,
            effective_gas_price,
            TO_NUMBER(REPLACE(DATA :maxFeePerGas :: STRING, '0x'), REPEAT('X', LENGTH(REPLACE(DATA :maxFeePerGas :: STRING, '0x')))) / pow(
                10,
                9
            ) AS max_fee_per_gas,
            TO_NUMBER(
                REPLACE(
                    DATA :maxPriorityFeePerGas :: STRING,
                    '0x'
                ),
                REPEAT(
                    'X',
                    LENGTH(
                        REPLACE(
                            DATA :maxPriorityFeePerGas :: STRING,
                            '0x'
                        )
                    )
                )
            ) / pow(
                10,
                9
            ) AS max_priority_fee_per_gas,
            r,
            s,
            v,
            tx_type,
            to_varchar(
                TO_NUMBER(REPLACE(DATA :value :: STRING, '0x'), REPEAT('X', LENGTH(REPLACE(DATA :value :: STRING, '0x'))))
            ) AS bnb_value_precise_raw,
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
            {{ ref('silver__transactions') }}
    )
