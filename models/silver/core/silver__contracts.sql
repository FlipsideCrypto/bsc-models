{{ config(
    materialized = 'incremental',
    unique_key = 'contract_address',
    tags = ['non_realtime']
) }}

WITH base_metadata AS (

    SELECT
        contract_address,
        block_number,
        function_sig AS function_signature,
        read_result AS read_output,
        _inserted_timestamp
    FROM
        {{ ref('bronze_api__token_reads') }}
    WHERE
        read_result IS NOT NULL
        AND read_result <> '0x'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
token_names AS (
    SELECT
        contract_address,
        block_number,
        function_signature,
        read_output,
        regexp_substr_all(SUBSTR(read_output, 3, len(read_output)), '.{64}') AS segmented_output,
        LTRIM(
            REGEXP_REPLACE(
                TRY_HEX_DECODE_STRING(
                    segmented_output [2] :: STRING
                ),
                '[\x00-\x1F\x7F-\x9F\xAD]',
                '',
                1
            )
        ) AS token_name
    FROM
        base_metadata
    WHERE
        function_signature = '0x06fdde03'
        AND segmented_output [1] :: STRING IS NOT NULL
),
token_symbols AS (
    SELECT
        contract_address,
        block_number,
        function_signature,
        read_output,
        regexp_substr_all(SUBSTR(read_output, 3, len(read_output)), '.{64}') AS segmented_output,
        LTRIM(
            REGEXP_REPLACE(
                TRY_HEX_DECODE_STRING(
                    segmented_output [2] :: STRING
                ),
                '[\x00-\x1F\x7F-\x9F\xAD]',
                '',
                1
            )
        ) AS token_symbol
    FROM
        base_metadata
    WHERE
        function_signature = '0x95d89b41'
        AND segmented_output [1] :: STRING IS NOT NULL
),
token_decimals AS (
    SELECT
        contract_address,
        CASE
            WHEN read_output IS NOT NULL THEN utils.udf_hex_to_int(LEFT(read_output :: STRING, 66))
            ELSE NULL
        END AS token_decimals,
        LENGTH(token_decimals) AS dec_length
    FROM
        base_metadata
    WHERE
        function_signature = '0x313ce567'
        AND read_output IS NOT NULL
        AND read_output <> '0x'
        AND dec_length < 3
),
contracts AS (
    SELECT
        contract_address,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        base_metadata
    GROUP BY
        1
)
SELECT
    c1.contract_address :: STRING AS contract_address,
    token_name,
    TRY_TO_NUMBER(token_decimals) AS token_decimals,
    token_symbol,
    _inserted_timestamp
FROM
    contracts c1
    LEFT JOIN token_names
    ON c1.contract_address = token_names.contract_address
    LEFT JOIN token_symbols
    ON c1.contract_address = token_symbols.contract_address
    LEFT JOIN token_decimals
    ON c1.contract_address = token_decimals.contract_address
    AND dec_length < 3 qualify(ROW_NUMBER() over(PARTITION BY c1.contract_address
ORDER BY
    _inserted_timestamp DESC)) = 1
