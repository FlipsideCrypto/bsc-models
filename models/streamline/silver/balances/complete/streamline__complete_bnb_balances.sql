{{ config (
    materialized = "incremental",
    unique_key = "id"
) }}

SELECT
    '0x123' AS id,
    '0x123' AS address,
    '0x124' AS token_address,
    CURRENT_TIMESTAMP AS _inserted_timestamp,
    100 AS block_number
