{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_decode_logs(object_construct('sql_source', '{{this.identifier}}', 'producer_batch_size', 20000000,'producer_limit_size', 20000000))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}
-- WITH look_back AS (
--     SELECT
--         block_number
--     FROM
--         {{ ref("_max_block_by_date") }}
--         qualify ROW_NUMBER() over (
--             ORDER BY
--                 block_number DESC
--         ) = 1
-- )

SELECT
    l.block_number,
    l._log_id,
    PARSE_JSON(
        A.abi_data :data :result
    ) AS abi,
    l.data
FROM
    {{ ref("streamline__decode_logs") }}
    l
    INNER JOIN {{ ref("bronze_api__contract_abis") }} A
    ON l.abi_address = A.contract_address
WHERE
    -- (
    --     l.block_number >= (
    --         SELECT
    --             block_number
    --         FROM
    --             look_back
    --     )
    -- )
    -- AND
    l.block_number IS NOT NULL
    AND A.contract_address = LOWER('0xb6D7bbdE7c46a8B784F4a19C7FDA0De34b9577DB') --remove
