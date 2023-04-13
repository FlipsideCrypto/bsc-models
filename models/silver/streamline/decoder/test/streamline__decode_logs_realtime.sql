{{ config (
    materialized = "view"
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
