{{ config (
    materialized = 'view',
    tags = ['recent_test']
) }}

SELECT
    *
FROM
    {{ ref('core__ez_native_transfers') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref("_block_lookback") }}
    )
