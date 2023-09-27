{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    tags = ['non_realtime']
) }}

WITH contract_deployments AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address AS deployer_address,
        to_address AS contract_address,
        _inserted_timestamp
    FROM
        {{ ref('silver__traces') }}
    WHERE
        from_address IN (
            '0x63ae536fec0b57bdeb1fd6a893191b4239f61bff',
            '0xa98242820ebf3a405d265ccd22a4ea8f64afb281',
            '0xb5574750a786a37e300a916974ecd63f93fc6754'
        )
        AND TYPE ILIKE 'create%'
        AND tx_status ILIKE 'success'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND to_address NOT IN (
    SELECT
        DISTINCT pool_address
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY to_address
ORDER BY
    block_timestamp ASC)) = 1
)

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    deployer_address,
    contract_address AS pool_address,
    _inserted_timestamp
FROM contract_deployments
