{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "pool_address",
    tags = ['curated']
) }}

WITH logs_pull AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        DATA,
        contract_address,
        origin_from_address,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        (contract_address = '0xde828fdc3f497f16416d1bb645261c7c6a62dab5'
        AND topics [0] :: STRING = '0xdbd2a1ea6808362e6adbec4db4969cbc11e3b0b28fb6c74cb342defaaf1daada')
        OR tx_hash = '0x094f160d022e2e4b6a0165a40064703e78373863de8cae38ba51e441a904e0cb'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
contract_deployments AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        origin_from_address AS deployer_address,
        C.token_name AS pool_name,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS pool_address,
        l._log_id,
        l._inserted_timestamp
    FROM
        logs_pull l
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON pool_address = c.contract_address
    WHERE
        l.contract_address = '0xde828fdc3f497f16416d1bb645261c7c6a62dab5'
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    origin_from_address AS deployer_address,
    'HashflowRouter' AS pool_name,
    contract_address AS pool_address,
    _log_id,
    _inserted_timestamp
FROM
    logs_pull
WHERE
    tx_hash = '0x094f160d022e2e4b6a0165a40064703e78373863de8cae38ba51e441a904e0cb'
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    deployer_address,
    pool_name,
    pool_address,
    _log_id,
    _inserted_timestamp
FROM
    contract_deployments qualify(ROW_NUMBER() over(PARTITION BY pool_address
ORDER BY
    _inserted_timestamp DESC)) = 1
