{{ config(
    materialized = 'incremental',
    unique_key = "lb_pair"
) }}

WITH pool_creation AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS tokenX,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS tokenY,
        ethereum.public.udf_hex_to_int(
            topics [3] :: STRING
        ) :: INT AS binStep,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS lb_pair,
        ethereum.public.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INT AS pool_id,
        CASE
            WHEN contract_address = '0x6e77932a92582f504ff6c4bdbcef7da6c198aeef' THEN 'v2'
            WHEN contract_address = '0x8e42f2f4101563bf679975178e880fd87d3efd4e' THEN 'v2.1'
        END AS version,
        _log_id,
        _inserted_timestamp
    FROM 
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN ('0x8e42f2f4101563bf679975178e880fd87d3efd4e','0x6e77932a92582f504ff6c4bdbcef7da6c198aeef')
        AND topics [0] :: STRING = '0x2c8d104b27c6b7f4492017a6f5cf3803043688934ebcaa6a03540beeaf976aff' --LB PairCreated

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
AND lb_pair NOT IN (
    SELECT
        DISTINCT lb_pair
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    tokenX,
    tokenY,
    binStep AS bin_step,
    lb_pair,
    pool_id,
    version,
    _log_id,
    _inserted_timestamp
FROM
    pool_creation
