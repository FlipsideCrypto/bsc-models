{{ config (
    materialized = 'view',
    tags = ['bronze_core']
) }}

SELECT
    partition_key,
    block_number,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__blocks_fr_v2') }}
{% if var('GLOBAL_USES_STREAMLINE_V1', false) %}
UNION ALL
SELECT
    _partition_by_block_id AS partition_key,
    block_number,
    VALUE,
    DATA :result AS DATA, --add to fsc-evm with if logic and new var, specific to BSC only
    metadata,
    file_name,
    _inserted_timestamp
FROM
   {{ ref('bronze__blocks_fr_v1') }}
{% endif %}
{% if var('GLOBAL_USES_BLOCKS_TRANSACTIONS_PATH', false) %}
UNION ALL
SELECT
    partition_key,
    block_number,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__blocks_fr_v2_1') }}
{% endif %}