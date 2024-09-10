-- depends_on: {{ ref('bronze__streamline_traces') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['core','non_realtime'],
    full_refresh = false
) }}
{{ fsc_evm.silver_traces_v1(
    full_reload_start_block = 2300000,
    full_reload_blocks = 1000000
) }}
