{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number'],
    cluster_by = "block_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['traces_reload']
) }}
{{ fsc_evm.gold_traces_v1(
    full_reload_start_block = 5000000,
    full_reload_blocks = 1000000,
    full_reload_mode = true,
    uses_overflow_steps = true
) }}
