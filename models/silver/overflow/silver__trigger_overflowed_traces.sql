{{ config (
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    tags = ['curated']
) }}

WITH observability_base AS (

    SELECT
        blocks_impacted_array
    FROM
        {{ ref("silver_observability__traces_completeness") }}
    WHERE
        test_timestamp >= DATEADD('hour', -24, SYSDATE())
    ORDER BY
        test_timestamp DESC
    LIMIT
        1), impacted_blocks AS (
            SELECT
                VALUE :: INT AS block_number
            FROM
                observability_base,
                LATERAL FLATTEN(
                    input => blocks_impacted_array
                )
        )
    SELECT
        COUNT(*) AS blocks_impacted,
        SYSDATE() AS test_timestamp,
        IFF(
            blocks_impacted > 0,
            TRUE,
            FALSE
        ) AS overflowed,
        IFF(
            blocks_impacted > 0,
            github_actions.workflow_dispatches(
                'FlipsideCrypto',
                'bsc-models',
                'dbt_run_overflow_models.yml',
                NULL
            ) :status_code :: INT,
            NULL
        ) AS trigger_workflow
    FROM
        impacted_blocks
