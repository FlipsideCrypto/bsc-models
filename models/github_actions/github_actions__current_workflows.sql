{{ config(
    materialized = 'view',
    tags = ['gha_tasks']
) }}

SELECT
    NAME,
    status,
    created_at,
    updated_at,
    run_started_at,
    run_attempt,
    run_number,
    TIMESTAMPDIFF(seconds, run_started_at, SYSDATE()) / 60 AS run_minutes,
    id,
    workflow_id,
    html_url
FROM
    TABLE(
        github_actions.tf_runs(
            'FlipsideCrypto',
            'bsc-models',{ 'status' :'in_progress' }
        )
    )
WHERE
    NAME IN (
        SELECT
            workflow_name
        FROM
            {{ ref('github_actions__runtime_detection') }}
    )
