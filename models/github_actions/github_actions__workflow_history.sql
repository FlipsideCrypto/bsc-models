{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'workflow_history_id',
    tags = ['gha_tasks'],
    full_refresh = false
) }}

WITH

{% if is_incremental() %}
-- get latest
workflow_runs AS (

    SELECT
        id,
        NAME,
        node_id,
        check_suite_id,
        check_suite_node_id,
        head_branch,
        head_sha,
        run_number,
        event,
        display_title,
        status,
        conclusion,
        workflow_id,
        url,
        html_url,
        pull_requests,
        created_at,
        updated_at,
        actor,
        run_attempt,
        run_started_at,
        triggering_actor,
        jobs_url,
        logs_url,
        check_suite_url,
        artifacts_url,
        cancel_url,
        rerun_url,
        workflow_url,
        head_commit,
        repository,
        head_repository
    FROM
        TABLE(
            github_actions.tf_runs(
                'FlipsideCrypto',
                'bsc-models',{ 'per_page' :'100',
                'page': '1' }
            )
        )
    WHERE
        NAME IN (
            SELECT
                workflow_name
            FROM
                {{ ref('github_actions__runtime_detection') }}
        )
        AND conclusion IS NOT NULL
)
{% else %}
    -- get history, last 100 pages
workflow_runs AS ({% for item in range(100) %}
    (
    SELECT
        id,
        NAME,
        node_id,
        check_suite_id,
        check_suite_node_id,
        head_branch,
        head_sha,
        run_number,
        event,
        display_title,
        status,
        conclusion,
        workflow_id,
        url,
        html_url,
        pull_requests,
        created_at,
        updated_at,
        actor,
        run_attempt,
        run_started_at,
        triggering_actor,
        jobs_url,
        logs_url,
        check_suite_url,
        artifacts_url,
        cancel_url,
        rerun_url,
        workflow_url,
        head_commit,
        repository,
        head_repository
    FROM
        TABLE(github_actions.tf_runs(
            'FlipsideCrypto',
            'bsc-models',{ 'per_page' :'100',
            'page': '{{ item }}' }
            )
        )
    WHERE
        NAME IN (
    SELECT
        workflow_name
    FROM
        {{ ref('github_actions__runtime_detection') }})
        AND conclusion IS NOT NULL) 
    {% if not loop.last %}
        UNION ALL
    {% endif %}
    {% endfor %})
{% endif %}

SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['id']
    ) }} AS workflow_history_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    workflow_runs qualify (ROW_NUMBER() over (PARTITION BY id
ORDER BY
    created_at DESC)) = 1
