{% macro db_comment() %}
    {% set query %}
SELECT
    TO_DATE(MIN(block_timestamp))
FROM
    silver.blocks {% endset %}
    {{ log(query) }}

    {% set results = run_query(query) %}
    {% do results.print_table() %}
    {% set results_list = results.columns [0].values() [0].strftime('%Y-%m-%d') %}
    {{ log(results_list) }}

    {% set sql %}
    COMMENT
    ON database bsc_dev IS 'Lite Mode dataset with recent data only. Min block_timestamp:{{results_list}} 🌱 ' {% endset %}
    {% do run_query(sql) %}
{% endmacro %}