{% macro create_udtf_get_base_table(schema) %}
    CREATE
    OR REPLACE FUNCTION {{ schema }}.udtf_get_base_table(
        max_height INTEGER
    ) returns TABLE (
        height NUMBER
    ) AS $$ WITH base AS (
        SELECT
            ROW_NUMBER() over (
                ORDER BY
                    SEQ4()
            ) AS id
        FROM
            TABLE(GENERATOR(rowcount => 100000000))
    )
SELECT
    id AS height
FROM
    base
WHERE
    id <= max_height $$;
{% endmacro %}
