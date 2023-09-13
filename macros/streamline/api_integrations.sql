{% macro create_aws_bsc_api() %}
    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_bsc_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/bsc-api-prod-rolesnowflakeudfsAF733095-1GUY4JJNG9FQL' api_allowed_prefixes = (
            'https://u0bch3zf8l.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% else %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_bsc_dev_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/bsc-api-dev-rolesnowflakeudfsAF733095-1S8A1YYLZ09VX' api_allowed_prefixes = (
            'https://q22a7542fk.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}