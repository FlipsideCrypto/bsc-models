{% macro create_aws_bsc_api() %}
    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_bsc_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/snowflake-api-bsc' api_allowed_prefixes = (
            'https://2ltt1xstoc.execute-api.us-east-1.amazonaws.com/prod/',
            'https://qqy8pvhork.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
