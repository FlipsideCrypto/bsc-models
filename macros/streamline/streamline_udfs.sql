{% macro create_udf_get_chainhead() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_chainhead() returns variant api_integration = aws_bsc_api AS {% if target.name == "prod" %}
        'https://u0bch3zf8l.execute-api.us-east-1.amazonaws.com/prod/get_chainhead'
    {% else %}
        'https://q22a7542fk.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'
    {%- endif %};
{% endmacro %}

{% macro create_udf_json_rpc() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_json_rpc(
        json OBJECT
    ) returns ARRAY api_integration = aws_bsc_api AS {% if target.name == "prod" %}
        'https://u0bch3zf8l.execute-api.us-east-1.amazonaws.com/prod/bulk_get_json_rpc'
    {% else %}
        'https://q22a7542fk.execute-api.us-east-1.amazonaws.com/dev/bulk_get_json_rpc'
    {%- endif %};
{% endmacro %}


{% macro create_udf_api() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_api(
        method VARCHAR,
        url VARCHAR,
        headers OBJECT,
        DATA OBJECT
    ) returns variant api_integration = aws_bsc_api AS {% if target.name == "prod" %}
        'https://u0bch3zf8l.execute-api.us-east-1.amazonaws.com/prod/udf_api'
    {% else %}
        'https://q22a7542fk.execute-api.us-east-1.amazonaws.com/dev/udf_api'
    {%- endif %};
{% endmacro %}

{% macro create_udf_decode_array_string() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_decode(
        abi ARRAY,
        DATA STRING
    ) returns ARRAY api_integration = aws_bsc_api AS {% if target.name == "prod" %}
        'https://u0bch3zf8l.execute-api.us-east-1.amazonaws.com/prod/decode_function'
    {% else %}
        'https://q22a7542fk.execute-api.us-east-1.amazonaws.com/dev/decode_function'
    {%- endif %};
{% endmacro %}

{% macro create_udf_decode_array_object() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_decode(
        abi ARRAY,
        DATA OBJECT
    ) returns ARRAY api_integration = aws_bsc_api AS {% if target.name == "prod" %}
        'https://u0bch3zf8l.execute-api.us-east-1.amazonaws.com/prod/decode_log'
    {% else %}
        'https://q22a7542fk.execute-api.us-east-1.amazonaws.com/dev/decode_log'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_decode_logs() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_bulk_decode_logs(
        json OBJECT
    ) returns ARRAY api_integration = aws_bsc_api AS {% if target.name == "prod" %}
        'https://u0bch3zf8l.execute-api.us-east-1.amazonaws.com/prod/bulk_decode_logs'
    {% else %}
        'https://q22a7542fk.execute-api.us-east-1.amazonaws.com/dev/bulk_decode_logs'
    {%- endif %};
{% endmacro %}