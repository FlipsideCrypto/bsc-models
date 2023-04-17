{% macro create_udf_get_chainhead() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_chainhead() returns variant api_integration = aws_bsc_api AS {% if target.name == "prod" %}
        'https://2ltt1xstoc.execute-api.us-east-1.amazonaws.com/prod/get_chainhead'
    {% else %}
        'https://qqy8pvhork.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'
    {%- endif %};
{% endmacro %}

{% macro create_udf_json_rpc() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_json_rpc(
        json OBJECT
    ) returns ARRAY api_integration = aws_bsc_api AS {% if target.name == "prod" %}
        'https://2ltt1xstoc.execute-api.us-east-1.amazonaws.com/prod/bulk_get_json_rpc'
    {% else %}
        'https://qqy8pvhork.execute-api.us-east-1.amazonaws.com/dev/bulk_get_json_rpc'
    {%- endif %};
{% endmacro %}

{% macro create_udf_decode_array_object() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_decode(
        abi ARRAY,
        DATA OBJECT
    ) returns ARRAY api_integration = aws_bsc_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/decode_log'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/decode_log'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_decode_logs() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_decode_logs(
        json OBJECT
    ) returns ARRAY api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_decode_logs'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_decode_logs'
    {%- endif %};
{% endmacro %}