{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% set sql %}
        CREATE schema if NOT EXISTS silver;
        {{ create_udtf_get_base_table(
            schema = "streamline"
        ) }}
        {{ create_udf_get_chainhead() }}
        {{ create_udf_json_rpc() }}
        {{ create_udf_api() }}
        {{ create_udf_decode_array_string() }}
        {{ create_udf_decode_array_object() }}
        {{ create_udf_bulk_decode_logs() }}

        {% endset %}
        {% do run_query(sql) %}
        {{- fsc_utils.create_udfs() -}}
    {% endif %}
{% endmacro %}
