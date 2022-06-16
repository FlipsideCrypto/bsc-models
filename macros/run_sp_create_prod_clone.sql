{% macro run_sp_create_prod_clone() %}
    {% set clone_query %}
    call bsc._internal.create_prod_clone(
        'bsc',
        'bsc_dev',
        'bsc_dev'
    );
{% endset %}
    {% do run_query(clone_query) %}
{% endmacro %}
