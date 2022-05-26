{% macro get_standard_etl_columns() %}
    {{- '"' + var('batch_id', invocation_id) + '"' + ' BATCH_ID_CREATED, ' -}}
    {{- '"' + var('batch_id', invocation_id) + '"'  + ' BATCH_ID_UPDATED, ' -}}
    {{- 'CURRENT_TIMESTAMP CREATED_TIMESTAMP, '-}}
    {{- 'CURRENT_TIMESTAMP UPDATED_TIMESTAMP ' -}}
{% endmacro %}