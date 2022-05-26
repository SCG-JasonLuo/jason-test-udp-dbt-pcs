{% macro get_batch_id() %}
    {% set return_batch_ids = [] %}
    {% set batch_ids = var('DATA_BATCH_ID', [invocation_id]) %}
    {% for batch_id in batch_ids %}
        {{ return_batch_ids.append('"' + batch_id + '"') }}
    {% endfor %}
    {{ return(return_batch_ids | join(',')) }}
{% endmacro %}
