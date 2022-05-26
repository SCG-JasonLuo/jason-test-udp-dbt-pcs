{% macro get_pi_project_name() %}
    {{ return('scg-udp-pi-' + target.name) }}
{% endmacro %}

{% macro get_anon_project_name() %}
    {{ return('scg-udp-anon-' + target.name) }}
{% endmacro %}

{% macro get_dw_project_name() %}
    {{ return('scg-udp-dw-' + target.name) }}
{% endmacro %}

{% macro get_lake_project_name() %}
    {{ return('scg-udp-lake-' + target.name) }}
{% endmacro %}