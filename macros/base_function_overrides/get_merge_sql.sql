{% macro bigquery__get_merge_sql(target, source, unique_key, dest_columns, predicates) -%}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {%- set incremental_updated_columns = config.get('incremental_updated_columns', none) -%}
    {%- set incremental_updated_columns_exists = 'N' -%}
    {% if incremental_updated_columns is none %}
         {% set incremental_updated_columns = dest_columns %}
    {% else %}
        {%- set incremental_updated_columns_exists = 'Y' -%}
    {% endif %}

    {# add config for AND in when match statement #}
    {%- set incremental_compare_columns = config.get('incremental_compare_columns', none) -%}
    {%- set incremental_compare_column_default = config.get('incremental_compare_column_default', default=var('null_in_hash_column_val')) -%}

    {%- set v_merge_predicates = config.get('merge_predicates', default=[]) -%}
    {%- if v_merge_predicates -%}
        {%- set v_cluster_by_in_upper =  config.get('cluster_by', default=[]) | upper -%}
        {%- set v_cluster_by_columns_condition = config.get('cluster_by_columns_condition', default='and') -%}
        {%- set list_predicate = [] -%}
        {%- set v_merge_predicates_condition = config.get('merge_predicates_condition', default='or') -%}   
        {%- for conditions in v_merge_predicates -%}
            {%- set condition = [] %}
            {%- for key, value in conditions.items() -%}
                {# add destination alias to cluster_by columns only, else warn #}
                {%- set key_and_value -%}
                    DBT_INTERNAL_DEST.{{ key ~ ' in unnest(' ~ value ~ ')' 
                       if value is iterable 
                       and (value is not string and value is not mapping)
                       else key ~ ' = \'' ~ value ~ '\'' }}
                {%- endset -%}
                {%- do condition.append(key_and_value) 
                    if (key | upper) in v_cluster_by_in_upper 
                    else exceptions.warn("Column does not exists in `cluster_by`. Got: " ~ key) -%}
            {%- endfor -%}
            {% do list_predicate.append('(' + condition | join(' ' + v_cluster_by_columns_condition + ' ') + ')') %}
        {%- endfor -%}
        {% set list_predicate %}
            (
              {{ list_predicate | join(' ' ~ v_merge_predicates_condition ~ ' ') }}
            )
        {% endset %}
        {% do predicates.append(list_predicate) %} 

    {%- endif -%}

    {% if unique_key %}
        {% set unique_key_match %}
            DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
        {% endset %}
        {% do predicates.append(unique_key_match) %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}

    {{ sql_header if sql_header is not none }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on {{ predicates | join(' and ') }}

    {% if unique_key %}
    when matched 
    {% if incremental_compare_columns is not none -%}
         {%- set columns = [] -%}
         {%- for column in incremental_compare_columns -%}
             {%- set _ = columns.append("coalesce(cast(DBT_INTERNAL_SOURCE." 
                                       ~ column 
                                       ~ " as string),'" 
                                       ~ incremental_compare_column_default
                                       ~ "')"
                                       ) 
             -%}
         {%- endfor -%}
         {%- set hashed_columns = "to_hex(md5(concat(" ~ columns | join(', ') ~ ")))" -%} 
         and (
              /* 
                compare the following source and target columns to detect if record is for update
                - {{ incremental_compare_columns | join('
                - ') }}
              */
              {{ hashed_columns }} <> {{ hashed_columns | replace('DBT_INTERNAL_SOURCE.','DBT_INTERNAL_DEST.') }}
             )
    {%- endif %}
        then update set
        {% for column in incremental_updated_columns -%}
            {% if incremental_updated_columns_exists == 'N' -%}
                {{ adapter.quote(column.name) }} = DBT_INTERNAL_SOURCE.{{ adapter.quote(column.name) }}
            {% else %}
                {{ adapter.quote(column) }} = DBT_INTERNAL_SOURCE.{{ adapter.quote(column) }}
            {% endif %}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    {% endif %}
    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

{% endmacro %}
