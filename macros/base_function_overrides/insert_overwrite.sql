{% macro bq_insert_overwrite(tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns) %}
  {%- set partition_type = 'date' 
      if partition_by.data_type in ('timestamp, datetime')
      else partition_by.data_type -%}

  {%- set v_merge_predicates = config.get('merge_predicates', default=[]) -%}
  {% if v_merge_predicates != [] %} {# static - replace partitions and clusters provided in "merge_predicates" config #}
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
      
      {% set predicate %}
        (
          {{ list_predicate | join(' ' ~ v_merge_predicates_condition ~ ' ') }} 
        )
      {% endset %}

      {%- set source_sql -%}
        (
          {{ sql }}
        )
      {%- endset -%}

      {{ get_insert_overwrite_merge_sql(target_relation, source_sql, dest_columns, [predicate], include_sql_header=true) }}
        
  {% elif partitions is not none and partitions != [] %} {# static - replace partitions provided in "partitions" config #}

      {% set predicate -%}
          {{ partition_by.render(alias='DBT_INTERNAL_DEST') }} in (
              {{ partitions | join (', ') }}
          )
      {%- endset %}

      {%- set source_sql -%}
        (
          {{ sql }}
        )
      {%- endset -%}

      {{ get_insert_overwrite_merge_sql(target_relation, source_sql, dest_columns, [predicate], include_sql_header=true) }}

  {% else %} {# dynamic (get latest partition in target then replace) #}

      {% set predicate -%}
          {{ partition_by.render(alias='DBT_INTERNAL_DEST') }} in unnest(dbt_partitions_for_replacement)
      {%- endset %}

      {%- set source_sql -%}
      (
        select * from {{ tmp_relation }}
      )
      {%- endset -%}

      -- generated script to merge partitions into {{ target_relation }}
      declare dbt_partitions_for_replacement array<{{ partition_type }}>;
      declare _dbt_max_partition {{ partition_by.data_type }};

      set _dbt_max_partition = (
          select max({{ partition_by.field }}) from {{ this }}
      );

      -- 1. create a temp table
      {{ create_table_as(True, tmp_relation, sql) }}

      -- 2. define partitions to update
      set (dbt_partitions_for_replacement) = (
          select as struct
              array_agg(distinct {{ partition_by.render() }})
          from {{ tmp_relation }}
      );

      {#
        TODO: include_sql_header is a hack; consider a better approach that includes
              the sql_header at the materialization-level instead
      #}
      -- 3. run the merge statement (Yeah)
      {{ get_insert_overwrite_merge_sql(target_relation, source_sql, dest_columns, [predicate], include_sql_header=false) }};

      -- 4. clean up the temp table
      drop table if exists {{ tmp_relation }}

  {% endif %}

{% endmacro %}