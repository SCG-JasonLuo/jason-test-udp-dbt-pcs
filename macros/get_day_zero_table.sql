
{% macro get_day_zero_table(day_zero_table, schema_source, is_day_zero) -%}

{% set v_day_zero_start_date =  var('day_zero_start_date') %}
{% set v_day_zero_batch_number =  0 %}
{% set v_null_defval = var('null_column_val') %}
{% set v_num_defval = var('numeric_column_default') %}
{% set v_default_sk = var('null_in_hash_column_val') %}
--{% set exclude_tma = ' carpark_no != "0038001"' %}    


    {%- if is_day_zero  -%}
        {%- if schema_source == "PCS_TCS" and day_zero_table == 'TCSCounts_test'   -%}
            {% for source_table in [day_zero_table, day_zero_table ~ "_Historical"] %}

                
                SELECT
                    DB_TAG
                    ,COUNT_DATE
                    ,COUNT_TIME
                    ,COUNT_DOOR
                    ,COUNT_VALUE
                    ,DELETED_YN
                    ,DSS_UPDATE_TIME
                    ,_FILE_NAME AS FILENAME_CREATED
                    ,CAST(COUNT_DATE AS DATE FORMAT 'YYYYMMDD') PARTITION_COL
                    ,{{ v_day_zero_batch_number }} AS Batch_Number
                FROM {{ source('DataLake_PCS_TCS', source_table) }}
                WHERE CAST(COUNT_DATE AS INT64) >= {{ v_day_zero_start_date }}
                --AND    {{ exclude_tma }}

                UNION DISTINCT 
            {% endfor %}

        {%- endif -%}

    {%- endif -%}

{%- endmacro %}