{% set v_unique_key_column_name = 'TCS_Counts_SK' %}
{% set v_business_key = ['Partition_Col','Count_Time', 'Count_Door', 'Db_Tag'] %}


{{
    config (
      materialized = 'incremental',
      project = get_anon_project_name(),
      schema = 'JASON_TEST_PCS',
      alias = 'TCS_Counts',
      partition_by = {'field': 'Partition_Col', 'data_type': 'date'},
      cluster_by = ['Partition_Col', 'COUNT_DOOR'],
      incremental_strategy = 'merge',
      unique_key = v_unique_key_column_name,
      tags = ['PCSLakeToAnon', 'TCSLakeToAnon'],
      incremental_updated_columns = ['Db_Tag'
                                    ,'Count_Date'
                                    ,'Count_Time'
                                    ,'Count_Door'
                                    ,'Count_Value'
                                    ,'Deleted_Yn'
                                    ,'Dss_Update_Time'
                                    ,'Batch_Id'
                                    ,'Partition_Col'
                                    ,'Batch_Id_Updated'
                                    ,'Updated_Timestamp']

    )
}}


--BATCH_ID WILL COME FROM AIRFLOW 

WITH 
  /* Get all qualified records based on batch id */
  QUALIFIEDRECORDS AS 
  (

    --{{ get_day_zero_table(schema_source = this.schema, day_zero_table = this.name, is_day_zero = var('Day0_Run_Flag') ) }} 

    SELECT
      Db_Tag
      ,Count_Date
      ,Count_Time
      ,Count_Door
      ,Count_Value
      ,Deleted_Yn
      ,Dss_Update_Time
      ,Batch_Id
      ,_FILE_NAME AS Filename_Created      
      ,CAST(Count_Date AS DATE FORMAT 'YYYYMMDD') Partition_Col
      --,CAST(SPLIT('{{ var("PROCESS_BATCH_ID") }}','-')[ORDINAL(1)] AS INT64) AS Batch_Number
    FROM
      {{ source('Lake_PCS', 'TCS_Counts') }}
    WHERE Deleted_Yn = 'N'

  )
/* Main query */
SELECT {{ dbt_utils.surrogate_key(v_business_key) + ' AS ' + v_unique_key_column_name }}
  ,*
  , {{ get_standard_etl_columns() }}
FROM QUALIFIEDRECORDS 

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE Batch_Id = '{{ var("batch_id") }}'

{% endif %}