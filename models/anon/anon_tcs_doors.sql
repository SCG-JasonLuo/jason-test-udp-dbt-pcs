{% set v_unique_key_column_name = 'TCS_Door_SK' %}
{% set v_business_key = ['DB_Tag', 'Door_ID'] %}

{{
    config (
      materialized = 'incremental',
      project = get_anon_project_name(),
      dataset = 'JASON_TEST_PCS',
      alias = 'TCS_Doors',
      unique_key = v_unique_key_column_name,
      tags = ['PCSLakeToAnon', 'TCSLakeToAnon'],
      merge_update_columns = [  "DB_Tag"
                                , "Door_ID"
                                , "Door_Card"
                                , "Door_Chan"
                                , "Door_Active"
                                , "Door_Archive"
                                , "Door_Desc"
                                , "Door_Type"
                                , "Last_Approved"
                                , "Door_Scratchreg"
                                , "Level_ID"
                                , "Installation_ID"
                                , "Ext_DB_Door_ID"
                                , "Door_Monitor"
                                , "Deleted_YN"
                                , "DSS_Update_Time"
                                , "Batch_Id"
                                , "Processed_Timestamp", "Batch_Id_UPDATED", "UPDATED_TIMESTAMP" ]
    )
}}

WITH BASE AS 
(
  SELECT 
        DB_Tag
        ,Door_ID 
        ,Door_Card
        ,Door_Chan
        ,Door_Active
        ,Door_Archive
        ,Door_Desc
        ,Door_Type
        ,Last_Approved
        ,Door_Scratchreg
        ,Level_ID
        ,Installation_ID
        ,Extdbdoorid AS Ext_DB_Door_ID
        ,Door_Monitor
        ,Deleted_YN
        ,DSS_Update_Time
        ,Batch_ID
        ,_file_name AS Filename_Created
  FROM   {{ source( 'Lake_PCS', 'TCS_Doors' ) }} 
)
SELECT 
       {{ dbt_utils.surrogate_key(v_business_key) + ' AS ' + v_unique_key_column_name }}
       ,*
       ,TIMESTAMP_MILLIS( UNIX_MILLIS( '{{ var("processed_timestamp") }}' ) ) AS Processed_Timestamp
       ,{{ get_standard_etl_columns() }}
FROM   BASE 
where Batch_ID = '{{ var("batch_id") }}'