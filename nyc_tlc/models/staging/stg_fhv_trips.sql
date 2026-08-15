{{ config(materialized = 'view') }}

WITH stg_fhv_trips AS (
    SELECT dispatching_base_num,
           pickup_datetime AS pickup_datetime, 
           dropOff_datetime AS dropoff_datetime, 
           pulocationid AS pu_location_id, 
           dolocationid AS do_location_id, 
           SR_Flag AS sr_flag, 
           Affiliated_base_number AS affiliated_base_number, 
           trip_duration_minutes, 
           source_file, 
           ingestion_timestamp, 
           pickup_date,
           'fhv' AS trip_type
           FROM {{source('raw','raw_fhv_trips')}}
)
SELECT * FROM stg_fhv_trips