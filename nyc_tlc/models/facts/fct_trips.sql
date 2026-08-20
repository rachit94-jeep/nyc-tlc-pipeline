{{ config(
    materialized = 'incremental',
    unique_key = 'trip_id'
) }}

WITH deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY pickup_datetime, dropoff_datetime, pu_location_id, 
                         do_location_id, trip_type, source_file,fare_amount
            ORDER BY ingestion_timestamp DESC
        ) AS row_num
    FROM {{ ref('int_all_trips_unioned') }}

    {% if is_incremental() %}
    WHERE pickup_date >= (SELECT MAX(pickup_date) FROM {{ this }})
    {% endif %}
),

fct_trips AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'pickup_datetime', 'dropoff_datetime',
            'pu_location_id', 'do_location_id',
            'trip_type', 'source_file','fare_amount'
        ]) }} AS trip_id,
        * EXCLUDE (row_num)
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM fct_trips