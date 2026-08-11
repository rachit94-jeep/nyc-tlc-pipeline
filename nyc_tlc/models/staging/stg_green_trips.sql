WITH stg_green_trips AS (
    SELECT VendorID AS vendor_id,
        lpep_pickup_datetime AS pickup_datetime,
        lpep_dropoff_datetime AS dropoff_datetime,
        store_and_fwd_flag,
        RatecodeID AS rate_code_id,
        PULocationID AS pu_location_id,
        DOLocationID AS do_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        trip_type AS green_trip_type,
        congestion_surcharge,
        cbd_congestion_fee,
        trip_duration_minutes,
        avg_speed_mph,
        source_file,
        ingested_at_timestamp AS ingestion_timestamp,
        pickup_date,
        'green' AS trip_type
    FROM {{source('raw', 'raw_green_trips')}}
)
SELECT *
FROM stg_green_trips