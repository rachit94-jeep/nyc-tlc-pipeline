WITH stg_yellow_trips AS (
    SELECT
    VendorID AS vendor_id,
    tpep_pickup_datetime AS pickup_datetime,      
    tpep_dropoff_datetime AS dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID AS rate_code_id,
    store_and_fwd_flag,
    PULocationID AS pu_location_id,
    DOLocationID AS do_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    trip_duration_minutes,
    avg_speed_mph,
    source_file,
    ingested_at_timestamp AS ingestion_timestamp,
    pickup_date,
    'yellow' AS trip_type
    FROM {{source('raw','raw_yellow_trips')}}
)
SELECT * FROM stg_yellow_trips