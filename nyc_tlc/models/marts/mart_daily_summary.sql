WITH mart_daily_summary AS (
    SELECT pickup_date,
           trip_type,
           COUNT(*) AS trip_count,
           AVG(fare_amount) AS avg_fare_amount,
           AVG(trip_distance) AS avg_trip_distance,
           AVG(trip_duration_minutes) AS avg_trip_duration_minutes,
           SUM(total_amount) AS total_revenue
           FROM {{ ref('fct_trips') }}
           GROUP BY pickup_date,trip_type
)
SELECT * FROM mart_daily_summary