WITH mart_zone_performance AS (
    SELECT pu_location_id,
           b.zone,
           b.borough,
           COUNT(*) AS trip_count,
           SUM(total_amount) AS total_revenue,
           AVG(fare_amount) AS avg_fare,
           AVG(trip_distance) AS avg_distance
           FROM {{ref('fct_trips')}} a
           INNER JOIN {{ref('dim_taxi_zones')}} b
           ON a.pu_location_id = b.LocationID
           GROUP BY pu_location_id,zone,borough
)
SELECT * FROM mart_zone_performance