WITH mart_revenue_breakdown AS (
    SELECT trip_type,
           pickup_date,
           SUM(fare_amount) AS fare_amount_total,
           SUM(COALESCE(tip_amount,0)) AS tip_amount_total,
           SUM(COALESCE(tolls_amount,0)) AS tolls_amount_total,
           SUM(COALESCE(congestion_surcharge,0)) AS surcharge_total,
           SUM(COALESCE(airport_fee,0)) AS airport_fee_total,
           SUM(total_amount) AS grand_total
           FROM {{ref('fct_trips')}}
           GROUP BY trip_type, pickup_date
)
SELECT * FROM mart_revenue_breakdown