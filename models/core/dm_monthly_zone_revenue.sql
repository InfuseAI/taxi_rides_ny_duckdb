{{ config(
    materialized = 'table'
) }}

WITH trips_data AS (

    SELECT
        *
    FROM
        {{ ref('fact_trips') }}
)
SELECT
    -- Reveneue grouping
    pickup_zone AS revenue_zone,
    DATE_TRUNC(
        'month',
        pickup_datetime
    ) AS revenue_month,
    --Note: For BQ use instead: date_trunc(pickup_datetime, month) as revenue_month,
    service_type,
    -- Revenue calculation
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(ehail_fee) AS revenue_monthly_ehail_fee,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge -- Additional calculations
    --count(tripid) as total_monthly_trips,
    --avg(passenger_count) as avg_montly_passenger_count,
    --avg(trip_distance) as avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1,
    2,
    3
