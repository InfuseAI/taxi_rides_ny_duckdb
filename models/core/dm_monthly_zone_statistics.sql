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
    -- Additional calculations
    COUNT(tripid) AS total_monthly_trips,
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1,
    2,
    3
