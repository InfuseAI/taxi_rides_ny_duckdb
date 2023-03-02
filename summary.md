<details>
<summary>Comparison Summary</summary>

Table | Rows | Columns 
--- | --- | ---
fact_trips | 105219 (+0) | 27 (+0) 
dim_zones | 265 (+0) | 4 (+0) 
dm_monthly_zone_stats (+) | 5599 | 15 
dm_monthly_zone_revenue (!) | 5599 (+0) | 12 (-3)<br/>(deleted=3) 


</details>
<details>
<summary>Tables Summary (added=1, schema changed=1)</summary>
<blockquote>

<details>
<summary>fact_trips</summary>

Column | Type | Valid % | Distinct %
--- | --- | --- | ---
tripid | VARCHAR | 100.0% (+0.0%) | 99.99% (+0.0%) 
vendorid | INTEGER | 100.0% (+0.0%) | 0.0% (+0.0%) 
service_type | VARCHAR | 100.0% (+0.0%) | 0.0% (+0.0%) 
ratecodeid | INTEGER | 95.7% (+0.0%) | 0.01% (+0.0%) 
pickup_locationid | INTEGER | 100.0% (+0.0%) | 0.23% (+0.0%) 
pickup_borough | VARCHAR | 100.0% (+0.0%) | 0.01% (+0.0%) 
pickup_zone | VARCHAR | 100.0% (+0.0%) | 0.23% (+0.0%) 
dropoff_locationid | INTEGER | 100.0% (+0.0%) | 0.24% (+0.0%) 
dropoff_borough | VARCHAR | 100.0% (+0.0%) | 0.01% (+0.0%) 
dropoff_zone | VARCHAR | 100.0% (+0.0%) | 0.24% (+0.0%) 
pickup_datetime | TIMESTAMP | 100.0% (+0.0%) | 99.96% (+0.0%) 
dropoff_datetime | TIMESTAMP | 100.0% (+0.0%) | 99.88% (+0.0%) 
store_and_fwd_flag | VARCHAR | 95.7% (+0.0%) | 0.0% (+0.0%) 
passenger_count | INTEGER | 95.7% (+0.0%) | 0.01% (+0.0%) 
trip_distance | NUMERIC(18, 3) | 100.0% (+0.0%) | 2.32% (+0.0%) 
trip_type | INTEGER | 98.83% (+0.0%) | 0.0% (+0.0%) 
fare_amount | NUMERIC(18, 3) | 100.0% (+0.0%) | 2.3% (+0.0%) 
extra | NUMERIC(18, 3) | 100.0% (+0.0%) | 0.03% (+0.0%) 
mta_tax | NUMERIC(18, 3) | 100.0% (+0.0%) | 0.0% (+0.0%) 
tip_amount | NUMERIC(18, 3) | 100.0% (+0.0%) | 1.43% (+0.0%) 
tolls_amount | NUMERIC(18, 3) | 100.0% (+0.0%) | 0.11% (+0.0%) 
ehail_fee | NUMERIC(18, 3) | 95.68% (+0.0%) | 0.0% (+0.0%) 
improvement_surcharge | NUMERIC(18, 3) | 100.0% (+0.0%) | 0.0% (+0.0%) 
total_amount | NUMERIC(18, 3) | 100.0% (+0.0%) | 4.19% (+0.0%) 
payment_type | INTEGER | 98.83% (+0.0%) | 0.0% (+0.0%) 
payment_type_description | VARCHAR | 95.7% (+0.0%) | 0.0% (+0.0%) 
congestion_surcharge | NUMERIC(18, 3) | 95.7% (+0.0%) | 0.0% (+0.0%) 

</details>
<details>
<summary>dim_zones</summary>

Column | Type | Valid % | Distinct %
--- | --- | --- | ---
locationid | NUMERIC(18, 3) | 100.0% (+0.0%) | 100.0% (+0.0%) 
borough | VARCHAR | 100.0% (+0.0%) | 2.64% (+0.0%) 
zone | VARCHAR | 100.0% (+0.0%) | 98.87% (+0.0%) 
service_zone | VARCHAR | 100.0% (+0.0%) | 1.89% (+0.0%) 

</details>
<details>
<summary>dm_monthly_zone_stats (+)</summary>

Column | Type | Valid % | Distinct %
--- | --- | --- | ---
revenue_zone (+) | VARCHAR | 100.0% | 4.3% 
revenue_month (+) | DATE | 100.0% | 0.68% 
service_type (+) | VARCHAR | 100.0% | 0.04% 
revenue_monthly_fare (+) | NUMERIC(38, 3) | 100.0% | 60.15% 
revenue_monthly_extra (+) | NUMERIC(38, 3) | 100.0% | 9.16% 
revenue_monthly_mta_tax (+) | NUMERIC(38, 3) | 100.0% | 3.8% 
revenue_monthly_tip_amount (+) | NUMERIC(38, 3) | 100.0% | 43.76% 
revenue_monthly_tolls_amount (+) | NUMERIC(38, 3) | 100.0% | 4.48% 
revenue_monthly_ehail_fee (+) | NUMERIC(38, 3) | 68.41% | 0.03% 
revenue_monthly_improvement_surcharge (+) | NUMERIC(38, 3) | 100.0% | 3.79% 
revenue_monthly_total_amount (+) | NUMERIC(38, 3) | 100.0% | 74.51% 
revenue_monthly_congestion_surcharge (+) | NUMERIC(38, 3) | 76.12% | 5.02% 
total_monthly_trips (+) | BIGINT | 100.0% | 3.68% 
avg_montly_passenger_count (+) | DOUBLE_PRECISION | 76.12% | 19.76% 
avg_montly_trip_distance (+) | DOUBLE_PRECISION | 100.0% | 68.16% 

</details>
<details>
<summary>dm_monthly_zone_revenue (!)</summary>

Column | Type | Valid % | Distinct %
--- | --- | --- | ---
revenue_zone | VARCHAR | 100.0% (+0.0%) | 4.3% (+0.0%) 
revenue_month | DATE | 100.0% (+0.0%) | 0.68% (+0.0%) 
service_type | VARCHAR | 100.0% (+0.0%) | 0.04% (+0.0%) 
revenue_monthly_fare | NUMERIC(38, 3) | 100.0% (+0.0%) | 60.15% (+0.0%) 
revenue_monthly_extra | NUMERIC(38, 3) | 100.0% (+0.0%) | 9.16% (+0.0%) 
revenue_monthly_mta_tax | NUMERIC(38, 3) | 100.0% (+0.0%) | 3.8% (+0.0%) 
revenue_monthly_tip_amount | NUMERIC(38, 3) | 100.0% (+0.0%) | 43.76% (+0.0%) 
revenue_monthly_tolls_amount | NUMERIC(38, 3) | 100.0% (+0.0%) | 4.48% (+0.0%) 
revenue_monthly_ehail_fee | NUMERIC(38, 3) | 68.41% (+0.0%) | 0.03% (+0.0%) 
revenue_monthly_improvement_surcharge | NUMERIC(38, 3) | 100.0% (+0.0%) | 3.79% (+0.0%) 
revenue_monthly_total_amount | NUMERIC(38, 3) | 100.0% (+0.0%) | 74.51% (+0.0%) 
revenue_monthly_congestion_surcharge | NUMERIC(38, 3) | 76.12% (+0.0%) | 5.02% (+0.0%) 
total_monthly_trips (-) | ~~BIGINT~~ | - | - 
avg_montly_passenger_count (-) | ~~DOUBLE_PRECISION~~ | - | - 
avg_montly_trip_distance (-) | ~~DOUBLE_PRECISION~~ | - | - 

</details>
</blockquote></details>