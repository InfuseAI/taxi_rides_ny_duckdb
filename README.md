## Maximizing Confidence in Your Data Model Changes with dbt and PipeRider

This project was created to accompany the PipeRider + dbt workshop on improving your code review for dbt projects.

This workshop project will run you through the following steps:

### PipeRider Walkthrough

- Initialize PipeRider inside a dbt project
- Run PipeRider to create a data report
- Compare data reports
- Use a compare recipe
- Define dbt metrics and view in report
- Compare dbt metrics


## Prequisites

- Ideally, you have completed the [Week 4 module on Analytics Engineering](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_4_analytics_engineering) of the DataTalksClub [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)
- A basic understanding of [dbt](https://docs.getdbt.com/)
- Install, or update to, [DuckDB](https://duckdb.org/#quickinstall) 0.7.0

## Workshop Steps

### 1. Initial setup

1. Fork this repo
2. Clone your forked repo

	```bash
	git clone <your-repo-url>
	cd taxi_rides_ny_duckdb
	```

3. Download the DuckDB database file

	```bash
	wget https://dtc-workshop.s3.ap-northeast-1.amazonaws.com/nyc_taxi.duckdb
	``` 
4. Set up a new venv

	```bash
	python -m venv ./venv
	source ./venv/bin/activate
	```
5. Update pip and install the neccessary dbt packages and PipeRider

	```bash
	pip install -U pip
	pip install dbt-core dbt-duckdb 'piperider[duckdb]'
	```
6. Create a new branch to work on

	```bash
	git switch -c data-modeling
	```
	
7. Install dbt deps and build dbt models

	```bash
	dbt deps
	dbt build
	```
	
8. Initialize PipeRider

	```bash
	piperider init
	```
	
9. Check PipeRider settings

	```bash
	piperider diagnose
	```
	
### 2. Run PipeRider and data model changes
	
1. Run PipeRider

	```bash
	piperider run
	```
	
	PipeRider will profile the database and output the path to your data report, e.g.
	
	```
	Generating reports from: /project/path/.piperider/outputs/latest/run.json
	Report generated in /project/path/.piperider/outputs/latest/index.html
	```
	
	View the HTML report to see the full statistical report of your data source.
	
2. Make data model changes (move statistics to their own model)

	a. Create a new model `models/core/dm_monthly_zone_statistics.sql`

	```sql
	{{ config(materialized='table') }}
	
	with trips_data as (
	select * from {{ ref('fact_trips') }}
	)
	select
	-- Reveneue grouping
	pickup_zone as revenue_zone,
	date_trunc('month', pickup_datetime) as revenue_month,
	--Note: For BQ use instead: date_trunc(pickup_datetime, month) as revenue_month,
	
	service_type,
	
	-- Additional calculations
	count(tripid) as total_monthly_trips,
	avg(passenger_count) as avg_montly_passenger_count,
	avg(trip_distance) as avg_montly_trip_distance
	
	from trips_data
	group by 1,2,3
	```

	b. Comment out lines 26-28 of `models/core/dm_monthly_zone_revnue.sql`
	
	```sql
	-- Additional calculations
	-- count(tripid) as total_monthly_trips,
	-- avg(passenger_count) as avg_montly_passenger_count,
	-- avg(trip_distance) as avg_montly_trip_distance
	```

3. Rebuild the dbt models

	```bash
	dbt build
	```
	
4. Run PipeRider again to generate the second data report with the new models

	```bash
	piperider run
	```
	
5. Use the `compare-reports` function to compare the data profile reports

	```bash
	piperider compare-reports --last
	```
	
	The `compare-reports` outputs two files:
	- Comparison report: An HTML report comparing the two data profiles
	- Comparison summary: A Markdown file with a summary of changes.

	The comparison summary markdown is used to insert into a pull request (PR) comment in a later step.
	
6. Commit your changes and push your branch

	```bash
	git add .
	git commit -m "Added statistics model, updated revenue model"
	git push origin datamodeling
	```
	
7. Create a pull request.

	a. Visit your repo on GitHub and clck `Compare & pull request`
	
	b. Copy the contents of the comparison summary Markdown file into your pull request comment box
	
	c. Click `preview` to see how the comparison looks 
	
	d. Click `Create pull request` to submit your changes
	
	
### 3. PipeRider Compare Recipe

In the above example we used the `compare-reports` command. PipeRider also has a separate `compare` command that uses the concept of compare 'recipes'. Recipes are a powerful way to define the specifics of how the compare will run, such as:

- The branches to compare
- The datasource/target to compare
- The dbt commands to run prior to the compare

When PipeRider is initialized a default compare recipe is created. For our project this looks like:

```yaml
base:
  branch: main
  dbt:
    commands:
    - dbt deps
    - dbt build
  piperider:
    command: piperider run
target:
  branch: data-modeling
  dbt:
    commands:
    - dbt deps
    - dbt build
  piperider:
    command: piperider run
```

Run the following command to run the above recipe:

```bash
piperider compare
```

As per the recipe, PipeRider will **automatically** do the following:

1. Check out the `main` branch
2. Build the models
3. Run PipeRider
4. Check out the `data-modeling` branch
4. Build the models
5. Run PipeRider
6. Compare the data reports of `main` and `data-modeling`
7. Output the compare report and summary



### 4. dbt-defined Metrics

PipeRider also supports profiling [dbt-defined metrics](https://docs.getdbt.com/docs/build/metrics). PipeRider will query dbt metrics and include them in the HTML report.

1. Edit `models/core/schema.yml` and add the following code:

	```yaml
	metrics:
	  - name: average_distance
	    label: Average Distance
	    model: ref('fact_trips')
	    description: "The average trip distance"
	
	    calculation_method: average
	    expression: trip_distance
	
	    timestamp: pickup_datetime
	    time_grains: [month, quarter, year]
	
	    tags:
	    - piperider
	```

	**Important:** Don't forget the `piperider` tag, this is how PipeRider is able to find and query your project metrics

	This defines a new metric on the `fact_trips` table that calculates the average `trip_distance` distance at the `time_grains` of month, quarter, and year.

2. Run dbt compile

	```bash
	dbt compile
	```
	
	**Note:** As we’re only adding metrics, it is not necessary to build the models again with `dbt build`.
	
3. Run PipeRider to generate a new report.

	```bash
	piperider run
	```

4. Check the PipeRider report and click the `metrics` tab to view the metrics graph.
    

### 5. Filter and compare dbt metrics

PipeRider also supports comparing metrics between runs. The comarison is visualized in the Comparison report and included in the comparison summary Markdown. 

1. Edit `models/core/schema.yml` again and add the following `filter` to the metric definition:

	```yaml
	filters:
	  - field: pickup_borough
	    operator: '='
	    value: "'Manhattan'"
	  - field: dropoff_borough
	    operator: '='
	    value: "'Manhattan'"
	```

	The filter will modify the metric to only apply to rows that meet the defined conditions - In this case, that the pickup and dropoff borough should be Manhattan.

	Your modified metric definition should now look like this:
	
	```yaml
	metrics:
	  - name: average_distance
	    label: Average Distance
	    model: ref('fact_trips')
	    description: "The average trip distance"

	    calculation_method: average
	    expression: trip_distance

	    timestamp: pickup_datetime
	    time_grains: [month, quarter, year]

	    filters:
	      - field: pickup_borough
	        operator: '='
	        value: "'Manhattan'"
	      - field: dropoff_borough
	        operator: '='
	        value: "'Manhattan'"

	    tags:
	    - piperider
	```

2. Compile your dbt project again.

	```bash
	dbt compile
	```

3. Run PipeRider again to generate a report with the new, filttered, metrics.

	```bash
	piperider run
	```

4. Lastly, run the PipeRider `compare-reports` command to create a comparison report that will include the two, differently defined, metrics.

	```bash
	piperider compare-reports --last
	```

5. View the newly generatec comparison report to see how the metrics compare.


6. The comparison summary also contains a summary of the metric differences between reports.




## PipeRider resources:
- Learn more about PipeRider [in the docs](https://docs.piperider.io)
- Visit the PipeRider [homepage](https://piperider.io)
- Join the PipeRider [Discord](https://discord.gg/328QcXnkKD) for help and discussion
- Read the PipeRider [blog](https://blog.piperider.io) for articles about PipeRider