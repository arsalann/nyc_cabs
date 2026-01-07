
## etl design

### ingestion layer
  - local duckdb
  - an in-memory table that only contains the recently ingested data 
  - uses bruin's interval start/end dates to ingest data for the specified dates
    - data from nyc website is provided at month level, concat the interval dates to months and get data accordingly
      - if the interval is 2025-01-01 to 2025-01-02 should ingest only 2025 month 01
      - if the interval is 2025-01-01 to 2025-02-01 should ingest only 2025 months 01 and 02
  - the taxi type is a bruin custom variable defined at the pipeline level and the default is set to `yellow`
  - ingestion asset:
    - should be a sql asset that queries the data from source website and reads the parquet file to memory (use HTTP Parquet Import + in-memory duckdb table)
      - e.g. `create or replace table nyc as from "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-03.parquet"`
    - a jinja macro is needed to properly loop through the provided months and taxi_type variables to generate the query accordingly
    - inject the taxi_type variable from bruin custom variables
  - lookup table should be ingested every time the pipeline runs and replaces the old lookup table


### transformation layer
  - local duckdb
  - raw ingested data is inserted into `tier_1.trips` table
    - reads from the in-memory table and insert into tier 1 table
  - transformed & cleaned data is stored in `tier_2.trips_summary` table
    - deduplicate
    - select only necessary columns
    - join with lookup table
  - final reports are stored in `tier_3.report_trips_monthly` tables and views

## data source

the raw taxi trips data is in: https://d37ci6vzurychx.cloudfront.net/trip-data/

- data is grouped by taxi type and month.
- there are files for each taxi type `yellow` and `green`
- the format of the file name is: `<taxi_type>_tripdata_<year>-<month>.parquet`

e.g. `https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-03.parquet`


lookup table: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv


## final reports
- monthly summary:
  - indicators:
    - trip duration
    - tip amount
    - total amount
    - total number of trips
  - schema:
    - taxi_type
    - month_date
    - trip_duration_avg
    - trip_duration_total
    - total_amount_avg
    - total_amount_total
    - tip_amount_avg
    - tip_amount_total
    - total_trips