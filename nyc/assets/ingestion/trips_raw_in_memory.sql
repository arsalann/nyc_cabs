/* @bruin
name: ingestion.trips_raw_in_memory
uri: neptune.ingestion.trips_raw_in_memory
type: duckdb.sql
description: |
  Ingests NYC taxi trip data from HTTP parquet files into an in-memory table.
  Uses Bruin's interval start/end dates to determine which months to ingest.
  Data is grouped by taxi type and month, and files are fetched from the NYC TLC website.
  The taxi_type is injected from pipeline-level custom variables (default: yellow).
  Sample query:
  ```sql
  SELECT *
  FROM ingestion.trips_raw_in_memory
  WHERE 1=1
  LIMIT 10
  ```

owner: data-engineering
tags:
  - ingestion
  - nyc-taxi
  - raw-data

materialization:
  type: table
  strategy: truncate+insert

@bruin */

{#
  Date Range to Months Conversion Logic:
  
  NYC TLC provides data at monthly granularity (one parquet file per month).
  Bruin provides start_date and end_date (e.g., 2022-01-15 to 2022-03-10).
  We need to determine which monthly files to download.
  
  Strategy:
  - Extract year and month from start_date and end_date
  - Generate list of all months between start and end (inclusive)
  - Example: 2022-01-15 to 2022-03-10 → [2022-01, 2022-02, 2022-03]
  
  Why inclusive of end month:
  - Even if end_date is early in the month (e.g., 2022-03-10), we still need March data
  - This ensures we don't miss any data that might be relevant
#}

-- Step 1: Parse start_date and end_date to extract year and month components
{% set start_date_str = start_date | string %}
{% set end_date_str = end_date | string %}
{% set start_date_clean = start_date_str[:10] %}
{% set end_date_clean = end_date_str[:10] %}
{% set start_year = start_date_clean[0:4] | int %}
{% set start_month = start_date_clean[5:7] | int %}
{% set end_year = end_date_clean[0:4] | int %}
{% set end_month = end_date_clean[5:7] | int %}

-- Step 2: Set taxi_type (default: 'yellow')
{% set taxi_type = 'yellow' %}

-- Step 3: Generate SQL query using CTE structure
WITH parquet_union AS (
{% if start_year == end_year %}
{% set first = true %}
{% for month in range(start_month, end_month + 1) %}
{% if first %}
SELECT
  *,
  '{{ taxi_type }}' AS taxi_type,
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/{{ taxi_type }}_tripdata_{{ start_year }}-{{ "%02d" | format(month) }}.parquet')
{% set first = false %}
{% else %}
UNION ALL
SELECT
  *,
  '{{ taxi_type }}' AS taxi_type,
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/{{ taxi_type }}_tripdata_{{ start_year }}-{{ "%02d" | format(month) }}.parquet')
{% endif %}
{% endfor %}
{% else %}
{% set first = true %}
{% for month in range(start_month, 13) %}
{% if first %}
SELECT
  *,
  '{{ taxi_type }}' AS taxi_type,
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/{{ taxi_type }}_tripdata_{{ start_year }}-{{ "%02d" | format(month) }}.parquet')
{% set first = false %}
{% else %}
UNION ALL
SELECT
  *,
  '{{ taxi_type }}' AS taxi_type,
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/{{ taxi_type }}_tripdata_{{ start_year }}-{{ "%02d" | format(month) }}.parquet')
{% endif %}
{% endfor %}
{% for year in range(start_year + 1, end_year) %}
{% for month in range(1, 13) %}
{% if first %}
SELECT
  *,
  '{{ taxi_type }}' AS taxi_type,
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/{{ taxi_type }}_tripdata_{{ year }}-{{ "%02d" | format(month) }}.parquet')
{% set first = false %}
{% else %}
UNION ALL
SELECT
  *,
  '{{ taxi_type }}' AS taxi_type,
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/{{ taxi_type }}_tripdata_{{ year }}-{{ "%02d" | format(month) }}.parquet')
{% endif %}
{% endfor %}
{% endfor %}
{% for month in range(1, end_month + 1) %}
{% if first %}
SELECT
  *,
  '{{ taxi_type }}' AS taxi_type,
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/{{ taxi_type }}_tripdata_{{ end_year }}-{{ "%02d" | format(month) }}.parquet')
{% set first = false %}
{% else %}
UNION ALL
SELECT
  *,
  '{{ taxi_type }}' AS taxi_type,
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/{{ taxi_type }}_tripdata_{{ end_year }}-{{ "%02d" | format(month) }}.parquet')
{% endif %}
{% endfor %}
{% endif %}
)

, final AS (
  SELECT
    *,
    taxi_type,
  FROM parquet_union
)

SELECT
  *,
  taxi_type,
FROM final;
