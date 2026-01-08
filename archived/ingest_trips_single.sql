/* @bruin
name: ingestion.ingest_trips_single
uri: neptune.ingestion.ingest_trips_single
type: duckdb.sql
description: |
  Ingests NYC taxi trip data from HTTP parquet file for a single month.
  Uses the interval start date's month to determine which file to ingest.
  This is useful for incremental processing where only one month is processed at a time.
  Sample query:
  ```sql
  SELECT *
  FROM ingestion.ingest_trips_single
  WHERE 1=1
  LIMIT 10
  ```

owner: data-engineering
tags:
  - ingestion
  - nyc-taxi
  - raw-data
  - single-month

materialization:
  type: table
  strategy: truncate+insert

@bruin */

{#
  Single Month Ingestion Logic:
  
  This asset ingests data for only the month specified in the interval start date.
  For example, if start_date is 2025-01-15, it will ingest 2025-01 data only.
  
  Why single month:
  - Simpler query structure (no UNION ALL needed)
  - Useful for incremental processing
  - Avoids parser errors with large UNION ALL queries
#}

-- Parse start_date to extract year and month
{% set start_date_str = start_date | string %}
{% set start_date_clean = start_date_str[:10] %}
{% set start_year = start_date_clean[0:4] | int %}
{% set start_month = start_date_clean[5:7] | int %}

-- Set taxi_type (default: 'yellow')
{% set taxi_type = 'yellow' %}

-- Read single month parquet file
SELECT
  *,
  '{{ taxi_type }}' AS taxi_type,
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/{{ taxi_type }}_tripdata_{{ start_year }}-{{ "%02d" | format(start_month) }}.parquet');

