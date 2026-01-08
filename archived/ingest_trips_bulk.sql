/* @bruin
name: ingestion.ingest_trips_bulk
uri: neptune.ingestion.ingest_trips_bulk
type: duckdb.sql
description: |
  Ingests NYC taxi trip data from HTTP parquet files for multiple months using UNION ALL.
  Uses Bruin's interval start/end dates to determine which months to ingest.
  Generates a UNION ALL query that reads all parquet files for the date range.
  Sample query:
  ```sql
  SELECT *
  FROM ingestion.ingest_trips_bulk
  WHERE 1=1
  LIMIT 10
  ```

owner: data-engineering
tags:
  - ingestion
  - nyc-taxi
  - raw-data
  - bulk-ingestion

materialization:
  type: table
  strategy: truncate+insert

@bruin */

{#
  Date Range to Months Conversion Logic:
  
  NYC TLC provides data at monthly granularity (one parquet file per month).
  Bruin provides start_date and end_date (e.g., 2024-01-15 to 2024-03-10).
  We need to determine which monthly files to download.
  
  Strategy:
  - Extract year and month from start_date and end_date
  - Generate UNION ALL statements for all months between start and end (inclusive)
  - Example: 2024-01-15 to 2024-03-10 → UNION ALL for months 01, 02, 03
#}

-- Parse start_date and end_date to extract year and month components
{% set start_date_str = start_date | string %}
{% set end_date_str = end_date | string %}
{% set start_date_clean = start_date_str[:10] %}
{% set end_date_clean = end_date_str[:10] %}
{% set start_year = start_date_clean[0:4] | int %}
{% set start_month = start_date_clean[5:7] | int %}
{% set end_year = end_date_clean[0:4] | int %}
{% set end_month = end_date_clean[5:7] | int %}

-- Set taxi_type (default: 'yellow')
{% set taxi_type = 'yellow' %}

-- Generate UNION ALL query for all months in the date range
WITH parquet_union AS (
  {% if start_year == end_year %}
    {# Same year: generate months from start_month to end_month #}
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
    {# Different years: handle start_year, intermediate years, and end_year #}
    {% set first = true %}
    
    {# Part A: Remaining months in start_year (from start_month to December) #}
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
    
    {# Part B: Full years in between (if any) #}
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
    
    {# Part C: Months in end_year (from January to end_month) #}
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

