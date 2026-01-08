/* @bruin
name: tier_1.trips
uri: neptune.tier_1.trips
type: duckdb.sql
description: |
  Stores raw ingested taxi trip data from the Python ingestion table.
  Reads all columns from ingestion.ingest_trips_python and normalizes column names to match tier_1 schema.
  This is the first persistent storage layer for raw trip data.
  
  Column normalization:
  - vendor_id -> vendorid
  - ratecode_id -> ratecodeid
  - pu_location_id -> pulocationid
  - do_location_id -> dolocationid
  Sample query:
  ```sql
  SELECT *
  FROM tier_1.trips
  WHERE 1=1
    AND tpep_pickup_datetime >= '2022-01-01'
  LIMIT 10
  ```

owner: data-engineering
tags:
  - tier-1
  - nyc-taxi
  - raw-data

depends:
  - ingestion.ingest_trips_python

materialization:
  type: table
  strategy: time_interval
  incremental_key: tpep_pickup_datetime
  time_granularity: timestamp

interval_modifiers:
  start: -3d
  end: 1d

columns:
  - name: vendorid
    type: INTEGER
    description: A code indicating the TPEP provider that provided the record (1=Creative Mobile Technologies, LLC; 2=VeriFone Inc.)
  - name: tpep_pickup_datetime
    type: TIMESTAMP
    description: The date and time when the meter was engaged
  - name: tpep_dropoff_datetime
    type: TIMESTAMP
    description: The date and time when the meter was disengaged
  - name: passenger_count
    type: DOUBLE
    description: The number of passengers in the vehicle (entered by the driver)
  - name: trip_distance
    type: DOUBLE
    description: The elapsed trip distance in miles reported by the taximeter
  - name: ratecodeid
    type: DOUBLE
    description: The final rate code in effect at the end of the trip (1=Standard rate, 2=JFK, 3=Newark, 4=Nassau or Westchester, 5=Negotiated fare, 6=Group ride)
  - name: store_and_fwd_flag
    type: VARCHAR
    description: This flag indicates whether the trip record was held in vehicle memory before sending to the vendor (Y=store and forward; N=not a store and forward trip)
  - name: pulocationid
    type: INTEGER
    description: TLC Taxi Zone in which the taximeter was engaged
  - name: dolocationid
    type: INTEGER
    description: TLC Taxi Zone in which the taximeter was disengaged
  - name: payment_type
    type: DOUBLE
    description: A numeric code signifying how the passenger paid for the trip (1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided trip)
  - name: fare_amount
    type: DOUBLE
    description: The time-and-distance fare calculated by the meter
  - name: extra
    type: DOUBLE
    description: Miscellaneous extras and surcharges (currently includes $0.50 rush hour and overnight charges)
  - name: mta_tax
    type: DOUBLE
    description: $0.50 MTA tax that is automatically triggered based on the metered rate in use
  - name: tip_amount
    type: DOUBLE
    description: Tip amount (automatically populated for credit card tips, manually entered for cash tips)
  - name: tolls_amount
    type: DOUBLE
    description: Total amount of all tolls paid in trip
  - name: improvement_surcharge
    type: DOUBLE
    description: $0.30 improvement surcharge assessed on hailed trips at the flag drop
  - name: total_amount
    type: DOUBLE
    description: The total amount charged to passengers (does not include cash tips)
  - name: congestion_surcharge
    type: DOUBLE
    description: Congestion surcharge for trips that start, end or pass through the Manhattan Central Business District
  - name: airport_fee
    type: DOUBLE
    description: Airport fee for trips that start or end at an airport
  - name: taxi_type
    type: VARCHAR
    description: Type of taxi (yellow or green)

@bruin */

WITH raw_trips AS (
      {# 
        Read raw trip data from the Python ingestion table and normalize column names
        
        Purpose:
        - This is the first persistent storage layer for raw trip data
        - Moves data from Python ingestion table to permanent tier_1 table
        - Normalizes column names from parquet format (snake_case with underscores) to tier_1 schema (lowercase without underscores)
        - Parquet files use: vendor_id, pu_location_id, do_location_id, ratecode_id
        - Tier_1 expects: vendorid, pulocationid, dolocationid, ratecodeid
        
        Interval Modifiers:
        - start_datetime and end_datetime are provided by Bruin based on interval_modifiers config
        - interval_modifiers: start: -3d, end: 1d means process last 3 days (allows for late-arriving data)
        - Filtering by tpep_pickup_datetime ensures we only process trips in the specified time window
        
        Data Quality:
        - tpep_pickup_datetime IS NOT NULL: Ensures we only store trips with valid pickup times
        - This is critical because tpep_pickup_datetime is used as the incremental_key
        
        Why read from ingestion.ingest_trips_python:
        - The Python ingestion asset downloads parquet files and materializes them as-is (raw format)
        - This tier_1 asset normalizes column names and persists with incremental strategy for efficient updates
      #}
  SELECT
    vendor_id AS vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecode_id AS ratecodeid,
    store_and_fwd_flag,
    pu_location_id AS pulocationid,
    do_location_id AS dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    taxi_type,
      FROM ingestion.ingest_trips_python
  WHERE 1=1
    {# 
      Filter by date range
      - For incremental runs: use start_datetime/end_datetime (from interval modifiers)
      - For full-refresh: use start_date/end_date (from command line)
      - When using start_date/end_date, we need to include all data from those months
        since ingestion loads full months, we should filter by month boundaries
    #}
    {% if start_datetime is defined and end_datetime is defined %}
      {# Incremental run: use exact datetime range from interval modifiers #}
      AND tpep_pickup_datetime >= '{{ start_datetime }}'
      AND tpep_pickup_datetime < '{{ end_datetime }}'
    {% elif start_date is defined and end_date is defined %}
      {# Full-refresh: include all data from months that overlap with the date range #}
      {# Extract year-month from start_date and end_date to match ingestion logic #}
      {% set start_date_str = start_date | string %}
      {% set end_date_str = end_date | string %}
      {% set start_year_month = start_date_str[0:7] %}
      {% set end_year_month = end_date_str[0:7] %}
      AND DATE_TRUNC('month', tpep_pickup_datetime) >= DATE('{{ start_year_month }}-01')
      AND DATE_TRUNC('month', tpep_pickup_datetime) <= DATE('{{ end_year_month }}-01')
    {% endif %}
    {# Data quality: ensure pickup datetime exists (required for incremental processing) #}
    AND tpep_pickup_datetime IS NOT NULL
)

, final AS (
  {# 
    Final select with all columns
    - Simple passthrough to maintain all original data
    - No transformations at this tier - we preserve raw data as-is
  #}
  SELECT
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    taxi_type,
  FROM raw_trips
)

SELECT
  vendorid,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  ratecodeid,
  store_and_fwd_flag,
  pulocationid,
  dolocationid,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  airport_fee,
  taxi_type,
FROM final;

