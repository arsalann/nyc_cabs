/* @bruin
name: tier_1.trips_historic
type: duckdb.sql
description: Store raw ingested data from Python ingestion table to persistent storage with normalized column names. Uses time-interval strategy for incremental processing based on pickup_time.
owner: nyc-taxi-team
tags:
  - tier_1
  - raw_data
  - time_series
materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_time
  time_granularity: timestamp
columns:
  - name: pickup_time
    type: TIMESTAMP
    description: Trip pickup timestamp
    nullable: false
  - name: dropoff_time
    type: TIMESTAMP
    description: Trip dropoff timestamp
    nullable: true
  - name: pickup_location_id
    type: INTEGER
    description: Taxi zone ID where trip was picked up
    nullable: true
  - name: dropoff_location_id
    type: INTEGER
    description: Taxi zone ID where trip was dropped off
    nullable: true
  - name: passenger_count
    type: INTEGER
    description: Number of passengers
    nullable: true
  - name: trip_distance
    type: DOUBLE
    description: Trip distance in miles
    nullable: true
  - name: fare_amount
    type: DOUBLE
    description: Fare amount in USD
    nullable: true
  - name: tip_amount
    type: DOUBLE
    description: Tip amount in USD
    nullable: true
  - name: total_amount
    type: DOUBLE
    description: Total trip amount in USD
    nullable: true
  - name: payment_type
    type: INTEGER
    description: Payment type code
    nullable: true
  - name: vendor_id
    type: INTEGER
    description: Vendor identifier
    nullable: true
  - name: rate_code_id
    type: INTEGER
    description: Rate code identifier
    nullable: true
  - name: store_and_fwd_flag
    type: VARCHAR
    description: Store and forward flag
    nullable: true
  - name: extra
    type: DOUBLE
    description: Extra charges
    nullable: true
  - name: mta_tax
    type: DOUBLE
    description: MTA tax amount
    nullable: true
  - name: tolls_amount
    type: DOUBLE
    description: Tolls amount
    nullable: true
  - name: improvement_surcharge
    type: DOUBLE
    description: Improvement surcharge
    nullable: true
  - name: congestion_surcharge
    type: DOUBLE
    description: Congestion surcharge
    nullable: true
  - name: airport_fee
    type: DOUBLE
    description: Airport fee
    nullable: true
  - name: taxi_type
    type: VARCHAR
    description: Type of taxi (yellow or green)
    nullable: false
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when data was extracted from source
    nullable: false
  - name: loaded_at
    type: TIMESTAMP
    description: Timestamp when data was loaded into tier_1
    nullable: false
@bruin */

SELECT
    COALESCE(
        CAST(tpep_pickup_datetime AS TIMESTAMP),
        CAST(lpep_pickup_datetime AS TIMESTAMP)
    ) AS pickup_time,
    COALESCE(
        CAST(tpep_dropoff_datetime AS TIMESTAMP),
        CAST(lpep_dropoff_datetime AS TIMESTAMP)
    ) AS dropoff_time,
    pu_location_id AS pickup_location_id,
    do_location_id AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    payment_type,
    vendor_id,
    ratecode_id AS rate_code_id,
    store_and_fwd_flag,
    extra,
    mta_tax,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    taxi_type,
    extracted_at,
    CURRENT_TIMESTAMP AS loaded_at
FROM tier_1.ingest_trips
WHERE COALESCE(
        CAST(tpep_pickup_datetime AS TIMESTAMP),
        CAST(lpep_pickup_datetime AS TIMESTAMP)
    ) >= CAST('{{ start_datetime }}' AS TIMESTAMP)
  AND COALESCE(
        CAST(tpep_pickup_datetime AS TIMESTAMP),
        CAST(lpep_pickup_datetime AS TIMESTAMP)
    ) < CAST('{{ end_datetime }}' AS TIMESTAMP)

