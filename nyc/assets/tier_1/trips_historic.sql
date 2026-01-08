/* @bruin
name: tier_1.trips_historic
uri: neptune.tier_1.trips_historic
type: duckdb.sql
description: |
  Stores raw ingested taxi trip data from the Python ingestion table.
  Reads all columns from ingestion.ingest_trips_python and normalizes column names to match tier_1 schema.
  This is the first persistent storage layer for raw trip data.
  Standardizes columns names.
  Filter by date range using month-level truncation
  - start_datetime and end_datetime are always provided by Bruin for time_interval strategy
  - Truncate interval dates to month level to match ingestion logic (ingestion loads full months)
  - The time_interval materialization strategy already handles deleting data in the interval range

owner: data-engineering

depends:
  - ingestion.ingest_trips_python

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_time
  time_granularity: timestamp

columns:
  - name: vendorid
    type: INTEGER
    description: A code indicating the TPEP provider that provided the record (1=Creative Mobile Technologies, LLC; 2=VeriFone Inc.)
  - name: pickup_time
    type: TIMESTAMP
    description: The date and time when the meter was engaged
  - name: dropoff_time
    type: TIMESTAMP
    description: The date and time when the meter was disengaged
    checks:
      - expression: dropoff_time > pickup_time
        description: Dropoff time must be after pickup time
  - name: passenger_count
    type: DOUBLE
    description: The number of passengers in the vehicle (entered by the driver)
  - name: trip_distance
    type: DOUBLE
    description: The elapsed trip distance in miles reported by the taximeter
    checks:
      - expression: trip_distance >= 0
        description: Trip distance must be non-negative
  - name: store_and_fwd_flag
    type: VARCHAR
    description: This flag indicates whether the trip record was held in vehicle memory before sending to the vendor (Y=store and forward; N=not a store and forward trip)
  - name: pickup_location_id
    type: INTEGER
    description: TLC Taxi Zone in which the taximeter was engaged
  - name: dropoff_location_id
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
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when the data was extracted from the source
  - name: loaded_at
    type: TIMESTAMP
    description: Timestamp when the data was loaded into tier_1

quality_checks:
  - name: dropoff_after_pickup
    query: SELECT COUNT(*) FROM tier_1.trips_historic WHERE dropoff_time <= pickup_time
    expectation: 0
    description: Validates that dropoff_time is after pickup_time (trip cannot end before it starts)
  - name: non_negative_trip_distance
    query: SELECT COUNT(*) FROM tier_1.trips_historic WHERE trip_distance < 0
    expectation: 0
    description: Ensures trip_distance is non-negative

@bruin */

SELECT
  vendor_id AS vendorid,
  CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_time,
  CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_time,
  passenger_count,
  trip_distance,
  ratecode_id,
  store_and_fwd_flag,
  pu_location_id AS pickup_location_id,
  do_location_id AS dropoff_location_id,
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
  extracted_at,
  CURRENT_TIMESTAMP AS loaded_at,
FROM ingestion.ingest_trips_python
WHERE 1=1
  AND DATE_TRUNC('month', CAST(tpep_pickup_datetime AS TIMESTAMP)) BETWEEN DATE_TRUNC('month', CAST('{{ start_datetime }}' AS TIMESTAMP)) AND DATE_TRUNC('month', CAST('{{ end_datetime }}' AS TIMESTAMP))
  AND tpep_pickup_datetime IS NOT NULL
