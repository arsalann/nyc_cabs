/* @bruin
name: tier_2.trips_summary
type: duckdb.sql
description: Clean, deduplicate, and enrich trip data with location and payment type information. Uses time-interval strategy for incremental processing based on pickup_time.
owner: nyc-taxi-team
tags:
  - tier_2
  - cleaned_data
  - enriched_data
materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_time
  time_granularity: timestamp
columns:
  - name: pickup_time
    type: TIMESTAMP
    description: Trip pickup timestamp
    primary_key: true
    nullable: false
  - name: dropoff_time
    type: TIMESTAMP
    description: Trip dropoff timestamp
    primary_key: true
    nullable: false
  - name: pickup_location_id
    type: INTEGER
    description: Taxi zone ID where trip was picked up
    primary_key: true
    nullable: false
  - name: dropoff_location_id
    type: INTEGER
    description: Taxi zone ID where trip was dropped off
    primary_key: true
    nullable: false
  - name: taxi_type
    type: VARCHAR
    description: Type of taxi (yellow or green)
    primary_key: true
    nullable: false
  - name: pickup_borough
    type: VARCHAR
    description: Borough where trip was picked up
    nullable: true
  - name: pickup_zone
    type: VARCHAR
    description: Zone where trip was picked up
    nullable: true
  - name: pickup_service_zone
    type: VARCHAR
    description: Service zone where trip was picked up
    nullable: true
  - name: dropoff_borough
    type: VARCHAR
    description: Borough where trip was dropped off
    nullable: true
  - name: dropoff_zone
    type: VARCHAR
    description: Zone where trip was dropped off
    nullable: true
  - name: dropoff_service_zone
    type: VARCHAR
    description: Service zone where trip was dropped off
    nullable: true
  - name: payment_type_id
    type: INTEGER
    description: Payment type code
    nullable: true
  - name: payment_type_description
    type: VARCHAR
    description: Human-readable payment type description
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
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when data was extracted from source
    nullable: false
  - name: updated_at
    type: TIMESTAMP
    description: Timestamp when data was last updated in tier_2
    nullable: false
@bruin */

WITH ranked_trips AS (
    SELECT
        t.pickup_time,
        t.dropoff_time,
        t.pickup_location_id,
        t.dropoff_location_id,
        t.taxi_type,
        t.payment_type AS payment_type_id,
        t.passenger_count,
        t.trip_distance,
        t.fare_amount,
        t.tip_amount,
        t.total_amount,
        t.extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY
                t.pickup_time,
                t.dropoff_time,
                t.pickup_location_id,
                t.dropoff_location_id,
                t.taxi_type
            ORDER BY t.extracted_at DESC
        ) AS rn
    FROM tier_1.trips_historic t
    WHERE t.pickup_time >= CAST('{{ start_datetime }}' AS TIMESTAMP)
      AND t.pickup_time < CAST('{{ end_datetime }}' AS TIMESTAMP)
      AND t.pickup_time IS NOT NULL
      AND t.dropoff_time IS NOT NULL
      AND t.pickup_location_id IS NOT NULL
      AND t.dropoff_location_id IS NOT NULL
      AND t.taxi_type IS NOT NULL
)
SELECT
    rt.pickup_time,
    rt.dropoff_time,
    rt.pickup_location_id,
    rt.dropoff_location_id,
    rt.taxi_type,
    pickup_z.borough AS pickup_borough,
    pickup_z.zone AS pickup_zone,
    pickup_z.service_zone AS pickup_service_zone,
    dropoff_z.borough AS dropoff_borough,
    dropoff_z.zone AS dropoff_zone,
    dropoff_z.service_zone AS dropoff_service_zone,
    rt.payment_type_id,
    pt.payment_type_description,
    rt.passenger_count,
    rt.trip_distance,
    rt.fare_amount,
    rt.tip_amount,
    rt.total_amount,
    rt.extracted_at,
    CURRENT_TIMESTAMP AS updated_at
FROM ranked_trips rt
LEFT JOIN tier_1.taxi_zone_lookup pickup_z
    ON rt.pickup_location_id = pickup_z.location_id
LEFT JOIN tier_1.taxi_zone_lookup dropoff_z
    ON rt.dropoff_location_id = dropoff_z.location_id
LEFT JOIN tier_1.payment_lookup pt
    ON rt.payment_type_id = pt.payment_type_id
WHERE rt.rn = 1

