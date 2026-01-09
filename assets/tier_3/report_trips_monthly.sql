/* @bruin
name: tier_3.report_trips_monthly
type: duckdb.sql
description: Generate monthly summary reports aggregated by taxi type and month. Uses time-interval strategy with month_date as incremental key.
owner: nyc-taxi-team
tags:
  - tier_3
  - reports
  - aggregations
materialization:
  type: table
  strategy: time_interval
  incremental_key: month_date
  time_granularity: timestamp
columns:
  - name: taxi_type
    type: VARCHAR
    description: Type of taxi (yellow or green)
    primary_key: true
    nullable: false
  - name: month_date
    type: DATE
    description: First day of the month for this report
    primary_key: true
    nullable: false
  - name: total_trips
    type: INTEGER
    description: Total number of trips in the month
    nullable: false
  - name: total_passengers
    type: INTEGER
    description: Total number of passengers in the month
    nullable: true
  - name: avg_trip_distance
    type: DOUBLE
    description: Average trip distance in miles
    nullable: true
  - name: total_trip_distance
    type: DOUBLE
    description: Total trip distance in miles
    nullable: true
  - name: avg_fare_amount
    type: DOUBLE
    description: Average fare amount in USD
    nullable: true
  - name: total_fare_amount
    type: DOUBLE
    description: Total fare amount in USD
    nullable: true
  - name: avg_tip_amount
    type: DOUBLE
    description: Average tip amount in USD
    nullable: true
  - name: total_tip_amount
    type: DOUBLE
    description: Total tip amount in USD
    nullable: true
  - name: avg_total_amount
    type: DOUBLE
    description: Average total trip amount in USD
    nullable: true
  - name: total_amount
    type: DOUBLE
    description: Total trip amount in USD
    nullable: true
  - name: latest_extracted_at
    type: TIMESTAMP
    description: Latest extraction timestamp for this month
    nullable: false
  - name: updated_at
    type: TIMESTAMP
    description: Timestamp when report was last updated
    nullable: false
@bruin */

SELECT
    taxi_type,
    DATE_TRUNC('month', pickup_time)::DATE AS month_date,
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    AVG(trip_distance) AS avg_trip_distance,
    SUM(trip_distance) AS total_trip_distance,
    AVG(fare_amount) AS avg_fare_amount,
    SUM(fare_amount) AS total_fare_amount,
    AVG(tip_amount) AS avg_tip_amount,
    SUM(tip_amount) AS total_tip_amount,
    AVG(total_amount) AS avg_total_amount,
    SUM(total_amount) AS total_amount,
    MAX(extracted_at) AS latest_extracted_at,
    CURRENT_TIMESTAMP AS updated_at
FROM tier_2.trips_summary
WHERE DATE_TRUNC('month', pickup_time)::DATE >= DATE_TRUNC('month', CAST('{{ start_datetime }}' AS TIMESTAMP))::DATE
  AND DATE_TRUNC('month', pickup_time)::DATE < DATE_TRUNC('month', CAST('{{ end_datetime }}' AS TIMESTAMP))::DATE
GROUP BY
    taxi_type,
    DATE_TRUNC('month', pickup_time)::DATE

