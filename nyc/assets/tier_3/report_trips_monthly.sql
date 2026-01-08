/* @bruin
name: tier_3.report_trips_monthly
uri: neptune.tier_3.report_trips_monthly
type: duckdb.sql
description: |
  Monthly summary report of NYC taxi trips aggregated by taxi type and month.
  Calculates average and total metrics for trip duration, total amount, and tip amount,
  as well as total trip count.
  Sample query:
  ```sql
  SELECT *
  FROM tier_3.report_trips_monthly
  WHERE 1=1
    AND taxi_type = 'yellow'
    AND month_date >= '2022-01-01'
  ORDER BY month_date DESC
  ```

owner: data-engineering
tags:
  - tier-3
  - nyc-taxi
  - reports
  - monthly-aggregation

depends:
  - tier_2.trips_summary

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
    description: First day of the month for which the report is generated
    primary_key: true
    nullable: false
  - name: trip_duration_avg
    type: DOUBLE
    description: Average trip duration in seconds for the month
  - name: trip_duration_total
    type: DOUBLE
    description: Total trip duration in seconds for the month
  - name: total_amount_avg
    type: DOUBLE
    description: Average total amount charged to passengers for the month
  - name: total_amount_total
    type: DOUBLE
    description: Total amount charged to passengers for the month
  - name: tip_amount_avg
    type: DOUBLE
    description: Average tip amount for the month
  - name: tip_amount_total
    type: DOUBLE
    description: Total tip amount for the month
  - name: total_trips
    type: BIGINT
    description: Total number of trips for the month

@bruin */

WITH trips_by_month AS (
  {# 
    Step 1: Extract month from pickup datetime and prepare data for aggregation
    
    Month Extraction:
    - DATE_TRUNC('month', tpep_pickup_datetime) truncates timestamp to first day of month
    - Example: 2022-03-15 14:30:00 → 2022-03-01 00:00:00
    - This creates a grouping key for monthly aggregation
    - month_date becomes the primary key component (one row per taxi_type per month)
    
    Data Quality Filters:
    - trip_duration_seconds IS NOT NULL: Required for average/total duration calculations
    - total_amount IS NOT NULL: Required for average/total amount calculations
    - tip_amount IS NOT NULL: Required for average/total tip calculations
    - Filtering out NULLs ensures accurate aggregations (NULL values would skew averages)
    
    Date Range Filtering:
    - start_datetime and end_datetime are always provided by Bruin for time_interval strategy
    - Truncate to month level to match tier_1/tier_2 logic (ingestion loads full months)
    - The time_interval materialization strategy already handles deleting data in the interval range
  #}
  SELECT
    taxi_type,
    DATE_TRUNC('month', tpep_pickup_datetime) AS month_date,
    trip_duration_seconds,
    total_amount,
    tip_amount,
  FROM tier_2.trips_summary
  WHERE 1=1
    {# 
      Filter by date range using month-level truncation
      - start_datetime and end_datetime are always provided by Bruin for time_interval strategy
      - Truncate interval dates to month level to match tier_1/tier_2 logic
      - Use BETWEEN to include all trips in the month range
    #}
    AND DATE_TRUNC('month', tpep_pickup_datetime) BETWEEN DATE_TRUNC('month', '{{ start_datetime }}') AND DATE_TRUNC('month', '{{ end_datetime }}')
    {# Data quality: ensure all metrics are present for accurate aggregations #}
    AND trip_duration_seconds IS NOT NULL
    AND total_amount IS NOT NULL
    AND tip_amount IS NOT NULL
)

, monthly_aggregates AS (
  {# 
    Step 2: Aggregate metrics by taxi type and month
    
    Aggregation Strategy:
    - GROUP BY taxi_type, month_date creates one row per taxi type per month
    - This matches the required schema: monthly summary by taxi type
    
    Metrics Calculated:
    1. Trip Duration:
       - trip_duration_avg: Average trip duration in seconds (for reporting)
       - trip_duration_total: Total trip duration in seconds (for analysis)
    
    2. Total Amount:
       - total_amount_avg: Average fare per trip (for reporting)
       - total_amount_total: Total revenue for the month (for analysis)
    
    3. Tip Amount:
       - tip_amount_avg: Average tip per trip (for reporting)
       - tip_amount_total: Total tips for the month (for analysis)
    
    4. Trip Count:
       - total_trips: Number of trips in the month (for reporting and analysis)
    
    Why both average and total:
    - Averages are useful for understanding typical trip characteristics
    - Totals are useful for understanding overall business metrics (revenue, volume)
    - Both metrics are required by the specification
    
    Aggregation Functions:
    - AVG(): Calculates mean value across all trips in the month
    - SUM(): Calculates total value across all trips in the month
    - COUNT(*): Counts number of trips (rows) in each group
  #}
  SELECT
    taxi_type,
    month_date,
    AVG(trip_duration_seconds) AS trip_duration_avg,
    SUM(trip_duration_seconds) AS trip_duration_total,
    AVG(total_amount) AS total_amount_avg,
    SUM(total_amount) AS total_amount_total,
    AVG(tip_amount) AS tip_amount_avg,
    SUM(tip_amount) AS tip_amount_total,
    COUNT(*) AS total_trips,
  FROM trips_by_month
  WHERE 1=1
  GROUP BY
    taxi_type,
    month_date
)

, final AS (
  {# 
    Step 3: Final select with all required columns
    
    Purpose:
    - Simple passthrough to maintain consistent CTE pattern
    - Ensures all columns are explicitly listed in final SELECT
    - Matches the schema defined in the @bruin config
    
    Column Order:
    - Matches the order specified in the requirements
    - Primary keys first (taxi_type, month_date)
    - Then metrics in logical groups (duration, amount, tip, count)
  #}
  SELECT
    taxi_type,
    month_date,
    trip_duration_avg,
    trip_duration_total,
    total_amount_avg,
    total_amount_total,
    tip_amount_avg,
    tip_amount_total,
    total_trips,
  FROM monthly_aggregates
)

SELECT
  taxi_type,
  month_date,
  trip_duration_avg,
  trip_duration_total,
  total_amount_avg,
  total_amount_total,
  tip_amount_avg,
  tip_amount_total,
  total_trips,
FROM final;

