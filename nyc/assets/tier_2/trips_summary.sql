/* @bruin
name: tier_2.trips_summary
uri: neptune.tier_2.trips_summary
type: duckdb.sql
description: |
  Transforms and cleans raw trip data from tier_1.
  Deduplicates trips, selects necessary columns, and joins with the taxi zone lookup table
  to enrich data with borough and zone names.
  Sample query:
  ```sql
  SELECT *
  FROM tier_2.trips_summary
  WHERE 1=1
    AND pickup_borough = 'Manhattan'
  LIMIT 10
  ```

owner: data-engineering
tags:
  - tier-2
  - nyc-taxi
  - cleaned-data

depends:
  - tier_1.trips
  - ingestion.taxi_zone_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: tpep_pickup_datetime
  time_granularity: timestamp

interval_modifiers:
  start: -3d
  end: 1d

columns:
  - name: tpep_pickup_datetime
    type: TIMESTAMP
    description: The date and time when the meter was engaged
    primary_key: true
    nullable: false
  - name: tpep_dropoff_datetime
    type: TIMESTAMP
    description: The date and time when the meter was disengaged
    primary_key: true
    nullable: false
  - name: pulocationid
    type: INTEGER
    description: TLC Taxi Zone in which the taximeter was engaged
    primary_key: true
    nullable: false
  - name: dolocationid
    type: INTEGER
    description: TLC Taxi Zone in which the taximeter was disengaged
    primary_key: true
    nullable: false
  - name: taxi_type
    type: VARCHAR
    description: Type of taxi (yellow or green)
    primary_key: true
    nullable: false
  - name: trip_distance
    type: DOUBLE
    description: The elapsed trip distance in miles reported by the taximeter
  - name: passenger_count
    type: DOUBLE
    description: The number of passengers in the vehicle
  - name: fare_amount
    type: DOUBLE
    description: The time-and-distance fare calculated by the meter
  - name: tip_amount
    type: DOUBLE
    description: Tip amount (automatically populated for credit card tips, manually entered for cash tips)
  - name: total_amount
    type: DOUBLE
    description: The total amount charged to passengers (does not include cash tips)
  - name: pickup_borough
    type: VARCHAR
    description: Borough name where the pickup occurred
  - name: pickup_zone
    type: VARCHAR
    description: Zone name where the pickup occurred
  - name: dropoff_borough
    type: VARCHAR
    description: Borough name where the dropoff occurred
  - name: dropoff_zone
    type: VARCHAR
    description: Zone name where the dropoff occurred
  - name: trip_duration_seconds
    type: DOUBLE
    description: Calculated trip duration in seconds (dropoff time - pickup time)

@bruin */

WITH raw_trips AS (
  {# 
    Step 1: Select necessary columns from tier_1 and apply data quality filters
    
    Purpose:
    - This tier focuses on cleaned, deduplicated, and enriched data
    - We only select columns needed for downstream processing and reporting
    - Removes columns like vendorid, ratecodeid that aren't needed for summaries
    
    Data Quality Filters:
    - All primary key columns must be NOT NULL (required for merge strategy)
    - tpep_pickup_datetime: Required for incremental processing and time-based filtering
    - tpep_dropoff_datetime: Required for trip duration calculation
    - pulocationid and dolocationid: Required for location enrichment and deduplication
    - taxi_type: Required for grouping and filtering by taxi type
    
    Why filter by interval modifiers:
    - Only process data in the specified time window (last 3 days by default)
    - This makes incremental updates efficient
  #}
  SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    pulocationid,
    dolocationid,
    taxi_type,
    trip_distance,
    passenger_count,
    fare_amount,
    tip_amount,
    total_amount,
  FROM tier_1.trips
  WHERE 1=1
    {# 
      Filter by date range
      - For incremental runs: use start_datetime/end_datetime (from interval modifiers)
      - For full-refresh: use start_date/end_date (from command line)
      - When using start_date/end_date, filter by month boundaries to match tier_1 logic
    #}
    {% if start_datetime is defined and end_datetime is defined %}
      {# Incremental run: use exact datetime range from interval modifiers #}
      AND tpep_pickup_datetime >= '{{ start_datetime }}'
      AND tpep_pickup_datetime < '{{ end_datetime }}'
    {% elif start_date is defined and end_date is defined %}
      {# Full-refresh: include all data from months that overlap with the date range #}
      {% set start_date_str = start_date | string %}
      {% set end_date_str = end_date | string %}
      {% set start_year_month = start_date_str[0:7] %}
      {% set end_year_month = end_date_str[0:7] %}
      AND DATE_TRUNC('month', tpep_pickup_datetime) >= DATE('{{ start_year_month }}-01')
      AND DATE_TRUNC('month', tpep_pickup_datetime) <= DATE('{{ end_year_month }}-01')
    {% endif %}
    {# Data quality: ensure all required fields are present #}
    AND tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND pulocationid IS NOT NULL
    AND dolocationid IS NOT NULL
    AND taxi_type IS NOT NULL
)

, deduplicated_trips AS (
  {# 
    Step 2: Deduplicate trips using window function
    
    Deduplication Strategy:
    - Composite key: (tpep_pickup_datetime, tpep_dropoff_datetime, pulocationid, dolocationid, taxi_type)
    - This combination uniquely identifies a trip
    - If the same trip appears multiple times (data quality issue), we keep the most recent
    
    Why ROW_NUMBER() window function:
    - PARTITION BY groups records with the same composite key
    - ORDER BY tpep_pickup_datetime DESC ensures most recent record gets rn=1
    - This handles edge cases where duplicate records might exist in source data
    
    Why keep most recent:
    - If a trip record was updated/corrected, the most recent version is likely the most accurate
    - This is a conservative approach to data quality
  #}
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY tpep_pickup_datetime, tpep_dropoff_datetime, pulocationid, dolocationid, taxi_type
      ORDER BY tpep_pickup_datetime DESC
    ) AS rn,
  FROM raw_trips
)

, cleaned_trips AS (
  {# 
    Step 3: Filter to keep only deduplicated records and calculate trip duration
    
    Deduplication Filter:
    - rn = 1 keeps only the first record (most recent) for each unique trip
    - This removes duplicates identified in the previous step
    
    Trip Duration Calculation:
    - EXTRACT(EPOCH FROM ...) converts the time difference to seconds
    - EPOCH extracts Unix timestamp (seconds since 1970-01-01)
    - Subtracting pickup from dropoff gives duration in seconds
    - This metric is needed for monthly reports (average trip duration)
    
    Why calculate here:
    - Trip duration is a derived metric, not in source data
    - Calculating once here avoids recalculating in downstream queries
    - Stored for use in tier_3 monthly reports
  #}
  SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    pulocationid,
    dolocationid,
    taxi_type,
    trip_distance,
    passenger_count,
    fare_amount,
    tip_amount,
    total_amount,
    {# Calculate trip duration: dropoff time - pickup time, converted to seconds #}
    EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) AS trip_duration_seconds,
  FROM deduplicated_trips
  WHERE 1=1
    {# Keep only the first (most recent) record for each unique trip #}
    AND rn = 1
)

, trips_with_lookup AS (
  {# 
    Step 4: Enrich trips with pickup location information
    
    Lookup Join Strategy:
    - LEFT JOIN ensures we keep all trips even if location ID doesn't exist in lookup table
    - Some location IDs might be invalid or missing from the lookup table
    - LEFT JOIN preserves data integrity - we don't lose trips due to missing lookup data
    
    Why separate pickup and dropoff joins:
    - We need to join the lookup table twice (once for pickup, once for dropoff)
    - Doing them separately makes the query clearer and easier to maintain
    - Allows us to alias the lookup table differently for each join
    
    Enrichment:
    - Adds human-readable Borough and Zone names for pickup location
    - Makes data more accessible for analysis and reporting
  #}
  SELECT
    ct.*,
    pickup_lookup.Borough AS pickup_borough,
    pickup_lookup.Zone AS pickup_zone,
  FROM cleaned_trips AS ct
  LEFT JOIN ingestion.taxi_zone_lookup AS pickup_lookup
    ON ct.pulocationid = pickup_lookup.LocationID
)

, final AS (
  {# 
    Step 5: Enrich trips with dropoff location information
    
    Second Lookup Join:
    - Similar to pickup join, but for dropoff location
    - Uses the same lookup table but with different alias (dropoff_lookup)
    - Adds Borough and Zone names for dropoff location
    
    Final Result:
    - Contains all trip data with both pickup and dropoff location enrichment
    - Ready for use in tier_3 monthly reports
    - All primary key columns are present and non-null (required for merge strategy)
  #}
  SELECT
    twl.tpep_pickup_datetime,
    twl.tpep_dropoff_datetime,
    twl.pulocationid,
    twl.dolocationid,
    twl.taxi_type,
    twl.trip_distance,
    twl.passenger_count,
    twl.fare_amount,
    twl.tip_amount,
    twl.total_amount,
    twl.pickup_borough,
    twl.pickup_zone,
    dropoff_lookup.Borough AS dropoff_borough,
    dropoff_lookup.Zone AS dropoff_zone,
    twl.trip_duration_seconds,
  FROM trips_with_lookup AS twl
  LEFT JOIN ingestion.taxi_zone_lookup AS dropoff_lookup
    ON twl.dolocationid = dropoff_lookup.LocationID
)

SELECT
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  pulocationid,
  dolocationid,
  taxi_type,
  trip_distance,
  passenger_count,
  fare_amount,
  tip_amount,
  total_amount,
  pickup_borough,
  pickup_zone,
  dropoff_borough,
  dropoff_zone,
  trip_duration_seconds,
FROM final;

