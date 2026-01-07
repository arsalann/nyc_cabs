/* @bruin
name: ingestion.taxi_zone_lookup
uri: neptune.ingestion.taxi_zone_lookup
type: duckdb.sql
description: |
  Loads the NYC taxi zone lookup table from HTTP CSV source.
  This table contains zone information including LocationID, Borough, Zone, and service_zone.
  The lookup table is replaced every time the pipeline runs to ensure it's up to date.
  Sample query:
  ```sql
  SELECT *
  FROM ingestion.taxi_zone_lookup
  WHERE 1=1
  LIMIT 10
  ```

owner: data-engineering
tags:
  - ingestion
  - nyc-taxi
  - lookup-table

materialization:
  type: table
  strategy: truncate+insert

columns:
  - name: LocationID
    type: INTEGER
    description: Unique identifier for the taxi zone location
    primary_key: true
    nullable: false
  - name: Borough
    type: VARCHAR
    description: Borough name where the taxi zone is located
  - name: Zone
    type: VARCHAR
    description: Zone name within the borough
  - name: service_zone
    type: VARCHAR
    description: Service zone classification (Airports, Boro Zone, Yellow Zone, etc.)

@bruin */

WITH raw_lookup AS (
  {# 
    Read taxi zone lookup table from HTTP CSV source
    
    Why read from HTTP each time:
    - Lookup table may be updated by NYC TLC (new zones, renamed zones, etc.)
    - Refreshing ensures we always have the latest zone information
    - Strategy is truncate+insert, so old data is replaced completely
    
    DuckDB read_csv() parameters:
    - header=true: First row contains column names
    - auto_detect=true: Automatically detect column types from data
    
    Data Quality Filter:
    - LocationID IS NOT NULL: Ensures we only load valid zones
    - LocationID is the primary key, so NULL values would break referential integrity
  #}
  SELECT
    LocationID,
    Borough,
    Zone,
    service_zone,
  FROM read_csv('https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv', header=true, auto_detect=true)
  WHERE 1=1
    AND LocationID IS NOT NULL
)

, final AS (
  {# 
    Final select with all required columns
    - Simple passthrough since we already filtered in raw_lookup CTE
    - This CTE follows the pattern of having a 'final' CTE before the final SELECT
  #}
  SELECT
    LocationID,
    Borough,
    Zone,
    service_zone,
  FROM raw_lookup
)

SELECT
  LocationID,
  Borough,
  Zone,
  service_zone,
FROM final;

