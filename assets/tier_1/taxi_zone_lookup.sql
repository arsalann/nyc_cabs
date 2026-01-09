/* @bruin
name: tier_1.taxi_zone_lookup
type: duckdb.sql
description: Load taxi zone lookup table from HTTP CSV source. Maps LocationID to Borough, Zone, and service_zone. Replaced on every pipeline run to ensure we have the latest zone information.
owner: nyc-taxi-team
tags:
  - tier_1
  - lookup
  - reference_data
columns:
  - name: location_id
    type: INTEGER
    description: Unique identifier for the taxi zone location
    primary_key: true
    nullable: false
  - name: borough
    type: VARCHAR
    description: Borough name (e.g., Manhattan, Queens, Brooklyn)
    nullable: true
  - name: zone
    type: VARCHAR
    description: Zone name within the borough
    nullable: true
  - name: service_zone
    type: VARCHAR
    description: Service zone classification
    nullable: true
@bruin */

SELECT
    CAST(LocationID AS INTEGER) AS location_id,
    Borough AS borough,
    Zone AS zone,
    service_zone
FROM read_csv(
    'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv',
    header = true,
    auto_detect = true
)
WHERE LocationID IS NOT NULL

