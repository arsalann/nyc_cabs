/* @bruin
name: tier_1.payment_type_lookup
uri: neptune.tier_1.payment_type_lookup
type: duckdb.sql
description: |
  Loads the payment type lookup table from local CSV file.
  This table contains payment type information including payment_type_id and payment_description.
  The lookup table is a static reference table that maps payment type codes to human-readable descriptions.

  Design Choices:
  - Why read from local CSV:
    - Payment type codes are standardized and do not change frequently
    - Local seed file provides version control and reproducibility
    - Strategy is truncate+insert, so table is replaced every time the pipeline runs

  - DuckDB read_csv() parameters:
    - header=true: First row contains column names
    - auto_detect=true: Automatically detect column types from data

  - Data Quality Filter:
    - payment_type_id IS NOT NULL: Ensures we only load valid payment types
    - payment_type_id is the primary key, so NULL values would break referential integrity

owner: data-engineering

materialization:
  type: table

columns:
  - name: payment_type_id
    type: INTEGER
    description: Numeric code signifying how the passenger paid for the trip
    primary_key: true
    nullable: false
  - name: payment_description
    type: VARCHAR
    description: Human-readable description of the payment type

@bruin */

SELECT
  payment_type_id,
  payment_description,
FROM read_csv(
  'nyc/assets/tier_1/payment_type_lookup.csv',
  header=true,
  auto_detect=true
)
WHERE 1=1
  AND payment_type_id IS NOT NULL
