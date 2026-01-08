# NYC Taxi Pipelines - Implementation Instructions

This document provides complete instructions to create and test a Bruin pipeline for NYC taxi trip data processing.

## Pipeline Overview

The pipeline extracts NYC taxi trip data from HTTP parquet files, cleans and transforms it, and generates monthly summary reports. It uses DuckDB for local processing and follows a three-tier architecture: ingestion → tier_1 (raw) → tier_2 (cleaned) → tier_3 (reports).

## Data Sources

### Trip Data
- **URL**: `https://d37ci6vzurychx.cloudfront.net/trip-data/`
- **Format**: Parquet files, one per taxi type per month
- **Naming**: `<taxi_type>_tripdata_<year>-<month>.parquet`
- **Examples**:
  - `yellow_tripdata_2022-03.parquet`
  - `green_tripdata_2025-01.parquet`
- **Taxi Types**: `yellow` (default), `green`

### Lookup Table
- **URL**: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
- **Purpose**: Maps LocationID to Borough, Zone, and service_zone
- **Refresh**: Replaced on every pipeline run

## Pipeline Structure

### Directory Layout
```
nyc/
├── pipeline.yml
└── assets/
    ├── ingestion/
    │   ├── ingest_trips_python.py
    │   ├── requirements.txt
    │   └── taxi_zone_lookup.sql
    ├── tier_1/
    │   └── trips_historic.sql
    ├── tier_2/
    │   └── trips_summary.sql
    └── tier_3/
        └── report_trips_monthly.sql
```

## Pipeline Configuration (`pipeline.yml`)

```yaml
name: nyc-taxi-pipelines
schedule: monthly
start_date: "2022-01-01"
default_connections:
  duckdb: "duckdb-default"
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

**Note**: The pipeline uses variables to configure which taxi types to ingest. The `start_date` should be set appropriately based on your data needs.

## Asset Specifications

### 1. Ingestion Layer

#### `ingestion.ingest_trips_python`
- **Type**: `python`
- **Strategy**: `create+replace`
- **Connection**: `duckdb-default`
- **Purpose**: Ingest raw trip data from HTTP parquet files using Python
- **Key Requirements**:
  - Use `requests` library to download parquet files from HTTP URLs
  - Loop through all months between `start_date` and `end_date`
  - Read parquet files into Pandas DataFrames using `pd.read_parquet()`
  - **Preserves original column names**: The ingestion layer keeps data as-is from parquet files (e.g., `vendor_id`, `tpep_pickup_datetime`, `pu_location_id`). Column normalization happens in tier_1.
  - Add `taxi_type` column from pipeline variables (default: `["yellow", "green"]`)
  - Add `extracted_at` timestamp column to track when data was extracted
  - Combine all DataFrames using `pd.concat()` and return for Bruin materialization
  - Handle date range to month conversion using `generate_month_range()` function:
    - Reads dates from `BRUIN_START_DATE` and `BRUIN_END_DATE` environment variables (YYYY-MM-DD format)
    - Generates list of months between start and end (inclusive)
    - Example: 2021-12-01 to 2022-01-01 → ingest 2 months (Dec 2021 and Jan 2022)
- **Dependencies**: `pandas`, `requests`, `python-dateutil`, `pyarrow`
- **Function**: `materialize()` - returns Pandas DataFrame
- **Date Handling**: 
  - Bruin provides dates via `BRUIN_START_DATE` and `BRUIN_END_DATE` environment variables in YYYY-MM-DD format
  - Uses `generate_month_range()` to convert date ranges to month lists
  - Raises error if no dataframes are successfully downloaded

#### `ingestion.taxi_zone_lookup`
- **Type**: `duckdb.sql`
- **Strategy**: `truncate+insert`
- **Purpose**: Load taxi zone lookup table from HTTP CSV
- **Key Requirements**:
  - Use `read_csv()` with `header=true, auto_detect=true`
  - Filter out NULL location_id values
  - Primary key: `location_id` (non-nullable)
  - Columns: `location_id`, `borough`, `zone`, `service_zone` (lowercase column names)

### 2. Tier 1: Raw Data Storage

#### `tier_1.trips_historic`
- **Type**: `duckdb.sql`
- **Strategy**: `time_interval`
- **Incremental Key**: `pickup_time`
- **Time Granularity**: `timestamp`
- **Purpose**: Store raw ingested data from Python ingestion table to persistent storage with normalized column names
- **Key Requirements**:
  - Read from `ingestion.ingest_trips_python`
  - **Column Normalization**: Transform source column names to more human-readable, lowercase formats:
    - `vendor_id` → `vendorid`
    - `tpep_pickup_datetime` → `pickup_time` (cast to TIMESTAMP)
    - `tpep_dropoff_datetime` → `dropoff_time` (cast to TIMESTAMP)
    - `pu_location_id` → `pickup_location_id`
    - `do_location_id` → `dropoff_location_id`
    - `ratecode_id` → `ratecodeid`
  - Filter by `start_datetime` and `end_datetime` using month-level truncation
  - Add `loaded_at` timestamp column to track when data was loaded into tier_1
  - Preserve `extracted_at` timestamp from ingestion layer
  - Filter out NULL `pickup_time` values
  - Cast datetime columns to TIMESTAMP type for proper date handling

### 3. Tier 2: Cleaned & Enriched Data

#### `tier_2.trips_summary`
- **Type**: `duckdb.sql`
- **Strategy**: `time_interval`
- **Incremental Key**: `pickup_time`
- **Time Granularity**: `timestamp`
- **Primary Key**: Composite (`pickup_time`, `dropoff_time`, `pickup_location_id`, `dropoff_location_id`, `taxi_type`)
- **Purpose**: Clean, deduplicate, and enrich trip data
- **Key Requirements**:
  - Read from `tier_1.trips_historic`
  - Deduplicate using `ROW_NUMBER()` window function with composite key
  - Calculate `trip_duration_seconds` (dropoff - pickup) using `EXTRACT(EPOCH FROM ...)`
  - Join with `ingestion.taxi_zone_lookup` for pickup and dropoff locations (LEFT JOIN to preserve all trips)
  - Select columns: datetime fields, location IDs, taxi_type, trip metrics, location names (borough, zone)
  - Add `updated_at` timestamp column to track when data was last updated in tier_2
  - Preserve `extracted_at` timestamp from tier_1
  - Filter by date range using month-level truncation and data quality checks (all primary key columns must be NOT NULL)

### 4. Tier 3: Reports

#### `tier_3.report_trips_monthly`
- **Type**: `duckdb.sql`
- **Strategy**: `time_interval`
- **Incremental Key**: `month_date`
- **Time Granularity**: `timestamp`
- **Primary Key**: Composite (`taxi_type`, `month_date`)
- **Purpose**: Generate monthly summary reports
- **Key Requirements**:
  - Read from `tier_2.trips_summary`
  - Group by `taxi_type` and `DATE_TRUNC('month', pickup_time)` AS `month_date`
  - Calculate metrics:
    - `trip_duration_avg`, `trip_duration_total`
    - `total_amount_avg`, `total_amount_total`
    - `tip_amount_avg`, `tip_amount_total`
    - `total_trips` (COUNT)
  - Aggregate `extracted_at` using `MAX(extracted_at)` to get latest extraction time for the month
  - Add `updated_at` timestamp column to track when data was last updated in tier_3
  - Filter by date range using month-level truncation and ensure metrics are not NULL

## SQL Style Requirements

Follow the Bruin SQL style guide:
- **Trailing Commas**: All SELECT columns end with comma (even last one)
- **Alias Alignment**: All column aliases aligned
- **WHERE Clauses**: Start with `WHERE 1=1`
- **CTE Format**: First CTE has no leading comma, subsequent CTEs start with comma
- **Final CTE**: Always have a `final` CTE before final SELECT
- **Keywords**: All SQL keywords UPPERCASE
- **Line Length**: Max 120 characters
- **Indentation**: 2 spaces

## Testing Instructions

### 1. Validate Pipeline
```bash
bruin validate ./nyc/pipeline.yml --environment dev
```

### 2. Test Individual Assets (Recommended)
```bash
# Test Python ingestion (recommended)
bruin run ./nyc/assets/ingestion/ingest_trips_python.py \
  --start-date 2021-01-01 \
  --end-date 2022-02-28

# Test lookup table
bruin run ./nyc/assets/ingestion/taxi_zone_lookup.sql

# Test tier_1
bruin run ./nyc/assets/tier_1/trips_historic.sql \
  --start-date 2021-01-01 \
  --end-date 2022-02-28

# Test tier_2
bruin run ./nyc/assets/tier_2/trips_summary.sql \
  --start-date 2021-01-01 \
  --end-date 2022-02-28

# Test tier_3
bruin run ./nyc/assets/tier_3/report_trips_monthly.sql \
  --start-date 2021-01-01 \
  --end-date 2022-02-28
```

### 3. Run Full Pipeline (Incremental)
```bash
# Run with Python ingestion (recommended)
bruin run ./nyc/pipeline.yml \
  --start-date 2021-01-01 \
  --end-date 2022-02-28 \
  --environment dev

# Verify 14 months of data in final report
bruin query --connection duckdb-default --query "SELECT COUNT(*) as month_count FROM tier_3.report_trips_monthly WHERE month_date >= '2021-01-01' AND month_date <= '2022-02-28'"
```

### 4. Test Different Date Ranges
```bash
# Single month (same year)
bruin run ./nyc/pipeline.yml --start-date 2025-01-01 --end-date 2025-01-31

# Multiple months (same year)
bruin run ./nyc/pipeline.yml --start-date 2025-01-01 --end-date 2025-03-31

# Cross-year
bruin run ./nyc/pipeline.yml --start-date 2024-12-01 --end-date 2025-01-31
```

### 5. Verify Data
```bash
# Check row counts
bruin query --asset ingestion.ingest_trips_python --query "SELECT COUNT(*) FROM ingestion.ingest_trips_python"

# Check monthly report (should show 14 months for 2021-01 to 2022-02)
bruin query --asset tier_3.report_trips_monthly --query "SELECT COUNT(*) as month_count FROM tier_3.report_trips_monthly WHERE month_date >= '2021-01-01' AND month_date <= '2022-02-28'"

# Check monthly report details
bruin query --asset tier_3.report_trips_monthly --query "SELECT * FROM tier_3.report_trips_monthly WHERE month_date >= '2021-01-01' AND month_date <= '2022-02-28' ORDER BY month_date"
```

## Known Issues & Workarounds

### Date Type Casting in DATE_TRUNC
- **Issue**: DATE_TRUNC requires explicit type casting when using template variables
- **Solution**: 
  - Cast template variables to TIMESTAMP: `CAST('{{ start_datetime }}' AS TIMESTAMP)`
  - Cast source datetime columns to TIMESTAMP: `CAST(tpep_pickup_datetime AS TIMESTAMP)`
  - This ensures proper type resolution in DuckDB

## Implementation Checklist

- [ ] Create `nyc/pipeline.yml` with correct configuration and variables
- [ ] Create `ingestion.ingest_trips_python.py` with date-to-month conversion logic
- [ ] Create `ingestion.taxi_zone_lookup.sql` with CSV ingestion
- [ ] Create `tier_1.trips_historic.sql` with time_interval strategy and column normalization
- [ ] Create `tier_2.trips_summary.sql` with deduplication and enrichment
- [ ] Create `tier_3.report_trips_monthly.sql` with monthly aggregations
- [ ] Add all required Bruin metadata (name, uri, description, owner, tags, columns)
- [ ] Set primary keys and nullable constraints correctly
- [ ] Add timestamp tracking columns (extracted_at, loaded_at, updated_at)
- [ ] Test individual assets
- [ ] Test full pipeline with different date ranges
- [ ] Verify data quality and row counts

## Key Implementation Details

1. **Date Range to Months**: 
   - Read dates from `BRUIN_START_DATE` and `BRUIN_END_DATE` environment variables (YYYY-MM-DD format)
   - Use `generate_month_range()` function to convert date range to list of (year, month) tuples
   - Handles cross-year ranges correctly (e.g., 2021-12-01 to 2022-01-01 → Dec 2021, Jan 2022)
2. **Column Normalization**: 
   - **Ingestion Layer**: Preserves original column names from parquet files as-is (e.g., `vendor_id`, `tpep_pickup_datetime`, `pu_location_id`)
   - **Tier_1 Layer**: Transforms column names to more human-readable, lowercase formats for better readability and consistency:
     - `vendor_id` → `vendorid`
     - `tpep_pickup_datetime` → `pickup_time`
     - `tpep_dropoff_datetime` → `dropoff_time`
     - `pu_location_id` → `pickup_location_id`
     - `do_location_id` → `dropoff_location_id`
     - `ratecode_id` → `ratecodeid`
   - This separation allows the ingestion layer to process data as-is, while tier_1 standardizes the schema for downstream consumption
3. **Taxi Types**: Configured via pipeline variables (default: `["yellow", "green"]`), accessible in Python assets via `BRUIN_VARS` environment variable
4. **Deduplication**: Use `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` and filter `rn = 1` to keep most recent record for each unique trip
5. **Lookup Joins**: Use `LEFT JOIN` to retain all trips even if location_id not found in lookup table
6. **Timestamp Tracking**: 
   - `extracted_at`: Set in ingestion layer when data is downloaded
   - `loaded_at`: Set in tier_1 when data is loaded into persistent storage
   - `updated_at`: Set in tier_2 and tier_3 when data is updated/refreshed
