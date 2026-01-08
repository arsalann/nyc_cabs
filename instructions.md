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

## Local Configuration (`.bruin.yml`)

Before running the pipeline, you need to create a `.bruin.yml` file in the project root directory to configure your local DuckDB connection.

### Setup Instructions

1. **Create `.bruin.yml` file** in the project root:
   ```yaml
   default_environment: default
   environments:
       default:
           connections:
               duckdb:
                   - name: duckdb-default
                     path: duckdb.db
   ```

2. **Add to `.gitignore`**: It's best practice to add `.bruin.yml` to your `.gitignore` file because:
   - It may contain sensitive connection information and authentication credentials
   - Different developers may have different local database paths
   - Environment-specific configurations should not be committed to version control

   Add this line to your `.gitignore`:
   ```
   .bruin.yml
   ```

The `.bruin.yml` file configures your local development environment and tells Bruin where to create and store the DuckDB database file.

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

### Configuration Sections

#### `start_date`
The `start_date` determines the earliest date for data processing. When a full-refresh run is triggered, the interval start is automatically set to this `start_date`, and the pipeline will ingest and process all data starting from this date. This is useful for:
- Setting a baseline for historical data backfills
- Limiting the scope of full-refresh operations to avoid processing extremely large date ranges
- Defining the earliest point in time your pipeline should consider

#### `default_connections`
This section initializes database connections that will be used throughout the pipeline. In this case, it initializes a DuckDB instance and provides a connection cursor named `duckdb-default` that can be referenced by assets. The connection name (`duckdb-default`) must match the connection name specified in your `.bruin.yml` file.

#### `variables`
Pipeline-level custom variables allow you to configure reusable values that can be accessed across all assets in the pipeline. Variables can be:
- **Used in Python assets**: Accessed via the `BRUIN_VARS` environment variable (parsed as JSON)
- **Used in SQL assets**: Referenced using Jinja templating syntax (e.g., `{{ taxi_types }}`)
- **Overridden at runtime**: Passed via command-line arguments when running the pipeline

In this pipeline, the `taxi_types` variable allows you to configure which taxi types to ingest (yellow, green, or both) without modifying the asset code.

## Asset Specifications

### 1. Ingestion Layer

#### `ingestion.ingest_trips_python`
- **Type**: `python`
- **Strategy**: `create+replace`
- **Connection**: `duckdb-default`
- **Purpose**: Ingest raw trip data from HTTP parquet files using Python

**Python Materialization Overview**:
Bruin's Python materialization allows you to write Python code that returns a Pandas DataFrame, which Bruin automatically materializes into a database table. This approach is beneficial because:
- **No manual database operations**: You don't need to use DuckDB's Python library directly or write SQL to create/insert data
- **Automatic schema handling**: Bruin infers the schema from your DataFrame and creates the table accordingly
- **Consistent with SQL assets**: The materialized table can be referenced by SQL assets just like any other table
- **Simplified data processing**: You can focus on data extraction and transformation logic without worrying about database connection management

The `materialize()` function is required and must return a Pandas DataFrame. Bruin calls this function, receives the DataFrame, and handles all the database operations to store it as a table based on the materialization strategy.

**Bruin Configuration**:
- Preserves original column names from parquet files (column normalization happens in tier_1)
- Adds `taxi_type` column from pipeline variables
- Adds `extracted_at` timestamp column
- Uses `create+replace` strategy to fully refresh the table on each run

#### `ingestion.taxi_zone_lookup`
- **Type**: `duckdb.sql`
- **Strategy**: `truncate+insert`
- **Purpose**: Load taxi zone lookup table from HTTP CSV
- **Bruin Configuration**:
  - Primary key: `location_id` (non-nullable)
  - Strategy: `truncate+insert` - deletes all existing rows and inserts new data on each run

### 2. Tier 1: Raw Data Storage

#### `tier_1.trips_historic`
- **Type**: `duckdb.sql`
- **Strategy**: `time_interval`
- **Incremental Key**: `pickup_time`
- **Time Granularity**: `timestamp`
- **Purpose**: Store raw ingested data from Python ingestion table to persistent storage with normalized column names

**Time-Interval Strategy**:
The `time_interval` strategy is designed for incremental processing based on time-based keys. How it works:
- Bruin automatically calculates a date range based on the run parameters (`start_datetime` and `end_datetime`)
- It deletes all rows in the target table where the `incremental_key` (pickup_time) falls within this date range
- Then it inserts the new data from the query results for that same date range
- This ensures efficient updates: only the affected time period is processed, not the entire table

Why we chose it: This strategy is ideal for time-series data where we want to reprocess specific date ranges (e.g., to handle late-arriving data or corrections) without affecting other time periods.

**Bruin Configuration**:
- Reads from `ingestion.ingest_trips_python`
- Normalizes column names (e.g., `tpep_pickup_datetime` → `pickup_time`)
- Adds `loaded_at` timestamp column
- Preserves `extracted_at` timestamp from ingestion layer

### 3. Tier 2: Cleaned & Enriched Data

#### `tier_2.trips_summary`
- **Type**: `duckdb.sql`
- **Strategy**: `time_interval`
- **Incremental Key**: `pickup_time`
- **Time Granularity**: `timestamp`
- **Primary Key**: Composite (`pickup_time`, `dropoff_time`, `pickup_location_id`, `dropoff_location_id`, `taxi_type`)
- **Purpose**: Clean, deduplicate, and enrich trip data

**Time-Interval Strategy**:
Same as tier_1 - processes data incrementally based on the pickup_time date range, allowing efficient updates to cleaned and enriched data.

**Bruin Configuration**:
- Reads from `tier_1.trips_historic`
- Enriches with location data from `ingestion.taxi_zone_lookup`
- Adds `updated_at` timestamp column
- Preserves `extracted_at` timestamp from tier_1
- All primary key columns are non-nullable

### 4. Tier 3: Reports

#### `tier_3.report_trips_monthly`
- **Type**: `duckdb.sql`
- **Strategy**: `time_interval`
- **Incremental Key**: `month_date`
- **Time Granularity**: `timestamp`
- **Primary Key**: Composite (`taxi_type`, `month_date`)
- **Purpose**: Generate monthly summary reports

**Time-Interval Strategy**:
Uses `month_date` as the incremental key, which is the first day of each month. This allows reprocessing of specific months (e.g., if source data is corrected) without affecting other months.

**Bruin Configuration**:
- Reads from `tier_2.trips_summary`
- Aggregates data by `taxi_type` and month
- Adds `updated_at` timestamp column
- Aggregates `extracted_at` using MAX to track latest extraction time per month

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
