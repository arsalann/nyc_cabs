# NYC Taxi Pipelines - Development Log

This document contains a step-by-step log of actions and design choices made during the creation of the NYC taxi pipelines. Following these steps will allow anyone to replicate the project.

## Project Overview

Created a Bruin pipeline to extract, clean, process, and report on NYC taxi trip data from HTTP parquet files. The pipeline follows a three-tier architecture: ingestion layer, transformation layer (tier_1 and tier_2), and reporting layer (tier_3).

## Step-by-Step Development Process

### Step 1: Project Structure Setup

**Action**: Created the pipeline directory structure in `/nyc` folder.

**Design Choices**:
- Followed Bruin pipeline structure: `pipeline.yml` at root, `assets/` folder for SQL assets, `macros/` folder for reusable Jinja macros
- Organized assets by tier: `ingestion/`, `tier_1/`, `tier_2/`, `tier_3/`
- This organization makes the data flow clear: ingestion → tier_1 (raw) → tier_2 (cleaned) → tier_3 (reports)

**Files Created**:
- `/nyc/pipeline.yml`
- `/nyc/macros/` (directory)
- `/nyc/assets/ingestion/` (directory)
- `/nyc/assets/tier_1/` (directory)
- `/nyc/assets/tier_2/` (directory)
- `/nyc/assets/tier_3/` (directory)

### Step 2: Pipeline Configuration

**Action**: Created `pipeline.yml` with pipeline metadata and custom variables.

**Design Choices**:
- Set `schedule: monthly` because data is provided at monthly granularity
- Set `start_date: "2022-01-01"` as a reasonable starting point for NYC taxi data
- Added `variables: taxi_type: "yellow"` to allow pipeline-level configuration of taxi type
- Used `default_connections: duckdb: "duckdb-default"` to match the existing `.bruin.yml` configuration
- Set `default: type: duckdb.sql` so all assets default to DuckDB SQL type

**Key Configuration**:
```yaml
name: nyc-taxi-pipelines
schedule: monthly
start_date: "2022-01-01"
variables:
  taxi_type: "yellow"
```

### Step 3: Ingestion Macro Creation

**Action**: Created `macros/ingestion.sql` to generate HTTP parquet ingestion queries.

**Design Choices**:
- Created a reusable macro to avoid code duplication when ingesting multiple months
- Macro accepts `taxi_type` and `months` list as parameters
- Uses `UNION ALL` to combine data from multiple parquet files (one per month)
- Adds `taxi_type` column to each SELECT to track which taxi type the data represents
- Includes fallback empty query if no months are provided (edge case handling)
- Uses `read_parquet()` DuckDB function to read directly from HTTP URLs

**Logic**:
- First month is selected without UNION ALL
- Subsequent months are added with `UNION ALL` in a loop
- Month formatting uses `"%02d"` to ensure two-digit month format (e.g., "01" not "1")

### Step 4: In-Memory Ingestion Asset

**Action**: Created `assets/ingestion/trips_raw_in_memory.sql` to ingest data from HTTP parquet files.

**Design Choices**:
- Used `materialization: strategy: truncate+insert` because this is an in-memory staging table that should be completely replaced each run
- Date parsing logic extracts year and month from `start_date` and `end_date` (provided by Bruin's interval modifiers)
- Month generation handles three cases:
  1. Same year: generates months from start_month to end_month
  2. Different years: generates all months from start to end, including full years in between
- Variable access uses `variables.taxi_type` with fallback to 'yellow' if not defined
- Calls the `generate_parquet_ingestion` macro to build the actual SQL query

**Key Logic**:
- Splits date strings on '-' to extract year and month components
- Builds a list of `[year, month]` pairs for all months in the interval
- Passes this list to the macro which generates UNION ALL queries

### Step 5: Lookup Table Asset

**Action**: Created `assets/ingestion/taxi_zone_lookup.sql` to load taxi zone lookup data.

**Design Choices**:
- Initially tried `duckdb.seed` type but discovered it only supports local file paths, not HTTP URLs
- Switched to `duckdb.sql` type with `read_csv()` function to read from HTTP
- Used `strategy: truncate+insert` because lookup table should be refreshed each run to ensure it's up to date
- Added `WHERE LocationID IS NOT NULL` filter to ensure data quality
- Set `LocationID` as primary key with `nullable: false` to enforce data integrity

**Key Logic**:
- Uses DuckDB's `read_csv()` function with `header=true` and `auto_detect=true` for automatic schema detection
- Filters out rows with NULL LocationID to maintain referential integrity

### Step 6: Tier 1 - Raw Data Storage

**Action**: Created `assets/tier_1/trips.sql` to persist raw trip data.

**Design Choices**:
- Used `strategy: time_interval` with `incremental_key: tpep_pickup_datetime` for incremental updates
- Set `interval_modifiers: start: -3d, end: 1d` to process last 3 days of data (allows for late-arriving data)
- Reads from `ingestion.trips_raw_in_memory` table (the materialized ingestion table)
- Filters by `start_datetime` and `end_datetime` to respect interval modifiers
- Includes all original columns from the parquet files plus the `taxi_type` column

**Key Logic**:
- This is the first persistent storage layer - data moves from in-memory to permanent table
- Time interval strategy ensures only recent data is processed, making runs efficient
- All columns are preserved to maintain data lineage

### Step 7: Tier 2 - Data Cleaning and Enrichment

**Action**: Created `assets/tier_2/trips_summary.sql` to clean and enrich trip data.

**Design Choices**:
- Used composite primary key: `(tpep_pickup_datetime, dropoff_time, pulocationid, dolocationid, taxi_type)`
- This composite key uniquely identifies a trip and prevents duplicates
- Deduplication logic uses `ROW_NUMBER()` window function partitioned by the primary key columns
- Keeps the most recent record (ORDER BY tpep_pickup_datetime DESC) if duplicates exist
- Calculates `trip_duration_seconds` using `EXTRACT(EPOCH FROM ...)` to get duration in seconds
- Joins with lookup table twice (once for pickup, once for dropoff) to get borough and zone names
- Filters out NULL values for critical fields to ensure data quality

**Key Logic**:
1. **Deduplication**: Uses window function to identify duplicates based on trip characteristics
2. **Data Quality**: Filters out NULL values for required fields
3. **Enrichment**: LEFT JOINs with lookup table to add human-readable location names
4. **Calculations**: Computes trip duration from pickup and dropoff timestamps

**Why LEFT JOIN**: Some location IDs might not exist in the lookup table, so LEFT JOIN preserves all trips even if location data is missing.

### Step 8: Tier 3 - Monthly Reports

**Action**: Created `assets/tier_3/report_trips_monthly.sql` to generate monthly summary reports.

**Design Choices**:
- Used `DATE_TRUNC('month', ...)` to group trips by month
- Composite primary key: `(taxi_type, month_date)` ensures one row per taxi type per month
- Set `interval_modifiers: start: -3M, end: 1M` to process last 3 months (monthly reports need longer lookback)
- Calculates both average and total metrics for:
  - Trip duration (avg and total seconds)
  - Total amount (avg and total dollars)
  - Tip amount (avg and total dollars)
- Includes `total_trips` count for each month
- Filters out NULL values for metrics to ensure accurate calculations

**Key Logic**:
- Groups by `taxi_type` and `month_date` to create monthly summaries
- Uses `AVG()` for averages and `SUM()` for totals
- Uses `COUNT(*)` for trip counts
- All metrics are calculated at the monthly level as specified in requirements

### Step 9: SQL Style Compliance

**Action**: Ensured all SQL follows the style guide from AGENTS.md.

**Design Choices**:
- All SELECT statements use trailing commas (even on last column)
- All column aliases are aligned
- All WHERE clauses start with `1=1` for consistent filter chaining
- CTEs follow the format: first CTE without leading comma, subsequent CTEs with leading comma
- Final CTE is named `final` for consistency
- All SQL keywords are UPPERCASE
- Indentation uses 2 spaces throughout

### Step 10: Column Descriptions and Documentation

**Action**: Added comprehensive column descriptions and metadata to all assets.

**Design Choices**:
- Every column has a detailed description explaining its purpose and source
- Primary key columns have `nullable: false` as required by Bruin
- Column types match the expected data types from NYC TLC data dictionary
- Sample queries included in asset descriptions for documentation purposes

## Key Design Decisions Summary

1. **In-Memory Staging Table**: Used for ingestion to allow flexible date range processing without storing intermediate data permanently.

2. **Time Interval Strategy**: Used for tier_1, tier_2, and tier_3 to enable incremental processing and efficient updates.

3. **Composite Primary Keys**: Used in tier_2 and tier_3 to uniquely identify records and enable merge strategies.

4. **Deduplication in Tier 2**: Handles potential duplicate records from source data by keeping the most recent record.

5. **Lookup Table Joins**: Enriches data with human-readable location names while preserving all trips (LEFT JOIN).

6. **Month-Based Aggregation**: Final reports aggregate by month as specified in requirements, using DATE_TRUNC for grouping.

7. **Pipeline Variables**: Used for taxi_type to allow easy switching between yellow and green taxi data without code changes.

## Data Flow

```
HTTP Parquet Files (NYC TLC Website)
    ↓
[Macro: generate_parquet_ingestion]
    ↓
ingestion.trips_raw_in_memory (in-memory staging)
    ↓
tier_1.trips (raw persistent storage)
    ↓
tier_2.trips_summary (cleaned + enriched)
    ↓
tier_3.report_trips_monthly (monthly aggregations)
```

## Dependencies

- `ingestion.trips_raw_in_memory` → depends on: none (reads from HTTP)
- `ingestion.taxi_zone_lookup` → depends on: none (reads from HTTP)
- `tier_1.trips` → depends on: `ingestion.trips_raw_in_memory`
- `tier_2.trips_summary` → depends on: `tier_1.trips`, `ingestion.taxi_zone_lookup`
- `tier_3.report_trips_monthly` → depends on: `tier_2.trips_summary`

## Testing Recommendations

1. Start with a small date range (e.g., 1 month) to test the pipeline
2. Validate each asset individually before running the full pipeline
3. Check that the `taxi_type` variable is accessible in the ingestion asset
4. Verify month calculation logic handles edge cases (same month, cross-year, etc.)
5. Test with both `yellow` and `green` taxi types
6. Verify deduplication logic works correctly
7. Check that lookup table joins return expected borough/zone names

## Notes for Future Development

- The pipeline assumes parquet file structure matches NYC TLC standard format
- Month calculation logic assumes dates are in YYYY-MM-DD format
- Variable access method (`variables.taxi_type`) may need adjustment based on Bruin version
- Consider adding data quality checks (e.g., trip duration > 0, total_amount > 0)
- Consider adding partitioning/clustering for large datasets
- Lookup table is refreshed every run - consider caching if it doesn't change frequently

## Known Issues

### Full-Refresh Parser Error
- **Issue**: When using `--full-refresh` flag, the `ingestion.trips_raw_in_memory` asset fails with "Parser Error: syntax error at or near SELECT" at LINE 21
- **Status**: Under investigation
- **Workaround**: Run without `--full-refresh` flag for incremental updates, or use `--full-refresh` only after the initial table creation
- **Root Cause**: Suspected issue with how Bruin processes Jinja templates when `--full-refresh` is used with `truncate+insert` strategy
- **Testing**: The query works correctly without `--full-refresh` flag, suggesting the issue is specific to how Bruin wraps/processes the query during full refresh operations

