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
├── macros/
│   └── ingestion.sql
└── assets/
    ├── ingestion/
    │   ├── ingest_trips_python.py (recommended)
    │   ├── ingest_trips_bulk.sql (alternative)
    │   ├── ingest_trips_single.sql (single month)
    │   └── taxi_zone_lookup.sql
    ├── tier_1/
    │   └── trips.sql
    ├── tier_2/
    │   └── trips_summary.sql
    └── tier_3/
        └── report_trips_monthly.sql
```

## Pipeline Configuration (`pipeline.yml`)

```yaml
name: nyc-taxi-pipelines
schedule: monthly
start_date: "2024-01-01"  # Use recent date to avoid large full-refresh queries
catchup: false
default_connections:
  duckdb: "duckdb-default"
default:
  type: duckdb.sql
```

**Note**: `start_date` should be set to a recent date (e.g., 2024-01-01) to avoid generating extremely large UNION ALL queries during full-refresh operations.

## Asset Specifications

### 1. Ingestion Layer

#### `ingestion.ingest_trips_python` (Recommended) ⭐
- **Type**: `python`
- **Strategy**: `create+replace` (Python assets use `create+replace`, not `truncate+insert`)
- **Connection**: `duckdb-default`
- **Purpose**: Ingest raw trip data from HTTP parquet files using Python
- **Key Requirements**:
  - Use `requests` library to download parquet files from HTTP URLs
  - Loop through all months between `start_date` and `end_date`
  - Read parquet files into Pandas DataFrames using `pd.read_parquet()`
  - **Column Normalization**: Parquet files use snake_case with underscores (e.g., `vendor_id`, `pu_location_id`), but tier_1 expects lowercase without underscores (e.g., `vendorid`, `pulocationid`). The asset includes a `normalize_column_names()` function to handle this mapping.
  - Add `taxi_type` column (default: `'yellow'`)
  - Combine all DataFrames using `pd.concat()` and return for Bruin materialization
  - Handle date range to month conversion using `generate_month_range()` function:
    - Reads dates from `BRUIN_START_DATE` and `BRUIN_END_DATE` environment variables (already in YYYY-MM-DD format)
    - Extracts first 10 characters to get YYYY-MM-DD format
    - Generates list of months between start and end (inclusive)
    - Example: 2021-12-01 to 2022-01-01 → ingest 2 months (Dec 2021 and Jan 2022)
- **Dependencies**: `pandas`, `requests`, `python-dateutil`, `pyarrow`
- **Function**: `materialize(start_date=None, end_date=None, **kwargs)` - returns Pandas DataFrame
- **Date Handling**: 
  - Bruin provides dates via `BRUIN_START_DATE` and `BRUIN_END_DATE` environment variables in YYYY-MM-DD format
  - Falls back to function parameters or other environment variables if needed
  - Raises error if dates are not provided

#### `ingestion.ingest_trips_bulk` (Alternative SQL approach)
- **Type**: `duckdb.sql`
- **Strategy**: `truncate+insert`
- **Purpose**: Ingest raw trip data from HTTP parquet files using SQL UNION ALL
- **Key Requirements**:
  - Use `read_parquet()` to fetch files from HTTP URLs
  - Generate UNION ALL statements for all months in the date range
  - Add `taxi_type` column (hardcoded to `'yellow'` for now)
  - Handle date range to month conversion using Jinja
  - Wrap in CTE structure: `WITH parquet_union AS (...) SELECT * FROM parquet_union`
- **Note**: May have parser issues with very large date ranges during full-refresh

#### `ingestion.ingest_trips_single` (Single month only)
- **Type**: `duckdb.sql`
- **Strategy**: `truncate+insert`
- **Purpose**: Ingest raw trip data for a single month (uses interval start date's month)
- **Key Requirements**:
  - Use `read_parquet()` to fetch single month file
  - Extract month from `start_date` only
  - Add `taxi_type` column
- **Use Case**: Incremental processing of one month at a time

#### `ingestion.taxi_zone_lookup`
- **Type**: `duckdb.sql`
- **Strategy**: `truncate+insert`
- **Purpose**: Load taxi zone lookup table from HTTP CSV
- **Key Requirements**:
  - Use `read_csv()` with `header=true, auto_detect=true`
  - Filter out NULL LocationID values
  - Primary key: `LocationID` (non-nullable)
  - Columns: `LocationID`, `Borough`, `Zone`, `service_zone`

### 2. Tier 1: Raw Data Storage

#### `tier_1.trips_historic`
- **Type**: `duckdb.sql`
- **Strategy**: `time_interval`
- **Incremental Key**: `pickup_time`
- **Time Granularity**: `timestamp`
- **Interval Modifiers**: `start: -3d, end: 1d`
- **Purpose**: Store raw ingested data from Python ingestion table to persistent storage
- **Key Requirements**:
  - Read from `ingestion.ingest_trips_python`
  - Filter by `start_datetime` and `end_datetime` (from interval modifiers)
  - Preserve all original columns from source (columns already normalized by Python asset)
  - Handle schema evolution (e.g., `cbd_congestion_fee` column in newer data)
  - Filter out NULL `pickup_time` values

### 3. Tier 2: Cleaned & Enriched Data

#### `tier_2.trips_summary`
- **Type**: `duckdb.sql`
- **Strategy**: `time_interval`
- **Incremental Key**: `pickup_time`
- **Time Granularity**: `timestamp`
- **Interval Modifiers**: `start: -3d, end: 1d`
- **Primary Key**: Composite (`pickup_time`, `dropoff_time`, `pulocationid`, `dolocationid`, `taxi_type`)
- **Purpose**: Clean, deduplicate, and enrich trip data
- **Key Requirements**:
  - Read from `tier_1.trips_historic`
  - Deduplicate using `ROW_NUMBER()` window function
  - Calculate `trip_duration_seconds` (dropoff - pickup)
  - Join with `ingestion.taxi_zone_lookup` for pickup and dropoff locations
  - Select columns: datetime fields, location IDs, taxi_type, trip metrics, location names
  - Filter by interval modifiers and data quality checks

### 4. Tier 3: Reports

#### `tier_3.report_trips_monthly`
- **Type**: `duckdb.sql`
- **Strategy**: `time_interval`
- **Incremental Key**: `month_date`
- **Time Granularity**: `timestamp`
- **Interval Modifiers**: `start: -3M, end: 1M`
- **Primary Key**: Composite (`taxi_type`, `month_date`)
- **Purpose**: Generate monthly summary reports
- **Key Requirements**:
  - Read from `tier_2.trips_summary`
  - Group by `taxi_type` and `DATE_TRUNC('month', pickup_time)`
  - Calculate metrics:
    - `trip_duration_avg`, `trip_duration_total`
    - `total_amount_avg`, `total_amount_total`
    - `tip_amount_avg`, `tip_amount_total`
    - `total_trips` (COUNT)
  - Filter by interval modifiers and ensure metrics are not NULL

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
bruin validate ./nyc/pipeline.yml --environment default
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
bruin run ./nyc/assets/tier_1/trips.sql \
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
  --environment default

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

### Python Asset Date Handling
- **Status**: ✅ Resolved
- **Implementation**: 
  - The Python asset reads dates from `BRUIN_START_DATE` and `BRUIN_END_DATE` environment variables (provided by Bruin in YYYY-MM-DD format)
  - Falls back to function parameters or other environment variables if needed
  - Uses simple string slicing (`[:10]`) to extract YYYY-MM-DD format since dates are already in the correct format
  - Includes a clean `generate_month_range()` function to convert date ranges to month lists
  - Raises clear error if dates are not provided

### SQL Bulk Ingestion Parser Error
- **Issue**: Using `--full-refresh` flag causes "Parser Error: syntax error at or near SELECT" in SQL bulk ingestion assets
- **Status**: Known issue with large UNION ALL queries
- **Workaround**: 
  - Use Python ingestion (`ingest_trips_python`) instead (recommended)
  - Or use single-month ingestion (`ingest_trips_single`) for incremental processing
  - Run without `--full-refresh` for normal operations

### Schema Evolution & Column Mapping
- **Issue**: Parquet file column names use snake_case with underscores (e.g., `vendor_id`, `pu_location_id`), but tier_1 schema expects lowercase without underscores (e.g., `vendorid`, `pulocationid`)
- **Solution**: 
  - Python asset includes `normalize_column_names()` function that maps:
    - `vendor_id` → `vendorid`
    - `ratecode_id` → `ratecodeid`
    - `pu_location_id` → `pulocationid`
    - `do_location_id` → `dolocationid`
  - Only renames columns that exist in the DataFrame (handles missing columns gracefully)
  - SQL assets use conditional Jinja: `{% if 'cbd_congestion_fee' in get_columns_in_relation('table_name') %}`

## Implementation Checklist

- [ ] Create `nyc/pipeline.yml` with correct configuration
- [ ] Create `nyc/macros/ingestion.sql` (optional - can inline Jinja in asset)
- [ ] Create `ingestion.ingest_trips_python.py` with date-to-month conversion logic and column normalization
- [ ] Create `ingestion.taxi_zone_lookup.sql` with CSV ingestion
- [ ] Create `tier_1.trips_historic.sql` with time_interval strategy
- [ ] Create `tier_2.trips_summary.sql` with deduplication and enrichment
- [ ] Create `tier_3.report_trips_monthly.sql` with monthly aggregations
- [ ] Add all required Bruin metadata (name, uri, description, owner, tags, columns)
- [ ] Set primary keys and nullable constraints correctly
- [ ] Add interval modifiers where needed
- [ ] Follow SQL style guide (trailing commas, alias alignment, etc.)
- [ ] Test individual assets
- [ ] Test full pipeline with different date ranges
- [ ] Verify data quality and row counts

## Key Implementation Details

1. **Date Range to Months**: 
   - Read dates from `BRUIN_START_DATE` and `BRUIN_END_DATE` environment variables (YYYY-MM-DD format)
   - Use `generate_month_range()` function to convert date range to list of (year, month) tuples
   - Handles cross-year ranges correctly (e.g., 2021-12-01 to 2022-01-01 → Dec 2021, Jan 2022)
2. **Column Normalization**: 
   - Parquet files use snake_case with underscores (`vendor_id`, `pu_location_id`)
   - Tier_1 expects lowercase without underscores (`vendorid`, `pulocationid`)
   - Python asset includes `normalize_column_names()` function to handle this mapping
3. **Taxi Type**: Defaults to `'yellow'`, can be overridden via environment variable or kwargs
4. **Deduplication**: Use `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` and filter `rn = 1`
5. **Lookup Joins**: Use `LEFT JOIN` to retain all trips even if LocationID not found
6. **Interval Modifiers**: Use `-3d` start and `1d` end to handle late-arriving data
7. **Schema Handling**: Use conditional Jinja to handle columns that may not exist in all data files
