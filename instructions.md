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
    │   ├── trips_raw_in_memory.sql
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

#### `ingestion.trips_raw_in_memory`
- **Type**: `duckdb.sql`
- **Strategy**: `truncate+insert`
- **Purpose**: Ingest raw trip data from HTTP parquet files into in-memory table
- **Key Requirements**:
  - Use `read_parquet()` to fetch files from HTTP URLs
  - Generate UNION ALL statements for all months in the date range
  - Add `taxi_type` column (hardcoded to `'yellow'` for now)
  - Handle date range to month conversion:
    - Extract year/month from `start_date` and `end_date` (provided by Bruin)
    - Generate list of months between start and end (inclusive)
    - Example: 2025-01-01 to 2025-02-15 → ingest months 01 and 02
  - Use Jinja to loop through months and generate UNION ALL statements
  - Wrap in CTE structure: `WITH parquet_union AS (...) SELECT * FROM parquet_union`
- **Date Parsing**: Handle both YYYY-MM-DD and ISO timestamp formats
- **Jinja Variables**: `start_date`, `end_date` (provided by Bruin)

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

#### `tier_1.trips`
- **Type**: `duckdb.sql`
- **Strategy**: `time_interval`
- **Incremental Key**: `tpep_pickup_datetime`
- **Time Granularity**: `timestamp`
- **Interval Modifiers**: `start: -3d, end: 1d`
- **Purpose**: Store raw ingested data from in-memory table to persistent storage
- **Key Requirements**:
  - Read from `ingestion.trips_raw_in_memory`
  - Filter by `start_datetime` and `end_datetime` (from interval modifiers)
  - Preserve all original columns from source
  - Handle schema evolution (e.g., `cbd_congestion_fee` column in newer data)
  - Filter out NULL `tpep_pickup_datetime` values

### 3. Tier 2: Cleaned & Enriched Data

#### `tier_2.trips_summary`
- **Type**: `duckdb.sql`
- **Strategy**: `time_interval`
- **Incremental Key**: `tpep_pickup_datetime`
- **Time Granularity**: `timestamp`
- **Interval Modifiers**: `start: -3d, end: 1d`
- **Primary Key**: Composite (`tpep_pickup_datetime`, `tpep_dropoff_datetime`, `pulocationid`, `dolocationid`, `taxi_type`)
- **Purpose**: Clean, deduplicate, and enrich trip data
- **Key Requirements**:
  - Read from `tier_1.trips`
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
  - Group by `taxi_type` and `DATE_TRUNC('month', tpep_pickup_datetime)`
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
# Test ingestion
bruin run ./nyc/assets/ingestion/trips_raw_in_memory.sql \
  --start-date 2025-01-01 \
  --end-date 2025-01-31

# Test lookup table
bruin run ./nyc/assets/ingestion/taxi_zone_lookup.sql

# Test tier_1
bruin run ./nyc/assets/tier_1/trips.sql \
  --start-date 2025-01-01 \
  --end-date 2025-01-31

# Test tier_2
bruin run ./nyc/assets/tier_2/trips_summary.sql \
  --start-date 2025-01-01 \
  --end-date 2025-01-31

# Test tier_3
bruin run ./nyc/assets/tier_3/report_trips_monthly.sql \
  --start-date 2025-01-01 \
  --end-date 2025-01-31
```

### 3. Run Full Pipeline (Incremental)
```bash
bruin run ./nyc/pipeline.yml \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --environment default
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
bruin query --asset ingestion.trips_raw_in_memory --query "SELECT COUNT(*) FROM ingestion.trips_raw_in_memory"

# Check monthly report
bruin query --asset tier_3.report_trips_monthly --query "SELECT * FROM tier_3.report_trips_monthly ORDER BY month_date DESC LIMIT 10"
```

## Known Issues & Workarounds

### Full-Refresh Parser Error
- **Issue**: Using `--full-refresh` flag causes "Parser Error: syntax error at or near SELECT" in `ingestion.trips_raw_in_memory`
- **Status**: Under investigation - appears to be a Bruin/Jinja rendering issue with `truncate+insert` strategy
- **Workaround**: 
  - Run without `--full-refresh` for normal operations (incremental updates work fine)
  - If full refresh is needed, run individual assets without the flag after initial table creation
  - Use a recent `start_date` in `pipeline.yml` (e.g., 2024-01-01) to minimize date range during full-refresh

### Schema Evolution
- **Issue**: Parquet file schemas change over time (e.g., `cbd_congestion_fee` added in 2025 data)
- **Solution**: Use conditional Jinja to check for column existence: `{% if 'cbd_congestion_fee' in get_columns_in_relation('table_name') %}`

## Implementation Checklist

- [ ] Create `nyc/pipeline.yml` with correct configuration
- [ ] Create `nyc/macros/ingestion.sql` (optional - can inline Jinja in asset)
- [ ] Create `ingestion.trips_raw_in_memory.sql` with date-to-month conversion logic
- [ ] Create `ingestion.taxi_zone_lookup.sql` with CSV ingestion
- [ ] Create `tier_1.trips.sql` with time_interval strategy
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

1. **Date Range to Months**: Parse `start_date` and `end_date` from Bruin, extract year/month, generate list of months (inclusive of end month)
2. **Taxi Type**: Currently hardcoded to `'yellow'` in ingestion asset (can be modified or passed via Jinja context)
3. **Deduplication**: Use `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` and filter `rn = 1`
4. **Lookup Joins**: Use `LEFT JOIN` to retain all trips even if LocationID not found
5. **Interval Modifiers**: Use `-3d` start and `1d` end to handle late-arriving data
6. **Schema Handling**: Use conditional Jinja to handle columns that may not exist in all data files
