# NYC Taxi Pipelines - Bruin Sample Project

A comprehensive ELT pipeline built with Bruin that demonstrates best practices for building data pipelines. This project processes NYC taxi trip data from public HTTP sources, transforms it through multiple tiers, and generates analytical reports.

## Documentation

- **[Implementation Instructions](instructions.md)**: Step-by-step tutorial for building and running this pipeline
- **[AGENTS.md](AGENTS.md)**: AI agent context and rules that help provide guidance for working with this codebase

## What This Project Aims to Achieve

This project serves as a **template and learning resource** for developers who want to understand Bruin's capabilities and how to build production-ready data pipelines. It demonstrates:

- **End-to-end ELT workflows**: From raw data ingestion to analytical reporting
- **Multi-tier data architecture**: Implementing a layered approach (ingestion → raw → cleaned → aggregated)
- **Incremental data processing**: Using time-based incremental strategies for efficient data updates
- **Data quality and transformation**: Deduplication, enrichment, and data quality checks
- **Python and SQL integration**: Combining Python-based ingestion with SQL transformations

## What Tools and Features of Bruin This Project Showcases

### Core Bruin Features

1. **Python Asset Materialization**
   - Demonstrates how to use Python for complex data ingestion
   - Shows integration with external APIs and HTTP data sources
   - Returns Pandas DataFrames that Bruin automatically materializes into tables

2. **Time-Interval Incremental Strategy**
   - Efficient incremental processing using `time_interval` materialization
   - Automatic date range handling and data deletion/replacement
   - Month-level truncation for batch processing

3. **Pipeline Variables**
   - Using pipeline-level variables (e.g., `taxi_types`) for configuration
   - Accessing variables in Python assets via `BRUIN_VARS` environment variable
   - Using variables in SQL assets via Jinja templating

4. **Data Lineage and Dependencies**
   - Explicit dependency declarations between assets
   - Automatic dependency resolution and execution ordering
   - Cross-tier data flow

5. **Metadata Management**
   - Comprehensive column descriptions and documentation
   - Primary key definitions and nullable constraints
   - Asset-level tags and ownership

6. **Data Quality Checks**
   - Custom quality checks to validate business rules and data integrity at the asset level
   - Column-level checks to ensure individual columns meet expected constraints
   - Examples in this pipeline:
     - **tier_1.trips_historic**: 
       - Custom check: Validates that dropoff_time is after pickup_time (trip cannot end before it starts)
       - Custom check: Ensures trip_distance is non-negative
       - Column check: `trip_distance >= 0` (business rule validation)
       - Column check: `dropoff_time > pickup_time` (temporal consistency)
     - **tier_2.trips_summary**: 
       - Custom check: Validates trip_duration_seconds is positive and reasonable (less than 24 hours)
       - Custom check: Ensures total_amount is non-negative
       - Column check: `trip_duration_seconds > 0 AND trip_duration_seconds < 86400` (reasonable trip duration)
       - Column check: `total_amount >= 0` (non-negative fare)
     - **tier_3.report_trips_monthly**: 
       - Custom check: Validates total_trips count is positive for each month
       - Custom check: Ensures aggregated amounts are non-negative
       - Column check: `total_trips > 0` (at least one trip per month)
       - Column check: `total_amount_total >= 0` (non-negative revenue)

### Data Processing Patterns

- **Column Normalization**: Transforming source column names to more readable formats in tier_1
- **Deduplication**: Using window functions to handle duplicate records
- **Data Enrichment**: Joining with lookup tables to convert location IDs to human-readable borough and zone names, adding dimensional information to trip records
- **Aggregation**: Monthly summaries with multiple metrics (averages and totals)
- **Error Handling**: Graceful handling of missing data and failed downloads

## Project Structure

```
nyc/
├── pipeline.yml              # Pipeline configuration
└── assets/
    ├── ingestion/
    │   ├── ingest_trips_python.py    # Python-based data ingestion
    │   └── taxi_zone_lookup.sql      # Lookup table ingestion
    ├── tier_1/
    │   └── trips_historic.sql        # Raw data storage
    ├── tier_2/
    │   └── trips_summary.sql         # Cleaned and enriched data
    └── tier_3/
        └── report_trips_monthly.sql  # Monthly aggregated reports
```

## Quick Start

1. **Validate the pipeline**:
   ```bash
   bruin validate ./nyc/pipeline.yml
   ```

2. **Run a test ingestion**:
   ```bash
   bruin run ./nyc/assets/ingestion/ingest_trips_python.py \
     --start-date 2022-01-01 \
     --end-date 2022-01-31
   ```

3. **Run the full pipeline**:
   ```bash
   bruin run ./nyc/pipeline.yml \
     --start-date 2022-01-01 \
     --end-date 2022-01-31
   ```

## Target Audience

This project is designed for:
- **Developers** learning Bruin for the first time
- **Data Engineers** looking for a reference implementation
- **Teams** evaluating Bruin's capabilities
- **Anyone** wanting to understand modern ELT pipeline patterns

## Learning Path

1. Start with `ingest_trips_python.py` to understand Python asset materialization
2. Review `trips_historic.sql` to see column normalization and time-interval strategy
3. Study `trips_summary.sql` for deduplication and enrichment patterns
4. Examine `report_trips_monthly.sql` for aggregation techniques
5. Explore `pipeline.yml` to understand configuration and variables

## Data Source

This project uses publicly available NYC taxi trip data from the [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). The data is available via HTTP endpoints that provide historical NYC taxi ride information in parquet format.

## License

This is a sample/template project for educational purposes.
