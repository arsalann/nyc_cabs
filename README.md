# NYC Taxi Pipelines - Bruin Sample Project

A comprehensive ELT pipeline built with Bruin that demonstrates best practices for building data pipelines. This project processes NYC taxi trip data from public HTTP sources, transforms it through multiple tiers, and generates analytical reports.

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
   - Returns Pandas DataFrames that Bruin automatically materializes

2. **Time-Interval Incremental Strategy**
   - Efficient incremental processing using `time_interval` materialization
   - Automatic date range handling and data deletion/replacement
   - Month-level truncation for batch processing

3. **SQL Transformations**
   - Multi-CTE query patterns following Bruin SQL style guide
   - Window functions for deduplication
   - JOIN operations for data enrichment
   - Aggregations for reporting

4. **Pipeline Variables**
   - Using pipeline-level variables (e.g., `taxi_types`) for configuration
   - Environment variable access in Python assets
   - Template variable usage in SQL assets

5. **Data Lineage and Dependencies**
   - Explicit dependency declarations between assets
   - Automatic dependency resolution and execution ordering
   - Cross-tier data flow

6. **Metadata Management**
   - Comprehensive column descriptions and documentation
   - Primary key definitions and nullable constraints
   - Asset-level tags and ownership

7. **Data Quality Patterns**
   - NULL value filtering
   - Data type casting and validation
   - Timestamp tracking (extracted_at, loaded_at, updated_at)

### Technical Patterns Demonstrated

- **Column Normalization**: Transforming source column names to more readable formats
- **Deduplication**: Using window functions to handle duplicate records
- **Data Enrichment**: Joining with lookup tables to add human-readable values
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

## Documentation

- **[Implementation Instructions](instructions.md)**: Detailed technical documentation
- **[Bruin Agent Rules](AGENTS.md)**: SQL style guide and best practices

## Data Source

This project uses publicly available NYC taxi trip data from the [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) hosted on AWS CloudFront.

## License

This is a sample/template project for educational purposes.

