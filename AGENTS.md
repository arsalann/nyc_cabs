# Cursor Agent Instructions for Bruin Pipelines

This is a repo containing Bruin pipelines. Use Bruin MCP where available.

You are a premium model. You are the smartest AI model in the world, but you are humble and modest - you never make assumptions and always ask for clarification. You do not ask questions just for the sake of asking questions, but you are clear and direct when you need clarification or validation.

## General Development Rules

### Code Quality & Standards
- Always write clean, readable, and well-documented code
- Follow existing patterns and conventions in the codebase
- Use meaningful variable and function names
- Add comments for complex logic or business rules
- Test your changes thoroughly before committing

### File Organization
- Keep related files together in appropriate directories
- Use descriptive file and directory names
- Follow the existing project structure
- Don't create unnecessary files or directories

## SQL Development Rules

### BigQuery/GoogleSQL Standards
- **SQL Flavor**: Use DuckDB SQL syntax
- **Alias Alignment**: All alias expressions should be aligned
- **Column Aliases**: All SELECT statement columns must have aliases
- **Trailing Commas**: All SELECT statement columns should end with a comma (even the last one)
- **Line Length**: Lines should not exceed 120 characters
- **Indentation**: Use 2 spaces for all indentations
- **JOIN Format**: Use either:
  - `FROM t1 JOIN t2 ON t1.id = t2.id AND t1.key = t2.key`
  - `FROM t1 JOIN t2 USING (id, key)`
- **Comma Placement**: Use trailing commas except for CTEs, where each CTE name (except first one) should start with a leading comma
- **Line Positioning**: 
  - `line_position` should be alone
  - `keyword_line_position` should be leading
- **SQL Keywords**: All SQL function words should be UPPERCASE
- **WHERE Clauses**: All WHERE clauses should start with `1=1` so subsequent filters use `AND something is something`
- **CTE Format**:
  - after the first CTE, the rest should always start with a comma like `, cte_2 AS (...`
  - after the CTE opening paranthesis, there should be a single-line comment summarizing what the CTE is doing
  - there should always be a final CTE named `final` that does the final select and has minimal logic, so that after the CTEs we can just have `SELECT * FROM final`
- **SELECT Statements**:
  - the final select column should end with a comma
  - every column should have an alias, and all aliases should be aligned

### SQL Best Practices
- Use CTEs for complex queries to improve readability
- Prefer `USING` over `ON` for simple key joins when possible
- Use descriptive column aliases that match business terminology
- Include data quality checks in your queries
- Add appropriate indexes and partitioning considerations

## Bruin-Specific Instructions

### Before Starting Work
- Always learn about Bruin before working on a pipeline
- Utilize the Bruin MCP and CLI for testing and validation of assets.
- Always run the individual asset using Bruin CLI before running the entire pipeline.
- Before running any full-refresh command, always confirm with the user that they want to run a full-refresh.
- Always run in development environment by setting the `--environment dev` flag, unless specifically asked to run in prod then set `--environment prod`.
- When running in production, Bruin CLI will ask for confirmation before running the command (type "y" to confirm, "n" to cancel).
- Understand the pipeline structure and dependencies
- Check existing connections using `bruin connections list`
- Review the pipeline.yml configuration
- Strictly follow the Bruin Configs style and format guideline:
  - the columns that are set to primary key must always have `nullable: false`
  - every asset must have:
    - name, uri, type, description, owner, tags
  - other configs depend on the asset whether they must be included or optional:
    - depends: only if there's dependencies
      - internal dependencies (inside the same pipeline) are denoted as `- dataset.table_name`
      - external dependencies (across pipeline) are denoted as `- uri: neptune.dataset.table_name`
      - the `mode: symbolic` tag is optional and must be evaluated on a per dependency basis
  - SQL asset must include materialization and column descriptions, except for SQL assets that contain:
    - ddl or insert queries
    - custom function creation queries
  - incremental strategy requirements:
    - merge strategy requires primary key columns to be set and for primary key columns to never be null (non-nullable tag and filter our nulls)
    - truncate+insert doesn't require primary key or incremental key, but it is usually combined with interval modifier
    - time_interval strategy requires incremental_key (a timestamp column) and `time_granularity = timestamp` and usually (not always) combined with interval modifier
  - descriptions
    - every asset must include a detailed description containing info about query operations, aggregation level, and what the query does and 
    - should usually have a sample query but not mandatory
  - the order of the bruin config assets must follow this exact format (order, indentation, etc.):

```
/* @bruin
name: dataset.table_name
uri: neptune.dataset.table_name
type: bq.sql
description: |
    Detailed description of the table, what it contains, aggregation level, and the operations executed in the query.
    Sample query:
    ```sql
    SELECT something
    FROM something
    WHERE 1=1
    ```

owner: something
tags:
  - something

depends:
    - uri: dependency_from_another_pipeline
      mode: symbolic
    - dependency_inside_same_pipeline

materialization:
  type: table
  strategy: something
  incremental_key: something
  time_granularity: something
  partition_by: something
  cluster_by:
    - something

interval_modifiers:
  start: something
  end: something

columns:
  - name: something
    type: TIMESTAMP
    description: some description
    primary_key: true
    nullable: false
@bruin */
```


### Development Workflow
- If changing an asset, always run the individual asset using Bruin CLI
- Use `bruin query` commands extensively for testing and validation
- Use `bruin data-diff` commands to compare data between tables
- Run `bruin validate` often when making changes
- Use full paths when running/validating assets

### Data Management
- For new tables, run backfills carefully with appropriate date intervals
- **NEVER** run full-refresh for tables you are not working on
- If adding new columns, run `--full-refresh` for the affected table
- Use `--downstream` flag with `bruin run` to run dependent assets
- Be cautious with date intervals for backfills

### Pipeline Structure
- A Bruin pipeline must contain a `pipeline.yml` file and assets in the `assets/` folder
- Use the existing project structure for new pipelines
- Define appropriate connections in pipeline.yml
- Add proper metadata and documentation to assets

### Testing & Validation
- Test individual assets before running entire pipelines
- Use `bruin validate` to check for issues
- Compare data between environments when appropriate
- Run downstream assets to ensure data consistency

### Safety Guidelines
- Avoid running full pipelines unless specifically requested
- Always run individual assets instead of entire pipelines
- Always run in development environment by setting the `--environment dev` flag, unless specifically asked to run in prod then set `--environment prod`
- Use appropriate flags for different environments
- Document any breaking changes or schema modifications

## Performance & Optimization Rules

### SQL Performance
- Use `LIMIT` clauses when testing queries to avoid large result sets
- Prefer `EXISTS` over `IN` for subqueries when possible
- Use window functions instead of self-joins for ranking/aggregation
- Consider partitioning and clustering for large tables
- Use `QUALIFY` clause for filtering window function results
- Avoid `SELECT *` in production queries - specify only needed columns
- Use `UNPIVOT` instead of multiple `UNION ALL` statements when appropriate

## Development Speed Rules

### Quick Testing
- Always test with small date ranges first (e.g., last 3 days)
- Use `--start-date` and `--end-date` flags for targeted testing
- Create test queries with `LIMIT 10` to verify logic
- Use `bruin query` to quickly validate data before running assets
- Test individual CTEs separately when debugging complex queries

### Code Reusability
- Create reusable SQL functions for common transformations
- Use consistent naming conventions across all assets
- Extract common logic into separate utility functions
- Create template queries for similar data sources
- Document parameterized queries for different environments

### Error Handling
- Always include data quality checks in your queries
- Use `TRY_CAST` and `SAFE_` functions for data type conversions
- Include row count validations in your assets
- Add descriptive error messages for common failure scenarios
- Use `COALESCE` and `IFNULL` for handling NULL values appropriately

### Documentation Standards
- Every columns should have a description and the descriptions should be consistent across assets
- Every special transformation, manipulation, calculation, cleaning, filter, or other logic should be accompanied with inline comment explaining the logic and reasoning
- Document data lineage and dependencies
- Add comments explaining complex business logic
- Include example queries in documentation
- Document any assumptions or limitations

### Environment Management
- Always validate assets before running them
- Use `bruin validate --fast` for quick checks during development
- Test in dev environment before promoting to production
- Keep environment-specific configurations separate
- Use consistent naming for environments (dev, staging, prod)

### Data Quality Rules
- Every primary key should be set to non-nullable
  - Bruin's format is `primary_key: true` and in the next line `nullable: false`
- Include data freshness checks in your assets
- Validate expected row counts and data ranges
- Check for duplicate records where appropriate
- Include data type validations
- Add business rule validations (e.g., positive values, valid dates)

## Naming Conventions & Data Catalog

### General Naming Patterns

#### Table Naming
- **Format**: `{domain}_{function}_{entity}_{granularity}_{type}`
- **Examples**: 
  - `nyctaxi_summary_rides_daily_agg`
  - `nyctaxi_summary_rides_daily_raw`

#### Column Naming Standards
- **Timestamps**: Use descriptive names
  - e.g. `extracted_at`, `inserted_at`, `measured_at`
- **Amounts/Values**: Use `_amount` or `_value` suffix
  - `target_amount`, `total_amount`, `predicted_value`



# Documentation Rules and Guidelines

Follow the following format below for creating, maintaining, and updating the `pipeline.md` file for each pipeline.

1) Pipeline Overview: 3-4 sentence summary of the pipeline, its purpose, and how it works.

2) Pipeline Design: key components of the pipeline, how they work together, and the data flow.
  - Architecture: structure of the assets in the pipeline.
  - Key Features: what/how/why the data is processed and transformed; 5-10 bullet points.
  - Pipeline Configuration: Bruin pipeline.yml configurations

3) Asset Design: list of each tier/category and each asset underneath it, with each asset's configuration/materialization/partition/cluster/etc. details as well as 
  - sensor
    - asset name
      - purpose (e.g. monitor data freshness in source tables)
      - monitored tables
      - logic (e.g. triggers when maximum `inserted_at` is greater than start interval)
      - poke interval (e.g. 30 seconds)
  - tier
    - asset name
      - purpose (e.g. SQL query to process raw xyz data, dedup, normalize, and aggregate to hour level)
      - consumers (e.g. "feeds into layer 5 assets", "read by application server", "read by XYZ API")
      - materialization
        - summary (e.g. table incrementally updated based on extracted_at being in the last 3 days, each hourly run processes the last 3 days of extracted data)
        - table
        - time interval incremental strategy
          - -3d start modifier and 1d end modifier
          - deletes last 3 days of data based on incremental key and reinserts new data
          - incremental key (e.g. extracted_at)
          - time granularity (e.g. timestamp)
          - source tables filtered by interval modifier
            - source table 1 - inside cte_1 `extracted_at` >= "{{ start_datetime }}"
            - source table 2 - inside cte_2 `inserted_at` >= "{{ start_datetime }}"
            - ...
        - partition by (e.g. TIMESTAMP_TRUNC(target_time, DAY))
        - cluster by (e.g. region_id, user_id, product_id)

4) Agent Rules/Instructions and Context:
  - Rules/Instructions:
    - Always follow the `Bruin-Specific Instructions` in the `AGENTS.md` file for asset and pipeline configurations.
    - Strictly follow the recommended format and style guide for SQL queries and documentation outlined in `SQL Development Rules` section of the `AGENTS.md` file.
    - Always use the latest version of the `AGENTS.md` file.
