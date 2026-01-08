"""@bruin
name: ingestion.ingest_trips_python
uri: neptune.ingestion.ingest_trips_python
type: python
image: python:3.11
connection: duckdb-default
description: |
  Ingests NYC taxi trip data from HTTP parquet files using Python requests library.
  Loops through all months between interval start/end dates and combines the data.
  Uses Bruin Python materialization - returns a Pandas DataFrame and Bruin automatically
  handles insertion into DuckDB based on the materialization strategy.

  This approach:
  - Downloads parquet files from HTTP URLs for all months in the date range
  - Combines data from multiple months into a single DataFrame
  - Adds taxi_type column to track which taxi type each record represents
  - Keeps data as raw as possible - preserves original column names from parquet files
  - Column normalization (vendor_id -> vendorid, etc.) is handled in tier_1 transformation layer
  - Returns DataFrame for Bruin to materialize into DuckDB table

owner: data-engineering
tags:
  - ingestion
  - nyc-taxi
  - raw-data
  - python-ingestion

materialization:
  type: table
  strategy: create+replace

@bruin"""

import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import io
import os


def generate_month_range(start_date_str: str, end_date_str: str) -> list[tuple[int, int]]:
    """
    Generate list of (year, month) tuples for all months between start and end dates (inclusive).

    Args:
        start_date_str: Start date in YYYY-MM-DD format
        end_date_str: End date in YYYY-MM-DD format

    Returns:
        List of (year, month) tuples
    """
    start_date = datetime.strptime(start_date_str[:10], '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str[:10], '%Y-%m-%d')

    months = []
    current = start_date.replace(day=1)
    end_month = end_date.replace(day=1)

    while current <= end_month:
        months.append((current.year, current.month))
        current += relativedelta(months=1)

    return months


def materialize(start_date=None, end_date=None, **kwargs):
    """
    Materialize function that returns a Pandas DataFrame.
    Bruin will automatically insert this DataFrame into DuckDB based on materialization strategy.
    """
    # Get dates from Bruin environment variables (BRUIN_START_DATE and BRUIN_END_DATE are YYYY-MM-DD format)
    start_date_str = (
        start_date 
        or os.environ.get('BRUIN_START_DATE') 
        or os.environ.get('START_DATE') 
        or kwargs.get('start_date')
    )
    end_date_str = (
        end_date 
        or os.environ.get('BRUIN_END_DATE') 
        or os.environ.get('END_DATE') 
        or kwargs.get('end_date')
    )
    
    if not start_date_str or not end_date_str:
        raise ValueError("start_date and end_date must be provided via BRUIN_START_DATE/BRUIN_END_DATE or function parameters")
    
    # Get taxi_type (default to 'yellow')
    taxi_type = os.environ.get('taxi_type') or kwargs.get('taxi_type', 'yellow')
    
    # Generate list of months to process
    months = generate_month_range(start_date_str, end_date_str)
    
    # Download and combine parquet files
    all_dataframes = []
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    
    for year, month in months:
        url = f'{base_url}/{taxi_type}_tripdata_{year}-{month:02d}.parquet'
        
        try:
            response = requests.get(url, timeout=300)
            response.raise_for_status()
            
            df = pd.read_parquet(io.BytesIO(response.content))
            df['taxi_type'] = taxi_type
            
            all_dataframes.append(df)
            print(f"Successfully downloaded {year}-{month:02d}: {len(df)} rows")
            
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {year}-{month:02d}: {e}")
            continue
        except Exception as e:
            print(f"Error processing {year}-{month:02d}: {e}")
            continue
    
    if not all_dataframes:
        return pd.DataFrame(columns=['taxi_type'])
    
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"Total rows combined: {len(combined_df)}")
    
    return combined_df
