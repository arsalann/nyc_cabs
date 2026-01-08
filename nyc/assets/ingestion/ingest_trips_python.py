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
  - Returns DataFrame for Bruin to materialize into DuckDB table
  
  Sample query:
  ```sql
  SELECT *
  FROM ingestion.ingest_trips_python
  WHERE 1=1
  LIMIT 10
  ```

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


def materialize(start_date=None, end_date=None, **kwargs):
    """
    Materialize function that returns a Pandas DataFrame.
    Bruin will automatically insert this DataFrame into DuckDB based on materialization strategy.
    
    Process:
    1. Parse start_date and end_date from Bruin interval
    2. Generate list of all months between start and end (inclusive)
    3. Download parquet files from HTTP URLs for each month
    4. Read parquet files into Pandas DataFrames
    5. Add taxi_type column to each DataFrame
    6. Combine all DataFrames into a single DataFrame
    7. Return combined DataFrame for Bruin to materialize
    """
    # Try multiple ways to get dates (Bruin may pass them differently)
    start_date_str = start_date or os.environ.get('START_DATE') or os.environ.get('start_date') or kwargs.get('start_date') or kwargs.get('START_DATE')
    end_date_str = end_date or os.environ.get('END_DATE') or os.environ.get('end_date') or kwargs.get('end_date') or kwargs.get('END_DATE')
    taxi_type = os.environ.get('taxi_type') or kwargs.get('taxi_type', 'yellow')
    
    if not start_date_str or not end_date_str:
        # Note: Bruin may not pass dates to Python assets automatically
        # For now, use the date range from command line arguments
        # TODO: Investigate how to access Bruin's interval dates in Python assets
        # If dates still not found, this will fail - dates should be provided via --start-date and --end-date flags
        raise ValueError(f"start_date and end_date must be provided. Check that --start-date and --end-date flags are used when running the asset.")
    
    # Parse dates from strings
    # Handle ISO format with Z or timezone
    start_date_clean = start_date_str.replace('Z', '+00:00') if 'Z' in start_date_str else start_date_str
    start_date = datetime.fromisoformat(start_date_clean.replace('T', ' ').split('.')[0])
    
    end_date_clean = end_date_str.replace('Z', '+00:00') if 'Z' in end_date_str else end_date_str
    end_date = datetime.fromisoformat(end_date_clean.replace('T', ' ').split('.')[0])
    
    # Generate list of months to process
    months = []
    current = start_date.replace(day=1)  # Start of month
    end_month = end_date.replace(day=1)
    
    while current <= end_month:
        months.append((current.year, current.month))
        current += relativedelta(months=1)
    
    # Download and combine parquet files
    all_dataframes = []
    
    for year, month in months:
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet'
        
        try:
            # Download parquet file
            response = requests.get(url, timeout=300)
            response.raise_for_status()
            
            # Read parquet from bytes
            df = pd.read_parquet(io.BytesIO(response.content))
            
            # Rename columns to match expected schema (parquet files use snake_case, we need lowercase)
            # Map common column name variations to expected names
            column_mapping = {
                'vendor_id': 'vendorid',
                'vendorID': 'vendorid',
                'VendorID': 'vendorid',
                'ratecode_id': 'ratecodeid',
                'ratecodeID': 'ratecodeid',
                'RatecodeID': 'ratecodeid',
                'pu_location_id': 'pulocationid',
                'PULocationID': 'pulocationid',
                'do_location_id': 'dolocationid',
                'DOLocationID': 'dolocationid',
                'payment_type': 'payment_type',  # Keep as is if already correct
                'Payment_type': 'payment_type',
            }
            
            # Rename columns that exist in the mapping
            df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
            
            # Add taxi_type column
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
        # Return empty DataFrame with expected schema
        return pd.DataFrame(columns=['taxi_type'])
    
    # Combine all dataframes
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"Total rows combined: {len(combined_df)}")
    
    return combined_df
