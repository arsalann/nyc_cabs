"""@bruin
name: tier_1.ingest_trips
type: python
description: Ingest raw trip data from HTTP parquet files using Python. Downloads NYC taxi trip data from NYC TLC Trip Record Data, adds taxi_type and extracted_at columns, and preserves original column names from parquet files.
owner: nyc-taxi-team
tags:
  - tier_1
  - ingestion
  - python
materialization:
  type: table
  strategy: create+replace
connection: duckdb-default
@bruin"""

import os
import json
from datetime import datetime
from typing import List, Tuple
import pandas as pd
import requests
from io import BytesIO


def generate_month_range(start_date: str, end_date: str) -> List[Tuple[int, int]]:
    """Convert date range to list of (year, month) tuples.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format (inclusive)
    
    Returns:
        List of (year, month) tuples
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    months = []
    current = start.replace(day=1)  # Start from first day of start month
    
    while current <= end:
        months.append((current.year, current.month))
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    return months


def download_parquet_file(url: str) -> pd.DataFrame:
    """Download and read a parquet file from URL.
    
    Args:
        url: URL to the parquet file
    
    Returns:
        Pandas DataFrame with the parquet file contents
    """
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return pd.read_parquet(BytesIO(response.content))
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error


def materialize() -> pd.DataFrame:
    """Main materialization function that returns the DataFrame to be materialized.
    
    This function is called by Bruin to get the data to materialize into a table.
    It reads BRUIN_START_DATE and BRUIN_END_DATE from environment variables,
    gets taxi_types from BRUIN_VARS, and downloads parquet files accordingly.
    
    Returns:
        Pandas DataFrame with all trip data
    """
    # Get date range from environment variables
    start_date = os.environ.get("BRUIN_START_DATE", "2022-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2022-01-31")
    
    # Get taxi types from pipeline variables
    taxi_types = ["yellow"]  # Default
    if "BRUIN_VARS" in os.environ:
        vars_data = json.loads(os.environ["BRUIN_VARS"])
        taxi_types = vars_data.get("taxi_types", ["yellow"])
    
    # Generate list of months to process
    months = generate_month_range(start_date, end_date)
    
    # Base URL for NYC TLC trip data
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    all_dataframes = []
    
    # Download data for each taxi type and month
    for taxi_type in taxi_types:
        for year, month in months:
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            url = f"{base_url}{filename}"
            
            print(f"Downloading {filename}...")
            df = download_parquet_file(url)
            
            if not df.empty:
                # Add taxi_type column
                df["taxi_type"] = taxi_type
                # Add extracted_at timestamp
                df["extracted_at"] = datetime.now()
                all_dataframes.append(df)
            else:
                print(f"Skipping {filename} (empty or failed download)")
    
    # Combine all DataFrames
    if all_dataframes:
        result = pd.concat(all_dataframes, ignore_index=True)
        print(f"Total rows ingested: {len(result)}")
        return result
    else:
        print("No data was downloaded")
        return pd.DataFrame()

