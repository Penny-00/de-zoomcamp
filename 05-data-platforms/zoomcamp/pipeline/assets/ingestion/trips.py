"""@bruin
name: ingestion.trips
type: python
image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: VendorID
    type: integer
    description: "A code indicating the provider."
  - name: pickup_datetime
    type: timestamp
    description: "The date and time when the meter was engaged."
  - name: dropoff_datetime
    type: timestamp
    description: "The date and time when the meter was disengaged."
  - name: passenger_count
    type: float
    description: "The number of passengers in the vehicle."
  - name: trip_distance
    type: float
    description: "The elapsed trip distance in miles."
  - name: RatecodeID
    type: float
    description: "The final rate code in effect."
  - name: store_and_fwd_flag
    type: string
    description: "Flag indicating store and forward."
  - name: PULocationID
    type: integer
    description: "TLC Taxi Zone of pickup location."
  - name: DOLocationID
    type: integer
    description: "TLC Taxi Zone of dropoff location."
  - name: payment_type
    type: float
    description: "Payment type."
  - name: fare_amount
    type: float
    description: "Fare amount."
  - name: extra
    type: float
    description: "Extra charges."
  - name: mta_tax
    type: float
    description: "MTA tax."
  - name: tip_amount
    type: float
    description: "Tip amount."
  - name: tolls_amount
    type: float
    description: "Tolls amount."
  - name: improvement_surcharge
    type: float
    description: "Improvement surcharge."
  - name: total_amount
    type: float
    description: "Total amount."
  - name: congestion_surcharge
    type: float
    description: "Congestion surcharge."
  - name: airport_fee
    type: float
    description: "Airport fee."
  - name: ehail_fee
    type: float
    description: "Ehail fee."
  - name: trip_type
    type: float
    description: "Trip type."
  - name: taxi_type
    type: string
    description: "Taxi type (yellow or green)."
  - name: extracted_at
    type: timestamp
    description: "Time of extraction."
@bruin"""

import os
import json
from datetime import datetime
import pandas as pd
from dateutil.parser import parse

def get_year_months_to_fetch(start_date_str, end_date_str):
    start_dt = parse(start_date_str)
    end_dt = parse(end_date_str)
    
    current_dt = datetime(start_dt.year, start_dt.month, 1)
    
    year_months = []
    while current_dt <= end_dt:
        if current_dt < end_dt or (start_dt == end_dt and current_dt.year == start_dt.year and current_dt.month == start_dt.month):
            year_months.append(f"{current_dt.year}-{current_dt.month:02d}")
        
        if current_dt.month == 12:
            current_dt = datetime(current_dt.year + 1, 1, 1)
        else:
            current_dt = datetime(current_dt.year, current_dt.month + 1, 1)
            
    if not year_months:
        year_months.append(f"{start_dt.year}-{start_dt.month:02d}")
        
    return list(sorted(set(year_months)))

def materialize():
    # Retrieve environment variables for the date window
    start_date_str = os.environ.get("BRUIN_START_DATE")
    end_date_str = os.environ.get("BRUIN_END_DATE")
    
    if not start_date_str or not end_date_str:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE environment variables must be set.")
        
    # Retrieve pipeline variables
    bruin_vars_str = os.environ.get("BRUIN_VARS")
    if bruin_vars_str:
        try:
            bruin_vars = json.loads(bruin_vars_str)
            taxi_types = bruin_vars.get("taxi_types", ["green"])
        except Exception as e:
            print(f"Error parsing BRUIN_VARS: {e}")
            taxi_types = ["green"]
    else:
        taxi_types = ["green"]
        
    # Ensure taxi_types is a list/iterable
    if isinstance(taxi_types, str):
        try:
            taxi_types = json.loads(taxi_types)
        except Exception:
            taxi_types = [t.strip() for t in taxi_types.split(",") if t.strip()]
            
    year_months = get_year_months_to_fetch(start_date_str, end_date_str)
    print(f"Ingesting data for taxi types: {taxi_types} and months: {year_months}")
    
    dfs = []
    for taxi_type in taxi_types:
        for ym in year_months:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{ym}.parquet"
            print(f"Fetching {taxi_type} taxi data for {ym} from {url}")
            try:
                df = pd.read_parquet(url)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to fetch data for taxi type '{taxi_type}' for month '{ym}' from URL '{url}': {e}"
                )
                
            # Rename columns to standardized names
            datetime_rename = {
                'lpep_pickup_datetime': 'pickup_datetime',
                'tpep_pickup_datetime': 'pickup_datetime',
                'lpep_dropoff_datetime': 'dropoff_datetime',
                'tpep_dropoff_datetime': 'dropoff_datetime'
            }
            df = df.rename(columns=datetime_rename)
            
            # Add metadata columns
            df['taxi_type'] = taxi_type
            df['extracted_at'] = datetime.utcnow()
            
            dfs.append(df)
            
    expected_cols = [
        'VendorID', 'pickup_datetime', 'dropoff_datetime', 'store_and_fwd_flag',
        'RatecodeID', 'PULocationID', 'DOLocationID', 'passenger_count',
        'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
        'tolls_amount', 'improvement_surcharge', 'total_amount',
        'payment_type', 'congestion_surcharge', 'airport_fee', 'ehail_fee', 'trip_type',
        'taxi_type', 'extracted_at'
    ]

    if not dfs:
        # Return an empty DataFrame with the expected columns
        return pd.DataFrame(columns=expected_cols)
        
    final_df = pd.concat(dfs, ignore_index=True)
    
    # Ensure all expected columns exist to maintain schema compatibility
    for col in expected_cols:
        if col not in final_df.columns:
            final_df[col] = None
            
    # Order columns to match the expected schema
    final_df = final_df[expected_cols]
    return final_df
