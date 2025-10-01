#!/usr/bin/env python3
"""
BMRCL Ridership Data Processing Script

This script processes Excel files containing BMRCL ridership data and converts them
to CSV and Parquet formats with normalized structure.

Input files:
- raw/station-hourly.xlsx: Station-wise hourly ridership data
- raw/stationpair-hourly.xlsx: Station pair-wise hourly ridership data
- raw/station-codes.csv: Station code to station name mapping
- raw/station-names.csv: Old station name to station name mapping

Output files:
- data/station-hourly.csv.zip and data/station-hourly.parquet
- data/stationpair-hourly.csv.zip and data/stationpair-hourly.parquet
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime
import re
import zipfile
import io


def load_station_code_mapping(station_codes_file='raw/station-codes.csv'):
    """
    Load station code to name mapping from CSV file.
    
    Returns:
        dict: Mapping from station codes to station names
    """
    try:
        # Read CSV with semicolon separator
        station_df = pd.read_csv(station_codes_file, sep=';')
        # Create mapping dictionary from code to name
        station_code_mapping = dict(zip(station_df['code'], station_df['name']))
        print(f"Loaded {len(station_code_mapping)} station code mappings")
        return station_code_mapping
    except Exception as e:
        print(f"Warning: Could not load station code mapping from {station_codes_file}: {e}")
        return {}


def load_old_name_mapping(station_names_file='raw/station-names.csv'):
    """
    Load old station name to station name mapping from CSV file.
    
    Returns:
        dict: Mapping from old station names to station names
    """
    try:
        # Read CSV with semicolon separator
        names_df = pd.read_csv(station_names_file, sep=';')
        # Create mapping dictionary from old name to name
        old_name_mapping = dict(zip(names_df['alt_name'], names_df['name']))
        print(f"Loaded {len(old_name_mapping)} old station name mappings")
        return old_name_mapping
    except Exception as e:
        print(f"Warning: Could not load station name mapping from {station_names_file}: {e}")
        return {}


def save_dataframe_to_zip(df, zip_path, csv_filename, separator=';'):
    """
    Save a DataFrame directly to a CSV file inside a zip archive.
    """
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Create CSV content in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, sep=separator)
        csv_content = csv_buffer.getvalue()
        
        # Write CSV content to zip file
        zipf.writestr(csv_filename, csv_content)


def process_station_hourly_data(input_file, output_csv_zip, output_parquet, old_name_mapping=None):
    """
    Process station hourly data from Excel to normalized CSV/Parquet format.
    Uses old_name_mapping.
    
    Expected output columns: Date, Hour, Station, Ridership
    """
    print(f"Processing {input_file}...")
    
    # Read the Excel file
    df = pd.read_excel(input_file)
    
    # Use empty mapping if none provided
    if old_name_mapping is None:
        old_name_mapping = {}
    
    # Prepare the output data
    output_data = []
    
    # Get hour columns (all columns except BUSINESS DATE, STATION, and TOTAL)
    hour_columns = [col for col in df.columns if col not in ['BUSINESS DATE', 'STATION', 'TOTAL']]
    
    # Process each row
    for _, row in df.iterrows():
        business_date = row['BUSINESS DATE']
        station = row['STATION']
        
        # Process each hour column
        for hour_col in hour_columns:
            ridership = row[hour_col]
            
            # Extract hour from column name using regex
            # Format: "HH:00 Hrs To     HH:00 Hrs" or "23:00 Hrs To Last train"
            hour_match = re.search(r'(\d{2}):00 Hrs', hour_col)
            if hour_match:
                hour = int(hour_match.group(1))
            else:
                continue  # Skip unknown hour formats
            
            # Handle NaN values
            if pd.isna(ridership):
                ridership = 0
            
            # Apply old name to name mapping
            if '-' in station:
                station_name = station.split('-', 1)[1]
        
            station_name = old_name_mapping.get(station_name, station_name)
            
            output_data.append({
                'Date': business_date,
                'Hour': hour,
                'Station': station_name,
                'Ridership': int(ridership) if not pd.isna(ridership) else 0
            })
    
    # Create output DataFrame
    output_df = pd.DataFrame(output_data)
    
    # Sort by Date, Station, Hour
    output_df = output_df.sort_values(['Date', 'Station', 'Hour'])
    
    # Save to CSV zip file and Parquet
    save_dataframe_to_zip(output_df, output_csv_zip, 'station-hourly.csv')
    output_df.to_parquet(output_parquet, index=False)
    
    print(f"Station hourly data processed: {len(output_df)} records")
    print(f"Saved to {output_csv_zip} and {output_parquet}")
    
    return output_df


def process_stationpair_hourly_data(input_file, output_csv_zip, output_parquet, station_code_mapping=None):
    """
    Process station pair hourly data from Excel to normalized CSV/Parquet format.
    Uses station code to name mapping.
    
    Expected output columns: Date, Hour, Origin Station, Destination Station, Ridership
    """
    print(f"Processing {input_file}...")
    
    # Read the Excel file
    df = pd.read_excel(input_file)
    
    # Use empty mapping if none provided
    if station_code_mapping is None:
        station_code_mapping = {}
    
    # Prepare the output data
    output_data = []
    
    # Get station columns (all columns except BUSINESS DATE and STATION)
    station_columns = [col for col in df.columns if col not in ['BUSINESS DATE', 'STATION']]
    
    # Process each row
    for _, row in df.iterrows():
        business_date_hour = row['BUSINESS DATE']
        origin_station = row['STATION']
        
        # Skip rows with NaN origin station (these are summary rows)
        if pd.isna(origin_station):
            continue
        
        # Extract date and hour from BUSINESS DATE column
        if pd.isna(business_date_hour):
            continue
            
        date_hour_str = str(business_date_hour)
        
        # Parse date and hour
        try:
            # Extract date part (YYYY-MM-DD)
            date_match = re.match(r'(\d{4}-\d{2}-\d{2})', date_hour_str)
            if not date_match:
                continue
            date_str = date_match.group(1)
            
            # Extract hour part
            hour_match = re.search(r'(\d+)Hrs-(\d+)hrs', date_hour_str)
            if not hour_match:
                continue
            hour = int(hour_match.group(1))
            
        except (ValueError, AttributeError):
            continue
        
        # Process each destination station column
        for dest_station_code in station_columns:
            ridership = row[dest_station_code]
            
            # Skip if no ridership data or ridership is 0
            if pd.isna(ridership) or ridership == 0:
                continue
            
            # Apply station code mapping for both stations
            origin_station_name = station_code_mapping.get(origin_station, origin_station)
            dest_station_name = station_code_mapping.get(dest_station_code, dest_station_code)
            
            output_data.append({
                'Date': date_str,
                'Hour': hour,
                'Origin Station': origin_station_name,
                'Destination Station': dest_station_name,
                'Ridership': int(ridership)
            })
    
    # Create output DataFrame
    output_df = pd.DataFrame(output_data)
    
    # Sort by Date, Hour, Origin Station, Destination Station
    output_df = output_df.sort_values(['Date', 'Hour', 'Origin Station', 'Destination Station'])
    
    # Save to CSV zip file and Parquet
    save_dataframe_to_zip(output_df, output_csv_zip, 'stationpair-hourly.csv')
    output_df.to_parquet(output_parquet, index=False)
    
    print(f"Station pair hourly data processed: {len(output_df)} records")
    print(f"Saved to {output_csv_zip} and {output_parquet}")
    
    return output_df


def main():
    """Main function to process both Excel files."""
    
    # Ensure output directory exists
    os.makedirs('data', exist_ok=True)
    
    print("Processing BMRCL Ridership data from Excel files...")
    
    # Load station mappings
    print("Loading station mappings...")
    old_name_mapping = load_old_name_mapping()  # For station hourly data
    station_code_mapping = load_station_code_mapping()  # For station pair hourly data
    
    print()
    
    # Process station hourly data (uses old name to name mapping)
    try:
        station_df = process_station_hourly_data(
            'raw/station-hourly.xlsx',
            'data/station-hourly.csv.zip',
            'data/station-hourly.parquet',
            old_name_mapping
        )
        print(f"Station hourly data shape: {station_df.shape}")
    except Exception as e:
        print(f"Error processing station hourly data: {e}")
    
    print()
    
    # Process station pair hourly data (uses station code mapping)
    try:
        stationpair_df = process_stationpair_hourly_data(
            'raw/stationpair-hourly.xlsx',
            'data/stationpair-hourly.csv.zip',
            'data/stationpair-hourly.parquet',
            station_code_mapping
        )
        print(f"Station pair hourly data shape: {stationpair_df.shape}")
    except Exception as e:
        print(f"Error processing station pair hourly data: {e}")
    
    print("Processing complete!")


if __name__ == "__main__":
    main()