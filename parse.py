#!/usr/bin/env python3
"""
BMRCL Ridership Data Processing Script

This script processes Excel files containing BMRCL ridership data and converts them
to CSV and Parquet formats with normalized structure.

The script automatically detects and handles two different Excel formats:
- Format v1 (August 2025): Single sheet with columns like "00:00 Hrs To 01:00 Hrs"
- Format v2 (September 2025+): Multiple sheets with columns like H00, H01, ..., H23

Input files:
- raw/august/station-hourly.xlsx: Station-wise hourly ridership data (August)
- raw/august/stationpair-hourly.xlsx: Station pair-wise hourly ridership data (August)
- raw/september/station-hourly.xlsx: Station-wise hourly ridership data (September)
- raw/station-codes.csv: Station code to station name mapping
- raw/station-names.csv: Old station name to station name mapping

Output files:
- data/station-hourly.csv.zip and data/station-hourly.parquet (entry ridership)
- data/station-hourly-exits.csv.zip and data/station-hourly-exits.parquet (exit ridership)
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


def process_station_hourly_data_v1(input_file, old_name_mapping=None):
    """
    Process station hourly data from Excel (version 1 format with single sheet and hour range columns).
    This handles the old format used in August 2025 and earlier.
    
    Expected input format:
    - Single sheet
    - Hour columns: "00:00 Hrs To 01:00 Hrs", "23:00 Hrs To Last train", etc.
    - Columns: BUSINESS DATE, STATION, hour range columns, TOTAL
    
    Returns:
        DataFrame with columns: Date, Hour, Station, Ridership
    """
    print(f"Processing {input_file} (v1 format)...")
    
    # Use empty mapping if none provided
    if old_name_mapping is None:
        old_name_mapping = {}
    
    # Read the Excel file
    df = pd.read_excel(input_file)
    
    # Prepare the output data for this file
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
            else:
                station_name = station
            
            station_name = old_name_mapping.get(station_name, station_name)
            
            output_data.append({
                'Date': business_date,
                'Hour': hour,
                'Station': station_name,
                'Ridership': int(ridership) if not pd.isna(ridership) else 0
            })
    
    # Create output DataFrame
    output_df = pd.DataFrame(output_data)
    
    print(f"  Processed {len(output_df)} records from {input_file}")
    
    return output_df


def process_station_hourly_data_v2(input_file, old_name_mapping=None):
    """
    Process station hourly data from Excel (version 2 format with multiple sheets and H00-H23 columns).
    This handles the new format used in September 2025 and later.
    
    Expected input format:
    - Multiple sheets (e.g., 'Sep-2025 Entry', 'Sep-2025 Exit')
    - Hour columns: H00, H01, H02, ..., H23
    - Columns: BUSINESS DATE, STATION, H00-H23, TOTAL
    
    Note: Only processes sheets containing 'Entry' in the name. Exit sheets are skipped.
    
    Returns:
        DataFrame with columns: Date, Hour, Station, Ridership
    """
    print(f"Processing {input_file} (v2 format)...")
    
    # Use empty mapping if none provided
    if old_name_mapping is None:
        old_name_mapping = {}
    
    # Read the Excel file to get sheet names
    xl = pd.ExcelFile(input_file)
    print(f"  Found sheets: {xl.sheet_names}")
    
    # Filter to only Entry sheets (skip Exit sheets)
    entry_sheets = [sheet for sheet in xl.sheet_names if 'Entry' in sheet]
    print(f"  Processing Entry sheets only: {entry_sheets}")
    
    # Prepare the output data
    output_data = []
    
    # Process each entry sheet
    for sheet_name in entry_sheets:
        print(f"  Processing sheet: {sheet_name}")
        df = pd.read_excel(input_file, sheet_name=sheet_name)
        
        # Get hour columns (H00 through H23)
        hour_columns = [col for col in df.columns if col.startswith('H') and col[1:].isdigit() and len(col) == 3]
        
        # Process each row
        for _, row in df.iterrows():
            business_date = row['BUSINESS DATE']
            station = row['STATION']
            
            # Process each hour column
            for hour_col in hour_columns:
                ridership = row[hour_col]
                
                # Extract hour from column name (e.g., 'H00' -> 0, 'H23' -> 23)
                hour = int(hour_col[1:])
                
                # Handle NaN values
                if pd.isna(ridership):
                    ridership = 0
                
                # Apply old name to name mapping
                if '-' in station:
                    station_name = station.split('-', 1)[1]
                else:
                    station_name = station
                
                station_name = old_name_mapping.get(station_name, station_name)
                
                output_data.append({
                    'Date': business_date,
                    'Hour': hour,
                    'Station': station_name,
                    'Ridership': int(ridership) if not pd.isna(ridership) else 0
                })
    
    # Create output DataFrame
    output_df = pd.DataFrame(output_data)
    
    print(f"  Processed {len(output_df)} records from {input_file}")
    
    return output_df


def process_station_hourly_exits_v1(input_file, station_code_mapping=None):
    """
    Process station hourly exit data from station-pair Excel data (version 1 format).
    Sums all destination station ridership for each origin-date-hour combination.
    Ensures all hours (0-23) are present for each station-date combination.
    
    Expected input format:
    - Single sheet with station pairs
    - BUSINESS DATE column with format: "YYYY-MM-DD HHHrs-HHhrs"
    - STATION column (origin station)
    - Multiple destination station columns
    
    Returns:
        DataFrame with columns: Date, Hour, Station, Ridership
    """
    print(f"Processing exits from {input_file} (v1 format)...")
    
    # Use empty mapping if none provided
    if station_code_mapping is None:
        station_code_mapping = {}
    
    # Read the Excel file
    df = pd.read_excel(input_file)
    
    # Prepare the output data
    exit_data = {}  # Key: (date, hour, station), Value: total ridership
    date_station_combinations = set()  # Track all date-station combinations
    
    # Get station columns (all columns except BUSINESS DATE and STATION)
    station_columns = [col for col in df.columns if col not in ['BUSINESS DATE', 'STATION']]
    
    # Process each row
    for _, row in df.iterrows():
        business_date_hour = row['BUSINESS DATE']
        origin_station = row['STATION']
        
        # Skip rows with NaN origin station
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
            
            # Apply station code mapping for destination station
            dest_station_name = station_code_mapping.get(dest_station_code, dest_station_code)
            
            # Track this date-station combination
            date_station_combinations.add((date_str, dest_station_name))
            
            # Skip if no ridership data
            if pd.isna(ridership) or ridership == 0:
                continue
            
            # Aggregate by destination station (this is the exit station)
            key = (date_str, hour, dest_station_name)
            exit_data[key] = exit_data.get(key, 0) + int(ridership)
    
    # Ensure all hours (0-23) exist for each date-station combination
    output_data = []
    for date_str, station in date_station_combinations:
        for hour in range(24):
            key = (date_str, hour, station)
            ridership = exit_data.get(key, 0)
            output_data.append({
                'Date': date_str,
                'Hour': hour,
                'Station': station,
                'Ridership': ridership
            })
    
    # Create output DataFrame
    output_df = pd.DataFrame(output_data)
    
    print(f"  Processed {len(output_df)} records from {input_file}")
    
    return output_df


def process_station_hourly_exits_v2(input_file, old_name_mapping=None):
    """
    Process station hourly exit data from Excel (version 2 format with Exit sheets).
    This handles the new format used in September 2025 and later.
    Ensures all hours (0-23) are present for each station-date combination.
    
    Expected input format:
    - Multiple sheets (e.g., 'Sep-2025 Entry', 'Sep-2025 Exit')
    - Hour columns: H00, H01, H02, ..., H23
    - Columns: BUSINESS DATE, STATION, H00-H23, TOTAL
    
    Note: Only processes sheets containing 'Exit' in the name. Entry sheets are skipped.
    
    Returns:
        DataFrame with columns: Date, Hour, Station, Ridership
    """
    print(f"Processing {input_file} (v2 format, Exit sheets)...")
    
    # Use empty mapping if none provided
    if old_name_mapping is None:
        old_name_mapping = {}
    
    # Read the Excel file to get sheet names
    xl = pd.ExcelFile(input_file)
    print(f"  Found sheets: {xl.sheet_names}")
    
    # Filter to only Exit sheets (skip Entry sheets)
    exit_sheets = [sheet for sheet in xl.sheet_names if 'Exit' in sheet]
    print(f"  Processing Exit sheets only: {exit_sheets}")
    
    # Prepare the output data
    output_data = []
    
    # Process each exit sheet
    for sheet_name in exit_sheets:
        print(f"  Processing sheet: {sheet_name}")
        df = pd.read_excel(input_file, sheet_name=sheet_name)
        
        # Get hour columns (H00 through H23)
        hour_columns = [col for col in df.columns if col.startswith('H') and col[1:].isdigit() and len(col) == 3]
        
        # Process each row
        for _, row in df.iterrows():
            business_date = row['BUSINESS DATE']
            station = row['STATION']
            
            # Apply old name to name mapping
            if '-' in station:
                station_name = station.split('-', 1)[1]
            else:
                station_name = station
            
            station_name = old_name_mapping.get(station_name, station_name)
            
            # Ensure all hours 0-23 are present
            for hour in range(24):
                hour_col = f'H{hour:02d}'
                
                # Get ridership from the column if it exists
                if hour_col in hour_columns:
                    ridership = row[hour_col]
                    # Handle NaN values
                    if pd.isna(ridership):
                        ridership = 0
                else:
                    # Hour column doesn't exist in the data
                    ridership = 0
                
                output_data.append({
                    'Date': business_date,
                    'Hour': hour,
                    'Station': station_name,
                    'Ridership': int(ridership) if not pd.isna(ridership) else 0
                })
    
    # Create output DataFrame
    output_df = pd.DataFrame(output_data)
    
    print(f"  Processed {len(output_df)} records from {input_file}")
    
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
    """Main function to process all Excel files."""
    
    # Ensure output directory exists
    os.makedirs('data', exist_ok=True)
    
    print("Processing BMRCL Ridership data from Excel files...")
    
    # Load station mappings
    print("Loading station mappings...")
    old_name_mapping = load_old_name_mapping()  # For station hourly data
    station_code_mapping = load_station_code_mapping()  # For station pair hourly data
    
    print()
    
    # Process station hourly data from multiple months
    # Collect all station hourly files to process
    station_hourly_files = [
        'raw/august/station-hourly.xlsx',
        'raw/september/station-hourly.xlsx'
    ]
    
    all_station_data = []
    
    for input_file in station_hourly_files:
        if not os.path.exists(input_file):
            print(f"Warning: File not found: {input_file}, skipping...")
            continue
            
        try:
            # Detect file format by checking the first sheet
            xl = pd.ExcelFile(input_file)
            first_sheet_df = pd.read_excel(input_file, sheet_name=xl.sheet_names[0], nrows=1)
            
            # Check if it's the new format (H00-H23 columns) or old format
            has_h_columns = any(col.startswith('H') and col[1:].isdigit() and len(col) == 3 
                               for col in first_sheet_df.columns)
            
            if has_h_columns:
                # Use the v2 processing function for files with H00-H23 format
                file_df = process_station_hourly_data_v2(input_file, old_name_mapping)
            else:
                # Use the v1 processing function for files with "HH:00 Hrs To..." format
                file_df = process_station_hourly_data_v1(input_file, old_name_mapping)
            
            all_station_data.append(file_df)
            
        except Exception as e:
            print(f"Error processing {input_file}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Combine all station hourly data
    if all_station_data:
        print("\nCombining all station hourly data...")
        combined_station_df = pd.concat(all_station_data, ignore_index=True)
        
        # Sort by Date, Station, Hour
        combined_station_df = combined_station_df.sort_values(['Date', 'Station', 'Hour'])
        
        # Save to CSV zip file and Parquet
        save_dataframe_to_zip(combined_station_df, 'data/station-hourly.csv.zip', 'station-hourly.csv')
        combined_station_df.to_parquet('data/station-hourly.parquet', index=False)
        
        print(f"Combined station hourly data: {len(combined_station_df)} records")
        print(f"Station hourly data shape: {combined_station_df.shape}")
        print(f"Saved to data/station-hourly.csv.zip and data/station-hourly.parquet")
    else:
        print("No station hourly data files were processed successfully.")
    
    print()
    
    # Process station hourly exit data from multiple months
    print("Processing station hourly exit data...")
    
    # For August (v1 format): Use station-pair data to derive exits
    all_exit_data = []
    
    # Process August exits from station-pair data
    august_stationpair_file = 'raw/august/stationpair-hourly.xlsx'
    if os.path.exists(august_stationpair_file):
        try:
            file_df = process_station_hourly_exits_v1(august_stationpair_file, station_code_mapping)
            all_exit_data.append(file_df)
        except Exception as e:
            print(f"Error processing exits from {august_stationpair_file}: {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"Warning: File not found: {august_stationpair_file}, skipping...")
    
    # Process September exits from v2 format with Exit sheets
    september_file = 'raw/september/station-hourly.xlsx'
    if os.path.exists(september_file):
        try:
            # Check if file has Exit sheets (v2 format)
            xl = pd.ExcelFile(september_file)
            has_exit_sheets = any('Exit' in sheet for sheet in xl.sheet_names)
            
            if has_exit_sheets:
                file_df = process_station_hourly_exits_v2(september_file, old_name_mapping)
                all_exit_data.append(file_df)
            else:
                print(f"  No Exit sheets found in {september_file}, skipping exit processing for this file")
        except Exception as e:
            print(f"Error processing exits from {september_file}: {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"Warning: File not found: {september_file}, skipping...")
    
    # Combine all station hourly exit data
    if all_exit_data:
        print("\nCombining all station hourly exit data...")
        combined_exit_df = pd.concat(all_exit_data, ignore_index=True)
        
        # Sort by Date, Station, Hour
        combined_exit_df = combined_exit_df.sort_values(['Date', 'Station', 'Hour'])
        
        # Save to CSV zip file and Parquet
        save_dataframe_to_zip(combined_exit_df, 'data/station-hourly-exits.csv.zip', 'station-hourly-exits.csv')
        combined_exit_df.to_parquet('data/station-hourly-exits.parquet', index=False)
        
        print(f"Combined station hourly exit data: {len(combined_exit_df)} records")
        print(f"Station hourly exit data shape: {combined_exit_df.shape}")
        print(f"Saved to data/station-hourly-exits.csv.zip and data/station-hourly-exits.parquet")
    else:
        print("No station hourly exit data files were processed successfully.")
    
    print()
    
    # Process station pair hourly data (uses station code mapping)
    try:
        stationpair_df = process_stationpair_hourly_data(
            'raw/august/stationpair-hourly.xlsx',
            'data/stationpair-hourly.csv.zip',
            'data/stationpair-hourly.parquet',
            station_code_mapping
        )
        print(f"Station pair hourly data shape: {stationpair_df.shape}")
    except Exception as e:
        print(f"Error processing station pair hourly data: {e}")
    
    print("\nProcessing complete!")


if __name__ == "__main__":
    main()