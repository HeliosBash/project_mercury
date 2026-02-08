#!/usr/bin/env python3
"""
CSV Aggregator - Averages watts and wattHours for rows with the same localTime
Ensures all output files have the same timestamps (fills missing with zeros)
"""

import pandas as pd
from datetime import datetime, timedelta
import sys
import os

def parse_local_datetime(date_str, time_str):
    """
    Convert localDate and localTime to UTC datetime
    Local time is UTC-8 (8 hours behind UTC)
    """
    # Parse the date and time
    dt_str = f"{date_str} {time_str}"
    
    # Try different date formats
    for fmt in ['%m/%d/%Y %H:%M', '%Y-%m-%d %H:%M', '%d/%m/%Y %H:%M']:
        try:
            local_dt = datetime.strptime(dt_str, fmt)
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"Could not parse date/time: {dt_str}")
    
    # Add 8 hours to convert from local (UTC-8) to UTC
    utc_dt = local_dt + timedelta(hours=8)
    
    return utc_dt.strftime('%Y-%m-%d %H:%M:%SZ')

def aggregate_single_file(input_file):
    """
    Read CSV and aggregate rows with same localTime
    Returns a DataFrame with aggregated data
    """
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Get the sourceId for this file
    source_id = df['sourceId'].iloc[0] if len(df) > 0 else None
    
    # Group by localDate and localTime
    grouped = df.groupby(['localDate', 'localTime'], as_index=False).agg({
        'nodeId': 'first',
        'sourceId': 'first',
        'watts': 'mean',
        'wattHours': 'mean'
    })
    
    # Create new UTC created column
    grouped.insert(0, 'created', grouped.apply(
        lambda row: parse_local_datetime(row['localDate'], row['localTime']), 
        axis=1
    ))
    
    # Select only the columns we want
    result = grouped[['created', 'nodeId', 'sourceId', 'localDate', 'localTime', 'watts', 'wattHours']]
    
    return result, source_id

def aggregate_multiple_files(input_files, output_dir=None):
    """
    Process multiple CSV files and ensure they all have the same timestamps
    """
    all_data = []
    source_ids = []
    
    # Process each file
    for input_file in input_files:
        print(f"Processing {input_file}...")
        aggregated, source_id = aggregate_single_file(input_file)
        all_data.append(aggregated)
        source_ids.append(source_id)
        print(f"  - Original rows: {len(pd.read_csv(input_file)) - 1}")
        print(f"  - Aggregated rows: {len(aggregated)}")
    
    # Get all unique timestamps from all files
    all_timestamps = set()
    for df in all_data:
        all_timestamps.update(df[['created', 'localDate', 'localTime']].apply(
            lambda row: (row['created'], row['localDate'], row['localTime']), axis=1
        ).tolist())
    
    # Sort timestamps
    all_timestamps = sorted(list(all_timestamps))
    print(f"\nTotal unique timestamps across all files: {len(all_timestamps)}")
    
    # Create output files with all timestamps
    output_files = []
    for i, (df, source_id, input_file) in enumerate(zip(all_data, source_ids, input_files)):
        # Create a DataFrame with all timestamps
        timestamp_df = pd.DataFrame(all_timestamps, columns=['created', 'localDate', 'localTime'])
        
        # Merge with the actual data
        merged = timestamp_df.merge(df, on=['created', 'localDate', 'localTime'], how='left')
        
        # Fill missing values
        merged['nodeId'] = merged['nodeId'].fillna(df['nodeId'].iloc[0] if len(df) > 0 else 1056)
        merged['sourceId'] = merged['sourceId'].fillna(source_id)
        merged['watts'] = merged['watts'].fillna(0)
        merged['wattHours'] = merged['wattHours'].fillna(0)
        
        # Convert nodeId to int
        merged['nodeId'] = merged['nodeId'].astype(int)
        
        # Determine output filename
        if output_dir:
            base_name = os.path.basename(input_file)
            output_file = os.path.join(output_dir, f"processed_{base_name}")
        else:
            output_file = input_file.replace('.csv', '_processed.csv')
        
        # Write to output file
        merged.to_csv(output_file, index=False)
        output_files.append(output_file)
        
        print(f"\n✓ {os.path.basename(input_file)}")
        print(f"  - Output rows: {len(merged)}")
        print(f"  - Output file: {output_file}")
    
    print(f"\n✓ All files processed successfully!")
    print(f"✓ All output files have {len(all_timestamps)} rows")
    
    return output_files

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python aggregate_csv.py <file1.csv> [file2.csv] [file3.csv] ...")
        print("   or: python aggregate_csv.py <file1.csv> <file2.csv> ... --output-dir <directory>")
        sys.exit(1)
    
    # Parse arguments
    input_files = []
    output_dir = None
    
    i = 1
    while i < len(sys.argv):
        if sys.argv[i] == '--output-dir':
            if i + 1 < len(sys.argv):
                output_dir = sys.argv[i + 1]
                i += 2
            else:
                print("Error: --output-dir requires a directory path")
                sys.exit(1)
        else:
            input_files.append(sys.argv[i])
            i += 1
    
    if not input_files:
        print("Error: No input files specified")
        sys.exit(1)
    
    # Create output directory if specified
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    try:
        aggregate_multiple_files(input_files, output_dir)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
