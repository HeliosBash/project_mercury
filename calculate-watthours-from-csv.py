#!/usr/bin/env python3
"""
CSV WattHours Accumulator with Date Conversion

Reads a CSV file, converts dates to UTC, multiplies watts/wattHours by 1000,
replaces empty values with 0, and accumulates wattHours values.

Usage:
    python script.py input.csv "America/New_York"
    python script.py input.csv "Asia/Manila" --output output.csv
"""

import argparse
import csv
import sys
import platform
from datetime import datetime
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
    USE_PYTZ = False
except ImportError:
    try:
        import pytz
        USE_PYTZ = True
    except ImportError:
        print("Error: Please install pytz for Python < 3.9: pip install pytz")
        sys.exit(1)


def find_date_column(headers):
    """Identify the date column from CSV headers."""
    date_keywords = ['date', 'datetime', 'timestamp', 'time', 'created', 'modified']
    
    # Try exact matches first
    for header in headers:
        if header.lower() in date_keywords:
            return header
    
    # Try partial matches
    for header in headers:
        for keyword in date_keywords:
            if keyword in header.lower():
                return header
    
    return None


# Date format configurations
DATE_FORMAT = '%-m/%-d/%Y %H:%M:%S'  # Unix/Mac
DATE_FORMAT_WIN = '%#m/%#d/%Y %H:%M:%S'  # Windows
DATE_FORMAT_PADDED = '%m/%d/%Y %H:%M:%S'  # Fallback


def convert_to_utc(date_str, source_tz_obj, utc_tz_obj, date_format):
    """Convert a date string from source timezone to UTC."""
    date_str = date_str.strip()
    
    # Try to parse with platform-specific format
    try:
        dt = datetime.strptime(date_str, date_format)
    except ValueError:
        # Fallback to padded format
        dt = datetime.strptime(date_str, DATE_FORMAT_PADDED)
    
    # Convert to UTC
    if USE_PYTZ:
        dt_local = source_tz_obj.localize(dt)
        dt_utc = dt_local.astimezone(utc_tz_obj)
    else:
        dt_local = dt.replace(tzinfo=source_tz_obj)
        dt_utc = dt_local.astimezone(utc_tz_obj)
    
    return dt_utc.strftime('%Y-%m-%d %H:%M:%S')


def process_csv(input_file, timezone, output_file=None):
    """
    Process CSV file: convert dates to UTC, multiply watts/wattHours by 1000,
    replace empty values with 0, and accumulate wattHours.
    """
    # Generate output filename if not provided
    if output_file is None:
        input_path = Path(input_file)
        output_file = input_path.stem + '_accumulated' + input_path.suffix
    
    # Setup timezone objects
    if USE_PYTZ:
        source_tz_obj = pytz.timezone(timezone)
        utc_tz_obj = pytz.UTC
    else:
        source_tz_obj = ZoneInfo(timezone)
        utc_tz_obj = ZoneInfo('UTC')
    
    # Determine date format based on platform
    if platform.system() == 'Windows':
        date_format = DATE_FORMAT_WIN
    else:
        date_format = DATE_FORMAT
    
    try:
        with open(input_file, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            headers = reader.fieldnames
            
            if not headers:
                print("Error: CSV file has no headers")
                sys.exit(1)
            
            # Verify wattHours column exists
            if 'wattHours' not in headers:
                print("Error: CSV must contain 'wattHours' column")
                sys.exit(1)
            
            # Find date column
            date_column = find_date_column(headers)
            if date_column:
                print(f"Found date column: '{date_column}'")
                print(f"Converting from timezone: {timezone} to UTC")
            else:
                print("Warning: No date column found, skipping date conversion")
            
            # Check for watts column
            has_watts = 'watts' in headers
            
            print(f"Processing file: {input_file}")
            print(f"Output file: {output_file}")
            
            # Process rows
            accumulated_rows = []
            cumulative_watthours = 0
            rows_processed = 0
            errors = []
            
            for i, row in enumerate(reader, start=2):
                try:
                    # Convert date to UTC if date column exists
                    if date_column and row.get(date_column):
                        row[date_column] = convert_to_utc(
                            row[date_column], 
                            source_tz_obj, 
                            utc_tz_obj, 
                            date_format
                        )
                    
                    # Process watts column: replace empty with 0 and multiply by 1000
                    if has_watts:
                        watts_val = row['watts'].strip() if row['watts'] else ''
                        if watts_val == '':
                            row['watts'] = 0
                        else:
                            try:
                                row['watts'] = float(watts_val) * 1000
                            except ValueError:
                                print(f"Warning: Invalid watts value '{watts_val}' in row {i}, setting to 0")
                                row['watts'] = 0
                    
                    # Process wattHours: replace empty with 0, multiply by 1000, accumulate
                    watthours_val = row['wattHours'].strip() if row['wattHours'] else ''
                    if watthours_val == '':
                        watthours = 0
                    else:
                        try:
                            watthours = float(watthours_val) * 1000
                            # If negative, treat as 0
                            if watthours < 0:
                                watthours = 0
                        except ValueError:
                            print(f"Warning: Invalid wattHours value '{watthours_val}' in row {i}, treating as 0")
                            watthours = 0
                    
                    # Accumulate wattHours
                    cumulative_watthours += watthours
                    row['wattHours'] = cumulative_watthours
                    
                    accumulated_rows.append(row)
                    rows_processed += 1
                    
                except Exception as e:
                    errors.append(f"Row {i}: {str(e)}")
                    # Add row with original values on error
                    accumulated_rows.append(row)
            
            # Write output CSV
            with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=headers)
                writer.writeheader()
                writer.writerows(accumulated_rows)
            
            print(f"Successfully processed {rows_processed} rows")
            print(f"Total accumulated wattHours: {cumulative_watthours}")
            
            if errors:
                print(f"\nWarnings/Errors:")
                for error in errors[:10]:
                    print(f"  {error}")
                if len(errors) > 10:
                    print(f"  ... and {len(errors) - 10} more errors")
    
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error processing file: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='Process CSV: convert dates to UTC, multiply watts/wattHours by 1000, and accumulate wattHours',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s data.csv "America/New_York"
  %(prog)s data.csv "Asia/Manila" --output converted.csv
  %(prog)s data.csv "Europe/London" -o output.csv

Common timezones:
  America/New_York, America/Los_Angeles, America/Chicago
  Europe/London, Europe/Paris, Europe/Berlin
  Asia/Manila, Asia/Tokyo, Asia/Shanghai
  Australia/Sydney, Pacific/Auckland
        '''
    )
    
    parser.add_argument('csv_file', help='Input CSV file')
    parser.add_argument('timezone', help='Source timezone (e.g., "America/New_York")')
    parser.add_argument('-o', '--output', help='Output CSV file (default: input_accumulated.csv)')
    
    args = parser.parse_args()
    
    # Check if input file exists
    if not Path(args.csv_file).exists():
        print(f"Error: File '{args.csv_file}' not found")
        sys.exit(1)
    
    # Validate timezone
    try:
        if USE_PYTZ:
            pytz.timezone(args.timezone)
        else:
            ZoneInfo(args.timezone)
    except Exception:
        print(f"Error: Invalid timezone '{args.timezone}'")
        print("Use format like 'America/New_York' or 'Asia/Manila'")
        sys.exit(1)
    
    # Process the CSV
    process_csv(args.csv_file, args.timezone, args.output)


if __name__ == '__main__':
    main()
