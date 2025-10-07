#!/usr/bin/env python3
"""
Script to calculate cumulative irradiance hours from a CSV file.
Formula: irradiance_hours[i] = (irradiance[i] / 12) + irradiance_hours[i-1]
First row: irradiance_hours[0] = irradiance[0]
"""

import pandas as pd
import argparse
import sys
import platform
from pathlib import Path
from datetime import datetime

# Try to use zoneinfo (Python 3.9+), fallback to pytz
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


# Fixed date format: m/d/yyyy h:mm
DATE_FORMAT = '%-m/%-d/%Y %H:%M:%S'  # Unix/Mac format with no leading zeros
DATE_FORMAT_WIN = '%#m/%#d/%Y %H:%M:%S'  # Windows format with no leading zeros
DATE_FORMAT_PADDED = '%m/%d/%Y %H:%M:%S'  # Fallback with leading zeros


def parse_date(date_str, date_format):
    """Parse date string according to the given format."""
    # Determine which date format to use (platform-specific)
    if platform.system() == 'Windows':
        primary_format = DATE_FORMAT_WIN
    else:
        primary_format = DATE_FORMAT
    
    # Try platform-specific format first
    try:
        return datetime.strptime(date_str, primary_format)
    except ValueError:
        # Fallback to padded format
        try:
            return datetime.strptime(date_str, DATE_FORMAT_PADDED)
        except ValueError:
            # Last resort: try custom format if provided
            return datetime.strptime(date_str, date_format)


def convert_to_utc(date_str, source_tz, date_format=None):
    """
    Convert a date string from source timezone to UTC.
    """
    # Parse the date string with known format
    dt = parse_date(date_str.strip(), date_format or DATE_FORMAT)
    
    if USE_PYTZ:
        # Using pytz
        tz = pytz.timezone(source_tz)
        # Localize the naive datetime to the source timezone
        dt_local = tz.localize(dt)
        # Convert to UTC
        dt_utc = dt_local.astimezone(pytz.UTC)
    else:
        # Using zoneinfo (Python 3.9+)
        tz = ZoneInfo(source_tz)
        # Attach timezone to the naive datetime
        dt_local = dt.replace(tzinfo=tz)
        # Convert to UTC
        dt_utc = dt_local.astimezone(ZoneInfo('UTC'))
    
    # Format as UTC string without 'Z' suffix
    return dt_utc.strftime('%Y-%m-%d %H:%M:%S')


def process_date_column(df, date_col='date', timezone=None, date_format=None):
    """
    Convert date column format and optionally convert timezone to UTC.
    
    Args:
        df: DataFrame with date data
        date_col: Name of the date column
        timezone: Source timezone (e.g., 'Asia/Manila', 'America/New_York'). If None, only reformats dates.
        date_format: Format of the input date strings (optional)
    
    Returns:
        DataFrame with converted date column as strings in YYYY-MM-DD HH:MM:SS format
    """
    if date_col not in df.columns:
        raise ValueError(f"Column '{date_col}' not found in CSV. Available columns: {df.columns.tolist()}")
    
    # Pre-create timezone objects if timezone conversion is requested
    if timezone:
        if USE_PYTZ:
            source_tz_obj = pytz.timezone(timezone)
            utc_tz_obj = pytz.UTC
        else:
            source_tz_obj = ZoneInfo(timezone)
            utc_tz_obj = ZoneInfo('UTC')
    
    # Determine which date format to use (platform-specific)
    if platform.system() == 'Windows':
        primary_format = DATE_FORMAT_WIN
    else:
        primary_format = DATE_FORMAT
    
    try:
        # Convert each date
        parsed_dates = []
        
        for date_str in df[date_col]:
            if pd.isna(date_str):
                parsed_dates.append(None)
                continue
            
            date_str = str(date_str).strip()
            
            # Try platform-specific format first
            try:
                dt = datetime.strptime(date_str, primary_format)
            except ValueError:
                # Fallback to padded format
                try:
                    dt = datetime.strptime(date_str, DATE_FORMAT_PADDED)
                except ValueError:
                    # Last resort: try custom format if provided
                    if date_format:
                        dt = datetime.strptime(date_str, date_format)
                    else:
                        raise
            
            # Convert to UTC if timezone is provided, otherwise just reformat
            if timezone:
                if USE_PYTZ:
                    dt_local = source_tz_obj.localize(dt)
                    dt_utc = dt_local.astimezone(utc_tz_obj)
                else:
                    dt_local = dt.replace(tzinfo=source_tz_obj)
                    dt_utc = dt_local.astimezone(utc_tz_obj)
                parsed_dates.append(dt_utc.strftime('%Y-%m-%d %H:%M:%S'))
            else:
                # Just reformat the date without timezone conversion
                parsed_dates.append(dt.strftime('%Y-%m-%d %H:%M:%S'))
        
        df[date_col] = parsed_dates
        
    except Exception as e:
        raise ValueError(f"Error processing date column: {e}")
    
    return df


def calculate_irradiance_hours(df, irradiance_col='irradiance'):
    """
    Calculate cumulative irradiance hours.
    
    Args:
        df: DataFrame with irradiance data
        irradiance_col: Name of the irradiance column
    
    Returns:
        DataFrame with added 'irradianceHours' column
    """
    if irradiance_col not in df.columns:
        raise ValueError(f"Column '{irradiance_col}' not found in CSV. Available columns: {df.columns.tolist()}")
    
    # Round irradiance values to whole numbers
    df[irradiance_col] = df[irradiance_col].round().astype('Int64')
    
    # Initialize the irradiance_hours column
    irradiance_hours = []
    prev_hours = None
    
    for irradiance in df[irradiance_col]:
        if pd.isna(irradiance):
            # Handle missing values
            current_hours = prev_hours if prev_hours is not None else 0
        elif prev_hours is None:
            # First row: match the irradiance value
            current_hours = irradiance
        else:
            # Subsequent rows: (current irradiance / 12) + previous irradiance hours
            current_hours = (irradiance / 12) + prev_hours
        
        # Keep as integer if the value is exactly 0
        if current_hours == 0:
            current_hours = 0
        
        irradiance_hours.append(current_hours)
        prev_hours = current_hours
    
    df['irradianceHours'] = irradiance_hours
    return df


def main():
    parser = argparse.ArgumentParser(
        description='Add cumulative irradiance hours column to a CSV file.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python script.py input.csv
  python script.py input.csv -t Asia/Manila
  python script.py input.csv -t America/New_York -o output.csv
  python script.py input.csv -t Europe/London -c solar_irradiance -d timestamp

Without timezone (-t):
  - Dates will only be reformatted to YYYY-MM-DD HH:MM:SS
  - No timezone conversion occurs

With timezone (-t):
  - Dates will be converted from source timezone to UTC
  - Output format: YYYY-MM-DD HH:MM:SS

Common timezones:
  America/New_York, America/Los_Angeles, America/Chicago
  Europe/London, Europe/Paris, Europe/Berlin
  Asia/Manila, Asia/Tokyo, Asia/Shanghai
  Australia/Sydney, Pacific/Auckland
        """
    )
    
    parser.add_argument('input_file', help='Input CSV file path')
    parser.add_argument('-t', '--timezone', 
                        help='Source timezone for conversion to UTC (e.g., Asia/Manila, America/New_York). If not provided, dates will only be reformatted.')
    parser.add_argument('-o', '--output', help='Output CSV file path (default: adds _with_hours suffix)')
    parser.add_argument('-c', '--column', default='irradiance', 
                        help='Name of the irradiance column (default: irradiance)')
    parser.add_argument('-d', '--date-column', default='date',
                        help='Name of the date column (default: date)')
    parser.add_argument('-f', '--date-format', default=None,
                        help='Custom date format (default: auto-detect m/d/yyyy h:mm)')
    
    args = parser.parse_args()
    
    # Validate input file exists
    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"Error: Input file '{args.input_file}' not found.", file=sys.stderr)
        sys.exit(1)
    
    # Validate timezone if provided
    if args.timezone:
        try:
            if USE_PYTZ:
                pytz.timezone(args.timezone)
            else:
                ZoneInfo(args.timezone)
        except Exception:
            print(f"Error: Invalid timezone '{args.timezone}'", file=sys.stderr)
            print("Use format like 'America/New_York' or 'Asia/Manila'")
            sys.exit(1)
    
    # Determine output file path
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = input_path.with_stem(f"{input_path.stem}_with_hours")
    
    try:
        # Read CSV
        print(f"Reading CSV file: {input_path}")
        df = pd.read_csv(input_path)
        print(f"Loaded {len(df)} rows with columns: {df.columns.tolist()}")
        
        # Process date column
        if args.timezone:
            print(f"Converting date column '{args.date_column}' from {args.timezone} to UTC")
        else:
            print(f"Reformatting date column '{args.date_column}' to YYYY-MM-DD HH:MM:SS")
        print(f"Expected input format: m/d/yyyy h:mm (e.g., 1/1/2023 0:00)")
        df = process_date_column(df, args.date_column, args.timezone, args.date_format)
        
        # Calculate irradiance hours
        print(f"Calculating irradiance hours using column: '{args.column}'")
        df = calculate_irradiance_hours(df, args.column)
        
        # Write output
        df.to_csv(output_path, index=False, float_format='%g')
        print(f"✓ Successfully wrote output to: {output_path}")
        print(f"  Added column 'irradianceHours' with {len(df)} values")
        
    except Exception as e:
        print(f"Error processing file: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
