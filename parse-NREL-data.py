#!/usr/bin/env python3

import sys
import csv
import argparse
from datetime import datetime
from decimal import Decimal, getcontext

def process_irradiance_data(node, source_id, file_path, start_date, end_date, output_file=None):
    """
    Process irradiance data from CSV file within specified datetime range.
    
    Args:
        node: Node identifier
        source_id: Source identifier  
        file_path: Path to CSV file
        start_date: Start datetime (YYYY-MM-DD HH:MM:SS format)
        end_date: End datetime (YYYY-MM-DD HH:MM:SS format)
        output_file: Optional output file path (if None, prints to stdout)
    """
    # Set precision for decimal calculations
    getcontext().prec = 28
    
    # Parse datetime range
    try:
        start_datetime_obj = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        end_datetime_obj = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        print(f"Error parsing dates: {e}. Use YYYY-MM-DD HH:MM:SS format.", file=sys.stderr)
        sys.exit(1)
    
    if start_datetime_obj > end_datetime_obj:
        print("Error: Start datetime must be before or equal to end datetime.", file=sys.stderr)
        sys.exit(1)
    
    first_row_hours = 0
    ghi_prev = Decimal('0')
    header_printed = False
    
    # Determine output destination
    output_handle = None
    if output_file:
        try:
            output_handle = open(output_file, 'w')
        except Exception as e:
            print(f"Error opening output file '{output_file}': {e}", file=sys.stderr)
            sys.exit(1)
    
    def write_output(text):
        """Write to file or stdout based on configuration"""
        if output_handle:
            output_handle.write(text + '\n')
        else:
            print(text)
    
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
            
        # Skip first 3 lines (equivalent to 3 sed commands)
        data_lines = lines[3:]
        
        for line in data_lines:
            line = line.strip()
            if not line:
                continue
                
            fields = line.split(',')
            
            # Extract date fields for filtering
            year = int(fields[0])
            month = int(fields[1]) 
            day = int(fields[2])
            hour = int(fields[3])
            minute = int(fields[4])
            
            # Create datetime object for comparison
            row_datetime = datetime(year, month, day, hour, minute, 0)
            
            # Skip rows outside datetime range
            if row_datetime < start_datetime_obj or row_datetime > end_datetime_obj:
                continue
            
            # Print header only once when first valid row is found
            if not header_printed:
                write_output("node,source,date,irradiance,irradianceHours")
                header_printed = True
            
            if first_row_hours == 0:
                first_row_hours = 1
                
                # Extract GHI value
                ghi = Decimal(fields[-1])  # Last field (NF in awk)
                
                # Create datetime string
                utc_datetime = f"{year}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:00"
                
                write_output(f"{node},{source_id},{utc_datetime},{ghi},{ghi}")
                ghi_prev = ghi
                
            else:
                # Extract GHI value
                ghi = Decimal(fields[-1])
                ghi_10min = ghi / Decimal('6')
                
                # Apply conditional logic for precision
                if 0 < ghi < 12:
                    ghi_new = ghi_prev + ghi_10min
                    # Format to 8 decimal places for small values
                    ghi_formatted = f"{ghi_new:.8f}"
                else:
                    ghi_new = ghi_prev + ghi_10min
                    ghi_formatted = str(ghi_new)
                
                ghi_prev = ghi_new
                
                # Create datetime string
                utc_datetime = f"{year}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:00"
                
                write_output(f"{node},{source_id},{utc_datetime},{ghi},{ghi_formatted}")
                
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error processing file: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        # Close output file if it was opened
        if output_handle:
            output_handle.close()
            print(f"Output saved to: {output_file}", file=sys.stderr)

def main():
    parser = argparse.ArgumentParser(
        description='Process irradiance data from CSV file within specified datetime range.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process data and output to stdout
  python script.py --node node1 --source-id source1 --file data.csv --start "2023-06-01 08:00:00" --end "2023-06-30 18:00:00"
  
  # Process data and save to file
  python script.py --node node1 --source-id source1 --file data.csv --start "2023-06-01 08:00:00" --end "2023-06-30 18:00:00" --output output.csv
  
  # Process specific time range (using short flags)
  python script.py -n node1 -s source1 -f data.csv --start "2023-06-15 09:30:00" --end "2023-06-15 16:45:00" -o results.csv
        """
    )
    
    parser.add_argument('-n', '--node', 
                       required=True,
                       help='Node identifier')
    parser.add_argument('-s', '--source-id', 
                       required=True,
                       help='Source identifier')
    parser.add_argument('-f', '--file', 
                       dest='file_path',
                       required=True,
                       help='Path to input CSV file')
    parser.add_argument('--start', '--start-datetime', 
                       dest='start_datetime',
                       required=True,
                       help='Start datetime (YYYY-MM-DD HH:MM:SS format)')
    parser.add_argument('--end', '--end-datetime', 
                       dest='end_datetime',
                       required=True,
                       help='End datetime (YYYY-MM-DD HH:MM:SS format)')
    parser.add_argument('-o', '--output', 
                       dest='output_file',
                       help='Output file path (if not specified, prints to stdout)')
    
    args = parser.parse_args()
    
    process_irradiance_data(
        args.node, 
        args.source_id, 
        args.file_path, 
        args.start_datetime, 
        args.end_datetime,
        args.output_file
    )

if __name__ == "__main__":
    main()
