#!/usr/bin/env python3
import argparse
import csv
import datetime
from decimal import Decimal, getcontext

def format_number(value):
    """Format number to 2 decimal places only if not zero"""
    if value == 0 or value == Decimal('0'):
        return "0"
    else:
        return f"{value:.2f}"

def main():

    getcontext().prec = 28

    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description='Process solar cast data and calculate energy metrics.')
    parser.add_argument('--solcast_csv_path', required=True, type=str, help='Path to the Solcast CSV file')
    parser.add_argument('--energyspike', required=True, type=Decimal, help='Energy spike in watthours')
    parser.add_argument("--node", required=True, type=str, help="Node ID (non-empty string)")
    parser.add_argument("--sourceids", required=True, type=str, help="Source ID in format /VI/SU/B1/GEN/1")
    parser.add_argument('--output', default='results-for-import.csv', 
                        help='Path to output CSV file (default: results-for-import.csv)')
    parser.add_argument('--log', default=None, help='Path to log file (optional)')
    
    args = parser.parse_args()
    
    # Initialize variables
    previous_watthours = Decimal('0')
    total_energy = args.energyspike
    electrical_energy_data_path = args.output
    runtime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Calculate total irradiance from solcast download
    ghitotal = Decimal('0')
    with open(args.solcast_csv_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if len(row) >= 4:  # Ensure the row has at least 4 columns
                try:
                    ghitotal += Decimal(row[3])
                except (ValueError, IndexError):
                    # Skip header row or invalid rows
                    pass
    
    # Log information
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = [
        f"{current_time} Total Irradiance Calculated: {ghitotal}",
        f"{current_time} Total Energy Spike in watthours: {args.energyspike}",
        f"{current_time} Calculating Backfill Data Driven by Irradiance Data"
    ]
    
    # Print and log messages
    for message in log_message:
        print(message)
        if args.log:
            with open(args.log, 'a') as log_file:
                log_file.write(message + '\n')
    
    # Create output file with header
    with open(electrical_energy_data_path, 'w', newline='') as outfile:
        outfile.write("NodeID,SourceID,Date,watts,wattHours\n")
        
        # Process each row in the input CSV
        with open(args.solcast_csv_path, 'r') as infile:
            csv_reader = csv.reader(infile)
            for row in csv_reader:
                if len(row) >= 4:  # Ensure the row has at least 4 columns
                    try:
                        endperiod, startperiod, period, irr = row[0], row[1], row[2], Decimal(row[3])
                        
                        # Format the startperiod similar to the sed command in bash
                        formatted_startperiod = startperiod.replace("T", " ").replace("Z", "")
                        
                        # Calculate watts and watthours
                        watts = (total_energy * (irr / ghitotal) * 12)
                        watthours = previous_watthours + (watts / 12)
                        previous_watthours = watthours
                        
                        watts_formatted = format_number(watts)
                        watthours_formatted = format_number(watthours)

                        # Write to output file
                        outfile.write(f"{args.node},{args.sourceids},{formatted_startperiod},{watts_formatted},{watthours_formatted}\n")
                    except (ValueError, IndexError, ZeroDivisionError):
                        # Skip header row or invalid rows
                        pass

if __name__ == "__main__":
    main()
