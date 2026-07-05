#!/usr/bin/env python3
import argparse
import csv
import datetime
from decimal import Decimal, getcontext, InvalidOperation
 
def format_number(value):
    """Format number to 2 decimal places only if not zero"""
    if value == 0 or value == Decimal('0'):
        return "0"
    else:
        return f"{value:.2f}"
 
def main():
    getcontext().prec = 28
 
    parser = argparse.ArgumentParser(description='Process solar cast data and calculate energy metrics (PT15M).')
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
 
    # PT15M: 60 / 15 = 4 intervals per hour
    INTERVALS_PER_HOUR = Decimal('4')
 
    # Calculate total irradiance from solcast download
    ghitotal = Decimal('0')
    with open(args.solcast_csv_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if len(row) >= 4:
                try:
                    ghitotal += Decimal(row[3])
                except (ValueError, IndexError, InvalidOperation):
                    pass
 
    # Log information
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = [
        f"{current_time} Period: PT15M ({INTERVALS_PER_HOUR} intervals/hour)",
        f"{current_time} Total Irradiance Calculated: {ghitotal}",
        f"{current_time} Total Energy Spike in watthours: {args.energyspike}",
        f"{current_time} Calculating Backfill Data Driven by Irradiance Data"
    ]
 
    for message in log_message:
        print(message)
        if args.log:
            with open(args.log, 'a') as log_file:
                log_file.write(message + '\n')
 
    # Create output file with header
    with open(electrical_energy_data_path, 'w', newline='') as outfile:
        outfile.write("NodeID,SourceID,Date,watts,wattHours\n")
 
        with open(args.solcast_csv_path, 'r') as infile:
            csv_reader = csv.reader(infile)
            for row in csv_reader:
                if len(row) >= 4:
                    try:
                        endperiod, startperiod, period, irr = row[0], row[1], row[2], Decimal(row[3])
 
                        formatted_startperiod = startperiod.replace("T", " ").replace("Z", "")
 
                        # PT15M: multiply/divide by 4 instead of 12
                        watts = (total_energy * (irr / ghitotal) * INTERVALS_PER_HOUR)
                        watthours = previous_watthours + (watts / INTERVALS_PER_HOUR)
                        previous_watthours = watthours
 
                        watts_formatted = format_number(watts)
                        watthours_formatted = format_number(watthours)
 
                        outfile.write(f"{args.node},{args.sourceids},{formatted_startperiod},{watts_formatted},{watthours_formatted}\n")
                    except (ValueError, IndexError, ZeroDivisionError, InvalidOperation):
                        pass
 
if __name__ == "__main__":
    main()
