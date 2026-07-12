#!/usr/bin/env python3
"""
Merged Solcast processor: calculates Irradiance Hours and Watthours in one pass.

Auto-detects the Solcast data period (5 / 10 / 15 / 30 min, etc.) from the
'period' column of the CSV (an ISO-8601 duration such as PT5M, PT10M, PT15M,
PT30M) and adjusts the per-period-to-hourly conversion factor accordingly,
instead of assuming a fixed 5-minute (divide-by-12) period.

Expected Solcast CSV columns (no header): period_end, period_start, period, ghi
"""

import csv
import re
import argparse
import datetime
from decimal import Decimal, getcontext, InvalidOperation


def convert_timestamp(timestamp):
    """'2024-01-01T05:00:00Z' -> '2024-01-01 05:00:00'"""
    return timestamp.replace("T", " ").replace("Z", "")


def format_number(value):
    """Format to 2 decimal places, strip trailing zeros, '0' for zero."""
    decimal_value = Decimal(str(value))
    if decimal_value == 0:
        return "0"
    formatted = f"{decimal_value:.2f}".rstrip('0').rstrip('.')
    return formatted if formatted not in ("", "-") else "0"


def parse_period_minutes(period_str):
    """
    Parse a Solcast 'period' field into minutes (as Decimal).
    Accepts ISO-8601 durations: PT5M, PT10M, PT15M, PT30M, PT1H, PT1H30M ...
    Falls back to treating a plain number as minutes.
    Raises ValueError if it can't be parsed (e.g. it's a header row).
    """
    period_str = period_str.strip()
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', period_str, re.IGNORECASE)
    if match and any(match.groups()):
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)
        total_minutes = hours * 60 + minutes + Decimal(seconds) / Decimal(60)
        if total_minutes <= 0:
            raise ValueError(f"Non-positive period parsed from '{period_str}'")
        return Decimal(total_minutes)
    # Fallback: plain number = minutes
    try:
        val = Decimal(period_str)
        if val <= 0:
            raise ValueError
        return val
    except (InvalidOperation, ValueError):
        raise ValueError(f"Unrecognized period format: '{period_str}'")


def detect_period(solcast_csv_path):
    """
    Scans the CSV to detect the data period. Uses the first parseable value
    and warns (does not fail) if later rows report a different period.
    Returns (period_minutes: Decimal, periods_per_hour: Decimal).
    """
    period_minutes = None
    with open(solcast_csv_path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 4:
                continue
            try:
                this_period = parse_period_minutes(row[2])
            except ValueError:
                continue  # e.g. header row
            if period_minutes is None:
                period_minutes = this_period
            elif this_period != period_minutes:
                print(f"WARNING: period changed from {period_minutes}min to {this_period}min "
                      f"mid-file (row: {row}). Continuing to use {period_minutes}min for all calculations.")
    if period_minutes is None:
        raise ValueError("Could not detect a data period (5/10/15/30 min etc.) from the Solcast CSV.")
    periods_per_hour = Decimal('60') / period_minutes
    return period_minutes, periods_per_hour


def process_irradiance_data(solcast_csv_path, irradiance_data_path, node, sourceid, periods_per_hour):
    a = 0
    ghi_prev = Decimal('0')
    with open(solcast_csv_path, 'r') as input_file, open(irradiance_data_path, 'w') as output_file:
        csv_reader = csv.reader(input_file)
        output_file.write("node,source,date,irradiance,irradianceHours\n")
        for row in csv_reader:
            if len(row) < 4:
                continue
            try:
                periodend, periodstart, period, ghi = row
                ghi_decimal = Decimal(ghi)
            except (ValueError, InvalidOperation):
                continue  # header row etc.

            periodstart_formatted = convert_timestamp(periodstart)

            if a == 0:
                # First record: irradiance-hours = the raw value (matches original behavior)
                output_file.write(f"{node},{sourceid},{periodstart_formatted},{ghi},{format_number(ghi_decimal)}\n")
                ghi_prev = ghi_decimal
                a = 1
            else:
                # Contribution of this period, scaled by the *detected* period length
                ghi_period = ghi_decimal / periods_per_hour
                ghi_new = ghi_prev + ghi_period
                ghi_new_formatted = format_number(ghi_new)
                ghi_prev = ghi_new
                output_file.write(f"{node},{sourceid},{periodstart_formatted},{ghi},{ghi_new_formatted}\n")


def process_watthours_data(solcast_csv_path, output_path, node, sourceid,
                            energyspike, log_path, periods_per_hour):
    previous_watthours = Decimal('0')
    total_energy = energyspike

    # Total irradiance across the whole file (used to distribute energyspike proportionally)
    ghitotal = Decimal('0')
    with open(solcast_csv_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if len(row) >= 4:
                try:
                    ghitotal += Decimal(row[3])
                except (ValueError, IndexError, InvalidOperation):
                    pass  # header row etc.

    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    period_minutes = Decimal('60') / periods_per_hour
    log_message = [
        f"{current_time} Total Irradiance Calculated: {ghitotal}",
        f"{current_time} Total Energy Spike in watthours: {total_energy}",
        f"{current_time} Detected data period: {period_minutes} minutes ({periods_per_hour} periods/hour)",
        f"{current_time} Calculating Backfill Data Driven by Irradiance Data",
    ]
    for message in log_message:
        print(message)
        if log_path:
            with open(log_path, 'a') as log_file:
                log_file.write(message + '\n')

    with open(output_path, 'w', newline='') as outfile:
        outfile.write("NodeID,SourceID,Date,watts,wattHours\n")
        with open(solcast_csv_path, 'r') as infile:
            csv_reader = csv.reader(infile)
            for row in csv_reader:
                if len(row) < 4:
                    continue
                try:
                    endperiod, startperiod, period, irr = row[0], row[1], row[2], Decimal(row[3])
                except (ValueError, IndexError, InvalidOperation):
                    continue  # header row etc.

                try:
                    formatted_startperiod = convert_timestamp(startperiod)
                    watts = total_energy * (irr / ghitotal) * periods_per_hour
                    watthours = previous_watthours + (watts / periods_per_hour)
                    previous_watthours = watthours
                    watts_formatted = format_number(watts)
                    watthours_formatted = format_number(watthours)
                    outfile.write(
                        f"{node},{sourceid},{formatted_startperiod},{watts_formatted},{watthours_formatted}\n"
                    )
                except ZeroDivisionError:
                    pass


def main():
    getcontext().prec = 28

    parser = argparse.ArgumentParser(
        description='Process a Solcast CSV into either Irradiance Hours or Watthours, '
                     'auto-detecting the data period (5/10/15/30 min...).'
    )
    parser.add_argument('--solcast_csv_path', required=True, help='Path to the input Solcast CSV file')
    parser.add_argument('--node', required=True, help='Node identifier')
    parser.add_argument('--sourceid', required=True,
                         help='Source identifier, e.g. /VI/SU/B1/GEN/1')
    parser.add_argument('--calculate', choices=['irradiance', 'watthours'], required=True,
                         help="Which calculation to run. 'watthours' internally derives total "
                              "irradiance from the same CSV first, then computes watts/wattHours "
                              "from it, matching the original two-script behavior.")
    parser.add_argument('--energyspike', type=Decimal, default=None,
                         help='Energy spike in watthours (required when --calculate is watthours)')
    parser.add_argument('--output', default='results-for-import.csv', help='Path to the output CSV file')
    parser.add_argument('--log', default=None, help='Path to log file (optional, watthours only)')
    args = parser.parse_args()

    if args.calculate == 'watthours' and args.energyspike is None:
        parser.error("--energyspike is required when --calculate is 'watthours'")

    period_minutes, periods_per_hour = detect_period(args.solcast_csv_path)
    print(f"Detected Solcast period: {period_minutes} minutes ({periods_per_hour} periods/hour)")

    if args.calculate == 'irradiance':
        process_irradiance_data(
            args.solcast_csv_path,
            args.output,
            args.node,
            args.sourceid,
            periods_per_hour
        )
    else:
        process_watthours_data(
            args.solcast_csv_path,
            args.output,
            args.node,
            args.sourceid,
            args.energyspike,
            args.log,
            periods_per_hour
        )


if __name__ == "__main__":
    main()
