#!/usr/bin/env python3
import argparse
import csv
import datetime
from datetime import timedelta


def parse_created(value):
    """Parse a created timestamp string into a UTC datetime."""
    s = value.strip().replace(' ', 'T')
    if not s.endswith('Z'):
        s += 'Z'
    return datetime.datetime.fromisoformat(s.replace('Z', '+00:00'))


def format_dt(dt):
    """Format datetime back to original space-separated UTC format."""
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def main():
    parser = argparse.ArgumentParser(
        description='Extract irradiance from a PT5M PYR source CSV and convert to standard interval format.'
    )
    parser.add_argument('--input', required=True, type=str,
                        help='Path to the input CSV file (must contain created and irradiance columns)')
    parser.add_argument('--output', default='irradiance-converted-5m.csv',
                        help='Path to output CSV file (default: irradiance-converted-5m.csv)')
    parser.add_argument('--log', default=None, help='Path to log file (optional)')
    args = parser.parse_args()

    rows_written = 0
    rows_skipped = 0

    def log(message):
        print(message)
        if args.log:
            with open(args.log, 'a') as log_file:
                log_file.write(message + '\n')

    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log(f"{current_time} Input file:  {args.input}")
    log(f"{current_time} Output file: {args.output}")
    log(f"{current_time} Period: PT5M (endDate = created + 5 minutes)")

    with open(args.input, 'r') as infile, open(args.output, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)

        if 'created' not in reader.fieldnames or 'irradiance' not in reader.fieldnames:
            raise ValueError(
                f"Input CSV must contain 'created' and 'irradiance' columns. Found: {reader.fieldnames}"
            )

        writer = csv.writer(outfile)
        writer.writerow(['endDate', 'created', 'period', 'irradiance'])

        for row in reader:
            try:
                created_dt = parse_created(row['created'])
                end_dt = created_dt + timedelta(minutes=5)
                irradiance = int(row['irradiance'].strip())

                writer.writerow([format_dt(end_dt), format_dt(created_dt), 'PT5M', irradiance])
                rows_written += 1
            except (ValueError, KeyError):
                rows_skipped += 1

    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log(f"{current_time} Done. Rows written: {rows_written}, Rows skipped: {rows_skipped}")


if __name__ == "__main__":
    main()
