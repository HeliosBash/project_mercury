#!/usr/bin/python3
import requests
import json
import sys
import re
import argparse
from datetime import datetime, timedelta

MAX_RANGE_DAYS = 30
MAX_RANGE_SECONDS = MAX_RANGE_DAYS * 24 * 60 * 60  # 2592000


def solcast_download(lat, long, startdate, enddate, token):
    bearer = f"Bearer {token}"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': bearer
    }
    formatted_startdate = startdate.replace(" ", "T").replace(":", "%3A")
    formatted_enddate = enddate.replace(" ", "T").replace(":", "%3A")
    url = (
        f"https://api.solcast.com.au/data/historic/radiation_and_weather"
        f"?latitude={lat}&longitude={long}&period=PT5M"
        f"&start={formatted_startdate}&end={formatted_enddate}"
        f"&format=json&time_zone=utc&output_parameters=ghi"
    )
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Error : Received status code {response.status_code}")
        sys.exit(1)
    try:
        data = response.json()
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON response")
        sys.exit(1)
    for i in data.get('estimated_actuals', []):
        period_end = i["period_end"].replace('+00:00', 'Z')
        dateobj = datetime.strptime(period_end.replace('T', ' ').replace('Z', ''), '%Y-%m-%d %H:%M:%S')
        ghi = i["ghi"]
        period = i["period"]
        duration = int(re.sub("[PTM]", "", period))
        period_start = str(dateobj - timedelta(minutes=duration)).replace(' ', 'T') + 'Z'
        print(f"{period_end},{period_start},{period},{ghi}")


def parse_date(datestr):
    """Parse a date string in 'YYYY-MM-DD HH:MM:SSZ' or 'YYYY-MM-DD HH:MM:SS.000Z' format to a datetime object."""
    datestr = datestr.strip()
    clean = datestr.rstrip('Z')
    if '.' in clean:
        clean = clean.split('.')[0]
    return datetime.strptime(clean, '%Y-%m-%d %H:%M:%S')


def format_date(dt):
    """Format a datetime object back to the expected API string format."""
    return dt.strftime('%Y-%m-%d %H:%M:%S') + 'Z'


def build_chunks(startdate, enddate):
    """
    Replicates the bash chunking logic exactly.
    Returns a list of (chunk_start, chunk_end) string tuples.
    """
    start_dt = parse_date(startdate)
    end_dt = parse_date(enddate)

    start_epoch = int(start_dt.timestamp())
    end_epoch = int(end_dt.timestamp())
    date_range_diff_seconds = end_epoch - start_epoch

    chunks = []

    if date_range_diff_seconds > MAX_RANGE_SECONDS:
        # First chunk: start → start + 30 days
        partial_end_dt = start_dt + timedelta(days=MAX_RANGE_DAYS)
        partial_end = format_date(partial_end_dt)
        chunks.append((format_date(start_dt), partial_end))

        # Next chunk starts exactly at the previous chunk's end (no offset needed)
        partial_start_dt = partial_end_dt
        partial_start = format_date(partial_start_dt)
        partial_epoch = int(partial_start_dt.timestamp())
        date_range_diff_seconds = end_epoch - partial_epoch

        # Middle chunks
        while date_range_diff_seconds > MAX_RANGE_SECONDS:
            partial_end_dt = partial_start_dt + timedelta(days=MAX_RANGE_DAYS)
            partial_end = format_date(partial_end_dt)
            chunks.append((partial_start, partial_end))

            partial_start_dt = partial_end_dt
            partial_start = format_date(partial_start_dt)
            partial_epoch = int(partial_start_dt.timestamp())
            date_range_diff_seconds = end_epoch - partial_epoch

        # Final chunk: last partial_start → original end
        chunks.append((partial_start, format_date(end_dt)))

    else:
        # Single chunk: date range is within 30 days
        chunks.append((format_date(start_dt), format_date(end_dt)))

    return chunks


def main():
    parser = argparse.ArgumentParser(description="Solcast Download Tool")
    parser.add_argument("--latitude", required=True, type=str, help="Site Latitude Coordinate")
    parser.add_argument("--longitude", required=True, type=str, help="Site Longitude Coordinate")
    parser.add_argument("--startdate", required=True, type=str, help="UTC startdate in 'YYYY-MM-DD HH:MM:SS.000Z' format")
    parser.add_argument("--enddate", required=True, type=str, help="UTC enddate in 'YYYY-MM-DD HH:MM:SS.000Z' format")
    parser.add_argument("--token", required=True, help="Solcast token")
    args = parser.parse_args()

    # Advance startdate by 5 minutes so the first period_start does not overlap
    # with the requested startdate. Only applied to the initial startdate, not chunk boundaries.
    start_dt = parse_date(args.startdate)
    adjusted_startdate = format_date(start_dt + timedelta(minutes=5))

    chunks = build_chunks(adjusted_startdate, args.enddate)

    if len(chunks) > 1:
        print(f"# Date range exceeds 30 days, splitting into {len(chunks)} queries", file=sys.stderr)

    for i, (chunk_start, chunk_end) in enumerate(chunks):
        if len(chunks) > 1:
            print(f"# Downloading chunk {i + 1}/{len(chunks)}: {chunk_start} → {chunk_end}", file=sys.stderr)
        solcast_download(args.latitude, args.longitude, chunk_start, chunk_end, args.token)


if __name__ == "__main__":
    main()
