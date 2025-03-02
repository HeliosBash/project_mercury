#!/usr/bin/python3

import requests  
import json
import sys
import re
from datetime import datetime, timedelta
import argparse

def parse_date(date_str):
    """Parse date string to datetime object."""
    if 'T' in date_str:
        date_str = date_str.replace('T', ' ')
    if 'Z' in date_str:
        date_str = date_str.replace('Z', '')
    if '.000' in date_str:
        date_str = date_str.replace('.000', '')
    return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

def format_date_for_url(date_obj):
    """Format datetime object for URL."""
    return date_obj.strftime('%Y-%m-%d %H:%M:%S').replace(' ', 'T').replace(':', '%3A')

def format_date_standard(date_obj):
    """Format datetime object to standard format."""
    return date_obj.strftime('%Y-%m-%d %H:%M:%S')

def solcast_download(lat, long, startdate, enddate, token):
    """Download data from Solcast API."""
    # Convert string dates to datetime objects
    start_dt = parse_date(startdate) if isinstance(startdate, str) else startdate
    end_dt = parse_date(enddate) if isinstance(enddate, str) else enddate

    # Calculate time difference in days
    time_diff = (end_dt - start_dt).days + 1

    # Process in chunks of 30 days or less
    chunk_start = start_dt
    while chunk_start < end_dt:
        # Calculate chunk end date (30 days from chunk start or end date, whichever is sooner)
        chunk_end = min(chunk_start + timedelta(days=29), end_dt)

        # Format dates for API request
        url_encoded_startdate = format_date_for_url(chunk_start)
        url_encoded_enddate = format_date_for_url(chunk_end)

        # Make API request for this chunk
        process_chunk(lat, long, url_encoded_startdate, url_encoded_enddate, token,
                     format_date_standard(start_dt), format_date_standard(end_dt))

        # Move to next chunk
        chunk_start = chunk_end + timedelta(days=1)

def process_chunk(lat, long, url_encoded_startdate, url_encoded_enddate, token, standard_format_startdate, standard_format_enddate):
    """Process a single chunk of the date range."""
    bearer = f"Bearer {token}"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': bearer
    }

    url = f"https://api.solcast.com.au/data/historic/radiation_and_weather?latitude={lat}&longitude={long}&period=PT5M&start={url_encoded_startdate}&end={url_encoded_enddate}&format=json&time_zone=utc&output_parameters=ghi"

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Error: Received status code {response.status_code}")
        return

    # JSON check
    try:
        data = response.json()
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON response")
        return

    for i in data.get('estimated_actuals', []):
        period_end = i["period_end"].replace('+00:00', 'Z')
        dateobj = datetime.strptime(period_end.replace('T', ' ').replace('Z', ''), '%Y-%m-%d %H:%M:%S')
        ghi = i["ghi"]
        period = i["period"]
        duration = int(re.sub("[PTM]", "", period))
        period_start = str(dateobj - timedelta(minutes=duration))

        if period_start > standard_format_startdate and period_start < standard_format_enddate:
            formatted_period_start = period_start.replace(' ', 'T') + 'Z'
            formatted_period_end = period_end
            print(f"{formatted_period_end},{formatted_period_start},{period},{ghi}")

def main():
    # Parser object to handle the arguments
    parser = argparse.ArgumentParser(description="Solcast Download Tool")
    # Add arguments
    parser.add_argument("--latitude", required=True, type=str, help="Site Latitude Coordinate")
    parser.add_argument("--longitude", required=True, type=str, help="Site Longitude Coordinate")
    parser.add_argument("--startdate", required=True, type=str, help="UTC startdate in 'YYYY-MM-DD HH:MM:SS.000Z' format")
    parser.add_argument("--enddate", required=True, type=str, help="UTC enddate in 'YYYY-MM-DD HH:MM:SS.000Z' format")
    parser.add_argument("--token", required=True, help="Solcast token")
    # Parse the arguments the user has provided
    args = parser.parse_args()
    # Call the solar query function with parsed arguments
    solcast_download(args.latitude, args.longitude, args.startdate, args.enddate, args.token)

if __name__ == "__main__":
    main()
