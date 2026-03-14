#!/usr/bin/python3
"""
solcast_pipeline.py

  1. Downloads Solcast GHI data (chunked across 30-day API limits)
  2. Saves the raw solcast CSV
  3. Depending on source ID type:
       PYR     → runs irradiance hours calculation
       GEN/CON → runs watt-hours calculation
"""

import requests
import json
import sys
import re
import argparse
import datetime
from decimal import Decimal, getcontext
from datetime import datetime as dt, timedelta

MAX_RANGE_DAYS = 30
MAX_RANGE_SECONDS = MAX_RANGE_DAYS * 24 * 60 * 60  # 2592000

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def parse_date(datestr):
    """Parse 'YYYY-MM-DD HH:MM:SSZ' or 'YYYY-MM-DD HH:MM:SS.000Z' to datetime."""
    clean = datestr.strip().rstrip('Z')
    if '.' in clean:
        clean = clean.split('.')[0]
    return dt.strptime(clean, '%Y-%m-%d %H:%M:%S')


def format_date(d):
    """Format datetime to 'YYYY-MM-DD HH:MM:SSZ'."""
    return d.strftime('%Y-%m-%d %H:%M:%S') + 'Z'


def format_number(value):
    """Format Decimal to 2 dp, stripping trailing zeros. Zero returns '0'."""
    if value == 0 or value == Decimal('0'):
        return "0"
    return f"{value:.2f}".rstrip('0').rstrip('.')


# ---------------------------------------------------------------------------
# Chunking logic (mirrors original bash script exactly)
# ---------------------------------------------------------------------------

def build_chunks(startdate, enddate):
    """Return list of (chunk_start, chunk_end) string tuples within 30-day limits."""
    start_d = parse_date(startdate)
    end_d   = parse_date(enddate)

    start_epoch = int(start_d.timestamp())
    end_epoch   = int(end_d.timestamp())
    diff        = end_epoch - start_epoch

    chunks = []

    if diff > MAX_RANGE_SECONDS:
        partial_end_d = start_d + timedelta(days=MAX_RANGE_DAYS)
        chunks.append((format_date(start_d), format_date(partial_end_d)))

        partial_start_d = partial_end_d
        diff = end_epoch - int(partial_start_d.timestamp())

        while diff > MAX_RANGE_SECONDS:
            partial_end_d = partial_start_d + timedelta(days=MAX_RANGE_DAYS)
            chunks.append((format_date(partial_start_d), format_date(partial_end_d)))
            partial_start_d = partial_end_d
            diff = end_epoch - int(partial_start_d.timestamp())

        chunks.append((format_date(partial_start_d), format_date(end_d)))
    else:
        chunks.append((format_date(start_d), format_date(end_d)))

    return chunks


# ---------------------------------------------------------------------------
# Solcast download
# ---------------------------------------------------------------------------

def solcast_download_chunk(lat, lon, startdate, enddate, token):
    """Download one chunk from the Solcast API. Returns list of row dicts."""
    bearer  = f"Bearer {token}"
    headers = {'Content-Type': 'application/json', 'Authorization': bearer}

    fmt_start = startdate.replace(" ", "T").replace(":", "%3A")
    fmt_end   = enddate.replace(" ", "T").replace(":", "%3A")
    url = (
        f"https://api.solcast.com.au/data/historic/radiation_and_weather"
        f"?latitude={lat}&longitude={lon}&period=PT5M"
        f"&start={fmt_start}&end={fmt_end}"
        f"&format=json&time_zone=utc&output_parameters=ghi"
    )

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Error: Received status code {response.status_code}", file=sys.stderr)
        sys.exit(1)

    try:
        data = response.json()
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON response", file=sys.stderr)
        sys.exit(1)

    rows = []
    for i in data.get('estimated_actuals', []):
        period_end   = i["period_end"].replace('+00:00', 'Z')
        dateobj      = dt.strptime(period_end.replace('T', ' ').replace('Z', ''), '%Y-%m-%d %H:%M:%S')
        ghi          = i["ghi"]
        period       = i["period"]
        duration     = int(re.sub("[PTM]", "", period))
        period_start = str(dateobj - timedelta(minutes=duration)).replace(' ', 'T') + 'Z'
        rows.append({
            'period_end':   period_end,
            'period_start': period_start,
            'period':       period,
            'ghi':          ghi
        })
    return rows


def download_all(lat, lon, startdate, enddate, token):
    """Download all chunks and return combined list of rows."""
    chunks = build_chunks(startdate, enddate)

    if len(chunks) > 1:
        print(f"Date range exceeds 30 days, splitting into {len(chunks)} chunks", file=sys.stderr)

    all_rows = []
    for idx, (chunk_start, chunk_end) in enumerate(chunks):
        if len(chunks) > 1:
            print(f"Downloading chunk {idx + 1}/{len(chunks)}: {chunk_start} → {chunk_end}", file=sys.stderr)
        all_rows.extend(solcast_download_chunk(lat, lon, chunk_start, chunk_end, token))

    return all_rows


def save_solcast_csv(rows, path):
    """Write raw solcast rows to CSV."""
    with open(path, 'w') as f:
        for row in rows:
            f.write(f"{row['period_end']},{row['period_start']},{row['period']},{row['ghi']}\n")
    print(f"Solcast data saved to: {path}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Irradiance hours calculation (PYR)
# ---------------------------------------------------------------------------

def calculate_irradiance(rows, irradiance_data_path, node, sourceids):
    """Calculate cumulative irradiance hours and write output CSV."""
    getcontext().prec = 28

    def convert_timestamp(ts):
        return re.sub(r'T', ' ', re.sub(r'Z', '', ts))

    ghi_prev = Decimal('0')

    with open(irradiance_data_path, 'w') as out:
        out.write("node,source,date,irradiance,irradianceHours\n")
        for a, row in enumerate(rows):
            period_start_fmt = convert_timestamp(row['period_start'])
            ghi_decimal      = Decimal(str(row['ghi']))

            if a == 0:
                out.write(f"{node},{sourceids},{period_start_fmt},{row['ghi']},{format_number(ghi_decimal)}\n")
                ghi_prev = ghi_decimal
            else:
                ghi_5min = ghi_decimal / Decimal('12')
                ghi_new  = ghi_prev + ghi_5min
                out.write(f"{node},{sourceids},{period_start_fmt},{row['ghi']},{format_number(ghi_new)}\n")
                ghi_prev = ghi_new

    print(f"Irradiance data saved to: {irradiance_data_path}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Watt-hours calculation (GEN / CON)
# ---------------------------------------------------------------------------

def calculate_watthours(rows, energyspike, node, sourceids, output_path):
    """Calculate watts and cumulative watt-hours and write output CSV."""
    getcontext().prec = 28

    total_energy = Decimal(str(energyspike))
    ghitotal     = sum(Decimal(str(row['ghi'])) for row in rows)

    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{current_time} Total Irradiance Calculated: {ghitotal}", file=sys.stderr)
    print(f"{current_time} Total Energy Spike in watthours: {energyspike}", file=sys.stderr)
    print(f"{current_time} Calculating Backfill Data Driven by Irradiance Data", file=sys.stderr)

    previous_watthours = Decimal('0')

    with open(output_path, 'w', newline='') as out:
        out.write("NodeID,SourceID,Date,watts,wattHours\n")
        for row in rows:
            irr             = Decimal(str(row['ghi']))
            formatted_start = row['period_start'].replace("T", " ").replace("Z", "")
            watts           = total_energy * (irr / ghitotal) * 12
            watthours       = previous_watthours + (watts / 12)
            previous_watthours = watthours
            out.write(f"{node},{sourceids},{formatted_start},{format_number(watts)},{format_number(watthours)}\n")

    print(f"Watt-hours data saved to: {output_path}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    getcontext().prec = 28

    parser = argparse.ArgumentParser(description="Solcast Pipeline: Download + Calculate")

    # Download args
    parser.add_argument("--latitude",   required=True,  help="Site latitude coordinate")
    parser.add_argument("--longitude",  required=True,  help="Site longitude coordinate")
    parser.add_argument("--startdate",  required=True,  help="UTC startdate 'YYYY-MM-DD HH:MM:SSZ'")
    parser.add_argument("--enddate",    required=True,  help="UTC enddate 'YYYY-MM-DD HH:MM:SSZ'")
    parser.add_argument("--token",      required=True,  help="Solcast API token")

    # Common output args
    parser.add_argument("--solcast_csv_path", required=True,  help="Path to save raw solcast CSV")
    parser.add_argument("--node",             required=True,  help="Node identifier")
    parser.add_argument("--sourceids",        required=True,  help="Source ID (e.g. /VI/SU/B1/PYR/1) — determines calculation type")

    # PYR output
    parser.add_argument("--irradiance_data_path",        default=None, help="Output path for irradiance hours CSV (PYR)")

    # GEN/CON output
    parser.add_argument("--energyspike",                 default=None, type=Decimal, help="Energy spike in watt-hours (GEN/CON)")
    parser.add_argument("--electrical_energy_data_path", default=None, help="Output path for watt-hours CSV (GEN/CON)")

    args = parser.parse_args()

    # --- Step 1: Download ---
    print("Starting Solcast download...", file=sys.stderr)
    rows = download_all(args.latitude, args.longitude, args.startdate, args.enddate, args.token)
    print(f"Downloaded {len(rows)} records total", file=sys.stderr)

    # --- Step 2: Save raw solcast CSV ---
    save_solcast_csv(rows, args.solcast_csv_path)

    # --- Step 3: Calculate based on source ID type ---
    if "PYR" in args.sourceids:
        if not args.irradiance_data_path:
            print("Error: --irradiance_data_path is required for PYR source type", file=sys.stderr)
            sys.exit(1)
        calculate_irradiance(rows, args.irradiance_data_path, args.node, args.sourceids)
        print(f"Pipeline complete. Import file: {args.irradiance_data_path}", file=sys.stderr)

    elif "GEN" in args.sourceids or "CON" in args.sourceids:
        if args.energyspike is None:
            print("Error: --energyspike is required for GEN/CON source type", file=sys.stderr)
            sys.exit(1)
        if not args.electrical_energy_data_path:
            print("Error: --electrical_energy_data_path is required for GEN/CON source type", file=sys.stderr)
            sys.exit(1)
        calculate_watthours(rows, args.energyspike, args.node, args.sourceids, args.electrical_energy_data_path)
        print(f"Pipeline complete. Import file: {args.electrical_energy_data_path}", file=sys.stderr)

    else:
        print("Error: Unable to determine calculation type from --sourceids. Must contain PYR, GEN, or CON.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
