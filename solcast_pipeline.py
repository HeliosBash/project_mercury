#!/usr/bin/python3
"""
solcast_pipeline.py

  1. Gets Solcast GHI data, either:
       - from the API (chunked across 30-day limits), or
       - from a previously downloaded CSV (--input_csv), skipping the API
  2. Saves the raw solcast CSV (API mode only)
  3. Depending on source ID type:
       PYR     → runs irradiance hours calculation
       GEN/CON → runs watt-hours calculation

Input CSV format (same as what this script writes, no header):
    period_end,period_start,period,ghi
    2025-08-07T18:00:00Z,2025-08-07T17:55:00Z,PT5M,884
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


def parse_iso(ts):
    """Parse '2025-08-07T18:00:00Z' (or +00:00 / fractional secs) to datetime."""
    clean = ts.strip().replace('+00:00', '').rstrip('Z').replace('T', ' ')
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
# Read previously downloaded Solcast CSV (replaces API download)
# ---------------------------------------------------------------------------

def load_solcast_csv(path, startdate=None, enddate=None):
    """
    Read rows from a CSV in the format save_solcast_csv() writes:
        period_end,period_start,period,ghi
    Blank lines and a header row (non-numeric ghi) are skipped.
    If startdate/enddate are given, keep only rows whose period_start (the
    timestamp written to the output) falls in [startdate, enddate] inclusive,
    so output timestamps always lie inside the requested window.
    """
    start_d = parse_date(startdate) if startdate else None
    end_d   = parse_date(enddate)   if enddate   else None

    rows = []
    skipped_header = 0
    with open(path, 'r') as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            parts = [p.strip() for p in line.split(',')]
            if len(parts) < 4:
                print(f"Error: line {lineno} of {path} has {len(parts)} fields, expected 4", file=sys.stderr)
                sys.exit(1)

            period_end, period_start, period, ghi = parts[:4]

            try:
                ghi_num = Decimal(ghi)
            except Exception:
                skipped_header += 1
                continue  # header row or junk

            if start_d or end_d:
                ps = parse_iso(period_start)
                if start_d and ps < start_d:
                    continue
                if end_d and ps > end_d:
                    continue

            # Keep ghi as the original text so output matches the API path
            # (API gives ints/floats; str() of either is the same as the CSV text).
            ghi_val = int(ghi_num) if ghi_num == ghi_num.to_integral_value() and '.' not in ghi else float(ghi)

            rows.append({
                'period_end':   period_end,
                'period_start': period_start,
                'period':       period,
                'ghi':          ghi_val
            })

    if skipped_header:
        print(f"Skipped {skipped_header} non-data line(s) (header?) in {path}", file=sys.stderr)

    # Sanity check: warn about gaps / non-5-minute periods, since the
    # calculations assume PT5M (the /12 and *12).
    non_5m = {r['period'] for r in rows if r['period'] != 'PT5M'}
    if non_5m:
        print(f"Warning: non-PT5M periods found: {sorted(non_5m)} — calculations assume 5-minute intervals", file=sys.stderr)

    prev = None
    gaps = 0
    for r in rows:
        pe = parse_iso(r['period_end'])
        if prev is not None and (pe - prev) != timedelta(minutes=5):
            gaps += 1
        prev = pe
    if gaps:
        print(f"Warning: {gaps} irregular interval(s) between consecutive rows in {path}", file=sys.stderr)

    return rows


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

def calculate_watthours(rows, energyspike, node, sourceids, output_path, start_watthours=Decimal('0')):
    getcontext().prec = 28

    total_energy = Decimal(str(energyspike))
    ghitotal     = sum(Decimal(str(row['ghi'])) for row in rows)

    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{current_time} Total Irradiance Calculated: {ghitotal}", file=sys.stderr)
    print(f"{current_time} Total Energy Spike in watthours: {energyspike}", file=sys.stderr)

    if ghitotal == 0:
        print(
            f"Warning: GHI is 0 across the entire gap ({len(rows)} rows) — "
            f"nighttime gap. Filling watts=0 and holding wattHours flat; "
            f"the real energyspike will land naturally on the boundary reading "
            f"that follows the gap, not inside the fill.",
            file=sys.stderr
        )

    # Running wattHours counter starts at the supplied baseline (0 unless --startwatthours is given),
    # so consecutive fills (e.g. month by month) can be chained into one continuous counter.
    previous_watthours = Decimal(str(start_watthours))

    with open(output_path, 'w', newline='') as out:
        out.write("NodeID,SourceID,Date,watts,wattHours\n")
        for row in rows:
            irr             = Decimal(str(row['ghi']))
            formatted_start = row['period_start'].replace("T", " ").replace("Z", "")

            # The counter at a row's timestamp (period_start) is the energy accumulated
            # BEFORE that interval; the interval's own energy (watts / 12) is added
            # afterwards, so it shows up on the next row. The first row therefore equals
            # the baseline exactly, with no artificial step right after the last real datum.
            if ghitotal == 0:
                watts = Decimal('0')
                # hold flat at the baseline — no synthetic step inside a dark gap
                watthours = previous_watthours
            else:
                watts = total_energy * (irr / ghitotal) * 12
                watthours = previous_watthours
                previous_watthours = previous_watthours + (watts / 12)

            out.write(f"{node},{sourceids},{formatted_start},{format_number(watts)},{format_number(watthours)}\n")

    print(f"Watt-hours data saved to: {output_path}", file=sys.stderr)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    getcontext().prec = 28

    parser = argparse.ArgumentParser(description="Solcast Pipeline: Download (or load CSV) + Calculate")

    # Input mode: local CSV instead of the API
    parser.add_argument("--input_csv", default=None,
                        help="Use this previously downloaded solcast CSV instead of calling the API "
                             "(format: period_end,period_start,period,ghi; no header)")

    # Download args (required only when NOT using --input_csv)
    parser.add_argument("--latitude",   default=None, help="Site latitude coordinate (API mode)")
    parser.add_argument("--longitude",  default=None, help="Site longitude coordinate (API mode)")
    parser.add_argument("--token",      default=None, help="Solcast API token (API mode)")
    parser.add_argument("--solcast_csv_path", default=None, help="Path to save raw solcast CSV (API mode)")

    # Dates: required for API mode; optional filter for --input_csv mode
    parser.add_argument("--startdate",  default=None, help="UTC startdate 'YYYY-MM-DD HH:MM:SSZ'")
    parser.add_argument("--enddate",    default=None, help="UTC enddate 'YYYY-MM-DD HH:MM:SSZ'")

    # Common args
    parser.add_argument("--node",      required=True, help="Node identifier")
    parser.add_argument("--sourceids", required=True, help="Source ID (e.g. /VI/SU/B1/PYR/1) — determines calculation type")

    # PYR output
    parser.add_argument("--irradiance_data_path",        default=None, help="Output path for irradiance hours CSV (PYR)")

    # GEN/CON output
    parser.add_argument("--energyspike",                 default=None, type=Decimal, help="Energy spike in watt-hours (GEN/CON)")
    parser.add_argument("--startwatthours",              default=Decimal('0'), type=Decimal,
                        help="Starting wattHours counter value for the fill (GEN/CON); default 0. "
                             "Use the previous fill's last wattHours to keep monthly fills continuous")
    parser.add_argument("--electrical_energy_data_path", default=None, help="Output path for watt-hours CSV (GEN/CON)")

    args = parser.parse_args()

    # --- Validate args for the chosen input mode ---
    if args.input_csv is None:
        missing = [n for n in ("latitude", "longitude", "token", "solcast_csv_path", "startdate", "enddate")
                   if getattr(args, n) is None]
        if missing:
            parser.error("API mode requires: " + ", ".join("--" + m for m in missing)
                         + " (or pass --input_csv to use a local file)")

    # --- Step 1/2: Get rows ---
    if args.input_csv:
        print(f"Reading Solcast data from {args.input_csv} (skipping API)...", file=sys.stderr)
        rows = load_solcast_csv(args.input_csv, args.startdate, args.enddate)
        print(f"Loaded {len(rows)} records total", file=sys.stderr)
        if not rows:
            print("Error: no rows loaded from input CSV (check file and --startdate/--enddate filter)", file=sys.stderr)
            sys.exit(1)
    else:
        print("Starting Solcast download...", file=sys.stderr)
        rows = download_all(args.latitude, args.longitude, args.startdate, args.enddate, args.token)
        print(f"Downloaded {len(rows)} records total", file=sys.stderr)
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
        calculate_watthours(rows, args.energyspike, args.node, args.sourceids, args.electrical_energy_data_path, args.startwatthours)
        print(f"Pipeline complete. Import file: {args.electrical_energy_data_path}", file=sys.stderr)

    else:
        print("Error: Unable to determine calculation type from --sourceids. Must contain PYR, GEN, or CON.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
