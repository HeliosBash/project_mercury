#!/usr/bin/python3
"""
solnet_filler.py

Unified Python port of the solnet-filler bash script.

Workflow:
  1. Parse and validate arguments
  2. Validate timezone; prompt for correction if invalid
  3. Convert local start/end datetimes to UTC
  4. Query SolarNetwork for existing datum in the gap
  5. If datum exist → optionally expire (delete) them
  6. Download Solcast GHI data and calculate irradiance/watt-hours (via solcast_pipeline)
  7. Display head/tail of output file
  8. Stage the import job (compressed if file ≥ 20 MB)
  9. Preview the staged import
 10. Prompt user to confirm → confirm or delete the staged job
"""

import argparse
import datetime
import os
import sys
import time
import lzma
import shutil
import zoneinfo
from decimal import Decimal, getcontext
from pathlib import Path

# ---------------------------------------------------------------------------
# Inline sub-modules (formerly separate scripts)
# ---------------------------------------------------------------------------

# ── solcast_pipeline ────────────────────────────────────────────────────────
import requests
import json
import re
from datetime import datetime as dt, timedelta

MAX_RANGE_DAYS = 30
MAX_RANGE_SECONDS = MAX_RANGE_DAYS * 24 * 60 * 60


def _parse_date(datestr):
    clean = datestr.strip().rstrip('Z')
    if '.' in clean:
        clean = clean.split('.')[0]
    return dt.strptime(clean, '%Y-%m-%d %H:%M:%S')


def _format_date(d):
    return d.strftime('%Y-%m-%d %H:%M:%S') + 'Z'


def _format_number(value):
    if value == 0 or value == Decimal('0'):
        return "0"
    return f"{value:.2f}".rstrip('0').rstrip('.')


def build_chunks(startdate, enddate):
    """Return list of (chunk_start, chunk_end) string tuples within 30-day limits."""
    start_d = _parse_date(startdate)
    end_d   = _parse_date(enddate)
    start_epoch = int(start_d.timestamp())
    end_epoch   = int(end_d.timestamp())
    diff        = end_epoch - start_epoch
    chunks = []
    if diff > MAX_RANGE_SECONDS:
        partial_end_d = start_d + timedelta(days=MAX_RANGE_DAYS)
        chunks.append((_format_date(start_d), _format_date(partial_end_d)))
        partial_start_d = partial_end_d
        diff = end_epoch - int(partial_start_d.timestamp())
        while diff > MAX_RANGE_SECONDS:
            partial_end_d = partial_start_d + timedelta(days=MAX_RANGE_DAYS)
            chunks.append((_format_date(partial_start_d), _format_date(partial_end_d)))
            partial_start_d = partial_end_d
            diff = end_epoch - int(partial_start_d.timestamp())
        chunks.append((_format_date(partial_start_d), _format_date(end_d)))
    else:
        chunks.append((_format_date(start_d), _format_date(end_d)))
    return chunks


def solcast_download_chunk(lat, lon, startdate, enddate, token):
    """Download one Solcast API chunk. Returns list of row dicts."""
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
        print(f"Error: Solcast returned status {response.status_code}", file=sys.stderr)
        sys.exit(1)
    try:
        data = response.json()
    except json.JSONDecodeError:
        print("Error: Failed to decode Solcast JSON response", file=sys.stderr)
        sys.exit(1)
    rows = []
    for i in data.get('estimated_actuals', []):
        period_end   = i["period_end"].replace('+00:00', 'Z')
        dateobj      = dt.strptime(period_end.replace('T', ' ').replace('Z', ''), '%Y-%m-%d %H:%M:%S')
        ghi          = i["ghi"]
        period       = i["period"]
        duration     = int(re.sub("[PTM]", "", period))
        period_start = str(dateobj - timedelta(minutes=duration)).replace(' ', 'T') + 'Z'
        rows.append({'period_end': period_end, 'period_start': period_start, 'period': period, 'ghi': ghi})
    return rows


def download_all(lat, lon, startdate, enddate, token):
    """Download all chunks and return combined list of rows."""
    chunks = build_chunks(startdate, enddate)
    if len(chunks) > 1:
        log(f"Date range exceeds 30 days, splitting into {len(chunks)} chunks")
    all_rows = []
    for idx, (chunk_start, chunk_end) in enumerate(chunks):
        if len(chunks) > 1:
            log(f"Downloading chunk {idx + 1}/{len(chunks)}: {chunk_start} → {chunk_end}")
        all_rows.extend(solcast_download_chunk(lat, lon, chunk_start, chunk_end, token))
    return all_rows


def save_solcast_csv(rows, path):
    with open(path, 'w') as f:
        for row in rows:
            f.write(f"{row['period_end']},{row['period_start']},{row['period']},{row['ghi']}\n")
    log(f"Solcast data saved to: {path}")


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
                out.write(f"{node},{sourceids},{period_start_fmt},{row['ghi']},{_format_number(ghi_decimal)}\n")
                ghi_prev = ghi_decimal
            else:
                ghi_5min = ghi_decimal / Decimal('12')
                ghi_new  = ghi_prev + ghi_5min
                out.write(f"{node},{sourceids},{period_start_fmt},{row['ghi']},{_format_number(ghi_new)}\n")
                ghi_prev = ghi_new
    log(f"Irradiance data saved to: {irradiance_data_path}")


def calculate_watthours(rows, energyspike, node, sourceids, output_path):
    """Calculate watts and cumulative watt-hours and write output CSV."""
    getcontext().prec = 28
    total_energy = Decimal(str(energyspike))
    ghitotal     = sum(Decimal(str(row['ghi'])) for row in rows)
    log(f"Total Irradiance Calculated: {ghitotal}")
    log(f"Total Energy Spike in watthours: {energyspike}")
    log("Calculating Backfill Data Driven by Irradiance Data")
    previous_watthours = Decimal('0')
    with open(output_path, 'w', newline='') as out:
        out.write("NodeID,SourceID,Date,watts,wattHours\n")
        for row in rows:
            irr             = Decimal(str(row['ghi']))
            formatted_start = row['period_start'].replace("T", " ").replace("Z", "")
            watts           = total_energy * (irr / ghitotal) * 12
            watthours       = previous_watthours + (watts / 12)
            previous_watthours = watthours
            out.write(f"{node},{sourceids},{formatted_start},{_format_number(watts)},{_format_number(watthours)}\n")
    log(f"Watt-hours data saved to: {output_path}")


# ── solnet_query ─────────────────────────────────────────────────────────────
from solarnetwork_python.client import Client as _Client


def _solar_query_batch(client, node, sourceids, startdate, enddate, aggregate, maxoutput, action='utc', verbose=False):
    formatted_sourceids = sourceids.replace("/", "%2F")
    formatted_startdate = startdate.replace(" ", "T").replace(":", "%3A")
    formatted_enddate   = enddate.replace(" ", "T").replace(":", "%3A")
    if action == 'utc':
        if aggregate == "None":
            param_str = f"endDate={formatted_enddate}&max={maxoutput}&nodeId={node}&offset=0&sourceIds={formatted_sourceids}&startDate={formatted_startdate}"
        else:
            param_str = f"aggregation={aggregate}&endDate={formatted_enddate}&max={maxoutput}&nodeId={node}&offset=0&sourceIds={formatted_sourceids}&startDate={formatted_startdate}"
    else:
        if aggregate == "None":
            param_str = f"localEndDate={formatted_enddate}&localStartDate={formatted_startdate}&max={maxoutput}&nodeId={node}&offset=0&sourceIds={formatted_sourceids}"
        else:
            param_str = f"aggregation={aggregate}&localEndDate={formatted_enddate}&localStartDate={formatted_startdate}&max={maxoutput}&nodeId={node}&offset=0&sourceIds={formatted_sourceids}"
    if verbose:
        log(f"API Query: {param_str[:200]}...")
    try:
        response = client.solarquery(param_str)
    except Exception as e:
        log(f"API Error: {str(e)}")
        return None, None
    if not response or 'results' not in response:
        return None, None
    return response.get('results', []), response


def _discover_columns_sorted(results_list):
    all_keys = set()
    for results in results_list:
        if results:
            for element in results:
                all_keys.update(element.keys())
    energy_columns  = [k for k in all_keys if k in ('irradiance', 'irradianceHours', 'watts', 'wattHours')]
    regular_columns = [k for k in all_keys if k not in energy_columns]
    return sorted(regular_columns) + sorted(energy_columns)


def solar_query_utc(node, sourceids, startdate, enddate, aggregate, maxoutput, token, secret, verbose=False):
    """UTC query (single batch). Returns (lines_list, error_flag)."""
    client = _Client(token, secret)
    results, _ = _solar_query_batch(client, node, sourceids, startdate, enddate,
                                    aggregate, maxoutput, 'utc', verbose)
    if results is None:
        return [], True
    if not results:
        return ["created,nodeId,sourceId"], False
    columns = _discover_columns_sorted([results])
    lines   = [','.join(columns)]
    for element in results:
        lines.append(','.join(str(element.get(col, '')) for col in columns))
    return lines, False


# ── solnet_expire ────────────────────────────────────────────────────────────

def expire_preview(node, sourceids, localstartdate, localenddate, token, secret):
    """Return datumCount (int) for the given range."""
    formatted_sourceids = sourceids.replace("/", "%2F")
    formatted_startdate = localstartdate.replace(" ", "T").replace(":", "%3A")
    formatted_enddate   = localenddate.replace(" ", "T").replace(":", "%3A")
    param_str = (f"aggregationKey=0&localEndDate={formatted_enddate}"
                 f"&localStartDate={formatted_startdate}&nodeIds={node}&sourceIds={formatted_sourceids}")
    client = _Client(token, secret)
    response = client.expirepreview(param_str)
    return int(response['datumCount'])


def expire_confirm(node, sourceids, localstartdate, localenddate, token, secret):
    """Execute expire/delete for the given range. Returns API response."""
    formatted_sourceids = sourceids.replace("/", "%2F")
    formatted_startdate = localstartdate.replace(" ", "T").replace(":", "%3A")
    formatted_enddate   = localenddate.replace(" ", "T").replace(":", "%3A")
    param_str = (f"aggregationKey=0&localEndDate={formatted_enddate}"
                 f"&localStartDate={formatted_startdate}&nodeIds={node}&sourceIds={formatted_sourceids}")
    client = _Client(token, secret)
    return client.expireconfirm(param_str)


# ── solnet_import ─────────────────────────────────────────────────────────────

def solnet_import(node, sourceids, timezone, compression, filepath, token, secret):
    """Stage a datum import job. Returns jobId string."""
    formatted_sourceids = sourceids.replace("/", "%2F")
    client = _Client(token, secret)
    service_properties = {
        "headerRowCount": "1",
        "dateFormat": "yyyy-MM-dd HH:mm:ss",
        "nodeIdColumn": "1",
        "sourceIdColumn": "2",
        "dateColumnsValue": "3",
        "instantaneousDataColumns": "4",
        "accumulatingDataColumns": "5",
    }
    input_config = {
        "name": f"{node}_{formatted_sourceids}_Input",
        "timeZoneId": timezone,
        "serviceIdentifier": "net.solarnetwork.central.datum.imp.standard.SimpleCsvDatumImportInputFormatService",
        "serviceProperties": service_properties,
    }
    import_config = {
        "name": f"{node}_{formatted_sourceids}_Import",
        "stage": True,
        "inputConfiguration": input_config,
    }
    if compression == "enabled":
        with open(filepath, 'rb') as f:
            csv_data = f.read()
        resp = client.import_compressed_data(import_config, csv_data)
    else:
        with open(filepath, 'r') as f:
            csv_data = f.read()
        resp = client.import_data(import_config, csv_data)
    return resp.get('jobId', '')


# ── solnet_manage_jobs ────────────────────────────────────────────────────────

def preview_import_job(jobid, token, secret):
    """Print a preview of staged import data. Returns list of result dicts."""
    client = _Client(token, secret)
    response = client.previewimportjobs(jobid)
    log(str(response))
    results = []
    for element in response.get('data', {}).get('results', []):
        row = ','.join(str(element.get(k, '')) for k in
                       ('created', 'localDate', 'localTime', 'nodeId', 'sourceId', 'i', 'a'))
        log(row)
        results.append(element)
    return results


def confirm_import_job(jobid, token, secret):
    client = _Client(token, secret)
    resp = client.confirmimportjobs(jobid)
    log(str(resp))
    return resp


def delete_import_job(jobid, token, secret):
    client = _Client(token, secret)
    resp = client.deleteimportjobs(jobid)
    log(str(resp))
    return resp


# ---------------------------------------------------------------------------
# Logging & interactive helpers
# ---------------------------------------------------------------------------

_log_path = None


def log(message):
    ts   = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"{ts} {message}"
    print(line)
    if _log_path:
        with open(_log_path, 'a') as f:
            f.write(line + '\n')


def confirm_action(prompt):
    """Prompt the user for Y/N. Loops until valid. Returns 'Y' or 'N'."""
    while True:
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        reply = input(f"{ts} {prompt} [Y/N]: ").strip().upper()
        if reply in ('Y', 'N'):
            log(f"User response: {reply}")
            return reply


def countdown(seconds, message):
    """Log a countdown message, then sleep. Ctrl+C cancels."""
    log(f"{message} in {seconds} seconds. Hit Ctrl+C to Cancel")
    time.sleep(seconds)


# ---------------------------------------------------------------------------
# Timezone validation
# ---------------------------------------------------------------------------

def validate_timezone(tz_name):
    """Return True if tz_name is a valid IANA timezone identifier."""
    try:
        zoneinfo.ZoneInfo(tz_name)
        return True
    except (zoneinfo.ZoneInfoNotFoundError, KeyError):
        return False


def prompt_for_valid_timezone(initial_tz):
    """Loop until user provides a valid timezone. Returns the valid name."""
    tz = initial_tz
    while not validate_timezone(tz):
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        tz = input(f"{ts} Not a valid timezone. Enter timezone: ").strip()
    return tz


# ---------------------------------------------------------------------------
# Datetime conversion helpers
# ---------------------------------------------------------------------------

def local_to_utc(local_dt_str, timezone_name):
    """
    Convert a naive local datetime string 'YYYY-MM-DD HH:MM:SS' in the given
    timezone to a UTC datetime string 'YYYY-MM-DD HH:MM:SSZ'.
    """
    tz  = zoneinfo.ZoneInfo(timezone_name)
    naive = datetime.datetime.strptime(local_dt_str, '%Y-%m-%d %H:%M:%S')
    aware = naive.replace(tzinfo=tz)
    utc   = aware.astimezone(datetime.timezone.utc)
    return utc.strftime('%Y-%m-%d %H:%M:%S') + 'Z'


def url_encode_datetime(dt_str):
    """'YYYY-MM-DD HH:MM:SSZ' → 'YYYY-MM-DDTHH%3AMM%3ASSZ' for filenames."""
    return dt_str.replace(' ', 'T').replace(':', '%3A')


# ---------------------------------------------------------------------------
# File helpers
# ---------------------------------------------------------------------------

def compress_xz(src_path):
    """Create src_path.xz alongside the original. Returns compressed path."""
    xz_path = src_path + '.xz'
    with open(src_path, 'rb') as f_in, lzma.open(xz_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    return xz_path


def show_head_tail(filepath, n=5):
    """Log the first and last n lines of a file."""
    with open(filepath, 'r') as f:
        lines = f.readlines()
    log(f"Displaying parts of output file {filepath}")
    for line in lines[:n]:
        log(line.rstrip())
    log(".....")
    for line in lines[-n:]:
        log(line.rstrip())


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def main():
    global _log_path
    getcontext().prec = 28

    # ── Argument parsing ───────────────────────────────────────────────────
    parser = argparse.ArgumentParser(
        description="solnet-filler: Fill SolarNetwork datum gaps using Solcast irradiance data.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--node",          "-n", required=True,  help="Node ID")
    parser.add_argument("--sourceid",      "-i", required=True,  help="Source ID  e.g. /VI/SU/B1/GEN/1")
    parser.add_argument("--timezone",      "-z", required=True,  help="Local timezone  e.g. Pacific/Auckland")
    parser.add_argument("--startdatetime", "-s", required=True,  help="Local start datetime  'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--enddatetime",   "-e", required=True,  help="Local end datetime    'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--latitude",      "-a", required=True,  help="Site latitude")
    parser.add_argument("--longitude",     "-o", required=True,  help="Site longitude")
    parser.add_argument("--energyspike",   "-g", required=True,  help="Energy spike in watt-hours (set 0 for PYR)")
    parser.add_argument("--api",           "-p", required=True,  help="Solcast API token")
    parser.add_argument("--token",         "-k", required=True,  help="SolarNetwork API token")
    parser.add_argument("--secret",        "-c", required=True,  help="SolarNetwork API secret")
    args = parser.parse_args()

    node            = args.node
    sid             = args.sourceid
    timezone        = args.timezone
    startdatetime   = args.startdatetime
    enddatetime     = args.enddatetime
    lat             = args.latitude
    lon             = args.longitude
    energyspike     = Decimal(args.energyspike)
    solcast_token   = args.api
    token           = args.token
    secret          = args.secret

    # ── Setup ──────────────────────────────────────────────────────────────
    os.makedirs("data", exist_ok=True)
    runtime  = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    _log_path = f"data/solnet-filler-{runtime}.log"
    maxoutput = 1000000

    # ── Source ID type check ───────────────────────────────────────────────
    sourceids = sid.replace("/", "%2F")   # URL-encoded form used in filenames / API calls

    if not any(t in sourceids for t in ("GEN", "PYR", "CON")):
        log("Unable to determine type of datum (GEN/PYR/CON) from source ID provided")
        sys.exit(1)

    if "GEN" in sourceids and energyspike == 0:
        log("For GEN Fills, energyspike should be greater than 0")
        sys.exit(1)

    log(
        f"Executed: solnet_filler.py --node {node} --sourceid {sid} --timezone {timezone} "
        f"--startdatetime \"{startdatetime}\" --enddatetime \"{enddatetime}\" "
        f"--latitude {lat} --longitude {lon} --energyspike {energyspike} "
        f"--api {solcast_token} --token {token} --secret {secret}"
    )

    # ── Timezone validation ────────────────────────────────────────────────
    timezone = prompt_for_valid_timezone(timezone)

    # ── Convert local → UTC ────────────────────────────────────────────────
    utcstartdate = local_to_utc(startdatetime, timezone)
    utcenddate   = local_to_utc(enddatetime,   timezone)

    log(f"Local : {startdatetime} ({timezone})")
    log(f"UTC   : {utcstartdate}")
    log(f"Local : {enddatetime} ({timezone})")
    log(f"UTC   : {utcenddate}")

    # URL-encoded forms used in filenames
    startdate_url = url_encode_datetime(utcstartdate)
    enddate_url   = url_encode_datetime(utcenddate)

    # ── Step 1 : Query for existing datum in the gap ───────────────────────
    log("Checking for any data between specified date range")
    log(
        f"Executing python3 solnet_query.py --action=\"utc\" --node=\"{node}\" --sourceids=\"{sid}\" "
        f"--startdate=\"{startdate_url}\" --enddate=\"{enddate_url}\" "
        f"--aggregate=\"None\" --maxoutput=\"{maxoutput}\" --token=\"{token}\" --secret=\"{secret}\""
    )

    datum_file = f"data/{node}_{sourceids}_{startdate_url}_{enddate_url}_datum"
    lines, query_error = solar_query_utc(
        node, sid, startdate_url, enddate_url, "None", maxoutput, token, secret
    )

    with open(datum_file, 'w') as f:
        f.write('\n'.join(lines) + '\n')

    if query_error or any("Error" in l for l in lines):
        log('\n'.join(lines))
        log(
            "Usage: solnet_filler.py --node N --sourceid S --timezone Z "
            "--startdatetime \"YYYY-MM-DD HH:MM:SS\" --enddatetime \"YYYY-MM-DD HH:MM:SS\" "
            "--latitude A --longitude O --energyspike G --api P --token K --secret C"
        )
        sys.exit(1)

    # datum_count = data rows (skip header)
    datum_count = max(0, len(lines) - 1)
    log(f"Datum count in gap: {datum_count}")

    # ── Step 2 : Optionally expire existing datum ──────────────────────────
    formatted_localstartdate = startdatetime
    formatted_localenddate   = enddatetime

    if datum_count > 0:
        log(
            f"Datum count in gap is greater than 0. Preview and Confirm expire commands will be executed:\n"
            f"  Preview: python3 solnet_expire.py --action=\"preview\" --node=\"{node}\" --sourceids=\"{sid}\" "
            f"--localstartdate=\"{formatted_localstartdate}\" --localenddate=\"{formatted_localenddate}\" "
            f"--token=\"{token}\" --secret=\"{secret}\"\n"
            f"  Confirm: python3 solnet_expire.py --action=\"confirm\" --node=\"{node}\" --sourceids=\"{sid}\" "
            f"--localstartdate=\"{formatted_localstartdate}\" --localenddate=\"{formatted_localenddate}\" "
            f"--token=\"{token}\" --secret=\"{secret}\""
        )

        continue_result = confirm_action("Would you like to remove the data between the gap now")

        if continue_result == 'Y':
            log(f"Executing python3 solnet_expire.py --action=\"preview\" --node=\"{node}\" --sourceids=\"{sid}\" --localstartdate=\"{formatted_localstartdate}\" --localenddate=\"{formatted_localenddate}\" --token=\"{token}\" --secret=\"{secret}\"")
            result_count = expire_preview(
                node, sid, formatted_localstartdate, formatted_localenddate, token, secret
            )
            log(f"COUNT RESULT = {result_count}")

            if result_count != datum_count:
                log("Skipping Process. Count doesn't match, adjust date and run manually.")
            else:
                log("Count of previewed data match count of data between date range.")
                countdown(10, "Executing expire_confirm")
                resp = expire_confirm(
                    node, sid, formatted_localstartdate, formatted_localenddate, token, secret
                )
                log(str(resp))
                log(
                    f"Execute python3 solnet_manage_jobs.py --job=\"expire\" --action=\"list\" "
                    f"--token=\"{token}\" --secret=\"{secret}\" to monitor progress"
                )
        else:
            log("Delete Process Aborted")

    else:
        log("No Data Detected. Preparing Solcast Query Data")

    # ── Step 3 : Solcast download + calculation ────────────────────────────
    solcast_csv_path = (
        f"data/{node}_{sourceids}_{lat}_{lon}_{startdate_url}_{enddate_url}_solcast_result.csv"
    )
    runtime = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')   # refresh runtime for output file names

    if "PYR" in sourceids:
        irradiance_data_path = (
            f"data/{node}_{sourceids}_{startdate_url}_{enddate_url}_PYRGAP_SolNetIMport_{runtime}.csv"
        )
        log(
            f"Executing python3 solcast_pipeline.py --latitude=\"{lat}\" --longitude=\"{lon}\" "
            f"--startdate=\"{utcstartdate}\" --enddate=\"{utcenddate}\" "
            f"--token=\"{solcast_token}\" --node=\"{node}\" --sourceids=\"{sid}\" "
            f"--solcast_csv_path=\"{solcast_csv_path}\" --irradiance_data_path=\"{irradiance_data_path}\" in 10 seconds"
        )
        countdown(10, "Starting Solcast pipeline")

        rows = download_all(lat, lon, utcstartdate, utcenddate, solcast_token)
        log(f"Downloaded {len(rows)} records total")
        save_solcast_csv(rows, solcast_csv_path)
        calculate_irradiance(rows, irradiance_data_path, node, sid)
        file_to_import = irradiance_data_path

    elif "GEN" in sourceids or "CON" in sourceids:
        electrical_energy_data_path = (
            f"data/{node}_{sourceids}_EEGAP_SolNetIMport_{runtime}.csv"
        )
        log(
            f"Executing python3 solcast_pipeline.py --latitude=\"{lat}\" --longitude=\"{lon}\" "
            f"--startdate=\"{utcstartdate}\" --enddate=\"{utcenddate}\" "
            f"--token=\"{solcast_token}\" --node=\"{node}\" --sourceids=\"{sid}\" "
            f"--solcast_csv_path=\"{solcast_csv_path}\" --energyspike={energyspike} "
            f"--electrical_energy_data_path=\"{electrical_energy_data_path}\" in 10 seconds"
        )
        countdown(10, "Starting Solcast pipeline")

        rows = download_all(lat, lon, utcstartdate, utcenddate, solcast_token)
        log(f"Downloaded {len(rows)} records total")
        save_solcast_csv(rows, solcast_csv_path)
        calculate_watthours(rows, energyspike, node, sid, electrical_energy_data_path)
        file_to_import = electrical_energy_data_path

    else:
        log("Unable to determine type of datum from source ID")
        sys.exit(1)

    # ── Step 4 : Preview output, compress ─────────────────────────────────
    show_head_tail(file_to_import)

    log(f"Compressing output file {file_to_import}")
    xz_path = compress_xz(file_to_import)
    log("Process completed")

    # ── Step 5 : Stage import ──────────────────────────────────────────────
    file_size_bytes = os.path.getsize(file_to_import)
    file_size_mb    = file_size_bytes // 1048576
    log(f"Size of file: {file_size_mb} MB")

    if file_size_mb < 20:
        compress      = "disabled"
        import_target = file_to_import
    else:
        compress      = "enabled"
        import_target = xz_path

    log(
        f"Executing python3 solnet_import.py --node=\"{node}\" --sourceids=\"{sid}\" --timezone=\"UTC\" "
        f"--compression=\"{compress}\" --filepath=\"{import_target}\" "
        f"--token=\"{token}\" --secret=\"{secret}\" in 10 seconds. Hit Ctrl+C to Cancel"
    )
    countdown(10, "Staging import")

    try:
        jobid = solnet_import(node, sid, "UTC", compress, import_target, token, secret)
    except Exception as e:
        log(f"Error occurred while importing data: {e}")
        sys.exit(1)

    log(f"Import staged. Job ID: {jobid}")

    # ── Step 6 : Preview staged import ────────────────────────────────────
    log(f"Executing python3 solnet_manage_jobs.py --job=\"import\" --action=\"preview\" --token=\"{token}\" --secret=\"{secret}\" --jobid=\"{jobid}\" to preview imported data")
    preview_import_job(jobid, token, secret)

    log(
        f"To apply staged data, python3 solnet_manage_jobs.py --job=\"import\" --action=\"confirm\" "
        f"--token=\"{token}\" --secret=\"{secret}\" --jobid=\"{jobid}\" will be executed"
    )

    # ── Step 7 : User confirmation → confirm or delete ────────────────────
    confirm_result = confirm_action("Would You Like To Proceed To Apply Staged Data")

    if confirm_result == 'Y':

        if datum_count > 0:
            log("Checking if expire process has completed.")
            result_count = expire_preview(
                node, sid, formatted_localstartdate, formatted_localenddate, token, secret
            )

            if result_count == 0:
                log(f"Expire result count: {result_count}. Process completed.")
                countdown(10, f"Executing python3 solnet_manage_jobs.py --job=\"import\" --action=\"confirm\" --token=\"{token}\" --secret=\"{secret}\" --jobid=\"{jobid}\"")
                confirm_import_job(jobid, token, secret)
                log(
                    f"Execute python3 solnet_manage_jobs.py --job=\"import\" --action=\"list\" "
                    f"--token=\"{token}\" --secret=\"{secret}\" to view import progress"
                )
            else:
                log("Process aborted. Expire process not completed. Run manually.")
                log(
                    f"Once unwanted data has been deleted, execute python3 solnet_manage_jobs.py "
                    f"--job=\"import\" --action=\"confirm\" --token=\"{token}\" --secret=\"{secret}\" "
                    f"--jobid=\"{jobid}\" in 10 seconds. Hit CTRL+C to cancel"
                )
        else:
            countdown(10, f"Executing python3 solnet_manage_jobs.py --job=\"import\" --action=\"confirm\" --token=\"{token}\" --secret=\"{secret}\" --jobid=\"{jobid}\"")
            confirm_import_job(jobid, token, secret)
            log(
                f"Execute python3 solnet_manage_jobs.py --job=\"import\" --action=\"list\" "
                f"--token=\"{token}\" --secret=\"{secret}\" to view import progress"
            )

    else:
        log(
            f"Import Process Aborted. Deleting Staged Data. Executing python3 solnet_manage_jobs.py "
            f"--job=\"import\" --action=\"delete\" --token=\"{token}\" --secret=\"{secret}\" "
            f"--jobid=\"{jobid}\" in 10 seconds. Hit Ctrl+C to cancel"
        )
        countdown(10, f"Deleting staged job {jobid}")
        delete_import_job(jobid, token, secret)


if __name__ == "__main__":
    main()
