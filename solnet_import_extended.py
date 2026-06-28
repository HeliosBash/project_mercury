########################################
from solarnetwork_python.client import Client
import sys
import argparse
import re

# Columns that are always excluded from data mapping
METADATA_COLS = {'created', 'nodeId', 'sourceId', 'localDate', 'localTime'}

# Column name substrings that indicate ever-increasing counters
ACCUMULATING_KEYWORDS = [
    'wattHours', 'opTime', 'feedInTime', 'opTimeFan', 'opTimeHeater',
    'currentCompleteEventNumber'
]

# Column name substrings that indicate text/categorical (tag) values
TAG_KEYWORDS = ['phase']

def classify_column(col, sample_value=None):
    """Classify a column as instantaneous, accumulating, or tag based on its name/value."""
    for kw in TAG_KEYWORDS:
        if kw.lower() in col.lower():
            return 'tag'
    for kw in ACCUMULATING_KEYWORDS:
        if kw.lower() in col.lower():
            return 'accumulating'
    # Fallback: if the sample value isn't numeric, treat as tag
    if sample_value is not None and sample_value != '':
        try:
            float(sample_value)
        except ValueError:
            return 'tag'
    return 'instantaneous'

def cols_to_ref(col_numbers):
    """
    Convert a list of column numbers to a compact CSV columns reference string.
    e.g. [6,7,8,10,11] -> '6-8,10-11'
    """
    if not col_numbers:
        return ''
    col_numbers = sorted(col_numbers)
    ranges = []
    start = end = col_numbers[0]
    for n in col_numbers[1:]:
        if n == end + 1:
            end = n
        else:
            ranges.append(f'{start}-{end}' if start != end else str(start))
            start = end = n
    ranges.append(f'{start}-{end}' if start != end else str(start))
    return ','.join(ranges)

def build_column_refs(headers, sample_row=None):
    """
    Read CSV headers and return column number references for
    instantaneous, accumulating, and tag data, skipping metadata columns.
    Column numbers are 1-based.
    """
    instantaneous = []
    accumulating = []
    tag = []

    for i, col in enumerate(headers, 1):
        if col in METADATA_COLS:
            continue
        sample_value = None
        if sample_row and i - 1 < len(sample_row):
            sample_value = sample_row[i - 1]
        classification = classify_column(col, sample_value)
        if classification == 'accumulating':
            accumulating.append(i)
        elif classification == 'tag':
            tag.append(i)
        else:
            instantaneous.append(i)

    return cols_to_ref(instantaneous), cols_to_ref(accumulating), cols_to_ref(tag)

def normalize_timestamp(ts):
    """Remove trailing Z and ensure milliseconds are always exactly 3 digits."""
    ts = ts.rstrip('Z')
    if '.' in ts:
        # pad/truncate existing milliseconds to 3 digits
        ts = re.sub(r'(\.\d{1,3})$', lambda m: (m.group(1) + '000')[:4], ts)
    else:
        # no milliseconds present at all - add .000
        ts = ts + '.000'
    return ts

def strip_z_from_csv(raw):
    """Normalize timestamps in the created column (col 1)."""
    lines = raw.splitlines()
    cleaned = [lines[0]]  # keep header as-is
    for line in lines[1:]:
        if line:
            cols = line.split(',')
            cols[0] = normalize_timestamp(cols[0])
            cleaned.append(','.join(cols))
    return '\n'.join(cleaned)

def solnet_import(node, sourceids, compression, filepath, token, secret):
    """Import data from a specified node and data sources"""

    formatted_sourceids = sourceids.replace("/", "%2F")

    client = Client(token, secret)

    # Read headers and a sample data row from CSV to dynamically build column references
    with open(filepath, 'r') as f:
        headers = f.readline().strip().split(',')
        sample_line = f.readline().strip()
        sample_row = sample_line.split(',') if sample_line else None

    instantaneous_ref, accumulating_ref, tag_ref = build_column_refs(headers, sample_row)

    print(f'Detected {len(headers)} columns in CSV')
    print(f'  instantaneousDataColumns : {instantaneous_ref}')
    print(f'  accumulatingDataColumns  : {accumulating_ref}')
    print(f'  tagDataColumns           : {tag_ref}')

    service_properties = {
        "headerRowCount": "1",
        "dateFormat": "yyyy-MM-dd HH:mm:ss.SSS",   # Z stripped, ms padded to 3 digits
        "nodeIdColumn": "2",
        "sourceIdColumn": "3",
        "dateColumnsValue": "1",                    # use 'created' column only
        "instantaneousDataColumns": instantaneous_ref,
        "accumulatingDataColumns": accumulating_ref,
    }

    if tag_ref:
        service_properties["tagDataColumns"] = tag_ref

    input_config = {
        "name": f"{node}_{formatted_sourceids}_Input",
        "timeZoneId": "UTC",   # created column is already in UTC
        "serviceIdentifier": "net.solarnetwork.central.datum.imp.standard.SimpleCsvDatumImportInputFormatService",
        "serviceProperties": service_properties
    }

    import_config = {
        "name": f"{node}_{formatted_sourceids}_Import",
        "stage": True,
        "inputConfiguration": input_config
    }

    if compression == "enabled":
        with open(filepath, 'rb') as openfile:
            csv_data = openfile.read()
        try:
            resp = client.import_compressed_data(import_config, csv_data)
            print(resp.get('jobId', 'No job ID returned'))
        except Exception as e:
            print(f"Error occurred while importing data: {e}")

    elif compression == "disabled":
        with open(filepath, 'r') as openfile:
            csv_data = strip_z_from_csv(openfile.read())
        try:
            resp = client.import_data(import_config, csv_data)
            print(resp.get('jobId', 'No job ID returned'))
        except Exception as e:
            print(f"Error occurred while importing data: {e}")

    else:
        print("Unknown compression setting. Set to either 'enabled' or 'disabled'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import data into Solar Network.")
    parser.add_argument("--node",        required=True, type=str, help="Node ID to import data from.")
    parser.add_argument("--sourceids",   required=True, type=str, help="Source ID in format /VI/SU/B1/GEN/1")
    parser.add_argument("--compression", required=True, type=str, help="Enable or disable compression: 'enabled' or 'disabled'.")
    parser.add_argument("--filepath",    required=True, type=str, help="Path to the CSV file containing data.")
    parser.add_argument("--token",       required=True, type=str, help="API token for authentication.")
    parser.add_argument("--secret",      required=True, type=str, help="API secret for authentication.")

    args = parser.parse_args()

    solnet_import(
        args.node,
        args.sourceids,
        args.compression,
        args.filepath,
        args.token,
        args.secret
    )
