from solarnetwork_python.client import Client
import sys
import argparse
import csv
import io
import gzip
import bz2
import lzma


def add_tag_column(csv_text, tag_value, tag_column_name="tag"):
    """Append a tag column with the given value to every data row of CSV text."""
    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)
    if not rows:
        return csv_text
    header, *data_rows = rows
    header.append(tag_column_name)
    for row in data_rows:
        row.append(tag_value)
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(data_rows)
    return output.getvalue()


def decompress_bytes(data, filepath):
    if filepath.endswith(".gz"):
        return gzip.decompress(data), "gz"
    elif filepath.endswith(".bz2"):
        return bz2.decompress(data), "bz2"
    elif filepath.endswith(".xz"):
        return lzma.decompress(data), "xz"
    elif filepath.endswith(".lz4"):
        try:
            import lz4.frame
        except ImportError:
            raise RuntimeError("lz4 package required for .lz4 files. Install with `pip install lz4`.")
        return lz4.frame.decompress(data), "lz4"
    else:
        raise ValueError(f"Unrecognized compression extension for file: {filepath}")


def compress_bytes(data, fmt):
    if fmt == "gz":
        return gzip.compress(data)
    elif fmt == "bz2":
        return bz2.compress(data)
    elif fmt == "xz":
        return lzma.compress(data)
    elif fmt == "lz4":
        import lz4.frame
        return lz4.frame.compress(data)
    else:
        raise ValueError(f"Unsupported compression format: {fmt}")


def solnet_import(node, sourceids, timezone, compression, filepath, token, secret, tag):
    """Import data from a specified node and data sources, tagging every row."""

    formatted_sourceids = sourceids.replace("/", "%2F")
    client = Client(token, secret)
    service_properties = {
        "headerRowCount": "1",
        "dateFormat": "yyyy-MM-dd HH:mm:ss",
        "nodeIdColumn": "1",
        "sourceIdColumn": "2",
        "dateColumnsValue": "3",
        "instantaneousDataColumns": "4",
        "accumulatingDataColumns": "5",
        "tagDataColumns": "6"
    }
    input_config = {
        "name": f"{node}_{formatted_sourceids}_Input",
        "timeZoneId": timezone,
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
            raw_data = openfile.read()
        try:
            decompressed, fmt = decompress_bytes(raw_data, filepath)
            tagged_text = add_tag_column(decompressed.decode("utf-8"), tag)
            csv_data = compress_bytes(tagged_text.encode("utf-8"), fmt)
            resp = client.import_compressed_data(import_config, csv_data)
            print(resp.get('jobId', 'No job ID returned'))
        except Exception as e:
            print(f"Error occurred while importing data: {e}")
    elif compression == "disabled":
        with open(filepath, 'r') as openfile:
            csv_text = openfile.read()
        try:
            csv_data = add_tag_column(csv_text, tag)
            resp = client.import_data(import_config, csv_data)
            print(resp.get('jobId', 'No job ID returned'))
        except Exception as e:
            print(f"Error occurred while importing data: {e}")
    else:
        print(f"Unknown compression setting. Set to either enabled or disabled.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import data into Solar Network.")
    parser.add_argument("--node", required=True, type=str, help="Node ID to import data from.")
    parser.add_argument("--sourceids", required=True, type=str, help="Source ID in format /VI/SU/B1/GEN/1")
    parser.add_argument("--timezone", required=True, type=str, help="Time zone for the data import.")
    parser.add_argument("--compression", required=True, type=str, help="Enable or disable compression.")
    parser.add_argument("--filepath", required=True, type=str, help="Path to the CSV file containing data.")
    parser.add_argument("--token", required=True, type=str, help="API token for authentication.")
    parser.add_argument("--secret", required=True, type=str, help="API secret for authentication.")
    parser.add_argument("--tag", default="synthetic", type=str, help="Tag value to apply to every imported row (default: synthetic).")

    args = parser.parse_args()

    solnet_import(args.node, args.sourceids, args.timezone, args.compression, args.filepath, args.token, args.secret, args.tag)
