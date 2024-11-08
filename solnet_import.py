from solarnetwork_python.client import Client
import sys
import argparse

def solnet_import(node, sourceids, timezone, compression, filepath, token, secret):
    """Import data from a specified node and data sources"""
    
    client = Client(token, secret)

    service_properties = {
        "headerRowCount": "1",
        "dateFormat": "yyyy-MM-dd HH:mm:ss",
        "nodeIdColumn": "1",
        "sourceIdColumn": "2",
        "dateColumnsValue": "3",
        "instantaneousDataColumns": "4",
        "accumulatingDataColumns": "5"
    }

    input_config = {
        "name": f"{node}_{sourceids}_Input",
        "timeZoneId": timezone,
        "serviceIdentifier": "net.solarnetwork.central.datum.imp.standard.SimpleCsvDatumImportInputFormatService",
        "serviceProperties": service_properties
    }
    
    import_config = {
        "name": f"{node}_{sourceids}_Import",
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
    else:
       if compression == "disabled":
          with open(filepath, 'r') as openfile:
              csv_data = openfile.read()
          try:
              resp = client.import_data(import_config, csv_data)
              print(resp.get('jobId', 'No job ID returned'))
          except Exception as e:
              print(f"Error occurred while importing data: {e}")
       else:
          print(f"Unknown compression setting. Set to either enabled or disabled.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import data into Solar Network.")
    parser.add_argument("--node", required=True, type=str, help="Node ID to import data from.")
    parser.add_argument("--sourceids", required=True, type=str, help="Source ID in format %2FVI%2FSU%2FB1%2FGEN%2F1")
    parser.add_argument("--timezone", required=True, type=str, help="Time zone for the data import.")
    parser.add_argument("--compression", required=True, type=str, help="Enable or disable compresion.")
    parser.add_argument("--filepath", required=True, type=str, help="Path to the CSV file containing data.")
    parser.add_argument("--token", required=True, type=str, help="API token for authentication.")
    parser.add_argument("--secret", required=True, type=str, help="API secret for authentication.")
    
    args = parser.parse_args()
    
    solnet_import(args.node, args.sourceids, args.timezone, args.compression, args.filepath, args.token, args.secret)

