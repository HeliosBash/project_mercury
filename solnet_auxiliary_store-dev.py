from solarnetwork_python.client import Client
import sys
import argparse
from datetime import datetime
import json

import argparse
import json
from datetime import datetime

def solnet_auxiliary(node_id, source_id, to_created, final_watt_hours, start_watt_hours):
    # Get current time as millisecond epoch
    current_time_ms = int(datetime.now().timestamp() * 1000)

    # Parse the to-created argument and convert to millisecond epoch
    to_created_dt = datetime.strptime(to_created, '%Y-%m-%d %H:%M:%S')
    to_created_ms = int(to_created_dt.timestamp() * 1000)

    auxiliary_data = {
        "from": {
            "created": current_time_ms,
            "nodeId": node_id,
            "sourceId": source_id,
            "type": "Reset"
        },
        "to": {
            "created": to_created_ms,
            "nodeId": node_id,
            "sourceId": source_id,
            "type": "Reset",
            "final": {
                "a": {
                    "wattHours": final_watt_hours
                }
            },
            "start": {
                "a": {
                    "wattHours": start_watt_hours
                }
            }
        }
    }

    client = Client(token, secret)
    try:
        resp = client.store_auxiliary(json.dumps(auxiliary_data))
        print(resp)
    except Exception as e:
        print(f"Error occurred while storing auxiliary data: {e}")

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Store auxiliary data with configurable node and source IDs')
    parser.add_argument('--node-id', required=True, help='Node ID for the data')
    parser.add_argument('--source-id', required=True, help='Source ID for both "from" and "to" sections')
    parser.add_argument('--to-created', required=True, help='Created timestamp for "to" section in format yyyy-MM-dd HH:mm:ss')
    parser.add_argument('--final-watt-hours', required=True, help='Final watt hours value')
    parser.add_argument('--start-watt-hours', required=True, help='Start watt hours value')
    args = parser.parse_args()

    solnet_auxiliary(args.node_id, args.source_id, args.to_created, args.final_watt_hours, args.start_watt_hours)
