from solarnetwork_python.client import Client
import json
import sys
import argparse
from datetime import datetime
from pprint import pprint

def parse_reset_data(json_data):
    """
    Parse the reset data from the JSON response and return a cleaner format
    """
    if not json_data.get('success'):
        return "Error: Data parsing was not successful"

    data = json_data.get('data', {})
    total_results = data.get('totalResults', 0)
    results = data.get('results', [])

    parsed_results = []

    for result in results:
        reset_event = {
            'created_date': result.get('created'),
            'node_id': result.get('nodeId'),
            'source_id': result.get('sourceId'),
            'event_type': result.get('type'),
            'local_date': result.get('localDate'),
            'local_time': result.get('localTime'),
            'watt_hours': {
                'start': result.get('start', {}).get('a', {}).get('wattHours', 0),
                'final': result.get('final', {}).get('a', {}).get('wattHours', 0)
            }
        }

        # Extract event details from meta data if available
        if 'meta' in result and 'pm' in result['meta'] and 'ecogyEvent' in result['meta']['pm']:
            event = result['meta']['pm']['ecogyEvent']
            reset_event['event_id'] = event.get('id')
            reset_event['event_cause'] = event.get('cause')
            reset_event['event_description'] = event.get('description')
            reset_event['user_name'] = event.get('userName')
            reset_event['location'] = event.get('location')

        parsed_results.append(reset_event)

    return {
        'total_results': total_results,
        'count': len(parsed_results),
        'resets': parsed_results
    }


def list_auxiliary(node, sourceids, startdate, enddate, token, secret):
    """List auxiliary datum records within a date range"""
    
    formatted_sourceids = sourceids.replace("/", "%2F")
    formatted_startdate = startdate.replace(" ", "T").replace(":", "%3A")
    formatted_enddate = enddate.replace(" ", "T").replace(":", "%3A")

    param_str = f"endDate={formatted_enddate}&nodeIds={node}&sourceIds={formatted_sourceids}&startDate={formatted_startdate}&withoutTotalResultsCount=true"
    client = Client(token, secret)

    try:
        response = client.get_auxiliary(param_str)
        # Parse the data
        parsed_data = parse_reset_data(response)

        # Print the parsed data
        print("== Reset Data Summary ==")
        print(f"Total Results: {parsed_data['total_results']}")
        print(f"Reset Events: {parsed_data['count']}")

        # Print details for each reset event
        for i, reset in enumerate(parsed_data['resets'], 1):
            print(f"\n--- Reset Event {i} ---")
            print(f"Created: {reset['created_date']}")
            print(f"Node ID: {reset['node_id']}")
            print(f"Source ID: {reset['source_id']}")
            print(f"Local Date/Time: {reset['local_date']} {reset['local_time']}")
            print(f"Watt Hours: {reset['watt_hours']['start']} → {reset['watt_hours']['final']}")

            if 'event_cause' in reset:
                print(f"Cause: {reset['event_cause']}")
            if 'event_description' in reset:
                print(f"Description: {reset['event_description']}")
            if 'user_name' in reset:
                print(f"User: {reset['user_name']}")

    except Exception as e:
        print(f"Error occurred: {e}")
        sys.exit(1)


def store_auxiliary(node, source, aux_type, created, notes, final_data, start_data, meta_data, token, secret):
    """Store or update a datum auxiliary record"""

    client = Client(token, secret)

    # Build the auxiliary data object
    auxiliary_data = {
        "created": created,
        "nodeId": int(node),
        "sourceId": source,
        "type": aux_type
    }

    # Add optional notes
    if notes:
        auxiliary_data["notes"] = notes

    # Parse and add final data
    if final_data:
        auxiliary_data["final"] = json.loads(final_data)

    # Parse and add start data
    if start_data:
        auxiliary_data["start"] = json.loads(start_data)

    # Parse and add metadata
    if meta_data:
        auxiliary_data["meta"] = json.loads(meta_data)

    try:
        resp = client.store_datum_auxiliary(auxiliary_data)
        print("Success! Datum auxiliary record stored.")
        print(json.dumps(resp, indent=2))
    except Exception as e:
        print(f"Error occurred while storing datum auxiliary: {e}")
        sys.exit(1)


def delete_auxiliary(node, sourceids, eventdate, token, secret):
    """Delete a datum auxiliary record"""
    
    formatted_sourceids = sourceids.replace("/", "%2F")
    formatted_eventdate = eventdate.replace(" ", "T").replace(":", "%3A")

    param_str = f"date={formatted_eventdate}&nodeId={node}&sourceIds={formatted_sourceids}&type=reset"
    client = Client(token, secret)

    try:
        response = client.delete_auxiliary(param_str)
        print(response)
    except Exception as e:
        print(f"Error occurred: {e}")
        sys.exit(1)


def main():
    # Main parser
    parser = argparse.ArgumentParser(description="Solar Network Auxiliary Data Management Tool")
    
    # Add action parameter
    parser.add_argument("--action", required=True, choices=['list', 'store', 'delete'],
                       help="Action to perform: list, store, or delete")
    
    # Common arguments
    parser.add_argument("--node", required=True, type=str, help="Node ID")
    parser.add_argument("--token", required=True, help="API token")
    parser.add_argument("--secret", required=True, help="API secret")
    
    # Arguments for list action
    parser.add_argument("--sourceids", type=str, help="Source ID in format /VI/SU/B1/GEN/1 (required for list and delete)")
    parser.add_argument("--startdate", type=str, help="Start date in format 'YYYY-MM-DD HH:MM:SS' (required for list)")
    parser.add_argument("--enddate", type=str, help="End date in format 'YYYY-MM-DD HH:MM:SS' (required for list)")
    
    # Arguments for store action
    parser.add_argument("--source", type=str, help="Source ID (required for store)")
    parser.add_argument("--type", type=str, help="Datum auxiliary type, e.g., Reset (required for store)")
    parser.add_argument("--created", type=str, help="Event date in 'yyyy-MM-dd HH:mm:ss.SSS' format or millisecond epoch (required for store)")
    parser.add_argument("--notes", type=str, default=None, help="Optional notes describing the event (for store)")
    parser.add_argument("--final", type=str, help='Final data as JSON string, e.g., \'{"a":{"wattHours":123456789}}\' (required for store)')
    parser.add_argument("--start", type=str, help='Start data as JSON string, e.g., \'{"a":{"wattHours":123}}\' (required for store)')
    parser.add_argument("--meta", type=str, default=None, help='Optional metadata as JSON string, e.g., \'{"m":{"foo":"bar"}}\' (for store)')
    
    # Arguments for delete action
    parser.add_argument("--eventdate", type=str, help="UTC event date in format 'YYYY-MM-DD HH:MM:SS' (required for delete)")
    
    args = parser.parse_args()
    
    # Route to appropriate function based on action
    if args.action == 'list':
        if not all([args.sourceids, args.startdate, args.enddate]):
            parser.error("list action requires --sourceids, --startdate, and --enddate")
        list_auxiliary(args.node, args.sourceids, args.startdate, args.enddate, args.token, args.secret)
    
    elif args.action == 'store':
        if not all([args.source, args.type, args.created, args.final, args.start]):
            parser.error("store action requires --source, --type, --created, --final, and --start")
        store_auxiliary(args.node, args.source, args.type, args.created, args.notes, 
                       args.final, args.start, args.meta, args.token, args.secret)
    
    elif args.action == 'delete':
        if not all([args.sourceids, args.eventdate]):
            parser.error("delete action requires --sourceids and --eventdate")
        delete_auxiliary(args.node, args.sourceids, args.eventdate, args.token, args.secret)


if __name__ == "__main__":
    main()
