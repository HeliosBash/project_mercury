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
    
    formatted_sourceids=sourceids.replace("/", "%2F")
    formatted_startdate=startdate.replace(" ","T").replace(":","%3A")
    formatted_enddate=enddate.replace(" ","T").replace(":","%3A")

    param_str = f"endDate={formatted_enddate}&nodeIds={node}&sourceIds={formatted_sourceids}&startDate={formatted_startdate}&withoutTotalResultsCount=true"
    client = Client(token, secret)
    
    try:
        response = client.get_auxiliary(param_str)
        #print(response)
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

def main():
    # Parser object to handle the arguments
    parser = argparse.ArgumentParser(description="API Query Tool")

    # Add arguments 
    parser.add_argument("--node", required=True, type=str, help="Node ID (non-empty string)")
    parser.add_argument("--sourceids", required=True, type=str, help="Source ID in format /VI/SU/B1/GEN/1")
    parser.add_argument("--startdate", required=True, type=str, help="Start date in format 'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--enddate", required=True, type=str, help="End date in format 'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--token", required=True, help="API token")
    parser.add_argument("--secret", required=True, help="API secret")

    # Parse the arguments the user has provided
    args = parser.parse_args()

    # Print parsed arguments for verification
    # print(f"Node: {args.node}, Source IDs: {args.sourceids}, Start Date: {args.startdate}, End Date: {args.enddate}")

    # Call the solar query function with parsed arguments
    list_auxiliary(args.node, args.sourceids, args.startdate, args.enddate, args.token, args.secret)

if __name__ == "__main__":
    main()


