from solarnetwork_python.client import Client
import json
import sys
import argparse

def solar_query(node, sourceids, startdate, enddate, aggregate, maxoutput, token, secret, header_mode='auto'):
    client = Client(token, secret)

    formatted_sourceids = sourceids.replace("/", "%2F")
    formatted_startdate = startdate.replace(" ", "T").replace(":", "%3A")
    formatted_enddate = enddate.replace(" ", "T").replace(":", "%3A")

    if aggregate == "None":
        param_str = f"localEndDate={formatted_enddate}&localStartDate={formatted_startdate}&max={maxoutput}&nodeId={node}&offset=0&sourceIds={formatted_sourceids}"
    else:
        param_str = f"aggregation={aggregate}&localEndDate={formatted_enddate}&localStartDate={formatted_startdate}&max={maxoutput}&nodeId={node}&offset=0&sourceIds={formatted_sourceids}"

    response = client.solarquery(param_str)
    
    if not response or 'results' not in response or len(response['results']) == 0:
        if header_mode in ['always', 'auto']:
            # Print empty header or standard columns even with no data
            print("created,localDate,localTime,nodeId,sourceId")
        return
    
    # Discover all columns in the order they appear in the first result
    # This preserves the API's original column order
    columns_ordered = []
    seen = set()
    
    # First pass: get columns from first result in their original order
    if response['results']:
        for key in response['results'][0].keys():
            columns_ordered.append(key)
            seen.add(key)
    
    # Second pass: add any additional columns from other results
    for element in response['results'][1:]:
        for key in element.keys():
            if key not in seen:
                columns_ordered.append(key)
                seen.add(key)
    
    sorted_columns = columns_ordered
    
    # Print header based on mode
    if header_mode == 'always' or (header_mode == 'auto' and response['results']):
        print(','.join(sorted_columns))
    
    # Print data rows
    for element in response['results']:
        row_values = [str(element.get(col, '')) for col in sorted_columns]
        print(','.join(row_values))

def main():
    parser = argparse.ArgumentParser(description="Solar query!")

    parser.add_argument("--node", required=True, type=str, help="Node ID (non-empty string)")
    parser.add_argument("--sourceids", required=True, type=str, help="Source ID in format /VI/SU/B1/GEN/1")
    parser.add_argument("--localstartdate", required=True, type=str, help="Start date in format 'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--localenddate", required=True, type=str, help="End date in format 'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--aggregate", required=True, help="Aggregation method")
    parser.add_argument("--maxoutput", required=True, help="Maximum output limit")
    parser.add_argument("--token", required=True, help="API token")
    parser.add_argument("--secret", required=True, help="API secret")
    parser.add_argument("--header", choices=['always', 'never', 'auto'], default='auto', 
                       help="Header mode: always=always print header, never=never print header, auto=print header only if data exists (default: auto)")

    args = parser.parse_args()

    try:
        solar_query(args.node, args.sourceids, args.localstartdate, args.localenddate, 
                   args.aggregate, args.maxoutput, args.token, args.secret, args.header)
    except Exception as e:
        print(f"Error: Unsuccessful API Call. {str(e)}")
        print("Please make sure that the token and secret are valid and have access to query the node and source ID, and check if the parameters are in the correct format")

if __name__ == "__main__":
    main()
