from solarnetwork_python.client import Client
import json
import sys
import argparse

def solar_query(node, sourceids, startdate, enddate, aggregate, maxoutput, token, secret):
    client = Client(token, secret)

    formatted_sourceids = sourceids.replace("/", "%2F")
    formatted_startdate = startdate.replace(" ", "T").replace(":", "%3A")
    formatted_enddate = enddate.replace(" ", "T").replace(":", "%3A")

    if aggregate == "None":
        param_str = f"endDate={formatted_enddate}&max={maxoutput}&nodeId={node}&offset=0&sourceIds={formatted_sourceids}&startDate={formatted_startdate}"
    else:
        param_str = f"aggregation={aggregate}&endDate={formatted_enddate}&max={maxoutput}&nodeId={node}&offset=0&sourceIds={formatted_sourceids}&startDate={formatted_startdate}"

    response = client.solarquery(param_str)

    # Auto-discover columns from the first result
    if response.get('results') and len(response['results']) > 0:
        # Get all unique keys from all results (in case some results have different keys)
        all_keys = set()
        for element in response['results']:
            all_keys.update(element.keys())
        
        # Sort keys with custom ordering - main columns alphabetically, then energy columns at the end
        energy_columns = []
        regular_columns = []
        
        for key in all_keys:
            # Check if it's an energy accumulation column (should go at the end)
            if key in ['irradiance', 'irradianceHours', 'watts', 'wattHours']:
                energy_columns.append(key)
            else:
                regular_columns.append(key)
        
        # Sort each group alphabetically
        regular_columns.sort()
        energy_columns.sort()
        
        # Combine: regular columns first, then energy columns
        columns = regular_columns + energy_columns
        
        # Print header
        print(','.join(columns))
        
        # Print data rows
        for element in response['results']:
            try:
                row_values = [str(element.get(col, '')) for col in columns]
                print(','.join(row_values))
            except Exception as e:
                print(f"Error processing row: {e}", file=sys.stderr)
    else:
        print("No results returned from query", file=sys.stderr)

def main():
    parser = argparse.ArgumentParser(description="Solar query with auto column discovery!")

    parser.add_argument("--node", required=True, type=str, help="Node ID (non-empty string)")
    parser.add_argument("--sourceids", required=True, type=str, help="Source ID in format /VI/SU/B1/GEN/1")
    parser.add_argument("--startdate", required=True, type=str, help="Start date in format YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--enddate", required=True, type=str, help="End date in format YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--aggregate", required=True, help="Aggregation method (use 'None' for no aggregation)")
    parser.add_argument("--maxoutput", required=True, help="Maximum output limit")
    parser.add_argument("--token", required=True, help="API token")
    parser.add_argument("--secret", required=True, help="API secret")

    args = parser.parse_args()

    try:
        solar_query(args.node, args.sourceids, args.startdate, args.enddate, args.aggregate, args.maxoutput, args.token, args.secret)
    except Exception as e:
        print(f"Error: Unsuccessful API Call. {str(e)}", file=sys.stderr)
        print("Please make sure that the token and secret are valid and has access to query the node and source ID and check if the parameters are in the correct format", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
