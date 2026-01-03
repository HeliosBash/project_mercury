from solarnetwork_python.client import Client
import json
import sys
import argparse

def solar_query(node, sourceids, startdate, enddate, token, secret, action):
    formatted_sourceids = sourceids.replace("/", "%2F")
    formatted_startdate = startdate.replace(" ", "T").replace(":", "%3A")
    formatted_enddate = enddate.replace(" ", "T").replace(":", "%3A")
    param_str = f"aggregationKey=0&localEndDate={formatted_enddate}&localStartDate={formatted_startdate}&nodeIds={node}&sourceIds={formatted_sourceids}"
    
    client = Client(token, secret)
    
    try:
        if action == "preview":
            response = client.expirepreview(param_str)
            print(response['datumCount'])
        elif action == "confirm":
            response = client.expireconfirm(param_str)
            print(response)
    except Exception as e:
        print(f"Error occurred: {e}")
        sys.exit(1)

def main():
    # Parser object to handle the arguments
    parser = argparse.ArgumentParser(description="SolarNetwork Expire Tool")
    
    # Add arguments
    parser.add_argument("--action", required=True, choices=["preview", "confirm"], 
                        help="Action to perform: 'preview' or 'confirm'")
    parser.add_argument("--node", required=True, type=str, help="Node ID (non-empty string)")
    parser.add_argument("--sourceids", required=True, type=str, help="Source ID in format /VI/SU/B1/GEN/1")
    parser.add_argument("--localstartdate", required=True, type=str, help="Local start date in format 'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--localenddate", required=True, type=str, help="Local end date in format 'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--token", required=True, help="API token")
    parser.add_argument("--secret", required=True, help="API secret")
    
    # Parse the arguments the user has provided
    args = parser.parse_args()
    
    # Print parsed arguments for verification (only for confirm action)
    if args.action == "confirm":
        print(f"Node: {args.node}, Source IDs: {args.sourceids}, Start Date: {args.localstartdate}, End Date: {args.localenddate}")
    
    # Call the solar query function with parsed arguments
    solar_query(args.node, args.sourceids, args.localstartdate, args.localenddate, 
                args.token, args.secret, args.action)

if __name__ == "__main__":
    main()
