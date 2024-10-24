from solarnetwork_python.client import Client
import json
import sys
import argparse

def solar_query(node, sourceids, startdate, enddate, token, secret):
    """Query the Solar Network API and print the response."""
    param_str = f"aggregationKey=0&localEndDate={enddate}&localStartDate={startdate}&nodeIds={node}&sourceIds={sourceids}"
    client = Client(token, secret)
    
    try:
        response = client.expireconfirm(param_str)
        print(response)
    except Exception as e:
        print(f"Error occurred: {e}")
        sys.exit(1)

def main():
    # Parser object to handle the arguments
    parser = argparse.ArgumentParser(description="API Query Tool")

    # Add arguments 
    parser.add_argument("--node", required=True, type=str, help="Node ID (non-empty string)")
    parser.add_argument("--sourceids", required=True, type=str, help="Source ID in format %2FVI%2FSU%2FB1%2FGEN%2F1")
    parser.add_argument("--localstartdate", required=True, type=str, help="Local start date in format YYYY-MM-DDTHH%3AMM%3ASS")
    parser.add_argument("--localenddate", required=True, type=str, help="Local end local date in format YYYY-MM-DDTHH%3AMM%3ASS")
    parser.add_argument("--token", required=True, help="API token")
    parser.add_argument("--secret", required=True, help="API secret")

    # Parse the arguments the user has provided
    args = parser.parse_args()

    # Print parsed arguments for verification
    print(f"Node: {args.node}, Source IDs: {args.sourceids}, Start Date: {args.localstartdate}, End Date: {args.localenddate}")
    
    # Call the solar query function with parsed arguments
    solar_query(args.node, args.sourceids, args.localstartdate, args.localenddate, args.token, args.secret)

if __name__ == "__main__":
    main()
