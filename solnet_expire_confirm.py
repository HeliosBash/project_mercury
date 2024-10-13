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
    parser.add_argument("--sourceids", required=True, type=str, help="Comma-separated list of source IDs")
    parser.add_argument("--startdate", required=True, type=str, help="Start date in format YYYY-MM-DDTHH%3AMM%3ASS")
    parser.add_argument("--enddate", required=True, type=str, help="End date in format YYYY-MM-DDTHH%3AMM%3ASS")
    parser.add_argument("--token", required=True, help="API token")
    parser.add_argument("--secret", required=True, help="API secret")

    # Parse the arguments the user has provided
    args = parser.parse_args()

    # Print parsed arguments for verification
    print(f"Node: {args.node}, Source IDs: {args.sourceids}, Start Date: {args.startdate}, End Date: {args.enddate}")
    
    # Call the solar query function with parsed arguments
    solar_query(args.node, args.sourceids, args.startdate, args.enddate, args.token, args.secret)

if __name__ == "__main__":
    main()
