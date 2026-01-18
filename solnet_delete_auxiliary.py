from solarnetwork_python.client import Client
import json
import sys
import argparse


def solar_query(node, sourceids, eventdate, token, secret):
    
    formatted_sourceids=sourceids.replace("/", "%2F")
    formatted_eventdate=eventdate.replace(" ","T").replace(":","%3A")

    param_str = f"date={formatted_eventdate}&nodeId={node}&sourceIds={formatted_sourceids}&type=reset"
    client = Client(token, secret)
    
    try:
        response = client.delete_auxiliary(param_str)
        print(response)
    except Exception as e:
        print(f"Error occurred: {e}")
        sys.exit(1)

def main():
    # Parser object to handle the arguments
    parser = argparse.ArgumentParser(description="API Query Tool")

    # Add arguments 
    parser.add_argument("--node", required=True, type=str, help="Node ID (non-empty string)")
    parser.add_argument("--sourceids", required=True, type=str, help="Source ID in format /VI/SU/B1/GEN/1")
    parser.add_argument("--eventdate", required=True, type=str, help="UTC event date in format 'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--token", required=True, help="API token")
    parser.add_argument("--secret", required=True, help="API secret")

    # Parse the arguments the user has provided
    args = parser.parse_args()

    # Print parsed arguments for verification
    # print(f"Node: {args.node}, Source IDs: {args.sourceids}, Start Date: {args.startdate}, End Date: {args.enddate}")

    # Call the solar query function with parsed arguments
    solar_query(args.node, args.sourceids, args.eventdate, args.token, args.secret)

if __name__ == "__main__":
    main()


