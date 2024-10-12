from solarnetwork_python.client import Client
import json
import sys
import argparse
from datetime import datetime

def validate_date(date_str: str):
    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_str}. Expected format: YYYY-MM-DDTHH:MM:SS")

def solar_query(node, sourceids, startdate, enddate, aggregate, maxoutput, token, secret):
    client = Client(token, secret)
    
    if aggregate == "None":
        param_str = f"endDate={enddate}&max={maxoutput}&nodeId={node}&offset=0&sourceIds={sourceids}&startDate={startdate}"
    else:
        param_str = f"aggregation={aggregate}&endDate={enddate}&max={maxoutput}&nodeId={node}&offset=0&sourceIds={sourceids}&startDate={startdate}"

    response = client.solarquery(param_str)
    
    if aggregate == "None":
        print('Created,localDate,localTime,nodeId,sourceId,irradiance,irradianceHours')
        for element in response['results']:
            try:
                print(element.get('created', ''), element.get('localDate', ''), element.get('localTime', ''),
                      element.get('nodeId', ''), element.get('sourceId', ''), element.get('irradiance', ''),
                      element.get('irradianceHours', ''), sep=',')
            except KeyError as e:
                print(f"Missing key in response: {e}")
    else:
        print('Created,localDate,localTime,nodeId,sourceId,irradiance_min,irradiance_max,irradiance,irradianceHours')
        for element in response['results']:
            try:
                print(element.get('created', ''), element.get('localDate', ''), element.get('localTime', ''),
                      element.get('nodeId', ''), element.get('sourceId', ''),
                      element.get('irradiance_min', ''), element.get('irradiance_max', ''),
                      element.get('irradiance', ''), element.get('irradianceHours', ''), sep=',')
            except KeyError as e:
                print(f"Missing key in response: {e}")

def main():
    parser = argparse.ArgumentParser(description="Solar query!")
    
    parser.add_argument("--node", required=True, type=str, help="Node ID (non-empty string)")
    parser.add_argument("--sourceids", required=True, type=str, help="Comma-separated list of source IDs")
    parser.add_argument("--startdate", required=True, type=validate_date, help="Start date in format YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--enddate", required=True, type=validate_date, help="End date in format YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--aggregate", required=True, help="Aggregation method")
    parser.add_argument("--maxoutput", required=True, help="Maximum output limit")
    parser.add_argument("--token", required=True, help="API token")
    parser.add_argument("--secret", required=True, help="API secret")
    
    args = parser.parse_args()
    
    solar_query(args.node, args.sourceids, args.startdate.isoformat(), args.enddate.isoformat(),
                args.aggregate, args.maxoutput, args.token, args.secret)

if __name__ == "__main__":
    main()
