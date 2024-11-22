#!/usr/bin/python3

import requests  
import json
import sys
import re
from datetime import datetime, timedelta
import argparse

lat = sys.argv[1]
long = sys.argv[2]
startdate = sys.argv[3]
enddate = sys.argv[4]
solcasttoken = sys.argv[5]

def solcast_download(lat, long, startdate, enddate, token):

    bearer = f"Bearer {token}"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': bearer
    }

    formatted_startdate=startdate.replace(" ","T").replace(":","%3A")
    formatted_enddate=enddate.replace(" ","T").replace(":","%3A")
    url = f"https://api.solcast.com.au/data/historic/radiation_and_weather?latitude={lat}&longitude={long}&period=PT5M&start={formatted_startdate}&end={formatted_enddate}&format=json&time_zone=utc&output_parameters=ghi"

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error : Received status code {response.status_code}")
        sys.exit(1)

    #json_check

    try:
        data = response.json()
    except json.JSONDecodeError :
        print("Error: Failed to decode JSON response")
        sys.exit(1)

    for i in data.get('estimated_actuals', []):
        period_end=i["period_end"].replace('+00:00','Z')
        dateobj = datetime.strptime(period_end.replace('T', ' ').replace('Z', ''), '%Y-%m-%d %H:%M:%S')
        ghi=i["ghi"]
        period=i["period"]
        duration=int(re.sub("[PTM]","",period))
        period_start=str(dateobj - timedelta(minutes=duration)).replace(' ','T') + 'Z'

        print (f"{period_end},{period_start},{period},{ghi}")

def main():
    # Parser object to handle the arguments
    parser = argparse.ArgumentParser(description="Solcast Download Tool")

    # Add arguments
    parser.add_argument("--latitude", required=True, type=str, help="Site Latitude Coordinate")
    parser.add_argument("--longitude", required=True, type=str, help="Site Longitude Coordinate")
    parser.add_argument("--startdate", required=True, type=str, help="UTC startdate in 'YYYY-MM-DD HH:MM:SS.000Z' format")
    parser.add_argument("--enddate", required=True, type=str, help="UTC enddate in 'YYYY-MM-DD HH:MM:SS.000Z' format")
    parser.add_argument("--token", required=True, help="Solcast token")

    # Parse the arguments the user has provided
    args = parser.parse_args()

    # Call the solar query function with parsed arguments
    solcast_download(args.latitude, args.longitude, args.startdate, args.enddate, args.token)



if __name__ == "__main__":
    main()

