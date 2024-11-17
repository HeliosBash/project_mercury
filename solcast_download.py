#!/usr/bin/python3

import requests  
import json
import sys
import re
from datetime import datetime, timedelta

lat = sys.argv[1]
long = sys.argv[2]
startdate = sys.argv[3]
enddate = sys.argv[4]
solcasttoken = sys.argv[5]

bearer = f"Bearer {solcasttoken}"
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

