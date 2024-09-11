#!/usr/bin/python3

import requests
import json
import sys
import re
from datetime import datetime, timedelta

lat = sys.argv[1]
long = sys.argv[2]
start = sys.argv[3]
end = sys.argv[4]
solcasttoken = sys.argv[5]

bearer = "Bearer %s" % solcasttoken

headers = {
         'Content-Type': 'application/json',
         'Authorization': bearer,
     }

url = "https://api.solcast.com.au/data/historic/radiation_and_weather?latitude=%s&longitude=%s&period=PT5M&start=%s&end=%s&format=json&time_zone=utc&output_parameters=ghi" % (lat,long,start,end)

response = requests.get(url, headers=headers)

data = response.json()

for i in data['estimated_actuals']:
   period_end=i["period_end"]
   formatted_period_end=period_end.replace('+00:00','Z')
   datestr=formatted_period_end.replace('T',' ').replace('Z','')
   dateobj=datetime.strptime(datestr, '%Y-%m-%d %H:%M:%S')
    
   ghi=i["ghi"]
   period=i["period"]
   duration=int(re.sub("[PTM]","",period))
   period_start=str(dateobj - timedelta(minutes=duration))
   formatted_period_start=period_start.replace(' ','T') + 'Z'

   print (formatted_period_end,formatted_period_start,period,ghi,sep=",")
