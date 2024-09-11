from solarnetwork_python.client import Client
import json
import sys

node = sys.argv[1]
sourceids = sys.argv[2]
startdate = sys.argv[3]
enddate = sys.argv[4]
aggregate = sys.argv[5]
maxoutput = sys.argv[6]
token = sys.argv[7]
secret = sys.argv[8]

def solar_query():
    if aggregate == "None" :
        param_str = "endDate=%s&max=%s&nodeId=%s&offset=0&sourceIds=%s&startDate=%s" % (enddate, maxoutput, node, sourceids, startdate)
        client = Client(token,secret)
        response = client.solarquery(param_str)
        
        print ('Created,localDate,localTime,nodeId,sourceId,irradiance,irradianceHours')
        
        for element in response['results']:
           try:
               print (element['created'],element['localDate'],element['localTime'],element['nodeId'],element['sourceId'],element['irradiance'],element['irradianceHours'],sep=',')
           except:
               pass
    else : 
        param_str = "aggregation=%s&endDate=%s&max=%s&nodeId=%s&offset=0&sourceIds=%s&startDate=%s" % (aggregate, enddate, maxoutput, node, sourceids, startdate)
        client = Client(token, secret)
        response = client.solarquery(param_str)
        print ('Created,localDate,localTime,nodeId,sourceId,irradiance_min,irradiance_max,irradiance,irradianceHours')
        for element in response['results']:
           try:
               print (element['created'],element['localDate'],element['localTime'],element['nodeId'],element['sourceId'],element['irradiance_min'],element['irradiance_max'],element['irradiance'],element['irradianceHours'],sep=',')
           except:
               pass

if __name__ == "__main__":
    solar_query()
