from solarnetwork_python.client import Client
import json
import sys

node = sys.argv[1]
sourceids = sys.argv[2]
startdate = sys.argv[3]
enddate = sys.argv[4]
token = sys.argv[5]
secret = sys.argv[6]

def solar_query():

        param_str = "aggregationKey=0&endDate=%s&nodeIds=%s&sourceIds=%s&startDate=%s" % ( enddate, node, sourceids, startdate )
        client = Client(sys.argv[5],sys.argv[6])
        response = client.expirepreview(param_str)
        #print (response)
        print (response['datumCount'])

if __name__ == "__main__":
    solar_query()

