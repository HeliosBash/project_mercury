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
        print ('Created,nodeId,sourceId,localDate,localTime,watts,current,voltage,current_a,current_b,current_c,frequency,volate_a,voltage_b,voltage_c,voltage_ab,voltage_bc,voltage_ca,lineVoltage,powerFactor,apparentPower,reactivePower,wattHours,wattHoursReverse,phase')
        for element in response['results']:
           try:
               print (element['created'],element['nodeId'],element['sourceId'],element['localDate'],element['localTime'],element['watts'],element['current'],element['voltage'],element['current_a'],element['current_b'],element['current_c'],element['frequency'],element['voltage_a'],element['voltage_b'],element['voltage_c'],element['voltage_ab'],element['voltage_bc'],element['voltage_ca'],element['lineVoltage'],element['powerFactor'],element['apparentPower'],element['reactivePower'],element['wattHours'],element['wattHoursReverse'],element['phase'],sep=',')
           except:
               try: 
                   print (element['created'],element['nodeId'],element['sourceId'],element['localDate'],element['localTime'],element['watts'],'','','','','','','','','','','','','','','','',element['wattHours'],'','',sep=',')
               except:
                   print (element['created'],element['nodeId'],element['sourceId'],element['localDate'],element['localTime'],'','','','','','','','','','','','','','','','','',element['wattHours'],'','',sep=',')
     else :
        param_str = "aggregation=%s&endDate=%s&max=%s&nodeId=%s&offset=0&sourceIds=%s&startDate=%s" % (aggregate, enddate, maxoutput, node, sourceids, startdate)
        client = Client(token,secret)
        response = client.solarquery(param_str)
        print ('Created,nodeId,sourceId,localDate,localTime,watts,current,voltage,current_a,current_b,current_c,frequency,volate_a,voltage_b,voltage_c,voltage_ab,voltage_bc,voltage_ca,lineVoltage,powerFactor,apparentPower,reactivePower,wattHours,wattHoursReverse,phase')
        for element in response['results']:
           try:
               print (element['created'],element['nodeId'],element['sourceId'],element['localDate'],element['localTime'],element['watts'],element['current'],element['voltage'],element['current_a'],element['current_b'],element['current_c'],element['frequency'],element['voltage_a'],element['voltage_b'],element['voltage_c'],element['voltage_ab'],element['voltage_bc'],element['voltage_ca'],element['lineVoltage'],element['powerFactor'],element['apparentPower'],element['reactivePower'],element['wattHours'],element['wattHoursReverse'],element['phase'],sep=',')
           except:
               try:
                   print (element['created'],element['nodeId'],element['sourceId'],element['localDate'],element['localTime'],element['watts'],'','','','','','','','','','','','','','','','',element['wattHours'],'','',sep=',')
               except:
                   print (element['created'],element['nodeId'],element['sourceId'],element['localDate'],element['localTime'],'','','','','','','','','','','','','','','','','',element['wattHours'],'','',sep=',')


if __name__ == "__main__":
    solar_query()
