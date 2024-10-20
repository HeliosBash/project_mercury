from solarnetwork_python.client import Client
import json
import sys
import argparse

def solar_query(node, sourceids, startdate, enddate, aggregate, maxoutput, token, secret):
    client = Client(token, secret)

    if aggregate == "None":
        param_str = f"endDate={enddate}&max={maxoutput}&nodeId={node}&offset=0&sourceIds={sourceids}&startDate={startdate}"
    else:
        param_str = f"aggregation={aggregate}&endDate={enddate}&max={maxoutput}&nodeId={node}&offset=0&sourceIds={sourceids}&startDate={startdate}"

    response = client.solarquery(param_str)
    if 'PYR' in sourceids:
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
    else:
        if aggregate == "None":
            print('created,localDate,localTime,nodeId,sourceId,watts,current,voltage,frequency,powerFactor,apparentPower,reactivePower,lineVoltage,current_a,current_b,current_c,voltage_a,voltage_b,voltage_c,voltage_ab,voltage_bc,voltage_ca,wattHours,wattHoursReverse,phase')
            for element in response['results']:
                try:
                    print(element.get('created', ''), element.get('localDate', ''), element.get('localTime', ''), element.get('nodeId', ''), 
                          element.get('sourceId', ''), element.get('watts', ''), element.get('current', ''), element.get('voltage', ''), 
                          element.get('frequency', ''), element.get('powerFactor', ''), element.get('apparentPower', ''), element.get('reactivePower', ''), 
                          element.get('lineVoltage', ''), element.get('current_a', ''), element.get('current_b', ''), element.get('current_c', ''), 
                          element.get('voltage_a', ''), element.get('voltage_b', ''), element.get('voltage_c', ''), element.get('voltage_ab', ''), 
                          element.get('voltage_bc', ''), element.get('voltage_ca', ''), element.get('wattHours', ''), element.get('wattHoursReverse', ''), element.get('phase', ''), sep=',')
                except KeyError as e:
                    print(f"Missing key in response: {e}")
        else:
            print('created,localDate,localTime,nodeId,sourceId,watts_min,watts_max,current_min,current_max,voltage_min,voltage_max,frequency_min,frequency_max,powerFactor_min,powerFactor_max,apparentPower_min,apparentPower_max,reactivePower_min,reactivePower_max,lineVoltage_min,lineVoltage_max,current_a_min,current_a_max,current_b_min,current_b_max,current_c_min,current_c_max,voltage_a_min,voltage_a_max,voltage_b_min,voltage_b_max,voltage_c_min,voltage_c_max,voltage_ab_min,voltage_ab_max,voltage_bc_min,voltage_bc_max,voltage_ca_min,voltage_ca_max,watts,current,voltage,frequency,powerFactor,apparentPower,reactivePower,lineVoltage,current_a,current_b,current_c,voltage_a,voltage_b,voltage_c,voltage_ab,voltage_bc,voltage_ca,wattHours,wattHoursReverse,phase')
            for element in response['results']:
                try:
                    print(element.get('created', ''), element.get('localDate', ''), element.get('localTime', ''), element.get('nodeId', ''), element.get('sourceId', ''), 
                        element.get('watts_min', ''), element.get('watts_max', ''), element.get('current_min', ''), element.get('current_max', ''), element.get('voltage_min', ''), 
                        element.get('voltage_max', ''), element.get('frequency_min', ''), element.get('frequency_max', ''), element.get('powerFactor_min', ''), element.get('powerFactor_max', ''), 
                        element.get('apparentPower_min', ''), element.get('apparentPower_max', ''), element.get('reactivePower_min', ''), element.get('reactivePower_max', ''), 
                        element.get('lineVoltage_min', ''), element.get('lineVoltage_max', ''), element.get('current_a_min', ''), element.get('current_a_max', ''), element.get('current_b_min', ''), 
                        element.get('current_b_max', ''), element.get('current_c_min', ''), element.get('current_c_max', ''), element.get('voltage_a_min', ''), element.get('voltage_a_max', ''), 
                        element.get('voltage_b_min', ''), element.get('voltage_b_max', ''), element.get('voltage_c_min', ''), element.get('voltage_c_max', ''), element.get('voltage_ab_min', ''), 
                        element.get('voltage_ab_max', ''), element.get('voltage_bc_min', ''), element.get('voltage_bc_max', ''), element.get('voltage_ca_min', ''), element.get('voltage_ca_max', ''), 
                        element.get('watts', ''), element.get('current', ''), element.get('voltage', ''), element.get('frequency', ''), element.get('powerFactor', ''), element.get('apparentPower', ''), 
                        element.get('reactivePower', ''), element.get('lineVoltage', ''), element.get('current_a', ''), element.get('current_b', ''), element.get('current_c', ''), element.get('voltage_a', ''), 
                        element.get('voltage_b', ''), element.get('voltage_c', ''), element.get('voltage_ab', ''), element.get('voltage_bc', ''), element.get('voltage_ca', ''), element.get('wattHours', ''), 
                        element.get('wattHoursReverse', ''), element.get('phase', ''), sep=",")
                except KeyError as e:
                    print(f"Missing key in response: {e}")

def main():
    parser = argparse.ArgumentParser(description="Solar query!")

    parser.add_argument("--node", required=True, type=str, help="Node ID (non-empty string)")
    parser.add_argument("--sourceids", required=True, type=str, help="Source ID in format %2FVI%2FSU%2FB1%2FGEN%2F1")
    parser.add_argument("--startdate", required=True, type=str, help="Start date in format YYYY-MM-DDTHH%3AMM%3ASS")
    parser.add_argument("--enddate", required=True, type=str, help="End date in format YYYY-MM-DDTHH%3AMM%3ASS")
    parser.add_argument("--aggregate", required=True, help="Aggregation method")
    parser.add_argument("--maxoutput", required=True, help="Maximum output limit")
    parser.add_argument("--token", required=True, help="API token")
    parser.add_argument("--secret", required=True, help="API secret")

    args = parser.parse_args()

    solar_query(args.node, args.sourceids, args.startdate, args.enddate, args.aggregate, args.maxoutput, args.token, args.secret)

if __name__ == "__main__":
    main()

