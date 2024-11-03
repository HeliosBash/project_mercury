from solarnetwork_python.client import Client
import sys
import argparse
from datetime import datetime

def solnet_auxiliary(node, sourceids, username, userid, cause, eventDate, finalValue, startValue, token, secret):
    """Import data from a specified node and data sources"""
    
    client = Client(token, secret)
    
    id = sourceids.split("%2F")
    currentDateTime = datetime.now()
    created = currentDateTime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + 'Z'

    final = {
            "wattHours" : finalValue
            }
    
    start = {
            "wattHours" : startValue
            }
    
    location = {
            "node": node,
            "site": f"{id[2]}",
            "device": f"{id[4]}/{id[5]}",
            "system": f"{id[3]}",
            "project": f"{id[1]}"
            }
    
    ecogyEvent = {
        "path": f"{sourceids}",
        "type": "solarnetwork",
        "cause": f"{cause}",
        "final": final,
        "start": start,
        "userId": f"{userid}",
        "created": f"{created}",
        "endDate": f"{eventDate}",
        "localtion": location,
        "priority": 1,
        "userName": f"{username}",
        "datumType": "Reset",
        "startDate": "f{eventDate}",
        "description": f"{cause}"
    }

    pm = {
        "ecogyEvent": ecogyEvent
        }

    auxiliary_data={
        "created": f"{created}",
        "nodeId": f"{node}",
        "sourceId": "{sourceids}",
        "type": "Reset",
        "final": {
                "a": {
                    "wattHours": finalValue
                    }
                },
        "start": {
                "a": {
                    "wattHours": startValue
                }
        },
        "meta": {
                "pm": pm
                }
        }


    try:
        resp = client.store_auxiliary(auxiliary_data)
        print(resp.get)
    except Exception as e:
        print(f"Error occurred while importing data: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import data into Solar Network.")
    parser.add_argument("--node", required=True, type=str, help="Node ID to import data from.")
    parser.add_argument("--sourceids", required=True, type=str, help="Source ID in format %2FVI%2FSU%2FB1%2FGEN%2F1")
    parser.add_argument("--username", required=True, type=str, help="Name of user with Ecosuite access")
    parser.add_argument("--userid", required=True, type=str, help="ID of user with Ecosuite access")
    parser.add_argument("--cause", required=True, type=str, help="Cause or description of the event")
    parser.add_argument("--eventDate", required=True, type=str, help="event data in local timezone")
    parser.add_argument("--finalValue", required=True, type=str, help="Final irradiancehours or watthours value")
    parser.add_argument("--startValue", required=True, type=str, help="Start irradiancehours or watthours value")
    parser.add_argument("--token", required=True, type=str, help="API token for authentication.")
    parser.add_argument("--secret", required=True, type=str, help="API secret for authentication.")
    
    args = parser.parse_args()
    
    solnet_auxiliary(args.node, args.sourceids, args.username, args.userid, args.cause, args.eventDate, args.finalValue, args.startValue, args.token, args.secret)

