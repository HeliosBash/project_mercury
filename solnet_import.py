from solarnetwork_python.client import Client
import sys

node = sys.argv[1]
sourceids = sys.argv[2]
timezone = sys.argv[3]
filepath = sys.argv[4]
token = sys.argv[5]
secret = sys.argv[6]


def solnet_import():
    client = Client(token, secret)

    service_properties = {
        "headerRowCount": "1",
        "dateFormat": "yyyy-MM-dd HH:mm:ss",
        "nodeIdColumn": "1",
        "sourceIdColumn": "2",
        "dateColumnsValue": "3",
        "instantaneousDataColumns": "4",
        "accumulatingDataColumns": "5"
    }

    innername = node + '_' + sourceids + '_' + 'Input'
    inner = {
        "name": innername,
        "timeZoneId": timezone,
        "serviceIdentifier": "net.solarnetwork.central.datum.imp.standard.SimpleCsvDatumImportInputFormatService",
        "serviceProperties": service_properties
    }
    outername = node + '_' + sourceids + '_' + 'Import'
    
    outer = {
        "name": outername,
        "stage": True,
        "inputConfiguration": inner
    }

    openfile = open(filepath, 'r')
    csv_data = (openfile.read())
    openfile.close()
    
    resp = client.import_data(outer, csv_data)
    print (resp['jobId'])

if __name__ == "__main__":
    solnet_import()
