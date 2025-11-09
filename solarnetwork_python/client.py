from datetime import datetime
import requests
import json
import base64
from solarnetwork_python.authentication import generate_auth_header, get_x_sn_date


class Client:
    def __init__(self, token: str, secret: str) -> None:
        self.token = token
        self.secret = secret

    def solarquery(self, paramstr: str) -> str:

        now = datetime.utcnow()
        date = get_x_sn_date(now)
        path = "/solarquery/api/v1/sec/datum/list"
        params = paramstr
        headers = {"host": "data.solarnetwork.net", "x-sn-date": date}

        auth = generate_auth_header(
            self.token, self.secret, "GET", path, params, headers, "", now
        )

        url = 'https://data.solarnetwork.net/solarquery/api/v1/sec/datum/list?' + params
        resp = requests.get(
            url,
            headers={
                "host": "data.solarnetwork.net",
                "x-sn-date": date,
                "Authorization": auth,
            },
        )

        v = resp.json()
        if v["success"] != True:
            raise Exception("Unsuccessful API call")

        return v["data"]
    
    def listdeletejobs(self) -> str:
        now = datetime.utcnow()
        date = get_x_sn_date(now)
        path = "/solaruser/api/v1/sec/user/expire/datum-delete/jobs"

        headers = {"host": "data.solarnetwork.net", "x-sn-date": date}

        auth = generate_auth_header(
            self.token, self.secret, "GET", path, "", headers, "", now
        )

        resp = requests.get(
            url="https://data.solarnetwork.net/solaruser/api/v1/sec/user/expire/datum-delete/jobs",

            headers={
                "host": "data.solarnetwork.net",
                "x-sn-date": date,
                "Authorization": auth,
            },
        )

        v = resp.json()
        if v["success"] != True:
            raise Exception("Unsuccessful API call")

        return v["data"]


    def expirepreview(self, paramstr: str) -> str:

        now = datetime.utcnow()
        date = get_x_sn_date(now)
        path = "/solaruser/api/v1/sec/user/expire/datum-delete"
        params = paramstr
        body = ""
        headers = { "content-type" : "application/x-www-form-urlencoded; charset=UTF-8" , "host": "data.solarnetwork.net", "x-sn-date": date}
        auth = generate_auth_header(
            self.token, self.secret, "POST", path, params, headers, body, now
        )
        
        url = 'https://data.solarnetwork.net/solaruser/api/v1/sec/user/expire/datum-delete?' + params
        
        resp = requests.post(
            url,
            headers={
                "content-type" : "application/x-www-form-urlencoded; charset=UTF-8",
                "host": "data.solarnetwork.net",
                "x-sn-date": date,
                "Authorization": auth,
            },
        )

        v = resp.json()
        if v["success"] != True:
            raise Exception("Unsuccessful API call")
    
        return v["data"]

    def expireconfirm(self, paramstr: str) -> str:

        now = datetime.utcnow()
        date = get_x_sn_date(now)
        path = "/solaruser/api/v1/sec/user/expire/datum-delete/confirm"
        params = paramstr
        body = ""
        headers = { "content-type" : "application/x-www-form-urlencoded; charset=UTF-8" , "host": "data.solarnetwork.net", "x-sn-date": date}
        auth = generate_auth_header(
            self.token, self.secret, "POST", path, params, headers, body, now
        )

        url = 'https://data.solarnetwork.net/solaruser/api/v1/sec/user/expire/datum-delete/confirm?' + params

        resp = requests.post(
            url,
            headers={
                "content-type" : "application/x-www-form-urlencoded; charset=UTF-8",
                "host": "data.solarnetwork.net",
                "x-sn-date": date,
                "Authorization": auth,
            },
        )

        v = resp.json()
        if v["success"] != True:
            raise Exception("Unsuccessful API call")

        return v["data"]

    def listimportjobs(self) -> str:
        now = datetime.utcnow()
        date = get_x_sn_date(now)
        path = "/solaruser/api/v1/sec/user/import/jobs"

        headers = {"host": "data.solarnetwork.net", "x-sn-date": date}

        auth = generate_auth_header(
            self.token, self.secret, "GET", path, "", headers, "", now
        )

        resp = requests.get(
            url="https://data.solarnetwork.net/solaruser/api/v1/sec/user/import/jobs",

            headers={
                "host": "data.solarnetwork.net",
                "x-sn-date": date,
                "Authorization": auth,
            },
        )

        v = resp.json()
        if v["success"] != True:
            raise Exception("Unsuccessful API call")

        return v["data"]

    def viewimportjobs(self, jobid) -> str:
        now = datetime.utcnow()
        date = get_x_sn_date(now)
        path = f'/solaruser/api/v1/sec/user/import/jobs/{jobid}'

        headers = { "host": "data.solarnetwork.net", "x-sn-date": date}


        auth = generate_auth_header(
            self.token, self.secret, "GET", path, "", headers, "", now
        )

        url = f"https://data.solarnetwork.net/solaruser/api/v1/sec/user/import/jobs/{jobid}"
        resp = requests.get(
            url,
            headers={
                "host": "data.solarnetwork.net",
                "x-sn-date": date,
                "Authorization": auth,
            },
        )

        v = resp.json()
        if v["success"] != True:
            raise Exception("Unsuccessful API call")

        return v["data"]

    def deleteimportjobs(self, jobid) -> str:
        now = datetime.utcnow()
        date = get_x_sn_date(now)
        path = f'/solaruser/api/v1/sec/user/import/jobs/{jobid}'

        headers = { "host": "data.solarnetwork.net", "x-sn-date": date}


        auth = generate_auth_header(
            self.token, self.secret, "DELETE", path, "", headers, "", now
        )

        url = f"https://data.solarnetwork.net/solaruser/api/v1/sec/user/import/jobs/{jobid}"
        resp = requests.delete(
            url,
            headers={
                "host": "data.solarnetwork.net",
                "x-sn-date": date,
                "Authorization": auth,
            },
        )

        v = resp.json()
        if v["success"] != True:
            raise Exception("Unsuccessful API call")

        return v["data"]


    def previewimportjobs(self, jobid) -> str:
        now = datetime.utcnow()
        date = get_x_sn_date(now)
        path = f'/solaruser/api/v1/sec/user/import/jobs/{jobid}/preview'

        headers = { "host": "data.solarnetwork.net", "x-sn-date": date}


        auth = generate_auth_header(
            self.token, self.secret, "GET", path, "", headers, "", now
        )

        url = f"https://data.solarnetwork.net/solaruser/api/v1/sec/user/import/jobs/{jobid}/preview"
        resp = requests.get(
            url,
            headers={
                "host": "data.solarnetwork.net",
                "x-sn-date": date,
                "Authorization": auth,
            },
        )

        v = resp.json()
        return v

    def confirmimportjobs(self, jobid) -> str:
        now = datetime.utcnow()
        date = get_x_sn_date(now)
        path = f'/solaruser/api/v1/sec/user/import/jobs/{jobid}/confirm'

        headers = { "host": "data.solarnetwork.net", "x-sn-date": date}


        auth = generate_auth_header(
            self.token, self.secret, "POST", path, "", headers, "", now
        )

        url = f"https://data.solarnetwork.net/solaruser/api/v1/sec/user/import/jobs/{jobid}/confirm"
        resp = requests.post(
            url,
            headers={
                "host": "data.solarnetwork.net",
                "x-sn-date": date,
                "Authorization": auth,
            },
        )

        v = resp.json()
        return v

    def import_data(self, description, importdata ):
        # The time has to be in UTC
        now = datetime.utcnow()
        date = get_x_sn_date(now)
        path = "/solaruser/api/v1/sec/user/import/jobs"

        # These should be present for all API calls
        headers = {"content-type": "multipart/form-data; charset=utf-8; boundary=__X_PAW_BOUNDARY__", "host": "data.solarnetwork.net", "x-sn-date": date}

        description_data = json.dumps(description)

        body = (
            f'--__X_PAW_BOUNDARY__\r\n'
            f'Content-Disposition: form-data; name="config"\r\n'
            f'Content-Type: application/json\r\n'
            f'\r\n'
            f'{description_data}\r\n'
            f'--__X_PAW_BOUNDARY__\r\n'
            f'Content-Disposition: form-data; name="data"; filename="data.csv"\r\n'
            f'Content-Type: text/csv\r\n'
            f'\r\n'
            f'{importdata}\r\n'
            f'--__X_PAW_BOUNDARY__--\r\n'
        )

        auth = generate_auth_header(
            self.token, self.secret, "POST", path, "", headers, body, now
        )

        resp = requests.post(
            url="https://data.solarnetwork.net/solaruser/api/v1/sec/user/import/jobs",
            data=body,

            # Make sure to actually include the headers given by the previous
            # headers argument
            headers={
                "Content-Type": "multipart/form-data; charset=utf-8; boundary=__X_PAW_BOUNDARY__",
                "host": "data.solarnetwork.net",
                "x-sn-date": date,
                "Authorization": auth,
            },
        )
        v = resp.json()
        if v["success"] != True:
            raise Exception("Unsuccessful API call")
        
        return v["data"]

    def import_compressed_data(self, description, importdata ):
        # The time has to be in UTC
        now = datetime.utcnow()
        date = get_x_sn_date(now)
        path = "/solaruser/api/v1/sec/user/import/jobs"

        # These should be present for all API calls
        headers = {"content-type": "multipart/form-data; charset=utf-8; boundary=__X_PAW_BOUNDARY__", "host": "data.solarnetwork.net", "x-sn-date": date}

        description_data = json.dumps(description)
        
        importdata_base64 = base64.b64encode(importdata).decode('latin-1')

        body = (
            f'--__X_PAW_BOUNDARY__\r\n'
            f'Content-Disposition: form-data; name="config"\r\n'
            f'Content-Type: application/json\r\n'
            f'\r\n'
            f'{description_data}\r\n'
            f'--__X_PAW_BOUNDARY__\r\n'
            f'Content-Disposition: form-data; name="data"; filename="data.csv.xz"\r\n'
            f'Content-Type: application/octet-stream\r\n'
            f'Content-Transfer-Encoding: base64\r\n'
            f'\r\n'
            f'{importdata_base64}\r\n'
            f'--__X_PAW_BOUNDARY__--\r\n'
        )

        auth = generate_auth_header(
            self.token, self.secret, "POST", path, "", headers, body, now
        )

        resp = requests.post(
            url="https://data.solarnetwork.net/solaruser/api/v1/sec/user/import/jobs",
            data=body,

            # Make sure to actually include the headers given by the previous
            # headers argument
            headers={
                "Content-Type": "multipart/form-data; charset=utf-8; boundary=__X_PAW_BOUNDARY__",
                "host": "data.solarnetwork.net",
                "x-sn-date": date,
                "Authorization": auth,
            },
        )
        v = resp.json()
        if v["success"] != True:
            raise Exception("Unsuccessful API call")

        return v["data"]

    def store_datum_auxiliary(self, auxiliary_data: dict) -> dict:
        """
            Store or update a datum auxiliary record.
    
         Args:
            auxiliary_data: Dictionary containing datum auxiliary record with keys:
            - created: Event date (string in 'yyyy-MM-dd HH:mm:ss.SSS' format or millisecond epoch)
            - nodeId: Node ID (int)
            - sourceId: Source ID (string)
            - type: Datum auxiliary type (string)
            - notes: Optional comment (string)
            - final: General datum sample representing final data (dict)
            - start: General datum sample representing starting data (dict)
            - meta: Optional metadata object (dict)
    
            Returns:
            dict: Response data from the API
        """
        # The time has to be in UTC
        now = datetime.utcnow()
        date = get_x_sn_date(now)
        path = "/solaruser/api/v1/sec/datum/auxiliary"
    
        # Headers for this API call
        headers = {
            "content-type": "application/json; charset=utf-8",
            "host": "data.solarnetwork.net",
            "x-sn-date": date
        }

        # Convert auxiliary data to JSON

        body = json.dumps(auxiliary_data)

        # Generate authorization header

        auth = generate_auth_header(
                self.token, self.secret, "POST", path, "", headers, body, now
                )
        # Make the POST request
        resp = requests.post(
                url="https://data.solarnetwork.net/solaruser/api/v1/sec/datum/auxiliary",
                data=body,
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                    "host": "data.solarnetwork.net",
                    "x-sn-date": date,
                    "Authorization": auth,
                    },
                )
        v = resp.json()
        if v.get("success") != True:
            raise Exception("Unsuccessful API call")
        return v.get("data")

    def delete_auxiliary(self, paramstr: str) -> str:

        now = datetime.utcnow()
        date = get_x_sn_date(now)
        path = "/solaruser/api/v1/sec/datum/auxiliary/"
        params = paramstr
        body = ""
        headers = { "content-type" : "application/json; charset=UTF-8" , "host": "data.solarnetwork.net", "x-sn-date": date}
        auth = generate_auth_header(
            self.token, self.secret, "DELETE", path, params, headers, body, now
        )

        url = 'https://data.solarnetwork.net/solaruser/api/v1/sec/datum/auxiliary?' + params

        resp = requests.delete(
            url,
            headers={
                "content-type" : "application/x-www-form-urlencoded; charset=UTF-8",
                "host": "data.solarnetwork.net",
                "x-sn-date": date,
                "Authorization": auth,
            },
        )

        v = resp.json()
        if v["success"] != True:
            raise Exception("Unsuccessful API call")

        return v["data"]

    def get_auxiliary(self, paramstr: str) -> str:

        now = datetime.utcnow()
        date = get_x_sn_date(now)
        path = "/solaruser/api/v1/sec/datum/auxiliary"
        params = paramstr
        body = ""
        headers = { "host": "data.solarnetwork.net", "x-sn-date": date}
        auth = generate_auth_header(
            self.token, self.secret, "GET", path, params, headers, "", now
        )

        url = 'https://data.solarnetwork.net/solaruser/api/v1/sec/datum/auxiliary?' + params

        resp = requests.get(
            url,
            headers={
                "host": "data.solarnetwork.net",
                "x-sn-date": date,
                "Authorization": auth,
            },
        )

        v = resp.json()
        return v
        #if v["success"] != True:
        #    raise Exception("Unsuccessful API call")
#
        #return v["data"]

