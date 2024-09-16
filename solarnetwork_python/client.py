from datetime import datetime
import requests
import json

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
            self.token, self.secret, "GET", path, "", headers, "", now
        )

        url = f"https://data.solarnetwork.net/solaruser/api/v1/sec/user/import/jobs/{jobid}/confirm"
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
