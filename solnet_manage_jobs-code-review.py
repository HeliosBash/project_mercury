import argparse
from solarnetwork_python.client import Client
import sys

def list_expire_jobs(token, secret):
    client = Client(token, secret)
    response = client.listdeletejobs()

    print("Job_ID,State,User_ID,Job_Duration,Result_Count,Success", sep=",")
    for element in response:
        jobid = element.get('jobId', 'N/A')
        state = element.get('jobState', 'N/A')
        userid = element.get('userId', 'N/A')
        jobduration = element.get('jobDuration', 'N/A')
        resultcount = element.get('resultCount', 'N/A')
        success = element.get('success', 'N/A')
        print(jobid, state, userid, jobduration, resultcount, success, sep=",")

def list_import_jobs(token, secret):
    client = Client(token, secret)
    response = client.listimportjobs()

    print("Job_ID,Job_State,Done,Cancelled,Name")
    for element in response:
        name = element['configuration']['inputConfiguration']['name'].replace(" ", "_")
        jobid = element.get('jobId', 'N/A')
        jobstate = element.get('jobState', 'N/A')
        done = element.get('done', 'N/A')
        cancelled = element.get('cancelled', 'N/A')
        print(jobid, jobstate, done, cancelled, name, sep=",")

def manage_import_jobs(action, token, secret, jobid):
    client = Client(token, secret)
    action_map = {
        "view": client.viewimportjobs,
        "preview": client.previewimportjobs,
        "delete": client.deleteimportjobs,
        "confirm": client.confirmimportjobs
    }

    if action in action_map:
        try:
            response = action_map[action](jobid)
            print(response)
        except Exception as e:
            print(f"Error occurred: {e}")
    else:
        print("Unknown action")

def main():
    parser = argparse.ArgumentParser(description="SolarNetwork Job Manager")
    parser.add_argument("job", choices=["expire", "import"], help="Job type")
    parser.add_argument("action", choices=["list", "view", "preview", "delete", "confirm"], help="Action to perform")
    parser.add_argument("token", help="API Token")
    parser.add_argument("secret", help="API Secret")
    parser.add_argument("jobid", nargs='?', help="Job ID (required for certain actions)")

    args = parser.parse_args()

    if args.job == "expire" and args.action == "list":
        list_expire_jobs(args.token, args.secret)
    elif args.job == "import" and args.action == "list":
        list_import_jobs(args.token, args.secret)
    elif args.job == "import" and args.jobid:
        manage_import_jobs(args.action, args.token, args.secret, args.jobid)
    else:
        print("Unknown job and action")

if __name__ == "__main__":
    main()