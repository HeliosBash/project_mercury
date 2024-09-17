from solarnetwork_python.client import Client
import json
import sys

def list_expire_jobs(tokenstr,secretstr):
        client = Client(tokenstr,secretstr)
        response = client.listdeletejobs()
        
        print ("Job_ID","State","User_ID","Job_Duration","Result_Count","Success",sep=",")
        
        for element in response:
            jobid = element['jobId']
            state = element['jobState'] 
            userid = element['userId'] 
            jobduration = element['jobDuration'] 
            resultcount = element['resultCount'] 
            success = element['success'] 
            print (jobid,state,userid,jobduration,resultcount,success,sep=",")

def list_import_jobs(tokenstr,secretstr):
        
        client = Client(tokenstr,secretstr)
        response = client.listimportjobs()
        
        print ("jobid,jobstate,done,cancelled,name")
        
        for element in response:
            name = element['configuration']['inputConfiguration']['name']
            formatname = name.replace(" ", "_")
            jobid = element['jobId']
            jobstate = element['jobState']
            done = element['done']
            cancelled = element['cancelled']
            print (jobid,jobstate,done,cancelled,formatname,sep=",")

def view_import_jobs(tokenstr,secretstr,jobimportid):

        client = Client(token, secret)
        response = client.viewimportjobs(jobimportid)
        print (response)

def preview_import_jobs(tokenstr,secretstr,jobimportid):

        client = Client(token, secret)
        response = client.previewimportjobs(jobimportid)
        print (response)

def delete_import_jobs(tokenstr,secretstr,jobimportid):

        client = Client(token, secret)
        response = client.deleteimportjobs(jobimportid)
        print (response)

def confirm_import_jobs(tokenstr,secretstr,jobimportid):

        client = Client(token, secret)
        response = client.confirmimportjobs(jobimportid)
        print (response)



if __name__ == "__main__":
    
    if ((len(sys.argv)< 5) or (len(sys.argv)> 6)):

        print ("Incorrect number of parameters")
        print ("python3 solnet_manage_jobs.py [expire|import] list token secret")
        print ("python3 solnet_manage_jobs.py [import] [view|preview|delete|confirm] token secret jobid")

    else:
        if ((len(sys.argv) == 5) or (len(sys.argv) == 6)) :
            job = sys.argv[1]
            action = sys.argv[2]
            token = sys.argv[3]
            secret = sys.argv[4]
            
            if len(sys.argv) == 6:
                jobid = sys.argv[5]
                if (job == "import") and (action == "view"):
                    view_import_jobs(token,secret,jobid)
                elif (job == "import") and (action == "preview"):
                    preview_import_jobs(token,secret,jobid)
                elif (job == "import") and (action == "delete"):
                    delete_import_jobs(token,secret,jobid)
                elif (job == "import") and (action == "confirm"):
                    confirm_import_jobs(token,secret,jobid)
                else:
                    print ("Unknown job and action")
            else: 
                if (job == "expire") and (action == "list"):
                    list_expire_jobs(token,secret)  
                elif (job == "import") and (action == "list"): 
                    list_import_jobs(token,secret)
                else:
                    print ("Unknown job and action")

