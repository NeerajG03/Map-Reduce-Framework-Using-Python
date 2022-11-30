from filesplit.merge import Merge
from filesplit.split import Split
import http.server
import threading
from threading import Thread
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
import urllib
import http
import socketserver
import sys
import os
import requests
import json
import time

SERVERRUN = False
count = 0
localcount = 0

# removing the contents of the directory
def purge_exit(directory):
    for f in os.listdir(directory):
                os.remove(f'{directory}/{f}')

#This class is used to handle the HTTP requests that arrive at the server. By itself, it cannot respond to any actual HTTP requests
class handler(http.server.BaseHTTPRequestHandler):
    # sending the message GOT IT to the server when the function is called
    def do_GET(self):       
        self.send_response(200)
        self.send_header('Content-type','text/html')    
        self.end_headers()
        message = "GOT IT"
        self.wfile.write(bytes(message, "utf8"))
        

    def do_POST(self):
        global SERVERRUN,localcount,count

        query = urllib.parse.urlparse(self.path).query
        q = dict(q.split("=") for q in query.split("&"))
        
        if q['request-type'] == "read":
            print('file chunk post req received')
            print(q['filename'])


            with open(f"./temp_partitions/{q['filename']}", 'wb') as f:
                f.write(self.rfile.read(int(self.headers['Content-Length'])))

            self.send_response(200, message="File Chunk Received")
            self.send_header('Content-type','text/html')
            self.end_headers()
            localcount+=1 
            if localcount == count:
                count = localcount = 0
                SERVERRUN = False

sys.tracebacklimit = 0
MasterPort = 8031

instructions = {'-r' : 'read','-w' : 'write','-mr' : 'mapreduce'}
flag = sys.argv[1]

# function for handling argument error. 
def errors_client():
    if flag not in instructions:
        raise Exception(f"\n{sys.argv[1]} is not supported\nThe supported command are: \n-r  : Read\n-w  : Write\n-mr : MapReduce\n")

class Task:
    def __init__(self, role):
        self.currole = role
        self.port = 8030

    def write(self):
        # error handling
        if len(sys.argv)<3:
            raise Exception("command not in format: \"python client.py [-w] [directory of file to be written]\"\n")
        if not os.path.exists(sys.argv[2]):
            raise Exception(f"\"{sys.argv[2]}\" is not a valid directory\n")  

        # request processing
        with socketserver.TCPServer(("", self.port), handler) as httpd:
            SERVERRUN = True
            print("serving write server at port", self.port)
            send_req = {'request-type' : f'{instructions[self.currole]}'}
            res = requests.get("http://localhost:8031/", params = send_req)
            print("Request sent")
            workerlist = json.loads(res.content.decode())
            numofworker = len(workerlist)
            print(workerlist,numofworker)

        # file open and process
            with open(sys.argv[2]) as writefile:
                numofline = len(writefile.readlines())
            split = Split(sys.argv[2],'./temp_partitions')
            filename = sys.argv[2].split('/')[-1].split('.')[0]
            extension = sys.argv[2].split('/')[-1].split('.')[1]
            split.manfilename=f'{filename}_manifest'

        # splitting of files
            if numofline < numofworker:
                split.bylinecount(1)
                with open(f'./temp_partitions/{filename}_manifest','a+') as manifest:
                    for i in range(numofline+1,numofworker+1):
                        emptyfile = f"{filename}_{i}.{extension}"
                        with open(f"./temp_partitions/{emptyfile}","w") as create:
                            pass
                        manifest.write(f"{emptyfile},0,False\n")
            else:  
                n = numofline/numofworker  
                if int(n) == n:
                    split.bylinecount(n)
                else:
                    split.bylinecount(n+1) 
            # res2 = []
            ports = []
            count=0
            i=1
            list_of_url = [(i+x,each) for x,each in enumerate(workerlist)]
            print(list_of_url)
            def post_req(value):
                print(value)
                with open(f'./temp_partitions/{filename}_{value[0]}.{extension}' ,'rb') as writefile:
                    return requests.post(f"http://localhost:{value[1]}",data = writefile.read(),params = {'request-type':'write','filename':f'{filename}_{value[0]}.{extension}'})
            
            with ThreadPoolExecutor(max_workers=10) as pool:
                response_list = list(pool.map(post_req,list_of_url))
            
            for i,each in enumerate(response_list):
                if each.status_code == 200:
                    count+=1
                ports.append(workerlist[i])

            if count != numofworker:
                raise Exception("Error while writing to worker nodes")   
                 
            param = {'request-type' : 'metadata_save'}  
            with open(f'./temp_partitions/{filename}_manifest') as tobew:  
                res3 = requests.post(f"http://localhost:8031",
                    data = {f'{filename}_manifest':tobew,f'{filename}.{extension}':f'{ports}'},
                    params = param) 
            print(res3.content.decode())  
            httpd.server_close()
        purge_exit('./temp_partitions')           

    def read(self):
        global SERVERRUN,count
        SERVERRUN = True
        print("in read") 
        if len(sys.argv)<4:
            raise Exception("command not in format: \"python client.py [-r] [filename] [output directory]\"\n")
        filename,extension = sys.argv[2].split('.')

        with socketserver.TCPServer(("", self.port), handler) as httpd:
            print("serving read server at ", self.port)
            send_req = {
                'request-type' : f'{instructions[self.currole]}',
                'filename':filename,
                'extension':extension
                }
            res = requests.get("http://localhost:8031/", params = (send_req))
            print("Request sent")
            if res.status_code == 500:
                raise Exception("The file is not present in the DFS")
            workerlist = json.loads(res.content.decode())
            count = len(workerlist["ports"])
            print("Global count",count)
            with open(f"./temp_partitions/{filename}_manifest","w") as manifest:
                manifest.write(workerlist[f"{filename}_manifest"])
            print("created manifest file")
            while SERVERRUN:
                httpd.handle_request()
            print("done")
            merge = Merge(inputdir="./temp_partitions", outputdir = sys.argv[3], outputfilename=sys.argv[2])
            merge.manfilename=f"{sys.argv[2].split('.')[0]}_manifest"
            merge.merge()
            purge_exit('./temp_partitions')
            print(f"Successfully read the data onto {sys.argv[3]}")

    def mapreduce(self):
        print("in mapreduce")
        # exception to handle argument error
        if len(sys.argv)<5:
            raise Exception("Command not in format: \"python client.py [-mr] [directory of mapper] [directory of reducer] [filename of file in dfs]\"\n")
        # exception to hamdle path error for mapper
        if not os.path.exists(sys.argv[2]):
            raise Exception(f"\"{sys.argv[2]}\" for mapper is not a valid directory\n")
        # exception to hamdle path error for reduced
        if not os.path.exists(sys.argv[3]):
            raise Exception(f"\"{sys.argv[3]}\" for reducer is not a valid directory\n")
        send_req = {
            'request-type' : f'{instructions[self.currole]}',
            'filename':sys.argv[4]
            }
        data = {}
        with socketserver.TCPServer(("", self.port), handler) as httpd:
            print("serving map-reduce server at ", self.port)
            with open(sys.argv[2],"r") as mapper:
                data["mapper"] = mapper.read()
            with open(sys.argv[3],"r") as reducer:
                data["reducer"] = reducer.read()  

            res = requests.get("http://localhost:8031/", params = (send_req), data = data)
            print(f"Map-Reduce process has finished and can be read using the filename : {res.content.decode()}")
            print("Request sent")
            httpd.server_close()

        
os.umask(0) #newly created files or directories created will have no privileges initially revoked
errors_client()
if not os.path.exists("./temp_partitions"):
    os.mkdir("./temp_partitions",mode=0o777)
task = Task(flag)
runny = getattr(task,instructions[flag])
runny()