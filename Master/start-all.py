import http.server
import socketserver
import sys
import subprocess
from masterhandler import handler

class Master:
    def __init__(self):
        self.workers=[]
        # initializing number of workers in info.txt file then spliting it into two different variables. 
        with open('./info.txt') as config:
            numofworker = int(config.readline().strip().split(",")[1])
            base    = int(config.readline().strip().split(",")[1])

        for i in range(numofworker):
            self.workers.append(subprocess.Popen(["python", "../Worker/worker_v1.py", f"{base + i}"], shell=True)) # making a subprocess and initiallizing pipe for connection

        with socketserver.TCPServer(("", 8031), handler) as httpd:
            print("MASTER: serving at port", 8031,"\n")
            httpd.serve_forever()



masterobj = Master()
