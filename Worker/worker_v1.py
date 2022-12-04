import multiprocessing
import sys
import http.server
import socketserver
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from subprocess import Popen, PIPE, STDOUT
import time
import os
import json
from shuffler import myHash 
from threading import Thread
import re
import requests
import http
from urllib.parse import urlparse
import urllib
import subprocess

os.umask(0)

def purge_exit(directory):
    for f in os.listdir(directory):
                os.remove(f'{directory}/{f}')

def fileregex(path,expression):
    regex = re.compile(expression)
    for root, dirs, files in os.walk(path):
        for f in files:
            if regex.match(f):
                return f


class handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type','text/html')
        self.end_headers()
        
        query = urlparse(self.path).query
        q = dict(q.split("=") for q in query.split("&"))


        if q['request-type']=='send-file':
            filename = fileregex(f"../Worker/FileStore/DIR_{worker_obj.port}",f"^{q['filename']}.*{q['extension']}$")
            print(q['filename'])
            if filename == None:
                filename = fileregex(f"../Worker/FileStore/DIR_{worker_obj.port}",f"^{q['filename']}_*")
            # print(filename)
            with open(f"../Worker/FileStore/DIR_{worker_obj.port}/{filename}","r") as read_file:
                # message = {
                #     filename : read_file.read()
                # }
            # time.sleep(1)
                res2 = requests.post("http://localhost:8030",data = read_file.read(),params = {'request-type':'read','filename':filename})
            print("Worker : ", worker_obj.port, " has sent the file")
        
        elif q['request-type'] == 'clear':
            purge_exit(f"../Worker/FileStore/DIR_{worker_obj.port}/Mapper_files")
            # purge_exit("../Worker/temp_handler")
            worker_obj.lines = ""
            worker_obj.newoutfile = None
            worker_obj.outfile = None

    def getwithcontent(self):
       content_length = int(self.headers['Content-Length'])
       file_content = self.rfile.read(content_length)
       fields = urllib.parse.parse_qs(file_content)
       return ''.join([i.decode() for i in list(fields.values())[0]]),fields 

    def getcontent(self):
        content_length = int(self.headers['Content-Length'])
        file_content = self.rfile.read(content_length)
        fields = urllib.parse.parse_qs(file_content)
        return fields 

    def do_POST(self):
        query = urlparse(self.path).query
        q = dict(q.split("=") for q in query.split("&"))
        ####
        if q['request-type']=="write":
            content_len = int(self.headers['Content-Length'])
            file_name = q['filename']

            if(content_len == 0):
               with open(f'../Worker/FileStore/DIR_{worker_obj.port}/{q["filename"]}','w') as writefile:
                    pass
            else:
                post_data = self.rfile.read(content_len)
                with open(f'../Worker/FileStore/DIR_{worker_obj.port}/{q["filename"]}', "wb") as writefile:
                    writefile.write(post_data)

            print(f"Worker_{worker_obj.port} : Successfully finished write")           
            self.send_response(200)
            self.send_header('Content-type','text/html')
            self.end_headers()
            message = "Done"
            self.wfile.write(bytes(message, "utf8"))
        
        elif q['request-type']=="mapper":
            name,ext = q['filename'].split('.')
            filename = fileregex(f"../Worker/FileStore/DIR_{worker_obj.port}",f"^{name}.*{ext}$")
            print(filename)
            content_len = int(self.headers['Content-Length'])
            if not os.path.exists(f"../Worker/FileStore/DIR_{worker_obj.port}/Mapper_files"):
                os.mkdir(f"../Worker/FileStore/DIR_{worker_obj.port}/Mapper_files", mode=0o777)
            with open(F"../Worker/FileStore/DIR_{worker_obj.port}/Mapper_files/mapper.py","wb") as mapperfile:
                mapperfile.write(self.rfile.read(content_len))
            # type mrtest.txt | python "mapper copy.py"
            worker_obj.outfile = f"a_{filename.split('.')[0].split('_')[1]}.out"
            worker_obj.newoutfile = f"p_{filename.split('.')[0].split('_')[1]}.out"
            # command = f"type ..\\Worker\\FileStore\\DIR_{worker_obj.port}\\{filename} | python ..\\Worker\\FileStore\\DIR_{worker_obj.port}\\Mapper_files\\mapper.py " 
            with open(f'../Worker/FileStore/DIR_{worker_obj.port}/{filename}','rb') as infile:
                mapper_run = subprocess.Popen(['python',f"..\\Worker\\FileStore\\DIR_{worker_obj.port}\\Mapper_files\\mapper.py"], stdout=PIPE, stdin = PIPE, stderr = PIPE ,shell=True)
                with open(f'../Worker/FileStore/DIR_{worker_obj.port}/Mapper_files/{worker_obj.outfile}','wb') as outfile:
                    outfile.write(mapper_run.communicate(input=infile.read())[0])   
            print(f"Mapper.py finished running on WORKER_{worker_obj.port}")
            self.send_response(200)
            self.send_header('Content-type','text/html')
            self.end_headers()
            message = "mapper done"
            self.wfile.write(bytes(message+f",{len([name for name in os.listdir('.') if os.path.isfile(name)])}", "utf8"))

        elif q['request-type']=="shuffler":
            fields = self.getcontent()
            hashport,tosend = {},defaultdict(str)
            for each in fields:
                valofeach = ''.join([x.decode() for x in fields[each]])
                hashport[int(each.decode())] = valofeach
            send_req = {'request-type':'shufflesave'}
            with open(f"../Worker/FileStore/DIR_{worker_obj.port}/Mapper_files/{worker_obj.outfile}",'r') as afile:
                for line in afile:
                    hashto = hashport[myHash(line.split(',')[0],len(hashport))]
                    if hashto == str(worker_obj.port):
                        worker_obj.lines += line
                    else:    
                        tosend[hashto] += line
            print([each for each in dict(tosend)])             
            url = [(each,tosend[each]) for each in dict(tosend)]
            def post_req(value):
                return requests.post(f"http://localhost:{value[0]}",data = value[1],params = {'request-type':'shufflesave'})

            with ThreadPoolExecutor(max_workers=20) as pool:
                response_list = list(pool.map(post_req,url))
            # linesendres = requests.post(f"http://localhost:{each}",data = {'lines':tosend[each]},params = {'request-type':'shufflesave'})
            
            with open(f"../Worker/FileStore/DIR_{worker_obj.port}/Mapper_files/p_{worker_obj.outfile.split('.')[0].split('_')[1]}.out",'a') as writeto:
                writeto.write(worker_obj.lines)
            self.send_response(200)
            self.send_header('Content-type','text/html')
            self.end_headers()
            message = "Shuffling done"
            self.wfile.write(bytes(message, "utf8"))
        

        elif q['request-type']=="shufflesave":
            content_len = int(self.headers['Content-Length'])

            with open(f"../Worker/FileStore/DIR_{worker_obj.port}/Mapper_files/p_{worker_obj.outfile.split('.')[0].split('_')[1]}.out",'ab') as files:
                files.write(self.rfile.read(content_len))
                
            
            self.send_response(200)
            self.send_header('Content-type','text/html')
            self.end_headers()
            self.wfile.write(bytes("saved",'utf-8'))
        
        # elif q['request-type']=="shufflesave2":
        #     content_len = int(self.headers['Content-Length'])
        #     with open(f"../Worker/FileStore/DIR_{worker_obj.port}/Mapper_Files/p_{worker_obj.outfile.split('.')[0].split('_')[1]}.out",'ab') as shuffletransmiter:
        #             shuffletransmiter.write(self.rfile.read(content_len))
        #     self.send_response(200)
        #     self.send_header('Content-type','text/html')
        #     self.end_headers()
        #     self.wfile.write(bytes("saved",'utf-8'))

        elif q['request-type']=="reducer": 
            print("here in reducer") 
            content,fields = self.getwithcontent()
            # print(fields)
            content_len = int(self.headers['Content-Length'])
            with open(f"../Worker/FileStore/DIR_{worker_obj.port}/Mapper_files/reducer.py","w") as reducerfile:
                reducerfile.write(content)
            with open(f"../Worker/FileStore/DIR_{worker_obj.port}/Mapper_files/{worker_obj.newoutfile}",'r') as tosort:
                # print("sorting")
                data=(tosort).readlines()
                data.sort()
                # print("sorted")
            with open(f"../Worker/FileStore/DIR_{worker_obj.port}/Mapper_files/{worker_obj.newoutfile}",'w') as tosort:
                tosort.write(''.join(data))
                print("written")

            savedas = f"{q['filename'].split('.')[0]}-part-00000_{worker_obj.newoutfile.split('.')[0].split('_')[1]}"
            print(worker_obj.port,savedas)
            try:
                with open(f'../Worker/FileStore/DIR_{worker_obj.port}/Mapper_files/{worker_obj.newoutfile}','rb') as infile:
                    mapper_run = subprocess.Popen(['python',f"..\\Worker\\FileStore\\DIR_{worker_obj.port}\\Mapper_files\\reducer.py"], stdout=PIPE, stdin = PIPE, stderr = PIPE ,shell=True)
                    with open(f"../Worker/FileStore/DIR_{worker_obj.port}/{savedas}",'wb') as outfile:
                        outfile.write(mapper_run.communicate(input=infile.read())[0])  
                print("running reducer")
            except Exception:
                self.send_response(500)
                print("here")
                self.send_header('Content-type','text/html')
                self.end_headers()
                self.wfile.write(bytes("Failed to do reducer",'utf-8'))
                raise Exception("Failed to execute reducer!")

            print(f"Reducer.py finished running on WORKER_{worker_obj.port}")
            result = {
                'filename' : savedas,
                'size' : str(os.path.getsize(f"../Worker/FileStore/DIR_{worker_obj.port}/{savedas}"))
            }

            self.send_response(200)
            self.send_header('Content-type','text/html')
            self.end_headers()
            self.wfile.write(bytes(f'{json.dumps(result)}','utf-8'))




class Worker:
    def __init__(self):
        self.port = int(sys.argv[1])
        self.outfile = None
        self.newoutfile = None
        self.lines = str()
        if not os.path.exists(f"../Worker/FileStore"):
            os.mkdir(f"../Worker/FileStore",mode=0o777)
        if not os.path.exists(f"../Worker/FileStore/DIR_{sys.argv[1]}"):
            os.mkdir(f"../Worker/FileStore/DIR_{sys.argv[1]}",mode=0o777)
        if not os.path.exists(f"../Worker/temp_handler"):
            os.mkdir(f"../Worker/temp_handler",mode=0o777) 
        if not os.path.exists(f"../Worker/worker_tempfiles"):
            os.mkdir(f"../Worker/worker_tempfiles",mode=0o777)        

    def run(self):
        with socketserver.TCPServer(("", self.port), handler) as httpd:
            print("WORKER: serving at port", self.port)
            httpd.serve_forever()
            

worker_obj = Worker()
worker_obj.run()
     