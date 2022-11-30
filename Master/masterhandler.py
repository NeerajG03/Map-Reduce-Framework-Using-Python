import http
from urllib.parse import urlparse
import urllib
from ast import literal_eval
from concurrent.futures import ThreadPoolExecutor
import requests
import json


with open('./info.txt') as config:
        numofworker = int(config.readline().strip().split(",")[1])
        base  = int(config.readline().strip().split(",")[1])


class handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        query = urlparse(self.path).query
        q = dict(q.split("=") for q in query.split("&"))
        
        if q['request-type'] == 'write':
            self.send_response(200)
            self.send_header('Content-type','text/html')
            self.end_headers()
            message = [base+x for x in range(numofworker)]
            self.wfile.write(bytes(f"{message}", "utf8"))
            print("Master : Write request fullfilled")     
        
        elif q['request-type'] == 'read':
            with open('./Metadata/references.json') as raw_file:
                metadata = json.load(raw_file)
                if f'{q["filename"]}.{q["extension"]}' not in metadata:
                    self.send_response(500)
                    self.send_header('Content-type','text/html')
                    self.end_headers()
                    message = "File unavailable in DFS"
                    self.wfile.write(bytes(f'{message}', "utf8"))
                else:
                    self.send_response(200)
                    self.send_header('Content-type','text/html')
                    self.end_headers()
                    ports = metadata[f"{q['filename']}.{q['extension']}"]
                    with open(f"./Metadata/{q['filename']}_manifest","r") as manifest:
                        message = {
                            "ports" : ports,
                            f"{q['filename']}_manifest" : manifest.read()
                        }
                    response = []   
                    send_req = {
                        'request-type' : 'send-file',
                        'filename' : q['filename'],
                        'extension' : q['extension']
                    } 
                    for each in ports:
                        try:
                            requests.get(f"http://localhost:{each}/", params = (send_req), timeout=0.0000001)
                        except Exception:
                            pass
                    print(message) 
                    self.wfile.write(bytes(f'{json.dumps(message)}', "utf8"))
        elif q['request-type'] == 'mapreduce':
            ports = None
            with open('./Metadata/references.json') as raw_file:
                metadata = json.load(raw_file)
                if f'{q["filename"]}' not in metadata:
                    self.send_response(500)
                    self.send_header('Content-type','text/html')
                    self.end_headers()
                    message = "File unavailable in DFS"
                    self.wfile.write(bytes(f'{message}', "utf8"))
                else:
                    self.send_response(200)
                    self.send_header('Content-type','text/html')
                    self.end_headers()
                    ports = metadata[f"{q['filename']}"]
            content_length = int(self.headers['Content-Length'])
            file_content = self.rfile.read(content_length)
            fields = urllib.parse.parse_qs(file_content)
            mapperfile = ''.join([i.decode() for i in list(fields.values())[0]])
            reducerfile = ''.join([i.decode() for i in list(fields.values())[1]])

            # mapper start
            print("_________________________________________________________________________")
            print("Sending mapper files to worker and initializing mapper stage\n")
            mapperres = []
            send_req = {'request-type':'mapper','filename':q['filename']}

            workerreq = None

            def post_req(value):
                return requests.post(f"http://localhost:{value}/", params = (send_req),data = mapperfile)

            with ThreadPoolExecutor(max_workers=10) as pool:
                response_list = list(pool.map(post_req,ports))
            
            for each in response_list:
                if each.status_code == 500:
                    raise Exception("Error while performing map function")   
            # mapper done
            
            # shuffling stage
            print("Mapper stage complete\n")
            print("_________________________________________________________________________\nInitializing shuffling stage")   
            shufflerres = []
            send_req = {'request-type':'shuffler','filename':q['filename']}
            data = {}

            for i,each in enumerate(ports):
                data[i] = each

            for each in ports:
                do = requests.post(f"http://localhost:{each}/", params = (send_req),data = data)
                if do.status_code == 500:
                    raise Exception("Error while performing mr") 

            # with ThreadPoolExecutor(max_workers=10) as pool:
            #     response_list = list(pool.map(post_req,ports))

            # for each in ports:
            #     shufflerres.append(requests.post(f"http://localhost:{each}/", params = (send_req),data = data))
            # for each in response_list:
            #     if each.status_code == 500:
            #         
            # shuffling ends
            print("done till here",ports)
            # send_req = {'request-type':'shufflesave'}
            # for each in ports[::-1]:
            #     do = requests.post(f"http://localhost:{each}/", params = send_req)

            # reduce stage
            print("Shuffling stage complete\n")
            print("_________________________________________________________________________\nInitializing reducer stage")     
            reducerres = []
            send_req = {'request-type':'reducer','filename':q['filename']}
            # data = {'reducer':reducerfile}
            # workerreq = None
            count = 0
            # print(reducerfile)
            # with open(f"./Metadata/{manifestfile}",'w') as getfile:
                # pass
            def post_req(value):
                return requests.post(f"http://localhost:{value}/", params = (send_req),data = {'reducer':reducerfile})

            with ThreadPoolExecutor(max_workers=10) as pool:
                response_list = list(pool.map(post_req,ports))
            
            for each in response_list:
                if each.status_code == 500:
                    raise Exception("Error while performing reduce function") 
                splitfileval = json.loads(each.content.decode())
                print(splitfileval)
                manifestfile = f"{q['filename'].split('.')[0]}-part-00000_manifest"
                
                with open(f"./Metadata/{manifestfile}",'a') as getfile:
                    if count == 0:
                        getfile.write('filename,filesize,header\n')
                    getfile.write(f"{splitfileval['filename'].replace('.txt','')},{splitfileval['size']},False\n")
                    count+=1  

            # for each in ports:
            #     reducerres.append(requests.post(f"http://localhost:{each}/", params = (send_req),data = data))
            #     if reducerres[-1].status_code == 500:
            #         raise Exception("Error while performing reduce function")
            #     splitfileval = json.loads(reducerres[-1].content.decode())
            #     # print(splitfileval)
            #     manifestfile = f"{q['filename'].split('.')[0]}-part-00000_manifest"
            #     with open(f"./Metadata/{manifestfile}",'a') as getfile:
            #         if count == 0:
            #             getfile.write('filename,filesize,header\n')
            #         getfile.write(f"{splitfileval['filename'].replace('.txt','')},{splitfileval['size']},False\n")
            #         count+=1

            with open('./Metadata/references.json',"r") as raw_file:
                metadata = json.load(raw_file)
            metadata[f"{q['filename'].split('.')[0]}-part-00000.txt"] = ports
            with open('./Metadata/references.json',"w") as raw_file:
                json.dump(metadata,raw_file)
            for each in ports:
                final = requests.get(f"http://localhost:{each}/",params = {'request-type':'clear'})
            # reducer done
            print("\nReducer has finished execution!")
            print("_______________________________________________________________________\n")
            self.send_response(200)
            self.send_header('Content-type','text/html')
            self.end_headers()
            self.wfile.write(bytes(f"{q['filename'].split('.')[0]}-part-00000.txt", "utf8"))

    def do_POST(self):
        query = urlparse(self.path).query
        print(query)
        
        q = dict(q.split("=") for q in query.split("&"))
        if q['request-type']=="metadata_save":
            content_length = int(self.headers['Content-Length'])
            file_content = self.rfile.read(content_length)
            fields = urllib.parse.parse_qs(file_content)
            for i,each in enumerate(fields):
                if "_manifest" in each.decode():
                    with open(f'./Metadata/{each.decode()}','w') as writefile:
                        content = ''.join([x.decode().replace("\r","") for x in list(fields.values())[i]])
                        writefile.write(content)
                else:
                    values = ','.join([x.decode() for x in fields[each]])
                    with open('./Metadata/references.json',"r") as raw_file:
                        metadata = json.load(raw_file)
                    metadata[each.decode()] = literal_eval(values)
                    with open('./Metadata/references.json',"w") as raw_file:
                        json.dump(metadata,raw_file)
            print("Master : Recieved Metadata and Split Reference File and Storage is done!\n",
            "__________________________________________________________________________________")            
            self.send_response(200)
            self.send_header('Content-type','text/html')
            self.end_headers()
            message = "Successfully completed the Write task"
            self.wfile.write(bytes(message, "utf8"))