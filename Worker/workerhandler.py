import http
from urllib.parse import urlparse
import urllib


class handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        print(self)
        self.send_response(200)
        self.send_header('Content-type','text/html')
        self.end_headers()
        message = "GOT IT"
        self.wfile.write(bytes(message, "utf8"))

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        file_content = self.rfile.read(content_length)
        # print(file_content)
        fields = urllib.parse.parse_qs(file_content)
        print(fields)
        
        # for each in fields:
            # with open()
            # fields[each]
        self.send_response(200)
        self.send_header('Content-type','text/html')
        self.end_headers()
        message = "Hello, World! Here is a POST response"
        self.wfile.write(bytes(message, "utf8"))