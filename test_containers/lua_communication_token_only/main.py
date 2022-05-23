from cassis import *
import http.server
import socketserver
import json
import base64

communication = ''
with open('communication_xmi.lua','r') as f:
    communication = f.read()

with open('dkpro-core-types.xml', 'rb') as f:
    typesystem = load_typesystem(f)

    class MyHttpRequestHandler(http.server.SimpleHTTPRequestHandler):
#/v1/process
        def do_POST(self):
            content_len = int(self.headers.get('Content-Length'))
            post_body = self.rfile.read(content_len).decode("utf-8")
            print(post_body)
            self.send_response(200)

            # Setting the header
            self.send_header("Content-type", "application/json")

            # Whenever using 'send_header', you also have to call 'end_headers'
            self.end_headers()
            self.wfile.write(b'100')
        def do_GET(self):
            if self.path == '/v1/communication_layer':
                # Sending an '200 OK' response
                if communication == '':
                    self.send_response(404)
                    return
                self.send_response(200)

                # Setting the header
                self.send_header("Content-type", "text/plain")

                # Whenever using 'send_header', you also have to call 'end_headers'
                self.end_headers()
                self.wfile.write(communication.encode('utf-8'))


            else:
                # /v1/typesystem
                # Sending an '200 OK' response
                self.send_response(200)

                # Setting the header
                self.send_header("Content-type", "application/json")

                # Whenever using 'send_header', you also have to call 'end_headers'
                self.end_headers()
                self.wfile.write(typesystem.to_xml().encode('utf-8'))
    # Create an object of the above class
    handler_object = MyHttpRequestHandler

    PORT = 9714
    my_server = socketserver.TCPServer(("0.0.0.0", PORT), handler_object)

    print("Server started on port 9714\r\n")
    # Star the server
    my_server.serve_forever()
