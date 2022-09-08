from cassis import *
import os
import http.server
import socketserver
import json
import base64
import sys
import argparse

communication = ''


ap = argparse.ArgumentParser()
ap.add_argument('--inputs', type=str, default="[]", help="")
ap.add_argument('--outputs', type=str, default="[]", help="")
ap.add_argument('--port', type=int, default=9714, help="port")
parsed_args = ap.parse_args()

input, output, PORT = parsed_args.inputs, parsed_args.outputs, parsed_args.port
input, output = json.loads(input), json.loads(output)

with open('communication_xmi.lua', 'r') as f:
    communication = f.read()

with open('dkpro-core-types.xml', 'rb') as f:
    typesystem = load_typesystem(f)

    class MyHttpRequestHandler(http.server.SimpleHTTPRequestHandler):
        def do_POST(self):
            content_len = int(self.headers.get('Content-Length'))
            post_body = self.rfile.read(content_len).decode("utf-8")
            print("POST Body")
            print(post_body)
            print()

            cas = load_cas_from_xmi(post_body, typesystem=typesystem, lenient=True)
            #loaded = json.loads(post_body)
            #print(loaded)
            #cas = load_cas_from_xmi(loaded["cas"], typesystem=loaded["typesystem"])

            # Sending an '200 OK' response
            self.send_response(200)

            # Setting the header
            self.send_header("Content-type", "application/json")

            # Whenever using 'send_header', you also have to call 'end_headers'
            self.end_headers()
            self.wfile.write(f"{PORT}".encode("utf-8"))  # calls deserialize in lua
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
            elif self.path == '/v1/details/input_output':
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                dictjs = {'inputs': input, 'outputs': output}
                self.wfile.write(json.dumps(dictjs).encode('utf-8'))
            else:
                # Sending an '200 OK' response
                self.send_response(200)

                # Setting the header
                self.send_header("Content-type", "application/json")

                # Whenever using 'send_header', you also have to call 'end_headers'
                self.end_headers()
                self.wfile.write(typesystem.to_xml().encode('utf-8'))
    # Create an object of the above class
    handler_object = MyHttpRequestHandler

    # PORT = int(os.environ["PORT"])
    my_server = socketserver.TCPServer(("0.0.0.0", PORT), handler_object)

    print(f"Server started on port {PORT}\r\n")
    # Start the server
    my_server.serve_forever()
