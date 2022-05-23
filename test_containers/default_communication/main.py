from cassis import *
import http.server
import socketserver
import json
import base64

with open('dkpro-core-types.xml', 'rb') as f:
    typesystem = load_typesystem(f)

    class MyHttpRequestHandler(http.server.SimpleHTTPRequestHandler):
        # /v1/process
        def do_POST(self):
            content_len = int(self.headers.get('Content-Length'))
            post_body = self.rfile.read(content_len).decode("utf-8")

            decoded = json.loads(post_body)
            print(decoded)

            cas = load_cas_from_xmi(decoded["cas"], typesystem=typesystem,lenient=True)
            #loaded = json.loads(post_body)
            #print(loaded)
            #cas = load_cas_from_xmi(loaded["cas"], typesystem=loaded["typesystem"])

            # Sending an '200 OK' response
            self.send_response(200)

            # Setting the header
            self.send_header("Content-type", "application/json")

            # Whenever using 'send_header', you also have to call 'end_headers'
            self.end_headers()
            new_obj = {"cas": cas.to_xmi()}
            self.wfile.write(json.dumps(new_obj).encode('utf-8'))
        def do_GET(self):
            if self.path == '/v1/communication_layer':
                # Sending an '404 Not found' response
                self.send_response(404)
                self.end_headers()
                self.wfile.close()
                return
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
