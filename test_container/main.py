from cassis import *
import http.server
import socketserver
import json


class MyHttpRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_POST(self):
        post_body = self.rfile.read(50).decode("utf-8")
        #loaded = json.loads(post_body)
        #print(loaded)
        #cas = load_cas_from_xmi(loaded["cas"], typesystem=loaded["typesystem"])

        # Sending an '200 OK' response
        self.send_response(200)

        # Setting the header
        self.send_header("Content-type", "application/json")

        # Whenever using 'send_header', you also have to call 'end_headers'
        self.end_headers()
        self.wfile.write(b"{\"cas\": \"cas\"}")

# Create an object of the above class
handler_object = MyHttpRequestHandler

PORT = 9714
my_server = socketserver.TCPServer(("0.0.0.0", PORT), handler_object)

print("Server started on port 9714\r\n")
# Star the server
my_server.serve_forever()
