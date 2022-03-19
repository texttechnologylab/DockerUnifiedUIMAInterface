from cassis import *
import http.server
import socketserver
import json
import zstandard
import base64


with open('dkpro-core-types.xml', 'rb') as f:
    typesystem = load_typesystem(f)

    class MyHttpRequestHandler(http.server.SimpleHTTPRequestHandler):
        def do_POST(self):
            content_len = int(self.headers.get('Content-Length'))
            post_body = self.rfile.read(content_len).decode("utf-8")


            decoded = json.loads(post_body)
            dctx = zstandard.ZstdDecompressor()
            cas_string = dctx.decompress(base64.b64decode(decoded["cas"]))

            cas = load_cas_from_xmi(cas_string, typesystem=typesystem,lenient=True)
            #loaded = json.loads(post_body)
            #print(loaded)
            #cas = load_cas_from_xmi(loaded["cas"], typesystem=loaded["typesystem"])

            # Sending an '200 OK' response
            self.send_response(200)

            # Setting the header
            self.send_header("Content-type", "application/json")

            # Whenever using 'send_header', you also have to call 'end_headers'
            self.end_headers()
            new_obj = {"cas": }
            cctx = zstandard.ZstdCompressor()
            compressed = cctx.compress(cas.to_xmi().encode('utf-8'))
            self.wfile.write(json.dumps(new_obj).encode('utf-8'))

    # Create an object of the above class
    handler_object = MyHttpRequestHandler

    PORT = 9714
    my_server = socketserver.TCPServer(("0.0.0.0", PORT), handler_object)

    print("Server started on port 9714\r\n")
    # Star the server
    my_server.serve_forever()
