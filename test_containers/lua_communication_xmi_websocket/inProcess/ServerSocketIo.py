import http
import socketserver

import eventlet
import socketio
from cassis import load_cas_from_xmi, load_typesystem

sio = socketio.Server()
static_files = {
    '/': ''
}
app = socketio.WSGIApp(sio, static_files=static_files)




@sio.event
def connect(sid, environ):
    print(f'Client {sid} is connected ')
    print("sending..... ")
    sio.emit("givo", {"HAkkkdfsd": "1211434"})


@sio.on('*')
def catch_all(message, sid, data):
    print("Message: ", message)
    print("ID: ", sid)
    print('data', data)
    sio.emit("miaw", {"ich": "givara"})


@sio.on('jcas')
def get_NLP(sid, data, type_system):
    print("111 sid: ")
    print(sid)
    print("111 data: ")
    print(data)
    print("111 typeSystem: ")
    print(type_system)
    jc = data.decode("utf-8")
    print("111 JC: ")
    print(jc)
    type_system = load_typesystem(type_system)
    cas = load_cas_from_xmi(jc, typesystem=type_system, lenient=True)
    print("cas")
    print(cas)


@sio.on('please_send_server_id')
def please_send_server_id(sid, message):
    print("client with "+sid+" want sever id => ", message)
    sio.emit("sever_id", {"annotator":"SpacyAnnotator"})


@sio.event
def disconnect(sid):
    print(f'Client {sid} is disconnected ')


if __name__ == '__main__':
    eventlet.wsgi.server(eventlet.listen(('localhost', 9716)), app)

