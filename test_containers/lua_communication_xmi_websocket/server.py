# import json
#
# import websockets
# import asyncio
# from cassis import *
#
# # Server data
# PORT = 7890
# print("Server listening on Port " + str(PORT))
#
# communication = ''
# with open('communication_xmi.lua', 'r') as f:
#     communication = f.read()
#
# typesystem = ''
# with open('dkpro-core-types.xml', 'rb') as f:
#     typesystem = load_typesystem(f)
#
# # A set of connected ws clients
# connected = set()
#
#
# def message_to_json(msg):
#     message_json = dict()
#     message_json["message"] = msg
#     return str(message_json)
#
#
# # The main behavior function for this server
# async def proto_anno(websocket, path):
#     print("A client just connected")
#     # Store a copy of the connected client
#     connected.add(websocket)
#     # Handle incoming messages
#     print(websocket.id)
#
# #     async def on_message(result):
# #         result_json = json.loads(result)
# #         message = result_json['message']
# #         if message == "communication":
# #             # communication_layer_handler
# #             if communication == "":
# #                 await websocket.send("Error with communication-layer!")
# #             await websocket.send(message_to_json(communication))
# #             await on_message(await websocket.recv())
# #
# #         if message == "typesystem":
# #
# #             await websocket.send(message_to_json(typesystem.to_xml().encode('utf-8')))
# #             await on_message(await websocket.recv())
# #         if message:
# #             print(message)
# #             cas = load_cas_from_xmi(message, typesystem=typesystem, lenient=True)
# #             print(cas.to_xmi().encode('utf-8'))
# #             await websocket.send(message_to_json(cas.to_xmi().encode('utf-8')))
# #             await on_message(await websocket.recv())
# #
# #     await on_message(await websocket.recv())
#
#
#     # Handle disconnecting clients
#     try:
#         async for message in websocket:
#             print(message[0])
#
#             code = message[0]
#             m = str(message[1:].decode("utf-8"))
#
#             if code == 101:
#                 # communication_layer_handler
#                 if communication == "":
#                     await websocket.send("Error with communication-layer!")
#
#                 await websocket.send(communication)
#
#                 print(communication)
#                 print("Communication layer sent!")
#
#
#             elif code == 107:
#                 # typesystem_handler
#                 if typesystem == "":
#                     await websocket.send("Error with typesystem!")
#
#                 await websocket.send(typesystem.to_xml().encode('utf-8'))
#                 print("Typesystem sent!")
#
#             elif message == 103:
#                 # process_handler
#                 pass
#
#     except websockets.exceptions.ConnectionClosed:
#         print("A client just disconnected")
#     finally:
#         connected.remove(websocket)
#
# # Start the server
# start_server = websockets.serve(proto_anno, "localhost", PORT)
# asyncio.get_event_loop().run_until_complete(start_server)
# asyncio.get_event_loop().run_forever()

import aiohttp
from aiohttp import web, WSCloseCode
import asyncio
from cassis import *
import json
import base64

communication = ''
with open('communication_xmi.lua','r') as f:
    communication = f.read()

with open('dkpro-core-types.xml', 'rb') as f:
    typesystem = load_typesystem(f)


async def http_handler(request):
    return web.Response(text=communication.encode('utf-8'), content_type="text/plain")

async def communication_layer_handler(request):
    if communication == '':
        return web.Response(status=404)
    return web.Response(body=communication.encode('utf-8'), content_type="text/plain")

async def typesystem_handler(request):
    return web.Response(body=typesystem.to_xml().encode('utf-8'), content_type="application/json")

async def process_handler(request):
    post_body = (await request.read()).decode("utf-8")
    cas = load_cas_from_xmi(post_body, typesystem=typesystem,lenient=True)
    return web.Response(body=cas.to_xmi().encode('utf-8'), content_type="application/json", status=200)

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    async for msg in ws:
        print(msg)
        if msg.type == aiohttp.WSMsgType.TEXT:
            if msg.data == 'close':
                await ws.close()
            else:
                await ws.send_str('some websocket message payload')
        elif msg.type == aiohttp.WSMsgType.ERROR:
            print('ws connection closed with exception %s' % ws.exception())

    return ws


def create_runner():
    app = web.Application()
    app.add_routes([
        web.get('/',   http_handler),
        web.get('/v1/communication_layer', communication_layer_handler),
        web.get('/ws', websocket_handler),
        web.get('/v1/typesystem', typesystem_handler),
        web.post("/v1/process", process_handler)

    ])
    return web.AppRunner(app)

async def start_server(host="127.0.0.1", port=9715):
    runner = create_runner()
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_server())
    loop.run_forever()
