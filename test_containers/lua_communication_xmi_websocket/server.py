import aiohttp
from aiohttp import web, WSCloseCode
import asyncio
from cassis import *
import json
import base64

communication = ''
with open('communication_xmi.lua', 'r') as f:
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
    cas = load_cas_from_xmi(post_body, typesystem=typesystem, lenient=True)
    print("REST ANALYSIS")
    return web.Response(body=cas.to_xmi().encode('utf-8'), content_type="application/json", status=200)


async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    async for msg in ws:
        #         print(msg)
        jc = msg.data.decode("utf-8")
        """
        @author
        Givara Ebo
        """
        print(msg.data)
        cas = load_cas_from_xmi(jc, typesystem=typesystem, lenient=True)
        print("WEBSOCKET ANALYSIS")
        await ws.send_bytes(cas.to_xmi().encode('utf-8'))
        # if msg.type == aiohttp.WSMsgType.TEXT:
        #     if msg.data == 'close':
        #         await ws.close()
        #     else:
        #         await ws.send_str('some websocket message payload')
        # elif msg.type == aiohttp.WSMsgType.ERROR:
        #     print('ws connection closed with exception %s' % ws.exception())

    return ws


def create_runner():
    app = web.Application()
    app.add_routes([
        web.get('/', http_handler),
        web.get('/v1/communication_layer', communication_layer_handler),
        web.get('/v1/process_websocket', websocket_handler),
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
