from typing import Any, AsyncGenerator, Dict, List, Callable
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import uvicorn
import logging
from asyncio import sleep
from aiostream import stream
import sqlite3 as sql
from data_model import (
    Status,
    PipelineMeasurements,
    GraphUpdate,
    DocumentMetaData,
    DocumentMeasurements,
    SystemStatic,
    SystemDynamic,
    SystemResources,
    PipelineState,
    SystemState,
    SystemDynamicState,
    ThreadState,
    ThreadStates,
    DockerStates,
    DocumentStates,
    DocumentMetaState,
    DocumentState,
    DockerState, 
    ContainerStats
)
from util import format_bytes, format_time, format_state


def innerHTML(id_: str, content: str):
    return f"""
        <div id={id_} hx-swap-oob="innerHTML">
            {content}
        </div>
    """


# TODO: Adjust Dockerfile
logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)
logger.info("TTLab TextImager DUUI Monitor")

run_active = True
state = PipelineState()
system_state = SystemState()
thread_states = ThreadStates()
docker_states = DockerStates()
document_states = DocumentStates()

app = FastAPI(
    # openapi_url="/openapi.json",
    # docs_url="/api",
    # redoc_url=None,
    title="DUUIMonitor",
    description="Monitoring DUUI execution pipeline.",
    terms_of_service="https://www.texttechnologylab.org/legal_notice/",
    contact={
        "name": "TTLab Team",
        "url": "https://texttechnologylab.org",
        "email": "s0424382@stud.uni-frankfurt.de",
    },
    # license_info={
    #     "name": "AGPL",
    #     "url": "http://www.gnu.org/licenses/agpl-3.0.en.html",
    # },
)


db = sql.connect("./pipelines.db")
logger.info("SQLite Database connected!")

templates = Jinja2Templates(directory="templates")


@app.on_event("shutdown")
async def shutdown_event():
    db.close()
    logger.info("SQLite Database disconnected!")


@app.post("/v1/status")
async def status(info: Status):
    global run_active, state, system_state, thread_states, docker_states, document_states
    
    if info.status.upper() == "STARTED":
        run_active = True
        state = PipelineState()
        system_state = SystemState()
        thread_states = ThreadStates()
        docker_states = DockerStates()
        document_states = DocumentStates()
    # else:
    #     run_active = False

    return 1


@app.post("/v1/system_dynamic_info")
async def system_dynamic_info(info: SystemDynamic):
    dynamic = SystemDynamicState()
    dynamic.cpu_load_average = f"{info.cpu_load_average}"
    dynamic.cpu_jvm_load = f"{info.cpu_jvm_load*100.0:.2f}%"
    dynamic.cpu_system_load = f"{info.cpu_system_load*100.0:.2f}%"
    dynamic.system_memory_total = format_bytes(info.system_memory_total)
    dynamic.system_memory_used = format_bytes(info.system_memory_used)
    dynamic.memory_heap_used = format_bytes(info.memory_heap_used)
    dynamic.memory_heap_committed = format_bytes(info.memory_heap_committed)
    dynamic.memory_non_heap_used = format_bytes(info.memory_non_heap_used)
    dynamic.memory_non_heap_committed = format_bytes(info.memory_non_heap_committed)
    dynamic.memory_used = format_bytes(info.memory_used)
    dynamic.memory_total = format_bytes(info.memory_total)

    system_state.dynamic = dynamic

    await system_state.system["dynamic"].count_down()

    for thread_stat in info.thread_stats:
        exists = thread_states.threads.get(str(thread_stat.thread_id), False)
        thread_state = (
            ThreadState()
            if not exists
            else thread_states.threads[str(thread_stat.thread_id)]
        )

        thread_state.thread_id = str(thread_stat.thread_id)
        thread_state.thread_name = thread_stat.thread_name
        thread_state.thread_state = format_state(thread_stat.thread_state)
        thread_state.thread_cpu_time = format_time(
            thread_stat.thread_cpu_time, "nanoseconds"
        )
        thread_state.jvm_cpu_time = format_time(thread_stat.jvm_cpu_time, "nanoseconds")
        thread_state.thread_memory_usage = format_bytes(thread_stat.thread_memory_usage)
        thread_state.thread_total_wait_time = format_time(
            thread_stat.thread_total_wait_time, "milliseconds"
        )
        thread_state.thread_total_block_time = format_time(
            thread_stat.thread_total_block_time, "milliseconds"
        )

        thread_states.threads[str(thread_stat.thread_id)] = thread_state

    await thread_states.lock.count_down()

    return 1


@app.post("/v1/system_static_info")
async def system_static_info(info: SystemStatic):
    system_state.os_processors = info.os_processors
    system_state.os_version = info.os_version
    system_state.os_name = info.os_name
    system_state.os_arch = info.os_arch
    system_state.os_jvm_vendor = info.os_jvm_vendor
    system_state.os_jvm_max_memory = format_bytes(float(info.os_jvm_max_memory))

    await system_state.system["static"].count_down()

    return 1


@app.post("/v1/system_resource_info")
async def test3(info: SystemResources):
    res: List[ContainerStats] = []

    if info.monitor:
        res.append(info.monitor)

    if info.docker_driver:
        res.extend(info.docker_driver)

    for r in res:
        ds = DockerState()
        ds.container_id = r.container_id
        ds.state = format_state(r.status)
        ds.image_id = r.image_id
        ds.cpu_usage = f"{r.cpu_usage}%"
        ds.memory_usage = format_bytes(r.memory_usage)
        ds.memory_max_usage = format_bytes(r.memory_max_usage)
        ds.memory_limit = format_bytes(r.memory_limit)
        ds.network_i = format_bytes(r.network_i)
        ds.network_o = format_bytes(r.network_o)
        docker_states.docker_states[r.container_id] = ds

    await docker_states.lock.count_down()


@app.post("/v1/document_table")
async def load_document_table(doc_measurements: DocumentMetaData):
    meta_state = DocumentMetaState()
    meta_state.name = doc_measurements.name
    meta_state.title = doc_measurements.title
    meta_state.initial_size = format_bytes(doc_measurements.initial_size)
    document_states.meta_states[doc_measurements.name] = meta_state

    await document_states.meta.count_down()

    logger.info(doc_measurements.json())
    return 1


@app.post("/v1/document_measurement")
async def load_document_measurements(doc_measurements: DocumentMeasurements):
    doc_state = DocumentState()
    doc_state.name = doc_measurements.name
    doc_state.component = doc_measurements.component
    doc_state.scale = str(doc_measurements.scale)
    doc_state.urlwait = format_time(float(doc_measurements.urlwait), "nanoseconds")
    doc_state.serialization = format_time(
        float(doc_measurements.serialization), "nanoseconds"
    )
    doc_state.deserialization = format_time(
        float(doc_measurements.deserialization), "nanoseconds"
    )
    doc_state.annotator = format_time(float(doc_measurements.annotator), "nanoseconds")
    doc_state.component_total = format_time(
        float(doc_measurements.component_total), "nanoseconds"
    )
    doc_state.parent_wait = format_time(
        float(doc_measurements.parent_wait), "nanoseconds"
    )
    doc_state.worker_total = format_time(
        float(doc_measurements.worker_total), "nanoseconds"
    )
    doc_state.document_size = format_bytes(int(doc_measurements.document_size))

    document_states.document_states[
        f"{doc_state.name}{doc_state.component}"
    ] = doc_state
    await document_states.docs.count_down()
    # logger.info(doc_measurements.json())

    return 1


# TODO: new run initialized here! State must be reset.
# state = PipelineState()
# system_state = SystemState()
# thread_states = ThreadStates()
@app.post("/v1/pipeline_measurement")
async def load_pipeline_measurements(pipe_measurements: PipelineMeasurements):
    def graph(png: str) -> str:
        return f'<img height="200" width="500" class="img-thumbnail" alt="..." src="data:image/png;base64,{png}">'

    if pipe_measurements.name:
        # state = PipelineState()
        # system_state = SystemState()
        # thread_states = ThreadStates()
        # docker_states = DockerStates()
        # document_states = DocumentStates()
        state.name = pipe_measurements.name
        await state.pipeline["name"].count_down()

    if pipe_measurements.workers:
        state.workers = pipe_measurements.workers
        await state.pipeline["workers"].count_down()

    if pipe_measurements.graph:
        state.graph = graph(pipe_measurements.graph)
        await state.pipeline["graph"].count_down()

    if pipe_measurements.document_count:
        state.document_count = pipe_measurements.document_count
        await state.pipeline["document_count"].count_down()

    if pipe_measurements.duration:
        state.duration = pipe_measurements.duration
        await state.pipeline["duration"].count_down()

    return 1


@app.post("/v1/graph_update")
async def load_graph_update(graph_update: GraphUpdate):
    graph = f'<img height="50" width="100" class="img-thumbnail" alt="..." src="data:image/png;base64,{graph_update.png}">'

    document_states.graph_states[graph_update.name] = graph

    await document_states.graph.count_down()
    return 3


def members(obj, exclude: Callable[[str], bool]):
    return (
        attr
        for attr in dir(obj)
        if not callable(getattr(obj, attr))
        and not attr.startswith("__")
        and not exclude(attr)
    )


async def document_poll():
    document_init: Dict[str, bool] = {}
    end = False
    while not end:
        for did, ds in document_states.meta_states.items():
            if not document_init.get(did, False):
                template = templates.TemplateResponse(
                    "document.html", {"request": "", **ds.todict()}
                ).body.decode()
                yield template
                document_init[did] = True
        document_states.reset_meta()
        await document_states.meta.wait()
        if not run_active:
            end = True


async def document_measurements_poll():
    measurement_init: Dict[str, bool] = {}
    end = False
    while not end:
        for did, ds in document_states.document_states.items():
            if not measurement_init.get(did, False):
                template = templates.TemplateResponse(
                    "measurement.html", {"request": "", **ds.todict()}
                ).body.decode()
                yield template
                measurement_init[did] = True
        document_states.reset_doc()
        await document_states.docs.wait()
        if not run_active:
            end = True


async def graph_poll():
    end = False
    while not end:
        pngs = list(document_states.graph_states.items())
        for did, ds in pngs:
            yield innerHTML(f"d_{did}_graph", ds)

        document_states.reset_graph()
        await document_states.graph.wait()
        if not run_active:
            end = True


async def container_stats_poll():
    container_sent_dict: Dict[str, bool] = {}
    end = False
    while not end:
        for tid, ts in docker_states.docker_states.items():
            if not container_sent_dict.get(tid, False):
                template = templates.TemplateResponse(
                    "docker_container.html", {"request": "", **ts.todict()}
                ).body.decode()
                yield template
                container_sent_dict[tid] = True
            else:
                yield innerHTML(f"d_{tid}_cpu_usage", ts.cpu_usage)
                yield innerHTML(f"d_{tid}_state", ts.state)
                yield innerHTML(f"d_{tid}_network", f"{ts.network_i} / {ts.network_o}")
                yield innerHTML(
                    f"d_{tid}_memory", f"{ts.memory_usage} / {ts.memory_limit}"
                )
                yield innerHTML(f"d_{tid}_memory_max", ts.memory_max_usage)

        docker_states.reset()
        await docker_states.lock.wait()
        if not run_active:
            end = True


async def thread_stats_poll():
    thread_sent_dict: Dict[str, bool] = {}
    end = False
    while not end:
        for tid, ts in thread_states.threads.items():
            if not thread_sent_dict.get(tid, False):
                template = templates.TemplateResponse(
                    "thread.html", {"request": "", **ts.todict()}
                ).body.decode()
                yield template
                thread_sent_dict[tid] = True
            else:
                yield innerHTML(f"t_{tid}_thread_name", ts.thread_name)
                yield innerHTML(f"t_{tid}_thread_state", ts.thread_state)
                yield innerHTML(
                    f"t_{tid}_thread_jvm_cpu_time",
                    f"{ts.thread_cpu_time} / {ts.jvm_cpu_time}",
                )
                yield innerHTML(f"t_{tid}_thread_memory_usage", ts.thread_memory_usage)
                yield innerHTML(
                    f"t_{tid}_thread_total_wait_time", ts.thread_total_wait_time
                )
                yield innerHTML(
                    f"t_{tid}_thread_total_block_time", ts.thread_total_block_time
                )

        thread_states.reset()
        await thread_states.lock.wait()
        if not run_active:
            end = True


async def dynamic_system_poll():
    dynamic_sent_dict: Dict[str, bool] = {}
    while not dynamic_sent_dict.get("dynamic", False) or run_active:
        dynamic = system_state.dynamic
        for member in members(dynamic, lambda _: False):
            yield innerHTML(f"dy_{member}", getattr(dynamic, member))
        dynamic_sent_dict["dynamic"] = True
        system_state.reset()
        await system_state.system["dynamic"].wait()


async def static_system_poll():
    static: Dict[str, bool] = {}
    while not static.get("static", False):
        for member in members(system_state, lambda attr: attr in ("system", "dynamic")):
            if (
                not static.get(member, False)
                and getattr(system_state, member) is not None
            ):
                yield innerHTML(member, getattr(system_state, member))
                static[member] = True
        static["static"] = True
        await system_state.system["static"].wait()


async def pipeline_stats_poll():
    pipeline: Dict[str, bool] = {}
    attrs = list(members(state, lambda attr: attr == "pipeline"))
    while not pipeline.get("pipeline", False):
        for member in attrs:
            if not pipeline.get(member, False) and getattr(state, member) is not None:
                yield innerHTML(f"pipeline_{member}", getattr(state, member))
                pipeline[member] = True

        if len(pipeline) >= len(attrs):
            pipeline["pipeline"] = True

        await sleep(1)


async def combined_poll():
    combine = stream.merge(
        static_system_poll(),
        dynamic_system_poll(),
        thread_stats_poll(),
        pipeline_stats_poll(),
        container_stats_poll(),
        document_poll(),
        document_measurements_poll(),
        graph_poll(),
    )

    async with combine.stream() as streamer:
        async for item in streamer:
            yield item


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    poll_gen: Callable[[], AsyncGenerator[Any, Any]] = combined_poll
    await websocket.accept()
    async for update in poll_gen():
        await websocket.send_text(update)


@app.get("/", response_class=HTMLResponse)
async def monitor():
    monitor_site: str
    with open("./index.html", "r", encoding="utf-8") as f:
        monitor_site = f.read()

    return monitor_site


# @app.get("/document_update")
# async def document_stream(request: Request):
#     global document_states

#     document_init: Dict[str, bool] = {}
#     measurement_init: Dict[str, bool] = {}

#     async def document_generator():
#         # If client was closed the connection

#         while True:
#             await sleep(2)

#             for did, ds in document_states.meta_states.items():

#                 if not document_init.get(did, False):
#                     template = templates.TemplateResponse("document.html", {"request": "", **ds.todict()}).body.decode()
#                     yield {
#                         "event": "document",
#                         "data": template
#                     }
#                     document_init[did] = True

#             await sleep(2)
#             for did, ds in document_states.document_states.items():
#                 if not measurement_init.get(did, False):
#                     template = templates.TemplateResponse("measurement.html", {"request": "", **ds.todict()}).body.decode()
#                     logging.info({
#                         "event": f"{ds.name}_measurement",
#                         "data": template
#                     })
#                     yield {
#                         "event": f"{ds.name}_measurement",
#                         "data": template
#                     }
#                 measurement_init[did] = True

#             await sleep(2)
#             pngs = list(document_states.graph_states.items())
#             for did, ds in pngs:
#                 yield {
#                     "event": f"{did}_graph",
#                     "data": ds
#                 }
#                 document_states.graph_states.pop(did)
#             document_states.reset_graph()
#             await sleep(1)

#     return EventSourceResponse(document_generator())

# @app.get("/docker_update")
# async def docker_stream(request: Request):


#     docker_init: Dict[str, bool] = {}

#     async def docker_generator():
#         # If client was closed the connection

#         while True:
#             await docker_states.lock.wait()
#             for did, ds in docker_states.docker_states.items():

#                 if not docker_init.get(did, False):
#                     template = templates.TemplateResponse("docker_container.html", {"request": "", **ds.todict()}).body.decode()
#                     logging.info("DOCKER UPDATE SENDING")
#                     logging.info(template)
#                     yield {
#                         "event": "docker",
#                         "data": template
#                     }
#                     docker_init[did] = True
#                 else:
#                     logging.info({"event": f"{did}-cpu_usage","data": ds.cpu_usage})
#                     logging.info({"event": f"{did}-network","data": f"{ds.network_i} / {ds.network_o}"})
#                     logging.info({"event": f"{did}-memory","data": f"{ds.memory_usage} / {ds.memory_limit}"})
#                     logging.info({"event": f"{did}-memory_max","data": ds.memory_max_usage})
#                     yield {"event": f"{did}-cpu_usage","data": ds.cpu_usage}
#                     yield {"event": f"{did}-network","data": f"{ds.network_i} / {ds.network_o}"}
#                     yield {"event": f"{did}-memory","data": f"{ds.memory_usage} / {ds.memory_limit}"}
#                     yield {"event": f"{did}-memory_max","data": ds.memory_max_usage}

#             docker_states.reset()


#     return EventSourceResponse(docker_generator())

# @app.get("/thread_update")
# async def thread_stream(request: Request):

#     thread_init: Dict[str, bool] = {}

#     async def thread_generator():
#         # If client was closed the connection

#         while True:
#             await thread_states.lock.wait()
#             for tid, ts in thread_states.thread_states.items():

#                 if not thread_init.get(tid, False):
#                     template = templates.TemplateResponse("thread.html", {"request": "", **ts.todict()}).body.decode()
#                     yield {
#                         "event": "thread",
#                         "data": template
#                     }
#                     thread_init[tid] = True
#                 else:
#                     yield {"event": f"{tid}-thread_jvm_cpu_time","data": f"{ts.thread_cpu_time} / {ts.jvm_cpu_time}"}
#                     yield {"event": f"{tid}-thread_memory_usage","data": f"{ts.thread_memory_usage}"}
#                     yield {"event": f"{tid}-thread_total_wait_time","data": f"{ts.thread_total_wait_time}"}
#                     yield {"event": f"{tid}-thread_total_block_time","data": f"{ts.thread_total_block_time}"}

#             thread_states.reset()
#             await sleep(1)


#     return EventSourceResponse(thread_generator())

# @app.get("/system_update")
# async def system_stream(request: Request):
#     global state, system_state, thread_states, docker_states, document_states

#     def members(obj, exclude: Callable[[str], bool]):
#         return (attr for attr in dir(obj) if not callable(getattr(obj, attr)) and not attr.startswith("__") and not exclude(attr))

#     async def system_generator():
#         # If client has closed the connection

#         count = 0
#         sent: Dict[str, bool] = {}
#         while count != 7:


#             if not sent.get("static", False):
#                 await sleep(1)
#                 for member in members(system_state, lambda attr: attr in ("system", "dynamic")):
#                     if  not sent.get(member, False) and getattr(system_state, member) is not None:
#                         yield {
#                                 "event": member,
#                                 "data": getattr(system_state, member)
#                         }
#                         count += 1; sent[member] = True
#                 sent["static"] = True

#             dynamic = system_state.dynamic
#             await sleep(1)
#             if not sent.get("dynamic", False):
#                 for member in members(dynamic, lambda _: False):
#                     yield {
#                             "event": f"dy_{member}",
#                             "data": getattr(dynamic, member)
#                     }

#                 # count += 1;

#             await sleep(1)


#     return EventSourceResponse(system_generator())

# @app.get("/pipeline_update")
# async def message_stream(request: Request):

#     def members(obj, exclude: Callable[[str], bool]):
#         return (attr for attr in dir(obj) if not callable(getattr(obj, attr)) and not attr.startswith("__") and not exclude(attr))


#     async def event_generator():
#         # If client was closed the connection

#         count = 0
#         sent: Dict[str, bool] = {}
#         while count != 5:
#             await sleep(1)
#             for member in members(state, lambda attr: attr == "pipeline"):
#                 await sleep(1)
#                 if not sent.get(member, False) and getattr(state, member) is not None:
#                     yield {
#                             "event": f"pipeline_{member}",
#                             "data": getattr(state, member)
#                     }
#                     count += 1;
#                     sent[member] = True

#             await sleep(1)


#     return EventSourceResponse(event_generator())


if __name__ == "__main__":
    uvicorn.run(
        "monitor:app", host="localhost", port=8086, log_level="debug", reload=True
    )


# {"os_version":"10.0","os_arch":"amd64","os_processors":8,"os_name":"Windows 10","os_jvm_vendor":"Oracle Corporation", "os_jvm_max_memory": 12412425352}{
