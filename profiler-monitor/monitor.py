from typing import Optional
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uvicorn as uvicorn
import queue as q
from asyncio import sleep
from copy  import deepcopy

class GraphUpdate(BaseModel):
    name: str
    engine_duration: str
    svg: str

class DocumentTable(BaseModel):
    name: str
    title: str
    component: str

class DocumentMeasurements(BaseModel):
    name: str
    component: str
    scale: Optional[str]
    urlwait: Optional[str]
    serialization: Optional[str]
    deserialization: Optional[str]
    annotator: Optional[str]
    total: Optional[str]

class PipelineMeasurements(BaseModel):
    name: Optional[str]
    workers: Optional[int]
    duration: Optional[str]
    document_count: Optional[int]

class DocumentTableHTML(BaseModel):
    name: str
    body: Optional[str]

class DocumentMeasurementHTML(BaseModel):
    name: str
    component: str
    measurement: str    

class GraphHTML(BaseModel):
    name: str
    body: str 

class Update(BaseModel):
    pipeline: Optional[str]
    document: Optional[str]
    document_table: Optional[str] 
    graph: Optional[str]

class Queues():
    updates: q.Queue[str]
    backup: q.Queue[str]

    def __init__(self, updates: q.Queue[str], backup: q.Queue[str]):
        self.updates = updates
        self.backup = backup

    def reset(self):
        self.updates = deepcopy(self.backup)
        self.backup = q.Queue()

queues = Queues(updates=q.Queue(), backup=q.Queue())

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

@app.post("/v1/document_table")
async def load_document_table(doc_measurements: DocumentTable): 


    doc_name = doc_measurements.name
    doc_title = doc_measurements.title if doc_measurements.title is not None else ""
    component= doc_measurements.component
    doc_html = DocumentTableHTML(name=doc_name)
    body = f"""
        <div class="col-xxl-6" id="{doc_name}">
          <div class="accordion accordion-flush" id="accordionFlush-{doc_name}">
            <div class="accordion-item rounded">
              <h4 class="accordion-header">
                <div
                  class="accordion-button collapsed rounded text-light bg-body-secondary mx-auto document-name"
                  data-bs-toggle="collapse"
                  data-bs-target="#flush-{doc_name}"
                  aria-expanded="false"
                  aria-controls="flush-{doc_name}">
                  {doc_name}
                </div>
              </h4>
              <div
                id="flush-{doc_name}"
                class="accordion-collapse collapse show"
                data-bs-parent="#accordionFlush-{doc_name}">
                <div class="card" aria-hidden="true">
                  <div class="card-body">
                    <h4 class="card-header">
                      {doc_name}
                    </h4>
                    <a class="card-title fw-lighter text-reset document-title">
                      {doc_title}
                    </a>
                    <div class="card-text col-xxl-5">
                      <table class="table table-hover">
                        <thead>
                          <tr>
                            <th scope="col">component</th>
                            <th scope="col">scale</th>
                            <th scope="col">url-wait</th>
                            <th scope="col">serialization</th>
                            <th scope="col">deserialization</th>
                            <th scope="col">annotator</th>
                            <th scope="col">total</th>
                          </tr>
                        </thead>
                        <tbody id="{doc_name}-document-table">
                        </tbody>
                      </table>
                    </div>
                    <div 
                      class="container-fluid img-thumbnail mx-auto card-img-bottom vstack"
                      id="{doc_name}-graph"
                    >

                    </div>
                  </div>
                </div>
            </div>
          </div>
        </div>
        </div>
    """
    doc_html.body = body.strip()

    queues.updates.put(Update(document=doc_html.json()).json())
    return 1

@app.post("/v1/document_measurement")
async def load_document_measurements(doc_measurements: DocumentMeasurements):

    doc_name = doc_measurements.name
    comp = doc_measurements.component
    
    comp_measurements = ""
    if doc_measurements.urlwait is not None:
        comp_measurements += f"<td>{doc_measurements.urlwait}</td>"
    if doc_measurements.serialization is not None:
        comp_measurements += f"<td>{doc_measurements.serialization}</td>"
    if doc_measurements.deserialization is not None:
        comp_measurements += f"<td>{doc_measurements.deserialization}</td>"
    if doc_measurements.annotator is not None:
        comp_measurements += f"<td>{doc_measurements.annotator}</td>"
    if doc_measurements.total is not None:
        comp_measurements += f"<td>{doc_measurements.total}</td>"

    measurement = f"""
      <tr>
        <td>{comp}</td>
        <td>{doc_measurements.scale}</td>
        {comp_measurements}
      </tr>
    """
    
    doc_html = DocumentMeasurementHTML(name=doc_name, component=comp, measurement=measurement.strip())

    queues.updates.put(Update(document_table=doc_html.json()).json())

    return 1 


@app.post("/v1/pipeline_measurement")
async def load_pipeline_measurements(pipe_measurements: PipelineMeasurements): 
    # pipeline_measurements_set = True
    # pipeline_measurements = pipe_measurements
    queues.updates.put(Update(pipeline=pipe_measurements.json()).json())
    return 1

@app.post("/v1/graph_update")
async def load_graph_update(graph_update: GraphUpdate): 

    body = f"""
        {graph_update.svg}
        <figcaption class="mx-auto figure-caption">Time to produce image: {graph_update.engine_duration}</figcaption>
    """
    queues.updates.put(Update(graph=GraphHTML(name=graph_update.name, body=body).json()).json())
    return 3


@app.get("/", response_class=HTMLResponse)
async def monitor(): 

    monitor_site: str
    with open('./index.html','r') as f:
        monitor_site = f.read()

    return monitor_site

@app.websocket("/v1/updates")
async def monitor_updates(websocket: WebSocket): 

    await websocket.accept()

    await websocket.send_text("READY.")

    try:
      while True:
          try:
              update = queues.updates.get(timeout=1)
              queues.backup.put(update)
              await websocket.send_text(update)
          except q.Empty:
              await sleep(1)
    except Exception as e:
        queues.reset()
        raise e
        

if __name__ == "__main__":
    uvicorn.run("monitor:app", host="localhost", port=8086, log_level='debug', reload=True)
