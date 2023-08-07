from typing import Optional, List, Dict
from pydantic import BaseModel
from util import CountDownLatch, CountDownLatch2

class Status(BaseModel):
    status: str 
    message: str


class GraphUpdate(BaseModel):
    name: str
    engine_duration: str
    png: str

class DocumentMetaData(BaseModel):
    name: str
    title: str
    # bytes
    initial_size: int

class DocumentMeasurements(BaseModel):
    name: str
    component: str
    scale: int
    # seconds
    urlwait: int 
    serialization: int 
    deserialization: int 
    annotator: int 
    component_total: int
    parent_wait: int 
    worker_total: int
    # bytes
    document_size: int

class PipelineMeasurements(BaseModel):
    name: Optional[str] = None 
    workers: Optional[int] = None
    graph: Optional[str] = None
    duration: Optional[str] = None # seconds 
    document_count: Optional[int] = None 

class ContainerStats(BaseModel):
    container_id: str
    status: str
    image_id: str
    cpu_usage: float = -0.0
    memory_usage: int = -0
    memory_max_usage: int = 0
    memory_limit: int = 0
    num_procs: int = 0
    network_i: int = 0
    network_o: int = 0

class SystemResources(BaseModel):
    monitor: Optional[ContainerStats] = None
    docker_driver: Optional[List[ContainerStats]] = []

class SystemStatic(BaseModel):
    os_processors: int
    os_version: str
    os_name: str
    os_arch: str
    os_jvm_vendor: str
    os_jvm_max_memory: int 

class ThreadInfo(BaseModel):
    thread_id: int
    thread_name: str
    thread_state: str 
    thread_total_wait_time: int # milliseconds
    thread_total_block_time: int # milliseconds
    thread_memory_usage: int 
    thread_cpu_time: int # nanoseconds
    jvm_cpu_time: int 

class SystemDynamic(BaseModel):
    cpu_load_average: float
    cpu_jvm_load: float 
    cpu_system_load: float
    thread_stats: List[ThreadInfo]
    # memory
    system_memory_total: int 
    system_memory_used: int 
    memory_heap_used: int 
    memory_heap_committed: int
    memory_non_heap_used: int
    memory_non_heap_committed: int 
    memory_used: int 
    memory_total: int


class DocumentMetaState():
    __slots__ = ("name", 
                 "title",
                 "initial_size")
    name: str
    title: str
    initial_size: str

    def todict(self): 
        return {
            "name": self.name,
            "title": self.title,
            "initial_size": self.initial_size
        }

class DocumentState():
    __slots__ = ("name", 
                 "component",
                 "scale", 
                 "urlwait", 
                 "serialization",
                 "deserialization",
                 "annotator", 
                 "parent_wait", 
                 "worker_total", 
                 "document_size", 
                 "component_total")
    name: str
    component: str
    scale: str
    # seconds
    urlwait: str 
    serialization: str 
    deserialization: str 
    annotator: str 
    component_total: str
    parent_wait: str 
    worker_total: str
    # bytes
    document_size: str

    def todict(self): 
        return {
            "name": self.name,
            "component": self.component,
            "scale": self.scale, 
            "urlwait": self.urlwait,
            "serialization": self.serialization,
            "deserialization": self.deserialization, 
            "annotator": self.annotator,
            "component_total": self.component_total,
            "parent_wait": self.parent_wait,
            "worker_total": self.worker_total,
            "document_size": self.document_size
        }

class DocumentStates():
    __slots__=("document_states", "meta_states", "graph_states", "docs", "meta", "graph")
    document_states: Dict[str, DocumentState]
    meta_states: Dict[str, DocumentMetaState]
    graph_states: Dict[str, str]

    def __init__(self) -> None:
        self.document_states = {}
        self.meta_states = {}
        self.graph_states = {}
        self.docs = CountDownLatch(1)
        self.meta = CountDownLatch(1)
        self.graph = CountDownLatch(1)
    
    def reset_graph(self):
        self.graph.count = 1
    
    def reset_meta(self):
        self.meta.count = 1

    def reset_doc(self):
        self.docs.count = 1

class DockerState():
    __slots__ = ("container_id", 
                 "image_id",
                 "state",
                 "cpu_usage", 
                 "memory_usage", 
                 "memory_max_usage",
                 "memory_limit",
                 "network_i", 
                 "network_o")
    container_id: str
    image_id: str
    state: str 
    cpu_usage: str 
    memory_usage: str
    memory_max_usage: str
    memory_limit: str 
    network_i: str
    network_o: str 

    def todict(self): 
        return {
            "container_id": self.container_id,
            "image_id": self.image_id,
            "cpu_usage": self.cpu_usage, 
            "memory_usage": self.memory_usage,
            "memory_max_usage": self.memory_max_usage,
            "memory_limit": self.memory_limit, 
            "network_i": self.network_i,
            "network_o": self.network_o
    
        }

class ThreadState():
    __slots__ = ("thread_id", 
                 "thread_name",
                 "thread_state", 
                 "thread_total_wait_time", 
                 "thread_total_block_time",
                 "thread_memory_usage",
                 "thread_cpu_time", 
                 "jvm_cpu_time")
    thread_id: str
    thread_name: str
    thread_state: str 
    thread_total_wait_time: str # milliseconds
    thread_total_block_time: str # milliseconds
    thread_memory_usage: str 
    thread_cpu_time: str # nanoseconds
    jvm_cpu_time: str 

    def todict(self): 
        return {
            "thread_id": self.thread_id,
            "thread_name": self.thread_name,
            "thread_state": self.thread_state, 
            "thread_total_wait_time": self.thread_total_wait_time, # milliseconds
            "thread_total_block_time": self.thread_total_block_time, # milliseconds
            "thread_memory_usage": self.thread_memory_usage, 
            "thread_cpu_time": self.thread_cpu_time, # nanoseconds
            "jvm_cpu_time": self.jvm_cpu_time 
    
        }

class DockerStates():
    __slots__=("docker_states", "lock")
    docker_states: Dict[str, DockerState]
    lock: CountDownLatch

    def __init__(self) -> None:
        self.docker_states = {}
        self.lock = CountDownLatch(1)
    
    def reset(self):
        self.lock.count = 1

class ThreadStates():
    __slots__=("threads", "lock")
    threads: Dict[str, ThreadState]
    lock: CountDownLatch

    def __init__(self) -> None:
        self.threads = {}
        self.lock = CountDownLatch(1)
    
    def reset(self):
        self.lock.count = 1

class SystemDynamicState():
    __slots__ = ("cpu_load_average",
                 "cpu_jvm_load", 
                 "cpu_system_load", 
                 "system_memory_total", 
                 "system_memory_used", 
                 "memory_heap_used", 
                 "memory_heap_committed",
                 "memory_non_heap_used",
                 "memory_non_heap_committed",
                 "memory_used",
                 "memory_total")
    # dunno
    cpu_load_average: Optional[str]
    # percentage
    cpu_jvm_load: Optional[str]
    cpu_system_load: Optional[str]
    # bytes
    system_memory_total: Optional[str]
    system_memory_used: Optional[str]
    memory_heap_used: Optional[str]
    memory_heap_committed: Optional[str]
    memory_non_heap_used: Optional[str]
    memory_non_heap_committed: Optional[str]
    memory_used: Optional[str]
    memory_total: Optional[str]

class SystemState():
    __slots__ = ("os_name",
                 "os_version", 
                 "os_processors", 
                 "os_arch", 
                 "os_jvm_vendor", 
                 "os_jvm_max_memory", 
                 "dynamic",
                 "system")
    
    os_processors: Optional[int]
    os_version: Optional[str]
    os_name: Optional[str]
    os_arch: Optional[str]
    os_jvm_vendor: Optional[str]
    os_jvm_max_memory: Optional[str] 
    dynamic: Optional[SystemDynamicState] 

    def __init__(self) -> None:
        self.os_processors = None
        self.os_version = None
        self.os_name = None
        self.os_arch = None
        self.os_jvm_vendor = None
        self.os_jvm_max_memory = None
        self.dynamic = None 
        self.system = {
            "static": CountDownLatch(1),
            "dynamic": CountDownLatch(1)        
        }

    def reset(self):
        self.system = {
            "static": CountDownLatch(1), 
            "dynamic": CountDownLatch(1)        
        }

class PipelineState():
    __slots__ = ("name", "workers", "graph", "duration", "document_count", "pipeline")
    name: Optional[str]
    workers: Optional[int]
    graph: Optional[str]
    duration: Optional[str] # seconds 
    document_count: Optional[int]
    pipeline: Dict[str, CountDownLatch2]
    # document_measurements: q.Queue[DocumentMeasurements]

    def __init__(self) -> None:
        self.name = None
        self.workers = None
        self.graph = None 
        self.duration = None 
        self.document_count = None 
        self.pipeline = {
            "name": CountDownLatch2(1),
            "workers": CountDownLatch2(1),
            "graph": CountDownLatch2(1),
            "document_count": CountDownLatch2(1),
            "duration": CountDownLatch2(1)
        }

    def reset(self):
        self.pipeline = {
            "name": CountDownLatch2(1),
            "workers": CountDownLatch2(1),
            "graph": CountDownLatch2(1),
            "document_count": CountDownLatch2(1),
            "duration": CountDownLatch2(1)
        }