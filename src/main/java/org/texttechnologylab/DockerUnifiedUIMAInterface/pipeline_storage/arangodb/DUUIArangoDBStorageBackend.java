package org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.arangodb;

import com.arangodb.*;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.CollectionType;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.mapping.ArangoJack;
import com.arangodb.model.CollectionCreateOptions;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ports;
import org.apache.uima.jcas.JCas;
import org.json.JSONObject;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.IDUUIStorageBackend;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.time.Instant;
import java.util.*;

import static java.lang.String.format;

/**
 * Is at the moment not at the newest version, therefore you are advised to not use it until it is up to date
 * again
 */
@Deprecated
public class DUUIArangoDBStorageBackend implements IDUUIStorageBackend {
    private String _password;
    private int _port;
    private DUUIDockerInterface _docker;
    private boolean _store_performance_metrics;
    private ArangoDB _client;
    private ArangoDatabase _db;
    private ArangoCollection _pipelineCollection;
    private ArangoCollection _pipelineComponentCollection;
    private ArangoCollection _pipelineComponentEdge;

    private ArangoCollection _pipelineComponentDocumentPerformance;
    private ArangoCollection _pipelineDocumentPerformance;


    public DUUIArangoDBStorageBackend(String password, int port) throws IOException, SQLException, InterruptedException {
        _password = password;
        _client = null;
        _docker = new DUUIDockerInterface();
        _port = port;
        _store_performance_metrics = false;
        setup();
        _client = new ArangoDB.Builder()
                .serializer(new ArangoJack())
                .password(password)
                .host("127.0.0.1",_port)
                .useSsl(false)
                .build();
        _db = _client.db(DbName.of("DUUIDatabase"));
        if(!_db.exists()) {
            System.out.println("[DUUIArangoDBStorageBackend] Creating database DUUIDatabase...");
            _db.create();
        }

        _pipelineCollection = _db.collection("pipeline");
        if(!_pipelineCollection.exists()) {
            System.out.println("[DUUIArangoDBStorageBackend] Creating collection pipeline...");
            _pipelineCollection.create();
        }

        _pipelineComponentCollection = _db.collection("pipeline_component");
        if(!_pipelineComponentCollection.exists()) {
            System.out.println("[DUUIArangoDBStorageBackend] Creating collection pipeline_component...");
            _pipelineComponentCollection.create();
        }

        _pipelineDocumentPerformance = _db.collection("pipeline_document_performance");
        if(!_pipelineDocumentPerformance.exists()) {
            System.out.println("[DUUIArangoDBStorageBackend] Creating collection pipeline_document_performance...");
            _pipelineDocumentPerformance.create();
        }

        _pipelineComponentDocumentPerformance = _db.collection("pipeline_component_document_performance");
        if(!_pipelineComponentDocumentPerformance.exists()) {
            System.out.println("[DUUIArangoDBStorageBackend] Creating edge collection pipeline_component_document_performance_edges...");
            _pipelineComponentDocumentPerformance.create();
        }

        _pipelineComponentEdge = _db.collection("pipeline_component_edges");
        if(!_pipelineComponentEdge.exists()) {
            _db.createCollection("pipeline_component_edges", new CollectionCreateOptions().type(CollectionType.EDGES));
            _pipelineComponentEdge = _db.collection("pipeline_component_edges");
            System.out.println("[DUUIArangoDBStorageBackend] Creating edge collection pipeline_component_edge...");
        }
        if(!_db.graph("pipelines").exists()) {
            List<EdgeDefinition> lst = new LinkedList<>();
            lst.add(new EdgeDefinition().collection("pipeline_component_edges").from("pipeline", "pipeline_component").to("pipeline_component"));
            _db.createGraph("pipelines", lst);
        }
    }

    public String generateURL() throws UnknownHostException {
        if(_port==-1) {
            return null;
        }
        InetAddress IP = InetAddress.getLocalHost();
        return format("http://%s:%d",IP.getHostAddress().toString(),_port);
    }

    public void setup() throws InterruptedException, UnknownHostException, SQLException {
            try {
                InspectContainerResponse result = _docker.getDockerClient().inspectContainerCmd("duui_arangodb_backend").exec();
                int mapping = _docker.extract_port_mapping(result.getId(),8529);
                if(result.getState().getRunning()) {
                    System.out.println("[DUUIArangoDBStorageBackend] Found existing arangodb backend container which is running already.");
                    System.out.printf("[DUUIArangoDBStorageBackend] Adjusting exposed port %d to running container port %d\n",_port,mapping);
                    _port = mapping;
                }
                else {

                    System.out.println("[DUUIArangoDBStorageBackend] Found existing arangodb backend container starting it now...");
                    _docker.getDockerClient().startContainerCmd(result.getId()).exec();
                    _port = _docker.extract_port_mapping(result.getId(), 8529);
                    System.out.printf("[DUUIArangoDBStorageBackend] Arangodb is opened at %s\n", generateURL());
                    Thread.sleep(2000);
                }
            }
            catch(Exception e) {
                _docker.pullImage("arangodb:3.9",null,null);
                System.out.println("[DUUIArangoDBStorageBackend] Could not find existing container creating one...");
                ExposedPort tcp8529 = ExposedPort.tcp(8529);

                Ports portBindings = new Ports();
                portBindings.bind(tcp8529, Ports.Binding.bindPort(_port));


                CreateContainerResponse container = _docker.getDockerClient().createContainerCmd("arangodb:3.9")
                        .withExposedPorts(tcp8529)
                        .withEnv("ARANGO_ROOT_PASSWORD="+_password)
                        .withHostConfig(new HostConfig()
                                .withPortBindings(portBindings)
                                .withExtraHosts("host.docker.internal:host-gateway"))
                        .withName("duui_arangodb_backend")
                        .exec();

                _docker.getDockerClient().startContainerCmd(container.getId()).exec();
                System.out.printf("[DUUIArangoDBStorageBackend] Arangodb is opened at %s\n",generateURL());
                Thread.sleep(2000);
            }
    }

    public void shutdown() throws UnknownHostException {
        System.out.printf("[DUUIArangoDBStorageBackend] To inspect the metrics visit ArangoDB at %s\n",generateURL());
        _client.shutdown();
    }

    @Override
    public boolean shouldTrackErrorDocs() {
        System.err.println("WARNING: Arango storage backend does not support error document tracking!");
        return false;
    }

    public void addNewRun(String name, DUUIComposer composer) throws SQLException {
        Vector<String> stringvec = new Vector<>();
        stringvec.add(_pipelineCollection.insertDocument(new DUUIArangoComposerConfiguration(name, composer.getWorkerCount())).getId());
        for (DUUIPipelineComponent comp : composer.getPipeline()) {
            //JSONObject obj = new JSONObject(comp.getOptions());
            JSONObject obj = new JSONObject();
            String key = String.valueOf(obj.toString().hashCode());

            BaseDocument base = _pipelineComponentCollection.getDocument(key, BaseDocument.class);
            if(base!=null) {
                System.out.printf("[DUUIArangoDBStorageBackend] Found existing pipeline component. Using existing component.\n");
                stringvec.add(base.getId());
              //  comp.setOption(DUUIComposer.COMPONENT_COMPONENT_UNIQUE_KEY,stringvec.lastElement());
            }
            else {
                Map<String,Object> map = obj.toMap();
                String named = comp.getName();
                if(named == null) {
                    map.put("name", comp.getClass().getName());
                }
                else {
                    map.put("name", named);
                }
                stringvec.add(_pipelineComponentCollection.insertDocument(new DUUIArangoPipelineComponent(key, map)).getId());
              //  comp.setOption(DUUIComposer.COMPONENT_COMPONENT_UNIQUE_KEY,stringvec.lastElement());
            }
        }

        for (int i = 0; i < (stringvec.size()-1); i++) {
            _pipelineComponentEdge.insertDocument(new DUUIArangoPipelineEdge(stringvec.get(i),stringvec.get(i+1),name).edge());
        }
    }

    @Override
    public void addMetricsForDocument(DUUIPipelineDocumentPerformance perf) {
        String key = _pipelineDocumentPerformance.insertDocument(perf.toArangoDocument()).getId();
        for(BaseDocument x : perf.generateComponentPerformance(key)) {
            _pipelineComponentDocumentPerformance.insertDocument(x);
        }
    }

    public IDUUIPipelineComponent loadComponent(String id) {
        BaseDocument docs = _pipelineComponentCollection.getDocument(id,BaseDocument.class);
        return new IDUUIPipelineComponent(docs.getProperties());
    }
    public void finalizeRun(String name, Instant start, Instant end) throws SQLException {
        BaseDocument doc = _pipelineCollection.getDocument(name,BaseDocument.class);
        Map<String,Object> total = doc.getProperties();
        total.put("starttime",start.toEpochMilli());
        total.put("endtime",end.toEpochMilli());
        _pipelineCollection.updateDocument(name,total);
    }

}
