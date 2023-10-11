package org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.postgres;

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
 * Do not use this class it is not finished and just a raw idea about how one could implement a postgres backend
 */
@Deprecated
public class DUUIPostgresSQLStorageBackend implements IDUUIStorageBackend {

    private String _url;
    private String _password;
    private String _user;
    private int _port;
    private DUUIDockerInterface _docker;
    private boolean _store_performance_metrics;
    private Connection _client;
    private


    DUUIPostgresSQLStorageBackend(String user, String password, int port) throws IOException, SQLException, InterruptedException {
        _password = password;
        _client = null;
        _docker = new DUUIDockerInterface();
        _port = port;
        _user = user;
        _store_performance_metrics = false;
        setup();
        _url = "jdbc:postgresql://localhost:"+_port+"/duui";

        _client = null;
        try {
            _client = DriverManager.getConnection(_url, _user, password);
            System.out.println("Connected to the PostgreSQL server successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        Statement stmt = _client.createStatement();
        stmt.execute("CREATE TABLE IF NOT EXISTS pipeline(name TEXT PRIMARY KEY, workers INT)");
        stmt.execute("CREATE TABLE IF NOT EXISTS pipeline_component(hash INT PRIMARY KEY, name TEXT, description JSONB");
        stmt.execute("CREATE TABLE IF NOT EXISTS pipeline_run(id SERIAL PRIMARY KEY, pipelinename TEXT REFERENCES pipeline(name), " +
                "pipelinecomp INT REFERENCES(hash), pipelineposition INT)");


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
    }

    @Override
    public boolean shouldTrackErrorDocs() {
        System.err.println("WARNING: Postgres storage backend does not support error document tracking!");
        return false;
    }

    public void addNewRun(String name, DUUIComposer composer) throws SQLException {
    }

    @Override
    public void addMetricsForDocument(DUUIPipelineDocumentPerformance perf) {
    }

    public IDUUIPipelineComponent loadComponent(String id) {
        return new IDUUIPipelineComponent();
    }
    public void finalizeRun(String name, Instant start, Instant end) throws SQLException {
    }

}

