package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ports;
import com.influxdb.client.*;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.CreateDashboardRequest;
import com.influxdb.client.domain.Organization;
import com.influxdb.client.write.Point;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIDockerInterface;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;

import static java.lang.String.format;

public class DUUIMonitor {
    private InfluxDBClient _client;
    private String _username;
    private String _password;
    private String _url;
    private int _port;
    private DUUIDockerInterface _docker;
    private WriteApi _writeAPI;
    private Duration _writeInterval;

    private static char[] token = "specialtoken".toCharArray();
    private static String org = "texttechnologylab";
    private static String bucket_pipeline = "duui_pipeline";
    private static String bucket_logs = "duui_logs";

    public DUUIMonitor(String username, String password, int port) throws IOException {
        _username = username;
        _password = password;
        _client = null;
        _docker = new DUUIDockerInterface();
        _port = port;
        _writeInterval = Duration.ofSeconds(10);
    }

    public DUUIMonitor(String url, String username, String password) {
        _url = url;
        _username = username;
        _password = password;
        _docker = null;
        _client = null;
        _port = -1;
        _writeInterval = Duration.ofSeconds(10);
    }

    public DUUIMonitor withWriteInterval(Duration duration) {
        _writeInterval = duration;
        return this;
    }

    public String generateURL() throws UnknownHostException {
        if(_url!=null) {
            return _url;
        }
        if(_port==-1) {
            return null;
        }
        InetAddress IP = InetAddress.getLocalHost();
        return format("http://%s:%d",IP.getHostAddress().toString(),_port);
    }

    public DUUIMonitor setup() throws InterruptedException, UnknownHostException {
        if(_url != null) {
            _client = InfluxDBClientFactory.create(_url, token, org, bucket_pipeline);
            _writeAPI = _client.makeWriteApi();
            return this;
        }
        else {
            try {
                InspectContainerResponse result = _docker.getDockerClient().inspectContainerCmd("duui_influx_backend").exec();
                int mapping = _docker.extract_port_mapping(result.getId(),8086);
                if(result.getState().getRunning()) {
                    System.out.println("[DUUIMonitor] Found existing influx backend container which is running already.");
                    System.out.printf("[DUUIMonitor] Adjusting exposed port %d to running container port %d\n",_port,mapping);
                    _port = mapping;
                }
                else {

                    System.out.println("[DUUIMonitor] Found existing influx backend container starting it now...");
                    _docker.getDockerClient().startContainerCmd(result.getId()).exec();
                    _port = _docker.extract_port_mapping(result.getId(), 8086);
                    System.out.printf("[DUUIMonitor] InfluxDB panel is opened at %s\n", generateURL());
                    Thread.sleep(2000);
                }
            }
            catch(Exception e) {
                _docker.pullImage("influxdb:alpine",null,null);
                System.out.println("[DUUIMonitor] Could not find existing container creating one...");
                ExposedPort tcp8086 = ExposedPort.tcp(8086);

                Ports portBindings = new Ports();
                portBindings.bind(tcp8086, Ports.Binding.bindPort(_port));


                CreateContainerResponse container = _docker.getDockerClient().createContainerCmd("influxdb:alpine")
                        .withExposedPorts(tcp8086)
                        .withEnv("DOCKER_INFLUXDB_INIT_MODE=setup",
                                "DOCKER_INFLUXDB_INIT_USERNAME="+_username,
                                "DOCKER_INFLUXDB_INIT_PASSWORD="+_password,
                                "DOCKER_INFLUXDB_INIT_ORG="+org,
                                "DOCKER_INFLUXDB_INIT_TOKEN="+"cooltoken",
                                "DOCKER_INFLUXDB_INIT_BUCKET="+bucket_pipeline)
                        .withHostConfig(new HostConfig()
                                .withPortBindings(portBindings)
                                .withExtraHosts("host.docker.internal:host-gateway"))
                        .withName("duui_influx_backend")
                        .exec();

                _docker.getDockerClient().startContainerCmd(container.getId()).exec();
                System.out.printf("[DUUIMonitor] InfluxDB panel is opened at %s\n",generateURL());
                Thread.sleep(2000);
            }
        }
        _client = InfluxDBClientFactory.create(generateURL(), _username,_password.toCharArray());
        _writeAPI = _client.makeWriteApi();
        return this;
    }

    public void addDatapoint(Point point) {
        _writeAPI.writePoint(bucket_pipeline,org,point);
    }

    public void shutdown() {
        _writeAPI.close();
        _client.close();
    }
}
