package org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite;

import static java.lang.String.format;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.Config;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver.DockerDriverView;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelinePerformancePoint;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.IDUUIStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.IDUUIResource.DockerContainerView;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.IDUUIResourceProfiler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.HostConfig;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.HostThreadView;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.HostUsage;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager.ResourceViews;

/**
 * Do not use this class it is not finished and just a raw idea about how one could implement a postgres backend
 */
public class DUUISqliteStorageBackend implements IDUUIStorageBackend, IDUUIResourceProfiler {
    private ConcurrentLinkedQueue<Connection> _client;
    private String _sqliteUrl;
    String name = null;

    public DUUISqliteStorageBackend(String sqliteurl) throws IOException, SQLException, InterruptedException {
        _client = null;
        _sqliteUrl = "jdbc:sqlite:"+sqliteurl;

        _client = null;
        try {
            _client = new ConcurrentLinkedQueue<>();
            _client.add(DriverManager.getConnection(_sqliteUrl));
            System.out.println("Connected to the Sqlite database successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        Connection conn = _client.poll();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE IF NOT EXISTS pipeline(name TEXT PRIMARY KEY, workers INT)");
        stmt.execute("CREATE TABLE IF NOT EXISTS pipeline_perf(name TEXT, startTime INT, endTime INT)");
        stmt.execute("CREATE TABLE IF NOT EXISTS pipeline_component(hash INT, name TEXT, description TEXT)");
        stmt.execute("CREATE TABLE IF NOT EXISTS pipeline_document(documentSize INT, waitTime INT, totalTime INT)");
        stmt.execute("CREATE TABLE IF NOT EXISTS pipeline_document_perf(pipelinename TEXT, componenthash INT, durationSerialize INT,\n" +
                "durationDeserialize INT," +
                "durationAnnotator INT," +
                "durationMutexWait INT," +
                "durationComponentTotal INT,totalAnnotations INT, documentSize INT, serializedSize INT)");

        stmt.execute(format("CREATE TABLE IF NOT EXISTS host_config(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
    "name TEXT PRIMARY KEY", "host_os TEXT", "jvm_vendor TEXT", "core_pool_size INT", 
            "max_pool_size INT", "cas_pool_size INT", "host_memory_size INT", "host_cpu_cores INT", "jvm_max_memory INT")
        );

        stmt.execute(format("CREATE TABLE IF NOT EXISTS cpu_usage(%s, %s, %s, %s, %s)",
        "timestamp INT", "name TEXT", "host_cpu_load REAL", "jvm_cpu_load REAL", "jvm_cpu_time INT")
        );

        stmt.execute(format("CREATE TABLE IF NOT EXISTS memory_usage(%s, %s, %s, %s, %s)",
            "timestamp INT", "name TEXT", "host_memory_usage INT", "jvm_memory_usage INT", "jvm_memory_total INT")
        );

        stmt.execute(format("CREATE TABLE IF NOT EXISTS thread_stats(%s, %s, %s, %s, %s, %s, %s, %s)",
            "timestamp INT", "name TEXT", "thread_name TEXT", "state TEXT", "waited_time INT", "blocked_time INT",
            "cpu_time INT", "cumulated_memory INT")
        );

        stmt.execute(format("CREATE TABLE IF NOT EXISTS container_stats(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
    "timestamp INT", "name TEXT", "image TEXT", "container_id TEXT", "state TEXT", "memory_usage INT", 
            "memory_peak_usage INT", "cpu_usage REAL", "network_in INT", "network_out INT")
        );


        _client.add(conn);
    }

    public DUUISqliteStorageBackend withConnectionPoolSize(int poolsize) throws SQLException {
        for(int i = 1; i <= poolsize; i++) {
            _client = new ConcurrentLinkedQueue<>();
            _client.add(DriverManager.getConnection(_sqliteUrl));
            System.out.printf("[DUUISqliteStorageBackend] Populated connection pool %d/%d\n",i,poolsize);
        }
        return this;
    }

    public void shutdown() throws UnknownHostException {
        System.out.printf("[DUUISqliteStorageBackend] Shutting down.\n");
    }

    public void addNewRun(String name, DUUIComposer composer) throws SQLException {
        Connection conn = null;
        while(conn == null) {
            conn = _client.poll();
        }

        this.name = name; 

        PreparedStatement dStmt = conn.prepareStatement("DELETE FROM pipeline WHERE name = ?");
        dStmt.setString(1, name);
        dStmt.execute();

        dStmt = conn.prepareStatement("DELETE FROM pipeline_perf WHERE name = ?");
        dStmt.setString(1, name);
        dStmt.execute();
        //dStmt.executeUpdate();

        PreparedStatement cleanUp = conn.prepareStatement(format("DELETE FROM pipeline_document_perf where pipelinename LIKE '%s%%*';", name));
        // cleanUp.setString(1, name);
        cleanUp.execute();

        PreparedStatement cleanUp2 = conn.prepareStatement("DELETE FROM pipeline_component where name = ?;");
        cleanUp2.setString(1, name);
        cleanUp2.execute();

        // Reset Resource Usage Data
        final Iterable<String> tables = () -> Arrays
            .asList("host_config", "cpu_usage", "memory_usage", "thread_stats", "container_stats").iterator();
        for (String table : tables) {
            PreparedStatement cleanUp3 = conn.prepareStatement(format("DELETE FROM %s WHERE name = ?;", table));
            cleanUp3.setString(1, name);
            cleanUp3.execute();
        }

        PreparedStatement stmt = conn.prepareStatement("INSERT INTO pipeline (name,workers) VALUES (?,?)");
        stmt.setString(1,name);
        stmt.setLong(2,composer.getWorkerCount());
        stmt.executeUpdate();

        for(DUUIPipelineComponent comp : composer.getPipeline()) {
            String value = comp.toJson();
            PreparedStatement stmt2 = conn.prepareStatement("INSERT INTO pipeline_component (hash,name,description) VALUES (?,?,?)");
            stmt2.setLong(1,value.hashCode());
            stmt2.setString(2,name);
            stmt2.setString(3,value);
            stmt2.executeUpdate();
        }
        _client.add(conn);
    }

    @Override
    public void addMetricsForDocument(DUUIPipelineDocumentPerformance perf) {
        Connection conn = null;
        while(conn == null) {
            conn = _client.poll();
        }
        try {
            PreparedStatement stmt = null;
            stmt = conn.prepareStatement("INSERT INTO pipeline_document(documentSize, waitTime, totalTime) VALUES (?,?,?)");
            stmt.setLong(1, perf.getDocumentSize());
            stmt.setLong(2, perf.getDocumentWaitTime());
            stmt.setLong(3, perf.getTotalTime());
            stmt.executeUpdate();
        }
        catch(SQLException e) {
            e.printStackTrace();
        }

        for(DUUIPipelinePerformancePoint points : perf.getPerformancePoints()) {
            PreparedStatement stmt2 = null;
            try {
                stmt2 = conn.prepareStatement("INSERT INTO pipeline_document_perf(pipelinename,componenthash,durationSerialize,durationDeserialize,durationAnnotator,durationMutexWait,durationComponentTotal,totalAnnotations, documentSize, serializedSize) VALUES (?,?,?,?,?,?,?,?,?,?)");

                stmt2.setString(1,perf.getRunKey());
                stmt2.setLong(2,Long.parseLong(points.getKey()));
                stmt2.setLong(3,points.getDurationSerialize());
                stmt2.setLong(4,points.getDurationDeserialize());
                stmt2.setLong(5,points.getDurationAnnotator());
                stmt2.setLong(6,points.getDurationMutexWait());
                stmt2.setLong(7,points.getDurationComponentTotal());
                stmt2.setLong(8,points.getNumberOfAnnotations());
                stmt2.setLong(9,points.getDocumentSize());
                stmt2.setLong(10,points.getSerializedSize());
                stmt2.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        _client.add(conn);
    }

    public IDUUIPipelineComponent loadComponent(String id) {
        return new IDUUIPipelineComponent();
    }
    
    public void finalizeRun(String name, Instant start, Instant end) throws SQLException {
        Connection conn = null;
        while(conn == null) {
            conn = _client.poll();
        }
        PreparedStatement stmt2 = conn.prepareStatement("INSERT INTO pipeline_perf(name,startTime,endTime) VALUES (?,?,?)");
        stmt2.setString(1,name);
        stmt2.setLong(2,start.toEpochMilli());
        stmt2.setLong(3,end.toEpochMilli());
        stmt2.executeUpdate();
        _client.add(conn);
    }

    @Override
    public void addMeasurements(ResourceViews views, boolean pipelineStarted) {
        Connection conn = null;
        while(conn == null) {
            conn = _client.poll();
        }
        try {
            
            PreparedStatement stmt = null;
            // Config INSERT
            if (pipelineStarted) {
                HostConfig config = views.getHostConfig();
                stmt = conn.prepareStatement("INSERT INTO host_config VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
                stmt.setString(1, this.name);
                stmt.setString(2, config.getOSName());
                stmt.setString(3, config.getJVMVendor());
                stmt.setInt(4, Config.strategy().getCorePoolSize());
                stmt.setInt(5, Config.strategy().getMaxPoolSize());
                stmt.setInt(6, config.getCASPoolSize());
                stmt.setLong(7, config.getHostMemoryTotal());
                stmt.setInt(8, config.getAvailableProcessors());
                stmt.setLong(9, config.getJVMMaxMemory());
                stmt.executeUpdate();
            }
    
            long time = System.nanoTime();
            HostUsage usage = views.getHostUsage();
    
            // CPU INSERT 
            stmt = conn.prepareStatement("INSERT INTO cpu_usage VALUES (?, ?, ?, ?, ?)");
            stmt.setLong(1, time);
            stmt.setString(2, this.name);
            stmt.setDouble(3, usage.getSystemCpuLoad());
            stmt.setDouble(4, usage.getJvmCpuLoad());
            stmt.setLong(5, usage.getJvmCpuTime());
            stmt.executeUpdate();
    
            // MEMORY INSERT 
            stmt = conn.prepareStatement("INSERT INTO memory_usage VALUES (?, ?, ?, ?, ?)");
            stmt.setLong(1, time);
            stmt.setString(2, this.name);
            stmt.setLong(3, usage.getHostMemoryUsage());
            stmt.setLong(4, usage.getHeapMemoryUsage());
            stmt.setLong(5, usage.getHeapMemoryTotal());
            stmt.executeUpdate();
    
            // Thread INSERT 
            for (HostThreadView threadview : usage.getThreadViews()) {
                stmt = conn.prepareStatement("INSERT INTO thread_stats VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
                stmt.setLong(1, time);
                stmt.setString(2, this.name);
                stmt.setString(3, threadview.getName());
                stmt.setString(4, threadview.getState());
                stmt.setLong(5, threadview.getWaitedTime());
                stmt.setLong(6, threadview.getBlockedTime());
                stmt.setLong(7, threadview.getCpuTime());
                stmt.setLong(8, threadview.getMemoryUsage());
                stmt.executeUpdate();
            }
    
            // Container INSERT
            DockerDriverView driver = (DockerDriverView) views.getDockerDriverView();
            if (driver == null) return;
            if (driver.getContainerViews().isEmpty()) return;
            for (DockerContainerView containerview : driver.getContainerViews()) {
                stmt = conn.prepareStatement("INSERT INTO container_stats VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                stmt.setLong(1, time);
                stmt.setString(2, this.name);
                stmt.setString(3, containerview.getImage());
                stmt.setString(4, containerview.getContainerId());
                stmt.setString(5, containerview.getState());
                stmt.setLong(6, containerview.getMemoryUsage());
                stmt.setLong(7, containerview.getPeakMemoryUsage());
                stmt.setDouble(8, containerview.getCpuUsage());
                stmt.setLong(9, containerview.getNetworkIn());
                stmt.setLong(10, containerview.getNetworkOut());
                stmt.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            _client.add(conn);
        }
        

    }

}

