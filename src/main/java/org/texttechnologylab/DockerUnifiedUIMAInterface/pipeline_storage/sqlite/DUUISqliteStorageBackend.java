package org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelinePerformancePoint;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.IDUUIStorageBackend;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.*;
import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Do not use this class it is not finished and just a raw idea about how one could implement a postgres backend
 */
public class DUUISqliteStorageBackend implements IDUUIStorageBackend {
    private ConcurrentLinkedQueue<Connection> _client;
    private String _sqliteUrl;


    /**
     * Whether to track error documents in the database or not
     */
    private final boolean trackErrorDocs;

    /**
     * Should error documents be tracked in the database?
     * @return true if error documents should be tracked, false otherwise
     */
    @Override
    public boolean shouldTrackErrorDocs() {
        return trackErrorDocs;
    }

    public DUUISqliteStorageBackend(String sqliteurl) throws SQLException, IOException, InterruptedException {
        this(sqliteurl, true);
    }

    public DUUISqliteStorageBackend(String sqliteurl, boolean trackErrorDocs) throws IOException, SQLException, InterruptedException {
        this.trackErrorDocs = trackErrorDocs;

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
        stmt.execute("CREATE TABLE IF NOT EXISTS pipeline_document(documentSize INT, waitTime INT, totalTime INT, document TEXT)");
        stmt.execute("CREATE TABLE IF NOT EXISTS pipeline_document_perf(pipelinename TEXT, componenthash INT, durationSerialize INT,\n" +
                "durationDeserialize INT," +
                "durationAnnotator INT," +
                "durationMutexWait INT," +
                "durationComponentTotal INT,totalAnnotations INT, documentSize INT, serializedSize INT," +
                "error TEXT," +
                "document TEXT)");


        _client.add(conn);
    }

    public DUUISqliteStorageBackend withConnectionPoolSize(int poolsize) throws SQLException {
        for(int i = 1; i < poolsize; i++) {
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

        PreparedStatement dStmt = conn.prepareStatement("DELETE FROM pipeline WHERE name = ?");;
        dStmt.setString(1, name);
        dStmt.execute();

        dStmt = conn.prepareStatement("DELETE FROM pipeline_perf WHERE name = ?");
        dStmt.setString(1, name);
        dStmt.execute();
        //dStmt.executeUpdate();

        PreparedStatement cleanUp = conn.prepareStatement("DELETE FROM pipeline_document_perf where pipelinename = ?;");
        cleanUp.setString(1, name);
        cleanUp.execute();

        PreparedStatement cleanUp2 = conn.prepareStatement("DELETE FROM pipeline_component where name = ?;");
        cleanUp2.setString(1, name);
        cleanUp2.execute();


        PreparedStatement stmt = conn.prepareStatement("INSERT INTO pipeline (name,workers) VALUES (?,?)");
        stmt.setString(1,name);
        stmt.setLong(2,composer.getWorkerCount());
        stmt.executeUpdate();

        for(DUUIPipelineComponent comp : composer.getPipeline()) {
            String value = comp.toJson();

            // Note switched from "value.hashCode()" to the "finalized" hash to be able to match the value to table "pipeline_document_perf"
            long hash = comp.getFinalizedRepresentationHash();

            PreparedStatement stmt2 = conn.prepareStatement("INSERT INTO pipeline_component (hash,name,description) VALUES (?,?,?)");
            stmt2.setLong(1,hash);
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
            stmt = conn.prepareStatement("INSERT INTO pipeline_document(documentSize, waitTime, totalTime, document) VALUES (?,?,?,?)");
            stmt.setLong(1, perf.getDocumentSize());
            stmt.setLong(2, perf.getDocumentWaitTime());
            stmt.setLong(3, perf.getTotalTime());
            stmt.setString(4,perf.getDocument());
            stmt.executeUpdate();
        }
        catch(SQLException e) {
            e.printStackTrace();
        }

        for(DUUIPipelinePerformancePoint points : perf.getPerformancePoints()) {
            PreparedStatement stmt2 = null;
            try {
                stmt2 = conn.prepareStatement("INSERT INTO pipeline_document_perf(pipelinename,componenthash,durationSerialize,durationDeserialize,durationAnnotator,durationMutexWait,durationComponentTotal,totalAnnotations, documentSize, serializedSize, error,document) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");

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
                stmt2.setString(11,points.getError());
                stmt2.setString(12,points.getDocument());
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

}

