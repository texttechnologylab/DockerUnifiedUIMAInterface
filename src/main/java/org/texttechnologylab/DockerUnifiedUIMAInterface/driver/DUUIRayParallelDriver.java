package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.IDUUIConnectionHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.CommunicationLayerException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.exception.PipelineComponentException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaCommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.fit.factory.JCasFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Driver for DUUI components that use Ray for internal parallelism
 *
 * Starts a Ray cluster (head + N workers), submits the FastAPI script as a Ray job,
 * and cleans everything up on shutdown. Can also connect to an existing cluster
 * if URL is given
 *
 * @author Daniel Bundan
 */
public class DUUIRayParallelDriver implements IDUUIDriverInterface {

    // These keys are stripped before passing params to Lua
    // num_workers is kept out of this set on purpose: Lua needs it for the request body
    private static final Set<String> RAY_PARALLEL_INTERNAL_KEYS = Set.of(
            "ray_parallel_component",
            "cpus_per_worker",
            "gpus_per_worker",
            "head_node_port",
            "dashboard_port",
            "processing_timeout",
            "finalize_timeout",
            "ray_executable",
            "keep_alive",
            "keep_job_alive",
            "delete_job",
            "working_dir",
            "entrypoint",
            "python_executable",
            "cluster_url",
            "stream_mode",
            "total_documents",
            "create_own_cas",
            "create_own_cas_name"
    );

    // ConcurrentHashMap because run() and destroy() come from different threads
    private final ConcurrentHashMap<String, InstantiatedComponent> components;
    private final HttpClient client;
    private DUUILuaContext luaContext;
    private final int connectionTimeout;

    // Shared cluster state: first component starts it, last one stops it
    private final Object rayLock = new Object();
    private boolean rayClusterStarted = false;
    private boolean clusterOwnedByDriver = false; // false when an existing cluster URL was provided
    private final AtomicInteger activeComponents = new AtomicInteger(0);
    private volatile boolean keepAlive = false;
    private volatile boolean keepJobAlive = false;
    private volatile boolean deleteJob = false;

    /** Ray file used to stop the cluster. Captured from the first component or set via {@link #withRaySource} */
    private volatile String activeRayExecutable = "ray";

    /** Driver level ray file override, wins over the components ray_executable param when set */
    private volatile String userRaySource = null;

    /** Job ID from {@code ray job submit} needed to stop or delete the job later */
    private volatile String activeJobId = null;

    /** Dashboard address we submitted the job to passed again when stopping it */
    private volatile String activeDashboardAddress = null;

    /** Gets the job ID from {@code ray job submit} output, e.g.Job "raysubmit_XXXX" submitted successfully */
    private static final java.util.regex.Pattern JOB_ID_PATTERN =
            java.util.regex.Pattern.compile("Job '(raysubmit_\\w+)'");

    public DUUIRayParallelDriver() {
        this(120);
    }

    public DUUIRayParallelDriver(int connectionTimeoutSeconds) {
        components = new ConcurrentHashMap<>();
        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(connectionTimeoutSeconds))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        connectionTimeout = connectionTimeoutSeconds;
    }

    /**
     * Sets the ray file for both starting and stopping the cluster
     * Use this when ray is not on PATH or you want a specific env (e.g. conda)
     * Takes priority over the component-level {@code withRayExecutable()}
     *
     * <pre>
     * new DUUIRayParallelDriver()
     *     .withRaySource("/opt/conda/envs/myenv/bin/ray")
     * </pre>
     */
    public DUUIRayParallelDriver withRaySource(String rayExecutablePath) {
        Objects.requireNonNull(rayExecutablePath, "rayExecutablePath must not be null");
        this.userRaySource = rayExecutablePath;
        this.activeRayExecutable = rayExecutablePath;
        return this;
    }

    @Override
    public void setLuaContext(DUUILuaContext ctx) {
        luaContext = ctx;
    }

    @Override
    public boolean canAccept(DUUIPipelineComponent component) {
        return component.getParameters().containsKey("ray_parallel_component");
    }

    /** Starts the cluster if not running yet, waits for component to be ready, loads the Lua layer */
    @Override
    public String instantiate(DUUIPipelineComponent component, JCas jc, boolean skipVerification, AtomicBoolean shutdown) throws Exception {
        String uuid = UUID.randomUUID().toString();
        while (components.containsKey(uuid)) {
            uuid = UUID.randomUUID().toString();
        }

        InstantiatedComponent comp = new InstantiatedComponent(component);
        final String uuidCopy = uuid;

        boolean activeCountIncremented = false;
        try {
            synchronized (rayLock) {
                if (!rayClusterStarted) {
                    startRayCluster(comp, uuidCopy);
                    rayClusterStarted = true;
                }
                activeComponents.incrementAndGet();
                activeCountIncremented = true;
                if (comp.isKeepAlive()) {
                    keepAlive = true;
                }
                if (comp.isKeepJobAlive()) {
                    keepJobAlive = true;
                }
                if (comp.isDeleteJob()) {
                    deleteJob = true;
                }
            }

            if (shutdown.get()) {
                // Undo the increment component will not be registered
                synchronized (rayLock) {
                    activeComponents.decrementAndGet();
                }
                return null;
            }

            String url = comp.getUrl();
            IDUUICommunicationLayer layer = responsiveAfterTime(
                    url, jc, connectionTimeout * 1000, client,
                    msg -> System.out.printf("[RayParallelDriver][%s] %s%n", uuidCopy, msg),
                    luaContext, skipVerification
            );

            comp.addComponent(new ComponentInstance(url, layer));
            components.put(uuid, comp);

            String workerDesc = comp.getNumWorkers() == 0
                    ? "head-only (no separate workers)"
                    : comp.getNumWorkers() + " worker(s), " + comp.getCpusPerWorker() + " CPUs/worker, " + comp.getGpusPerWorker() + " GPUs/worker";
            System.out.printf("[RayParallelDriver][%s] Component at %s ready. %s%n", uuid, url, workerDesc);

            return shutdown.get() ? null : uuid;

        } catch (Exception e) {
            if (activeCountIncremented) {
                synchronized (rayLock) {
                    activeComponents.decrementAndGet();
                }
            }
            throw e;
        }
    }

    /** Starts head node + N workers as local processes, then submits the job */
    private void startRayCluster(InstantiatedComponent comp, String uuid) throws IOException, InterruptedException {
        String workingDir = comp.getWorkingDir();
        String entrypoint = comp.getEntrypoint();
        if (workingDir == null) {
            throw new IllegalArgumentException(
                    "[RayParallelDriver][" + uuid + "] 'working_dir' must not be null. " +
                            "Specify it via the Component(workingDir, entrypoint) constructor.");
        }
        if (entrypoint == null) {
            throw new IllegalArgumentException(
                    "[RayParallelDriver][" + uuid + "] 'entrypoint' must not be null. " +
                            "Specify it via the Component(workingDir, entrypoint) constructor.");
        }

        String rayExec = userRaySource != null ? userRaySource : comp.getRayExecutable();
        // Store ray executable
        activeRayExecutable = rayExec;

        // External cluster: skip starting anything, just submit the job
        if (comp.getClusterUrl() != null) {
            System.out.printf("[RayParallelDriver][%s] Using existing Ray cluster at %s — skipping head/worker startup.%n",
                    uuid, comp.getClusterUrl());
            // clusterOwnedByDriver stays false so we don't stop it on shutdown
            submitRayJob(comp, uuid, rayExec);
            return;
        }

        int headPort = comp.getHeadNodePort();
        int dashboardPort = comp.getDashboardPort();
        int numWorkers = comp.getNumWorkers();
        int cpusPerWorker = comp.getCpusPerWorker();
        int gpusPerWorker = comp.getGpusPerWorker();
        File cwd = new File(workingDir);

        System.out.printf("[RayParallelDriver][%s] Starting Ray head node on port %d (dashboard: %d)...%n",
                uuid, headPort, dashboardPort);

        runRayCommand(Arrays.asList(
                rayExec, "start", "--head",
                "--port=" + headPort,
                "--dashboard-host=0.0.0.0",
                "--dashboard-port=" + dashboardPort,
                "--disable-usage-stats"
        ), uuid, "head node", cwd);

        if (numWorkers == 0) {
            System.out.printf("[RayParallelDriver][%s] numWorkers=0 — head node will act as the sole worker at localhost:%d%n",
                    uuid, headPort);
        } else {
            System.out.printf("[RayParallelDriver][%s] Ray head started. Launching %d worker(s)...%n", uuid, numWorkers);

            String address = "localhost:" + headPort;
            for (int i = 0; i < numWorkers; i++) {
                List<String> workerCmd = new ArrayList<>(Arrays.asList(
                        rayExec, "start",
                        "--address=" + address,
                        "--num-cpus=" + cpusPerWorker
                ));
                if (gpusPerWorker > 0) {
                    workerCmd.add("--num-gpus=" + gpusPerWorker);
                }
                runRayCommand(workerCmd, uuid, "worker " + (i + 1) + "/" + numWorkers, cwd);
            }

            System.out.printf("[RayParallelDriver][%s] Ray cluster ready: head + %d worker(s) at localhost:%d%n",
                    uuid, numWorkers, headPort);
        }

        clusterOwnedByDriver = true;
        submitRayJob(comp, uuid, rayExec);
    }

    /** Submits the FastAPI script via {@code ray job submit --no-wait}. instantiate() will poll until it's up */
    private void submitRayJob(InstantiatedComponent comp, String uuid, String rayExec) throws IOException, InterruptedException {
        String workingDir = comp.getWorkingDir();
        String entrypoint = comp.getEntrypoint();

        // Use external cluster's dashboard if given, otherwise localhost
        String dashboardAddress = comp.getClusterUrl() != null
                ? comp.getClusterUrl()
                : "http://localhost:" + comp.getDashboardPort();

        System.out.printf("[RayParallelDriver][%s] Submitting Ray job (working-dir: %s, entrypoint: %s, dashboard: %s)...%n",
                uuid, workingDir, entrypoint, dashboardAddress);

        activeDashboardAddress = dashboardAddress;

        // --working-dir is left out on purpose
        List<String> cmd = new ArrayList<>(Arrays.asList(
                rayExec, "job", "submit",
                "--address", dashboardAddress,
                "--no-wait"
        ));

        if(comp.getClusterUrl() != null) {
            cmd.add("--working-dir");
            cmd.add(comp.getWorkingDir());
        }

        cmd.add("--");
        cmd.add(comp.getPythonExecutable().replace("\r", ""));

        // Make the script path absolute so Ray finds it without a working-dir package
        // Extra tokens after the script name are passed as-is
        String[] parts = entrypoint.replace("\r", "").trim().split("\\s+");
        File scriptFile = new File(parts[0]);
        if (!scriptFile.isAbsolute()) {
            scriptFile = new File(workingDir, parts[0]);
        }
        cmd.add(scriptFile.getAbsolutePath());
        for (int i = 1; i < parts.length; i++) {
            cmd.add(parts[i]);
        }

        // Capture stdout to get the job ID. stderr goes to console as usual
        String output = captureRayCommand(cmd, uuid, "job submit", comp.getClusterUrl() == null ? new File(workingDir) : new File("./"));
        java.util.regex.Matcher m = JOB_ID_PATTERN.matcher(output);
        if (m.find()) {
            activeJobId = m.group(1);
            System.out.printf("[RayParallelDriver][%s] Ray job ID: %s%n", uuid, activeJobId);
        } else {
            System.err.printf("[RayParallelDriver][%s] Warning: could not parse Ray job ID from submit output.%n", uuid);
        }
    }

    /** Runs a ray command, inherits all output, waits for it to finish */
    private void runRayCommand(List<String> cmd, String uuid, String label, File workingDir)
            throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.inheritIO();
        if (workingDir != null) {
            pb.directory(workingDir);
        }

        Process proc = pb.start();
        int exitCode = proc.waitFor();

        if (exitCode != 0) {
            throw new IOException(String.format(
                    "[RayParallelDriver][%s] Failed to start Ray %s (exit code %d). " +
                            "Make sure the ray executable is installed and accessible.",
                    uuid, label, exitCode));
        }

        System.out.printf("[RayParallelDriver][%s] Ray %s started (exit 0)%n", uuid, label);
    }

    /**
     * Like {@link #runRayCommand} but captures stdout and returns it as a string
     * Used for {@code ray job submit} so we can extract the job ID
     */
    private String captureRayCommand(List<String> cmd, String uuid, String label, File workingDir)
            throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        if (workingDir != null) {
            pb.directory(workingDir);
        }

        Process proc = pb.start();
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                sb.append(line).append("\n");
            }
        }
        int exitCode = proc.waitFor();

        if (exitCode != 0) {
            throw new IOException(String.format(
                    "[RayParallelDriver][%s] Failed to run Ray %s (exit code %d). " +
                            "Make sure the ray executable is installed and accessible.",
                    uuid, label, exitCode));
        }

        System.out.printf("[RayParallelDriver][%s] Ray %s completed (exit 0)%n", uuid, label);
        return sb.toString();
    }

    /**
     * Sends {@code ray job stop <jobId>} to stop the ray job from running (If users do not shutdown on their own)
     */
    private void stopRayJob() {
        System.out.printf("[RayParallelDriver] stopRayJob() called — activeJobId=%s, activeDashboardAddress=%s%n",
                activeJobId, activeDashboardAddress);
        if (activeJobId == null) return;
        System.out.printf("[RayParallelDriver] Issuing stop command for job %s...%n", activeJobId);
        try {
            System.out.printf("[RayParallelDriver] Stopping Ray job %s...%n", activeJobId);
            List<String> cmd = new ArrayList<>(Arrays.asList(
                    activeRayExecutable, "job", "stop", activeJobId
            ));
            if (activeDashboardAddress != null) {
                cmd.add("--address");
                cmd.add(activeDashboardAddress);
            }
            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.inheritIO();
            pb.directory(new File(System.getProperty("user.home")));
            Process p = pb.start();
            if (!p.waitFor(30, TimeUnit.SECONDS)) {
                p.destroyForcibly();
            }
            System.out.printf("[RayParallelDriver] Ray job %s stopped.%n", activeJobId);
            activeJobId = null;
        } catch (Exception e) {
            System.err.printf("[RayParallelDriver] Warning: could not stop Ray job %s: %s%n",
                    activeJobId, e.getMessage());
        }
    }

    /**
     * Deletes the job record from the dashboard via {@code ray job delete}
     * Call this only after {@link #stopRayJob()}
     * Takes the Id as param because stopRayJob() already cleared {@code activeJobId}
     */
    private void deleteRayJob(String jobId) {
        if (jobId == null) return;
        try {
            System.out.printf("[RayParallelDriver] Deleting Ray job %s...%n", jobId);
            List<String> cmd = new ArrayList<>(Arrays.asList(
                    activeRayExecutable, "job", "delete", jobId
            ));
            if (activeDashboardAddress != null) {
                cmd.add("--address");
                cmd.add(activeDashboardAddress);
            }
            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.inheritIO();
            pb.directory(new File(System.getProperty("user.home")));
            Process p = pb.start();
            if (!p.waitFor(30, TimeUnit.SECONDS)) {
                p.destroyForcibly();
            }
            System.out.printf("[RayParallelDriver] Ray job %s deleted.%n", jobId);
        } catch (Exception e) {
            System.err.printf("[RayParallelDriver] Warning: could not delete Ray job %s: %s%n",
                    jobId, e.getMessage());
        }
    }

    /** Kills all local Ray nodes with {@code ray stop} */
    private void stopRayCluster() {
        try {
            System.out.println("[RayParallelDriver] Stopping Ray cluster (ray stop)...");
            ProcessBuilder pb = new ProcessBuilder(activeRayExecutable, "stop");
            pb.inheritIO();
            pb.directory(new File(System.getProperty("user.home")));
            Process p = pb.start();
            if (!p.waitFor(30, TimeUnit.SECONDS)) {
                p.destroyForcibly();
            }
            System.out.println("[RayParallelDriver] Ray cluster stopped.");
        } catch (Exception e) {
            System.err.printf("[RayParallelDriver] Warning: could not stop Ray cluster: %s%n", e.getMessage());
        }
    }

    // ======== notifyCollectionSize ========

    @Override
    public void notifyCollectionSize(String uuid, long totalDocuments) {
        InstantiatedComponent comp = components.get(uuid);
        if (comp == null || !comp.isStreamMode()) return;

        // Builder-set value takes priority over reader-reported size
        if (comp.getTotalDocuments() > 0) return;

        if (totalDocuments <= 0) {
            throw new IllegalStateException(
                    "[RayParallelDriver][" + uuid + "] Stream mode is enabled but the reader " +
                            "reported unknown size (" + totalDocuments + "). " +
                            "Call .withTotalDocuments(n) on the Component builder.");
        }
        comp.setTotalDocuments(totalDocuments);
    }

    @Override
    public void notifyDocumentFailed(String uuid){
        InstantiatedComponent comp = components.get(uuid);
        if(comp == null || !comp.isStreamMode()) return;

        long remaining = comp.decrementAndGetTotalDocuments();
        System.out.printf("[RayParallelDriver][%s] Document failed. Expected count reduced to %d%n", uuid, remaining);
    }

    // ======== run ========

    @Override
    public void run(String uuid, JCas aCas, DUUIPipelineDocumentPerformance perf, DUUIComposer composer)
            throws CASException, PipelineComponentException, CompressorException, IOException,
            InterruptedException, SAXException, CommunicationLayerException {

        InstantiatedComponent comp = components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException(
                    "Invalid UUID — component has not been instantiated by DUUIRayParallelDriver");
        }

        if (comp.isStreamMode()) {
            runStream(comp, aCas, perf);
        } else {
            runProcess(comp, aCas, perf);
        }
    }

    /** Builds the Lua parameter map, stripping internal Ray keys */
    private Map<String, String> buildLuaParams(InstantiatedComponent comp) {
        Map<String, String> luaParams = new HashMap<>();
        for (Map.Entry<String, String> entry : comp.getParameters().entrySet()) {
            if (!RAY_PARALLEL_INTERNAL_KEYS.contains(entry.getKey())) {
                luaParams.put(entry.getKey(), entry.getValue());
            }
        }
        return luaParams;
    }

    /** Standard process path: one CAS in, one CAS out via /v1/process */
    private void runProcess(InstantiatedComponent comp, JCas aCas, DUUIPipelineDocumentPerformance perf)
            throws IOException, InterruptedException, CommunicationLayerException, CASException {

        Triplet<IDUUIUrlAccessible, Long, Long> queue = comp.getComponent();
        try {
            ComponentInstance instance = (ComponentInstance) queue.getValue0();
            IDUUICommunicationLayer layer = instance.getCommunicationLayer();
            String url = instance.generateURL();

            Map<String, String> luaParams = buildLuaParams(comp);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            layer.serialize(aCas, out, luaParams, comp.getSourceView());
            byte[] payload = out.toByteArray();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS))
                    .timeout(Duration.ofSeconds(comp.getProcessingTimeout()))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                    .build();

            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());

            if (response.statusCode() != 200) {
                String errorBody = new String(response.body(), java.nio.charset.StandardCharsets.UTF_8);
                throw new IOException(String.format(
                        "[RayParallelDriver] /v1/process returned HTTP %d: %s",
                        response.statusCode(), errorBody));
            }

            ByteArrayInputStream resultStream = new ByteArrayInputStream(response.body());
            layer.deserialize(aCas, resultStream, comp.getTargetView());

        } finally {
            comp.addComponent(queue.getValue0());
        }
    }

    /**
     * Stream mode path: serialize and POST to /v1/stream for every document
     * On the last document, also POST to /v1/finalize and deserialize the result into the CAS
     * N−1 CAS objects are left unchanged
     */
    private void runStream(InstantiatedComponent comp, JCas aCas, DUUIPipelineDocumentPerformance perf)
            throws IOException, InterruptedException, CommunicationLayerException, CASException {

        long total = comp.getTotalDocuments();
        if (total <= 0) {
            throw new IllegalStateException("[RayParallelDriver] " +
                    "Stream mode requires a known total document count. " +
                    "Use .withTotalDocuments(n) on the Component builder, " +
                    "or use a reader that reports getSize().");
        }

        Triplet<IDUUIUrlAccessible, Long, Long> queue = comp.getComponent();
        try {
            ComponentInstance instance = (ComponentInstance) queue.getValue0();
            IDUUICommunicationLayer layer = instance.getCommunicationLayer();
            String url = instance.generateURL();

            Map<String, String> luaParams = buildLuaParams(comp);

            // Serialize this document using the same Lua path as /v1/process
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            layer.serialize(aCas, out, luaParams, comp.getSourceView());

            HttpRequest streamReq = HttpRequest.newBuilder()
                    .uri(URI.create(url + DUUIComposer.V1_COMPONENT_ENDPOINT_STREAM))
                    .timeout(Duration.ofSeconds(comp.getProcessingTimeout()))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(out.toByteArray()))
                    .build();

            HttpResponse<byte[]> streamResp = client.send(streamReq, HttpResponse.BodyHandlers.ofByteArray());
            if (streamResp.statusCode() != 200) {
                String body = new String(streamResp.body(), java.nio.charset.StandardCharsets.UTF_8);
                throw new IOException("[RayParallelDriver] /v1/stream returned HTTP "
                        + streamResp.statusCode() + ": " + body);
            }

            long count = comp.incrementAndGetStreamedCount();

            if (count >= total) {
                // Last document: finalize and write Ray result into this CAS
                HttpRequest finalReq = HttpRequest.newBuilder()
                        .uri(URI.create(url + DUUIComposer.V1_COMPONENT_ENDPOINT_FINALIZE))
                        .timeout(Duration.ofSeconds(comp.getFinalizeTimeout()))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build();

                HttpResponse<byte[]> finalResp = client.send(finalReq, HttpResponse.BodyHandlers.ofByteArray());
                if (finalResp.statusCode() != 200) {
                    String body = new String(finalResp.body(), java.nio.charset.StandardCharsets.UTF_8);
                    throw new IOException("[RayParallelDriver] /v1/finalize returned HTTP "
                            + finalResp.statusCode() + ": " + body);
                }

                if (comp.isCreateOwnCas()) {
                    TypeSystemDescription tsd = comp.getCachedTypeSystem();
                    JCas resultCas = tsd != null
                            ? JCasFactory.createJCas(tsd)
                            : JCasFactory.createJCas();
                    layer.deserialize(resultCas, new ByteArrayInputStream(finalResp.body()), comp.getTargetView());
                    if (comp.getCasName() != null) {
                        DocumentMetaData dmd = DocumentMetaData.create(resultCas);
                        dmd.setDocumentTitle(comp.getCasName());
                        dmd.setDocumentId(comp.getCasName());
                        dmd.addToIndexes();
                    }
                    comp.setResultCas(resultCas);
                } else {
                    layer.deserialize(aCas, new ByteArrayInputStream(finalResp.body()), comp.getTargetView());
                }
            }
            // else: CAS is returned unchanged (pass-through for N−1 documents)

        } catch (ResourceInitializationException e) {
            throw new RuntimeException(e);
        } finally {
            comp.addComponent(queue.getValue0());
        }
    }

    // ======== IDUUIDriverInterface boilerplate ========

    @Override
    public TypeSystemDescription get_typesystem(String uuid)
            throws InterruptedException, IOException, SAXException, CompressorException, ResourceInitializationException {
        InstantiatedComponent comp = components.get(uuid);
        if (comp == null) throw new InvalidParameterException("Invalid UUID");
        TypeSystemDescription desc = IDUUIInstantiatedPipelineComponent.getTypesystem(uuid, comp);
        comp.setCachedTypeSystem(desc);
        return desc;
    }

    /**
     * Returns the JCas created by this driver after a stream-mode finalize when
     * {@link Component#withCreateOwnCas(boolean)} was enabled.
     * Returns {@code null} if the feature was not enabled, stream mode was not used,
     * or finalize has not been called yet
     */
    public JCas getResultCas(String uuid) {
        InstantiatedComponent comp = components.get(uuid);
        if (comp == null) return null;
        return comp.getResultCas();
    }

    /**
     * Returns the result CAS from every component in this driver that had
     * {@link Component#withCreateOwnCas(boolean)} enabled.
     * Components whose finalize has not been called yet, or that did not enable the feature,
     * are omitted from the list
     */
    public List<JCas> getResultCases() {
        return components.values().stream()
                .map(InstantiatedComponent::getResultCas)
                .filter(Objects::nonNull)
                .collect(java.util.stream.Collectors.toList());
    }

    @Override
    public void printConcurrencyGraph(String uuid) {
        InstantiatedComponent comp = components.get(uuid);
        if (comp == null) throw new InvalidParameterException("Invalid UUID");
        System.out.printf("[RayParallelDriver][%s]: Ray parallel component (%d workers, %d CPUs/worker, %d GPUs/worker)%n",
                uuid, comp.getNumWorkers(), comp.getCpusPerWorker(), comp.getGpusPerWorker());
    }

    @Override
    public int initReaderComponent(String uuid, Path filePath) {
        throw new UnsupportedOperationException("DUUIRayParallelDriver does not support reader components");
    }

    @Override
    public boolean destroy(String uuid) {
        // Check if this UUID was actually registered
        InstantiatedComponent removed = components.remove(uuid);
        if (removed == null) return false;

        synchronized (rayLock) {
            int remaining = activeComponents.decrementAndGet();
            // deleteJob overwrites keepJobAlive
            if (remaining <= 0 && rayClusterStarted) {
                if (deleteJob || !keepJobAlive) {
                    String capturedJobId = activeJobId; // capture before stopRayJob() clears it
                    stopRayJob();
                    if (deleteJob) deleteRayJob(capturedJobId);
                }
                if (!keepAlive) {
                    if (clusterOwnedByDriver) {
                        stopRayCluster();
                    }
                    rayClusterStarted = false;
                }

                client.shutdownNow();
            }
        }
        return true;
    }

    @Override
    public void shutdown() {
        components.clear();
        synchronized (rayLock) {
            activeComponents.set(0);
            if (rayClusterStarted) {
                if (deleteJob || !keepJobAlive) {
                    String capturedJobId = activeJobId;
                    stopRayJob();
                    if (deleteJob) deleteRayJob(capturedJobId);
                }
                if (!keepAlive) {
                    if (clusterOwnedByDriver) {
                        stopRayCluster();
                    }
                    rayClusterStarted = false;
                }
            }
        }
        client.shutdownNow();
    }

    /**
     * Polls /v1/communication_layer until the component responds or timeout
     */
    private static IDUUICommunicationLayer responsiveAfterTime(
            String url, JCas jc, int timeoutMs, HttpClient client,
            Consumer<String> logger, DUUILuaContext luaContext,
            boolean skipVerification) throws Exception {

        long startTime = System.currentTimeMillis();

        while (true) {
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(url + DUUIComposer.V1_COMPONENT_ENDPOINT_COMMUNICATION_LAYER))
                        .GET()
                        .timeout(Duration.ofSeconds(10))
                        .build();

                HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) {
                    logger.accept("Component is responsive — communication layer fetched.");
                    IDUUICommunicationLayer layer =
                            new DUUILuaCommunicationLayer(resp.body(), "ray_parallel_component", luaContext);
                    if (!skipVerification) {
                        // Quick smoke-test: make sure Lua can serialize an empty CAS
                        ByteArrayOutputStream stream = new ByteArrayOutputStream();
                        layer.serialize(jc, stream, null, "_InitialView");
                    }
                    return layer;
                }
                logger.accept("Waiting for component to become responsive (HTTP " + resp.statusCode() + ")...");

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (Exception ignored) {
                logger.accept("Waiting for component to become responsive...");
            }

            if (System.currentTimeMillis() - startTime > timeoutMs) {
                throw new IOException(String.format(
                        "Ray parallel component at %s did not become responsive within %d ms",
                        url, timeoutMs));
            }
            Thread.sleep(1000);
        }
    }

    // =================
    // Component (builder)
    // =================

    public static class Component {
        private final DUUIPipelineComponent component;
        private final String workingDir;
        private final String entrypoint;
        private String pythonExecutable = "python3";
        private String taskUrl = "http://127.0.0.1:25590";
        private int numWorkers = 2;
        private boolean numWorkersExplicitlySet = false;
        private int cpusPerWorker = 1;
        private int gpusPerWorker = 0;
        private int headNodePort = 6379;
        private int dashboardPort = 8265;
        private long processingTimeout = 300;
        private long finalizeTimeout = -1; // -1 means fall back to processingTimeout
        private String rayExecutable = "ray";
        private boolean keepAlive = false;
        private boolean keepJobAlive = false;
        private boolean deleteJob = false;
        private String clusterUrl = null;
        private boolean streamMode = false;
        private long totalDocuments = -1;
        private boolean createOwnCas = false;
        private String casName = null;

        public Component(String workingDir, String entrypoint) throws URISyntaxException, IOException {
            this.workingDir = workingDir;
            this.entrypoint = entrypoint;
            this.pythonExecutable = "python3";
            component = new DUUIPipelineComponent();
            component.withParameter("ray_parallel_component", "true");
        }

        /** FastAPI component base URL (default: {@code "http://127.0.0.1:25590"}) */
        public Component withTaskUrl(String url) {
            this.taskUrl = url;
            return this;
        }

        /** Number of Ray workers to start (default: 2). 0 = head-only. No effect when {@link #withClusterUrl} is used */
        public Component withNumWorkers(int n) {
            if (n < 0) throw new IllegalArgumentException("numWorkers must be >= 0");
            this.numWorkers = n;
            this.numWorkersExplicitlySet = true;
            return this;
        }

        /**
         * Dashboard URL of an existing Ray cluster (e.g. {@code "http://10.0.0.1:8265"})
         * When set, no nodes are started and the cluster is never stopped on shutdown;
         * we just submit the job to it
         *
         * <pre>
         * new DUUIRayParallelDriver.Component("/home/user/my_component", "server.py")
         *     .withClusterUrl("http://10.0.0.1:8265")
         *     .withTaskUrl("http://10.0.0.1:25590")
         *     .build()
         * </pre>
         */
        public Component withClusterUrl(String url) {
            Objects.requireNonNull(url, "clusterUrl must not be null");
            this.clusterUrl = url;
            return this;
        }

        /** CPUs allocated to each Ray worker node (default: 1) */
        public Component withCPUsPerWorker(int n) {
            if (n < 1) throw new IllegalArgumentException("cpusPerWorker must be >= 1");
            this.cpusPerWorker = n;
            return this;
        }

        /** GPUs per worker node (default: 0 = CPU-only) */
        public Component withGPUsPerWorker(int n) {
            if (n < 0) throw new IllegalArgumentException("gpusPerWorker must be >= 0");
            this.gpusPerWorker = n;
            return this;
        }

        /** Ray GCS head node port (default: 6379) */
        public Component withHeadNodePort(int port) {
            this.headNodePort = port;
            return this;
        }

        /** Ray Dashboard port (default: 8265) */
        public Component withDashboardPort(int port) {
            this.dashboardPort = port;
            return this;
        }

        /** Max seconds to wait for a single /v1/process response (default: 300) */
        public Component withProcessingTimeout(long seconds) {
            this.processingTimeout = seconds;
            return this;
        }

        /**
         * Max seconds to wait for /v1/finalize (default: same as processingTimeout)
         * Set this to a larger value when finalize triggers long-running work such as model training
         */
        public Component withFinalizeTimeout(long seconds) {
            if (seconds < 1) throw new IllegalArgumentException("finalizeTimeout must be >= 1");
            this.finalizeTimeout = seconds;
            return this;
        }

        /** Python interpreter to use (default: "python3") */
        public Component withPythonExecutable(String pythonExecutable) {
            this.pythonExecutable = pythonExecutable;
            return this;
        }

        /**
         * Ray executable name or full path (default: "ray")
         */
        public Component withRayExecutable(String executable) {
            this.rayExecutable = executable;
            return this;
        }

        /** Keep the cluster running after shutdown (default: {@code false}). Only matters for driver started clusters. External ones are never stopped! */
        public Component withKeepAlive(boolean keepAlive) {
            this.keepAlive = keepAlive;
            return this;
        }

        /**
         * Keep the Ray job alive when the component is destroyed (default: {@code false})
         *
         * <p>Warning: the FastAPI process and its ports (e.g. {@code 25590}) stay open
         * until Python exits or you run {@code ray stop} manually. This will cause
         * port conflicts if you start the pipeline again
         */
        public Component withKeepJobAlive(boolean keepJobAlive) {
            this.keepJobAlive = keepJobAlive;
            return this;
        }

        /**
         * Stop and delete the job from the dashboard once finished (default: {@code false})
         * Overrides {@link #withKeepJobAlive} stop & delete always runs when this is set
         */
        public Component withDeleteJob(boolean delete) {
            this.deleteJob = delete;
            return this;
        }

        /**
         * Enable stream mode: every document's CAS is serialized and sent to {@code /v1/stream},
         * the CAS is left unchanged, and only after all N documents are streamed does the driver
         * call {@code /v1/finalize} and write the Ray job result into the last CAS
         *
         * <p>Stream mode requires a known document count. Either set it explicitly via
         * {@link #withTotalDocuments(long)}, or use a collection reader that reports
         * {@code getSize()} (e.g. {@code DUUIFileReader})
         */
        public Component withStreamMode(boolean streamMode) {
            this.streamMode = streamMode;
            return this;
        }

        /**
         * Total number of documents to stream. Required when stream mode is enabled and the
         * collection reader does not report a document count via {@code getSize()}
         * If the reader reports a size, this value takes priority
         */
        public Component withTotalDocuments(long n) {
            if (n < 1) throw new IllegalArgumentException("totalDocuments must be >= 1");
            this.totalDocuments = n;
            return this;
        }

        /**
         * When enabled, the finalize result is written into a freshly created JCas instead of
         * the last document CAS received from the reader. The last document CAS is left unchanged.
         * Retrieve the result CAS via {@link DUUIRayParallelDriver#getResultCas(String)}.
         * Only meaningful when combined with {@link #withStreamMode(boolean)}
         */
        public Component withCreateOwnCas(boolean createOwnCas) {
            this.createOwnCas = createOwnCas;
            return this;
        }

        /**
         * Same as {@link #withCreateOwnCas(boolean)}, but also adds a {@code DocumentMetaData}
         * annotation to the new CAS with both {@code documentTitle} and {@code documentId}
         * set to {@code name}
         */
        public Component withCreateOwnCas(boolean createOwnCas, String name) {
            this.createOwnCas = createOwnCas;
            this.casName = name;
            return this;
        }

        public Component withParameter(String key, String value) {
            component.withParameter(key, value);
            return this;
        }

        public Component withDescription(String description) {
            component.withDescription(description);
            return this;
        }

        public Component withName(String name) {
            component.withName(name);
            return this;
        }

        public Component withView(String viewName) {
            component.withView(viewName);
            return this;
        }

        public Component withSourceView(String viewName) {
            component.withSourceView(viewName);
            return this;
        }

        public Component withTargetView(String viewName) {
            component.withTargetView(viewName);
            return this;
        }

        public DUUIPipelineComponent build() {
            if (clusterUrl != null && numWorkersExplicitlySet) {
                System.err.printf(
                        "[RayParallelDriver] Warning: withNumWorkers(%d) is ignored when withClusterUrl() is set" +
                                " — the existing cluster topology is used as-is.%n", numWorkers);
            }
            component.withUrl(taskUrl);
            component.withDriver(DUUIRayParallelDriver.class);
            component.withScale(1); // Ray handles parallelism internally; multiple DUUI instances would conflict on ports
            component.withParameter("num_workers", String.valueOf(numWorkers));
            component.withParameter("cpus_per_worker", String.valueOf(cpusPerWorker));
            component.withParameter("gpus_per_worker", String.valueOf(gpusPerWorker));
            component.withParameter("head_node_port", String.valueOf(headNodePort));
            component.withParameter("dashboard_port", String.valueOf(dashboardPort));
            component.withParameter("processing_timeout", String.valueOf(processingTimeout));
            component.withParameter("finalize_timeout", String.valueOf(finalizeTimeout >= 1 ? finalizeTimeout : processingTimeout));
            component.withParameter("ray_executable", rayExecutable);
            component.withParameter("keep_alive", String.valueOf(keepAlive));
            component.withParameter("keep_job_alive", String.valueOf(keepJobAlive));
            component.withParameter("delete_job", String.valueOf(deleteJob));
            component.withParameter("working_dir", workingDir);
            component.withParameter("entrypoint", entrypoint);
            component.withParameter("python_executable", pythonExecutable);
            if (clusterUrl != null) {
                component.withParameter("cluster_url", clusterUrl);
            }
            component.withParameter("stream_mode", String.valueOf(streamMode));
            component.withParameter("total_documents", String.valueOf(totalDocuments));
            component.withParameter("create_own_cas", String.valueOf(createOwnCas));
            if (casName != null) {
                component.withParameter("create_own_cas_name", casName);
            }
            return component;
        }
    }

    // =================
    // ComponentInstance
    // =================

    private static class ComponentInstance implements IDUUIUrlAccessible {
        private final String url;
        private final IDUUICommunicationLayer communicationLayer;

        ComponentInstance(String url, IDUUICommunicationLayer layer) {
            this.url = url;
            this.communicationLayer = layer;
        }

        @Override public String generateURL() { return url; }
        @Override public IDUUICommunicationLayer getCommunicationLayer() { return communicationLayer; }
        @Override public IDUUIConnectionHandler getHandler() { return null; }
    }

    // =================
    // InstantiatedComponent
    // =================

    private static class InstantiatedComponent implements IDUUIInstantiatedPipelineComponent {
        private final LinkedBlockingQueue<ComponentInstance> instances;
        private final DUUIPipelineComponent component;
        private final Map<String, String> parameters;
        private final String sourceView;
        private final String targetView;
        private final String url;
        private final int numWorkers;
        private final int cpusPerWorker;
        private final int gpusPerWorker;
        private final int headNodePort;
        private final int dashboardPort;
        private final long processingTimeout;
        private final long finalizeTimeout;
        private final String rayExecutable;
        private final boolean keepAlive;
        private final boolean keepJobAlive;
        private final boolean deleteJob;
        private final String pythonExecutable;
        private final String workingDir;
        private final String entrypoint;
        private final String clusterUrl;
        private final boolean streamMode;
        private final AtomicLong totalDocuments;
        private final AtomicLong streamedCount = new AtomicLong(0);
        private final boolean createOwnCas;
        private final String casName;
        private volatile JCas resultCas = null;
        private volatile TypeSystemDescription cachedTypeSystem = null;

        InstantiatedComponent(DUUIPipelineComponent comp) {
            component = comp;
            instances = new LinkedBlockingQueue<>();
            parameters = comp.getParameters();
            sourceView = comp.getSourceView();
            targetView = comp.getTargetView();

            List<String> urls = comp.getUrl();
            url = (urls != null && !urls.isEmpty()) ? urls.get(0) : null;

            numWorkers = Integer.parseInt(parameters.getOrDefault("num_workers", "2"));
            cpusPerWorker = Integer.parseInt(parameters.getOrDefault("cpus_per_worker", "1"));
            gpusPerWorker = Integer.parseInt(parameters.getOrDefault("gpus_per_worker", "0"));
            headNodePort = Integer.parseInt(parameters.getOrDefault("head_node_port", "6379"));
            dashboardPort = Integer.parseInt(parameters.getOrDefault("dashboard_port", "8265"));
            processingTimeout = Long.parseLong(parameters.getOrDefault("processing_timeout", "300"));
            finalizeTimeout = Long.parseLong(parameters.getOrDefault("finalize_timeout",
                    parameters.getOrDefault("processing_timeout", "300")));
            rayExecutable = parameters.getOrDefault("ray_executable", "ray");
            keepAlive = Boolean.parseBoolean(parameters.getOrDefault("keep_alive", "false"));
            keepJobAlive = Boolean.parseBoolean(parameters.getOrDefault("keep_job_alive", "false"));
            deleteJob = Boolean.parseBoolean(parameters.getOrDefault("delete_job", "false"));
            pythonExecutable = parameters.getOrDefault("python_executable", "python3");
            workingDir = parameters.getOrDefault("working_dir", null);
            entrypoint = parameters.getOrDefault("entrypoint", null);
            clusterUrl = parameters.getOrDefault("cluster_url", null);
            streamMode = Boolean.parseBoolean(parameters.getOrDefault("stream_mode", "false"));
            totalDocuments = new AtomicLong(Long.parseLong(parameters.getOrDefault("total_documents", "-1")));
            createOwnCas = Boolean.parseBoolean(parameters.getOrDefault("create_own_cas", "false"));
            casName = parameters.getOrDefault("create_own_cas_name", null);
        }

        @Override public DUUIPipelineComponent getPipelineComponent() { return component; }
        @Override public Map<String, String> getParameters() { return parameters; }
        @Override public String getSourceView() { return sourceView; }
        @Override public String getTargetView() { return targetView; }
        @Override public String getUniqueComponentKey() { return url; }

        @Override
        public Triplet<IDUUIUrlAccessible, Long, Long> getComponent() {
            long mutexStart = System.nanoTime();
            try {
                ComponentInstance inst;
                do {
                    inst = instances.poll(100, TimeUnit.MILLISECONDS);
                } while (inst == null);
                long mutexEnd = System.nanoTime();
                return Triplet.with(inst, mutexStart, mutexEnd);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for a free component instance", e);
            }
        }

        @Override
        public void addComponent(IDUUIUrlAccessible item) {
            instances.offer((ComponentInstance) item);
        }

        public String getUrl() { return url; }
        public int getNumWorkers() { return numWorkers; }
        public int getCpusPerWorker() { return cpusPerWorker; }
        public int getGpusPerWorker() { return gpusPerWorker; }
        public int getHeadNodePort() { return headNodePort; }
        public int getDashboardPort() { return dashboardPort; }
        public long getProcessingTimeout() { return processingTimeout; }
        public long getFinalizeTimeout() { return finalizeTimeout; }
        public String getRayExecutable() { return rayExecutable; }
        public boolean isKeepAlive() { return keepAlive; }
        public boolean isKeepJobAlive() { return keepJobAlive; }
        public boolean isDeleteJob() { return deleteJob; }
        public String getPythonExecutable() { return pythonExecutable; }
        public String getWorkingDir() { return workingDir; }
        public String getEntrypoint() { return entrypoint; }
        public String getClusterUrl() { return clusterUrl; }
        public boolean isStreamMode() { return streamMode; }
        public long getTotalDocuments() { return totalDocuments.get(); }
        public void setTotalDocuments(long n) { totalDocuments.set(n); }
        public long decrementAndGetTotalDocuments() { return totalDocuments.decrementAndGet(); }
        public long incrementAndGetStreamedCount() { return streamedCount.incrementAndGet(); }
        public boolean isCreateOwnCas() { return createOwnCas; }
        public String getCasName() { return casName; }
        public JCas getResultCas() { return resultCas; }
        public void setResultCas(JCas cas) { resultCas = cas; }
        public TypeSystemDescription getCachedTypeSystem() { return cachedTypeSystem; }
        public void setCachedTypeSystem(TypeSystemDescription desc) { cachedTypeSystem = desc; }
    }
}
