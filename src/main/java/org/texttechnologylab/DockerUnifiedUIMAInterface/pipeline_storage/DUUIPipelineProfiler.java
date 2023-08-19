package org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage;

import static java.lang.String.format;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.io.ByteArrayOutputStream;

import static guru.nidi.graphviz.model.Factory.mutGraph;
import static guru.nidi.graphviz.model.Factory.graphAttrs;
import static guru.nidi.graphviz.model.Factory.linkAttrs;
import static guru.nidi.graphviz.model.Factory.node;

import java.time.Instant;
import java.time.LocalTime;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.Config;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.Signature;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUISimpleMonitor;

import guru.nidi.graphviz.attribute.Color;
import guru.nidi.graphviz.attribute.Rank;
import guru.nidi.graphviz.attribute.Rank.RankDir;
import guru.nidi.graphviz.attribute.Style;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.Node;

public class DUUIPipelineProfiler {
    // e.g.: {"ParallelTest1": {"Token => POS": {"urlwait":"00:00:01", ...}}}
    private final static Map<String, Map<String, Object>> _pipeline_measurements = new ConcurrentHashMap<>();
    private static Map<String, MutableGraph> _pipelineGraphs = new HashMap<>();
    private static String _name;
    private static Map<String, Set<String>> _pipeline; // Every node with a Set containing child nodes.
    private static Set<String> _components = null;
    private static DUUISimpleMonitor _monitor;
    private static AtomicBoolean withProfiler = new AtomicBoolean(false);

    private static final ByteArrayOutputStream high = new ByteArrayOutputStream(1024*1024*5);
    private static final ByteArrayOutputStream low = new ByteArrayOutputStream(1024*50);

    public DUUIPipelineProfiler(String name, Map<String, Set<String>> pipeline, DUUISimpleMonitor monitor) {
        if (name == null || monitor == null) {
            withProfiler.set(false);
            return;
        }
        if (withProfiler.get()) {
            _pipeline_measurements.clear();
            _pipelineGraphs.clear();
        }

        withProfiler.set(true);
        _name = name;
        _pipeline = pipeline;
        _components = _pipeline.keySet();
        _monitor = monitor;

        _pipelineGraphs.put(name, mutGraph().setDirected(true).use((gr, ctx) -> {
            graphAttrs().add(Rank.dir(RankDir.TOP_TO_BOTTOM));
            linkAttrs().add("class", "link-class");
        }));

        for (String vertex : _components) {
            Node parent = node(vertex);
            _pipelineGraphs.get(name).add(parent);
            for (String child : _pipeline.get(vertex)) {
                _pipelineGraphs.get(name).add(parent.link(node(child)));
            }
        }

        statusUpdate("STARTED", "New run started.");
        Map<String, Object> pipe_update = new HashMap<>();
        pipe_update.put("name", name);
        pipe_update.put("workers", Config.workers()); 
        pipe_update.put("graph", writeToFile(name, true));  
        send(pipe_update, DUUISimpleMonitor.V1_MONITOR_PIPELINE_UPDATE);  
    }

    public static synchronized void add(String name, Signature signature, int scale) {
        if (!withProfiler.get())
            return;

        
        Map<String, Object> componentMeasurements = new HashMap<>(11);
        componentMeasurements.put("name", name);
        componentMeasurements.put("component", signature.toString());
        componentMeasurements.put("scale", scale);
        _pipeline_measurements.put(key(name, signature.toString()), componentMeasurements);

        if (_pipelineGraphs.containsKey(name))
            return; 

        _pipelineGraphs.put(name, mutGraph().setDirected(true).use((gr, ctx) -> {
            graphAttrs().add(Rank.dir(RankDir.TOP_TO_BOTTOM));
            linkAttrs().add("class", "link-class");
        }));

        for (String vertex : _components) {
            Node parent = node(vertex);
            _pipelineGraphs.get(name).add(parent);
            for (String child : _pipeline.get(vertex)) {
                _pipelineGraphs.get(name).add(parent.link(node(child)));
            }
        }
    }

    public static void documentMetaDataUpdate(String name, String title, int initialSize) {
        if (! withProfiler.get())
            return;

        Map<String, Object> doc_update = new HashMap<>();
        doc_update.put("name", name);
        doc_update.put("title", title);
        doc_update.put("initial_size", initialSize);

        send(doc_update, DUUISimpleMonitor.V1_MONITOR_DOCUMENT_UPDATE);
    }

    public static void documentUpdate(String name, Signature signature, String updateValue, Object value) {
        if (! withProfiler.get() || ! _pipeline_measurements.containsKey(key(name, signature.toString())))
            return;

        Map<String, Object> componentMeasurements = _pipeline_measurements.get(key(name, signature.toString()));
        if (value instanceof Instant) {
            componentMeasurements.put(updateValue, ((Instant)value).getEpochSecond());
        } else {
            componentMeasurements.put(updateValue, value);
        }
    }

    public static void statusUpdate(String status, String message) {
        if (! withProfiler.get())
            return; 

        Map<String, Object> state = new HashMap<>(2);
        state.put("status", status);
        state.put("message", message);
        send(state, DUUISimpleMonitor.V1_MONITOR_STATUS);
    }

    public static void pipelineUpdate(String updateValue, Object value) {
        if (! withProfiler.get())
            return;

        send(updateValue, value, DUUISimpleMonitor.V1_MONITOR_PIPELINE_UPDATE);
    }

    public static void updatePipelineGraphStatus(String name, String signature, Color progress) {
        if (!withProfiler.get() || _pipeline == null)
            return;
        if (!_pipelineGraphs.containsKey(name))
            return;

        _pipelineGraphs.get(name).nodes().forEach(comp -> {
            if (comp.name().contentEquals(signature)) {
                comp.add(progress, Style.lineWidth(2), Style.RADIAL);
            }
        });
        Instant start = Instant.now();
        String png = writeToFile(name, false); 
        Instant end = Instant.now().minusSeconds(start.getEpochSecond());
        Map<String, Object> graphUpdate = new HashMap<>();
        graphUpdate.put("name", name);
        graphUpdate.put("engine_duration", formatTime(end));
        graphUpdate.put("png", png);
        send(graphUpdate, DUUISimpleMonitor.V1_MONITOR_GRAPH_UPDATE);
    }

    public synchronized static String writeToFile(String name, boolean highQuality) {
        ByteArrayOutputStream out; // TODO: Switch to ByteBuffer for efficiency
        int width = 500; int height = 300;
        if (highQuality) {
            width = 1500; height = 500; 
            out = high; // 5 MB for high quality pic
        } else {
            out = low; // 50 KB for low quality pic
        }
        
        try {
            Graphviz.fromGraph(_pipelineGraphs.get(name))
                    .width(width)
                    .height(height)
                    .render(Format.PNG)
                    .toOutputStream(out);
            // System.out.printf("[DUUIExecutionPipeline] Generated execution graph:
            // ./Execution-Pipeline/%s.png%n", name);
            return Base64.getEncoder().encodeToString(((ByteArrayOutputStream) out).toByteArray());
        } catch (Exception e) {
            System.out.println(format("[%s] Writing pipeline-graph to file failed: %n", name));
            e.printStackTrace();
            return "";
        } finally {
            out.reset();
        }
    }

    public synchronized static void finalizeDocument(String name, String signature) {
        if (! withProfiler.get())
            return;

        Map<String, Object> componentMeasurements = _pipeline_measurements.get(key(name, signature));
        // // System.out.printf("Key: %s => Value: %s%n", measurement, _pipeline_measurements.get(measurement));
        // System.out.println(componentMeasurements);

        send(componentMeasurements, DUUISimpleMonitor.V1_MONITOR_DOCUMENT_MEASUREMENT_UPDATE);
    }

    private static void send(String updateValue, Object value, String type) {
        Map<String, Object> update = new HashMap<>();
        update.put(updateValue, value);
        send(update, type);
    }

    private static void send(Map<String, Object> update, String type) {
        try {
            _monitor.sendUpdate(update, type);
        } catch (IOException | NullPointerException e) {
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }

    public static String formatTime(Instant seconds) {
        return LocalTime.ofSecondOfDay(seconds.getEpochSecond()).toString();
    }

    private static String key(String name, String signature) {
        return format("%s%s-",name, signature);
    }
}
