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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.json.JSONObject;
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
    private final static Map<String, Object> _pipeline_measurements = new ConcurrentHashMap<>();
    private static Map<String, MutableGraph> _pipelineGraphs = new HashMap<>();
    private static String _name;
    private static Map<String, Set<String>> _pipeline;
    private static Set<String> _components = null;
    private static DUUISimpleMonitor _monitor;
    private static AtomicBoolean withProfiler = new AtomicBoolean(false);
    // TODO: Add implementation with executor
    private static ExecutorService profileRunner = Executors.newSingleThreadExecutor();

    public DUUIPipelineProfiler(String name, Map<String, Set<String>> pipeline, DUUISimpleMonitor monitor) {
        if (name == null)
            return;
        if (withProfiler.get()) {
            _pipeline_measurements.clear();
            _pipelineGraphs.clear();
        }

        withProfiler.set(true);
        _name = name;
        _pipeline = pipeline;
        _components = _pipeline.keySet();
        _monitor = monitor;

        Map<String, Object> pipe_update = new HashMap<>();
        pipe_update.put("name", name);
        send(pipe_update, DUUISimpleMonitor.V1_MONITOR_PIPELINE_UPDATE);
    }

    public static synchronized void add(String name, String title, Signature signature, int scale) {
        if (!withProfiler.get() || _pipelineGraphs.containsKey(name))
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
        
        _pipeline_measurements.put(key(name, signature.toString()) + "scale", scale+"");

        Map<String, Object> doc_update = new HashMap<>();
        doc_update.put("name", name);
        doc_update.put("title", title);
        doc_update.put("component", signature.toString());

        send(doc_update, DUUISimpleMonitor.V1_MONITOR_DOCUMENT_UPDATE);
        // graphUpdate(name);
    }

    private static void graphUpdate(String name) {
        if (_monitor == null)
            return;

        Instant start = Instant.now();
        String svg = writeToFile(name);
        Instant end = Instant.now().minusSeconds(start.getEpochSecond());
        Map<String, Object> graphUpdate = new HashMap<>();
        graphUpdate.put("name", name);
        graphUpdate.put("engine_duration", formatTime(end));
        graphUpdate.put("svg", svg);
        send(graphUpdate, DUUISimpleMonitor.V1_MONITOR_GRAPH_UPDATE);
    }

    public static void pipelineUpdate(String updateValue, Object value) {
        if (_monitor == null)
            return;

        send(updateValue, value, DUUISimpleMonitor.V1_MONITOR_PIPELINE_UPDATE);
    }

    public static void documentUpdate(String name, Signature signature, String updateValue, Object value) {
        if (_monitor == null)
            return;

        if (value instanceof Instant) {
            value = formatTime((Instant) value);
        }
        _pipeline_measurements.put(key(name, signature.toString()) + updateValue, value);
    }

    public static synchronized void updatePipelineGraphStatus(String name, String signature, Color progress) {
        if (!withProfiler.get() || _pipeline == null)
            return;
        if (!_pipelineGraphs.containsKey(name))
            return;

        _pipelineGraphs.get(name).nodes().forEach(comp -> {
            if (comp.name().contentEquals(signature)) {
                comp.add(progress, Style.lineWidth(2), Style.RADIAL);
            }
        });

        graphUpdate(name);
    }

    public static String writeToFile(String name) {
        OutputStream out = new ByteArrayOutputStream();
        try {
            Graphviz.fromGraph(_pipelineGraphs.get(name))
                    .width(1000)
                    .height(1500)
                    .render(Format.SVG)
                    .toOutputStream(out);
            // System.out.printf("[DUUIExecutionPipeline] Generated execution graph:
            // ./Execution-Pipeline/%s.png%n", name);
            return new String(((ByteArrayOutputStream) out).toByteArray(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            System.out.println(format("[%s] Writing pipeline to file failed: %n", name) + e.getMessage());
            return "";
        }
    }

    public synchronized static void finalizeDocument(String name, String signature) {

        Map<String, Object> update = new HashMap<>();
        update.put("name", name);
        update.put("component", signature);
        update.put("scale", _pipeline_measurements.get(key(name, signature) + "scale"));
        
        System.out.println("FINALIZED DOCUMENT");

        _pipeline_measurements.keySet()
            .stream().filter(key -> key.contains(key(name, signature)))
            .forEach(compKey -> 
                {
                // System.out.printf("Key: %s => Value: %s%n", compKey, _pipeline_measurements.get(compKey));
                update.put(
                    compKey.replace(key(name, signature), ""), 
                    _pipeline_measurements.get(compKey)
            );
        });
    
        
        send(update, DUUISimpleMonitor.V1_MONITOR_DOCUMENT_MEASUREMENT_UPDATE);
    }

    private static void send(String updateValue, Object value, String type) {
        Map<String, Object> update = new HashMap<>();
        update.put(updateValue, value);
        send(update, type);
    }

    private static void send(Map<String, Object> update, String type) {
        try {
            _monitor.sendUpdate(update, type);
        } catch (IOException e) {
            System.out.println(e.getMessage());
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
