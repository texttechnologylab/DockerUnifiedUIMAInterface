package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import static java.lang.String.format;

import java.io.File;

import static guru.nidi.graphviz.model.Factory.mutGraph;
import static guru.nidi.graphviz.model.Factory.graphAttrs;
import static guru.nidi.graphviz.model.Factory.linkAttrs;
import static guru.nidi.graphviz.model.Factory.node;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.uima.jcas.JCas;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import guru.nidi.graphviz.attribute.Color;
import guru.nidi.graphviz.attribute.Rank;
import guru.nidi.graphviz.attribute.Rank.RankDir;
import guru.nidi.graphviz.attribute.Style;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.Node;

public class DUUIPipelineProfiler {
    
    private static class Measurement {
        final String _measurementObject;
        final Instant _start;
        Instant _end = null; 
        
        Measurement(String measurementObject, Instant start) {
            _measurementObject = measurementObject; 
            _start = start;
        }

        long total() {
            return _end != null ? _end.minusSeconds(_start.getEpochSecond()).getEpochSecond() : 0; 
        }
    }

    private final static Map<String, Measurement> _doc_measurements = new ConcurrentHashMap<>(); 
    private final static Map<String, Instant> _pipeline_measurements = new ConcurrentHashMap<>(); 
    private final static Map<String, JCas> _documents = new ConcurrentHashMap<>();
    private final static Map<Instant, Pair<String, Triplet<Long, Long, Long>>> _timeline = new ConcurrentHashMap<>();
    private static Map<String, MutableGraph> _pipelineGraphs = new HashMap<>(); 
    private static String _name; 
    private static Map<String, Set<String>> _pipeline;
    private static AtomicBoolean withProfiler = new AtomicBoolean(false); 
    private static ExecutorService profileRunner = Executors.newSingleThreadExecutor();

    public DUUIPipelineProfiler(String name, Map<String, Set<String>> pipeline) {
        if(name == null) return;
        if (withProfiler.get()) {
            _doc_measurements.clear();
            _pipeline_measurements.clear();
            _documents.clear();
            _timeline.clear();
            _pipelineGraphs.clear();
        }

        withProfiler.set(true);
        _name = name;
        _pipeline = pipeline;         
    }

    public static synchronized void add(String name, JCas jc) {
        if (!withProfiler.get() || _pipeline == null) return; 

        _pipelineGraphs.put(name, mutGraph("example1").setDirected(true).use( (gr, ctx) -> {
            graphAttrs().add(Rank.dir(RankDir.TOP_TO_BOTTOM));
            linkAttrs().add("class", "link-class");
        }));

        for(String vertex : _pipeline.keySet()) {
            Node parent = node(vertex);  
            _pipelineGraphs.get(name).add(parent);

            for (String child : _pipeline.get(vertex)) {
                _pipelineGraphs.get(name).add(parent.link(node(child)));
            }
        }

        _documents.put(name, jc);
        _timeline.put(Instant.now(), timelineEntry(format("[Profiler] Analysis for %s has started", name)));
    }

    public static synchronized void updatePipelineGraphStatus(String name, String signature, Color progress) {
        // TODO: Execute with executorservice
        if (!withProfiler.get() || _pipeline == null) return; 
        if (!_pipelineGraphs.containsKey(name)) return;
        
        _timeline.put(Instant.now(), timelineEntry(format("[Profiler][%s] Updating graph status", name)));

        measureStart(name, format("[%s] Updating graph status", signature));

        _pipelineGraphs.get(name).nodes().forEach(comp -> {
            if (comp.name().contentEquals(signature)) {
                comp.add(progress, Style.lineWidth(2), Style.RADIAL);
            } 
        });

        writeToFile(name);
        measureEnd(name, format("[%s] Updating graph status", signature));
    }
    
    public static void writeToFile(String name) {
        try {
            Graphviz.fromGraph(_pipelineGraphs.get(name)).width(1920).height(1080).render(Format.PNG).toFile(
                new File(format("./Execution-Pipeline/%s.png", name)));
            // System.out.printf("[DUUIExecutionPipeline] Generated execution graph: ./Execution-Pipeline/%s.png%n", name);
        } catch (Exception e) {
            System.out.println(format("[%s] Writing pipeline to file failed: %n", name) + e.getMessage());
        }
    }

    public static void printMeasurements() {
        if (!withProfiler.get()) return; 

        System.out.println("[PipelineProfiler] Measurements: ");
        _pipeline_measurements.forEach((measurement, time) -> 
        {
            System.out.printf("%s: %d seconds. %n", measurement, time.getEpochSecond());
        });
        _doc_measurements.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(entry -> 
        {
            System.out.printf("%s: %d seconds. %n", entry.getKey(), entry.getValue().total());
        });

        // _timeline.forEach((measurement, memory) -> 
        // {
        //     System.out.printf("At %s: %s%n", measurement, memory.getValue0());
        // });

    }

    private static Pair<String, Triplet<Long, Long, Long>> timelineEntry(String description) {
        return Pair.with(description, getMemorySnapshot()); 
    }

    private static Triplet<Long, Long, Long> getMemorySnapshot() {
        return Triplet.with(
                Runtime.getRuntime().freeMemory(),
                Runtime.getRuntime().totalMemory(), 
                Runtime.getRuntime().maxMemory());
    }

    public synchronized static void measureStart(String measurementObject) {
        if (!withProfiler.get()) return; 
        if (measurementObject == null) return; 
        if (_pipeline_measurements.containsKey(measurementObject)) return;
        
        Instant time = Instant.now();
        _timeline.put(time, timelineEntry(format("[%s] %s started.", _name, measurementObject)));
        _pipeline_measurements.put(measurementObject, time);
    }

    public synchronized static void measureStart(String name, String measurementObject) {
        if (!withProfiler.get()) return; 
        if (measurementObject == null || name == null) return; 

        String key = format("[%s][%s]", name, measurementObject);

        if (_doc_measurements.containsKey(key)) return;

        Instant time = Instant.now();
        _timeline.put(time, timelineEntry(format("[%s] %s started.", name, measurementObject)));
        _doc_measurements.put(key, new Measurement(name, time));
    }

    public synchronized static void measureEnd(String measurementObject) {
        if (!withProfiler.get()) return; 
        if (measurementObject == null) return; 
        if (!_pipeline_measurements.containsKey(measurementObject)) return; 

        Instant time = Instant.now();
        _timeline.put(time, timelineEntry(format("[%s] %s finished.", _name, measurementObject)));
        _pipeline_measurements.put(measurementObject, time.minusSeconds(_pipeline_measurements.get(measurementObject).getEpochSecond()));
    }

    public synchronized static void measureEnd(String name, String measurementObject) {
        if (!withProfiler.get()) return; 
        if (measurementObject == null || name == null) return; 

        String key = format("[%s][%s]", name, measurementObject);

        if (!_doc_measurements.containsKey(key)) return; 

        Instant time = Instant.now();
        _timeline.put(time, timelineEntry(format("[%s] %s finished.", name, measurementObject)));
        _doc_measurements.get(key)._end = time;
    }

    synchronized static void finalize(int i) {
        if (!withProfiler.get()) return; 
        // TODO: finalization
        // Get document metadata
        // map document to measurments 
        // map measurements to timeline
    }
}
