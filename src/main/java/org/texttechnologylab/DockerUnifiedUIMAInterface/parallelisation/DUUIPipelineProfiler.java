package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import static java.lang.String.format;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


import org.apache.uima.jcas.JCas;
import org.javatuples.Pair;
import org.javatuples.Triplet;

public class DUUIPipelineProfiler {
    
    private static class Measurement {
        final String _measurementObject;
        final long _start;
        long _end = 0; 
        
        Measurement(String measurementObject, long start) {
            _measurementObject = measurementObject; 
            _start = start;
        }

        long total() {
            return _end != 0 ? _end  - _start : 0; 
        }
    }

    private final static Map<String, Measurement> _measurements = new ConcurrentHashMap<>(); 
    private final static Map<String, JCas> _documents = new ConcurrentHashMap<>();
    private final static Map<Long, Pair<String, Triplet<Long, Long, Long>>> _timeline = new ConcurrentHashMap<>();


    public DUUIPipelineProfiler(String name, JCas jc) {
        _documents.put(name, jc);
        _timeline.put(System.nanoTime(), timelineEntry(format("[%s] Analysis for %s has started", name, name)));
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

    public synchronized static void measureStart(String name, String measurementObject) {
        if(name != null || _measurements.containsKey(name + measurementObject)) return; 
        long time = System.nanoTime();
        _timeline.put(time, timelineEntry(format("[%s] %s started.")));
        _measurements.put(name + measurementObject, new Measurement(name, System.nanoTime()));
    }

    public synchronized static void measureEnd(String name, String measurementObject) {
        if(name != null || !_measurements.containsKey(name + measurementObject)) return; 
        long time = System.nanoTime();
        _timeline.put(time, timelineEntry(format("[%s] %s finished.")));
        _measurements.get(name + measurementObject)._end = System.nanoTime();
    }

    synchronized static void finalize(int i) {
        // TODO: finalization
        // Get document metadata
        // map document to measurments 
        // map measurements to timeline
    }
}
