package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import static guru.nidi.graphviz.model.Factory.graph;
import static guru.nidi.graphviz.model.Factory.mutGraph;
import static guru.nidi.graphviz.model.Factory.graphAttrs;
import static guru.nidi.graphviz.model.Factory.linkAttrs;
import static guru.nidi.graphviz.model.Factory.nodeAttrs;
import static guru.nidi.graphviz.model.Factory.node;
import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.javatuples.Pair;
import org.javatuples.Tuple;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.GraphCycleProhibitedException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.PipelinePart;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;

import guru.nidi.graphviz.attribute.Color;
import guru.nidi.graphviz.attribute.Rank;
import guru.nidi.graphviz.attribute.Style;
import guru.nidi.graphviz.attribute.Rank.RankDir;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.Graph;
import guru.nidi.graphviz.model.LinkSource;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.Node;

public class DUUIParallelExecutionPipeline extends DirectedAcyclicGraph<String, CustomEdge>  {
    public final Map<String, PipelinePart> _pipeline;
    private final Map<String, Pair<Map<Class<? extends Annotation>, CountDownLatch>, List<CountDownLatch>>> _locks; 
    private final Map<String, Map<Class<? extends Annotation>, Integer>> _latchesCovered;
    private static MutableGraph _pipelineGraph; 

    public DUUIParallelExecutionPipeline(Vector<PipelinePart> flow) {
        super(CustomEdge.class);
        _pipeline = flow.stream()
                        .collect(Collectors.toMap(
                            ppart -> ppart.getUUID(),
                            ppart -> ppart));
        _locks = new HashMap<>(_pipeline.size());
        _latchesCovered = new ConcurrentHashMap<>(_pipeline.size());

        initialiseDAG();
        initialiseLocks();

        _pipelineGraph = mutGraph("example1").setDirected(true).use( (gr, ctx) -> {
            graphAttrs().add(Rank.dir(RankDir.TOP_TO_BOTTOM));
            linkAttrs().add("class", "link-class");
        });
        for(String vertex : vertexSet()) {
            Node parent = node(_pipeline.get(vertex).getSignature().toString()).with(Color.RED2);  
            _pipelineGraph = _pipelineGraph.add(parent);
            for (String child : getChildren(vertex)) {
                _pipelineGraph = _pipelineGraph.add(parent.link(node(_pipeline.get(child).getSignature().toString())));
            }
        }
        _pipelineGraph.nodes().forEach(comp -> comp.add(Color.RED3, Style.lineWidth(2), Style.RADIAL));
    }

    public List<Callable<Void>> getWorkers(String name, JCas jc, DUUIPipelineDocumentPerformance perf) throws Exception {

        List<Callable<Void>> workers =  _pipeline.entrySet().stream()
                .map(entry -> 
                {
                    String uuid = entry.getKey();
                    PipelinePart comp = entry.getValue();
                    List<CountDownLatch> selfLocks = _locks.get(uuid).getValue0()
                    .entrySet().stream().map(selfLock -> 
                    {
                        if (!JCasUtil.select(jc, selfLock.getKey()).isEmpty()) {
                            selfLock.getValue().countDown(); // releases locks if already in JCas 
                            Map<Class<? extends Annotation>, Integer> covered = _latchesCovered.get(uuid);
                            covered.put(selfLock.getKey(), 0);
                            _latchesCovered.put(uuid, covered);
                        }
                        return selfLock.getValue(); 
                    }).collect(Collectors.toList());

                    return new DUUIWorker(
                            name,
                            comp,
                            jc, 
                            perf, 
                            selfLocks,
                            _locks.get(uuid).getValue1()
                        );
                })
                .collect(Collectors.toList());

        if (!_latchesCovered.values().stream().allMatch(map -> map.values().stream().allMatch(c -> c.intValue() == 0)))
            throw new Exception(
                "[ExecutionPipeline] There are components in this pipeline whose inputs are not available. Analysis is cancelled");
    
        return workers;
    }

    public Set<String> getChildren(String child) {
        return outgoingEdgesOf(child).stream()
            .map(CustomEdge::getTarget)
            .collect(Collectors.toSet());
    }

    public static synchronized void updatePipelineGraphStatus(String name, String signature, Color progress) {
        synchronized(_pipelineGraph) {
            _pipelineGraph.nodes().forEach(comp -> {
                if (comp.name().contentEquals(signature)) {
                    comp.add(progress, Style.lineWidth(2), Style.RADIAL);
                }
            
            });

            printPipeline(name);
        }
    }

    public static void printPipeline(String name) {

        try {
            Graphviz.fromGraph(_pipelineGraph).scale(2.f).width(1500).height(500).render(Format.PNG).toFile(
                new File(format("./Execution-Pipeline/%s.png", name)));
            // System.out.printf("[DUUIExecutionPipeline] Generated execution graph: ./Execution-Pipeline/%s.png%n", name);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initialiseDAG() {
        for (PipelinePart part1 : _pipeline.values()) {
            this.addVertex(part1.getUUID());

            for (PipelinePart part2 : _pipeline.values()) {
                if (part1.getUUID() == part2.getUUID()) continue;

                this.addVertex(part2.getUUID());

                try {

                    switch (part1.getSignature().compareSignatures(part2.getSignature())) {
                        // Direct Dependency
                        case 1:
                            this.addEdge(part1.getUUID(), part2.getUUID());
                            break;
                        case -1: 
                            this.addEdge(part2.getUUID(), part1.getUUID());
                            break;
                        // Cycle
                        case -2:
                            break;
                        // No-Dependency
                        default:
                            break;
                        
                    }
                } catch (GraphCycleProhibitedException e) {
                    System.out.println("[DUUIExecutionPipeline] There is a circular dependency between the pipeline components. "
                        + "Parallel execution is impossible!");
                    throw e;
                }
            }
        }
    }  

    private void initialiseLocks() {
        
        Map<String, Map<Class<? extends Annotation>, CountDownLatch>> 
            latches = new ConcurrentHashMap<>(_pipeline.size());  

        // initialize latches map
        _pipeline.forEach((self, part) -> {
            List<Class<? extends Annotation>> dependencies = part.getSignature().getInputs();
            Map<Class<? extends Annotation>, CountDownLatch> 
                selfLatches = new ConcurrentHashMap<>(dependencies.size());
            Map<Class<? extends Annotation>, Integer> 
                selfLatchesCovered = new ConcurrentHashMap<>(dependencies.size());
            dependencies.forEach(depen -> {
                selfLatches.put(depen, new CountDownLatch(1));
                selfLatchesCovered.put(depen, 1);
            });
            latches.put(self, selfLatches);
            _latchesCovered.put(self, selfLatchesCovered);
        });

        _pipeline
        .forEach((self, pipelinePart) -> 
        {
            List<Class<? extends Annotation>> 
                selfOutputs = pipelinePart.getSignature().getOutputs();

            List<CountDownLatch> childLatches = new ArrayList<>();
            getChildren(self).stream().forEach(child -> {
                Map<Class<? extends Annotation>, CountDownLatch> childLatchesMap = latches.get(child);
                childLatchesMap.forEach((annotation, lock) -> {
                    if (selfOutputs.contains(annotation)) {
                        Map<Class<? extends Annotation>, Integer> covered = _latchesCovered.get(child);
                        covered.put(annotation, 0);
                        _latchesCovered.put(child, covered);
                        childLatches.add(lock);
                    }
                });
            });

            _locks.put(self, Pair.with(latches.get(self), childLatches));
        });
    }

    public static class DUUIWorker implements Callable<Void> {
        private final String name;
        private final PipelinePart component;
        private final JCas jc;
        private final DUUIPipelineDocumentPerformance perf;
        private final Iterable<CountDownLatch> selfLatches;
        private final Iterable<CountDownLatch> childLatches;

        public DUUIWorker(String name, 
                    PipelinePart component, 
                    JCas jc, 
                    DUUIPipelineDocumentPerformance perf, 
                    Iterable<CountDownLatch> selfLatches, 
                    Iterable<CountDownLatch> childLatches) {
            this.name = name;
            this.component = component; 
            this.jc = jc; 
            this.perf = perf; 
            this.selfLatches = selfLatches; 
            this.childLatches = childLatches; 
        }

        @Override
        public Void call() throws Exception {
            try {
                for (CountDownLatch latch : selfLatches) {
                    latch.await();
                }; 
                DUUIParallelExecutionPipeline.updatePipelineGraphStatus(name, component.getSignature().toString(), Color.YELLOW2);
                System.out.printf(
                    "[DUUIWorker-%s] Pipeline component %s starting analysis.%n", 
                    Thread.currentThread().getName(), component.getSignature());

                component.run(name, jc, perf); 

                DUUIParallelExecutionPipeline.updatePipelineGraphStatus(name, component.getSignature().toString(), Color.GREEN3);
                System.out.printf(
                    "[DUUIWorker-%s] Pipeline component %s finished analysis.%n", 
                    Thread.currentThread().getName(), component.getSignature());
            
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {  
                childLatches.forEach(CountDownLatch::countDown);
            }

            return null; 
        }
    }
}

class CustomEdge extends DefaultEdge {
    public String getSource() {
        return super.getSource().toString();
    }

    public String getTarget() {
        return super.getTarget().toString();
    }
}
