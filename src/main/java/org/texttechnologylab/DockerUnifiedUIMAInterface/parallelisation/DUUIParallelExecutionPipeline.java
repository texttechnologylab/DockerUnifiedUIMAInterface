package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import static guru.nidi.graphviz.model.Factory.graph;
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
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.javatuples.Pair;
import org.javatuples.Tuple;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.GraphCycleProhibitedException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.PipelinePart;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;

import guru.nidi.graphviz.attribute.Rank;
import guru.nidi.graphviz.attribute.Rank.RankDir;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.Graph;

public class DUUIParallelExecutionPipeline extends DirectedAcyclicGraph<String, CustomEdge>  {
    public final Map<String, PipelinePart> _pipeline;
    private final Map<String, Pair<List<CountDownLatch>, List<CountDownLatch>>> _locks; 

    public DUUIParallelExecutionPipeline(Vector<PipelinePart> flow) {
        super(CustomEdge.class);
        _pipeline = flow.stream()
                        .collect(Collectors.toMap(
                            ppart -> ppart.getUUID(),
                            ppart -> ppart));
        _locks = new HashMap<>(_pipeline.size());

        initialiseDAG();
        initialiseLocks();
    }

    public List<Callable<Void>> getWorkers(String name, JCas jc, DUUIPipelineDocumentPerformance perf) {

        return _pipeline.entrySet().stream()
                .map(entry -> 
                {
                    String uuid = entry.getKey();
                    PipelinePart comp = entry.getValue();

                    return new DUUIWorker(
                            name,
                            comp,
                            jc, 
                            perf, 
                            _locks.get(uuid).getValue0(),
                            _locks.get(uuid).getValue1()
                        );
                })
                .collect(Collectors.toList());
    }

    public Set<String> getChildren(String child) {
        return outgoingEdgesOf(child).stream()
            .map(CustomEdge::getTarget)
            .collect(Collectors.toSet());
    }

    public void printPipeline(String name) {

        Graph g = graph("example1").directed()
        .graphAttr().with(Rank.dir(RankDir.TOP_TO_BOTTOM))
        .linkAttr().with("class", "link-class");
        
        for (CustomEdge edge : edgeSet()) {
            g = g.with(
                    node(_pipeline.get(edge.getSource()).getSignature().toString())
                        .link(node(_pipeline.get(edge.getTarget()).getSignature().toString()))
            );
        }

        try {
            Graphviz.fromGraph(g).width(1000).height(600).render(Format.PNG).toFile(
                new File(format("./Execution-Pipeline/%s.png", name)));
            System.out.printf("[DUUIExecutionPipeline] Generated execution graph: ./Execution-Pipeline/%s.png%n", name);
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
            dependencies.forEach(depen -> selfLatches.put(depen, new CountDownLatch(1)));
            latches.put(self, selfLatches);
        });

        _pipeline
        .forEach((self, pipelinePart) -> 
        {
            List<Class<? extends Annotation>> 
                selfOutputs = pipelinePart.getSignature().getOutputs();

            List<CountDownLatch> selfLatches = new ArrayList<>();
            latches.get(self).values().forEach(selfLatches::add);

            List<CountDownLatch> childLatches = new ArrayList<>();
            getChildren(self).stream().map(latches::get).forEach(childLatchesMap -> {
                childLatchesMap.forEach((annotation, lock) -> {
                    if (selfOutputs.contains(annotation))
                        childLatches.add(lock);
                });
            });

            _locks.put(self, Pair.with(selfLatches, childLatches));
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
                System.out.printf(
                    "[DUUIWorker-%s] Pipeline component %s starting analysis.%n", 
                    Thread.currentThread().getName(), component.getSignature());

                component.run(name, jc, perf); 

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
