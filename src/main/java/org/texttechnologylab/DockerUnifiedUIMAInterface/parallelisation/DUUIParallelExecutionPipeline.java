package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import static guru.nidi.graphviz.model.Factory.mutGraph;
import static guru.nidi.graphviz.model.Factory.graphAttrs;
import static guru.nidi.graphviz.model.Factory.linkAttrs;
import static guru.nidi.graphviz.model.Factory.node;
import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.javatuples.Pair;
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
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.Node;

public class DUUIParallelExecutionPipeline extends DirectedAcyclicGraph<String, CustomEdge>  {
    public final Map<String, PipelinePart> _pipeline;
    public final Iterable<String> _executionplan; 
    private static Map<String, MutableGraph> _pipelineGraph = new HashMap<>(); 
    private final Map<String, Callable<Void>> _componentRunners = new ConcurrentHashMap<>(); 

    public DUUIParallelExecutionPipeline(Vector<PipelinePart> flow) {
        super(CustomEdge.class);
        _pipeline = flow.stream()
                        .collect(Collectors.toMap(
                            ppart -> ppart.getUUID(),
                            ppart -> ppart));

        initialiseDAG();
        _executionplan = () -> iterator();
    }

    public List<Callable<Void>> getAllComponentRunners(String name, JCas jc, DUUIPipelineDocumentPerformance perf) throws Exception {

        List<Callable<Void>> compRunners = new ArrayList<>(_pipeline.size()); 

        for (String compId : _pipeline.keySet()) {
            compRunners.add(getComponentRunner(name, compId, jc, perf));
        }
        
        return compRunners; 
    }

    public boolean runnerExists(String name, String uuid) {
        return _componentRunners.containsKey(name+uuid);
    }

    public Callable<Void> getRunner(String name, String uuid) {
        return _componentRunners.get(name+uuid);
    }

    public Callable<Void> getComponentRunner(String name, String uuid, JCas jc, DUUIPipelineDocumentPerformance perf) throws Exception {

        if (_componentRunners.containsKey(name+uuid)) 
            return _componentRunners.get(name+uuid);

        initialiseComponentRunners(name, jc, perf);

        return _componentRunners.get(name+uuid);
    }

    public synchronized void initialiseComponentRunners(String name, JCas jc, DUUIPipelineDocumentPerformance perf) throws Exception {

        // _pipelineGraph.put(name, mutGraph("example1").setDirected(true).use( (gr, ctx) -> {
        //     graphAttrs().add(Rank.dir(RankDir.TOP_TO_BOTTOM));
        //     linkAttrs().add("class", "link-class");
        // }));
        // for(String vertex : vertexSet()) {
        //     Node parent = node(_pipeline.get(vertex).getSignature().toString());  
        //     _pipelineGraph.get(name).add(parent);
        //     for (String child : getChildren(vertex)) {
        //         _pipelineGraph.get(name).add(parent.link(node(_pipeline.get(child).getSignature().toString())));
        //     }
        // }

        // printPipeline(name);

        Map<String, Pair<Iterable<ComponentLock>, Iterable<ComponentLock>>> 
            _locks = initialiseLocks(jc);
        _pipeline.forEach((uuid, comp) -> 
            {
                Iterable<ComponentLock> selfLocks = _locks.get(uuid).getValue0();
                Iterable<ComponentLock> childLocks = _locks.get(uuid).getValue1();

                _componentRunners.put(name+uuid, new DUUIWorker(name, comp, jc, perf, selfLocks, childLocks));
            });

    }

    public Set<String> getChildren(String child) {
        return outgoingEdgesOf(child).stream()
            .map(CustomEdge::getTarget)
            .collect(Collectors.toSet());
    }

    public synchronized static void updatePipelineGraphStatus(String name, String signature, Color progress) {
        _pipelineGraph.get(name).nodes().forEach(comp -> {
            if (comp.name().contentEquals(signature)) {
                comp.add(progress, Style.lineWidth(2), Style.RADIAL);
            }
        });
        printPipeline(name);
    }

    public static void printPipeline(String name) {
        try {
            Graphviz.fromGraph(_pipelineGraph.get(name)).width(1920).height(1080).render(Format.PNG).toFile(
                new File(format("./Execution-Pipeline/%s.png", name)));
            // System.out.printf("[DUUIExecutionPipeline] Generated execution graph: ./Execution-Pipeline/%s.png%n", name);
        } catch (Exception e) {
            System.out.println(e.getMessage());
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

    private Map<String, Pair<Iterable<ComponentLock>, Iterable<ComponentLock>>> initialiseLocks(JCas jc) {
        
        Map<String, Map<Class<? extends Annotation>, ComponentLock>> 
            latches = new ConcurrentHashMap<>(_pipeline.size()); 
        Map<String, Map<Class<? extends Annotation>, AtomicInteger>> 
            latchesCovered = new ConcurrentHashMap<>(_pipeline.size()); 
        Map<String, List<ComponentLock>> childLatches = new HashMap<>(_pipeline.size()); 
        // for every node save a lock for every dependency if not already in jc 
        for (PipelinePart part : _pipeline.values()) {
            String self = part.getUUID();
            List<Class<? extends Annotation>> dependencies = part.getSignature().getInputs();
            Map<Class<? extends Annotation>, ComponentLock> selfLatches = 
                new HashMap<>(dependencies.size());
            Map<Class<? extends Annotation>, AtomicInteger> selfLatchesCovered = 
                new HashMap<>(dependencies.size());
            for (Class<? extends Annotation> dependency : dependencies) {
                if (JCasUtil.select(jc, dependency).isEmpty()) {
                    selfLatches.put(dependency, new ComponentLock(1));
                    selfLatchesCovered.put(dependency, new AtomicInteger(1));
                }
            }
            latches.put(self, selfLatches);
            latchesCovered.put(self, selfLatchesCovered);
        }

        // for every child save every lock which corresponds to a provided output
        for (PipelinePart part : _pipeline.values()) {
            String self = part.getUUID();
            List<Class<? extends Annotation>> outputs = part.getSignature().getOutputs();
            List<ComponentLock> childLocks =  new ArrayList<>();
            for (String child : getChildren(self)) {
                for (Class<? extends Annotation> dependency : latches.get(child).keySet()) {
                    if (outputs.contains(dependency)) {
                        childLocks.add(latches.get(child).get(dependency));
                        latchesCovered.get(child).get(dependency).decrementAndGet();
                    }
                }
            }
            childLatches.put(self, childLocks);
        }

        // check that for every component every input is provided
        if (!latchesCovered.values().stream()
        .map(map -> map.values())
        .filter(atomintlist -> atomintlist.size() > 0)
        .map(atomintlist -> atomintlist.stream().mapToInt(atomint -> atomint.get()))
        .allMatch(intlist -> intlist.max().getAsInt() < 1))
            throw new RuntimeException(
                "[ExecutionPipeline] There are pipeline components who will never get their inputs!");

        return latches.entrySet().stream()
            .map(entry -> 
            {
                String self = entry.getKey();
                Iterable<ComponentLock> selfLocks = entry.getValue().values(); 
                Iterable<ComponentLock> childLocks = childLatches.get(self);
                return Pair.with(self, Pair.with(selfLocks, childLocks));
            }).collect(Collectors.toMap(Pair::getValue0, Pair::getValue1));
        
    }

    public static class DUUIWorker implements Callable<Void> {
        private final String name;
        private final PipelinePart component;
        private final JCas jc;
        private final DUUIPipelineDocumentPerformance perf;
        private final Iterable<ComponentLock> selfLocks;
        private final Iterable<ComponentLock> childrenLocks;

        public DUUIWorker(String name, 
                    PipelinePart component, 
                    JCas jc, 
                    DUUIPipelineDocumentPerformance perf, 
                    Iterable<ComponentLock> selfLatches, 
                    Iterable<ComponentLock> childLatches) {
            this.name = name;
            this.component = component; 
            this.jc = jc; 
            this.perf = perf; 
            this.selfLocks = selfLatches; 
            this.childrenLocks = childLatches; 
        }

        @Override
        public Void call() throws Exception {
            try {
                for (ComponentLock parent : selfLocks) {
                    parent.await(1, TimeUnit.SECONDS);
                    if (parent.failed()) {
                        throw new Exception(format("[DUUIWorker-%s][%s][Component: %s] Parent failed.%n", 
                            Thread.currentThread().getName(), name, component.getSignature()));
                    }
                }
                
                // updatePipelineGraphStatus(name, component.getSignature().toString(), Color.YELLOW2);
                System.out.printf(
                    "[DUUIWorker-%s][%s] Pipeline component %s starting analysis.%n", 
                    Thread.currentThread().getName(), name, component.getSignature());

                component.run(name, jc, perf); 

                // updatePipelineGraphStatus(name, component.getSignature().toString(), Color.GREEN3);
                System.out.printf(
                    "[DUUIWorker-%s][%s] Pipeline component %s finished analysis.%n", 
                    Thread.currentThread().getName(), name, component.getSignature());
                
                for (ComponentLock childLock : childrenLocks)
                    childLock.countDown(); // release
            } catch (Exception e) {
                // updatePipelineGraphStatus(name, component.getSignature().toString(), Color.RED3);
                System.out.printf("[DUUIWorker-%s][%s] Pipeline component %s failed.%n",
                    Thread.currentThread().getName(), name, component.getSignature());
                for (ComponentLock childLock : childrenLocks)
                    childLock.fail();
                throw e;
            }

            return null; 
        }
    }

    private static class ComponentLock extends CountDownLatch {

        AtomicBoolean failed = new AtomicBoolean(false);

        public ComponentLock(int count) {
            super(count);
        }

        public boolean failed() {
            return failed.get();
        }

        public void fail() {
            failed.set(true);
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
