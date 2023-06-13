package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import static guru.nidi.graphviz.model.Factory.graph;
import static guru.nidi.graphviz.model.Factory.node;
import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.uima.jcas.JCas;
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
    public final int _workers;

    public DUUIParallelExecutionPipeline(Vector<PipelinePart> flow, int workers) {
        super(CustomEdge.class);
        _pipeline = flow.stream()
                        .collect(Collectors.toMap(
                            ppart -> ppart.getUUID(),
                            ppart -> ppart));
        _workers = workers; 

        initialiseDAG();
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

    public static class DUUIWorker implements Callable<Boolean> {
        private final String name;
        private final PipelinePart pipelinePart;
        private final JCas jc;
        private final DUUIPipelineDocumentPerformance perf;
        private final Iterable<CountDownLatch> selfLatches;
        private final Iterable<CountDownLatch> childLatches; 
        private boolean success; 

        public DUUIWorker(String name, 
                    PipelinePart pipelinePart, 
                    JCas jc, 
                    DUUIPipelineDocumentPerformance perf, 
                    Iterable<CountDownLatch> selfLatches, 
                    Iterable<CountDownLatch> childLatches) {
            this.name = name;
            this.pipelinePart = pipelinePart; 
            this.jc = jc; 
            this.perf = perf; 
            this.selfLatches = selfLatches; 
            this.childLatches = childLatches; 
        }

        @Override
        public Boolean call() throws Exception {
            try {
                success = true; 
                
                for (CountDownLatch latch : selfLatches) {
                    latch.await();
                }; 
                System.out.printf("[DUUIWorker-%s] Pipeline component %s starting analysis.\n", 
                    Thread.currentThread().getName(), pipelinePart.getSignature());

                pipelinePart.run(name, jc, perf); 

                System.out.printf("[DUUIWorker-%s] Pipeline component %s finished analysis. Exiting Thread...\n", 
                    Thread.currentThread().getName(), pipelinePart.getSignature());
            
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                success = false; 
                throw e;  
            } finally {  
                childLatches.forEach(CountDownLatch::countDown);
            }
            return success; 
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
