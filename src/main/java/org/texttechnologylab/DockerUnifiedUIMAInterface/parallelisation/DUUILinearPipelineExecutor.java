package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;

import org.apache.uima.jcas.JCas;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.GraphCycleProhibitedException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.PipelinePart;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;

public class DUUILinearPipelineExecutor extends DirectedAcyclicGraph<String, CustomEdge> implements IDUUIPipelineExecutor {
    
    public final Map<String, PipelinePart> _pipeline;
    public final Iterable<String> _executionplan;


    public DUUILinearPipelineExecutor(Vector<PipelinePart> instantiatedPipeline) {
        super(CustomEdge.class);

        _pipeline = instantiatedPipeline.stream()
        .collect(Collectors.toMap(
                            ppart -> ppart.getUUID(),
                            ppart -> ppart));

        initialiseDAG();
        _executionplan = () -> iterator();

    }

    public void run(String name, JCas jc, DUUIPipelineDocumentPerformance perf) throws Exception {

        for (String component : _executionplan) {
            PipelinePart part = _pipeline.get(component);
            part.run(name, jc, perf);
        }
        
        DUUIComposer.Config.write(jc);
    }
    
    public int getHeight(String node) {
        if (incomingEdgesOf(node).isEmpty())
            return 1;

        return 1 + incomingEdgesOf(node).stream()
            .map(CustomEdge::getSource)
            .mapToInt(this::getHeight)
            .max().orElse(0);

    }

    public Set<String> getChildren(String child) {
        return outgoingEdgesOf(child).stream()
            .map(CustomEdge::getTarget)
            .collect(Collectors.toSet());
    }

    public Map<String, Set<String>> getGraph() {
        Map<String, Set<String>> graph = new LinkedHashMap<>(_pipeline.size());

        for (String parent : _executionplan) {
            graph.put(
                _pipeline.get(parent).getSignature().toString(), 
                getChildren(parent)
                    .stream()
                    .map(c -> _pipeline.get(c).getSignature().toString())
                    .collect(Collectors.toSet())
            );
        }
        return graph; 
    }

    private void initialiseDAG() {

        for (PipelinePart part1 : _pipeline.values()) {
            this.addVertex(part1.getUUID());
            for (PipelinePart part2 : _pipeline.values()) {
                if (part1.getUUID() == part2.getUUID()) 
                    continue;

                this.addVertex(part2.getUUID());

                try {

                    switch (part1.getSignature().compare(part2.getSignature())) {
                        // Direct Dependency
                        case 1:
                            this.addEdge(part1.getUUID(), part2.getUUID());
                            break;
                        case -1: 
                            this.addEdge(part2.getUUID(), part1.getUUID());
                            break;
                        // Cycle
                        case -2:
                            throw new GraphCycleProhibitedException();
                        // No-Dependency
                        default:
                            break;
                    }
                } catch (GraphCycleProhibitedException e) {
                    System.out.println("[DUUIExecutionPipeline] There is a circular dependency between the pipeline components. "
                        + "Parallel execution is impossible!");
                    System.out.printf("Components: %s, %s", part1.getSignature(), part2.getSignature());
                    throw e;
                }
            }
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
