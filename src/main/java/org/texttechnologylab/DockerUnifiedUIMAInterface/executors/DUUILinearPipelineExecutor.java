package org.texttechnologylab.DockerUnifiedUIMAInterface.executors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.GraphCycleProhibitedException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.Signature;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.Signature.DependencyType;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager;

public class DUUILinearPipelineExecutor extends DirectedAcyclicGraph<String, CustomEdge> implements IDUUIPipelineExecutor {
    
    final Map<String, PipelinePart> _pipeline;
    final Iterable<String> _executionplan;
    final Set<Map.Entry<Class<? extends Annotation>, Boolean>> inputs;
    final IdentityHashMap<Class<? extends Annotation>, Boolean> outputs;
    final int _maxWidth;


    public DUUILinearPipelineExecutor(Vector<PipelinePart> instantiatedPipeline) {
        this(instantiatedPipeline, Integer.MAX_VALUE);
    }

    public DUUILinearPipelineExecutor(Vector<PipelinePart> instantiatedPipeline, int maxWidth) {
        super(CustomEdge.class);

        _maxWidth = maxWidth;
        _pipeline = instantiatedPipeline.stream()
        .collect(Collectors.toMap(
                            ppart -> ppart.getUUID(),
                            ppart -> ppart));

        initialiseDAG();
        _executionplan = () -> iterator();

        inputs =
            _pipeline.values().stream()
            .map(PipelinePart::getSignature)
            .map(Signature::getInputs)
            .flatMap(List::stream)
            .distinct()
            .collect(Collectors.toMap(Function.identity(), (t) -> Boolean.TRUE, (o, n) -> o, IdentityHashMap::new)).entrySet();

        outputs =
            _pipeline.values().stream()
            .map(PipelinePart::getSignature)
            .map(Signature::getOutputs)
            .flatMap(List::stream)
            .distinct()
            .collect(Collectors.toMap(Function.identity(), (t) -> Boolean.TRUE, (o, n) -> o, IdentityHashMap::new));

    }

    public void run(String name, JCas jc, DUUIPipelineDocumentPerformance perf) throws Exception {
        for (String component : _executionplan) {
            PipelinePart part = _pipeline.get(component);
            try {
                if (typeCheck(jc)) {
                    part.run(name, jc, perf);
                    System.out.printf("[%s] finished analysis.%n", name + "-" + part.getSignature());
                } else System.out.printf("[%s] failed type checking.%n", name + "-" + part.getSignature());
            } catch (Exception e) {
                System.out.printf("[%s] failed analysis.%n", name + "-" + part.getSignature());
            }
        }
        
        if (DUUIComposer.Config.storage() != null) 
            DUUIComposer.Config.storage().addMetricsForDocument(perf);
        ResourceManager.getInstance().returnCas(jc);
    }

    boolean typeCheck(JCas jc) {
        if (!DUUIComposer.Config.typeCheck()) return true; 
        for (Map.Entry<Class<? extends Annotation>, Boolean> type : inputs) {
            if (JCasUtil.select(jc, type.getKey()).isEmpty()) { // && !outputs.containsKey(type.getKey())
                return false;
                // throw new RuntimeException(String.format("Input type %s missing! Document cannot be processed. ", type.getKey()));
            }
        }
        return true;
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
                if (part1.getUUID() == part2.getUUID()) continue;

                this.addVertex(part2.getUUID());
                switch (part1.getSignature().compare(part2.getSignature())) {
                    // Direct Dependency
                    case LEFT_TO_RIGHT:
                        this.addEdge(part1.getUUID(), part2.getUUID());
                        break;
                    case RIGHT_TO_LEFT: 
                        this.addEdge(part2.getUUID(), part1.getUUID());
                        break;
                    // Cycle
                    case CYCLE:
                        System.out.println("[DUUIExecutionPipeline] There is a circular dependency between the pipeline components. "
                            + "Parallel execution is impossible!");
                        System.out.printf("Components: %s, %s", part1.getSignature(), part2.getSignature());
                        throw new GraphCycleProhibitedException();
                    // No-Dependency
                    case NO_DEPENDENCY:
                    default:
                        break;
                }
            }
        }

        
        Map<String, Integer> heights = new HashMap<>(_pipeline.size());
        List<String> comps = _pipeline.values().stream().map(PipelinePart::getUUID)
            .peek(uuid -> heights.put(uuid, getHeight(uuid)))
            .sorted((uuid1, uuid2) -> Integer.compare(getAncestors(uuid1).size(), getAncestors(uuid2).size()))
            .sorted((uuid1, uuid2) -> Integer.compare(getDescendants(uuid1).size(), getDescendants(uuid2).size()))
            .sorted((uuid1, uuid2) -> Integer.compare(getHeight(uuid1), getHeight(uuid2)))
            .collect(Collectors.toList());
        final List<?>[] compsFinal = { comps };
        int depth = comps.stream().mapToInt(this::getHeight).max().orElse(1);
        final int[] depthFinal = { depth };

        BooleanSupplier isLessThanWidth = () -> IntStream.rangeClosed(1, depthFinal[0])
            .allMatch(height -> compsFinal[0].stream().filter(comp -> getHeight((String)comp) == height).count() <= _maxWidth);
        // MAKE MAP FOR PREVIOUS HEIGHTS TO SORT COMPONENTS BY CURRENT HEIGHT FIRST; PREVIOUS SECOND!
        do {
            for (int i = comps.size() - 1; i > 0; i--) {
                // System.out.printf("%s: %d AND %s: %d \n", 
                //     _pipeline.get(comps.get(i - 1)).getSignature(), getHeight(comps.get(i - 1)), 
                //     _pipeline.get(comps.get(i)).getSignature(), getHeight(comps.get(i)));
                if (comps.stream().map(this::getHeight).filter(Predicate.isEqual(getHeight(comps.get(i - 1)))).count() > _maxWidth) {
                    this.addEdge(comps.get(i - 1), comps.get(i));
                    comps = _pipeline.values().stream().map(PipelinePart::getUUID)
                        .sorted((uuid1, uuid2) -> Integer.compare(getAncestors(uuid1).size(), getAncestors(uuid2).size()))
                        .sorted((uuid1, uuid2) -> Integer.compare(getDescendants(uuid1).size(), getDescendants(uuid2).size()))
                        .sorted((uuid1, uuid2) -> Integer.compare(getHeight(uuid1), getHeight(uuid2)))
                        .sorted((uuid1, uuid2) -> Integer.compare(heights.get(uuid1), heights.get(uuid2)))
                        .collect(Collectors.toList());
                    depthFinal[0] = comps.stream().mapToInt(this::getHeight).max().orElse(1);
                }
        }
        } while (!isLessThanWidth.getAsBoolean());
        System.out.println();
        comps.stream().peek(c -> System.out.println(_pipeline.get(c).getSignature())).peek(c -> System.out.println(getHeight(c))).forEach(System.out::println);
        System.out.println();
    }

}

//  x  x  x  x
// x x  x  x  x

class CustomEdge extends DefaultEdge {
    public String getSource() {
        return super.getSource().toString();
    }

    public String getTarget() {
        return super.getTarget().toString();
    }
}
