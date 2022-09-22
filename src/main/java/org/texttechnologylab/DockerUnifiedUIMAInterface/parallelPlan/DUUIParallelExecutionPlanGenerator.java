package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelPlan;

import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIExecutionPlan;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIExecutionPlanGenerator;

import java.util.*;

public class DUUIParallelExecutionPlanGenerator implements IDUUIExecutionPlanGenerator {

    private final Collection<DUUIComposer.PipelinePart> pipelineParts;

    public DUUIParallelExecutionPlanGenerator(Collection<DUUIComposer.PipelinePart> pipelineParts) throws ResourceInitializationException {
        this.pipelineParts = pipelineParts;
    }

    @Override
    public IDUUIExecutionPlan generate(JCas jc) {

        DUUIParallelExecutionPlan root = new DUUIParallelExecutionPlan(null, jc);
        root.setAnnotated();


        // wrap PipelineParts in ParallelExecutionPlans
        Collection<DUUIParallelExecutionPlan> allNodes = new ArrayList<>();
        for (DUUIComposer.PipelinePart part : pipelineParts)
            allNodes.add(new DUUIParallelExecutionPlan(part));

        Collection<DUUIParallelExecutionPlan> remaining = new ArrayList<>(allNodes);

        // build a Map that maps from output to ParallelExecutionPlan
        Map<String, Set<DUUIParallelExecutionPlan>> satisfiesToPipelinePart = new HashMap<>();
        for (DUUIParallelExecutionPlan plan : remaining) {
            for (String s : plan.getOutputs()) {
                if (satisfiesToPipelinePart.containsKey(s))
                    satisfiesToPipelinePart.get(s).add(plan);
                else
                    satisfiesToPipelinePart.put(s, new HashSet<>(Collections.singletonList(plan)));
            }
        }

        // plans with no inputs are processed after root
        for (DUUIParallelExecutionPlan plan : remaining) {
            if (plan.getInputs().size() == 0) {
                root.addNext(plan);
                plan.addPrevious(root);
            }
        }

        // build graph
        Set<String> satisfied = new HashSet<>();
        while (!remaining.isEmpty()) {
            for (Iterator<DUUIParallelExecutionPlan> iterator = remaining.iterator(); iterator.hasNext(); ) {
                DUUIParallelExecutionPlan plan = iterator.next();

                if (satisfied.containsAll(plan.getInputs())) {
                    // run
                    for (String inputs : plan.getInputs())
                        for (DUUIParallelExecutionPlan requiredPlan : satisfiesToPipelinePart.get(inputs)) {
                            // add first plan
                            if(!remaining.contains(requiredPlan)) {  // needed if there are more than one node that with the same output
                                requiredPlan.addNext(plan);
                                plan.addPrevious(requiredPlan);
                                break;
                            }
                        }

                    satisfied.addAll(plan.getOutputs());
                    iterator.remove();
                }
            }
        }

        // add final merge node
        DUUIParallelExecutionPlan endNode = new DUUIParallelExecutionPlan(null);
        for(DUUIParallelExecutionPlan plan:allNodes){
            // add all nodes without successor
            if(plan.getNext().isEmpty()) {
                plan.addNext(endNode);
                endNode.addPrevious(plan);
            }
        }

        //PlanToGraph.writeGraph(PlanToGraph.toGraph(root), "graph.graphml");
        return root;
    }

}

