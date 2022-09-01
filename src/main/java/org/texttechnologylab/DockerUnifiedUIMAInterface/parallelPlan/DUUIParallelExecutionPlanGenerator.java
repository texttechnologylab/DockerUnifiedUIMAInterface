package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelPlan;

import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIExecutionPlan;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIExecutionPlanGenerator;

import java.util.*;

public class DUUIParallelExecutionPlanGenerator implements IDUUIExecutionPlanGenerator {

    private final Collection<DUUIComposer.PipelinePart> pipelineParts;

    public DUUIParallelExecutionPlanGenerator(Collection<DUUIComposer.PipelinePart> pipelineParts) {
        this.pipelineParts = pipelineParts;
    }

    @Override
    public IDUUIExecutionPlan generate(JCas jc) {

        DUUIParallelExecutionPlan root = new DUUIParallelExecutionPlan(null, jc);

        Set<String> satisfied = new HashSet<>();

        // wrap PipelineParts in ParallelExecutionPlans
        Collection<DUUIParallelExecutionPlan> remaining = new ArrayList<>();
        for (DUUIComposer.PipelinePart part : pipelineParts)
            remaining.add(new DUUIParallelExecutionPlan(part));

        // build a Map that maps from output to ParallelExecutionPlan
        Map<String, Set<DUUIParallelExecutionPlan>> satisfiesToPipelinePart = new HashMap<>();
        for (DUUIParallelExecutionPlan plan : remaining) {
            try {
                for (String s : plan.getPipelinePart().getDriver().getInputsOutputs(plan.getPipelinePart().getUUID()).getOutputs()) {
                    if (satisfiesToPipelinePart.containsKey(s))
                        satisfiesToPipelinePart.get(s).add(plan);
                    else
                        satisfiesToPipelinePart.put(s, new HashSet<>(Collections.singletonList(plan)));
                }
            } catch (ResourceInitializationException e) {
                throw new RuntimeException(e);
            }
        }

        // plans with no inputs are processed after root
        for (DUUIParallelExecutionPlan plan : remaining) {
            try {
                if (plan.getPipelinePart().getDriver().getInputsOutputs(plan.getPipelinePart().getUUID()).getInputs().size() == 0) {
                    root.addNext(plan);
                }
            } catch (ResourceInitializationException e) {
                throw new RuntimeException(e);
            }
        }

        while (!remaining.isEmpty()) {
            for (Iterator<DUUIParallelExecutionPlan> iterator = remaining.iterator(); iterator.hasNext(); ) {
                DUUIParallelExecutionPlan plan = iterator.next();

                try {
                    if (satisfied.containsAll(plan.getPipelinePart().getDriver().getInputsOutputs(plan.getPipelinePart().getUUID()).getInputs())) {
                        // run
                        for (String inputs : plan.getPipelinePart().getDriver().getInputsOutputs(plan.getPipelinePart().getUUID()).getInputs())
                            for (DUUIParallelExecutionPlan requiredPlan : satisfiesToPipelinePart.get(inputs)) {
                                requiredPlan.addNext(plan);
                                plan.addPrevious(requiredPlan);
                            }

                        satisfied.addAll(plan.getPipelinePart().getDriver().getInputsOutputs(plan.getPipelinePart().getUUID()).getOutputs());
                        iterator.remove();
                    }
                } catch (ResourceInitializationException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return root;
    }

}

