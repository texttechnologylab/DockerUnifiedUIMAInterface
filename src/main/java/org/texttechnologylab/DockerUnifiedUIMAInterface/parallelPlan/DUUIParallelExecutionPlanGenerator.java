package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelPlan;

import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIExecutionPlan;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIExecutionPlanGenerator;
import org.texttechnologylab.DockerUnifiedUIMAInterface.InputsOutputs;

import java.util.*;

public class DUUIParallelExecutionPlanGenerator implements IDUUIExecutionPlanGenerator {

    private final Collection<DUUIComposer.PipelinePart> pipelineParts;
    private final Map<String, InputsOutputs> part2IO;

    public DUUIParallelExecutionPlanGenerator(Collection<DUUIComposer.PipelinePart> pipelineParts) throws ResourceInitializationException {
        this.pipelineParts = pipelineParts;
        // cache input and output
        part2IO = new TreeMap<>();
        for (DUUIComposer.PipelinePart part : pipelineParts)
            part2IO.put(part.getUUID(), part.getDriver().getInputsOutputs(part.getUUID()));
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
            for (String s : part2IO.get(plan.getPipelinePart().getUUID()).getOutputs()) {
                if (satisfiesToPipelinePart.containsKey(s))
                    satisfiesToPipelinePart.get(s).add(plan);
                else
                    satisfiesToPipelinePart.put(s, new HashSet<>(Collections.singletonList(plan)));
            }
        }

        // plans with no inputs are processed after root
        for (DUUIParallelExecutionPlan plan : remaining) {
            if (part2IO.get(plan.getPipelinePart().getUUID()).getInputs().size() == 0) {
                root.addNext(plan);
            }
        }

        while (!remaining.isEmpty()) {
            for (Iterator<DUUIParallelExecutionPlan> iterator = remaining.iterator(); iterator.hasNext(); ) {
                DUUIParallelExecutionPlan plan = iterator.next();

                if (satisfied.containsAll(part2IO.get(plan.getPipelinePart().getUUID()).getInputs())) {
                    // run
                    for (String inputs : part2IO.get(plan.getPipelinePart().getUUID()).getInputs())
                        for (DUUIParallelExecutionPlan requiredPlan : satisfiesToPipelinePart.get(inputs)) {
                            requiredPlan.addNext(plan);
                            plan.addPrevious(requiredPlan);
                        }

                    satisfied.addAll(part2IO.get(plan.getPipelinePart().getUUID()).getOutputs());
                    iterator.remove();
                }
            }
        }
        return root;
    }

}

