package parallelExecution;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIExecutionPlan;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUIExecutionPlanGenerator;

import java.util.*;

public class DUUIParallelExecutionPlanGenerator implements IDUUIExecutionPlanGenerator {
    DUUIParallelExecutionPlan root = new DUUIParallelExecutionPlan(null,null);


    public DUUIParallelExecutionPlanGenerator(Collection<DUUIComposer.PipelinePart> pipelineParts) {
        Set<String> satisfied = new HashSet<>();

        // wrap PipelineParts in ParallelExecutionPlans
        Collection<DUUIParallelExecutionPlan> remaining = new ArrayList<>();
        for(DUUIComposer.PipelinePart part:pipelineParts)
            remaining.add(new DUUIParallelExecutionPlan(part, null));

        // build a Map that maps from output to ParallelExecutionPlan
        Map<String, Set<DUUIParallelExecutionPlan>> satisfiesToPipelinePart = new HashMap<>();
        for (DUUIParallelExecutionPlan plan : remaining) {
            for (String s : plan.getPipelinePart().getDriver().getInputsOutputs().getOutputs()) {
                if (satisfiesToPipelinePart.containsKey(s))
                    satisfiesToPipelinePart.get(s).add(plan);
                else
                    satisfiesToPipelinePart.put(s, new HashSet<>(Collections.singletonList(plan)));
            }
        }

        // plans with no inputs are processed after root
        for (DUUIParallelExecutionPlan plan : remaining) {
            if (plan.getPipelinePart().getDriver().getInputsOutputs().getInputs().size() == 0) {
                root.addNext(plan);
            }
        }

        while (!remaining.isEmpty()) {
            for (Iterator<DUUIParallelExecutionPlan> iterator = remaining.iterator(); iterator.hasNext();) {
                DUUIParallelExecutionPlan plan = iterator.next();

                if (satisfied.containsAll(plan.getPipelinePart().getDriver().getInputsOutputs().getInputs())) {
                    // run
                    for (String inputs : plan.getPipelinePart().getDriver().getInputsOutputs().getInputs())
                        for (DUUIParallelExecutionPlan requiredPlan : satisfiesToPipelinePart.get(inputs)) {
                            requiredPlan.addNext(plan);
                            plan.addPrevious(requiredPlan);
                        }

                    satisfied.addAll(plan.getPipelinePart().getDriver().getInputsOutputs().getOutputs());
                    iterator.remove();
                }
            }
        }
    }

    @Override
    public IDUUIExecutionPlan generate(JCas jc) {
        root.setJCas(jc);
        return root;
    }
}
