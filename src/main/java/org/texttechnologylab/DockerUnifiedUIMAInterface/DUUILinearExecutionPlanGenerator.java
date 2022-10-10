package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.uima.jcas.JCas;

import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class DUUILinearExecutionPlanGenerator implements IDUUIExecutionPlanGenerator {
    private Vector<DUUIComposer.PipelinePart> _pipeline;


    public class DUUILinearExecutionPlan implements IDUUIExecutionPlan {
        private Vector<DUUIComposer.PipelinePart> _pipeline;
        private JCas _jc;
        int _index;

        public DUUILinearExecutionPlan(Vector<DUUIComposer.PipelinePart> flow, JCas jc) {
            _pipeline = flow;
            _jc = jc;
            _index = 0;
        }

        public DUUILinearExecutionPlan(Vector<DUUIComposer.PipelinePart> flow, JCas jc, int index) {
            _pipeline = flow;
            _jc = jc;
            _index = index;
        }

        public List<IDUUIExecutionPlan> getNextExecutionPlans() {
            LinkedList<IDUUIExecutionPlan> exec = new LinkedList<>();
            if(_index < _pipeline.size()) {
                exec.add(new DUUILinearExecutionPlan(_pipeline, _jc, _index + 1));
            }
            return exec;
        }

        public DUUIComposer.PipelinePart getPipelinePart() {
            if(_index < _pipeline.size()) {
                return _pipeline.get(_index);
            }
            else {
                return null;
            }
        }

        public JCas getJCas() {
            return _jc;
        }

        //Wait for previous nodes, merge results of previous nodes.
        public Future<IDUUIExecutionPlan> awaitMerge() {
            return CompletableFuture.completedFuture(this);
        }
    }

    public DUUILinearExecutionPlanGenerator(Vector<DUUIComposer.PipelinePart> pipeline) {
        _pipeline = pipeline;
    }

    public IDUUIExecutionPlan generate(JCas jc) {
        return new DUUILinearExecutionPlan(_pipeline,jc);
    }
}
