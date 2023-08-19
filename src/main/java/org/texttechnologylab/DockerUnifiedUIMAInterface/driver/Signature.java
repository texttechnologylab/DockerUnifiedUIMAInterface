package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import java.util.List;

import org.apache.uima.jcas.tcas.Annotation;

public class Signature {

    private final List<Class<? extends Annotation>> inputs;
    private final List<Class<? extends Annotation>> outputs;

    public Signature(List<Class<? extends Annotation>> inputs, List<Class<? extends Annotation>> outputs) {
        this.inputs = inputs;
        this.outputs = outputs;
    }

    public List<Class<? extends Annotation>> getInputs() {
        return inputs;
    }

    public List<Class<? extends Annotation>> getOutputs() {
        return outputs;
    }

    @Override
    public boolean equals(Object s2) {
        
        if (!(s2 instanceof Signature)) return false;

        return ((Signature)s2).getInputs().equals(this.getInputs()) &&
        ((Signature)s2).getOutputs().equals(this.getOutputs());
    }

    @Override
    public String toString() {
        StringBuilder signature = new StringBuilder();

        // this.inputs.forEach( input ->
        //     signature.append(input.getSimpleName() + " ")
        // );
        // signature.append(" => ");
        this.outputs.forEach( output ->
        signature.append(output.getSimpleName() + " ")
        );

        return signature.toString();
    }

    public int compare(Signature s2) {

        boolean s1DependentOnS2 = this.getInputs().stream()
        .anyMatch(s2.getOutputs()::contains);
        
        boolean s2DependentOnS1 = s2.getInputs().stream()
            .anyMatch(this.getOutputs()::contains);

        if (s1DependentOnS2 && s2DependentOnS1) {
            return -2; // Error: Cycle 
        } else if (s2DependentOnS1) {
            return 1; // Full-Dependency edge from s1 to s2
        } else if (s1DependentOnS2) {
            return -1; // Full-Dependency edge from s2 to s1
        } else {
            return 0; // No-Edge
        } 
    }
}