package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.uima.jcas.tcas.Annotation;

public class Signature {

    public static enum DependencyType {
        FIRST,
        LAST,
        NORMAL
    }

    
    final List<Class<? extends Annotation>> inputs;
    final List<Class<? extends Annotation>> outputs;
    final DependencyType type;
    
    public Signature(DependencyType type) {
        switch (type) {
            case FIRST:
            case LAST:
                this.type = type;
                break;
            default:
                throw new IllegalArgumentException("Only dependency types FIRST and LAST are valid!");
        }

        inputs = outputs = new ArrayList<>();
    }

    public Signature(List<Class<? extends Annotation>> inputs, List<Class<? extends Annotation>> outputs) {
        this.inputs = inputs;
        this.outputs = outputs;
        this.type = DependencyType.NORMAL;
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
        return String.join(" ", 
            outputs.stream().map(Class::getSimpleName).collect(Collectors.toList()));
    }

    public int compare(Signature s2) {
        switch (type) {
            case FIRST:
                if (s2.type == DependencyType.FIRST)
                    return -2; // Error: Cycle 
                else return 1;
            case LAST:
                if (s2.type == DependencyType.LAST)
                    return -2; // Error: Cycle 
                else return -1;
            default:
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
}