package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.uima.jcas.tcas.Annotation;


/**
 * Data-structure storing the inputs and outputs of a Component. 
 * 
 */
public class Signature {

    /**
     * Dependency types used to determiine the dependency between two Components.
     * 
     * {@link DependencyType.FIRST} and {@link DependencyType.LAST} are used for special Components which need
     * to be ran at the beginning or the end of the pipeline. 
     */
    public static enum DependencyType {
        FIRST,
        LAST,
        NORMAL,
        CYCLE,
        LEFT_TO_RIGHT,
        RIGHT_TO_LEFT,
        NO_DEPENDENCY
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

    /**
     * Compare two signatures.
     * 
     * @param s2 Signature to compare current signature with.
     * @return Dependency relationship between the signatures.
     */
    public DependencyType compare(Signature s2) {
        switch (type) {
            case FIRST:
                if (s2.type == DependencyType.FIRST)
                    return DependencyType.CYCLE; // Error: Cycle 
                else return DependencyType.LEFT_TO_RIGHT;
            case LAST:
                if (s2.type == DependencyType.LAST)
                    return DependencyType.CYCLE; // Error: Cycle 
                else return DependencyType.RIGHT_TO_LEFT;
            default:
                boolean s1DependentOnS2 = this.getInputs().stream()
                .anyMatch(s2.getOutputs()::contains);
                
                boolean s2DependentOnS1 = s2.getInputs().stream()
                    .anyMatch(this.getOutputs()::contains);
        
        
        
                if (s1DependentOnS2 && s2DependentOnS1) {
                    return DependencyType.CYCLE; // Error: Cycle 
                } else if (s2DependentOnS1) {
                    return DependencyType.LEFT_TO_RIGHT; // Full-Dependency edge from s1 to s2
                } else if (s1DependentOnS2) {
                    return DependencyType.RIGHT_TO_LEFT; // Full-Dependency edge from s2 to s1
                } else {
                    return DependencyType.NO_DEPENDENCY; // No-Edge
                } 
        }
    }
}