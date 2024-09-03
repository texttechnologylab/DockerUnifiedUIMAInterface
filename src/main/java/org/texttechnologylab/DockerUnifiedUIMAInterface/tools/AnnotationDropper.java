package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.Type;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;

/**
 * @author Manuel Stoeckel
 * @version 0.2.0
 */
public class AnnotationDropper extends JCasAnnotator_ImplBase {
    /**
     * The types to drop from the CAS.
     * Must be the fully qualified class name of the type.
     */
    public static final String PARAM_TYPES_TO_DROP = "typesToDrop";
    @ConfigurationParameter(name = PARAM_TYPES_TO_DROP, mandatory = false, defaultValue = {})
    private String[] paramTypesToDrop;

    /**
     * The types to drop from the CAS.
     * Must be the fully qualified class name of the type.
     */
    public static final String PARAM_TYPES_TO_RETAIN = "typesToRetain";
    @ConfigurationParameter(name = PARAM_TYPES_TO_RETAIN, mandatory = false, defaultValue = {})
    private String[] paramTypesToRetain;

    enum Mode {
        _UNSET,
        RETAIN,
        DROP
    }

    private Mode mode = Mode._UNSET;
    private HashSet<String> typeSet = new HashSet<>();

    public Mode getMode() {
        switch (this.mode) {
            case RETAIN:
                return Mode.RETAIN;
            case DROP:
                return Mode.DROP;
            case _UNSET:
            default:
                throw new IllegalStateException("Mode is unset");
        }
    }

    public Set<String> getTypeSet() {
        return Set.copyOf(this.typeSet);
    }

    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        super.initialize(context);

        if (this.paramTypesToDrop.length == 0 && this.paramTypesToRetain.length == 0) {
            throw new ResourceInitializationException(
                    new IllegalArgumentException("At least one of typesToDrop or typesToRetain must be set"));
        } else if (this.paramTypesToDrop.length > 0 && this.paramTypesToRetain.length > 0) {
            throw new ResourceInitializationException(
                    new IllegalArgumentException("Only one of typesToDrop or typesToRetain can be set"));
        }

        if (this.paramTypesToDrop.length > 0) {
            this.mode = Mode.DROP;
            this.typeSet = new HashSet<>(List.of(this.paramTypesToDrop));
        } else {
            this.mode = Mode.RETAIN;
            this.typeSet = new HashSet<>(List.of(this.paramTypesToRetain));
        }
    }

    @Override
    public void process(JCas aJCas) throws AnalysisEngineProcessException {
        switch (this.mode) {
            case RETAIN:
                retainTypes(aJCas, this.typeSet);
                break;
            case DROP:
                dropTypes(aJCas, this.typeSet);
                break;
        }
    }

    static void retainTypes(JCas aJCas, Set<String> typesToRetain) {
        Set<String> typesToDrop = aJCas.getAnnotationIndex().iterator()
                .stream()
                .map(a -> a.getType().getName())
                .distinct()
                .filter(Predicate.not(typesToRetain::contains))
                .collect(Collectors.toSet());

        dropTypes(aJCas, typesToDrop);
    }

    static void dropTypes(JCas aJCas, Iterable<String> typesToDrop) {
        for (String typeName : typesToDrop) {
            dropType(aJCas, typeName);
        }
    }

    static void dropType(JCas aJCas, String typeName) {
        Type type = aJCas.getTypeSystem().getType(typeName);
        aJCas.select(type).forEach(a -> a.removeFromIndexes(aJCas));
    }
}
