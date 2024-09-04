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
 * A {@link JCasAnnotator_ImplBase JCasAnnotator} that drops or retains specific
 * types from processed CASes.
 * 
 * @author Manuel Stoeckel
 * @version 0.2.0
 */
public class AnnotationDropper extends JCasAnnotator_ImplBase {
    /**
     * The types to drop from the CAS.
     * Must be the fully qualified class name of the type.
     * 
     * @apiNote You can use the
     *          {@link org.apache.uima.jcas.cas.TOP#_TypeName _TypeName} field of
     *          any {@link org.apache.uima.jcas.tcas.Annotation annotation} to
     *          access the fully qualified class name for convenience.
     * @apiNote Only one of {@link #PARAM_TYPES_TO_DROP} or
     *          {@link #PARAM_TYPES_TO_RETAIN} can be set.
     */
    public static final String PARAM_TYPES_TO_DROP = "typesToDrop";
    @ConfigurationParameter(name = PARAM_TYPES_TO_DROP, mandatory = false, defaultValue = {})
    private String[] paramTypesToDrop;

    /**
     * The types to drop from the CAS.
     * Must be the fully qualified class name of the type.
     * 
     * @apiNote WARNING: Make sure to include integral base types like
     *          {@link org.apache.uima.jcas.cas.Sofa Sofa}!
     * @apiNote You can use the
     *          {@link org.apache.uima.jcas.cas.TOP#_TypeName _TypeName} field of
     *          any {@link org.apache.uima.jcas.tcas.Annotation annotation} to
     *          access the fully qualified class name for convenience.
     * @apiNote Only one of {@link #PARAM_TYPES_TO_DROP} or
     *          {@link #PARAM_TYPES_TO_RETAIN} can be set.
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

    /**
     * @return The mode of operation.
     *         Will always be either {@link Mode#RETAIN} or {@link Mode#DROP}.
     * @throws IllegalStateException If the mode is unset (i.e. prior to
     *                               {@link #initialize initialization}).
     */
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

    /**
     * @return An immutable copy of the {@link #typeSet}.
     * @apiNote The returned set can only be empty prior to
     *          {@link #initialize initialization}.
     */
    public Set<String> getTypeSet() {
        return Set.copyOf(this.typeSet);
    }

    /**
     * Initializes the annotator.
     * 
     * You can either drop or retain specific types from the CAS.
     * The mode of operations is determined automatically based on the
     * configuration.
     * 
     * @throws IllegalArgumentException If both parameters
     *                                  {@link #PARAM_TYPES_TO_DROP} and
     *                                  {@link #PARAM_TYPES_TO_RETAIN} are set.
     * @throws IllegalArgumentException If both parameters are empty.
     */
    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        super.initialize(context);

        if (this.paramTypesToDrop.length == 0 && this.paramTypesToRetain.length == 0) {
            throw new ResourceInitializationException(
                    new IllegalArgumentException("At least one of PARAM_TYPES_TO_DROP or PARAM_TYPES_TO_RETAIN must be set"));
        } else if (this.paramTypesToDrop.length > 0 && this.paramTypesToRetain.length > 0) {
            throw new ResourceInitializationException(
                    new IllegalArgumentException("Only one of PARAM_TYPES_TO_DROP or PARAM_TYPES_TO_RETAIN can be set"));
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
            case _UNSET:
            default:
                throw new IllegalStateException("Mode is unset");
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
