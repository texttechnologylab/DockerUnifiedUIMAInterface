package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.Type;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;

/**
 * @param PARAM_TARGET_VIEW   Name of the target view to create, mandatory.
 * @param PARAM_SOURCE_VIEW   Name of the source view to copy from (if any),
 *                            optional, default: {@code _InitialView}.
 * @param PARAM_TYPES_TO_COPY List of types to copy from the source view,
 *                            optional.
 * @param PARAM_EXIST_OKAY    If false, raise an error if the target view
 *                            already exists, optional, default: {@code true}.
 */
public class ViewCreator extends JCasAnnotator_ImplBase {

    /**
     * Name of the target view to create.
     */
    public static final String PARAM_TARGET_VIEW = "targetView";
    @ConfigurationParameter(name = PARAM_TARGET_VIEW, mandatory = true)
    private String paramTargetView;

    /**
     * Name of the source view to copy from (if any).
     * Defaults to "_InitialView".
     */
    public static final String PARAM_SOURCE_VIEW = "sourceView";
    @ConfigurationParameter(name = PARAM_SOURCE_VIEW, mandatory = false, defaultValue = "_InitialView")
    private String paramSourceView;

    /**
     * The types to copy from the source view.
     * Must be a list of the fully qualified class name of the types.
     * 
     * @apiNote You can use the
     *          {@link org.apache.uima.jcas.cas.TOP#_TypeName _TypeName} field of
     *          any {@link org.apache.uima.jcas.tcas.Annotation annotation} to
     *          access the fully qualified class name for convenience.
     */
    public static final String PARAM_TYPES_TO_COPY = "typesToCopy";
    @ConfigurationParameter(name = PARAM_TYPES_TO_COPY, mandatory = false, defaultValue = {})
    private String[] paramTypesToCopy;

    /**
     * Name of the source view to copy from (if any).
     * Defaults to "_InitialView".
     */
    public static final String PARAM_EXIST_OKAY = "existOkay";
    @ConfigurationParameter(name = PARAM_EXIST_OKAY, mandatory = false, defaultValue = "true")
    private Boolean paramExistOkay;

    private HashSet<String> typeSet = new HashSet<>();

    /**
     * @return An immutable copy of the {@link #typesToCopy}.
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
     * @throws ResourceInitializationException If {@link #PARAM_TARGET_VIEW} is
     *                                         empty.
     */
    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        super.initialize(context);

        if (this.paramTargetView.isEmpty()) {
            throw new ResourceInitializationException(
                    new IllegalArgumentException("The target view cannot be empty!"));
        }

        this.typeSet = new HashSet<>(List.of(this.paramTypesToCopy));
    }

    @Override
    public void process(JCas aJCas) throws AnalysisEngineProcessException {
        final JCas targetView = this.creatOrGetTargetView(aJCas, this.paramTargetView);
        for (String typeName : this.typeSet) {
            Type type = aJCas.getTypeSystem().getType(typeName);
            aJCas.select(type).forEach(a -> {
                targetView.getIndexRepository().addFS(a);
            });
        }
    }

    private JCas creatOrGetTargetView(JCas aJCas, String targetViewName) throws AnalysisEngineProcessException {
        try {
            JCas view = aJCas.createView(targetViewName);
            view.setDocumentText(aJCas.getDocumentText());
            view.setDocumentLanguage(aJCas.getDocumentLanguage());
            return view;
        } catch (CASException createException) {
            if (!this.paramExistOkay) {
                throw new AnalysisEngineProcessException(createException);
            } else {
                try {
                    return aJCas.getView(targetViewName);
                } catch (CASException getException) {
                    throw new AnalysisEngineProcessException(getException);
                }
            }
        }
    }

}
