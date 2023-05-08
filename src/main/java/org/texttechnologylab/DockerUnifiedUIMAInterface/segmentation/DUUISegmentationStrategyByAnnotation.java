package org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCopier;
import org.apache.uima.util.TypeSystemUtil;
import org.texttechnologylab.annotation.AnnotationComment;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.stream.Collectors;

/***
 * Automatic simple document segmentation by annotation type.
 */
public class DUUISegmentationStrategyByAnnotation extends DUUISegmentationStrategy {
    // Name to store needed meta infos in cas
    static public final String DUUI_SEGMENTED_REF = "__textimager_duui_segmented_ref__";
    static public final String DUUI_SEGMENTED_POS = "__textimager_duui_segmented_pos__";

    // Annotation typo to use for splitting cas contents
    protected Class<? extends Annotation> SegmentationClass = null;

    // Max number of annotations (eg sentences) per segments
    public static final int MAX_ANNOTATIONS_PER_SEGMENT_DEFAULT = 2;
    protected int maxAnnotationsPerSegment = MAX_ANNOTATIONS_PER_SEGMENT_DEFAULT;

    // Max number of characters per segment
    public static final int MAX_CHARS_PER_SEGMENT_DEFAULT = 100;
    protected int maxCharsPerSegment = MAX_CHARS_PER_SEGMENT_DEFAULT;

    public DUUISegmentationStrategyByAnnotation withSegmentationClass(Class<? extends Annotation> clazz) {
        this.SegmentationClass = clazz;
        return this;
    }

    public DUUISegmentationStrategyByAnnotation withMaxAnnotationsPerSegment(int maxAnnotationsPerSegment) {
        this.maxAnnotationsPerSegment = maxAnnotationsPerSegment;
        return this;
    }

    public DUUISegmentationStrategyByAnnotation withMaxCharsPerSegment(int maxCharsPerSegment) {
        this.maxCharsPerSegment = maxCharsPerSegment;
        return this;
    }

    /***
     * Create a new CAS for this segment of the document.
     * @param jCas The full document CAS
     * @param typeSystemDescription The type system description to use for the new CAS segment
     * @param segmentBegin The begin position of the segment
     * @param segmentEnd The end position of the segment
     * @return The new CAS segment
     * @throws UIMAException
     */
    protected JCas createSegment(JCas jCas, TypeSystemDescription typeSystemDescription, int segmentBegin, int segmentEnd) throws UIMAException {
        String documentText = jCas.getDocumentText().substring(segmentBegin, segmentEnd);
        System.out.println("Create segment from " + segmentBegin + " to " + segmentEnd + " with text: \"" + documentText + "\"");

        // Copy segment annotations to new cas
        JCas jCasSegment = JCasFactory.createJCas(typeSystemDescription);
        CasCopier copier = new CasCopier(jCas.getCas(), jCasSegment.getCas());

        // Save begin of this segment to allow merging later
        AnnotationComment commentPos = new AnnotationComment(jCasSegment);
        commentPos.setKey(DUUI_SEGMENTED_POS);
        commentPos.setValue(String.valueOf(segmentBegin));
        commentPos.addToIndexes();

        // First, copy all annotations with position in the segment bounds,
        // Second copy all without positions
        for (TOP annotation : JCasUtil.select(jCas, TOP.class)) {
            boolean hasPosition = false;
            if (annotation instanceof Annotation) {
                hasPosition = true;
                // Make sure annotation is in segment bounds
                Annotation positionAnnotation = (Annotation) annotation;
                if (!(positionAnnotation.getBegin() >= segmentBegin && positionAnnotation.getEnd() <= segmentEnd)) {
                    continue;
                }
            }

            // Annotation either has no position or is in segment bounds
            TOP copy = (TOP) copier.copyFs(annotation);
            if (hasPosition) {
                // Shift begin and end to segment
                Annotation positionCopy = (Annotation) copy;
                positionCopy.setBegin(positionCopy.getBegin() - segmentBegin);
                positionCopy.setEnd(positionCopy.getEnd() - segmentBegin);
            }
            copy.addToIndexes(jCasSegment);

            // Mark this annotations as copied
            AnnotationComment commentId = new AnnotationComment(jCasSegment);
            commentId.setKey(DUUI_SEGMENTED_REF);
            commentId.setReference(copy);
            commentId.setValue(String.valueOf(annotation.getAddress()));
            commentId.addToIndexes();
        }

        // Add relevant document text
        jCasSegment.setDocumentLanguage(jCas.getDocumentLanguage());
        jCasSegment.setDocumentText(documentText);

        return jCasSegment;
    }

    /***
     * Check, if a given annotation can be added to the current segment.
     * @param segmentCount Amount of annotations already in the current segment
     * @param segmentBegin Position begin of the current segment
     * @param annotationEnd Position end of the annotation to add
     * @return true, if the annotation can be added to the current segment, else false
     */
    protected boolean tryAddToSegment(int segmentCount, int segmentBegin, int annotationEnd) {
        // Can only add, if below limit for annotations per segment
        if (segmentCount >= maxAnnotationsPerSegment) {
            return false;
        }

        // Can only add, if below char limit
        int segmentLength = annotationEnd - segmentBegin;
        if (segmentLength >= maxCharsPerSegment) {
            // Handle special case if even a single annotation is too long
            if (segmentCount == 0) {
                System.err.println("Warning: The annotation is too long with " + segmentLength + " characters, which is over the specified limit of " + maxCharsPerSegment + ".");
                return true;
            }
            return false;
        }

        // Below all limits, add this to current segment
        return true;
    }

    /***
     * Segments the given CAS into multiple CASes.
     * @param jCas The full CAS to segment
     * @return List of CAS segments
     * @throws UIMAException
     */
    @Override
    public List<JCas> segment(JCas jCas) throws UIMAException {
        // Type must have been set
        if (SegmentationClass == null) {
            // If not, provide the full cas without segmentation
            System.err.println("No annotation type for CAS segmentation provided, add using \"withSegmentationClass\".");
            System.err.println("Running without segmentation, this might take a while.");
            return List.of(jCas);
        }

        // Get the annotation type to segment the document, we expect it to be available in the cas
        List<? extends Annotation> annotations = new ArrayList<>(JCasUtil.select(jCas, SegmentationClass));
        if (annotations.isEmpty()) {
            // If not, provide the full cas without segmentation
            System.err.println("No annotations of type \"" + Sentence.class.getCanonicalName() + "\" for CAS segmentation found!");
            System.err.println("Running without segmentation, this might take a while.");
            return List.of(jCas);
        }

        // TODO check if even more than max annotations, else just return single document

        // Copy original cas's typesystem to use for new cas
        TypeSystemDescription typeSystemDescription = TypeSystemUtil.typeSystem2TypeSystemDescription(jCas.getTypeSystem());

        // Collect segments
        // NOTE: This will effectively at least duplicate all content,
        // this could later be improved by using an iterator-based approach
        List<JCas> results = new ArrayList<>();
        List<Annotation> currentSegment = new ArrayList<>();

        // In the first step, we only consider positioned annotations, i.e. "Annotation",
        // and not "AnnotationBase", which will be handled in a second step
        ListIterator<? extends Annotation> annotationIt = annotations.listIterator();
        while (annotationIt.hasNext()) {
            Annotation annotation = annotationIt.next();

            // Get begin/end of current segment to align to the exact boundaries
            int segmentBegin = annotation.getBegin();
            int segmentEnd = annotation.getEnd();
            if (!currentSegment.isEmpty()) {
                segmentBegin = currentSegment.get(0).getBegin();
                segmentEnd = currentSegment.get(currentSegment.size()-1).getEnd();
            }

            // Try adding as many annotations as possible to the current segment
            boolean canAdd = tryAddToSegment(currentSegment.size(), segmentBegin, annotation.getEnd());

            // Create CAS from segment if over limit
            if (!canAdd) {
                JCas jCasSegment = createSegment(jCas, typeSystemDescription, segmentBegin, segmentEnd);
                results.add(jCasSegment);
                currentSegment.clear();

                // Step back to continue with this annotation in the next segment
                annotationIt.previous();
            }
            else {
                // Ok, add to current segment
                currentSegment.add(annotation);
            }
        }

        // If there are still annotations left, create a final segment without checking the limits again
        if (!currentSegment.isEmpty()) {
            int segmentBegin = currentSegment.get(0).getBegin();
            int segmentEnd = currentSegment.get(currentSegment.size()-1).getEnd();
            JCas jCasSegment = createSegment(jCas, typeSystemDescription, segmentBegin, segmentEnd);
            results.add(jCasSegment);
        }

        return results;
    }

    /***
     * Combines the given CAS segments back into the single base CAS.
     * @param jCasSegmenteds List of CAS segments
     * @param jCas The full CAS to combine into
     */
    @Override
    public void combine(List<JCas> jCasSegmenteds, JCas jCas) {
        // TODO check if only a single document, we then do not have to merge but instead return this
        // Recombine the segments back into the main cas
        for (JCas jCasSegment : jCasSegmenteds) {
            CasCopier copier = new CasCopier(jCasSegment.getCas(), jCas.getCas());

            // Collect all annotations that were prevoiusly copied and thus are not new
            Map<TOP, List<AnnotationComment>> copiedIds = JCasUtil
                    .select(jCasSegment, AnnotationComment.class)
                    .stream()
                    .filter(c -> c.getKey().equals(DUUI_SEGMENTED_REF))
                    .collect(Collectors.groupingBy(AnnotationComment::getReference));

            // Find segment begin position for reindexing, use 0 as default
            int segmentBegin = JCasUtil
                    .select(jCasSegment, AnnotationComment.class)
                    .stream()
                    .filter(c -> c.getKey().equals(DUUI_SEGMENTED_POS))
                    .map(c -> Integer.parseInt(c.getValue()))
                    .findFirst()
                    .orElse(0);

            // Copy newly generated annotations
            for (TOP annotation : JCasUtil.select(jCasSegment, TOP.class)) {
                // Only copy newly generated annotations
                if (!copiedIds.containsKey(annotation)) {
                    TOP copy = (TOP) copier.copyFs(annotation);
                    boolean hasPosition = (annotation instanceof Annotation);
                    if (hasPosition) {
                        // Shift begin and end back to original
                        Annotation positionCopy = (Annotation) copy;
                        positionCopy.setBegin(positionCopy.getBegin() + segmentBegin);
                        positionCopy.setEnd(positionCopy.getEnd() + segmentBegin);
                    }
                    copy.addToIndexes(jCas);
                }
            }
        }

        // Remove meta information used for segmentation
        List<AnnotationComment> metaComments = JCasUtil
                .select(jCas, AnnotationComment.class)
                .stream()
                .filter(c -> c.getKey().equals(DUUI_SEGMENTED_REF) || c.getKey().equals(DUUI_SEGMENTED_POS))
                .collect(Collectors.toList());
        for (AnnotationComment comment : metaComments) {
            comment.removeFromIndexes();
        }
    }
}
