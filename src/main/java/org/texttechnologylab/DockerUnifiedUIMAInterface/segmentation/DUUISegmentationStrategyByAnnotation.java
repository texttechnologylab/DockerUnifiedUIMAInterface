package org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation;

import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCopier;
import org.apache.uima.util.TypeSystemUtil;
import org.jetbrains.annotations.NotNull;
import org.texttechnologylab.annotation.AnnotationComment;

import java.util.*;
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

    // Ignore missing annotations and just use full document
    public static final boolean IGNORE_MISSING_ANNOTATIONS_DEFAULT = false;
    protected boolean ignoreMissingAnnotations = IGNORE_MISSING_ANNOTATIONS_DEFAULT;

    // Current list of annotations
    private List<? extends Annotation> annotations;
    private TypeSystemDescription typeSystemDescription;

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

    public DUUISegmentationStrategyByAnnotation withIgnoreMissingAnnotations(boolean ignoreMissingAnnotations) {
        this.ignoreMissingAnnotations = ignoreMissingAnnotations;
        return this;
    }

    @Override
    protected void initialize() {
        // Type must have been set
        if (SegmentationClass == null) {
            throw new IllegalArgumentException("No annotation type for CAS segmentation provided, add using \"withSegmentationClass\".");
        }

        // Get the annotation type to segment the document, we expect it to be available in the cas
        annotations = new ArrayList<>(JCasUtil.select(jCas, SegmentationClass));
        if (annotations.isEmpty()) {
            if (!ignoreMissingAnnotations) {
                throw new IllegalArgumentException("No annotations of type \"" + SegmentationClass.getCanonicalName() + "\" for CAS segmentation found!");
            }
            else {
                System.err.println("No annotations of type \"" + SegmentationClass.getCanonicalName() + "\" for CAS segmentation found!");
                System.err.println("Running without segmentation, this might take a while.");
            }
        }
        else {
            System.out.println("Found " + annotations.size() + " annotations of type \"" + SegmentationClass.getCanonicalName() + "\" for CAS segmentation.");
        }

        // Copy original cas's typesystem to use for new cas
        typeSystemDescription = TypeSystemUtil.typeSystem2TypeSystemDescription(jCas.getTypeSystem());
    }

    class DUUISegmentationStrategyByAnnotationIterator implements Iterator<JCas> {
        private final ListIterator<? extends Annotation> annotationIt;
        private long annotationCount = 0;

        // Current and next segmented JCas
        private final JCas jCasCurrentSegment;
        private final JCas jCasNextSegment;

        private boolean hasMore = true;

        DUUISegmentationStrategyByAnnotationIterator()  {
            annotationIt = DUUISegmentationStrategyByAnnotation.this.annotations.listIterator();
            annotationCount = 0;

            try {
                // Create the segmented jCas, only create one as it is a slow operation
                jCasNextSegment = JCasFactory.createJCas(typeSystemDescription);
                jCasCurrentSegment = JCasFactory.createJCas(typeSystemDescription);
            } catch (UIMAException e) {
                throw new RuntimeException(e);
            }

            nextSegment();
        }

        /***
         * Check, if a given annotation can be added to the current segment.
         * @param segmentCount Amount of annotations already in the current segment
         * @param segmentBegin Position begin of the current segment
         * @param annotationEnd Position end of the annotation to add
         * @return true, if the annotation can be added to the current segment, else false
         */
        boolean tryAddToSegment(int segmentCount, int segmentBegin, int annotationEnd) {
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
         * Create a new CAS for this segment of the document.
         * @param segmentBegin The begin position of the segment
         * @param segmentEnd The end position of the segment
         * @return The new CAS segment
         */
        void createSegment(int segmentBegin, int segmentEnd) {
            String documentText = jCas.getDocumentText().substring(segmentBegin, segmentEnd);

            // Reset next cas
            jCasNextSegment.reset();
            CasCopier copierNext = new CasCopier(jCas.getCas(), jCasNextSegment.getCas());

            // Save begin of this segment to allow merging later
            AnnotationComment commentPos = new AnnotationComment(jCasNextSegment);
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
                TOP copy = (TOP) copierNext.copyFs(annotation);
                if (hasPosition) {
                    // Shift begin and end to segment
                    Annotation positionCopy = (Annotation) copy;
                    positionCopy.setBegin(positionCopy.getBegin() - segmentBegin);
                    positionCopy.setEnd(positionCopy.getEnd() - segmentBegin);
                }
                copy.addToIndexes(jCasNextSegment);

                // Mark this annotations as copied
                AnnotationComment commentId = new AnnotationComment(jCasNextSegment);
                commentId.setKey(DUUI_SEGMENTED_REF);
                commentId.setReference(copy);
                commentId.setValue(String.valueOf(annotation.getAddress()));
                commentId.addToIndexes();
            }

            // Add relevant document text
            jCasNextSegment.setDocumentLanguage(jCas.getDocumentLanguage());
            jCasNextSegment.setDocumentText(documentText);

            System.out.println("Created new CAS segment with " + JCasUtil.select(jCasNextSegment, TOP.class).size() + " annotations from " + segmentBegin + " to " + segmentEnd + ".");
        }

        void nextSegment() {
            System.out.println("Processed " + annotationCount + "/" + annotations.size() + " annotations of type \"" + SegmentationClass.getCanonicalName() + "\" for CAS segmentation.");

            // Assume we have no more segments
            hasMore = false;

            List<Annotation> currentSegment = new ArrayList<>();
            while (annotationIt.hasNext()) {
                Annotation annotation = annotationIt.next();
                annotationCount++;

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
                    createSegment(segmentBegin, segmentEnd);
                    currentSegment.clear();

                    // Step back to continue with this annotation in the next segment
                    annotationIt.previous();
                    annotationCount--;

                    // We have more segments later, stop here
                    hasMore = true;
                    return;
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
                createSegment(segmentBegin, segmentEnd);

                // We have more segments
                hasMore = true;
            }
        }

        @Override
        public boolean hasNext() {
            return hasMore;
        }

        @Override
        public JCas next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            // Copy as current segment
            jCasCurrentSegment.reset();
            CasCopier.copyCas(jCasNextSegment.getCas(), jCasCurrentSegment.getCas(), true);

            // Try to create the next segment
            nextSegment();

            // Return the current segment
            return jCasCurrentSegment;
        }
    }

    @NotNull
    @Override
    public Iterator<JCas> iterator() {
        return new DUUISegmentationStrategyByAnnotationIterator();
    }

    @Override
    public void merge(JCas jCasSegment) {
        // Recombine the segments back into the main cas
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
        Collection<TOP> annotations = JCasUtil.select(jCasSegment, TOP.class);
        long annotationCount = annotations.size();
        long copiedCounter = 0;
        for (TOP annotation : annotations) {
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
                copiedCounter++;
            }
        }
        System.out.println("Merged " + copiedCounter + "/" + annotationCount + " annotations.");

        // Remove meta information used for segmentation
        // TODO already filter on copy
        long deletedCounter = 0;
        List<AnnotationComment> metaComments = JCasUtil
                .select(jCas, AnnotationComment.class)
                .stream()
                .filter(c -> c.getKey().equals(DUUI_SEGMENTED_REF) || c.getKey().equals(DUUI_SEGMENTED_POS))
                .collect(Collectors.toList());
        for (AnnotationComment comment : metaComments) {
            comment.removeFromIndexes();
            deletedCounter++;
        }
        System.out.println("Deleted " + deletedCounter + " temp annotations.");
    }
}
