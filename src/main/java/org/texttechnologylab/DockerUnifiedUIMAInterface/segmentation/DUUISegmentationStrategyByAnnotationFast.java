package org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.util.CasCopier;
import org.texttechnologylab.annotation.AnnotationComment;

import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class DUUISegmentationStrategyByAnnotationFast extends DUUISegmentationStrategy {

    private int iLength = 500000;
    private Class pClass;

    private Set<String> currentOffset = new HashSet<>();

    private JCas emptyCas = null;


    private boolean bDebug = false;

    public DUUISegmentationStrategyByAnnotationFast() {
        super();
    }

    public DUUISegmentationStrategyByAnnotationFast withLength(int iLength) {
        this.iLength = iLength;
        return this;
    }

    public DUUISegmentationStrategyByAnnotationFast withDebug() {
        this.bDebug = true;
        return this;
    }

    public boolean hasDebug() {
        return this.bDebug;
    }

    public int getSegments() {
        return this.currentOffset.size();
    }

    public DUUISegmentationStrategyByAnnotationFast withSegmentationClass(Class pClass) {
        this.pClass = pClass;
        return this;
    }


    @Override
    public JCas getNextSegment() {
        emptyCas.reset();

        if (currentOffset.size() == 0) {
            return null;
        }
        long iStartTime = System.currentTimeMillis();

        String sOffset = currentOffset.stream().findFirst().get();
        String[] sSplit = sOffset.split("-");
        int iStart = Integer.valueOf(sSplit[0]);
        int iEnde = Integer.valueOf(sSplit[1]);


        emptyCas.setDocumentText(jCasInput.getDocumentText().substring(iStart, iEnde));
        emptyCas.setDocumentLanguage(jCasInput.getDocumentLanguage());


        JCasUtil.selectCovered(jCasInput, Annotation.class, iStart, iEnde).forEach(a -> {
            try {
                if (!(a instanceof DocumentMetaData)) {
                    TOP fs = (TOP) a.getClass().getConstructor(JCas.class).newInstance(emptyCas);
                    fs.getType().getFeatures().forEach(f -> {
                        try {
                            fs.setFeatureValueFromString(f, ((TOP) a).getFeatureValueAsString(f));
                        } catch (Exception e) {
//                        System.out.println(e.getMessage());
                        }
                    });
                    fs.addToIndexes();
                }

            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e);
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        });


        JCasUtil.select(emptyCas, Annotation.class).stream().forEach(a -> {
            a.setBegin(a.getBegin() - iStart);
            a.setEnd(a.getEnd() - iStart);
        });

        AnnotationComment da = new AnnotationComment(emptyCas);
        da.setKey("offset");
        da.setValue("" + iEnde);
        da.addToIndexes();

        currentOffset.remove(sOffset);
        long iEndTime = System.currentTimeMillis();
        SimpleDateFormat df = new SimpleDateFormat("mm:ss:SSS");
        if (hasDebug()) {
            System.out.println("Duration Split: " + df.format(new Date(iEndTime - iStartTime)) + " (" + currentOffset.size() + " left)");
        }
        return emptyCas;
    }

    @Override
    protected void initialize() throws UIMAException {
        this.emptyCas = JCasFactory.createJCas();

        String sText = this.jCasInput.getDocumentText();

        int tLength = sText.length();


        int iCount = 0;

        while ((iCount + iLength) < tLength) {

            List<Annotation> pList = new ArrayList<>(0);

            pList = (List<Annotation>) JCasUtil.selectCovered(this.jCasInput, pClass, iCount, (iCount + this.iLength)).stream().collect(Collectors.toList());

            currentOffset.add(pList.get(0).getBegin() + "-" + pList.get(pList.size() - 1).getEnd());
            iCount = pList.get(pList.size() - 1).getEnd();

        }
        if (iCount < tLength) {
            currentOffset.add(iCount + "-" + tLength);
        }

//        currentOffset.stream().forEach(co->{
//            System.out.println(co);
//        });
    }

    @Override
    public void merge(JCas jCasSegment) {
        long iStartTime = System.currentTimeMillis();
        int iOffset;
        AnnotationComment offset = JCasUtil.select(jCasSegment, AnnotationComment.class).stream().filter(ac -> {
            return ac.getKey().equalsIgnoreCase("offset");
        }).findFirst().get();

        if (offset != null) {
            iOffset = Integer.valueOf(offset.getValue());
        } else {
            iOffset = 0;
        }

        if (iOffset > 0) {
//            System.out.println("Offset: "+iOffset);
            JCasUtil.select(jCasSegment, Annotation.class).stream().forEach(a -> {
                a.setBegin(a.getBegin() + iOffset);
                a.setEnd(a.getEnd() + iOffset);
            });
        }
        CasCopier.copyCas(jCasSegment.getCas(), jCasInput.getCas(), false);
        long iEndTime = System.currentTimeMillis();
        SimpleDateFormat df = new SimpleDateFormat("mm:ss:SSS");
        if (hasDebug()) {
            System.out.println("Duration Merge: " + df.format(new Date(iEndTime - iStartTime)));
        }
    }

    @Override
    public void finalize(JCas jCas) {
//        System.out.println("Finish");
    }
}
