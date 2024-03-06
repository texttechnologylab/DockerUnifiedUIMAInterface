package org.texttechnologylab.DockerUnifiedUIMAInterface.io;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.AdvancedProgressMeter;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.LongStream;

/**
 * Class for asynchronous processing of DUUI readers
 * @author Giuseppe Abrami
 */
public class DUUIAsynchronousProcessor {

    // Set for readers
    private Set<DUUICollectionReader> readerSet = new HashSet<>(0);

    // max length of all collection readers
    private long lMax = -1;

    /**
     * Constructor
     *
     * @param pSet
     */
    public DUUIAsynchronousProcessor(Set<DUUICollectionReader> pSet) {

        this.readerSet = pSet;

        System.out.printf("Found %d elements in total!", getSumMax());
    }

    /**
     * Constructor
     * @param pValues
     */
    public DUUIAsynchronousProcessor(DUUICollectionReader... pValues) {

        for (DUUICollectionReader pValue : pValues) {
            readerSet.add(pValue);
        }

        System.out.printf("Found %d elements in total!", getSumMax());
    }

    /**
     * return the current progress
     * @return
     */
    public AdvancedProgressMeter getProgress() {

        long lDone = readerSet.stream().flatMapToLong(r -> LongStream.of(r.getDone())).sum();
        AdvancedProgressMeter pProgress = new AdvancedProgressMeter(getSumMax());
        pProgress.setDone(lDone);
        pProgress.setLeft(getSumMax() - lDone);

        return pProgress;
    }

    /**
     * Sum all sizes
     * @return
     */
    private long getSumMax() {
        if (lMax < 0) {
            lMax = readerSet.stream().flatMapToLong(r -> LongStream.of(r.getSize())).sum();
        }
        return lMax;
    }

    /**
     * Return the next CAS-object
     * @param empty
     * @return
     */
    public boolean getNextCAS(JCas empty) {

        Optional<DUUICollectionReader> readers = this.readerSet.stream().filter(reader -> reader.hasNext()).findFirst();

        if (!readers.isEmpty()) {
            DUUICollectionReader pReader = readers.get();
            pReader.getNextCas(empty);
        }

        return !readers.isEmpty();

    }

    /**
     * Is the processor, means all collection readers finish?
     * @return
     */
    public boolean isFinish() {
        boolean bReturn = true;
        for (DUUICollectionReader duuiCollectionReader : readerSet) {
            if (bReturn) {
                bReturn = !duuiCollectionReader.hasNext();
            }
        }
        return bReturn;
    }


}
