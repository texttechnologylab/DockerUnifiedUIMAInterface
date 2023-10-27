package org.texttechnologylab.DockerUnifiedUIMAInterface.io;

import de.tudarmstadt.ukp.dkpro.core.api.io.ProgressMeter;
import org.apache.uima.jcas.JCas;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.LongStream;

/**
 * Class for asynchronous processing of DUUI processes
 *
 * @author Giuseppe Abrami
 */
public class DUUIAsynchronousProcessor {

    // Set for readers
    private Set<DUUICollectionReader> readerSet = new HashSet<>(0);


    private long lMax = -1;


    public DUUIAsynchronousProcessor(Set<DUUICollectionReader> pSet) {

        this.readerSet = pSet;

        System.out.printf("Found %d elements in total!", getSumMax());
    }

    public DUUIAsynchronousProcessor(DUUICollectionReader... pValues) {

        for (DUUICollectionReader pValue : pValues) {
            readerSet.add(pValue);
        }

        System.out.printf("Found %d elements in total!", getSumMax());
    }

    public ProgressMeter getProgress() {

        long lDone = readerSet.stream().flatMapToLong(r -> LongStream.of(r.getDone())).sum();
        ProgressMeter pProgress = new ProgressMeter(getSumMax());
        pProgress.setDone(lDone);
        pProgress.setLeft(getSumMax() - lDone);

        return pProgress;
    }

    private long getSumMax() {
        if (lMax < 0) {
            lMax = readerSet.stream().flatMapToLong(r -> LongStream.of(r.getSize())).sum();
        }
        return lMax;
    }

    public boolean getNextCAS(JCas empty) {

        DUUICollectionReader pReader = this.readerSet.stream().filter(reader -> reader.hasNext()).findFirst().get();
        pReader.getNextCas(empty);
        return true;
    }

    public boolean isFinish() {
        boolean bReturn = true;
        for (DUUICollectionReader duuiCollectionReader : readerSet) {
            if (bReturn) {
                bReturn = duuiCollectionReader.hasNext();
            }
        }
        return bReturn;
    }


}
