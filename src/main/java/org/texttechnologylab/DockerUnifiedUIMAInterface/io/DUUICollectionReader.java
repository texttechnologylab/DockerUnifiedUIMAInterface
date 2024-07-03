package org.texttechnologylab.DockerUnifiedUIMAInterface.io;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.AdvancedProgressMeter;

/**
 * Interface for a CollectionReader
 * @author Giuseppe Abrami
 */
public interface DUUICollectionReader {

    /**
     * Get the Progress
     *
     * @return
     */
    AdvancedProgressMeter getProgress();

    /**
     * Fill and get the next JCas
     * @param pCas
     */
    public void getNextCas(JCas pCas);

    /**
     * Are there still cas to be processed?
     * @return
     */
    public boolean hasNext();

    /**
     * Get size of Collection
     * @return
     */
    long getSize();

    /**
     * How many JCas have already been processed?
     * @return
     */
    long getDone();

}
