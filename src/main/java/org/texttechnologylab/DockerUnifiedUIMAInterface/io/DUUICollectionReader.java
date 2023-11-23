package org.texttechnologylab.DockerUnifiedUIMAInterface.io;

import de.tudarmstadt.ukp.dkpro.core.api.io.ProgressMeter;
import org.apache.uima.jcas.JCas;

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
    ProgressMeter getProgress();

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
