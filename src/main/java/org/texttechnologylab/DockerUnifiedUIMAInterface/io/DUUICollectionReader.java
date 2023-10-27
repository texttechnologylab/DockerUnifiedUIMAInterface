package org.texttechnologylab.DockerUnifiedUIMAInterface.io;

import de.tudarmstadt.ukp.dkpro.core.api.io.ProgressMeter;
import org.apache.uima.jcas.JCas;

/**
 * Interface for a CollectionReader
 *
 * @author Giuseppe Abrami
 */
public interface DUUICollectionReader {

    ProgressMeter getProgress();

    public void getNextCas(JCas pCas);

    public boolean hasNext();

    long getSize();

    long getDone();

}
