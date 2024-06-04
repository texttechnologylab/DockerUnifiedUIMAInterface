package org.texttechnologylab.DockerUnifiedUIMAInterface.io;

import org.apache.uima.jcas.JCas;

import java.util.List;

public interface DUUICollectionDBReader extends DUUICollectionReader {
    /**
     * Fill and get the next JCas based on the tool
     *
     * @param pCas     :     JCas to be filled
     * @param toolUUID : toolUUID
     * @param pipelinePosition
     * @return true if cas was filled, false if no cas was filled
     */
    public boolean getNextCas(JCas pCas, String toolUUID, int pipelinePosition);

    void updateCas(JCas pCas, String toolUUID, boolean status, List<String> pipelineUUIDs);

    boolean finishedLoading();

    void merge();
}
