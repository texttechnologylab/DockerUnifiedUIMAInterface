package org.texttechnologylab.DockerUnifiedUIMAInterface.io.writer;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.apache.commons.io.FileUtils;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.dkpro.core.api.io.JCasFileWriter_ImplBase;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.MultimodalUtil;
import org.texttechnologylab.annotation.type.AudioToken;

import java.io.File;
import java.io.IOException;

public class AudioSegmentWriter extends JCasFileWriter_ImplBase {

    public static final String PARAM_AUDIO_TOKEN_VIEW = "audioTokenView";
    @ConfigurationParameter(name = PARAM_AUDIO_TOKEN_VIEW, defaultValue = "_InitialView")
    private String audioTokenView;

    public static final String PARAM_AUDIO_CONTENT_VIEW = "audioView";
    @ConfigurationParameter(name = PARAM_AUDIO_CONTENT_VIEW, defaultValue = "_InitialView")
    private String audioView;

    @Override
    public void process(JCas jCas) {


        try {
            DocumentMetaData meta = null;
            if (JCasUtil.select(jCas, DocumentMetaData.class).size() > 0) {
                meta = DocumentMetaData.get(jCas);
            }

            DocumentMetaData finalMeta = meta;

            JCas audioFileView = jCas.getView(audioView);

            MultimodalUtil.getAllCoveredAudio(jCas.getView(audioTokenView), audioFileView, AudioToken.class, "wav").forEach(file -> {

                    String moveTo = getTargetLocation();

                    if(!moveTo.endsWith("/") && !moveTo.endsWith("\\")){
                        moveTo = moveTo + "/";
                    }

                    String documentName;

                    if(finalMeta != null && finalMeta.getDocumentId() != null){
                        documentName = finalMeta.getDocumentId() + "_";
                    }else{
                        documentName = "File_";
                    }

                    try {
                        FileUtils.moveFile(new File(file.getAbsolutePath()), new File(moveTo + documentName + file.getName()));
                    } catch (IOException e) {
                        e.printStackTrace();
                        }
                }

            );
        } catch (CASException e) {
            e.printStackTrace();
        }
    }
}
