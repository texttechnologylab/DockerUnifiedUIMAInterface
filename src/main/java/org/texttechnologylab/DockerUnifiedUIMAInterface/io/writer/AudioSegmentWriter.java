package org.texttechnologylab.DockerUnifiedUIMAInterface.io.writer;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.io.FileUtils;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;
import org.dkpro.core.api.io.JCasFileWriter_ImplBase;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.MultimodalUtil;
import org.texttechnologylab.annotation.type.AudioToken;

import java.io.File;
import java.io.IOException;

public class AudioSegmentWriter extends JCasFileWriter_ImplBase {

    public static final String PARAM_AUDIO_TOKEN_VIEW = "audioTokenView";
    @ConfigurationParameter(name = PARAM_AUDIO_TOKEN_VIEW, defaultValue = "audio_token_view")
    private String audioTokenView;

    public static final String PARAM_AUDIO_CONTENT_VIEW = "audioView";
    @ConfigurationParameter(name = PARAM_AUDIO_CONTENT_VIEW, defaultValue = "audio_view")
    private String audioView;

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {

        try {
            DocumentMetaData meta = DocumentMetaData.get(jCas);
            MultimodalUtil.getAllCoveredAudio(jCas.getView(audioTokenView), AudioToken.class, audioView, "wav").forEach(file -> {

                    String moveTo = getTargetLocation();

                    if(!moveTo.endsWith("/") && !moveTo.endsWith("\\")){
                        moveTo = moveTo + "/";
                    }

                    String documentName;

                    if(meta.getDocumentId() != null){
                        documentName = meta.getDocumentId() + "_";
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
