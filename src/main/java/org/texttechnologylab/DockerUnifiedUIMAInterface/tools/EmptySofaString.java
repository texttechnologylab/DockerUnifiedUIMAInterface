package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import org.dkpro.core.api.io.JCasFileWriter_ImplBase;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.texttechnologylab.utilities.helper.FileUtils;

import java.io.File;
import java.io.IOException;

public class EmptySofaString extends JCasFileWriter_ImplBase {

    public static final String PARM_OUTPUTFILE = "output";
    static StringBuilder sb = new StringBuilder();
    @ConfigurationParameter(name = PARM_OUTPUTFILE, mandatory = false, defaultValue = "/tmp/emptySofa.txt")
    protected String output;

    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        super.initialize(context);
    }

    @Override
    public void destroy() {

        try {
            FileUtils.writeContent(sb.toString(), new File(output));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        super.destroy();
    }

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {

        if (jCas.getDocumentText().length() < 10) {
            DocumentMetaData dmd = DocumentMetaData.get(jCas);
            if (sb.length() > 0) {
                sb.append("\n");
            }
            sb.append(dmd.getDocumentUri());
        }

    }
}
