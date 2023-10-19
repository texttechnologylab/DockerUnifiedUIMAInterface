package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.annotation.DocumentAnnotation;

import java.io.File;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class RenameMetaDataGerParCor extends JCasAnnotator_ImplBase {

    public static final String PARAM_OUTPUT = "outpath";
    @ConfigurationParameter(name = PARAM_OUTPUT, mandatory = true, defaultValue = "/tmp/")
    protected String outpath;

    Map<String, Integer> fileDoubles = new HashMap<>();

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {

        DocumentMetaData dmd = DocumentMetaData.get(jCas);

        String sID = dmd.getDocumentId();
        while (sID.startsWith(" ")) {
            sID = sID.substring(1, sID.length());
        }
        DocumentAnnotation da = null;
        try {
            da = JCasUtil.select(jCas, DocumentAnnotation.class).stream().findFirst().get();

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Timestamp pTimestamp = new Timestamp(da.getTimestamp());

            System.out.println(dmd.getDocumentBaseUri());
            System.out.println(dmd.getDocumentUri());

            if (pTimestamp != null) {
                String sFileName = sdf.format(pTimestamp);
                String sOldURI = dmd.getDocumentUri();
                String sNewUri = sOldURI.replace(dmd.getDocumentBaseUri(), outpath);
                sNewUri.replace(dmd.getDocumentId(), sFileName + ".xmi.gz");

                File tFile = new File(sNewUri);
                if (tFile.exists()) {
                    int iCounter = 1;
                    if (fileDoubles.containsKey(sFileName)) {
                        iCounter = fileDoubles.get(sFileName);
                    } else {
                        iCounter = 1;
                    }
                    iCounter++;
                    fileDoubles.put(sFileName, iCounter);
                    sFileName = sFileName + "_" + iCounter;
                }
                dmd.setDocumentUri(dmd.getDocumentUri().replace(dmd.getDocumentId(), sFileName));

                dmd.setDocumentId(sFileName);
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }
}
