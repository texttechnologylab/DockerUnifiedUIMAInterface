package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.jcas.JCas;

import java.util.regex.Pattern;

public class ChangeMetaData extends JCasAnnotator_ImplBase {

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {
        DocumentMetaData dmd = null;
        try {
            dmd = DocumentMetaData.get(jCas);
        }
        catch (Exception e){
            dmd = DocumentMetaData.create(jCas);
        }

        if(dmd.getDocumentUri()==null){
            String sID = dmd.getDocumentId();
            char alpha = sID.toCharArray()[0];
            char beta = sID.toCharArray()[1];

            String sAlpha = String.valueOf(alpha).toLowerCase();
            String sBeta = String.valueOf(beta).toLowerCase();
            sAlpha = replaceUmlaut(sAlpha);
            sBeta = replaceUmlaut(sBeta);
            if(isNumeric(sAlpha) || isNumeric(sBeta)){
                sAlpha = "0";
                sBeta = "0";
            }


            dmd.setDocumentUri("/storage/projects/abrami/verbs/xmi/"+sAlpha+"/"+sBeta+"/"+sID);
            dmd.setDocumentBaseUri("/storage/projects/abrami/verbs/xmi");
        }

    }

    public boolean isNumeric(String sInput){
        Pattern pattern = Pattern.compile("-?\\d+(\\.\\d+)?");
        if (sInput == null) {
            return false;
        }
        return pattern.matcher(sInput).matches();

    }

    public String replaceUmlaut(String sInput){

        return sInput.replace("ü", "ue")
                .replace("ö", "oe")
                .replace("ä", "ae")
                .replace("Ü", "ue")
                .replace("Ö", "oe")
                .replace("Ä", "ae")
                .replace("ß", "ss");

    }
}
