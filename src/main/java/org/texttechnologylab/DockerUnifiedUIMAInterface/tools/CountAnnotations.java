package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.resource.ResourceInitializationException;
import org.dkpro.core.api.io.JCasFileWriter_ImplBase;
import org.json.JSONArray;
import org.json.JSONObject;
import org.texttechnologylab.utilities.helper.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CountAnnotations extends JCasFileWriter_ImplBase {

    private static Map<String, Integer> annoCount = new HashMap();

    public static final String PARAM_NAME = "name";
    @ConfigurationParameter(name = PARAM_NAME, mandatory = false, defaultValue = "project.json")
    protected String sName = "";

    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        super.initialize(context);
        annoCount.clear();
    }

    @Override
    public void destroy() {

            String sTargetLocation = this.getTargetLocation();

            JSONArray tArray = new JSONArray();

            annoCount.keySet().forEach(k->{
                JSONObject tObject = new JSONObject();
                tObject.put("name", k);
                tObject.put("value", annoCount.get(k));
                tArray.put(tObject);
            });

            try {
                FileUtils.writeContent(tArray.toString(1), new File(sTargetLocation+"/"+this.sName));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        super.destroy();
    }

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {


        JCasUtil.select(jCas, TOP.class).forEach(a->{
            String sType = a.getType().getName();
//
//            if(a instanceof POS){
//                sType = ((POS)a).getPosValue();
//            }

            int iCount =0;

            if(annoCount.containsKey(sType)){
                iCount = annoCount.get(sType);
            }
            iCount++;

            annoCount.put(sType, iCount);

        });

    }
}
