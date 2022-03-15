import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.InvalidXMLException;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class DUUIUIMADriver implements IDUUIDriverInterface {
    private HashMap<String, AnalysisEngine> _engines;

    public DUUIUIMADriver() {
        _engines = new HashMap<String, AnalysisEngine>();
    }

    public static class Component extends IDUUIPipelineComponent {
        public Component(AnalysisEngineDescription desc) throws IOException, SAXException {
            StringWriter writer = new StringWriter();
            desc.toXML(writer);
            setOption("engine", writer.getBuffer().toString());
        }
    }

    public boolean canAccept(IDUUIPipelineComponent component) {
        return component.getClass().getCanonicalName().toString() == Component.class.getCanonicalName().toString();
    }

    public String instantiate(IDUUIPipelineComponent component) throws InterruptedException, TimeoutException, UIMAException, SAXException, IOException {
        String uuid = UUID.randomUUID().toString();
        while ((_engines.containsKey(uuid))) {
            uuid = UUID.randomUUID().toString();
        }
        String engine = component.getOption("engine");
        if (engine == null) {
            throw new InvalidParameterException("The component does not contain a valid engine!");
        }
        System.out.printf("[UIMADriver] Assigned new pipeline component unique id %s\n", uuid);


        String tempanno = Files.createTempFile("duuid_driver_uima", ".xml").toFile().getAbsolutePath();
        Files.write(Paths.get(tempanno), engine.getBytes(StandardCharsets.UTF_8));
        AnalysisEngineDescription analysis_engine_desc = AnalysisEngineFactory.createEngineDescriptionFromPath(tempanno);
        AnalysisEngine ana = AnalysisEngineFactory.createEngine(analysis_engine_desc);
        String annotator = analysis_engine_desc.getAnnotatorImplementationName();
        if (annotator != null) {
            System.out.printf("[UIMADriver][%s] Instantiated native UIMA Analysis Engine Annotator %s without problems\n", uuid, annotator);
        } else {
            System.out.println("[UIMADriver] Instantiated native UIMA Analysis Engine without problems");
        }
        Files.delete(Paths.get(tempanno));
        _engines.put(uuid, ana);
        return uuid;
    }

    public synchronized DUUIEither run(String uuid, DUUIEither aCas) throws InterruptedException, IOException, SAXException, AnalysisEngineProcessException {
        AnalysisEngine engine = _engines.get(uuid);
        if (engine == null) {
            throw new InvalidParameterException("The given instantiated component uuid was not instantiated by the remote driver");
        }
        JCas jc = aCas.getAsJCas();
        engine.process(jc.getCas());
        aCas.updateJCas(jc);
        return aCas;
    }

    public void destroy(String uuid) {
        AnalysisEngine engine = _engines.remove(uuid);
        if (engine == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        engine.destroy();
    }
}
