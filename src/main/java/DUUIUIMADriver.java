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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;

public class DUUIUIMADriver implements IDUUIDriverInterface {
    private HashMap<String, InstantiatedComponent> _engines;

    public DUUIUIMADriver() {
        _engines = new HashMap<String, InstantiatedComponent>();
    }

    public static class InstantiatedComponent {
        private ConcurrentLinkedQueue<AnalysisEngine> _engines;

        public InstantiatedComponent() {
            _engines = new ConcurrentLinkedQueue<AnalysisEngine>();
        }

        public InstantiatedComponent add(AnalysisEngine engine) {
            _engines.add(engine);
            return this;
        }

        public ConcurrentLinkedQueue<AnalysisEngine> getEngines() {
            return _engines;
        }
    }


    public static class Component extends IDUUIPipelineComponent {
        public Component(AnalysisEngineDescription desc) throws IOException, SAXException {
            StringWriter writer = new StringWriter();
            desc.toXML(writer);
            setOption("engine", writer.getBuffer().toString());
        }

        public Component withScale(int scale) {
            setOption("scale",String.valueOf(scale));
            return this;
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
        String scale_string = component.getOption("scale");
        int scale = 1;
        if (scale_string != null) {
            scale = Integer.valueOf(scale_string);
        }
        System.out.printf("[UIMADriver] Assigned new pipeline component unique id %s\n", uuid);


        String tempanno = Files.createTempFile("duuid_driver_uima", ".xml").toFile().getAbsolutePath();
        Files.write(Paths.get(tempanno), engine.getBytes(StandardCharsets.UTF_8));
        AnalysisEngineDescription analysis_engine_desc = AnalysisEngineFactory.createEngineDescriptionFromPath(tempanno);

        InstantiatedComponent comp = new InstantiatedComponent();
        for(int i = 0; i < scale; i++) {
            AnalysisEngine ana = AnalysisEngineFactory.createEngine(analysis_engine_desc);
            String annotator = analysis_engine_desc.getAnnotatorImplementationName();
            if (annotator != null) {
                System.out.printf("[UIMADriver][%s][Replication %d/%d] Instantiated native UIMA Analysis Engine Annotator %s without problems\n", uuid,i+1,scale, annotator);
            } else {
                System.out.printf("[UIMADriver][%s][Replication %d/%d] Instantiated native UIMA Analysis Engine without problems\n",uuid,i+1,scale);
            }
            comp.add(ana);
        }
        Files.delete(Paths.get(tempanno));
        _engines.put(uuid, comp);
        return uuid;
    }

    public DUUIEither run(String uuid, DUUIEither aCas) throws InterruptedException, IOException, SAXException, AnalysisEngineProcessException {
        InstantiatedComponent component = _engines.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("The given instantiated component uuid was not instantiated by the remote driver");
        }
        AnalysisEngine engine = component.getEngines().poll();
        while(engine==null) {
            engine = component.getEngines().poll();
        }
        try {
            JCas jc = aCas.getAsJCas();
            engine.process(jc.getCas());
            aCas.updateJCas(jc);
            component.add(engine);
        }
        catch(Exception e) {
            component.add(engine);
            throw e;
        }
        return aCas;
    }

    public void destroy(String uuid) {
        InstantiatedComponent component = _engines.remove(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        for(AnalysisEngine engine : component.getEngines()) {
            engine.destroy();
        }
    }
}
