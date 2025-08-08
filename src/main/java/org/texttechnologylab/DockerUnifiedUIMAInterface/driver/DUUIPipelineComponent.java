package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import com.google.common.collect.ImmutableList;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.util.InvalidXMLException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Encapsulation of a component for a pipeline.
 * @author Alexander Leonhardt
 */
public class DUUIPipelineComponent {
    private HashMap<String, String> _options;
    private HashMap<String,String> _parameters;

    private AnalysisEngineDescription _engine;
    private String _finalizedEncoded;
    private int _finalizedEncodedHash;
    private String _compression;
    private boolean _websocket = false;
    private int _ws_elements = 50;

    private List<String> _constraints = new ArrayList<>(0);
    private List<String> _env = new ArrayList<>(0);

    public static String compressionMethod = CompressorStreamFactory.XZ;

    // Segmentation strategy to split and merge large documents
    private DUUISegmentationStrategy segmentationStrategy;

    private static String engineOptionName = "engine";
    private static String scaleOptionName = "scale";
    final private static String workersOptionName = "workers";

    private static String ignoring200 = "ignoring200";
    private static String urlOptionName = "url";
    private static String websocketOptionName = "websocket";
    private String websocketElementsOptionName = "websocketElements";

    private static String dockerPasswordOptionName = "dockerPassword";
    private static String dockerUsernameOptionName = "dockerUsername";

    private static String dockerNoShutdown = "dockerNoShutdown";
    private static String dockerWithGPU = "dockerWithGPU";
    private static String dockerImageName = "dockerImageName";
    private static String dockerImageFetching = "dockerImageFetch";

    private static String versionInformation = "version";
    private static String writeToViewName = "uimaViewName";
    private static String initialViewFromInitialViewName = "uimaViewInitializeFromInitial";

    private static String componentName = "name";

    private static String driverName = "driver";
    private static String descriptionName = "description";

    private static String sourceView = "sourceView";
    private static String targetView = "targetView";
    private static String timeout = "timeout";

    private String getVersion() throws URISyntaxException, IOException {
        ClassLoader classLoader = DUUIPipelineComponent.class.getClassLoader();
        try {
            return classLoader.getResourceAsStream("git.properties").toString();
        } catch (NullPointerException e) {
            System.err.println("Could not find resource \"git.properties\"!");
            return "undefined";
        }
    }

    public DUUIPipelineComponent() throws URISyntaxException, IOException {
        _options = new HashMap<>();
        _finalizedEncoded = null;
        _parameters = new HashMap<>();

        String version = getVersion();
        if(version == null) {
            _options.put(versionInformation,"Unknown");
        }
        else {
            _options.put(versionInformation,version);
        }
        _parameters.put(websocketOptionName, String.valueOf(_websocket));
    }

    public void finalizeComponent() throws CompressorException, IOException, SAXException {
        if(_engine!=null) {
            StringWriter writer = new StringWriter();
            _engine.toXML(writer);
            _options.put(engineOptionName, writer.getBuffer().toString());
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompressorOutputStream cos = new CompressorStreamFactory().createCompressorOutputStream(compressionMethod,out);
        cos.write(toJson().getBytes(StandardCharsets.UTF_8));
        cos.close();
        _finalizedEncoded = Base64.getEncoder().encodeToString(out.toByteArray());
        _finalizedEncodedHash = _finalizedEncoded.hashCode();
    }

    public String getFinalizedRepresentation() {
        return _finalizedEncoded;
    }

    public int getFinalizedRepresentationHash() {
        return _finalizedEncodedHash;
    }

    public <Y> DUUIPipelineComponent withDriver(Class<Y> t) {
        _options.put(driverName,t.getCanonicalName());
        return this;
    }

    public String getDriver() {
        return _options.get(driverName);
    }

    public DUUIPipelineComponent withDescription(String description) {
        if(_finalizedEncoded!=null) {
            throw new RuntimeException("DUUIPipelineComponent has already been finalized, it is immutable now!");
        }
        if(description == null) {
            _options.remove(descriptionName);
            return this;
        }
        _options.put(descriptionName, description);
        return this;
    }

    public String getDescription() {
        return _options.get(descriptionName);
    }


    public DUUIPipelineComponent withName(String name) {
        if(_finalizedEncoded!=null) {
            throw new RuntimeException("DUUIPipelineComponent has already been finalized, it is immutable now!");
        }
        if(name == null) {
            _options.remove(componentName);
            return this;
        }
        _options.put(componentName, name);
        return this;
    }

    public String getName() {
        return getName(null);
    }

    public String getName(String defaultValue) {
        String value = _options.get(componentName);
        if(value==null) return defaultValue;
        return value;
    }

    public DUUIPipelineComponent withEngine(AnalysisEngineDescription desc) throws IOException, SAXException {
        if(_finalizedEncoded!=null) {
            throw new RuntimeException("DUUIPipelineComponent has already been finalized, it is immutable now!");
        }
        _engine = desc;
        if(desc == null) {
            _options.remove(engineOptionName);
            return this;
        }
        return this;
    }

    public AnalysisEngineDescription getEngine() throws IOException, SAXException, InvalidXMLException {
        if(_engine!=null) return _engine;

        String engine = _options.get(engineOptionName);
        if(engine==null) return null;

        String temp = Files.createTempFile("duuid_driver_uima", ".xml").toFile().getAbsolutePath();
        Files.write(Paths.get(temp), engine.getBytes(StandardCharsets.UTF_8));
        _engine = AnalysisEngineFactory.createEngineDescriptionFromPath(temp);
        return _engine;
    }

    public DUUIPipelineComponent withScale(Integer iScale) {
        if(_finalizedEncoded!=null) {
            throw new RuntimeException("DUUIPipelineComponent has already been finalized, it is immutable now!");
        }

        if (iScale == null) {
            _options.remove(scaleOptionName);
            return this;
        }
        _options.put(scaleOptionName, String.valueOf(iScale));
        return this;
    }

    public DUUIPipelineComponent withWorkers(Integer workers) {
        if(_finalizedEncoded!=null) {
            throw new RuntimeException("DUUIPipelineComponent has already been finalized, it is immutable now!");
        }

        if (workers == null) {
            _options.remove(workersOptionName);
            return this;
        }
        _options.put(workersOptionName, String.valueOf(workers));
        return this;
    }

    public DUUIPipelineComponent withIgnoringHTTP200Error(boolean bValue) {

        _options.put(ignoring200, String.valueOf(bValue));
        return this;
    }

    public boolean getIgnoringHTTP200Error() {
        return Boolean.parseBoolean(_options.getOrDefault(ignoring200, "false"));
    }

    public DUUIPipelineComponent withConstraints(List<String> constraints) {
        _constraints.addAll(constraints);
        return this;
    }

    public DUUIPipelineComponent withConstraint(String constraints) {
        _constraints.add(constraints);
        return this;
    }

    public List<String> getConstraints(){
        return _constraints;
    }

    /**
     * Set environment variables.
     * @param envString Any number of environment variables as {@code "NAME=VALUE"}.
     * @return The updated component.
     */
    public DUUIPipelineComponent withEnv(String... envString) {
        _env.addAll(Arrays.asList(envString));
        return this;
    }

    /**
     * @return An {@link ImmutableList immutable} copy of the environment variable list.
     */
    public List<String> getEnv(){
        return ImmutableList.copyOf(_env);
    }

    public Integer getScale() {
        return getScale(null);
    }

    public Integer getScale(Integer defaultValue) {
        String scale = _options.get(scaleOptionName);
        if(scale == null) return defaultValue;
        return Integer.parseInt(scale);
    }

    public Integer getWorkers() {
        return getWorkers(null);
    }

    public Integer getWorkers(Integer defaultValue) {
        String workers = _options.get(workersOptionName);
        if(workers == null) return defaultValue;
        return Integer.parseInt(workers);
    }

    public DUUIPipelineComponent withUrl(String url) {
        if(_finalizedEncoded!=null) {
            throw new RuntimeException("DUUIPipelineComponent has already been finalized, it is immutable now!");
        }

        if(url == null) {
            _options.remove(urlOptionName);
            return this;
        }
        JSONArray js = new JSONArray();
        js.put(url);
        _options.put(urlOptionName,js.toString());
        return this;
    }

    public DUUIPipelineComponent withUrls(List<String> urls) {
        if(_finalizedEncoded!=null) {
            throw new RuntimeException("DUUIPipelineComponent has already been finalized, it is immutable now!");
        }

        if(urls == null) {
            _options.remove(urlOptionName);
            return this;
        }
        JSONArray arr = new JSONArray();
        for(String s : urls) {
            arr.put(s);
        }
        _options.put(urlOptionName,arr.toString());
        return this;
    }

    public List<String> getUrl() {
        LinkedList<String> lst = new LinkedList<>();
        String urls = _options.get(urlOptionName);
        if(urls == null) return null;
        JSONArray arr = new JSONArray(urls);
        for(int i = 0; i < arr.length(); i++) {
            lst.push(arr.getString(i));
        }
        return lst;
    }

    public DUUIPipelineComponent withDockerAuth(String name, String password) {
        if(_finalizedEncoded!=null) {
            throw new RuntimeException("DUUIPipelineComponent has already been finalized, it is immutable now!");
        }

        if(name==null && password == null) {
            _options.remove(dockerPasswordOptionName);
            _options.remove(dockerUsernameOptionName);
            return this;
        }
        _options.put(dockerPasswordOptionName,password);
        _options.put(dockerUsernameOptionName,name);
        return this;
    }

    public String getDockerAuthUsername() {
        return getDockerAuthUsername(null);
    }

    public String getDockerAuthUsername(String defaultValue) {
        String value = _options.get(dockerUsernameOptionName);
        if(value == null) return defaultValue;
        return value;
    }

    public String getDockerAuthPassword() {
        return getDockerAuthPassword(null);
    }

    public String getDockerAuthPassword(String defaultValue) {
        String value = _options.get(dockerPasswordOptionName);
        if(value == null) return defaultValue;
        return value;
    }

    public DUUIPipelineComponent withDockerRunAfterExit(Boolean b) {
        if(_finalizedEncoded!=null) {
            throw new RuntimeException("DUUIPipelineComponent has already been finalized, it is immutable now!");
        }

        if(b==null) {
            _options.remove(dockerNoShutdown);
            return this;
        }
        _options.put(dockerNoShutdown,String.valueOf(b));
        return this;
    }

    public Boolean getDockerRunAfterExit() {
        return getDockerRunAfterExit(null);
    }

    public Boolean getDockerRunAfterExit(Boolean defaultValue) {
        String result = _options.get(dockerNoShutdown);
        if(result == null) return defaultValue;
        return Boolean.parseBoolean(result);
    }

    public DUUIPipelineComponent withDockerGPU(Boolean b) {
        if(_finalizedEncoded!=null) {
            throw new RuntimeException("DUUIPipelineComponent has already been finalized, it is immutable now!");
        }

        if(b==null) {
            _options.remove(dockerWithGPU);
            return this;
        }
        _options.put(dockerWithGPU,String.valueOf(b));
        return this;
    }

    public Boolean getDockerGPU() {
        return getDockerGPU(null);
    }

    public Boolean getDockerGPU(Boolean defaultValue) {
        String result = _options.get(dockerWithGPU);
        if(result == null) return defaultValue;
        return Boolean.parseBoolean(result);
    }

    public DUUIPipelineComponent withDockerImageName(String imageName) {
        if(_finalizedEncoded!=null) {
            throw new RuntimeException("DUUIPipelineComponent has already been finalized, it is immutable now!");
        }

        if(imageName==null) {
            _options.remove(dockerImageName);
            return this;
        }
        _options.put(dockerImageName,imageName);
        return this;
    }

    public DUUIPipelineComponent __internalPinDockerImage(String imageName, String pinName) {
        if(pinName==null) {
            System.err.println("Could not add the digest since this image has not been pushed and pulled from a registry V2");
            _options.put(dockerImageName,imageName);
            return this;
        }

        _options.put(dockerImageName,pinName);
        return this;
    }

    public String getDockerImageName() {
        return getDockerImageName(null);
    }

    public String getDockerImageName(String defaultValue) {
        String result = _options.get(dockerImageName);
        if(result == null) {
            return defaultValue;
        }
        return result;
    }

    public DUUIPipelineComponent withDockerImageFetching(Boolean b) {
        if(_finalizedEncoded!=null) {
            throw new RuntimeException("DUUIPipelineComponent has already been finalized, it is immutable now!");
        }

        if(b==null) {
            _options.remove(dockerImageFetching);
            return this;
        }
        _options.put(dockerImageFetching,String.valueOf(b));
        return this;
    }

    public Boolean getDockerImageFetching() {
        return getDockerImageFetching(null);
    }

    public Boolean getDockerImageFetching(Boolean defaultValue) {
        String result = _options.get(dockerImageFetching);
        if(result == null) return defaultValue;
        return Boolean.parseBoolean(result);
    }

    public DUUIPipelineComponent withWriteToView(String viewName) {
        return withWriteToView(viewName,false);
    }

    public DUUIPipelineComponent withWriteToView(String viewName, boolean createViewFromInitialView) {
        if(_finalizedEncoded!=null) {
            throw new RuntimeException("DUUIPipelineComponent has already been finalized, it is immutable now!");
        }

        if(viewName==null) {
            _options.remove(writeToViewName);
            _options.remove(initialViewFromInitialViewName);
            return this;
        }
        _options.put(writeToViewName,viewName);
        _options.put(initialViewFromInitialViewName,String.valueOf(createViewFromInitialView));
        return this;
    }

    public Boolean getCreateViewFromInitialView() {
        String value = _options.get(initialViewFromInitialViewName);
        if(value == null) return null;
        return Boolean.valueOf(value);
    }

    public String getViewName() {
        return getViewName(null);
    }

    public String getViewName(String defaultValue) {
        String value = _options.get(writeToViewName);
        if(value==null) return defaultValue;
        return value;
    }

    public String toJson() {
        JSONObject js = new JSONObject();
        js.put("options",_options);
        js.put("parameters",_parameters);
        return js.toString();
    }

    /**
     * @author Dawit Terefe (edited)
     *
     * Option to choose websocket as protocol.
     * Default is false.
     * Option to choose number of elements for
     * partition size.
     *
     */
    public boolean isWebsocket() { return _websocket; }

    public DUUIPipelineComponent withWebsocket(boolean b) {
        _websocket = b;
        return withParameter(websocketOptionName, String.valueOf(b));
    }

    public int getWebsocketElements () { return _ws_elements; }

    public DUUIPipelineComponent withWebsocket(boolean b, int elements) {
        _websocket = b;
        _ws_elements = elements;
        return withParameter(websocketOptionName, String.valueOf(b))
                .withParameter(websocketElementsOptionName, String.valueOf(elements));
    }

    public DUUIPipelineComponent withParameter(String key, String value) {
        _parameters.put(key,value);
        return this;
    }

    public DUUIPipelineComponent withView(String viewName){
        withSourceView(viewName);
        withTargetView(viewName);
        return this;
    }

    public DUUIPipelineComponent withSourceView(String viewName) {
        _options.put(sourceView, viewName);
        return this;
    }


    public DUUIPipelineComponent withTargetView(String viewName) {
        _options.put(targetView, viewName);
        return this;
    }

    public DUUIPipelineComponent withTimeout(long lLong) {
        _parameters.put(timeout, String.valueOf(lLong));
        return this;
    }

    public static DUUIPipelineComponent fromJson(String json) throws URISyntaxException, IOException {
        JSONObject jobj = new JSONObject(json);

        HashMap<String,String> optionsMap = new HashMap<>();
        JSONObject options = jobj.getJSONObject("options");
        for (Iterator<String> it = options.keys(); it.hasNext(); ) {
            String key = it.next();
            optionsMap.put(key,options.getString(key));
        }

        HashMap<String,String> parametersMap = new HashMap<>();
        JSONObject parameters = jobj.getJSONObject("parameters");
        for (Iterator<String> it = parameters.keys(); it.hasNext(); ) {
            String key = it.next();
            parametersMap.put(key,parameters.getString(key));
        }

        DUUIPipelineComponent comp = new DUUIPipelineComponent();
        comp._options = optionsMap;
        comp._parameters = parametersMap;
        return comp;
    }

    public static DUUIPipelineComponent fromEncodedJson(String encodedJson) throws URISyntaxException, IOException, CompressorException {
        return fromEncodedJson(encodedJson,compressionMethod);
    }

    public static DUUIPipelineComponent fromEncodedJson(String encodedJson, String compressionUsed) throws URISyntaxException, IOException, CompressorException {
        byte[] decoded = Base64.getDecoder().decode(encodedJson.getBytes(StandardCharsets.UTF_8));
        CompressorInputStream stream = new CompressorStreamFactory().createCompressorInputStream(compressionUsed,new ByteArrayInputStream(decoded));
        String json = new String(stream.readAllBytes(),StandardCharsets.UTF_8);

        return fromJson(json);
    }

    public DUUIPipelineComponent join(DUUIPipelineComponent comp, DUUIPipelineComponentJoinStrategy strategy) {
        if(_finalizedEncoded!=null) {
            throw new RuntimeException("DUUIPipelineComponent has already been finalized, it is immutable now!");
        }

        if(strategy == DUUIPipelineComponentJoinStrategy.LEFT_WINS) {
            for(String i : comp._options.keySet()) {
                //Discard keys we have too write others into us
                if(!_options.containsKey(i)) {
                    _options.put(i,comp._options.get(i));
                }
            }
        }
        else if(strategy == DUUIPipelineComponentJoinStrategy.RIGHT_WINS) {
            //Write everything since we loose on conflict and need missing options anyways
            for(String i : comp._options.keySet()) {
                _options.put(i,comp._options.get(i));
            }
        }
        else {
            for(String i : comp._options.keySet()) {
                if(_options.containsKey(i)) {
                    if(!_options.get(i).equals(comp._options.get(i))) {
                        throw new RuntimeException(String.format("Key conflict in join in key %s", i));
                    }
                }
                else {
                    _options.put(i,comp._options.get(i));
                }
            }
        }
        return this;
    }

    public final Map<String,String> getParameters() {
        return _parameters;
    }

    public String getSourceView() {
        String result = _options.get(sourceView);
        if(result == null) {
            return "_InitialView";
        }
        return result;
    }

    public String getTargetView() {
        String result = _options.get(targetView);
        if(result == null) {
            return "_InitialView";
        }
        return result;
    }

    public DUUIPipelineComponent clearParameters() {
        _parameters.clear();
        return this;
    }

    public boolean isCompatible(IDUUIDriverInterface driver) throws InvalidXMLException, IOException, SAXException {
        return driver.canAccept(this);
    }

    public DUUIDockerDriver.Component asDockerDriverComponent() throws URISyntaxException, IOException {
        return new DUUIDockerDriver.Component(this);
    }

    public DUUISwarmDriver.Component asSwarmDriverComponent() throws URISyntaxException, IOException {
        return new DUUISwarmDriver.Component(this);
    }

    public DUUIRemoteDriver.Component asRemoteDriverComponent() throws URISyntaxException, IOException {
        return new DUUIRemoteDriver.Component(this);
    }

    public DUUIUIMADriver.Component asUIMADriverComponent() throws URISyntaxException, IOException, InvalidXMLException, SAXException {
        return new DUUIUIMADriver.Component(this);
    }

    public String attemptAutomaticDescription() throws InvalidXMLException, URISyntaxException, IOException, SAXException {
        if(getDriver().equals(DUUIUIMADriver.class.getCanonicalName())) {
            return String.format("UIMA annotator: %s, scale: %d",asUIMADriverComponent().getAnnotatorName(),getScale(1));
        }
        else if(getDriver().equals(DUUIDockerDriver.class.getCanonicalName())) {
            return String.format("Docker annotator: %s, scale: %d",getDockerImageName(),getScale(1));
        }
        else if(getDriver().equals(DUUISwarmDriver.class.getCanonicalName())) {
            return String.format("Swarm annotator: %s, scale: %d",getDockerImageName(),getScale(1));
        }
        else if(getDriver().equals(DUUIRemoteDriver.class.getCanonicalName())) {
            String urls = "[";
            for(String x : getUrl()) {
                urls+=x;
                urls+=",";
            }
            urls = urls.substring(0,urls.length()-1)+"]";
            return String.format("Remote annotator: %s, scale: %d",urls,getScale(1));
        }
        else {
            return "Unkown annotator driver!";
        }
    }

    public DUUIPipelineComponent withSegmentationStrategy(DUUISegmentationStrategy strategy) {
        this.segmentationStrategy = strategy;
        return this;
    }

    public DUUISegmentationStrategy getSegmentationStrategy() {
        return this.segmentationStrategy;
    }

    public long getTimeout() {
        return Long.valueOf(_parameters.getOrDefault(timeout, "60"));
    }

}
