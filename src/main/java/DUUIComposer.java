import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.TypeSystemUtils;
import org.apache.uima.cas.impl.XCASSerializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.TypeSystemUtil;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Vector;

public class DUUIComposer {
    private Vector<String> _repositories;
    private DUUIDockerInterface _interface;
    private OkHttpClient _client;
    private String _testCas;
    private String _testTypesystem;

    public DUUIComposer() throws IOException, UIMAException, SAXException {
        _interface = new DUUIDockerInterface();
        _client = new OkHttpClient();
        JCas _basic = JCasFactory.createJCas();
        _basic.setDocumentLanguage("en");
        _basic.setDocumentText("Hello World!");

        ByteArrayOutputStream arr = new ByteArrayOutputStream();
        XmiCasSerializer.serialize(_basic.getCas(), _basic.getTypeSystem(), arr);
        _testCas = arr.toString();
        System.out.printf("Generated test cas %s\n",_testCas);

        TypeSystemDescription desc = TypeSystemUtil.typeSystem2TypeSystemDescription(_basic.getTypeSystem());
        StringWriter wr = new StringWriter();
        desc.toXML(wr);
        _testTypesystem = wr.getBuffer().toString();
        System.out.printf("Generated test cas typesystem %s\n",_testTypesystem);
    }

    public DUUIPipeline generatePipeline(DUUIPipeline pipeline, int timeout_ms) throws InterruptedException, IOException {
        for(String image : pipeline.getImages()) {
            //_interface.pullImage(image);
            String containerid = _interface.run(image,false,false);
            int port = _interface.extract_port_mapping(containerid);

            System.out.printf("Got container %s with port %d\n",containerid,port);
            if(port==0) {
                throw new UnknownError("Could not read the container port!");
            }


            JSONObject obj = new JSONObject();
            obj.put("cas",_testCas);
            obj.put("typesystem",_testTypesystem);
            String ok = obj.toString();

            RequestBody bod = RequestBody.create(obj.toString().getBytes(StandardCharsets.UTF_8));

            long start = System.currentTimeMillis();
            while(true) {
                Request request = new Request.Builder()
                        .url("http://localhost:"+String.valueOf(port)+"/v1/process")
                        .post(bod)
                        .build();
                try {
                    Response resp = _client.newCall(request).execute();
                    if (resp.code()==200) {
                        String body = new String(resp.body().bytes(), Charset.defaultCharset());
                        System.out.println("Response "+body);
                        JSONObject response = new JSONObject(body);
                        if(response.has("cas") || response.has("error")) {
                            break;
                        }
                        else {
                            System.out.println("The response is not in the expected format!");
                        }
                    }

                }
                catch(Exception e) {
                    //e.printStackTrace();
                }
                long finish = System.currentTimeMillis();
                long timeElapsed = finish - start;
                if(timeElapsed>timeout_ms) {
                    _interface.stop_container(containerid);
                    throw new UnknownError("Container did not respond in time, shutting down");
                }
            }
            System.out.printf("Container for image %s is online and seems to understand DUUI V1 format!\n",image);
            System.out.printf("Stopping container now...",image);
            _interface.stop_container(containerid);
        }

        return pipeline;
    }

    public static void main(String[] args) throws IOException, InterruptedException, UIMAException, SAXException {
        DUUIComposer comp = new DUUIComposer();
        DUUIPipeline pipe = new DUUIPipeline();

        DUUILocalDriver driver = new DUUILocalDriver();

        //DUUISwarmDriver swarmDriver = new DUUISwarmDriver()
        //                                  .with_replicas(4);

        pipe.addLocal("new:latest");
        //pipe.addLocal("local_image:latest");
        //pipe.addRemote("http://www.texttechnologylab.de/uima/container_spacy")

        pipe = comp.generatePipeline(pipe,5000);

        JCas jc = JCasFactory.createJCas();
        jc.setDocumentLanguage("en");
        jc.setDocumentText("Hello World!");

        driver.run(pipe,jc);

        ByteArrayOutputStream arr = new ByteArrayOutputStream();
        XmiCasSerializer.serialize(jc.getCas(),arr);
        System.out.printf("Result %s",arr.toString());

        System.out.println("Hello, World!");
    }
}