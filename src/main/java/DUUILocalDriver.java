import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.TypeSystemUtil;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class DUUILocalDriver {
    private DUUIDockerInterface _interface;
    private OkHttpClient _client;

    DUUILocalDriver() throws IOException {
        _interface = new DUUIDockerInterface();
        _client = new OkHttpClient();
    }

    public void run(DUUIPipeline pipeline, JCas aCas) throws InterruptedException, IOException, SAXException {
        for(String image : pipeline.getImages()) {
            String containerid = _interface.run(image,false,false);
            int port = _interface.extract_port_mapping(containerid);

            System.out.printf("Got container %s with port %d\n",containerid,port);
            if(port==0) {
                throw new UnknownError("Could not read the container port!");
            }
            ByteArrayOutputStream arr = new ByteArrayOutputStream();
            XmiCasSerializer.serialize(aCas.getCas(), aCas.getTypeSystem(), arr);
            String cas = arr.toString();

            TypeSystemDescription desc = TypeSystemUtil.typeSystem2TypeSystemDescription(aCas.getTypeSystem());
            StringWriter wr = new StringWriter();
            desc.toXML(wr);
            String typesystem = wr.getBuffer().toString();

            JSONObject obj = new JSONObject();
            obj.put("cas",cas);
            obj.put("typesystem",typesystem);
            String ok = obj.toString();

            int max_errors = 500;
            int errors = 0;
            RequestBody bod = RequestBody.create(obj.toString().getBytes(StandardCharsets.UTF_8));
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
                            aCas.reset();
                            XmiCasDeserializer.deserialize(new ByteArrayInputStream(response.getString("cas").getBytes(StandardCharsets.UTF_8)),aCas.getCas());
                            break;
                        }
                        else {
                            System.out.println("The response is not in the expected format!");
                        }
                    }

                }
                catch(Exception e) {
                    e.printStackTrace();
                    errors+=1;
                    if(errors>=max_errors) {
                        System.out.printf("Too many errors, stopping container now...",image);
                        _interface.stop_container(containerid);
                        throw e;
                    }
                }
            }
            System.out.printf("Container for image %s is online and seems to understand DUUI V1 format!\n",image);
            System.out.printf("Stopping container now...",image);
            _interface.stop_container(containerid);
        }
    }
}
