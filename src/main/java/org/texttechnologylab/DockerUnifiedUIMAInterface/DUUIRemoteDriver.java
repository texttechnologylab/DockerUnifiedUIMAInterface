package org.texttechnologylab.DockerUnifiedUIMAInterface;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

public class DUUIRemoteDriver implements IDUUIDriverInterface {
    private HashMap<String, InstantiatedComponent> _components;
    private OkHttpClient _client;
    private DUUICompressionHelper _helper;
    private DUUILuaContext _luaContext;


    public static class Component extends IDUUIPipelineComponent {
        Component(String url) {
            setOption("url", url);
        }

        public Component withScale(int scale) {
            setOption("scale", String.valueOf(scale));
            return this;
        }
    }

    public void setLuaContext(DUUILuaContext luaContext) {
        _luaContext = luaContext;
    }

    private static class InstantiatedComponent extends IDUUIPipelineComponent {
        private String _url;
        private int _maximum_concurrency;
        private ConcurrentLinkedQueue<IDUUICommunicationLayer> _communication;


        InstantiatedComponent(IDUUIPipelineComponent comp) {
            _url = comp.getOption("url");
            if (_url == null) {
                throw new InvalidParameterException("Missing parameter URL in the pipeline component descriptor");
            }

            String max = comp.getOption("scale");
            if(max != null) {
                _maximum_concurrency = Integer.valueOf(max);
            }
            else {
                _maximum_concurrency = 1;
                _communication = new ConcurrentLinkedQueue<>();
            }
        }

        public void addCommunicationLayer(IDUUICommunicationLayer layer) {
            for(int i = 0; i < _maximum_concurrency; i++) {
                _communication.add(layer.copy());
            }
        }

        public ConcurrentLinkedQueue<IDUUICommunicationLayer> getInstances() {
            return _communication;
        }

        public void returnCommunicationLayer(IDUUICommunicationLayer layer) {
            _communication.add(layer);
        }

        public int getScale() {
            return _maximum_concurrency;
        }

        public String getUrl() {
            return _url;
        }
    }

    DUUIRemoteDriver(int timeout) {
        _client = new OkHttpClient.Builder()
                .connectTimeout(timeout, TimeUnit.SECONDS)
                .writeTimeout(timeout, TimeUnit.SECONDS)
                .readTimeout(timeout, TimeUnit.SECONDS)
                .build();
        _components = new HashMap<String, InstantiatedComponent>();
        _helper = new DUUICompressionHelper(CompressorStreamFactory.ZSTANDARD);
    }

    DUUIRemoteDriver() {
        _components = new HashMap<String, InstantiatedComponent>();
        _client = new OkHttpClient();
        _helper = new DUUICompressionHelper(CompressorStreamFactory.ZSTANDARD);
    }

    public boolean canAccept(IDUUIPipelineComponent component) {
        return component.getClass().getCanonicalName() == component.getClass().getCanonicalName();
    }

    public String instantiate(IDUUIPipelineComponent component) throws Exception {
        String uuid = UUID.randomUUID().toString();
        while (_components.containsKey(uuid)) {
            uuid = UUID.randomUUID().toString();
        }
        InstantiatedComponent comp = new InstantiatedComponent(component);
        JCas _basic = JCasFactory.createJCas();
        _basic.setDocumentLanguage("de");
        _basic.setDocumentText("Halo Welt!");
        System.out.printf("[RemoteDriver] Assigned new pipeline component unique id %s\n", uuid);


        final String uuidCopy = uuid;
        IDUUICommunicationLayer layer = DUUILocalDriver.responsiveAfterTime(comp.getUrl(), _basic, 100000, _client, (msg) -> {
            System.out.printf("[RemoteDriver][%s] %s\n", uuidCopy,msg);
        },_luaContext);
        comp.addCommunicationLayer(layer);
        _components.put(uuid, comp);
        System.out.printf("[RemoteDriver][%s] Remote URL %s is online and seems to understand DUUI V1 format!\n", uuid, comp.getUrl());
        System.out.printf("[RemoteDriver][%s] Maximum concurrency for this endpoint %d\n", uuid, comp.getScale());
        return uuid;
    }

    public void printConcurrencyGraph(String uuid) {
        InstantiatedComponent component = _components.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        System.out.printf("[RemoteDriver][%s]: Maximum concurrency %d\n",uuid,component.getScale());
    }

    public TypeSystemDescription get_typesystem(String uuid) throws InterruptedException, IOException, SAXException, CompressorException {
        DUUIRemoteDriver.InstantiatedComponent comp = _components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        Request request = new Request.Builder()
                .url(comp.getUrl() + "/v1/typesystem")
                .get()
                .build();
        Response resp = _client.newCall(request).execute();
        if (resp.code() == 200) {
            String body = new String(resp.body().bytes(), Charset.defaultCharset());
            File tmp = File.createTempFile("duui.composer","_type");
            tmp.deleteOnExit();
            FileWriter writer = new FileWriter(tmp);
            writer.write(body);
            writer.flush();
            writer.close();
            return TypeSystemDescriptionFactory.createTypeSystemDescriptionFromPath(tmp.getAbsolutePath());
        } else {
            throw new InvalidObjectException("Response code != 200, error");
        }
    }

    public DUUIEither run(String uuid, DUUIEither aCas) throws InterruptedException, IOException, SAXException, CompressorException {
        InstantiatedComponent comp = _components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("The given instantiated component uuid was not instantiated by the remote driver");
        }

        IDUUICommunicationLayer inst = comp.getInstances().poll();
        while(inst == null) {
            inst = comp.getInstances().poll();
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        inst.serialize(aCas.getAsJCas(),out);
        String ok = out.toString();
        RequestBody bod = RequestBody.create(ok.getBytes(StandardCharsets.UTF_8));

        Request request = new Request.Builder()
                .url(comp.getUrl() + "/v1/process")
                .post(bod)
                .header("Content-Length", String.valueOf(ok.length()))
                .build();

        Response resp = _client.newCall(request).execute();
        if (resp.code() == 200) {
            InputStream stream = resp.body().byteStream();
            inst.deserialize(aCas.getAsJCas(),stream);
            comp.returnCommunicationLayer(inst);
        } else {
            comp.returnCommunicationLayer(inst);
            throw new InvalidObjectException("Response code != 200, error");
        }
        return aCas;
    }

    public void destroy(String uuid) {
        _components.remove(uuid);
    }
}
