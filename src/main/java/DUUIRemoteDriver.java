import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.TypeSystemUtil;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

public class DUUIRemoteDriver implements IDUUIDriverInterface {
    private HashMap<String, InstantiatedComponent> _components;
    private OkHttpClient _client;
    private DUUICompressionHelper _helper;


    public static class Component extends IDUUIPipelineComponent {
        Component(String url) {
            setOption("url", url);
        }

        public Component withScale(int scale) {
            setOption("scale", String.valueOf(scale));
            return this;
        }
    }

    private static class InstantiatedComponent extends IDUUIPipelineComponent {
        private String _url;
        private AtomicInteger _maximum_concurrency;

        InstantiatedComponent(IDUUIPipelineComponent comp) {
            _url = comp.getOption("url");
            if (_url == null) {
                throw new InvalidParameterException("Missing parameter URL in the pipeline component descriptor");
            }

            String max = comp.getOption("scale");
            if(max != null) {
                _maximum_concurrency = new AtomicInteger(Integer.valueOf(max));
            }
            else {
                _maximum_concurrency = new AtomicInteger(1);
            }
        }

        public void enter() {
            while(true) {
                int before = _maximum_concurrency.get();
                while (before < 1) {
                    before = _maximum_concurrency.get();
                }
                int result = _maximum_concurrency.compareAndExchange(before,before-1);
                if(result==before)
                    return;
            }
        }

        public void leave() {
            _maximum_concurrency.addAndGet(1);
        }

        public int getScale() {
            return _maximum_concurrency.get();
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

    public String instantiate(IDUUIPipelineComponent component) throws InterruptedException, TimeoutException, UIMAException, SAXException, CompressorException, IOException {
        String uuid = UUID.randomUUID().toString();
        while (_components.containsKey(uuid)) {
            uuid = UUID.randomUUID().toString();
        }
        InstantiatedComponent comp = new InstantiatedComponent(component);
        JCas _basic = JCasFactory.createJCas();
        DUUIEither aCas = new DUUIEither(_basic,CompressorStreamFactory.GZIP);
        System.out.printf("[RemoteDriver] Assigned new pipeline component unique id %s\n", uuid);

        String cas = aCas.getAsString();
        String typesystem = aCas.getTypesystem();

        JSONObject obj = new JSONObject();
        obj.put("cas", cas);
        obj.put("typesystem", typesystem);
        obj.put("typesystem_hash", typesystem.hashCode());
        obj.put("cas_hash", cas.hashCode());
        obj.put("compression",aCas.getCompressionMethod());

        String ok = obj.toString();
        RequestBody bod = RequestBody.create(obj.toString().getBytes(StandardCharsets.UTF_8));

        if (DUUILocalDriver.responsiveAfterTime(comp.getUrl(), bod, 10000, _client)) {
            _components.put(uuid, comp);
            System.out.printf("[RemoteDriver][%s] Remote URL %s is online and seems to understand DUUI V1 format!\n", uuid, comp.getUrl());
            System.out.printf("[RemoteDriver][%s] Maximum concurrency for this endpoint %d\n", uuid, comp.getScale());

        } else {
            throw new TimeoutException(format("The URL %s did not provide one succesful answer! Aborting...", comp.getUrl()));
        }

        return uuid;
    }

    public void printConcurrencyGraph(String uuid) {
        InstantiatedComponent component = _components.get(uuid);
        if (component == null) {
            throw new InvalidParameterException("Invalid UUID, this component has not been instantiated by the local Driver");
        }
        System.out.printf("[RemoteDriver][%s]: Maximum concurrency %d\n",uuid,component.getScale());
    }

    public DUUIEither run(String uuid, DUUIEither aCas) throws InterruptedException, IOException, SAXException, CompressorException {
        InstantiatedComponent comp = _components.get(uuid);
        if (comp == null) {
            throw new InvalidParameterException("The given instantiated component uuid was not instantiated by the remote driver");
        }
        String cas = aCas.getAsString();
        String typesystem = aCas.getTypesystem();

        JSONObject obj = new JSONObject();
        obj.put("cas", cas);
        obj.put("typesystem", typesystem);
        obj.put("typesystem_hash", typesystem.hashCode());
        obj.put("cas_hash", cas.hashCode());
        obj.put("compression",aCas.getCompressionMethod());
        String ok = obj.toString();

        RequestBody bod = RequestBody.create(ok.getBytes(StandardCharsets.UTF_8));

        Request request = new Request.Builder()
                .url(comp.getUrl() + "/v1/process")
                .post(bod)
                .header("Content-Length", String.valueOf(ok.length()))
                .build();
        comp.enter();
        Response resp = _client.newCall(request).execute();
        comp.leave();
        if (resp.code() == 200) {
            String body = new String(resp.body().bytes(), Charset.defaultCharset());
            JSONObject response = new JSONObject(body);
            if (response.has("cas") || response.has("error")) {
                aCas.updateStringBuffer(response.getString("cas"));
                return aCas;
            } else {
                System.out.println("The response is not in the expected format!");
                throw new InvalidObjectException("Response is not in the right format!");
            }
        } else {
            throw new InvalidObjectException("Response code != 200, error");
        }
    }

    public void destroy(String uuid) {
        _components.remove(uuid);
    }
}
