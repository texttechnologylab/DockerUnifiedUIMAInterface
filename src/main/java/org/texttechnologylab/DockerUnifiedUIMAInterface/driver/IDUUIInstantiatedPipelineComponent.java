package org.texttechnologylab.DockerUnifiedUIMAInterface.driver;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.IDUUICommunicationLayer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.Charset;
import java.security.InvalidParameterException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public interface IDUUIInstantiatedPipelineComponent {
    static OkHttpClient _client = new OkHttpClient();
    public IDUUICommunicationLayer getCommunicationLayer();
    public Triplet<IDUUIUrlAccessible,Long,Long> getComponent();
    public void addComponent(IDUUIUrlAccessible item);

    public Map<String,String> getParameters();
    public String getUniqueComponentKey();
    public void setCommunicationLayer(IDUUICommunicationLayer layer);

    public static TypeSystemDescription getTypesystem(String uuid, IDUUIInstantiatedPipelineComponent comp) throws IOException, ResourceInitializationException {
        Triplet<IDUUIUrlAccessible,Long,Long> queue = comp.getComponent();
        Request request = new Request.Builder()
                .url(queue.getValue0().generateURL() + DUUIComposer.V1_COMPONENT_ENDPOINT_TYPESYSTEM)
                .get()
                .build();
        Response resp = _client.newCall(request).execute();
        if (resp.code() == 200) {
            String body = new String(resp.body().bytes(), Charset.defaultCharset());
            File tmp = File.createTempFile("duui.composer","_type");
            FileWriter writer = new FileWriter(tmp);
            writer.write(body);
            writer.flush();
            writer.close();
            comp.addComponent(queue.getValue0());
            return TypeSystemDescriptionFactory.createTypeSystemDescriptionFromPath(tmp.getAbsolutePath());
        } else {
            comp.addComponent(queue.getValue0());
            System.out.printf("[%s]: Endpoint did not provide typesystem, using default one...\n",uuid);
            return TypeSystemDescriptionFactory.createTypeSystemDescription();
        }
    }

    public static void process(JCas jc, IDUUIInstantiatedPipelineComponent comp, DUUIPipelineDocumentPerformance perf) throws CompressorException, IOException, SAXException {
        Triplet<IDUUIUrlAccessible,Long,Long> queue = comp.getComponent();

        IDUUICommunicationLayer layer = comp.getCommunicationLayer();
        long serializeStart = System.nanoTime();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        layer.serialize(jc,out,comp.getParameters());
        byte[] ok = out.toByteArray();
        long serializeEnd = System.nanoTime();

        long annotatorStart = serializeEnd;

        RequestBody body = RequestBody.create(ok);
        Request request = new Request.Builder()
                .url(queue.getValue0().generateURL()+ DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS)
                .post(body)
                .header("Content-Length", String.valueOf(ok.length))
                .build();
        Response resp = _client.newCall(request).execute();

        if (resp.code() == 200) {
            ByteArrayInputStream st = new ByteArrayInputStream(resp.body().bytes());
            long annotatorEnd = System.nanoTime();
            long deserializeStart = annotatorEnd;
            layer.deserialize(jc,st);
            long deserializeEnd = System.nanoTime();
            perf.addData(serializeEnd-serializeStart,deserializeEnd-deserializeStart,annotatorEnd-annotatorStart,queue.getValue2()-queue.getValue1(),deserializeEnd-queue.getValue1(),comp.getUniqueComponentKey());
            comp.addComponent(queue.getValue0());
        } else {
            comp.addComponent(queue.getValue0());
            throw new InvalidObjectException("Response code != 200, error");
        }
    }
}
