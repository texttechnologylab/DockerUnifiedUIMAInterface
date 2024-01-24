package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import com.google.protobuf.MessageLite;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.util.XMLSerializer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIStatus;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DUUIDocument {

    private String name;
    /**
     * The absolute path to the document including the name.
     */
    private String path;
    private long size;
    private byte[] bytes;
    private final AtomicInteger progess = new AtomicInteger(0);
    private String status = DUUIStatus.WAITING;
    private String error;
    private boolean isFinished = false;
    private long durationDecode = 0L;
    private long durationDeserialize = 0L;
    private long durationWait = 0L;
    private long durationProcess = 0L;
    private long startedAt = 0L;
    private long finishedAt = 0L;
    private long uploadProgress;
    private long downloadProgress;
    private ByteArrayOutputStream outputStream;
    private String outputName;
    private String result;
    private Map<String, Integer> annotations = new HashMap<>();

    public DUUIDocument(String name, String path, long size) {
        this.name = name;
        this.path = path;
        this.size = size;
        this.outputName = name.replace(getFileExtension(), ".txt");
    }

    public DUUIDocument(String name, String path) {
        this.name = name;
        this.path = path;
        this.outputName = name.replace(getFileExtension(), ".txt");
    }

    public DUUIDocument(String name, String path, byte[] bytes) {
        this.name = name;
        this.path = path;
        this.bytes = bytes;
        this.outputName = name.replace(getFileExtension(), ".txt");
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof DUUIDocument)) {
            return false;
        }

        DUUIDocument _o = (DUUIDocument) o;
        return _o.getPath().equals(getPath());
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Retrieve the documents file extension.
     *
     * @return The file extension including the dot character. E.G. '.txt'.
     */
    public String getFileExtension() {
        int extensionStartIndex = name.lastIndexOf('.');
        if (extensionStartIndex == -1) return "";
        return name.substring(extensionStartIndex);
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
        this.size = bytes.length;
    }

    /**
     * Convert the bytes into a ByteArrayInputStream for processing.
     *
     * @return A new {@link ByteArrayInputStream} containing the content of the document.
     */
    public ByteArrayInputStream toInputStream() {
        return new ByteArrayInputStream(bytes);
    }

    /**
     * Convert the bytes into a String.
     *
     * @return A new String containing the content of the document.
     */
    public String getText() {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Increment the document progress by one.
     */
    public void incrementProgress() {
        progess.incrementAndGet();
    }

    public AtomicInteger getProgess() {
        return progess;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public boolean isFinished() {
        return isFinished;
    }

    public void setFinished(boolean finished) {
        isFinished = finished;
    }

    public long getDurationDecode() {
        return durationDecode;
    }

    public void setDurationDecode(long durationDecode) {
        this.durationDecode = durationDecode;
    }

    public long getDurationDeserialize() {
        return durationDeserialize;
    }

    public void setDurationDeserialize(long durationDeserialize) {
        this.durationDeserialize = durationDeserialize;
    }

    public long getDurationWait() {
        return durationWait;
    }

    public void setDurationWait(long durationWait) {
        this.durationWait = durationWait;
    }

    public long getDurationProcess() {
        return durationProcess;
    }

    public void setDurationProcess(long durationProcess) {
        this.durationProcess = durationProcess;
    }

    public long getStartedAt() {
        return startedAt;
    }

    /**
     * Utility method to set the startedAt timestamp to the current time.
     */
    public void setStartedAt() {
        startedAt = Instant.now().toEpochMilli();
    }

    public void setStartedAt(long startedAt) {
        this.startedAt = startedAt;
    }

    public long getFinishedAt() {
        return finishedAt;
    }

    /**
     * Utility method to set the finishedAt timestamp to the current time.
     */
    public void setFinishedAt() {
        finishedAt = Instant.now().toEpochMilli();
    }

    public void setFinishedAt(long finishedAt) {
        this.finishedAt = finishedAt;
    }

    public long getUploadProgress() {
        return uploadProgress;
    }

    public void setUploadProgress(long uploadProgress) {
        this.uploadProgress = uploadProgress;
    }

    public long getDownloadProgress() {
        return downloadProgress;
    }

    public void setDownloadProgress(long downloadProgress) {
        this.downloadProgress = downloadProgress;
    }

    public ByteArrayInputStream getResult() {
        return new ByteArrayInputStream(outputStream.toByteArray());
    }

    public void setResult(JCas cas, boolean serialize) {
        if (serialize) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            XmiCasSerializer xmiCasSerializer = new XmiCasSerializer(null);
            XMLSerializer sax2xml = new XMLSerializer(outputStream);


            try {
                xmiCasSerializer.serialize(cas.getCas(), sax2xml.getContentHandler(), null, null, null);
                this.outputStream = outputStream;
            } catch (SAXException ignored) {
            }
        }

        JCasUtil.select(cas, TOP.class).forEach(a -> {
            String name = a.getType().getName();

            int count = 0;

            if (annotations.containsKey(name)) {
                count = annotations.get(name);
            }
            count++;
            annotations.put(name, count);
        });

    }

    public String getOutputName() {
        return outputName;
    }

    public void setOutputName(String outputName) {
        this.outputName = outputName;
    }

    public ByteArrayOutputStream getOutputStream() {
        return this.outputStream;
    }

    public Map<String, Integer> getAnnotations() {
        return annotations;
    }
}
