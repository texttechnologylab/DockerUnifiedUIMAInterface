package org.texttechnologylab.DockerUnifiedUIMAInterface.io.writer;

import com.dropbox.core.DbxException;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.oauth.DbxCredential;
import com.dropbox.core.v2.files.WriteMode;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASRuntimeException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.TypeSystemUtil;
import org.apache.uima.util.XMLSerializer;
import org.dkpro.core.api.io.JCasFileWriter_ImplBase;
import org.dkpro.core.api.parameter.ComponentParameters;
import org.dkpro.core.api.resources.CompressionUtils;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.*;
import org.xml.sax.SAXException;

import javax.xml.transform.OutputKeys;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;

import static java.util.Arrays.asList;
import static org.apache.commons.io.IOUtils.closeQuietly;

/**
 * A version of the xmi writer that writes its output to an external data storage.
 */
public class DocumentWriter extends JCasFileWriter_ImplBase {


    public static final String PARAM_PRETTY_PRINT = "prettyPrint";
    @ConfigurationParameter(name = PARAM_PRETTY_PRINT, mandatory = true, defaultValue = "true")
    private boolean prettyPrint;

    /**
     * Location to write the type system to. If this is not set, a file called typesystem.xml will
     * be written to the XMI output path. If this is set, it is expected to be a file relative
     * to the current work directory or an absolute file.
     * <br>
     * If this parameter is set, the {@link #PARAM_COMPRESSION} parameter has no effect on the
     * type system. Instead, if the file name ends in ".gz", the file will be compressed,
     * otherwise not.
     */
    public static final String PARAM_TYPE_SYSTEM_FILE = "typeSystemFile";
    @ConfigurationParameter(name = PARAM_TYPE_SYSTEM_FILE, mandatory = false)
    private File typeSystemFile;

    /**
     * Specify the suffix of output files. Default value <code>.xmi</code>. If the suffix is not
     * needed, provide an empty string as value.
     */
    public static final String PARAM_FILENAME_EXTENSION =
        ComponentParameters.PARAM_FILENAME_EXTENSION;
    @ConfigurationParameter(name = PARAM_FILENAME_EXTENSION, mandatory = true, defaultValue = ".xmi")
    private String filenameSuffix;

    /**
     * Defines the XML version used for serializing the data. The default is XML {@code "1.0"}.
     * However, XML 1.0 does not support certain Unicode characters. To support a wider range of
     * characters, you can switch this parameter to {@code "1.1"}.
     */
    public static final String PARAM_VERSION = "version";
    @ConfigurationParameter(name = PARAM_VERSION, mandatory = true, defaultValue = "1.0")
    private String version;

    public static final String PARAM_PROVIDER = "provider";
    @ConfigurationParameter(name = PARAM_PROVIDER, mandatory = true, defaultValue = "none")
    private String provider;

    public static final String PARAM_WRITEMODE = "writeMode";
    @ConfigurationParameter(name = PARAM_WRITEMODE, mandatory = false, defaultValue = "add")
    private String writemode;

    private boolean typeSystemWritten;

    private IDUUIDocumentHandler handler;

    public DocumentWriter(IDUUIDocumentHandler handler) {
        this.handler = handler;
    }
    @Override
    public void initialize(UimaContext aContext)
        throws ResourceInitializationException {
        super.initialize(aContext);

        if (!asList("1.0", "1.1").contains(version)) {
            throw new ResourceInitializationException(new IllegalArgumentException(
                "Invalid value for parameter version: [" + version + "]"));
        }

        switch (provider.toLowerCase()) {
            case "dropbox":
                try {
                    handler = new DUUIDropboxDocumentHandler(
                        new DbxRequestConfig("DUUI"),
                        new DbxCredential(
                            System.getenv("dbx_personal_access_token"),
                            1L,
                            System.getenv("dbx_personal_refresh_token"),
                            System.getenv("dbx_app_key"),
                            System.getenv("dbx_app_secret"))
                    );
                    if (writemode.equalsIgnoreCase("add")) {
                        ((DUUIDropboxDocumentHandler) handler).setWriteMode(WriteMode.ADD);
                    } else {
                        ((DUUIDropboxDocumentHandler) handler).setWriteMode(WriteMode.OVERWRITE);
                    }
                } catch (DbxException e) {
                    throw new RuntimeException(e);
                }
                break;
            case "local":
                handler = new DUUILocalDocumentHandler();
                break;
            case "minio":
                handler = new DUUIMinioDocumentHandler(
                    "http://192.168.2.122:9000",
                    System.getenv("minio_key"),
                    System.getenv("minio_secret")
                );
                break;
            default:
                throw new RuntimeException("Unsupported provider: " + provider);
        }

        typeSystemWritten = false;
    }

    @Override
    public void process(JCas aJCas) throws AnalysisEngineProcessException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            XmiCasSerializer xmiCasSerializer = new XmiCasSerializer(null);
            XMLSerializer sax2xml = new XMLSerializer(byteArrayOutputStream, prettyPrint);
            sax2xml.setOutputProperty(OutputKeys.VERSION, version);
            xmiCasSerializer.serialize(aJCas.getCas(), sax2xml.getContentHandler(), null, null, null);

            if (!typeSystemWritten) {
                writeTypeSystem(aJCas);
                typeSystemWritten = true;
            }
            DocumentMetaData metaData = DocumentMetaData.get(aJCas);
            String name = Paths.get(metaData.getDocumentUri()).getFileName().toString();

            DUUIDocument document = new DUUIDocument(
                name,
                metaData.getDocumentUri(),
                byteArrayOutputStream.toByteArray());

            if (!document.getFileExtension().equals(filenameSuffix)) {
                document.setName(name.replace(document.getFileExtension(), filenameSuffix));
            }

            handler.writeDocument(document, getTargetLocation());

        } catch (Exception e) {
            throw new AnalysisEngineProcessException(e);
        }
    }

    private void writeTypeSystem(JCas aJCas)
        throws IOException, CASRuntimeException, SAXException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        OutputStream typeOS = null;

        try {
            if (typeSystemFile != null) {
                typeOS = CompressionUtils.getOutputStream(typeSystemFile);
            } else {
                typeOS = byteArrayOutputStream;
            }

            TypeSystemUtil.typeSystem2TypeSystemDescription(aJCas.getTypeSystem()).toXML(typeOS);
            DUUIDocument document = new DUUIDocument(
                "TypeSystem.xml",
                "",
                byteArrayOutputStream.toByteArray());

            handler.writeDocument(document, getTargetLocation());
        } finally {
            closeQuietly(typeOS);
        }
    }
}
