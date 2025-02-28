package org.texttechnologylab.DockerUnifiedUIMAInterface.io.writer;

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
import org.xml.sax.SAXException;

import javax.xml.transform.OutputKeys;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import static java.util.Arrays.asList;
import static org.apache.commons.io.IOUtils.closeQuietly;

/**
 * TTLabXmiWriter to write files to XMI.
 *
 * @author Giuseppe Abrami
 */
public class TTLabXmiWriter extends JCasFileWriter_ImplBase {

    @Override
    public void process(JCas aJCas) throws AnalysisEngineProcessException {

        try (OutputStream docOS = getOutputStream(aJCas, filenameSuffix)) {
            XmiCasSerializer xmiCasSerializer = new XmiCasSerializer(aJCas.getTypeSystem());
            XMLSerializer sax2xml = new XMLSerializer(docOS, prettyPrint);
            sax2xml.setOutputProperty(OutputKeys.VERSION, version);

//            XmiSerializationSharedData sharedObject = null;
//            sharedObject = AsyncCollectionReader.deserialize(aJCas);
//
//            if(sharedObject!=null){
//                CAS tCas = aJCas.getCas();
//                JCasUtil.select(tCas.getJCas(), SharedData.class).forEach(sd->{
//                    tCas.removeFsFromIndexes(sd);
//                });
//                xmiCasSerializer.serialize(aJCas.getCas(), sax2xml.getContentHandler(), null, sharedObject,
//                        null);
//            }
//            else{

                xmiCasSerializer.serialize(aJCas.getCas(), sax2xml.getContentHandler(), null, null,
                        null);
//            }


            if (!typeSystemWritten) {
                writeTypeSystem(aJCas);
                typeSystemWritten = true;
            }
        }
        catch (Exception e) {
            throw new AnalysisEngineProcessException(e);
        }

    }

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


    private boolean typeSystemWritten;

    @Override
    public void initialize(UimaContext aContext)
            throws ResourceInitializationException
    {
        super.initialize(aContext);

        if (!asList("1.0", "1.1").contains(version)) {
            throw new ResourceInitializationException(new IllegalArgumentException(
                    "Invalid value for parameter version: [" + version + "]"));
        }

        typeSystemWritten = false;
    }

    private void writeTypeSystem(JCas aJCas)
            throws IOException, CASRuntimeException, SAXException
    {
        @SuppressWarnings("resource")
        OutputStream typeOS = null;

        try {
            if (typeSystemFile != null) {
                typeOS = CompressionUtils.getOutputStream(typeSystemFile);
            }
            else {
                typeOS = getOutputStream("TypeSystem", ".xml");
            }

            TypeSystemUtil.typeSystem2TypeSystemDescription(aJCas.getTypeSystem()).toXML(typeOS);
        }
        finally {
            closeQuietly(typeOS);
        }
    }
}
