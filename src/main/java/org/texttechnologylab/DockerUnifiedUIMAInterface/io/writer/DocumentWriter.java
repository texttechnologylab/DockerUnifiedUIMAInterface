package org.texttechnologylab.DockerUnifiedUIMAInterface.io.writer;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASRuntimeException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.TypeSystemUtil;
import org.apache.uima.util.XMLSerializer;
import org.dkpro.core.api.io.JCasFileWriter_ImplBase;
import org.dkpro.core.api.parameter.ComponentParameters;
import org.dkpro.core.api.resources.CompressionUtils;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.IDUUIDocumentHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.annotation.SharedData;
import org.xml.sax.SAXException;

import javax.xml.transform.OutputKeys;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import static java.util.Arrays.asList;
import static org.apache.commons.io.IOUtils.closeQuietly;

/**
 * TTLabXmiWriter to write files to XMI.
 *
 * @author Giuseppe Abrami
 */
public class DocumentWriter extends JCasFileWriter_ImplBase {

    public static final String PARAM_FILENAME_EXTENSION = "filenameExtension";
    @ConfigurationParameter(
        name = "filenameExtension",
        mandatory = true,
        defaultValue = {".xmi"},
        description = "Specify the suffix of output files. Default value <code>.xmi</code>. If the suffix is not\nneeded, provide an empty string as value."
    )
    private String filenameSuffix;

    private PrintStream outputStream;

    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        super.initialize(context);

        outputStream = System.out;
    }

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {
        // Perform your analysis and write the output to the OutputStream
        String analysisResult = "Your analysis result here";
        outputStream.println(analysisResult);
    }
}
