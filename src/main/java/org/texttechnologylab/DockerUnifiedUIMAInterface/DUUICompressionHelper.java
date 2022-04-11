package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.uima.util.TypeSystemUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 *
 * @author Alexander Leonhardt
 */
public class DUUICompressionHelper {
    CompressorStreamFactory _factory;
    String _method;

    /**
     *
     * @param method
     */
    public DUUICompressionHelper(String method) {
        _factory = new CompressorStreamFactory();
        _method = method;
    }

    /**
     *
     * @param input
     * @return
     * @throws IOException
     * @throws CompressorException
     */
    public String compress(String input) throws IOException, CompressorException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompressorOutputStream output = _factory.createCompressorOutputStream(_method,out);
        output.write(input.getBytes(StandardCharsets.UTF_8));
        output.close();
        return new String(Base64.getEncoder().encode(out.toByteArray()), StandardCharsets.UTF_8);
    }

    /**
     *
     * @param input
     * @return
     * @throws IOException
     * @throws CompressorException
     */
    public String decompress(String input) throws IOException, CompressorException {
        ByteArrayInputStream inputstream = new ByteArrayInputStream(Base64.getDecoder().decode(input.getBytes(StandardCharsets.UTF_8)));
        CompressorInputStream inpst = _factory.createCompressorInputStream(_method, inputstream);
        return new String(inpst.readAllBytes(), StandardCharsets.UTF_8);
    }

    /**
     *
     * @return
     */
    public String getCompressionMethod() {
        return _method;
    }
}
