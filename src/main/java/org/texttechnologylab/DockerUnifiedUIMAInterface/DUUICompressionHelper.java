package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Helper class for compression and decompression of strings.
 */
public class DUUICompressionHelper {
    CompressorStreamFactory _factory;
    String _method;

    /**
     * Constructor using a {@link CompressorStreamFactory} and a compression method.
     * @param method
     */
    public DUUICompressionHelper(String method) {
        _factory = new CompressorStreamFactory();
        _method = method;
    }

    /**
     * Compresses a string.
     * @param input The string to compress.
     * @return The compressed string as a base64 encoded string.
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
     * Decompresses a string.
     * @param input The compressed string as a base64 encoded string.
     * @return The decompressed string.
     * @throws IOException
     * @throws CompressorException
     */
    public String decompress(String input) throws IOException, CompressorException {
        ByteArrayInputStream inputstream = new ByteArrayInputStream(Base64.getDecoder().decode(input.getBytes(StandardCharsets.UTF_8)));
        CompressorInputStream inpst = _factory.createCompressorInputStream(_method, inputstream);
        return new String(inpst.readAllBytes(), StandardCharsets.UTF_8);
    }

    public String getCompressionMethod() {
        return _method;
    }
}
