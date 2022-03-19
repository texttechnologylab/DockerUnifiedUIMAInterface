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

public class DUUICompressionHelper {
    CompressorStreamFactory _factory;
    String _method;

    public DUUICompressionHelper(String method) {
        _factory = new CompressorStreamFactory();
        _method = method;
    }

    public String compress(String input) throws IOException, CompressorException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompressorOutputStream output = _factory.createCompressorOutputStream(CompressorStreamFactory.ZSTANDARD,out);
        output.write(input.getBytes(StandardCharsets.UTF_8));
        output.close();
        return new String(Base64.getEncoder().encode(out.toByteArray()), StandardCharsets.UTF_8);
    }

    public String decompress(String input, String method) throws IOException, CompressorException {
        ByteArrayInputStream inputstream = new ByteArrayInputStream(Base64.getDecoder().decode(input.getBytes(StandardCharsets.UTF_8)));
        CompressorInputStream inpst = _factory.createCompressorInputStream(method, inputstream);
        return new String(inpst.readAllBytes(), StandardCharsets.UTF_8);
    }

    public String getCompressionMethod() {
        return _method;
    }
}
