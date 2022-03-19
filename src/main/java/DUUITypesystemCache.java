import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.TypeSystemUtil;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class DUUITypesystemCache {
    private TypeSystem _last_typesystem;
    private String _compressed_typesystem;
    private int _compressed_typesystem_hash;

    public DUUITypesystemCache() {
        _last_typesystem = null;
        _compressed_typesystem = null;
    }

    public void update(TypeSystem jcas_typesystem, DUUICompressionHelper helper) throws CompressorException, IOException, SAXException {
        if(_last_typesystem != jcas_typesystem) {
            StringWriter writer = new StringWriter();
            TypeSystemUtil.typeSystem2TypeSystemDescription(jcas_typesystem).toXML(writer);

            _compressed_typesystem = helper.compress(writer.getBuffer().toString());
            _compressed_typesystem_hash = _compressed_typesystem.hashCode();
        }
    }

    public String getCompressedTypesystem() {
        return _compressed_typesystem;
    }

    public int getCompressedTypesystemHash() {
        return _compressed_typesystem_hash;
    }
}
