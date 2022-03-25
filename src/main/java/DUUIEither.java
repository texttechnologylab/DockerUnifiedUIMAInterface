import org.apache.commons.compress.archivers.ar.ArArchiveInputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.TypeSystemUtil;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

//Taken from https://stackoverflow.com/questions/26162407/is-there-an-equivalent-of-scalas-either-in-java-8
public class DUUIEither {
    private String _string_buffer;
    private JCas _jcas_buffer;
    private String _typesystem_buffer;

    private boolean _current_selected_jcas;
    private int _transform_steps;
    private DUUICompressionHelper _compression;

    public DUUIEither(JCas jc, String compression) throws IOException, SAXException, CompressorException {
        _jcas_buffer = jc;
        _string_buffer = "";
        _current_selected_jcas = true;
        _transform_steps = 0;
        _compression = new DUUICompressionHelper(compression);
        StringWriter writer = new StringWriter();
        TypeSystemUtil.typeSystem2TypeSystemDescription(_jcas_buffer.getTypeSystem()).toXML(writer);
        _typesystem_buffer = new String(Base64.getEncoder().encode(_compression.compress(writer.getBuffer().toString()).getBytes(StandardCharsets.UTF_8)));
    }

    public String getCompressionMethod() {
        return _compression.getCompressionMethod();
    }

    public String getTypesystem() {
        return _typesystem_buffer;
    }

    public String getAsString() throws SAXException, CompressorException, IOException {
        if (_current_selected_jcas) {
            ByteArrayOutputStream arr = new ByteArrayOutputStream();
            XmiCasSerializer.serialize(_jcas_buffer.getCas(), null, arr);
            _transform_steps++;
            _string_buffer = arr.toString();
            _string_buffer = new String(Base64.getEncoder().encode(_compression.compress(_string_buffer).getBytes(StandardCharsets.UTF_8)));
        }
        return _string_buffer;
    }

    public void updateJCas(JCas jc) {
        _jcas_buffer = jc;
        _current_selected_jcas = true;
    }

    public void updateStringBuffer(String buffer) {
        _string_buffer = buffer;
        _current_selected_jcas = false;
    }

    public int getTransformSteps() {
        return _transform_steps;
    }

    public JCas getAsJCas() throws SAXException, IOException, CompressorException {
        if (!_current_selected_jcas) {
            _jcas_buffer.reset();
            String deserialized = _compression.decompress(new String(Base64.getDecoder().decode(_string_buffer)));
            XmiCasDeserializer.deserialize(new ByteArrayInputStream(deserialized.getBytes(StandardCharsets.UTF_8)), _jcas_buffer.getCas(), true);
            _transform_steps++;
        }
        return _jcas_buffer;
    }
}
