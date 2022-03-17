package org.texttechnology.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.archivers.ar.ArArchiveInputStream;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.jcas.JCas;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

//Taken from https://stackoverflow.com/questions/26162407/is-there-an-equivalent-of-scalas-either-in-java-8
public class DUUIEither {
    private String _string_buffer;
    private JCas _jcas_buffer;
    private boolean _current_selected_jcas;
    private int _transform_steps;

    public DUUIEither(JCas jc) {
        _jcas_buffer = jc;
        _string_buffer = "";
        _current_selected_jcas = true;
        _transform_steps = 0;
    }

    public String getAsString() throws SAXException {
        if (_current_selected_jcas) {
            ByteArrayOutputStream arr = new ByteArrayOutputStream();
            XmiCasSerializer.serialize(_jcas_buffer.getCas(), null, arr);
            _transform_steps++;
            _string_buffer = arr.toString();
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

    public JCas getAsJCas() throws SAXException, IOException {
        if (!_current_selected_jcas) {
            _jcas_buffer.reset();
            XmiCasDeserializer.deserialize(new ByteArrayInputStream(_string_buffer.getBytes(StandardCharsets.UTF_8)), _jcas_buffer.getCas(), true);
            _transform_steps++;
        }
        return _jcas_buffer;
    }
}
