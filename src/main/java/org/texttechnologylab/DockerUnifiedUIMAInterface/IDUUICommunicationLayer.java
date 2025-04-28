package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIHttpRequestHandler;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Interface for communication between the DUUI composer {@link DUUIComposer} and the components {@link org.texttechnologylab.DockerUnifiedUIMAInterface.driver.IDUUIDriverInterface}.
 */
public interface IDUUICommunicationLayer {

  public void serialize(JCas jc, ByteArrayOutputStream out, Map<String,String> parameters, String sourceView) throws CompressorException, IOException, SAXException, CASException;

  /**
   * Serializes a JCas to a byte array output stream by using the LUA script provided by the component.
   * @param jc Input JCas.
   * @param out Output stream, i.e. the input to the component.
   * @param parameters Parameters for use in the LUA script.
   * @throws CompressorException
   * @throws IOException
   * @throws SAXException
   */
  default void serialize(JCas jc, ByteArrayOutputStream out, Map<String,String> parameters) throws CompressorException, IOException, SAXException, CASException {
      serialize(jc, out, parameters, "_InitialView");
  }

  public void deserialize(JCas jc, ByteArrayInputStream input, String targetView) throws IOException, SAXException, CASException;

  /**
   * Deserializes a byte array input stream to a JCas by using the LUA script provided by the component.
   * @param jc Output JCas, note that the CAS is not reset before deserialization.
   * @param input Input stream, i.e. the output of the component.
   * @throws IOException
   * @throws SAXException
   */
  default void deserialize(JCas jc, ByteArrayInputStream input) throws IOException, SAXException, CASException {
    deserialize(jc, input, "_InitialView");
  }
  default void process(JCas jCas, DUUIHttpRequestHandler handler, Map<String, String> parameters) throws CompressorException, IOException, SAXException, CASException {
    process(jCas, handler, parameters, jCas);
  }

  public void process(JCas jCas, DUUIHttpRequestHandler handler, Map<String, String> parameters, JCas targetCas) throws CompressorException, IOException, SAXException, CASException;

  public boolean supportsProcess();

  public boolean supportsSerialize();

  /**
   *
   * @return
   */
  public IDUUICommunicationLayer copy();

  /**
   *
   * @param results
   * @return
   */
  public ByteArrayInputStream merge(List<ByteArrayInputStream> results);

  String myLuaTestMerging();

}
