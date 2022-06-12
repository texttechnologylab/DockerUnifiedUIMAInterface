serial = luajava.bindClass("org.apache.uima.util.CasIOUtils")
deserial = luajava.bindClass("org.apache.uima.cas.impl.XmiCasDeserializer")
SerialFormat = luajava.bindClass("org.apache.uima.cas.SerialFormat")
function serialize(inputCas,outputStream,params)
  serial:save(inputCas:getCas(),outputStream,SerialFormat.XMI_1_1)
end
function deserialize(inputCas,inputStream)
    inputCas:reset()
    deserial:deserialize(inputStream,inputCas:getCas(),true)
end