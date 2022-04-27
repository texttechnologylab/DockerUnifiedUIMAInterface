serial = luajava.bindClass("org.apache.uima.util.CasIOUtils")
serialtype = luajava.bindClass("org.apache.uima.cas.SerialFormat");

function serialize(inputCas,outputStream,params)
  serial:save(inputCas:getCas(),outputStream,serialtype.BINARY_TSI)
end

function deserialize(inputCas,inputStream)
  inputCas:reset()
  serial:load(inputStream,inputCas:getCas())
end