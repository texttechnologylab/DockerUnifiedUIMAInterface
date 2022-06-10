casio = luajava.bindClass("org.apache.uima.util.CasIOUtils")
SerialFormat = luajava.bindClass("org.apache.uima.cas.SerialFormat")

function serialize(inputCas,outputStream,params)
  casio:save(inputCas:getCas(),outputStream,SerialFormat.BINARY);
end

function deserialize(inputCas,inputStream)
  inputCas:reset()
  casio:load(inputStream,inputCas:getCas())
end