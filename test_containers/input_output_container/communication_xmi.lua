serial = luajava.bindClass("org.apache.uima.cas.impl.XmiCasSerializer")
deserial = luajava.bindClass("org.apache.uima.cas.impl.XmiCasDeserializer")

function serialize(inputCas,outputStream,params)
  serial:serialize(inputCas:getCas(),outputStream)
end

function deserialize(inputCas,inputStream)
  -- Get string from stream, assume UTF-8 encoding
  local inputString = luajava.newInstance("java.lang.String", inputStream:readAllBytes(), StandardCharsets.UTF_8)
  -- Parse JSON data from string into object
  local results = json.decode(inputString)
end