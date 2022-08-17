serial = luajava.bindClass("org.apache.uima.cas.impl.XmiCasSerializer")
deserial = luajava.bindClass("org.apache.uima.cas.impl.XmiCasDeserializer")

function serialize(inputCas,outputStream,params)
  print(params)
  print(type(params))
  print(params.fuchs)
  serial:serialize(inputCas:getCas(),outputStream)
end

function deserialize(inputCas,inputStream)
  inputCas:reset()
  deserial:deserialize(inputStream,inputCas:getCas(),true)
end