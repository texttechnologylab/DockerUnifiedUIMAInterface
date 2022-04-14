serial = luajava.bindClass("org.apache.uima.cas.impl.XmiCasSerializer")
deserial = luajava.bindClass("org.apache.uima.cas.impl.XmiCasDeserializer")

function serialize(inputCas,outputStream,params)
  outputStream:write(json.encode({1,2,3,4}))
  print('works right?')
  print('seems right?')
end

function deserialize(inputCas,inputStream)
  inputCas:reset()
  deserial:deserialize(inputStream,inputCas:getCas(),true)
end