function serialize(inputCas,outputStream,params)
  json.encode({1,2,3,4})
end

function deserialize(inputCas,inputStream)
  inputCas:reset()
  deserial:deserialize(inputStream,inputCas:getCas(),true)
end