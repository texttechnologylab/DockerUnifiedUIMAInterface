serial = luajava.bindClass("org.apache.uima.cas.impl.XmiCasSerializer")
deserial = luajava.bindClass("org.apache.uima.cas.impl.XmiCasDeserializer")
token = luajava.bindClass("de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token")
util = luajava.bindClass("org.apache.uima.fit.util.JCasUtil")

function serialize(inputCas,outputStream)
  for token in util:select(inputCas,token.class) do
    print(token)
    outputStream:write(token:getBegin():toString())
    outputStream:write(token:getBegin():toString())
  end
end

function deserialize(inputCas,inputStream)
  print('Deserialize skipping, nothing to do...')
end