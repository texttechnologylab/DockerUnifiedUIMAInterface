token = luajava.bindClass("de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token")
util = luajava.bindClass("org.apache.uima.fit.util.JCasUtil")
msgpack = luajava.bindClass("org.msgpack.core.MessagePack")


-- Serialize all tokens as performance test
function serialize(inputCas,outputStream,params)
  local foo = {}
  table.insert(foo,inputCas:getDocumentText())
  local size = util:select(inputCas,token):size()

  local result = util:select(inputCas,token):iterator()
  while result:hasNext() do
      local x = result:next()
      table.insert(foo,x:getBegin())
      table.insert(foo,x:getEnd())
  end
  outputStream:write(nativem.pack(foo))
end

function deserialize(inputCas,inputStream)
  inputCas:reset()
  deserial:deserialize(inputStream,inputCas:getCas(),true)
end