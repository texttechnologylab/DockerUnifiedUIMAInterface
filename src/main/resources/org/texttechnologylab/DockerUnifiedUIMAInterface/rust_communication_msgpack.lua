token = luajava.bindClass("de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token")
util = luajava.bindClass("org.apache.uima.fit.util.JCasUtil")
msgpack = luajava.bindClass("org.msgpack.core.MessagePack")


-- Serialize all tokens as performance test
function serialize(inputCas,outputStream,params)
  packer = msgpack:newDefaultPacker(outputStream)
  packer:packString(inputCas:getDocumentText())

  local size = util:select(inputCas,token):size()
  packer:packArrayHeader(size*2)
  local result = util:select(inputCas,token):iterator()
  while result:hasNext() do
      local x = result:next()
	  packer:packInt(x:getBegin())
      packer:packInt(x:getEnd())
  end
  packer:close()
end

function deserialize(inputCas,inputStream)
  inputCas:reset()
  deserial:deserialize(inputStream,inputCas:getCas(),true)
end