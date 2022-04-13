sentence = luajava.bindClass("de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
util = luajava.bindClass("org.apache.uima.fit.util.JCasUtil")
msgpack = luajava.bindClass("org.msgpack.core.MessagePack")


-- Serialize all tokens as performance test
function serialize(inputCas,outputStream)
  packer = msgpack:newDefaultBufferPacker()
  packer:packString(inputCas:getDocumentText())

  local size = util:select(inputCas,sentence):size()
  packer:packArrayHeader(size*2)
  local result = util:select(inputCas,sentence):iterator()
  while result:hasNext() do
      local x = result:next()
	    packer:packInt(x:getBegin())
      packer:packInt(x:getEnd())
  end
  packer:close()
  outputStream:write(packer:toByteArray())
end

function deserialize(inputCas,inputStream)
  inputCas:reset()
  deserial:deserialize(inputStream,inputCas:getCas(),true)
end