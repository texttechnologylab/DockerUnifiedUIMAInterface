token = luajava.bindClass("de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token")
util = luajava.bindClass("org.apache.uima.fit.util.JCasUtil")


-- Serialize all tokens as performance test
function serialize(inputCas,outputStream)
  beginsent = luajava.newInstance("org.json.JSONArray")
  endsent = luajava.newInstance("org.json.JSONArray")
  send = luajava.newInstance("org.json.JSONArray")
  
  local result = util:select(inputCas,token):iterator()
  while result:hasNext() do
    local x = result:next()
	  beginsent:put(x:getBegin())
      endsent:put(x:getEnd())
  end
  send:put(inputCas:getDocumentText())
  send:put(beginsent)
  send:put(endsent)
  outputStream:write(send:toString())
end

function deserialize(inputCas,inputStream)
  inputCas:reset()
  deserial:deserialize(inputStream,inputCas:getCas(),true)
end