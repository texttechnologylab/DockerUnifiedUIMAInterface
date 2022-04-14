token = luajava.bindClass("de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token")
util = luajava.bindClass("org.apache.uima.fit.util.JCasUtil")


-- Serialize all tokens as performance test
function serialize(inputCas,outputStream)
  local beginsent = luajava.newInstance("org.json.JSONArray")
  local endsent = luajava.newInstance("org.json.JSONArray")
  local sending = luajava.newInstance("org.json.JSONArray")

  local result = util:select(inputCas,token):iterator()
  while result:hasNext() do
    local x = result:next()
	beginsent:put(x:getBegin())
    endsent:put(x:getEnd())
  end

  sending:put(inputCas:getDocumentText())
  sending:put(beginsent)

  sending:put(endsent)
  -- Differentiate function call to prevent best effort lookup
  local ok = sending:toString()
  outputStream:write(ok)
end

function deserialize(inputCas,inputStream)
  inputCas:reset()
  deserial:deserialize(inputStream,inputCas:getCas(),true)
end