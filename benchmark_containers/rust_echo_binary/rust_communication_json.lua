token = luajava.bindClass("de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token")
util = luajava.bindClass("org.apache.uima.fit.util.JCasUtil")
arr = luajava.bindClass("org.json.JSONArray")
obj = luajava.bindClass("org.json.JSONObject")
StandardCharsets = luajava.bindClass("java.nio.charset.StandardCharsets")


-- Serialize all tokens as performance test
function serialize(inputCas,outputStream,params)
  local doc = {}
  doc["text"] = inputCas:getDocumentText()
  local result = util:select(inputCas,token):iterator()
  local tokens = {}
  while result:hasNext() do
      local x = result:next()
	  table.insert(tokens,x:getBegin())
      table.insert(tokens,x:getEnd())
  end
  doc["tokens"] = tokens
  outputStream:write(json.encode(doc))
end

function deserialize(inputCas,inputStream)
  inputCas:reset()
  local stringser = luajava.newInstance("java.lang.String",inputStream:readAllBytes(),StandardCharsets.UTF_8);
  local ok = json.decode(stringser)
 -- local doc = luajava.newInstance("org.json.JSONObject",stringser);
  local text = ok["text"];

  inputCas:setDocumentText(text)
  inputCas:setDocumentLanguage("de")
  local tokens = ok["tokens"];
  local numTokens = #(tokens)
  while numTokens > 0 do
    local begin = tokens[numTokens-1]
    local tend = token[numTokens-2]

    local token = luajava.newInstance("de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token",inputCas)
    token:setBegin(begin)
    token:setEnd(tend)
    token:addToIndexes()
    numTokens = numTokens-2
  end
end