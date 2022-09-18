StandardCharsets = luajava.bindClass("java.nio.charset.StandardCharsets")

serial = luajava.bindClass("org.apache.uima.cas.impl.XmiCasSerializer")
deserial = luajava.bindClass("org.apache.uima.cas.impl.XmiCasDeserializer")

token = luajava.bindClass("org.texttechnologylab.annotation.DocumentModification")
util = luajava.bindClass("org.apache.uima.fit.util.JCasUtil")

function serialize(inputCas,outputStream,params)

  serial:serialize(inputCas:getCas(), outputStream)
  --print(luajava.newInstance("java.lang.String", outputStream:toByteArray(), StandardCharsets.UTF_8))
  local result = util:select(inputCas,token):iterator()
  while result:hasNext() do
      local x = result:next()
      print(x)

	  --outputStream:write("DocumentModification: ")
	  --outputStream:write(x:getText())
	  --outputStream:write("\n")
  end
end

function deserialize(inputCas,inputStream)
    -- Get string from stream, assume UTF-8 encoding
    local inputString = luajava.newInstance("java.lang.String", inputStream:readAllBytes(), StandardCharsets.UTF_8)
    --print(inputString)
    local mod = luajava.newInstance("org.texttechnologylab.annotation.DocumentModification", inputCas);
    mod:setUser(inputString)
    mod:setComment("annotation")
    mod:addToIndexes()
end