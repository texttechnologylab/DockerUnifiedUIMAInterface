serial = luajava.bindClass("org.apache.uima.cas.impl.XmiCasSerializer")
deserial = luajava.bindClass("org.apache.uima.cas.impl.XmiCasDeserializer")
token = luajava.bindClass("de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token")
util = luajava.bindClass("org.apache.uima.fit.util.JCasUtil")

function serialize(inputCas,outputStream,params)
    local result = util:select(inputCas,token):iterator()
    while result:hasNext() do
        local x = result:next()
	    outputStream:write("Token: ")
	    outputStream:write(x:getText())
	    outputStream:write("\n")
    end
end

function deserialize(inputCas,inputStream)
  print(inputStream:readAllBytes())
end