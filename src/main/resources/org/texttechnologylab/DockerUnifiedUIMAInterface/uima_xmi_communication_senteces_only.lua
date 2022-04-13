serial = luajava.bindClass("org.apache.uima.cas.impl.XmiCasSerializer")
deserial = luajava.bindClass("org.apache.uima.cas.impl.XmiCasDeserializer")
sentence = luajava.bindClass("de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
util = luajava.bindClass("org.apache.uima.fit.util.JCasUtil")

function serialize(inputCas,outputStream)
    local result = util:select(inputCas,sentence):iterator()
    while result:hasNext() do
        local x = result:next()
	    outputStream:write(x:getCoveredText())
	    outputStream:write("\n")
    end
end

function deserialize(inputCas,inputStream)
   print('Deserialize skipping, nothing to do...')
end
