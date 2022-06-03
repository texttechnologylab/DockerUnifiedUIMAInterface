-- Bind static classes from java
StandardCharsets = luajava.bindClass("java.nio.charset.StandardCharsets")
Token = luajava.bindClass("de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token")
Sentences = luajava.bindClass("de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
util = luajava.bindClass("org.apache.uima.fit.util.JCasUtil")
msgpack = luajava.bindClass("org.msgpack.core.MessagePack")

-- This "serialize" function is called to transform the CAS object into an stream that is sent to the annotator
-- Inputs:
--  - inputCas: The actual CAS object to serialize
--  - outputStream: Stream that is sent to the annotator, can be e.g. a string, JSON payload, ...
function serialize(inputCas, outputStream)
    -- Get data from CAS
    -- For spaCy, we need the documents text and its language
    -- TODO add additional params?
    local doc_text = inputCas:getDocumentText()
    local doc_lang = inputCas:getDocumentLanguage()
    -- Encode data as JSON object and write to stream
    -- TODO Note: The JSON library is automatically included and available in all Lua scripts
    local sentences_cas = {}
    local sents = util:select(inputCas, Sentences):iterator()
    while sents:hasNext() do
        local sent = sents:next()
        local tokens = util:selectCovered(Token, sent):iterator()
        local begin_sen = sent:getBegin()
        local end_sen = sent:getEnd()
        sentences_cas[tostring(begin_sen) .. ","..tostring(end_sen)] = {}
        while tokens:hasNext() do
             local token = tokens:next()
             local token_begin = token:getBegin()
             local token_end = token:getEnd()
             sentences_cas[tostring(begin_sen) .. ","..tostring(end_sen)][tostring(token_begin) .. ","..tostring(token_end)] = token:getText()
         end
    end
    outputStream:write(json.encode({
        text = doc_text,
        lang = doc_lang,
        sen = sentences_cas
    }))
end

-- This "deserialize" function is called on receiving the results from the annotator that have to be transformed into a CAS object
-- Inputs:
--  - inputCas: The actual CAS object to deserialize into
--  - inputStream: Stream that is received from to the annotator, can be e.g. a string, JSON payload, ...
function deserialize(inputCas, inputStream)
    -- Get string from stream, assume UTF-8 encoding
    print(inputStream:readAllBytes())
end