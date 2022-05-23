token = luajava.bindClass("de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token")
util = luajava.bindClass("org.apache.uima.fit.util.JCasUtil")


-- Serialize all tokens as performance test
function serialize(inputCas : JCAS,outputStream : OutputStream,params : Map<String,String>)
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

-- Server liest Dokumente (1,2,3)
-- Server startet annotatoren (WebsocketAnnotator - Service)
-- Server holt lua script von annotatoren (WebsocketAnnotator => LUA Script)

-- Server benutzt LUA script serialize(1) -> OutputStream wird geschickt an WebsocketAnnotator
-- WebsocketAnnotator annotiert
-- WebsocketAnnotator schickt zurueck
-- Server benutzt LUA script deserialize({WebsocketAnnotator output})

-- for (Server ein teil des Dokumentes schickt partition(2)):
    -- Server benutzt LUA script serialize_websocket(2) -> OutputStream wird geschickt an WebsocketAnnotator
    -- WebsocketAnnotator annotiert
    -- WebsocketAnnotator schickt zurueck
    -- Server benutzt LUA script deserialize_websocket({WebsocketAnnotator output})

-- Annotator besitzt dieses LUA script
-- Server fragt annotator nach lua script -> Server
-- Server schaut ob websocket unterstuetzt ist, verwende Websocket

-- Server verarbeitet CAS dokument
-- Server serialize(cas document) -> Annotator
-- Annotator macht Arbeit
-- Annotator schickt Antwort -> Server deserialize(antwort vom annotator)
function serialize_websocket(inputCas, outputStream, progress)
    if progress==1
        return 1
    end

    -- Serialize 1/4
    return progress+0.25
end

function is_finished(inputCas, progress)
    return true
end

function deserialize_websocket(inputCas, outputStream, token)
end