StandardCharsets = luajava.bindClass("java.nio.charset.StandardCharsets")

function serialize(inputCas, outputStream, params)
    local text = inputCas:getDocumentText()
    if text == nil then
        text = ""
    end

    outputStream:write(json.encode({
        text = text,
    }))

    DUUILogging:logInfo("serialized")
end

-- Called by the driver after receiving the HTTP response.
-- inputCas   : target view JCas
-- inputStream : response body bytes
function deserialize(inputCas, inputStream)
    local inputString = luajava.newInstance(
        "java.lang.String",
        inputStream:readAllBytes(),
        StandardCharsets.UTF_8
    )

    local results = json.decode(inputString)

    -- Write the counts map as a JSON string into the target SOFA.
    -- Callers can read inputCas:getSofaDataString() to obtain the result.
    if results["counts"] ~= nil then
        inputCas:setSofaDataString(
            json.encode(results["counts"]),
            "application/json"
        )
    end

    DUUILogging:logInfo("deserialized")
end
