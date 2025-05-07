# Lua `process` Example 3: DUUI-API-less Components

As the `process` interface does not mandate the use of any specific endpoint on the component, we may use it to build components that do not rely on the DUUI API, for example if the underlying tool already provides a HTTP server with its own API.
In any case, our component needs to provide the Lua communication layer at the regular DUUI API endpoint.
The example below demonstrates this by example of [`gnfinder`](https://github.com/gnames/gnfinder).

## Lua Script

The Lua script deviates from our previous pattern, as we now call a specific endpoint on the component server that is not related to DUUI.

### Helper Functions

Inititally, we define some variables and helper function for the creation of `Taxon` annotations later on.

```lua
---Indicates that this component supports the "new" `process` method.
SUPPORTS_PROCESS = true
---Indicates that this component does NOT support the old `serialize`/`deserialize` methods.
SUPPORTS_SERIALIZE = false

------------------------------------------------------

local FSArray = "org.apache.uima.jcas.cas.FSArray"
local DocumentModification = "org.texttechnologylab.annotation.DocumentModification"
local Taxon = "org.texttechnologylab.annotation.biofid.gnfinder.Taxon"
local VerifiedTaxon = "org.texttechnologylab.annotation.biofid.gnfinder.VerifiedTaxon"
local OddsDetails = "org.texttechnologylab.annotation.biofid.gnfinder.OddsDetails"
local MetaData = "org.texttechnologylab.annotation.biofid.gnfinder.MetaData"
local MetaDataKeyValue = "org.texttechnologylab.annotation.biofid.gnfinder.MetaDataKeyValue"

------------------------------------------------------

---Set fields common to Taxon and VerifiedTaxon
---@param targetCas any the target JCas
---@param taxon any the Taxon or VerifiedTaxon object
---@param name table<string, any> the name object from the GNFinder response
local function set_common_taxon_fields(targetCas, taxon, name)
    taxon:setBegin(name.start)
    taxon:setEnd(name.start + string.len(name.verbatim))
    taxon:setValue(name.name)
    taxon:setCardinality(name.cardinality)
    taxon:setOddsLog10(name.oddsLog10)
    if name.oddsDetails ~= nil then
        local odds_details = luajava.newInstance(FSArray, targetCas, #name.oddsDetails)
        local details = nil
        for i, detail in ipairs(name.oddsDetails) do
            details = luajava.newInstance(OddsDetails, targetCas)
            details:setFeature(detail.feature)
            details:setOdds(detail.value)
            details:addToIndexes()
            odds_details:set(i - 1, details)
        end
        odds_details:addToIndexes()
        taxon:setOddsDetails(odds_details)
    end
end

---Handle the response from GNFinder and create the corresponding annotations
---@param targetCas any the target JCas
---@param gnfinder_names table<string, any> the names found by GNFinder
---@param gnfinder_metadata table<string, string> the metadata from the GNFinder response
local function handle_response(targetCas, gnfinder_names, gnfinder_metadata)
    local references = {}
    for _, name in ipairs(gnfinder_names) do
        if name.verification == nil then
            local taxon = luajava.newInstance(Taxon, targetCas)

            handle_common_taxon_fields(targetCas, taxon, name)

            taxon:addToIndexes()
            references[#references + 1] = taxon
        else
            local verified_names = {}
            if name.verification.bestResult ~= nil then
                verified_names[1] = name.verification.bestResult
            elseif name.verification.results ~= nil then
                verified_names = name.verification.results
            else
                ---unreachable
                error("Invalid response format: response must contain either 'bestResult' or 'results'!")
            end

            for _, verif in ipairs(verified_names) do
                local taxon = luajava.newInstance(VerifiedTaxon, targetCas)

                handle_common_taxon_fields(targetCas, taxon, name)

                taxon:setDataSourceId(verif.dataSourceId)
                taxon:setRecordId(verif.recordId)

                if verif.globalId ~= nil then
                    taxon:setGlobalId(verif.globalId)
                end
                if verif.localId ~= nil then
                    taxon:setLocalId(verif.localId)
                end
                if verif.outlink ~= nil then
                    taxon:setOutlink(verif.outlink)
                    taxon:setIdentifier(verif.outlink)
                else
                    taxon:setIdentifier(verif.recordId)
                end
                taxon:setSortScore(verif.sortScore)
                taxon:setMatchedName(verif.matchedName)
                taxon:setCurrentName(verif.currentName)
                taxon:setMatchedCanonicalSimple(verif.matchedCanonicalSimple)
                taxon:setMatchedCanonicalFull(verif.matchedCanonicalFull)
                taxon:setTaxonomicStatus(verif.taxonomicStatus)
                taxon:setMatchType(verif.matchType)
                taxon:setEditDistance(verif.editDistance)

                taxon:addToIndexes()
                references[#references + 1] = taxon
            end
        end
    end

    ---GNFinder metadata handling
    local metadata = luajava.newInstance(MetaData, targetCas)

    ---@type table<string, any> GNFinder metadata
    metadata:setDate(gnfinder_metadata.date)
    metadata:setVersion(gnfinder_metadata.gnfinderVersion)
    metadata:setLanguage(gnfinder_metadata.language)

    ---Add references to all created taxon annotations

    local taxon_references = luajava.newInstance(FSArray, targetCas, #references)
    for i, ref in ipairs(references) do
        taxon_references:set(i - 1, ref)
    end
    taxon_references:addToIndexes()
    metadata:setReferences(taxon_references)

    ---Add metadata fields for any other settings (starting with "with"), like "withNoBayes"

    local other = {}
    for key, value in pairs(gnfinder_metadata) do
        if string.sub(key, 1, 4) == "with" then
            other[#other + 1] = { key, value }
        end
    end

    if #other > 0 then
        local fs_array = luajava.newInstance(FSArray, targetCas, #other)
        for i, kv in ipairs(other) do
            local key, value = table.unpack(kv)
            local fs = luajava.newInstance(MetaDataKeyValue, targetCas)
            fs:setKey(key)
            fs:setValue(value)
            fs:addToIndexes()
            fs_array:set(i - 1, fs)
        end
        fs_array:addToIndexes()
        metadata:setOther(fs_array)
    end

    metadata:addToIndexes()

    -- Add modification annotation
    local document_modification = luajava.newInstance(DocumentModification, targetCas)
    document_modification:setUser("duui-lite-gnfinder")
    document_modification:setTimestamp(gnfinder_metadata.date)
    document_modification:setComment(
        "GNFinder " .. gnfinder_metadata.gnfinderVersion ..
        ", language = " .. gnfinder_metadata.language
    )
    document_modification:addToIndexes()
end
```

### Process Function

Then we define the `process` function, where we set parameters for the query and infer the document language (GNFinder only supports German and English).
The resulting query is sent to GNFinder using `handler:post("/api/v1/find", ...)` which requires the `Content-Type` header to be set appropriately.
After that, we process the results using the functions defined above.

```lua
---Process the document text of the given source JCas
---@param sourceCas any JCas (view) to process
---@param handler any DuuiHttpRequestHandler with a connection to the running component
---@param parameters table<string, string> optional parameters
---@param targetCas any JCas (view) to write the results to (optional)
function process(sourceCas, handler, parameters, targetCas)
    targetCas = targetCas or sourceCas

    ---@type table <string, any> POST query for the GNFinder HTTP API
    local query = {
        text = sourceCas:getDocumentText(),
        language = parameters.language or "detect",
        ambiguousNames = parameters.ambiguousNames == "true",
        noBayes = parameters.noBayes == "true",
        oddsDetails = parameters.oddsDetails == "true",
        verification = parameters.verification ~= "false",
        allMatches = parameters.allMatches == "true",
        ---format is fixed to "compact" (JSON)
        format = "compact",
        ---sources are fixed to 11 (= GBIF) for this example
        sources = {11},
    }

    local lang = sourceCas:getDocumentLanguage()
    if lang ~= nil then
        if lang:lower():sub(2) == "de" then
            query["language"] = "deu"
        elseif lang:lower():sub(2) then
            query["language"] = "eng"
        end
    end

    handler:setHeader("Content-Type", "application/json")
    local response = handler:post("/api/v1/find", json.encode(query))

    if not response:ok() then
        error("Error " .. response:statusCode() .. " in communication with component: " .. response:bodyUtf8())
    end

    ---@type table<string, table>
    local results = json.decode(response:bodyUtf8())
    handle_response(targetCas, results.names, results.metadata)
end
```

## Python Code

To be able to make calls to the GNFinder HTTP server while also serving the Lua script from the same component, we have to wrap the GNFinder server in a tiny "reverse proxy" server, that is able to respond to GET requests to the `/v1/communication_layer` and `/v1/typesystem` endpoints, but forwards other requests to the GNFinder server.

### GNFinder Server in Sub-Process

To this end, we start the GNFinder server in a `asyncio` sub-process using FastAPI the [`lifespan` event hook](https://fastapi.tiangolo.com/advanced/events/#lifespan).

```python
import asyncio
import logging
import os
import shutil
import traceback
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Final

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.logger import logger
from fastapi.responses import JSONResponse, PlainTextResponse
from starlette.background import BackgroundTask

LOGGING_CONFIG: Final[dict] = uvicorn.config.LOGGING_CONFIG
LOGGING_CONFIG["loggers"][""] = {
    "handlers": ["default"],
    "level": "INFO",
    "propagate": False,
}
logging.config.dictConfig(LOGGING_CONFIG)


GNFINDER_PATH: Final[Path] = Path(
    os.environ.get("GNFINDER_PATH", shutil.which("gnfinder"))
)
STARTUP_DELAY: Final[int] = int(os.environ.get("STARTUP_DELAY", "2"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not hasattr(app.state, "gnfinder_process"):
        logger.info("[startup] Starting GNFinder process")
        app.state.gnfinder_process = await asyncio.create_subprocess_exec(
            GNFINDER_PATH, "-p", "8999"
        )

    try:
        # Wait for the GNFinder server to start
        # If the server exits during this time, raise a RuntimeError
        async with asyncio.timeout(STARTUP_DELAY):
            if exit_code := await app.state.gnfinder_process.wait():
                raise RuntimeError(
                    f"GNFinder server exited unexpectedly with code {exit_code}"
                )
    except TimeoutError:
        pass

    async with httpx.AsyncClient(base_url="http://localhost:8999/") as gnfinder_client:
        try:
            # Check if the GNFinder server is running
            (await gnfinder_client.get("/api/v1/ping")).raise_for_status()
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=httpx.codes.SERVICE_UNAVAILABLE,
                detail="Could not connect to GNFinder server",
            ) from e

        yield {
            "gnfinder_client": gnfinder_client,
        }

    try:
        if hasattr(app.state, "gnfinder_process"):
            logger.info("[shutdown] Terminating GNFinder process")
            app.state.gnfinder_process.terminate()

            # Wait for the process to terminate
            async with asyncio.timeout(5):
                await app.state.gnfinder_process.communicate()
    except TimeoutError as e:
        raise RuntimeError(
            "GNFinder process did not terminate in a timely manner"
        ) from e
    except ProcessLookupError:
        # expected during shutdown, process already terminated by context manager, but required for reloading
        pass
    finally:
        del app.state.gnfinder_process


app = FastAPI(lifespan=lifespan)
```

### Static Files

As before, we serve the Lua script from a static file.

```python
with open("type_system.xml", "r") as f:
    TYPE_SYSTEM_XML: Final[str] = f.read()


@app.get("/v1/typesystem")
def get_type_system():
    return Response(TYPE_SYSTEM_XML, media_type="application/xml")


with open("communication_layer.lua", "r") as f:
    LUA_COMMUNICATION_LAYER: Final[str] = f.read()


@app.get("/v1/communication_layer")
def get_communication_layer():
    return Response(LUA_COMMUNICATION_LAYER, media_type="text/x-lua")
```

### Reverse Proxy

The GNFinder server defines several endpoints which we would like to expose from our reverse proxy server.
The function below handles all requests to any endpoint, as long as it starts with `/api/v1/`.
Taking the raw FastAPI `Request` we, extract the request URL path and query and wrap them in `httpx` classes.
While we could support streaming requests directly from the `httpx.AsyncClient` without buffering them in our reverse proxy function, DUUI does not (yet) fully support streamed responses.
Instead, we just `await` the response and fashion a FastAPI `Response` from the returned data, forwarding the response body's content, and the responses status code and `content-type` header.

```python
@app.get("/api/v1/{path:path}")
@app.post("/api/v1/{path:path}")
async def forward_api_request(request: Request):
    client: httpx.AsyncClient = request.state.gnfinder_client
    try:
        response: httpx.Response = await client.send(
            client.build_request(
                request.method,
                url=httpx.URL(
                    path=request.url.path, query=request.url.query.encode("utf-8")
                ),
                headers=[(k, v) for k, v in request.headers.raw if k != b"host"],
                content=request.stream(),
            )
        )
        response_headers: dict[str, str] = {k.lower(): v for k, v in response.headers.items()}
        content_type_header = (
            {"content-type": content_type}
            if (content_type := response_headers.get("content-type")) is not None
            else {}
        )
        return Response(
            content=response.content,
            headers=content_type_header,
            status_code=response.status_code,
            background=BackgroundTask(response.aclose),
        )
    except httpx.HTTPStatusError as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=e.response.status_code,
            detail=traceback.format_exc(),
        ) from e
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=traceback.format_exc(),
        ) from e
```

## Type System Definition (XML)

The type system associated with the GNFinder is quite large, so we did not include it here.
You can find it in the [TTLab Type System repository](https://github.com/texttechnologylab/UIMATypeSystem/blob/uima-3/src/main/resources/desc/type/BIOfidTypeSystem.xml)

# Author

[Manuel Schaaf](https://www.texttechnologylab.org/team/manuel-schaaf/)

If you have any questions or need more information, feel free to reach out to the author.
