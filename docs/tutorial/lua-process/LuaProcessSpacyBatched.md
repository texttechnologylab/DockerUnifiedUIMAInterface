# Lua `process` Example 2: Conditionals & Batched Processing

End-of-Sentence Detection (usually) requires the whole body of text to work properly.
It is a very simple task that spaCy implements in a very efficient way,
so we usually will not run into any issues &ndash; even for very large documents.
Other tasks, however, require much more computational power and memory, so running them on large documents
usually requires chunking the documents beforehand using DUUI pipeline components (on the user-end)
and subsequently merging the annotated documents again.

With the Lua `process` interface, we can implement _batched processing_ which alleviates the need for manual
chunking and thus user intervention and enabling component compatibility with any document size.

The following example will outline the changes needed to expand the previous EOS component
to cover the entire spaCy pipeline. For a full implementation, refer to [`duui-uima/duui-spacy-lua-process`](https://github.com/texttechnologylab/duui-uima/tree/main/duui-spacy-lua-process)

## Conditional Component Stage

The `serialize`/`deserialize` interface does not allow component designs with conditionals that change the behavior of the component.
With the process interface, we can make different calls to the component depending on conditions that are present at runtime.
For example: the spaCy pipeline operates on sentences so `Sentence` annotations are usually required or need to be created on the fly.
While we could implement a conditional like this in the component backend code, separating routes for EOS detection and
other tasks simplifies the process.

### Conditional EOS Detection

Let's define a new route on our component backend that takes over the current functionality of the
`/v1/process` route:

```python
@app.post("/api/eos", description="End-of-Sentence Detection")
async def api_eos(params: EosRequest, request: Request) -> DuuiResponse:
    # snip: code from above
```

Then, we define the new `/v1/process` route that handles the entire spaCy pipeline.
After annotating the sentences, we can now utilize `nlp.pipe` to efficiently process the sentences:

```python
class Sentence(BaseModel):
    text: str
    begin: int

class SpaCyRequest(BaseModel):
    sentences: list[Sentence]
    config: Optional[SpacySettings] = None

@app.post("/v1/process", description="DUUI API v1 process endpoint")
async def v1_process(
    params: SpaCyRequest,
    request: Request,
):
    config: SpacySettings = params.config or SETTINGS

    nlp: Language = load_model(request.app.state, config)

    to_disable = set(["senter", "sentencizer"]).intersection(nlp.pipe_names)
    with nlp.select_pipes(disable=to_disable):
        results = []

        texts = [sentence.text for sentence in params.sentences]
        for doc, sent in zip(nlp.pipe(texts), params.sentences):
            # snip: handle annotations (tokens, POS, lemma, dependencies, named entities, ...)

        return results
```

Then, we implement the conditional in our Lua script:

```lua
--snip
function process(sourceCas, handler, parameters, targetCas)
    --snip
    local sentences = JCasUtil:select(sourceCas, Sentence)

    if sentences:isEmpty() then
        local response = handler:post("/api/eos", json.encode({
            text = sourceCas:getDocumentText(),
            config = config,
        }))
        if not response:ok() then
            error("Error " .. response:statusCode() .. " in communication with component /api/eos: " .. response:body())
        end

        local results = json.decode(response:bodyUtf8())
        for _, sentence in ipairs(results.sentences) do
            local sentence_anno = luajava.newInstance("de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence",
                targetCas)
            sentence_anno:setBegin(sentence["begin"])
            sentence_anno:setEnd(sentence["end"])
            sentence_anno:addToIndexes()
        end

        sentences = JCasUtil:select(targetCas, Sentence)
        if sentences:isEmpty() then
            error("No sentences found in the source or target JCas.")
        end
    end
    --snip
end
```

## Batched Processing

Above, we used `nlp.pipe` to efficiently process the sentences.
However, for extremely large documents, we may want to further _batch the requests send to the component itself_, i.e. to prevent memory issues.

We can implement a batched iterator directly in Lua with a [`coroutine`](https://www.lua.org/pil/9.1.html):
```lua
---Create and yield batches of sentences from a sentence iterator.
---@param iterator any an iterator over sentences
---@param batch_size integer size of each batch sent to the component
function get_sentence_batches(iterator, batch_size)
    local batch = {}
    while iterator:hasNext() do
        local sentence = iterator:next()
        batch[#batch + 1] = {
            text = sentence:getCoveredText(),
            begin = sentence:getBegin(),
        }
        if #batch == batch_size then
            coroutine.yield(batch)
            batch = {}
        end
    end

    if #batch > 0 then
        coroutine.yield(batch)
    end
end

---Iterate over batches of sentences.
---@param iterator any an iterator over sentences
---@param batch_size integer size of each batch
---@return fun(): table an iterator over batches to process
function batched_sentences(iterator, batch_size)
    local co = coroutine.create(function() get_sentence_batches(iterator, batch_size) end)
    return function()
        local _, batch = coroutine.resume(co)
        return batch
    end
end
```

The resulting batches are then sent one-by-one to the component from our `process` function:
```lua
--snip: previous definitions

function process(sourceCas, handler, parameters, targetCas)
    --snip: conditional EOS detection & sentence selection with JCasUtil.select()

    local batch_size = parameters.batch_size or 1024
    for batch in batched_sentences(sentences:iterator(), batch_size) do
        if type(batch) ~= "table" then
            error("Error while batching: " .. batch)
        end

        local response = handler:process(
            json.encode({
                sentences = batch,
                config = config,
            })
        )

        if not response:ok() then
            error("Error " .. response:statusCode() .. " in communication with component: " .. response:bodyUtf8())
        end

        local results = json.decode(response:bodyUtf8())

        --snip: create annotations in the target CAS
    end
end
```

The `process` interface would also allow us to make "meta" requests to the component to query the optimal batch size, etc.
