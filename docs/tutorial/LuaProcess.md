---
layout: default
---

# Implementing the Lua `process` Interface

Components that support the `process` interface advertise this to DUUI with the global variable `SUPPORTS_PROCESS = true`.
Developers then need only implement a single method:

```lua
SUPPORTS_PROCESS = true

---Process the sentences in the given JCas in small batches.
---@param sourceCas any JCas (view) to process
---@param handler any RequestHandler with a connection to the running component
---@param parameters table optional parameters
---@param targetCas any JCas (view) to write the results to (optional)
function process(sourceCas, handler, parameters, targetCas)
    -- ...
end
```

The `handler` is an instance of a `RequestHandler` which has the following public interface:

```java
public interface IRequestHandler {
    public Response get();
    public Response get(String endpoint);
    public Response get(String endpoint, String... parameters);
    public Response post(String, byte[] data);

    public void header(String name, String value);
    public void headers(String... headers);
    public void setHeader(String name, String value);
    public IHttpRequestHandler withHeaders(String... headers)

    public Response process(byte[] data);
    public Response getDocumentation();

    public record Response(int statusCode, byte[] body) {
        public boolean ok();
        public byte[] body();
        public String bodyAsUtf8();
        public String bodyAsString(Charset charset);
        public byte[] bodyAsBase64();
        public MessageUnpacker bodyAsMsgPack();
        public ByteArrayInputStream bodyAsByteArrayInputStream();
    }
}
```

Server and port of the RequestHandler are pre-defined by DUUI during instantiiation of the pipeline component.
If you specify an `endpoint` for `post(String, byte[])`, `get(String)`, or `get(String, String...)` it must start with a preceding slash, i.e. `/v1/process` for the DUUI process endpoint.
The variable length arguments for the GET query parameters in `get(String, String...)` and HTTP headers for `headers(String...)` are expected to be *strings in key-value-pairs*, i.e. `headers("Content-Type", "application/json")`.

The return value of each request method is an instance of the inner `Response` record class which forwards the HTTP response status code and body from the Java HTTP request response object.
As with the request body, the `process` interface deals with raw byte arrays `byte[]`.
However, thanks to automatic type coersion and `Response` convenience methods, you usually do not need to worry about converting Lua structures to or from byte arrays; i.e. the Lua string returned by `json.encode(...)` is coerced into a byte array by the LuaJ runtime.

# Examples

In the following, we will go through three examples that make use of the Lua `process` interface:

1. [End-of-Sentence Detection with spaCy](LuaProcessSpacyEos)
2. [Batched Processing](LuaProcessSpacyBatched)
3. [DUUI-API-less Components](LuaProcessGNFinderLite)

# Author

[Manuel Schaaf](https://www.texttechnologylab.org/team/manuel-schaaf/)

If you have any questions or need more information, feel free to reach out to the author.
