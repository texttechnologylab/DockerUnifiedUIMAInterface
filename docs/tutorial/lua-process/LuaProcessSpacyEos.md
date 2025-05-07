# Lua `process` Example 1: End-of-Sentence Detection with spaCy

## Lua Script

```lua
---Indicates that this component supports the "new" `process` method.
SUPPORTS_PROCESS = true
---Indicates that this component does NOT support the old `serialize`/`deserialize` methods.
SUPPORTS_SERIALIZE = false

------------------------------------------------------

---Process the sentences in the given JCas in small batches.
---@param sourceCas any JCas (view) to process
---@param handler any RequestHandler with a connection to the component service
---@param parameters table optional parameters
---@param targetCas any JCas (view) to write the results to (optional)
function process(sourceCas, handler, parameters, targetCas)
    parameters = parameters or {}
    local language = parameters.language_override or sourceCas:getDocumentLanguage()
    if language == nil or language == "" or language == "x-unspecified" then
        language = "xx"
    end
    local config = {
        spacy_language = language
    }
    targetCas = targetCas or sourceCas

    ---Construct a request and send it to the component's /v1/process endpoint
    local response = handler:process(
        json.encode({
            text = sourceCas:getDocumentText(),
            config = config,
        })
    )

    ---Check if the response is valid, otherwise handle the error
    if not response:ok() then
        error("Error " .. response:statusCode() .. " in communication with component: " .. response:body())
    end

    local results = json.decode(response:bodyUtf8())

    ---Collect references to all created annotations in a table for later use in metadata
    ---@type table<integer, any>
    local references = {}

    ---Create a Sentence annotation for each detected sentence
    for _, sentence in ipairs(results.sentences) do
        local annotation = luajava.newInstance("de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence", targetCas)
        annotation:setBegin(sentence["begin"])
        annotation:setEnd(sentence["end"])
        annotation:addToIndexes()

        references[#references + 1] = annotation
    end

    ---After successful processing, create a SpacyAnnotatorMetaData annotation

    ---The metadata is provided by the component in the response
    ---@type table<string, any>
    local metadata = results.metadata

    local reference_array = luajava.newInstance("org.apache.uima.jcas.cas.FSArray", targetCas, #references)
    for i, ref in ipairs(references) do
        reference_array:set(i - 1, ref)
    end

    local annotation = luajava.newInstance("org.texttechnologylab.annotation.SpacyAnnotatorMetaData", targetCas)
    annotation:setReference(reference_array)
    annotation:setName(metadata.name)
    annotation:setVersion(metadata.version)
    annotation:setSpacyVersion(metadata.spacy_version)
    annotation:setModelName(metadata.model_name)
    annotation:setModelVersion(metadata.model_version)
    annotation:setModelLang(metadata.model_lang)
    annotation:setModelSpacyVersion(metadata.model_spacy_version)
    annotation:setModelSpacyGitVersion(metadata.model_spacy_git_version)
    annotation:addToIndexes()
end
```

## Component Code (Python)

The Python script `duui.py` wraps a spaCy `Language` pipeline in some `FastAPI` code to serve both static files (the Lua communication layer and the type system description) via GET routes as well as the `/v1/process` POST route.
Most of the code is no different from regular DUUI component code.
The last function (`async def v1_process()`) may serve as a good starting point for the interested reader.

```python
import logging
from typing import Final, Literal, Optional, get_args

import spacy
import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.datastructures import State
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from spacy import Language

LOGGING_CONFIG: Final[dict] = uvicorn.config.LOGGING_CONFIG
LOGGING_CONFIG["loggers"][""] = {
    "handlers": ["default"],
    "level": "INFO",
    "propagate": False,
}
logging.config.dictConfig(LOGGING_CONFIG)


##### Settings #####


SpacyLanguage = Literal[
    "ca",
    "zh",
    "hr",
    "da",
    "nl",
    "en",
    "fi",
    "fr",
    "de",
    "el",
    "it",
    "ja",
    "ko",
    "lt",
    "mk",
    "nb",
    "pl",
    "pt",
    "ro",
    "ru",
    "sl",
    "es",
    "sv",
    "uk",
    "xx",
    "x-unspecified",
]

SpacyModel = Literal[
    "ca_core_news_sm",
    "zh_core_web_sm",
    "hr_core_news_sm",
    "da_core_news_sm",
    "nl_core_news_sm",
    "en_core_web_sm",
    "fi_core_news_sm",
    "fr_core_news_sm",
    "de_core_news_sm",
    "el_core_news_sm",
    "it_core_news_sm",
    "ja_core_news_sm",
    "ko_core_news_sm",
    "lt_core_news_sm",
    "mk_core_news_sm",
    "nb_core_news_sm",
    "pl_core_news_sm",
    "pt_core_news_sm",
    "ro_core_news_sm",
    "ru_core_news_sm",
    "sl_core_news_sm",
    "es_core_news_sm",
    "sv_core_news_sm",
    "uk_core_news_sm",
    "xx_sent_ud_sm",
]

SPACY_MODEL_LOOKUP: Final[dict[SpacyLanguage, SpacyModel]] = {
    "ca": "ca_core_news_sm",  # Catalan
    "zh": "zh_core_web_sm",  # Chinese
    "hr": "hr_core_news_sm",  # Croatian
    "da": "da_core_news_sm",  # Danish
    "nl": "nl_core_news_sm",  # Dutch
    "en": "en_core_web_sm",  # English
    "fi": "fi_core_news_sm",  # Finnish
    "fr": "fr_core_news_sm",  # French
    "de": "de_core_news_sm",  # German
    "el": "el_core_news_sm",  # Greek
    "it": "it_core_news_sm",  # Italian
    "ja": "ja_core_news_sm",  # Japanese
    "ko": "ko_core_news_sm",  # Korean
    "lt": "lt_core_news_sm",  # Lithuanian
    "mk": "mk_core_news_sm",  # Macedonian
    "nb": "nb_core_news_sm",  # Norwegian Bokmal
    "pl": "pl_core_news_sm",  # Polish
    "pt": "pt_core_news_sm",  # Portugese
    "ro": "ro_core_news_sm",  # Romanian
    "ru": "ru_core_news_sm",  # Russian
    "sl": "sl_core_news_sm",  # Slovenian
    "es": "es_core_news_sm",  # Spanish
    "sv": "sv_core_news_sm",  # Swedish
    "uk": "uk_core_news_sm",  # Ukrainian
    "xx": "xx_sent_ud_sm",  # Multi-Language / Unknown Language
    # Unknown Language (UIMA; default value for a CAS when the language is not specified)
    "x-unspecified": "xx_sent_ud_sm",
}


class SpacySettings(BaseSettings):
    spacy_language: SpacyLanguage = "xx"


class AppSettings(SpacySettings):
    component_name: str = "duui-spacy-eos"
    component_version: str = "0.1.0"


SETTINGS: Final[SpacySettings] = AppSettings()


##### Initialization #####


def load_model(state: State, settings: SpacySettings):
    language: SpacyLanguage = settings.spacy_language
    if language not in SPACY_MODEL_LOOKUP:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid language '{language}'. Supported languages are: {', '.join(get_args(SpacyLanguage))}",
        )
    if not hasattr(state, "model") or state.model.lang != language:
        state.model = spacy.load(SPACY_MODEL_LOOKUP[language])
        logging.getLogger(__name__).info(
            f"Loaded Model: {app.state.model.lang} ({app.state.model.meta['name']})"
        )
    return state.model


app = FastAPI()
if not hasattr(app.state, "model"):
    load_model(app.state, SETTINGS)


##### DUUI V1 Communication Layer #####

with open("lua/communication_layer.lua", "r") as f:
    LUA_COMMUNICATION_LAYER: Final[str] = f.read()


@app.get("/v1/communication_layer", response_class=PlainTextResponse)
def get_communication_layer() -> str:
    return LUA_COMMUNICATION_LAYER


##### DUUI V1 Communication Layer #####

with open("resources/type_system.xml", "r") as f:
    TYPE_SYSTEM_XML: Final[str] = f.read()


@app.get("/v1/typesystem")
def get_typesystem() -> Response:
    return Response(content=TYPE_SYSTEM_XML, media_type="application/xml")


##### Models #####


class EosRequest(BaseModel):
    text: str
    config: Optional[SpacySettings] = None


class AnnotationMeta(BaseModel):
    name: str
    version: str
    spacy_version: str
    model_name: str
    model_pipes: list[str]
    model_version: str
    model_lang: str
    model_spacy_version: str
    model_spacy_git_version: str

    @classmethod
    def from_nlp(cls, nlp: Language):
        return cls(
            name=SETTINGS.component_name,
            version=SETTINGS.component_version,
            spacy_version=spacy.__version__,
            model_lang=nlp.lang,
            model_name=nlp.meta["name"],
            model_pipes=nlp.pipe_names,
            model_spacy_git_version=nlp.meta["spacy_git_version"],
            model_spacy_version=nlp.meta["spacy_version"],
            model_version=nlp.meta["version"],
        )


class AnnotationType(BaseModel):
    begin: int
    end: int


class DuuiResponse(BaseModel):
    metadata: AnnotationMeta
    sentences: list[AnnotationType]


##### DUUI V1 Process Endpoint #####


@app.post("/v1/process", description="DUUI API v1 process endpoint")
async def v1_process(params: EosRequest, request: Request) -> DuuiResponse:
    config: SpacySettings = params.config or SETTINGS

    nlp: Language = load_model(request.app.state, config)

    # Determine the appropriate sentence segmentation component
    # based on the loaded spaCy model
    if "senter" in nlp.pipe_names:
        eos_pipe = ["senter"]
    elif "parser" in nlp.pipe_names:
        eos_pipe = ["senter"]
        nlp.enable_pipe("senter")
    elif "sentencizer" in nlp.pipe_names:
        eos_pipe = ["sentencizer"]
    else:
        raise HTTPException(
            status_code=500,
            detail=f"spaCy model {nlp.meta['name']} does not have a sentence segmentation component",
        )

    # Enable only the sentence segmentation pipeline component
    with nlp.select_pipes(enable=eos_pipe):
        # (potentially) increase maximum input length
        nlp.max_length = len(params.text) + 1

        return DuuiResponse(
            metadata=AnnotationMeta.from_nlp(nlp),
            sentences=[
                AnnotationType(
                    begin=sent.start_char,
                    end=sent.end_char,
                )
                for sent in nlp(params.text).sents
            ],
        )
```

## Component Container (Dockerfile)

The Dockerfile encompasses "efficency" spaCy models for all available languages (for spaCy v3.8.5).

```Dockerfile
FROM nvidia/cuda:12.8.1-cudnn-runtime-ubi8 AS base

RUN dnf install -y python3.12 python3.12-pip && python3.12 -m pip install -U pip setuptools wheel

WORKDIR /app/
RUN python3.12 -m venv venv && ./venv/bin/python -m pip install -U 'spacy[cuda12x]' 'fastapi[standard]' uvicorn pydantic_settings spacy-lookups-data

RUN ./venv/bin/python -m spacy download ca_core_news_sm && \
    ./venv/bin/python -m spacy download zh_core_web_sm && \
    ./venv/bin/python -m spacy download hr_core_news_sm && \
    ./venv/bin/python -m spacy download da_core_news_sm && \
    ./venv/bin/python -m spacy download nl_core_news_sm && \
    ./venv/bin/python -m spacy download en_core_web_sm && \
    ./venv/bin/python -m spacy download fi_core_news_sm && \
    ./venv/bin/python -m spacy download fr_core_news_sm && \
    ./venv/bin/python -m spacy download de_core_news_sm && \
    ./venv/bin/python -m spacy download el_core_news_sm && \
    ./venv/bin/python -m spacy download it_core_news_sm && \
    ./venv/bin/python -m spacy download ja_core_news_sm && \
    ./venv/bin/python -m spacy download ko_core_news_sm && \
    ./venv/bin/python -m spacy download lt_core_news_sm && \
    ./venv/bin/python -m spacy download mk_core_news_sm && \
    ./venv/bin/python -m spacy download nb_core_news_sm && \
    ./venv/bin/python -m spacy download pl_core_news_sm && \
    ./venv/bin/python -m spacy download pt_core_news_sm && \
    ./venv/bin/python -m spacy download ro_core_news_sm && \
    ./venv/bin/python -m spacy download ru_core_news_sm && \
    ./venv/bin/python -m spacy download sl_core_news_sm && \
    ./venv/bin/python -m spacy download es_core_news_sm && \
    ./venv/bin/python -m spacy download sv_core_news_sm && \
    ./venv/bin/python -m spacy download uk_core_news_sm && \
    ./venv/bin/python -m spacy download xx_sent_ud_sm

COPY src/ /app/

ARG COMPONENT_NAME="duui-spacy-eos"
ENV COMPONENT_NAME=${COMPONENT_NAME}

ARG VERSION="0.1.0"
ENV COMPONENT_VERSION=${VERSION}

ARG SPACY_LANGUAGE="xx"
ENV SPACY_LANGUAGE=${SPACY_LANGUAGE}

ENTRYPOINT ["./venv/bin/uvicorn", "python.duui:app", "--host", "0.0.0.0", "--port" ,"9714", "--use-colors"]
CMD ["--workers", "1"]
```

## Type System Definition (XML)

The component creates `Sentence` annotations which are sourced from DKPro.
It creates also one `SpacyAnnotatorMetaData` annotation per call to `process`.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<typeSystemDescription xmlns="http://uima.apache.org/resourceSpecifier">
    <name>SpacySentenceDetectionTypes</name>
    <version>1.0</version>
    <types>
        <typeDescription>
            <name>de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence</name>
            <description />
            <supertypeName>uima.tcas.Annotation</supertypeName>
            <features>
                <featureDescription>
                    <name>id</name>
                    <description>
                        If this unit had an ID in the source format from which it was imported, it
                        may be stored here. IDs are typically not assigned by DKPro Core components.
                        If an ID is present, it should be respected by writers.
                    </description>
                    <rangeTypeName>uima.cas.String</rangeTypeName>
                </featureDescription>
            </features>
        </typeDescription>
        <typeDescription>
            <name>org.texttechnologylab.annotation.AnnotatorMetaData</name>
            <description />
            <supertypeName>uima.cas.AnnotationBase</supertypeName>
            <features>
                <featureDescription>
                    <name>reference</name>
                    <description />
                    <rangeTypeName>uima.cas.TOP</rangeTypeName>
                </featureDescription>
                <featureDescription>
                    <name>name</name>
                    <description />
                    <rangeTypeName>uima.cas.String</rangeTypeName>
                </featureDescription>
                <featureDescription>
                    <name>version</name>
                    <description />
                    <rangeTypeName>uima.cas.String</rangeTypeName>
                </featureDescription>
                <featureDescription>
                    <name>modelName</name>
                    <description />
                    <rangeTypeName>uima.cas.String</rangeTypeName>
                </featureDescription>
                <featureDescription>
                    <name>modelVersion</name>
                    <description />
                    <rangeTypeName>uima.cas.String</rangeTypeName>
                </featureDescription>
            </features>
        </typeDescription>
        <typeDescription>
            <name>org.texttechnologylab.annotation.SpacyAnnotatorMetaData</name>
            <description />
            <supertypeName>org.texttechnologylab.annotation.AnnotatorMetaData</supertypeName>
            <features>
                <featureDescription>
                    <name>spacyVersion</name>
                    <description />
                    <rangeTypeName>uima.cas.String</rangeTypeName>
                </featureDescription>
                <featureDescription>
                    <name>modelLang</name>
                    <description />
                    <rangeTypeName>uima.cas.String</rangeTypeName>
                </featureDescription>
                <featureDescription>
                    <name>modelSpacyVersion</name>
                    <description />
                    <rangeTypeName>uima.cas.String</rangeTypeName>
                </featureDescription>
                <featureDescription>
                    <name>modelSpacyGitVersion</name>
                    <description />
                    <rangeTypeName>uima.cas.String</rangeTypeName>
                </featureDescription>
            </features>
        </typeDescription>
    </types>
</typeSystemDescription>
```

## Brining It All Together

We organize our files as such:

```
.
├── Dockerfile
└── src
    ├── lua
    │   └── communication_layer.lua
    ├── python
    │   └── duui.py
    └── resources
        └── type_system.xml
```

and run the docker build script:

```bash
export VERSION=0.1.0
docker build \
    -f Dockerfile \
    --tag docker.texttechnologylab.org/duui-spacy-eos:$VERSION \
    --build-arg VERSION=$VERSION \
    .
```

# Author

[Manuel Schaaf](https://www.texttechnologylab.org/team/manuel-schaaf/)

If you have any questions or need more information, feel free to reach out to the author.
