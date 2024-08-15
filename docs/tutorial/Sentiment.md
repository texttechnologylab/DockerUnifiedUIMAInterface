---
layout: default
---

# Integration of a Sentiment Tool

In this section, we will explain how to integrate a sentiment analysis tool into DUUI step by step.
The Integration in this example is developed using the  Python programming language, because the underlying sentiment tool is also implemented in Python.
We use the Hugging Face implementation of the model [cardiffnlp/twitter-xlm-roberta-base-sentiment](https://huggingface.co/cardiffnlp/twitter-xlm-roberta-base-sentiment) (see [Barbieri et al. 2022](https://aclanthology.org/2022.lrec-1.27/)) as the tool, which allows us to predict a sentiment polarity label (i.e. Positive, Neutral, or Negative) and a confidence value for text in 8 languages.
The full example code can be found in the [GitHub repository](https://github.com/texttechnologylab/duui-uima/tree/main/duui-transformers-sentiment-example).

## Typeystem for Sentiment Analysis
The first step when integrating a tool into DUUI is to define the typesystem required, i.e. what types (e.g. SentimentCategory) are needed to process the input and store the output of the tool. If the needed types are not already defined in the typesystem repository ([UIMATypeSystem](https://github.com/texttechnologylab/UIMATypeSystem)) by the Text Technology Lab, they need to be created and added.
The typesystem for the sentiment analysis tool is shown in the following XML snippet, where a `Sentiment` type (full name `org.hucompute.textimager.uima.type.Sentiment`) is defined with two features: `sentiment` (a floating number, but by definition we use a value of `-1`, `0`, or `+1`, depending on the polarity) and `subjectivity`. Note that by using the super class `uima.tcas.Annotation`, the `Sentiment` type can be used to annotate text spans in the CAS document by providing a `begin` end `end` index.
```xml
<?xml version="1.0" encoding="UTF-8"?>
<typeSystemDescription xmlns="http://uima.apache.org/resourceSpecifier">
    <types>
        <typeDescription>
            <name>org.hucompute.textimager.uima.type.Sentiment</name>
            <description/>
            <supertypeName>uima.tcas.Annotation</supertypeName>
            <features>
                <featureDescription>
                    <name>sentiment</name>
                    <description/>
                    <rangeTypeName>uima.cas.Double</rangeTypeName>
                </featureDescription>
                <featureDescription>
                    <name>subjectivity</name>
                    <description/>
                    <rangeTypeName>uima.cas.Double</rangeTypeName>
                </featureDescription>
            </features>
        </typeDescription>
    </types>
</typeSystemDescription>
```

## Sentiment Integration in DUUI with Python
After defining the typesystem, we can integrate the sentiment analysis tool into DUUI.
Install the needed required packages (as listed in the `requirements.txt` file) with the following command, after creating a virtual environment (e.g. via virtualenv or (Mini)Conda) using Python 3.8 in IntelliJ (recommended).<br>
```pip install -r requirements.txt```<br>
The following code shows how to integrate the sentiment tool into DUUI and can be found in the file `duui-transformers-sentiment-example/src/main/python/duui_transformers_sentiment.py` in the repository. Please refer to the comments for further details.
```python
import logging
from typing import Optional

from cassis import load_typesystem
from fastapi import FastAPI, Response
from fastapi.responses import PlainTextResponse
from pydantic import BaseSettings, BaseModel
from transformers import pipeline


class Settings(BaseSettings):
    """
    Tool settings, this is used to configure the tool using environment variables given to Docker
    """

    # Name of annotator
    annotator_name: str

    # Version of annotator
    annotator_version: str

    # Log level
    log_level: Optional[str]

    class Config:
        """
        Extra settings configuration
        """

        # Prefix for environment variables, note that env vars have to be provided fully uppercase
        env_prefix = 'ttlab_duui_transformers_sentiment_example_'


class DUUIRequest(BaseModel):
    """
    This is the request sent by DUUI to this tool, i.e. the input data. This is beeing created by the Lua transformation and is thus specific to the tool.
    """

    # The full text to be analyzed
    text: str

    # The language of the text
    lang: str

    # The length of the document
    doc_len: int


class DUUIResponse(BaseModel):
    """
    This is the response of this tool back to DUUI, i.e. the output data. This is beeing transformed back to UIMA/CAS by Lua and is thus specific to the tool.
    """

    # The sentiment label, i.e. -1, 0 or 1 showing the polarity of the sentiment
    sentiment_label: int

    # The sentiment score, i.e. the confidence of the sentiment
    sentiment_score: float


# Initialize settings, this will pull the settings from the environment
settings = Settings()

# Set up logging using the log level provided in the settings
logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)
logger.info("TTLab DUUI Transformers Sentiment Example")
logger.info("Name: %s", settings.annotator_name)
logger.info("Version: %s", settings.annotator_version)

# Load the type system
typesystem_filename = 'src/main/resources/TypeSystemSentiment.xml'
logger.info("Loading typesystem from \"%s\"", typesystem_filename)
with open(typesystem_filename, 'rb') as f:
    typesystem = load_typesystem(f)
    logger.debug("Base typesystem:")
    logger.debug(typesystem.to_xml())

# Load the Lua communication layer
lua_communication_script_filename = "src/main/lua/duui_transformers_sentiment.lua"
logger.info("Loading Lua communication script from \"%s\"", lua_communication_script_filename)
with open(lua_communication_script_filename, 'rb') as f:
    lua_communication_script = f.read().decode("utf-8")
    logger.debug("Lua communication script:")
    logger.debug(lua_communication_script)

# Load the model
model = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-xlm-roberta-base-sentiment",
    tokenizer="cardiffnlp/twitter-xlm-roberta-base-sentiment",
    revision="f3e34b6c30bf27b6649f72eca85d0bbe79df1e55",
    top_k=3
)

# Start the FastAPI app and provide some meta information, these are accessible via the /docs endpoint
app = FastAPI(
    title=settings.annotator_name,
    description="Transformers-based sentiment analysis for TTLab DUUI",
    version=settings.annotator_version,
    terms_of_service="https://www.texttechnologylab.org/legal_notice/",
    contact={
        "name": "TTLab Team",
        "url": "https://texttechnologylab.org",
        "email": "baumartz@em.uni-frankfurt.de",
    },
    license_info={
        "name": "AGPL",
        "url": "http://www.gnu.org/licenses/agpl-3.0.en.html",
    },
)


@app.get("/v1/communication_layer", response_class=PlainTextResponse)
def get_communication_layer() -> str:
    """
    This is a DUUI API endpoint that needs to be present in every tool.
    :return: The Lua communication script
    """
    return lua_communication_script


@app.get("/v1/typesystem")
def get_typesystem() -> Response:
    """
    This is a DUUI API endpoint that needs to be present in every tool.
    :return: The typesystem as XML, this should include all types the tool can produce
    """
    xml = typesystem.to_xml()
    xml_content = xml.encode("utf-8")

    return Response(
        content=xml_content,
        media_type="application/xml"
    )


@app.post("/v1/process")
def post_process(request: DUUIRequest) -> DUUIResponse:
    """
    This is a DUUI API endpoint that needs to be present in every tool. This is the main processing endpoint, that will be called for each document. It receives the data produced by the Lua transformation script and returns the processed data, that will then be transformed back by Lua. Note that the data handling is specific for each tool.
    :param request: The request object containing the data transformed by Lua.
    :return: The processed data.
    """
    sentiment_label = None
    sentiment_score = None

    try:
        logger.debug("Received:")
        logger.debug(request)

        # Run the sentiment analysis
        result = model(
            request.text,
            truncation=True,
            padding=True,
            max_length=512
        )
        logger.debug(result)

        # get the top sentiment label, i.e. "Positive"
        sentiment_score = result[0][0]["score"]

        # map the label to the sentiment type, i.e. to -1, 0 or 1
        label = result[0][0]["label"]
        sentiment_mapping = {
            "Positive": 1,
            "Neutral": 0,
            "Negative": -1
        }
        sentiment_label = sentiment_mapping[label]

    except Exception as ex:
        logger.exception(ex)

    # Return the response back to DUUI where it will be transformed using Lua
    return DUUIResponse(
        sentiment_label=sentiment_label,
        sentiment_score=sentiment_score
    )
```

## Lua
The Lua script is used to transform the CAS document, which contains all document data (text and annotations), to a format that is supported by the tool. This means, it first extracts the needed information from the CAS object and generates a JSON-based request that is sent to the tool by DUUI. Secondly, it transforms the response of the tool, which is a JSON object, back to the CAS object. The Lua script can be found in the file `duui-transformers-sentiment-example/src/main/lua/duui_transformers_sentiment.lua` in the repository. Please refer to the comments for further details.
```lua
StandardCharsets = luajava.bindClass("java.nio.charset.StandardCharsets")
Class = luajava.bindClass("java.lang.Class")
JCasUtil = luajava.bindClass("org.apache.uima.fit.util.JCasUtil")
SentimentUtils = luajava.bindClass("org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaUtils")

function serialize(inputCas, outputStream, parameters)
    -- Get document text, language and size from CAS
    local doc_lang = inputCas:getDocumentLanguage()
    local doc_text = inputCas:getDocumentText()
    local doc_len = SentimentUtils:getDocumentTextLength(inputCas)

    -- Encode as JSON and write to the output stream, this is then sent to the tool
    outputStream:write(json.encode({
        text = doc_text,
        lang = doc_lang,
        doc_len = doc_len,
    }))
end

function deserialize(inputCas, inputStream)
    -- Read the JSON from the input stream and decode it
    local inputString = luajava.newInstance("java.lang.String", inputStream:readAllBytes(), StandardCharsets.UTF_8)
    local results = json.decode(inputString)

    -- Check if the JSON contains the expected keys...
    if results["sentiment_label"] ~= nil and results["sentiment_score"] ~= nil then
        -- Create the sentiment metadata
        local sentiment_anno = luajava.newInstance("org.hucompute.textimager.uima.type.Sentiment", inputCas)
        sentiment_anno:setBegin(0)
        sentiment_anno:setEnd(SentimentUtils:getDocumentTextLength(inputCas))
        sentiment_anno:setSentiment(sentiment_label)
        sentiment_anno:addToIndexes()
    end
end
```

## Docker

The Dockerfile for this example can be found in `duui-transformers-sentiment-example/src/main/docker/Dockerfile` in the repository. It is used to build a Docker image that can be used to run the tool in a containerized environment. Special care should be taken that this image is as static as possible to enable reproducibility, i.e. it should contain all needed code and data (in this case, the actual model file is preloaded pinned to a specific version and Hugging Face is setup to not check online for model information or updates).
```docker
FROM python:3.8

WORKDIR /usr/src/app

# This port is specified by the DUUI API
EXPOSE 9714

# Install dependencies
RUN pip install setuptools wheel
COPY ./requirements.txt ./requirements.txt
RUN pip install --no-deps -r requirements.txt

# Preload Models during Docker build
RUN python3 -c 'from transformers import pipeline; pipeline("sentiment-analysis", model="cardiffnlp/twitter-xlm-roberta-base-sentiment", tokenizer="cardiffnlp/twitter-xlm-roberta-base-sentiment", revision="f3e34b6c30bf27b6649f72eca85d0bbe79df1e55")'

# Log level
ARG TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_LOG_LEVEL="DEBUG"
ENV TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_LOG_LEVEL=$TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_LOG_LEVEL

# Meta data
ARG TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_ANNOTATOR_NAME="duui-transformers-sentiment-example"
ENV TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_ANNOTATOR_NAME=$TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_ANNOTATOR_NAME
ARG TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_ANNOTATOR_VERSION="dev"
ENV TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_ANNOTATOR_VERSION=$TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_ANNOTATOR_VERSION

# Enable offline mode for Hugging Face
ARG TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_TRANSFORMERS_OFFLINE=1
ENV TRANSFORMERS_OFFLINE=$TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_TRANSFORMERS_OFFLINE

# Copy all needed scripts
COPY ./src/main/resources/TypeSystemSentiment.xml ./src/main/resources/TypeSystemSentiment.xml
COPY ./src/main/python/duui_transformers_sentiment.py ./src/main/python/duui_transformers_sentiment.py
COPY ./src/main/lua/duui_transformers_sentiment.lua ./src/main/lua/duui_transformers_sentiment.lua

# Start the Python server
ENTRYPOINT ["uvicorn", "src.main.python.duui_transformers_sentiment:app", "--host", "0.0.0.0", "--port" ,"9714"]
CMD ["--workers", "1"]
```

The Docker image is built using the following command:
```bash
#!/usr/bin/env bash
set -euo pipefail

# Name and version of the tool
# These are defined here to keep the versions in Python and Docker synchronized
export TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_ANNOTATOR_NAME=duui-transformers-sentiment-example
export TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_ANNOTATOR_VERSION=1.0.0

# docker registry to use
export TTLAB_DUUI_DOCKER_REGISTRY="docker.texttechnologylab.org/"

# Build the Docker image
docker build \
  --build-arg TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_ANNOTATOR_NAME \
  --build-arg TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_ANNOTATOR_VERSION \
  -t ${TTLAB_DUUI_DOCKER_REGISTRY}${TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_ANNOTATOR_NAME}:${TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_ANNOTATOR_VERSION} \
  -f "src/main/docker/Dockerfile" \
  .

# Automatically tag the newest image as "latest"
docker tag \
  ${TTLAB_DUUI_DOCKER_REGISTRY}${TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_ANNOTATOR_NAME}:${TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_ANNOTATOR_VERSION} \
  ${TTLAB_DUUI_DOCKER_REGISTRY}${TTLAB_DUUI_TRANSFORMERS_SENTIMENT_EXAMPLE_ANNOTATOR_NAME}:latest
```
This automatically builds the Docker image and generates a tag for the latest version. The image can then be pushed to the Text Technology Lab Docker registry.

Finally, see the Java JUnit tests for the sentiment tool in the file `duui-transformers-sentiment-example/src/test/java/org/hucompute/textimager/uima/transformers/sentiment/CardiffnlpTwitterXlmRobertaBaseSentimentTest.java` in the repository. The tests are used to check the functionality of the tool and to ensure that it works as expected.

# Author

[Daniel Baumartz](https://www.texttechnologylab.org/team/daniel-baumartz/)

If you have any questions or need more information, feel free to reach out to the author.
