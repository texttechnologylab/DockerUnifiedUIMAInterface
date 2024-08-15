---
layout: default
---
# Integration of HateCheck
The Integration for this example is with python, because the HateCheck tool is implemented in python.
In this section, we will explain how to integrate a HateCheck tool into the DUUI pipeline step by step.
The HateCheck module is a part of the DUUI pipeline that is responsible for calculating the hate speech score of a given text.

The full example code can be found in the [GitHub repository](https://github.com/texttechnologylab/duui-uima/tree/main/duui-Hate).

## Typeystem for HateCheck
The first step is to define the typesystem for the HatCheck tool, if the needed types are not already defined in the typesystem repository ([UIMATypeSystem](https://github.com/texttechnologylab/UIMATypeSystem)).
The typesystem for the HateCheck tool is defined in the following XML format:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<typeSystemDescription xmlns="http://uima.apache.org/resourceSpecifier">
    <name>TypeSystemFactChecking</name>
    <description/>
    <version>1.0</version>
    <vendor/>
    <!-- Import the needed types -->
    <imports>
        <import name="desc.type.TextTechnologyAnnotation"/>
        <import name="desc.type.TypeSystemModelMeta"/>
    </imports>
    <types>
        <typeDescription>
            <!-- Define the type for HateCheck -->
            <name>org.texttechnologylab.annotation.Hate</name>
            <description>
                Hate Output
            </description>
            <!-- The supertype of HateCheck, get every function of Annotation -->
            <supertypeName>uima.tcas.Annotation</supertypeName>
            <features>
                <featureDescription>
                    <!-- Probability of Hate -->
                    <name>Hate</name>
                    <description>Probability of Hate</description>
                    <rangeTypeName>uima.cas.Double</rangeTypeName>
                </featureDescription>
                <featureDescription>
                    <!-- Probability of not Hate -->
                    <name>NonHate</name>
                    <description>Probability of not Hate</description>
                    <rangeTypeName>uima.cas.Double</rangeTypeName>
                </featureDescription>
                <featureDescription>
                    <!-- Used model for the annotation -->
                    <name>model</name>
                    <description/>
                    <rangeTypeName>org.texttechnologylab.annotation.model.MetaData</rangeTypeName>
                </featureDescription>
            </features>
        </typeDescription>
    </types>
</typeSystemDescription>
```

## HateCheck Integration in the DUUI Pipeline with Python
After defining the typesystem, we can integrate the HateCheck tool into the DUUI pipeline.
The HateCheck tool is implemented in python.
Install the needed packages with the following command, after creating a virtual environment(via conda) with python 3.8 in IntelliJ.<br>
```pip install -r requirements.txt```<br>
The following code snippet shows how to integrate the HateCheck tool into the DUUI pipeline `duui-HateCheckTest/src/main/python/duui_hate.py.
```python
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from typing import List, Optional, Dict, Union
import logging
from time import time
from fastapi import FastAPI, Response
from cassis import load_typesystem
import torch
from functools import lru_cache
# Import the HateCheck tool
from hatechecker import HateCheck
from threading import Lock
from starlette.responses import PlainTextResponse
model_lock = Lock()

sources = {
    "tomh": "tomh/toxigen_hatebert",
    "gronlp": "GroNLP/hateBERT"
}

languages = {
    "tomh": "en",
    "gronlp": "en"
}

class Settings(BaseSettings):
    # Name of this annotator
    annotator_name: str
    # Version of this annotator
    annotator_version: str
    # Log level
    log_level: str
    # model_name
    model_name: str
    # Name of this annotator
    model_version: str
    #cach_size
    model_cache_size: int
    # url of the model
    model_source: str
    # language of the model
    model_lang: str


# Load settings from env vars
settings = Settings()
lru_cache_with_size = lru_cache(maxsize=settings.model_cache_size)
# Set up logging with the specified log level
logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)

# Set the device to GPU if available, otherwise to CPU
device = 0 if torch.cuda.is_available() else "cpu"
logger.info(f'USING {device}')

# Load the predefined typesystem that is needed for this annotator to work
typesystem_filename = 'TypeSystemHate.xml'
logger.debug("Loading typesystem from \"%s\"", typesystem_filename)
with open(typesystem_filename, 'rb') as f:
    typesystem = load_typesystem(f)
    logger.debug("Base typesystem:")
    logger.debug(typesystem.to_xml())

# Load the Lua communication script
lua_communication_script_filename = "duui_hate.lua"
logger.debug("Loading Lua communication script from \"%s\"", lua_communication_script_filename)

class UimaSentence(BaseModel):
    text: str
    begin: int
    end: int


class UimaSentenceSelection(BaseModel):
    selection: str
    sentences: List[UimaSentence]

# Request sent by DUUI
# Note, this is transformed by the Lua script
class DUUIRequest(BaseModel):
    # Length of the document
    doc_len: int
    # Language of the document
    lang: str
    # List of input of the selected type
    selections:  List[UimaSentenceSelection]


# UIMA type: mark modification of the document 
# This is used to store the modification meta information in the CAS, for example who annotated the document, when and with which tool
class DocumentModification(BaseModel):
    user: str
    timestamp: int
    comment: str

# UIMA type: adds metadata to each annotation
# This is used to store the meta information in the CAS, for example which tool was used for the annotation and which version of the tool was used.
# It is important for the reproducibility of the analysis.
class AnnotationMeta(BaseModel):
    name: str
    version: str
    modelName: str
    modelVersion: str

def fix_unicode_problems(text):
    # fix emoji in python string and prevent json error on response
    # File "/usr/local/lib/python3.8/site-packages/starlette/responses.py", line 190, in render
    # UnicodeEncodeError: 'utf-8' codec can't encode characters in position xx-yy: surrogates not allowed
    clean_text = text.encode('utf-16', 'surrogatepass').decode('utf-16', 'surrogateescape')
    return clean_text

# Process function for HateCheck
def process_selection(model_name, selection):
    begin = []
    end = []
    non_hate = []
    hate = []
    # Fix unicode problems
    # Extract the text from the sentences
    for s in selection.sentences:
        s.text = fix_unicode_problems(s.text)

    texts = [
        s.text
        for s in selection.sentences
    ]
    with model_lock:
        classifier = load_model(model_name)
        # Get the results from the HateCheck tool
        results = classifier.hate_prediction(texts)
        for c, res in enumerate(results):
            sentence = selection.sentences[c]
            for r in res:
                # Add the results to the respective lists
                if r["label"] == "NOT HATE":
                    non_hate.append(r["score"])
                else:
                    hate.append(r["score"])
            begin.append(sentence.begin)
            end.append(sentence.end)
    # Return the results
    return {"begin": begin, "end": end, "non_hate": non_hate, "hate": hate}



# Response sent by DUUI
# Note, this is transformed by the Lua script
# Output of the HateCheck tool
class DUUIResponse(BaseModel):
    # Meta information, one per document
    meta: AnnotationMeta
    # Modification meta, one per document
    modification_meta: DocumentModification
    # Begin position of the annotations
    begins: List[int]
    # End position of the annotations
    ends: List[int]
    # Probability of non hate
    non_hate: List[float]
    # Probability of hate
    hate: List[float]
    # Model information
    # Needed for the reproducibility of the analysis that every annotation has the information which tool was used for the annotation.
    model_name: str
    # Version of the model used for the annotation
    model_version: str
    # Source of the model used for the annotation
    model_source: str
    # Language of the model used for the annotation
    model_lang: str


# REST API definition with FastAPI
app = FastAPI(
    openapi_url="/openapi.json",
    docs_url="/api",
    redoc_url=None,
    title=settings.annotator_name,
    description="Hate annotator",
    version=settings.model_version,
    terms_of_service="https://www.texttechnologylab.org/legal_notice/",
    # Contact of creator
    contact={
        "name": "TTLab Team",
        "url": "https://texttechnologylab.org",
        "email": "bagci@em.uni-frankfurt.de",
    },
    license_info={
        "name": "AGPL",
        "url": "http://www.gnu.org/licenses/agpl-3.0.en.html",
    },
)

# Reading Lua communication script
with open(lua_communication_script_filename, 'rb') as f:
    lua_communication_script = f.read().decode("utf-8")
logger.debug("Lua communication script:")
logger.debug(lua_communication_script_filename)


# Get typesystem of this annotator
@app.get("/v1/typesystem")
def get_typesystem() -> Response:
    # TODO remove cassis dependency, as only needed for typesystem at the moment?
    xml = typesystem.to_xml()
    xml_content = xml.encode("utf-8")

    return Response(
        content=xml_content,
        media_type="application/xml"
    )


# Return Lua communication script
@app.get("/v1/communication_layer", response_class=PlainTextResponse)
def get_communication_layer() -> str:
    return lua_communication_script


# Return documentation info
@app.get("/v1/documentation")
def get_documentation():
    return "Test"


# Process request from DUUI
@app.post("/v1/process")
def post_process(request: DUUIRequest):
    # Return data
    # Save modification start time for later
    modification_timestamp_seconds = int(time())
    # Initialize lists for the output
    # Begin position of the annotations
    begins = []
    # End position of the annotations
    ends = []
    # Probability of non hate
    non_hate = []
    # Probability of hate
    hate = []

    try:
        # Get the model information from the settings
        model_source = settings.model_source
        model_lang = settings.model_lang
        # set meta Informations
        meta = AnnotationMeta(
            name=settings.annotator_name,
            version=settings.annotator_version,
            modelName=settings.model_name,
            modelVersion=settings.model_version,
        )
        # Add modification info
        modification_meta_comment = f"{settings.annotator_name} ({settings.annotator_version}))"
        modification_meta = DocumentModification(
            user=settings.annotator_name,
            timestamp=modification_timestamp_seconds,
            comment=modification_meta_comment
        )
        # Process the selections
        for selection in request.selections:
            # get the output of the HateCheck tool
            output = process_selection(settings.model_name, selection)
            # Add the output to the lists
            begins.extend(output["begin"])
            ends.extend(output["end"])
            non_hate.extend(output["non_hate"])
            hate.extend(output["hate"])
    except Exception as ex:
        logger.exception(ex)
    # Return the response
    return DUUIResponse(meta=meta, modification_meta=modification_meta, begins=begins, ends=ends, non_hate=non_hate, hate=hate, model_name=settings.model_name, model_version=settings.model_version, model_source=settings.model_source, model_lang=settings.model_lang)

@lru_cache_with_size
def load_model(model_name):
    model_i = HateCheck(model_name, device)
    return model_i


```

## Lua
Here is an example of the LUA script, with some comments to explain the code for HateCheck LUA script:
```lua
-- Bind static classes from java to lua variables
StandardCharsets = luajava.bindClass("java.nio.charset.StandardCharsets")
Class = luajava.bindClass("java.lang.Class")
JCasUtil = luajava.bindClass("org.apache.uima.fit.util.JCasUtil")
-- Bind the classes from the typesystem to lua variables
HateCheck = luajava.bindClass("org.texttechnologylab.annotation.Hate")

function serialize(inputCas, outputStream, parameters)
   -- Get the document language, text and length from the CAS object
    local doc_lang = inputCas:getDocumentLanguage()
    local doc_text = inputCas:getDocumentText()
    local doc_len = TopicUtils:getDocumentTextLength(inputCas)
    -- Get the selection types from the input parameters
    -- The selection types are the types of annotations that the user wants to send to the annotator
    -- The parameter is a comma-separated list of different inputs
    local selection_types = parameters["selection"]
    local selections = {}
    local selections_count = 1
    -- Iterate over the selection types
    for selection_type in string.gmatch(selection_types, "([^,]+)") do
       local sentences = {}
       -- If the selection type is text, create a selection with the whole document text
       if selection_type == "text" then
           local s = {
               text = doc_text,
               begin = 0,
               ['end'] = doc_len
           }
           sentences[1] = s
       else
           -- If the selection type is a specific annotation type, get the covered text of the annotation and its begin and end offsets
           local sentences_count = 1
           local clazz = Class:forName(selection_type);
           local sentences_it = JCasUtil:select(inputCas, clazz):iterator()
           while sentences_it:hasNext() do
               local sentence = sentences_it:next()
               local s = {
                   text = sentence:getCoveredText(),
                   begin = sentence:getBegin(),
                   ['end'] = sentence:getEnd()
               }
               print(sentence:getCoveredText())
               sentences[sentences_count] = s
               sentences_count = sentences_count + 1
           end
       end
       local selection = {
           sentences = sentences,
           selection = selection_type
       }
       selections[selections_count] = selection
       selections_count = selections_count + 1
    end
    -- the Inputs for the python process, note that names must be the same as in the main python script 
    outputStream:write(json.encode({
        selections = selections,
        lang = doc_lang,
        doc_len = doc_len
    }))
end

-- This "deserialize" function is called on receiving the results from the annotator that have to be transformed into a CAS object
-- Inputs:
--  - inputCas: The actual CAS object to deserialize into
--  - inputStream: Stream that is received from to the annotator, can be e.g. a string, JSON payload, ...
function deserialize(inputCas, inputStream)
    local inputString = luajava.newInstance("java.lang.String", inputStream:readAllBytes(), StandardCharsets.UTF_8)
    local results = json.decode(inputString)
    
    -- Check if the results contain the necessary information
    if results["modification_meta"] ~= nil and results["meta"] ~= nil and results["begins"] ~= nil then
        
        -- Get the model information from the results
        local source = results["model_source"]
        local model_version = results["model_version"]
        local model_name = results["model_name"]
        local model_lang = results["model_lang"]
        local modification_meta = results["modification_meta"]
        
        -- Create a new DocumentModification annotation and add it to the CAS
        local modification_anno = luajava.newInstance("org.texttechnologylab.annotation.DocumentModification", inputCas)
        modification_anno:setUser(modification_meta["user"])
        modification_anno:setTimestamp(modification_meta["timestamp"])
        modification_anno:setComment(modification_meta["comment"])
        modification_anno:addToIndexes()
        
        -- Create a new MetaData annotation and add it to the CAS
        local model_meta = luajava.newInstance("org.texttechnologylab.annotation.model.MetaData", inputCas)
        model_meta:setModelVersion(model_version)
        model_meta:setModelName(model_name)
        model_meta:setSource(source)
        model_meta:setLang(model_lang)
        model_meta:addToIndexes()
        
        -- Get the results from the annotator
        local meta = results["meta"]
        local begins = results["begins"]
        local ends = results["ends"]
        local hates = results["hate"]
        local non_hates = results["non_hate"]
        
        -- Iterate over the results and create new Hate annotations for each result
        for index_i, res in ipairs(hates) do
            local hate = luajava.newInstance("org.texttechnologylab.annotation.Hate", inputCas)
            hate:setBegin(begins[index_i])
            print(begins[index_i])
            hate:setEnd(ends[index_i])
            print(ends[index_i])
            hate:setHate(hates[index_i])
            print(hates[index_i])
            hate:setNonHate(non_hates[index_i])
            print(non_hates[index_i])
            hate:setModel(model_meta)
            hate:addToIndexes()
        end
    end
end
```
## Docker Container
The Docker container is defined in the Dockerfile, here is an example for building a Docker container for HateCheck:
```dockerfile
# HateCheck is implemented in Python for this reason we use the Python with necessary version
FROM python:3.8

# Define the working directory
WORKDIR /usr/src/app

# Expose the port of DUUI, which is 9714
# Note that the port 9714 must be expose by every build Docker container
EXPOSE 9714

# Copy the requirements file to the working directory
COPY ./reqiurements.txt ./reqiurements.txt
# Install the necessary packages
RUN pip install torch==2.2.0 torchvision==0.17.0 torchaudio==2.2.0 --index-url https://download.pytorch.org/whl/cu118
RUN pip install -r reqiurements.txt

# Download the necessary models to save time when the Docker container is started
RUN python -c "from transformers import pipeline; pipeline('text-classification', model='Andrazp/multilingual-hate-speech-robacofi')"

# Copy the necessary files to the working directory
# It is possible to python directory to the working directory
# For the demonstration we copy every file to the working directory separately
# TypeSystemHate.xml is the typesystem for the HateCheck tool
COPY ./src/main/python/TypeSystemHate.xml ./TypeSystemHate.xml
# Evaluation.py is the evaluation script for the HateCheck tool
COPY ./src/main/python/evaluator.py ./evaluator.py
# HateCheck.py define seperatly the HateCheck tool
COPY ./src/main/python/hatechecker.py ./hatechecker.py
# HateCheck.lua is the LUA script for the communication between the UIMA pipeline and the HateCheck tool
COPY ./src/main/python/duui_hate.lua ./duui_hate.lua
# HateCheck.py is the main python script for the HateCheck tool
COPY ./src/main/python/duui_hate.py ./duui_hate.py


# Define the necessary environment variables
# ARG is used to define the default environment variables
ARG ANNOTATOR_NAME="duui-hate:app"
# ENV is used to define the environment variables, will be overwritten by the docker-compose file, if the environment variables are defined in the build process
ENV ANNOTATOR_NAME=$ANNOTATOR_NAME
ARG ANNOTATOR_VERSION="unset"
ENV ANNOTATOR_VERSION=$ANNOTATOR_VERSION

# log level
ARG LOG_LEVEL="DEBUG"
ENV LOG_LEVEL=$LOG_LEVEL

# config
ARG MODEL_CACHE_SIZE=1
ENV MODEL_CACHE_SIZE=$MODEL_CACHE_SIZE
ARG MODEL_NAME=""
ENV MODEL_NAME=$MODEL_NAME
ARG MODEL_VERSION=""
ENV MODEL_VERSION=$MODEL_VERSION
ARG MODEL_SOURCE=""
ENV MODEL_SOURCE=$MODEL_SOURCE
ARG MODEL_LANG=""
ENV MODEL_LANG=$MODEL_LANG

# Define the entrypoint for the Docker container, the entrypoint is the main script that is executed when the Docker container is started, where POST Process is called
# The name of the main script must written before :app
# The Entyproint used the uvicorn server to start the main script.
# The host is defined as 0.0.0.0 to ensure that the Docker container is accessible from the outside
# The port is defined as 9714, which is port to the outside
ENTRYPOINT ["uvicorn", "duui_hate:app", "--host", "0.0.0.0", "--port" ,"9714"]
# Define the number of workers
CMD ["--workers", "1"]
```

## Docker Bash Script
The Docker Bash Script is used to build the Docker container and save the Docker container with right name and version.
Here is an example of the Docker Bash Script for HateCheck:
```bash
# Define the necessary environment variables
export ANNOTATOR_NAME=duui-hate
export ANNOTATOR_VERSION=0.1.0
export LOG_LEVEL=INFO
eport MODEL_CACHE_SIZE=3

#---------------------------------------------------------------------
# Define the necessary environment variables for the HateCheck tool
export  MODEL_NAME="Andrazp/multilingual-hate-speech-robacofi"
export MODEL_SPECNAME="andrazp"
export MODEL_VERSION="c2b98c47f5e13c326a7af48ba544fff4d93fbc70"
export MODEL_SOURCE="https://huggingface.co/Andrazp/multilingual-hate-speech-robacofi"
export MODEL_LANG="Multi"
#--------------------------------------------------------------------

# Define the Docker Registry, the Docker Registry is used to save the Docker container with the right name and version
# The name docker.texttechnologylab.org is the Docker Registry of the Texttechnology Lab at the Goethe University Frankfurt
export DOCKER_REGISTRY="docker.texttechnologylab.org/"
export DUUI_CUDA=
# If the tool is implemented with CUDA, then the name of the Docker container should be extended with -cuda, so that users can see that the tool is implemented with CUDA
#export DUUI_CUDA="-cuda"

# Build the Docker container with the necessary environment variables
# --build-arg is necessary to define the environment variables in the Docker container
docker build \
  --build-arg ANNOTATOR_NAME \
  --build-arg ANNOTATOR_VERSION \
  --build-arg LOG_LEVEL \
  --build-arg MODEL_CACHE_SIZE \
  --build-arg MODEL_NAME \
  --build-arg MODEL_VERSION \
  --build-arg MODEL_SOURCE \
  --build-arg MODEL_LANG \
  # -t is used to define the name and version of the Docker container
  -t ${DOCKER_REGISTRY}${ANNOTATOR_NAME}"-"${MODEL_SPECNAME}:${ANNOTATOR_VERSION}${DUUI_CUDA} \
  -f src/main/docker/Dockerfile${DUUI_CUDA} \
  .

# Save the Docker container with the right name and version
docker tag \
  ${DOCKER_REGISTRY}${ANNOTATOR_NAME}"-"${MODEL_SPECNAME}:${ANNOTATOR_VERSION}${DUUI_CUDA} \
  ${DOCKER_REGISTRY}${ANNOTATOR_NAME}"-"${MODEL_SPECNAME}:latest${DUUI_CUDA}
```

## Testing
The testing of the integrated tool is important to ensure that the tool is working correctly.
It also helps to check how tool reacts to different inputs and outputs, so that the tool can be corrected if necessary, optimized or improved.
Junit test can be used to test the tool, here is an example of the Junit test for HateCheck:
```java
// import the necessary packages
package org.hucompute.textimager.uima.hate;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.XmlCasSerializer;
import org.junit.jupiter.api.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.*;

// import the necessary types from the typesystem
import org.texttechnologylab.annotation.Hate;
public class MultiTestHate {
    static DUUIComposer composer;
    static JCas cas;

    // Define the URL of the Docker container
    // Docker container must be started locally
    static String url = "http://127.0.0.1:9714";

    // Define the composer and the cas object
    @BeforeAll
    static void beforeAll() throws URISyntaxException, IOException, UIMAException, SAXException, CompressorException {
        composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(remoteDriver);
//        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
//        composer.addDriver(docker_driver);


        cas = JCasFactory.createJCas();
    }

    // Shutdown the composer in the end
    @AfterAll
    static void afterAll() throws UnknownHostException {
        composer.shutdown();
    }

    // Reset the pipeline and the cas object after each test
    @AfterEach
    public void afterEach() throws IOException, SAXException {
        composer.resetPipeline();

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        XmlCasSerializer.serialize(cas.getCas(), null, stream);
        System.out.println(stream.toString(StandardCharsets.UTF_8));

        cas.reset();
    }

    // Function to integrate the list of sentences into the cas object, with beginning and end of the sentence
    public void createCas(String language, List<String> sentences) throws UIMAException {
        cas.setDocumentLanguage(language);

        StringBuilder sb = new StringBuilder();
        for (String sentence : sentences) {
            Sentence sentenceAnnotation = new Sentence(cas, sb.length(), sb.length()+sentence.length());
            sentenceAnnotation.addToIndexes();
            sb.append(sentence).append(" ");
        }

        cas.setDocumentText(sb.toString());
    }

    // Test the tools with english sentences
    @Test
    public void EnglishTest() throws Exception {
        // Add the component to the composer with the necessary parameters
        // The selection parameter is used to define the type of annotations that the user wants to send to the annotator 
        // In this case, the user wants to send the Sentence annotations to the annotator
        composer.add(
                new DUUIRemoteDriver.Component(url)
                        .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
        );
        // Define the sentences that are used for the test
        List<String> sentences = Arrays.asList(
                "I hate hate it. How can you do that bad thing to me! HOW!",
                "I very happy to be here. I love this place."
        );

        // Create the CAS object with the necessary language and sentences
        createCas("en", sentences);

        // Run the composer with the CAS object
        composer.run(cas);

        // Define the expected output
        HashMap<String, String> expected = new HashMap<>();
        expected.put("0_57", "HATE");
        expected.put("58_101", "NonHate");
        
        // Get all the Hate annotations from the CAS object and check if the output is the same as the expected output
        Collection<Hate> all_hate = JCasUtil.select(cas, Hate.class);
        for (Hate hate : all_hate) {
            int begin = hate.getBegin();
            int end = hate.getEnd();
            double hate_i = hate.getHate();
            double non_hate = hate.getNonHate();
            String out_i = "HATE";
            if (hate_i < non_hate){
                out_i = "NonHate";
            }
            String expected_i = expected.get(begin+"_"+end);
            Assertions.assertEquals(expected_i, out_i);
        }
    }
}
```


# Author

[Mevlüt Bağcı](https://www.texttechnologylab.org/team/mevl%C3%BCt-bagci/)

If you have any questions or need more information, feel free to reach out to the author.
