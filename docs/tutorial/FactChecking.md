---
layout: default
---
# Integration of Fact-Checking
The Integration for this example is with python, because the fact-checking tool is implemented in python.
In this section, we will explain how to integrate a fact-checking tool into the DUUI pipeline step by step.
The fact-checking module is a part of the DUUI pipeline that is responsible for calculating how strong the claim is supported by the fact.
We use UniEval ([Zhong et al. 2022](https://aclanthology.org/2022.emnlp-main.131/)) as the tool, which allows us to check whether a claim is supported by a fact.

The full example code can be found in the [GitHub repository](https://github.com/texttechnologylab/duui-uima/tree/main/duui-FactChecking).

## Typeystem for Fact-Checking
The first step is to define the typesystem for the fact-checking tool, if the needed types are not already defined in the typesystem repository ([UIMATypeSystem](https://github.com/texttechnologylab/UIMATypeSystem)).
The typesystem for the fact-checking tool is defined in the following XML format:
```xml
<typeSystemDescription xmlns="http://uima.apache.org/resourceSpecifier">
    <name>TypeSystemFactChecking</name>
    <description/>
    <version>1.0</version>
    <vendor/>
    <!-- Imports of all needed Types -->
    <imports>
        <import name="desc.type.TextTechnologyDokumentAnnotation"/>
        <import name="desc.type.TextTechnologyAnnotation"/>
        <import name="desc.type.TypeSystemModelMeta"/>
        <import name="desc.type.TypeSystemModelAnnotation"/>
    </imports>
    <types>
        <!--
            Needed Input Types for Fact-Checking.
            The Claim inheritance from Annotation.
            Note that every child gets the features of the parent.
        -->
        <typeDescription>
            <name>org.texttechnologylab.annotation.Claim</name>
            <description> One Claim for different facts </description>
            <supertypeName>uima.tcas.Annotation</supertypeName>
            <features>
                <!-- Information of Claim like the source-->
                <featureDescription>
                    <name>value</name>
                    <description>Information of Claim</description>
                    <rangeTypeName>uima.cas.String</rangeTypeName>
                </featureDescription>
                <!-- Set of Fact -->
                <featureDescription>
                    <name>Facts</name>
                    <description>Set of Fact</description>
                    <rangeTypeName>uima.cas.FSArray</rangeTypeName>
                    <elementType>org.texttechnologylab.annotation.Fact</elementType>
                </featureDescription>
            </features>
        </typeDescription>
        
        <!--
            Needed Input Types for Fact-Checking.
            Fact inheritance from Annotation.
        -->
        <typeDescription>
            <name>org.texttechnologylab.annotation.Fact</name>
            <description> One Fact for different claims </description>
            <supertypeName>uima.tcas.Annotation</supertypeName>
            <features>
                <!-- Information of Fact like the source-->
                <featureDescription>
                    <name>value</name>
                    <description>Information for the fact</description>
                    <rangeTypeName>uima.cas.String</rangeTypeName>
                </featureDescription>
                <!-- Set of Claims -->
                <featureDescription>
                    <name>Claims</name>
                    <description>Set of Claims</description>
                    <rangeTypeName>uima.cas.FSArray</rangeTypeName>
                    <elementType>org.texttechnologylab.annotation.Claim</elementType>
                </featureDescription>
            </features>
        </typeDescription>
        
        <!-- 
        Needed Output Types for Fact-Checking.
        MetaData inheritance from model.MetaData, which save in CAS document which tool annotated the claim fact pair.
        -->
        <typeDescription>
            <name>org.texttechnologylab.annotation.model.FactCheckingMetaData</name>
            <description/>
            <supertypeName>org.texttechnologylab.annotation.model.MetaData</supertypeName>
            <features>
                <featureDescription>
                    <name>DependeciesVersion</name>
                    <description>Dependency Library Version e.g. Pytorch...</description>
                    <rangeTypeName>uima.cas.StringArray</rangeTypeName>
                </featureDescription>
            </features>
        </typeDescription>
        
        <!--
            Needed Output Types for Fact-Checking
            FactChecking inheritance from Annotation
        -->
        <typeDescription>
            <name>org.texttechnologylab.annotation.FactChecking</name>
            <description> Does the assertion confirm the statement </description>
            <supertypeName>uima.tcas.Annotation</supertypeName>
            <features>
                <!-- The Fact that is checked -->
                <featureDescription>
                    <name>Fact</name>
                    <description/>
                    <rangeTypeName>org.texttechnologylab.annotation.Fact</rangeTypeName>
                </featureDescription>
                <!-- The Claim that is checked -->
                <featureDescription>
                    <name>Claim</name>
                    <description/>
                    <rangeTypeName>org.texttechnologylab.annotation.Claim</rangeTypeName>
                </featureDescription>
                <!-- The result of the check -->
                <featureDescription>
                    <name>consistency</name>
                    <description/>
                    <rangeTypeName>uima.cas.Double</rangeTypeName>
                </featureDescription>
                
                <!-- 
                    The Reference for the used tool, so that the user can see which tool was used for the annotation.
                    It is important for the reproducibility of the analysis.
                -->
                <featureDescription>
                    <name>model</name>
                    <description/>
                    <rangeTypeName>org.texttechnologylab.annotation.model.MetaData</rangeTypeName>
                </featureDescription>
            </features>
        </typeDescription>
    </types>
</typeSystemDescription>
```

## FactChecking Integration in the DUUI Pipeline with Python
After defining the typesystem, we can integrate the fact-checking tool into the DUUI pipeline.
The of the fact-checking tool is implemented in python.
Install the needed packages with the following command, after creating a virtual environment(via conda) with python 3.8 in IntelliJ.<br>
```pip install -r requirements.txt```<br>
The following code snippet shows how to integrate the fact-checking tool into the DUUI pipeline `duui-FactCheckingTest/src/main/python/duui_fact.py.
```python
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from typing import List, Optional
import logging
from time import time
from fastapi import FastAPI, Response
from cassis import load_typesystem
import torch
from functools import lru_cache
# Import the fact-checking tools from the factchecker.py, which is in the same directory will be explained in the next step.
from factchecker import UniEvalFactCheck, NubiaFactCheck
from threading import Lock
# from sp_correction import SentenceBestPrediction

# Settings
# These are automatically loaded from env variables
from starlette.responses import PlainTextResponse

model_lock = Lock()

# Sources of the models for the meta information
# It is important for the reproducibility of the analysis and needed because the user can see which tool was used for the annotation.
sources = {
    "nubia": "https://github.com/wl-research/nubia",
    "unieval": "https://github.com/maszhongming/UniEval"
}

# Languages of the models for the meta information and needed because the user can see which tool was used for the annotation.
languages = {
    "nubia": "en",
    "unieval": "en"
}


class Settings(BaseSettings):
    # Name of this annotator
    fact_annotator_name: str
    # Version of this annotator
    fact_annotator_version: str
    # Log level
    fact_log_level: str
    # model_name in this case nubia or unieval
    fact_model_name: str
    # Version of the model used by this annotator
    fact_model_version: str
    # cach_size for the model
    fact_model_cache_size: int


# Load settings from env vars
settings = Settings()
lru_cache_with_size = lru_cache(maxsize=settings.fact_model_cache_size)
# Set up logging with the specified log level from the settings
logging.basicConfig(level=settings.fact_log_level)
logger = logging.getLogger(__name__)

# Set device to GPU if available
device = 0 if torch.cuda.is_available() else "cpu"
logger.info(f'USING {device}')

# Load the predefined typesystem that is needed for this annotator to work
typesystem_filename = 'TypeSystemFactChecking.xml'
logger.debug("Loading typesystem from \"%s\"", typesystem_filename)
with open(typesystem_filename, 'rb') as f:
    typesystem = load_typesystem(f)
    logger.debug("Base typesystem:")
    logger.debug(typesystem.to_xml())

# Load the Lua communication script
lua_communication_script_filename = "duui_fact.lua"
logger.debug("Loading Lua communication script from \"%s\"", lua_communication_script_filename)


# Request sent by DUUI
# Note, this is transformed by the Lua script
class DUUIRequest(BaseModel):
    # List of Claims every claim element is a dictionary with following Infos: text, begin, end and facts(to check the claim with the facts)
    claims_all: Optional[list] = None
    # List of Facts every fact element is a dictionary with following Infos: text, begin, end and claims(to check the fact with the claims)
    facts_all: Optional[list] = None
    # Optional map/dict of parameters
    parameters: Optional[dict] = None


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


# Response sent by DUUI
# Note, this is transformed by the Lua script
# Output of the fact-checking tool
class DUUIResponse(BaseModel):
    # Meta information, one per document
    meta: AnnotationMeta
    # Modification meta, one per document
    modification_meta: DocumentModification
    # List of consistencies for each claim-fact pair
    # List of begin positions of the claims needed for finding the claim in the CAS
    begin_claims: List[int]
    # List of end positions of the claims needed for finding the claim in the CAS
    end_claims: List[int]
    # List of begin positions of the facts needed for finding the fact in the CAS
    begin_facts: List[int]
    # List of end positions of the facts needed for finding the fact in the CAS
    end_facts: List[int]
    # List of consistencies for each claim-fact pair
    consistency: List[float]
    # Model information
    # Needed for the reproducibility of the analysis that every annotation has the information which tool was used for the annotation.
    # Name of the model used for the annotation, for example nubia or unieval
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
    title=settings.fact_annotator_name,
    description="Factuality annotator",
    version=settings.fact_model_version,
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


# Get typesystem of this tool
@app.get("/v1/typesystem")
def get_typesystem() -> Response:
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
    return "This is Python RestAPI "


# Process request from DUUI
@app.post("/v1/process")
def post_process(request: DUUIRequest):
    # Return data
    meta = None
    # Already checked claim-fact pairs
    checkfacts = {}
    # Consistency of the claim-fact pair
    consistency = []
    # Begin and end positions of the claims and facts
    begin_claims = []
    end_claims = []
    begin_facts = []
    end_facts = []
    # Save modification start time for later
    modification_timestamp_seconds = int(time())
    try:
        # Get the model source and language
        # Needed for the reproducibility of the analysis that every annotation has the information which tool was used for the annotation.
        # Normally DUUI is atomar for each model, however it is possible to use multiple models in one DUUI instance.
        # In this case, the source and language of the model is needed for every call.
        model_source = sources[settings.fact_model_name]
        model_lang = languages[settings.fact_model_name]
        # Add meta info to the response for the user to see which tool was used for the annotation and which version of the tool was used.
        # Like Prevoiusly mentioned it change for every call, if multiple models are used in one DUUI instance.
        meta = AnnotationMeta(
            name=settings.fact_annotator_name,
            version=settings.fact_annotator_version,
            modelName=settings.fact_model_name,
            modelVersion=settings.fact_model_version,
        )
        # Add modification info
        modification_meta_comment = f"{settings.fact_annotator_name} ({settings.fact_annotator_version}))"
        modification_meta = DocumentModification(
            user=settings.fact_annotator_name,
            timestamp=modification_timestamp_seconds,
            comment=modification_meta_comment
        )
        # Get the claims and facts from the request, which were transformed by the Lua script in the Serilization.
        claims = request.claims_all
        facts = request.facts_all
        # Model loading and checking
        with model_lock:
            # Load the model
            model_run = load_model(settings.fact_model_name)
            claim_list = []
            fact_list = []
            counters = []
            # get the claim_list and fact_list
            for c, claim in enumerate(claims):
                for fc, fact_i in enumerate(claim["facts"]):
                    claim_list.append(claim["text"])
                    fact_list.append(fact_i["text"])
                    # Needed to identify the claim-fact pair
                    counters.append(f"{c}_{fc}")
            # Compute every pair of claim and fact
            factchecked = model_run.check(claim_list, fact_list)
            for c, fact_check_i in enumerate(factchecked):
                checkfacts[counters[c]] = fact_check_i
            # check fact_list
            claim_list = []
            fact_list = []
            counters = []
            factscheck = {}
            # must be checked in both directions claim-fact and fact-claim
            for fc, fact_i in enumerate(facts):
                for c, claim in enumerate(fact_i["claims"]):
                    # Skip the claim-fact pair if it was already checked
                    if f"{c}_{fc}" not in checkfacts:
                        claim_list.append(claim["text"])
                        fact_list.append(fact_i["text"])
                        counters.append(f"{c}_{fc}")
            if len(claim_list) > 0:
                factchecked = model_run.check(claim_list, fact_list)
            else:
                factchecked = {}
            for c, fact_check_i in enumerate(factchecked):
                factscheck[counters[c]] = fact_check_i
            for key_i in checkfacts:
                # Get the position of the claim and fact in list
                key_claim = int(key_i.split("_")[0])
                key_facts = int(key_i.split("_")[1])
                # Get the consistency of the claim-fact pair
                cons = checkfacts[key_i]['consistency']
                # Save the begin, end position and the fact-checking result
                consistency.append(cons)
                claim_i = claims[key_claim]
                # Get the begin and end position of the claim
                begin_claims.append(claim_i["begin"])
                end_claims.append(claim_i["end"])
                fact_i = claim_i["facts"][key_facts]
                begin_facts.append(fact_i["begin"])
                end_facts.append(fact_i["end"])
            # save for both direction
            for key_i in factscheck:
                key_claim = int(key_i.split("_")[0])
                key_facts = int(key_i.split("_")[1])
                cons = checkfacts[key_i]['consistency']
                consistency.append(cons)
                fact_i = facts[key_facts]
                claim_i = fact_i["claims"][key_claim]
                begin_claims.append(claim_i["begin"])
                end_claims.append(claim_i["end"])
                begin_facts.append(fact_i["begin"])
                end_facts.append(fact_i["end"])
    except Exception as ex:
        # Log exception
        logger.exception(ex)
    # Return the response to the DUUI with the meta information, modification meta information, consistency, begin and end positions of the claims and facts and model information
    return DUUIResponse(meta=meta, modification_meta=modification_meta, consistency=consistency,
                        begin_claims=begin_claims, end_claims=end_claims, begin_facts=begin_facts, end_facts=end_facts,
                        model_name=settings.fact_model_name, model_version=settings.fact_model_version,
                        model_source=model_source, model_lang=model_lang)

# Load the model with the lru_cache with the size of the cache_size from the settings file
# The model is loaded only once and then stored in the cache.
@lru_cache_with_size
def load_model(model_name):
    if model_name == "nubia":
        model_i = NubiaFactCheck()
    else:
        model_i = UniEvalFactCheck(device=device)
    return model_i
```
The fact checking tool can be implemented also in the main python file, but it is possible to implement it in a separate file and test it separately, before integrating it into the DUUI pipeline.
The fact-checking tool is implemented in the factchecker.py file in the same directory as the duui_fact.py file.
The following code snippet shows the implementation of the fact-checking tool in the factchecker.py file.
The imports `evaluator.py`, `scorer.py` and `utils.py` are the implementation of the fact-checking tool UniEval ([Zhong et al. 2022](https://aclanthology.org/2022.emnlp-main.131/)) from their repository ([UniEval](https://github.com/maszhongming/UniEval)).
```python
from utils import convert_to_json
from evaluator import get_evaluator
from nubia_score import Nubia
import torch
from typing import List


class NubiaFactCheck:
    # Load the Nubia model only runs with CPU
    def __init__(self):
        print("Loading Nubia...")
        self.nubia = Nubia()
        print("Nubia loaded.")

    # Check the claim with the evidence
    def check_i(self, claim: str, evidence: str, six_dim=False, aggregator="agg_two"):
        scores = self.nubia.score(claim, evidence, get_features=True, six_dim=six_dim, aggregator=aggregator)
        labels = {k: v for k, v in scores["features"].items()}
        return {"consistency": scores["nubia_score"], "Labels": labels}

    # Checks a list of claims and facts, each claim is checked with the corresponding fact via the index
    def check(self, claims: List[str], evidences: List[str], six_dim=False, aggregator="agg_two"):
        out_i = []
        for c, claim_i in enumerate(claims):
            fact_i = evidences[c]
            out_i.append(self.check_i(claim_i, fact_i, six_dim, aggregator))
        return out_i


class UniEvalFactCheck:
    # Intialisation UniEval
    def __init__(self, device="cpu"):
        # Set the device GPU(cuda) or CPU
        self.device = device
        # Load the UniEval model fact for the fact-checking
        self.evaluator = get_evaluator("fact", device=self.device)

    # Compute the consistency of the claims the facts, each claim is checked with the corresponding fact via the index
    def check(self, claim: List[str], evidence: List[str]):
        data = convert_to_json(output_list=claim, src_list=evidence)
        eval_scores = self.evaluator.evaluate(data, print_result=True)
        return eval_scores


if __name__ == '__main__':
    # Test the fact-checking tool
    _evidence = """
    Jane writes code for Huggingface.
    """

    _claim = 'Jane is an engineer.'
    device_1 = "cuda:0" if torch.cuda.is_available() else "cpu"
    evidence1 = """
    Justine Tanya Bateman (born February 19, 1966) is an American writer, producer, and actress . She is best known for her regular role as Mallory Keaton on the sitcom Family Ties (1982 -- 1989). Until recently, Bateman ran a production and consulting company, SECTION 5 . In the fall of 2012, she started studying computer science at UCLA.
    """
    claim1 = 'Justine Bateman is a producer.'
    # factchecking = FactCheck()
    # print(factchecking.all_check(claim1, evidence1))
    # print(factchecking.all_check(_claim, _evidence))
    unicheck = UniEvalFactCheck(device=device_1)
    print(unicheck.check([evidence1], [claim1]))
    nubiacheck = NubiaFactCheck()
    print(nubiacheck.check_i(claim1, evidence1))
```
## Lua
The Lua script is used to transform the request and response from the DUUI pipeline of the CAS document to the REST API.
```lua
-- Bind static classes from java to lua variables
StandardCharsets = luajava.bindClass("java.nio.charset.StandardCharsets")
util = luajava.bindClass("org.apache.uima.fit.util.JCasUtil")
-- Bind the classes from the typesystem to lua variables
facts = luajava.bindClass("org.texttechnologylab.annotation.Fact")
claims = luajava.bindClass("org.texttechnologylab.annotation.Claim")
FactCheck = luajava.bindClass("org.texttechnologylab.annotation.FactChecking")

-- This "serialize" function is called to transform the CAS object into an stream that is sent to the annotator
-- Inputs:
--  - inputCas: The actual CAS object to serialize
--  - outputStream: Stream that is sent to the annotator, can be e.g. a string, JSON payload, ...
function serialize(inputCas, outputStream)
    -- Get data from CAS
    local doc_text = inputCas:getDocumentText()
    local doc_lang = inputCas:getDocumentLanguage()
    -- Get all claims and facts from the CAS
    local all_facts = {}
    local all_claims = {}
    local sen_counter = 1
    -- Via Iterator get all claims and facts from the CAS
    local claims_in = util:select(inputCas, claims):iterator()
    -- Until there are no more claims
    while claims_in:hasNext() do
        local claim = claims_in:next()
        -- Claim inherits from Annotation, so we can use the methods of the Annotation class
        -- Get the begin and end position of the claim, needed to find the claim in the CAS
        local begin_claim = claim:getBegin()
        local end_claim = claim:getEnd()
        -- Get the text of the claim
        local claim_text = claim:getCoveredText()
        -- Save the claim in the all_claims list
        all_claims[sen_counter] = {}
        all_claims[sen_counter]["begin"] = begin_claim
        all_claims[sen_counter]["end"] = end_claim
        all_claims[sen_counter]["text"] = claim_text
        all_claims[sen_counter]["facts"] = {}
        local facts_in = claim:getFacts():iterator()
        local fact_counter = 1
        -- get every fact, which should be compared with the claim
        while facts_in:hasNext() do
            local fact = facts_in:next()
            local begin_fact = fact:getBegin()
            local end_fact = fact:getEnd()
            local fact_text = fact:getCoveredText()
            all_claims[sen_counter]["facts"][fact_counter] = {}
            all_claims[sen_counter]["facts"][fact_counter]["begin"] = begin_fact
            all_claims[sen_counter]["facts"][fact_counter]["end"] = end_fact
            all_claims[sen_counter]["facts"][fact_counter]["text"] = fact_text
            fact_counter = fact_counter + 1
        end
        sen_counter = sen_counter + 1
    end
    -- both directions claim-fact and fact-claim
    -- same process as for the claims but for the facts
    local fact_counter = 1
    local facts_in = util:select(inputCas, facts):iterator()
    while facts_in:hasNext() do
        local fact_now = facts_in:next()
        local begin_fact = fact_now:getBegin()
        local end_fact = fact_now:getEnd()
        local fact_text = fact_now:getCoveredText()
        all_facts[fact_counter] = {}
        all_facts[fact_counter]["begin"] = begin_fact
        all_facts[fact_counter]["end"] = end_fact
        all_facts[fact_counter]["text"] = fact_text
        all_facts[fact_counter]["claims"] = {}
        local claim_counter = 1
        local claims_in = fact_now:getClaims():iterator()
        while claims_in:hasNext() do
            local claim = claims_in:next()
            local begin_claim = claim:getBegin()
            local end_claim = claim:getEnd()
            local claim_text = claim:getCoveredText()
            all_facts[fact_counter]["claims"][claim_counter] = {}
            all_facts[fact_counter]["claims"][claim_counter]["begin"] = begin_claim
            all_facts[fact_counter]["claims"][claim_counter]["end"] = end_claim
            all_facts[fact_counter]["claims"][claim_counter]["text"] = claim_text
            claim_counter = claim_counter + 1
        end
        fact_counter = fact_counter + 1
    end
    -- the Inputs for the python process, note that names must be the same as in the python script duui_fact.py
    outputStream:write(json.encode({
        claims_all = all_claims,
        facts_all = all_facts
    }))
-- --     print("sendToPython")
end

-- This "deserialize" function is called on receiving the results from the annotator that have to be transformed into a CAS object
-- Inputs:
--  - inputCas: The actual CAS object to deserialize into
--  - inputStream: Stream that is received from to the annotator, can be e.g. a string, JSON payload, ...
function deserialize(inputCas, inputStream)
    local inputString = luajava.newInstance("java.lang.String", inputStream:readAllBytes(), StandardCharsets.UTF_8)
    local results = json.decode(inputString)

    -- Only write to CAS if the results are not nil
    if results["modification_meta"] ~= nil and results["meta"] ~= nil and results["consistency"] ~= nil then
        -- Model information
        print("GetInfo")
        local source = results["model_source"]
        local model_version = results["model_version"]
        local model_name = results["model_name"]
        local model_lang = results["model_lang"]
        print("meta")
        -- Modification meta information
        local modification_meta = results["modification_meta"]
        local modification_anno = luajava.newInstance("org.texttechnologylab.annotation.DocumentModification", inputCas)
        modification_anno:setUser(modification_meta["user"])
        modification_anno:setTimestamp(modification_meta["timestamp"])
        modification_anno:setComment(modification_meta["comment"])
        -- write the information into CAS document
        modification_anno:addToIndexes()

        print("setMetaData")
        -- Define the model information for the meta data
        local model_meta = luajava.newInstance("org.texttechnologylab.annotation.model.MetaData", inputCas)
        model_meta:setModelVersion(model_version)
        print(model_version)
        model_meta:setModelName(model_name)
        print(model_name)
        model_meta:setSource(source)
        print(source)
        model_meta:setLang(model_lang)
        print(model_lang)
        model_meta:addToIndexes()

        local meta = results["meta"]
        -- Get the begin and end positions of the claims and facts
        local begin_claims = results["begin_claims"]
        local end_claims = results["end_claims"]
        local begin_facts = results["begin_facts"]
        local end_facts = results["end_facts"]
        local consistency = results["consistency"]
        for index_i, cons in ipairs(consistency) do
            local begin_claim_i = begin_claims[index_i]
            local end_claim_i = end_claims[index_i]
            local begin_fact_i = begin_facts[index_i]
            local end_fact_i = end_facts[index_i]
            -- Get the claim and fact from the CAS
            local claim_i = util:selectAt(inputCas, claims, begin_claim_i, end_claim_i):iterator():next()
            local fact_i  = util:selectAt(inputCas, facts, begin_fact_i, end_fact_i):iterator():next()
            -- Create the fact-checking annotation
            local factcheck_i = luajava.newInstance("org.texttechnologylab.annotation.FactChecking", inputCas)
            -- Set the claim, fact, consistency and model information
            factcheck_i:setClaim(claim_i)
            factcheck_i:setFact(fact_i)
            factcheck_i:setConsistency(cons)
            factcheck_i:setModel(model_meta)
            factcheck_i:addToIndexes()
        end
    end
end
```



# Author

[Mevlüt Bağcı](https://www.texttechnologylab.org/team/mevl%C3%BCt-bagci/)

If you have any questions or need more information, feel free to reach out to the author.
