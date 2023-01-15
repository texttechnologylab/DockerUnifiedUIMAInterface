# DockerUnifiedUIMAInterface
This code should give a small introduction into the usage of the framework DockerUnifiedUIMAInterface. The code is separated into multiple small projects each show casing a different use case of the framework. Every project contains another README to explain the example in a bit more depth. Since the full code is not disclosed in our submission (but will be published on github at a later point in time), this code samples should provide more insight into the inner workings of our framework and help to prove our point that NLP can be easier, scalable and reproducible with a minimum amount of work through the usage of our framework.

Lua scripts transform both input and output into formats understandable by the container. Therefore the presented code examples here work with different formats (String,JSON,MsgPack,nothing) to show this unique ability.


1. `uima_driver` - Shows the basic usage of the framework as most people use UIMA today, by creating the full pipeline on a single machine by using dedicated class names to instantiate the given analysis engine.
2. `usage_with_container` - Shows a container embedding sentiment analysis with detected with state of the art transformers from huggingface in a UIMA JCas Document. The server is written in a completely different language (rust) in less than 100 lines of code using multiple data exchange formats (input: String) (output: MsgPack Bytecode)
3. `language_detector` - A container written in python detecting the language of the text and annotating this information into a UIMA JCas Document. This container also has less than 100 lines of code and uses another data exchange format (input:String) (output:JSON). There is also a dockerfile attached to built this into a functioning container.


The projects above should show how simple it is to implement the interface in different languages with different data exchange formats in either a container or even in a bare metal http server. Aggregation and scaling of any container that implements the presented interface is managed, orchestrated and aggregated by our presented solution.


In addition to the orchestration the containerization makes the installation of a new NLP component as easy as using `docker pull nlpcomponent:0.1` and the framework will take care of the interaction with the component.