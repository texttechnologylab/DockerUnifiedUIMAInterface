---
layout: default
---

# Tutorial

This documentation is designed as a tutorial for the creation and integration of new NLP analysis tools to be used within the **Docker Unified UIMA Interface** (short DUUI).
For this purpose, the tutorial is structured in two parts: a general part describing DUUI and the global context as well as several concrete examples for creating your own components based on existing DUUI components.

> <img src="https://raw.githubusercontent.com/FortAwesome/Font-Awesome/6.x/svgs/solid/circle-info.svg" width="15" height="15"> **Note**
> We recommend reading the following publication before using the tutorial: [![EMNLP-2023](http://img.shields.io/badge/paper-FindingsEMNLP--2023-fb44ce.svg)](https://aclanthology.org/2023.findings-emnlp.29)

# DUUI in a nutshell

## What is DUUI?
**Docker Unified UIMA Interface** (DUUI) is a [UIMA](https://uima.apache.org/)-based framework for the unified and distributed use of natural language processing (NLP) methods utilizing micro-services such as [Docker](https://www.docker.com/) or [Kubernetes](https://kubernetes.io/).
Through DUUI, flexible pipelines for the analysis of unstructured information, currently texts, can be processed based on documents. The processing can be distributed both, horizontally (on several nodes) as well as vertically (several times on the nodes), using a cluster solution for Docker or within Kubernetes.

DUUI is implemented in Java and can be used in this way, whereby a [web-based solution](https://duui.texttechnologylab.org/) for encapsulation is currently also being recommended; DUUI essentially consists of three elements:
- **Composer**

  It controls the start of the **Component**s (which perform the actual analysis(es)) - and terminates them as well - and is also responsible among other things for performing the analysis.
- **Driver**

  DUUI contains a set of **Driver**s which provides an interface for the execution of **Component**s in a respective runtime environment (Docker, Docker Swarm, Kubernetes, Java Runtime Environment, etc.).

- **Component**

  A **Component** encapsulates a defined NLP analysis within a Docker image, which is used for analysis as a Docker container independently and autonomously as an instance for analyzing documents as part of a pipeline.
  This guide is intended to explain the creation of a **Component**.

Further information as well as the DUUI project, which can be reused under the ![GitHub License](https://img.shields.io/github/license/Texttechnologylab/DockerUnifiedUIMAInterface), can be found in the [![publication](http://img.shields.io/badge/paper-FindingsEMNLP--2023-fb44ce.svg)](https://aclanthology.org/2023.findings-emnlp.29)  as well as in the [![GitHub repository](https://img.shields.io/badge/GitHub_repository-blue)](https://github.com/texttechnologylab/DockerUnifiedUIMAInterface).

> <img src="https://raw.githubusercontent.com/FortAwesome/Font-Awesome/6.x/svgs/solid/info-circle.svg" width="15" height="15"> **Important**
> For using DUUI as a Java application, a JRE of at least 17 is required.

## For whom is DUUI?
DUUI is designed as a general framework for the uniform analysis of unstructured information (text, video, audio and images) and is intended to be used in a variety of disciplines.
Above all, the straightforward extension of new analysis methods, the completeness of the resulting data as a UIMA document, the reusability in databases and as well as the flexibility of the annotation format in combination with a [web-based interface](https://duui.texttechnologylab.org/) should streamline, standardize and improve the utilization of NLP processes.

However, since the usability of DUUI goes hand in hand with the amount of available components and since different analysis methods exist for various applications and research, the development of individual analysis methods as **DUUI-Component**s represents a core task for users.

# In medias res
With the basics now explained in a nutshell, the tutorial can begin, whereby we also refer to existing **DUUI-Components** that are already available and which can all be used as [![blueprints](https://img.shields.io/badge/blueprints-blue)](https://github.com/texttechnologylab/duui-uima) for new components.

## The creation of DUUI components
In order to create a DUUI component, three building blocks are required, which are explained in detail in the following instructions

TypeSystem | Lua-Script | Programm |
--- | --- | --- |
The TypeSystem (as part of UIMA) defines the schema of the types of annotations which are created by the component. This TypeSystem is necessary for DUUI to understand the annotated document. | The Lua script enables any component to (de)serialize annotations from a UIMA document in a programming language-independent approach without the need for native UIMA support in the respective programming language. | The analysis is a script / program which operates as a REST interface and uses or reuses an existing analysis or an existing program to perform the actual NLP analysis.  |

> <img src="https://raw.githubusercontent.com/FortAwesome/Font-Awesome/6.x/svgs/solid/lightbulb.svg" width="15" height="15"> **Tip**
> Generally, the complete [TypeSystem from Texttechnologylab](https://github.com/texttechnologylab/UIMATypeSystem) can be integrated in a DUUI tool. But this normally contains far more Types or Typesytems than might be needed for a specific tool. However, the idea of DUUI is that each component only returns the TypeSystem that can be created by the component.

### Pipeline
The DUUI Pipeline for a component starts with reading the document and ends with writing the annotations back to the document.
The graph below shows the flow of the pipeline. After reading the document, they are passed to a Lua script which extracts the needed information and passes it to the analysis tool. The analysis tool then returns the annotations which will be deserialized by the Lua script and written back to the document.
For a complete pipeline you can check the tutorials.
[![](https://mermaid.ink/img/pako:eNqFUUtrwzAM_itG52Y_IINCmwcMsks22CPuwYuVxhDbwbEZW-l_n5KwtOkO00nW9xLyCWorEWJoOvtZt8J5VpT33DCqIXwcnehbtjPGeuGVNSyxurcGjZ8pY-UVhyvGs7UdhwO7EDIiFEHQcJ6hkTcBk-uA7qJZoHcSs0X6W1I5rKe4olwjuyq1ddC04YFF0ZbtqxKFRHdjsOywCnv7J6ws1khSvTjlyXyKSqvXx4cr7ZKxNH_X3rPojqTZ_MrIh9EdlOjUN06m-YzkIyJxjV2ptiyBDWh0WihJ33kaIQ6-RY0cYmolNiJ0ngM3Z6KK4O3Tl6kh9i7gBpwNxxbiRnQDvUIvhcdUCbqLninnH4ldoPU?type=png)](https://mermaid.live/edit#pako:eNqFUUtrwzAM_itG52Y_IINCmwcMsks22CPuwYuVxhDbwbEZW-l_n5KwtOkO00nW9xLyCWorEWJoOvtZt8J5VpT33DCqIXwcnehbtjPGeuGVNSyxurcGjZ8pY-UVhyvGs7UdhwO7EDIiFEHQcJ6hkTcBk-uA7qJZoHcSs0X6W1I5rKe4olwjuyq1ddC04YFF0ZbtqxKFRHdjsOywCnv7J6ws1khSvTjlyXyKSqvXx4cr7ZKxNH_X3rPojqTZ_MrIh9EdlOjUN06m-YzkIyJxjV2ptiyBDWh0WihJ33kaIQ6-RY0cYmolNiJ0ngM3Z6KK4O3Tl6kh9i7gBpwNxxbiRnQDvUIvhcdUCbqLninnH4ldoPU)


### Lua script
The Lua script is used as a communication interface between the annotation tool and the DUUI composer.
The script contains two parts, the first being the `serialize` function that is called to transform the CAS object into a stream that is sent to the annotation tool.
The second part is the `deserialize` function that is utilized to transform the output of the annotation tool back into the CAS object.
For different examples of Lua scripts you can check our tutorials.
[![](https://mermaid.ink/img/pako:eNqNU9luwyAQ_BXEc9IPcKVKjnNVclTJSdXD5IGadYxkwMKgqo3y78VGoY578sTu7O4MAxxxoRjgCJe1ei0qqg1Ks2sikVutfTlo2lQollIZariSKFGiURKk8SUIrXOCB_hOqZrgPTrDYUZ6H4fkBdCC5rTm7zBEu8W4hqIfmmZjbOFYk3iLiAN2msq2VFpweeiop9MbtMzvrGms2RoNVOyH7SDZtzoY_ENJlo6xlVPyoLkBRIMLTsUPjGEbNoG_d9ZJ-OwM0LPjQKOhv_kT53NVWOFuybsxyzOgDPRowIUTgezpD7KxBUneH197qnn-uLkd9H498FfZMzS9cq0LHy19tPbR2kXI3yXq0iufXvmiBE-wAC0oZ-4JHzuMYFOBAIIjt2VQUlsbgok8uVJqjdq-yQJHRluYYK3socJRSevWRbZh1MCcU-eDOJcA40bpjf8j_Vc5fQA4R-qm?type=png)](https://mermaid.live/edit#pako:eNqNU9luwyAQ_BXEc9IPcKVKjnNVclTJSdXD5IGadYxkwMKgqo3y78VGoY578sTu7O4MAxxxoRjgCJe1ei0qqg1Ks2sikVutfTlo2lQollIZariSKFGiURKk8SUIrXOCB_hOqZrgPTrDYUZ6H4fkBdCC5rTm7zBEu8W4hqIfmmZjbOFYk3iLiAN2msq2VFpweeiop9MbtMzvrGms2RoNVOyH7SDZtzoY_ENJlo6xlVPyoLkBRIMLTsUPjGEbNoG_d9ZJ-OwM0LPjQKOhv_kT53NVWOFuybsxyzOgDPRowIUTgezpD7KxBUneH197qnn-uLkd9H498FfZMzS9cq0LHy19tPbR2kXI3yXq0iufXvmiBE-wAC0oZ-4JHzuMYFOBAIIjt2VQUlsbgok8uVJqjdq-yQJHRluYYK3socJRSevWRbZh1MCcU-eDOJcA40bpjf8j_Vc5fQA4R-qm)



# Docker

Docker is used to run the tool.
Docker containers are used to ensure that the analysis is performed in the same environment, regardless of the underlying host system.
Also, the Docker container is used to ensure that the analysis is reproducible, because every build of the Docker container is versioned, containing all needed code and data (e.g. model files).
The diagram below shows the structure of a Dockerfile:

[![](https://mermaid.ink/img/pako:eNp1kTtrwzAQgP_KoTleO3goxJYNHgqBkqnuoEhnR1Qv9Cg1If-9igSNly7HffqO03F3I9wKJC25KMu_mgtGNhsA4FYlbQK8FDp-zIRmjx4mzVacyScExzhCl83w42xAoOfzBCfr49P22U4mRKYUUHRoBBouMTwrSvua_hd3JeNjDlykQRhM9Juz0uy-G_b6W3prNO49zb63boNRqjJEaV0CHKFpXqGr0BXoK_QFaAVaYKgwFBjJgWj0mkmR93h7qJnEK-q8pjanAheWVB5jNvdcylK075vhpI0-4YF4m9YraRemQqbkBItIJVs903-vKGS0_q1eqhzs_guFyIvu?type=png)](https://mermaid.live/edit#pako:eNp1kTtrwzAQgP_KoTleO3goxJYNHgqBkqnuoEhnR1Qv9Cg1If-9igSNly7HffqO03F3I9wKJC25KMu_mgtGNhsA4FYlbQK8FDp-zIRmjx4mzVacyScExzhCl83w42xAoOfzBCfr49P22U4mRKYUUHRoBBouMTwrSvua_hd3JeNjDlykQRhM9Juz0uy-G_b6W3prNO49zb63boNRqjJEaV0CHKFpXqGr0BXoK_QFaAVaYKgwFBjJgWj0mkmR93h7qJnEK-q8pjanAheWVB5jNvdcylK075vhpI0-4YF4m9YraRemQqbkBItIJVs903-vKGS0_q1eqhzs_guFyIvu)


# Rest Interface

The DUUI is designed to be used with REST interface, which is defined as:
[![](https://mermaid.ink/img/pako:eNpV0MtqwzAQBdBfEQPZJYRuXSiksZNuSkJtuqlKGaRJbLAe6FEwIf_eqUKKu9PMPTDiXkA5TVDBOaDvRVc_Srv5kPDWtJ2ET7FaPYlnnvdNJ9bfD-s0eYpTTGQ4ZVrAdga0U9mQTZgGZ2emnhnljMl2UMV8jThRmMmG5fHQ3qgPTlGMJV4stgXsGGysdXzDhX_JnpN3CvF--r5_4f3B_17DURwxoKHEjA0swVAwOGju4CKtEBJST4YkVPzUdMI8JgnSXpliTq6drIIqhUxLCC6fe6hOOEaesteYqB6QuzR_W9IDf_P11nIp-_oDP5h3lg?type=png)](https://mermaid.live/edit#pako:eNpV0MtqwzAQBdBfEQPZJYRuXSiksZNuSkJtuqlKGaRJbLAe6FEwIf_eqUKKu9PMPTDiXkA5TVDBOaDvRVc_Srv5kPDWtJ2ET7FaPYlnnvdNJ9bfD-s0eYpTTGQ4ZVrAdga0U9mQTZgGZ2emnhnljMl2UMV8jThRmMmG5fHQ3qgPTlGMJV4stgXsGGysdXzDhX_JnpN3CvF--r5_4f3B_17DURwxoKHEjA0swVAwOGju4CKtEBJST4YkVPzUdMI8JgnSXpliTq6drIIqhUxLCC6fe6hOOEaesteYqB6QuzR_W9IDf_P11nIp-_oDP5h3lg)

```
The REST interface is used as a standardized way to communicate with the DUUI.
-  **GET /v1/typesystem** is the TypeSystem that is used by the analysis tool, which contains all UIMA types.
-  **GET /v1/documentation** is the documentation of the analysis tool, which should contain at least the annotator name and the version as fields. Optional parameters can be added.
-  **GET /v1/communication_layer** is the communication layer that is used by the analysis tool. This represents the Lua script, with the serialize and deserialize functions. In this way, DUUI enables the tool to supply its own communication layer that can be easily and dynamically executed in the DUUI context without having DUUI to "know" all tools beforehand.
-  **POST /v1/process** is the processing function that is used by the analysis tool. This function is called for every CAS document in the pipeline. Therefore, it is the main function of the analysis tool.


# Tutorials

We have prepared three tutorials with different levels of complexity. All tutorials use Python as the programming platform, however, all languages where a REST service can be created could be used as base for a DUUI tool.
[![](https://mermaid.ink/img/pako:eNptkDFvwyAQhf8KuiVLIu-ui5Taqrp0craQ4QJnG8Vgi0LVKsp_75lWHiozIHh3993Tu4OeDEEJfcB5EKfmSXnB53g-pTgFi-PlcJAv5wrFEKh73sU_uWjJR-v42snqKlvr5pHEKlbFVVYFyssWrtnAvWGkeiB9y7ij-USvSSyqyPIK3DRYbxBfUcc8an2fofW0ePwSS-E_FPbgKDi0hrO4LysUxIEcKSj5aajDNEYFyj-4FXlH--01lDEk2kOYUj9A2eH4wb80G7bdWORM3aqSsWzs_TftHPrjB1xuf0M?type=png)](https://mermaid.live/edit#pako:eNptkDFvwyAQhf8KuiVLIu-ui5Taqrp0craQ4QJnG8Vgi0LVKsp_75lWHiozIHh3993Tu4OeDEEJfcB5EKfmSXnB53g-pTgFi-PlcJAv5wrFEKh73sU_uWjJR-v42snqKlvr5pHEKlbFVVYFyssWrtnAvWGkeiB9y7ij-USvSSyqyPIK3DRYbxBfUcc8an2fofW0ePwSS-E_FPbgKDi0hrO4LysUxIEcKSj5aajDNEYFyj-4FXlH--01lDEk2kOYUj9A2eH4wb80G7bdWORM3aqSsWzs_TftHPrjB1xuf0M)

* [Simple Sentiment](Sentiment)
* [Advance Hate Check](HateCheck)
* [Complex Fact Check](FactChecking)

# Autors
- [Giuseppe Abrami](https://www.texttechnologylab.org/team/giuseppe-abrami/)
- [Mevlüt Bağcı](https://www.texttechnologylab.org/team/mevl%c3%bct-bagci/)
- [Daniel Baumartz](https://www.texttechnologylab.org/team/daniel-baumartz/)
- [Alexander Mehler](https://www.texttechnologylab.org/team/alexander-mehler/)
