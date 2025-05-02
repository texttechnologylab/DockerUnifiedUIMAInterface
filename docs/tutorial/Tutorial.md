---
layout: default
---

# Tutorial

This documentation is designed as a tutorial for the creation and integration of new NLP analysis tools to be used within the **Docker Unified UIMA Interface** (short DUUI).
For this purpose, the tutorial is structured in two parts: a general part describing DUUI and the global context as well as several concrete examples for creating your own components based on existing DUUI components.

> <img src="https://raw.githubusercontent.com/FortAwesome/Font-Awesome/6.x/svgs/solid/circle-info.svg" width="15" height="15"> **Note**
>
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

> <img src="https://raw.githubusercontent.com/FortAwesome/Font-Awesome/6.x/svgs/solid/triangle-exclamation.svg" width="15" height="15"> **Important**
>
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
>
> Generally, the complete [TypeSystem from Texttechnologylab](https://github.com/texttechnologylab/UIMATypeSystem) can be integrated in a DUUI tool. But this normally contains far more Types or Typesytems than might be needed for a specific tool. However, the idea of DUUI is that each component only returns the TypeSystem that can be created by the component.


### Pipeline
The DUUI Pipeline for a component starts with reading the document and ends with writing the annotations back to the document.
The graph below shows the flow of the pipeline. After reading the document, they are passed to a Lua script which extracts the needed information and passes it to the analysis tool. The analysis tool then returns the annotations which will be deserialized by the Lua script and written back to the document.
For a complete pipeline you can check the tutorials.

![](https://mermaid.ink/svg/pako:eNqlVm2r2jAU_ish-7KJuur1tYPB5crYhXvZ0MHGWpHMnmqhTVyasHnF_7406btWvSygbU7Oec7pkyenPeA18wDb2A_Zn_WWcIGe5h9citSI5a8NJ7st4owJx8XIxUuzkgwv4LAWAaMqoLDmMQ8s2rEY-CPdSeFkMxvpeQmmGSoZWdyMrWUEVDhzIB7wJep0PuaLxuZkPiVwoJ5LzxR3TykTJMmoMWiC7OLCinJz5ZEv15qjP0nylbM1xLGjbm202pnZqgZ1Ga4O-YlmoK2WL6kOarUugl9PkIyi3HSv7heK3y4SnNDYZzxCXUX2HH5LiMVnQr0Q-Hmkqk-CIXceEaABamnK-3K-mnwP3pY25htj4bum2NP8qaVWQLG7t5ZgsNR9DDexoYV3YmjWy0JJHFK1KEUHJAxeYIXeo5UHxfw_9bPIgGoKupjgZg0t8rOu9dMp6adj2FceX6RIXMxlITiQaNnA3qk54-mSMOox1ziZFfTWWLlC_M28zMDQov_TR07oSY9Gwc0MvvNAgObvlZwYPpVIS2AXRH7ikkemxZ7R7rWWmjXjdIeLhm8Mr-74mgvumEu13_94fnTUr7nPV18MFYoLp8xSgTbpCqdrmGmfqKCmtkbcnM5Y7EPQL1fkB2Fov_H9ykpeoVkFqK-WjnTq43lw4lOSeKNXVnNzpqqHQsBtvOGBh23BJbRxBDwiyRQfklgXiy1E4GJb3XrgExmqd6lLjypsR-hPxqIskjO52WLbJ2GsZuZYzAKiZBXlVq5IA_7AJBXYHo40BrYP-C-2e_3uYDTpWYO7UX8wGY-nd228x3Z_YnUH_ZHVH95NrVF_eGzjF52017Usq2eNx4PxdDgd9XrTNiZSsMWerrOSwAsE48_my0h_IB3_Ac4qvBo)


### Lua script
The Lua script is used as a communication interface between the annotation tool and the DUUI composer.
The script supports two distinct interfaces:

 - `serialize`/`deserialize`: a pair of complementary functions that represent a one-shot process where an entire CAS object is processed by the `serialize` function which writes the serialized representation as bytes into an output stream which in turn is sent to the Annotation Component, and a `deserialize` function that receives the response from the Annotation Component in a byte input stream and handles the updates to be applied to the CAS object.
 - `process`: a single function which has access to a Request Handler, allowing a developer to write more complex tranformation logic and make multiple HTTP requests to the Annotation Component.

For different examples of Lua scripts you can check our tutorials.


#### Sequence Diagram: `serialize`/`deserialize`
![](https://mermaid.ink/svg/pako:eNqVVF1vmzAU_SuWH6ZUIx8UQgKaIlV00iZtbVX6NPHiwk1qCWxmX2dLo_z32dBsNGNt5yeufc-5x0fH7GkhS6AJ1fDdgCjgkrONYnUuiF2sQL5lCORCCIkMuRQklXUjBQjsWoZOxqvV-y-GkaxQvMGEaFCcVfwRRulF5pFrg43BDBWw-qxjcetK2kFyC4r0sTcKxjdKFqA1eUc-Cif3w71a9UmSH4ojjEqG7InvD4PVMh7SmJChycOdqawqKJDcOo80kntZ7ogTQdZK1s_u84IpzpXewZ2Ulb3edXY3mm79adPdceq17GeviOvAJxudpCezQB_l_iXJ9Y7_7csnxOYWtC01jN4iZtCpDt9Jav3ignwWb_HpJDwlnMSnx_Jqei7B5aUPcYJS-_Es1Pp_U1PCi0-DenSjeEkTVAY8WoOqmSvp3sFzig9QQ04tGy1hzUyFOc3FwcIaJr5JWR-RSprNA03WrNK2Mo2N-PGF_t5VIEpQqTR2buLH8axlocme_rR1GEzCIFz6YTgP_PO5R3d2cxFMlnE0W5wHy0UUz4ODRx_bqbPJ0vdntjGOojDyF3HgUWZQZjtRHDVByVGqr91vo_17HH4Bmn5v9w)


#### Sequence Diagram: `process`
![](https://mermaid.ink/svg/pako:eNqNU91v2jAQ_1ese5hASyBhhRRrQurSSn3YR1X6tGUPJrlCpMSX2Q4aQ_zvsxNSIOu0-ck-3_0-fOc9pJQhcND4o0aZ4m0u1kqUiWR2idTkW2GQ3UhJRpicJIuprEiiNIlsk1678xeLtx9rwZapyivDWaUoRa0H8c3SY4-OSpt7IbMC1bBFKYgqRpWDEUWx48fAFhVbCZNuULd53fpMVlZzfc7zoNB_aLnYG3YnnblT3SnT6buUwXuyRp3kTBgxPGFcZvkO6OwBnogKq-LL8mkw3objI8bYYyvKdsO_OfgDoBdg71dqwY6-UHcaTnC9fKfK79u7N6Z6RG37o3HQk9MzZavPH7Wr2rNcGqYtUa1j-7DW1c7gt--NOXb4n_60DOxOKVLaubpF16IXCvbBQrl4rPBy7o7tR5m1m4te-q_NIGfdhGb4j0EGD9Yqz4AbVaMHJapSuCPsHUACZoMlJmARIcNnURcmgUQebFkl5FeisqtUVK83wJ9Foe2pruzwdD_qJaqsB1Qx1dIAv7qaNCDA9_ATeBhej8LZPIimQTiZBFHwzoOdDUfBaDqfzaL57DqaHjz41XAGI3vyQNSGljuZdiIwyw2pT-2_br734Te5nUsP)


# Docker

Docker is used to run the tool.
Docker containers are used to ensure that the analysis is performed in the same environment, regardless of the underlying host system.
Also, the Docker container is used to ensure that the analysis is reproducible, because every build of the Docker container is versioned, containing all needed code and data (e.g. model files).
The diagram below shows the structure of a Dockerfile:

![](https://mermaid.ink/svg/pako:eNp1kTtrwzAQgP_KoTleO3goxJYNHgqBkqnuoEhnR1Qv9Cg1If-9igSNly7HffqO03F3I9wKJC25KMu_mgtGNhsA4FYlbQK8FDp-zIRmjx4mzVacyScExzhCl83w42xAoOfzBCfr49P22U4mRKYUUHRoBBouMTwrSvua_hd3JeNjDlykQRhM9Juz0uy-G_b6W3prNO49zb63boNRqjJEaV0CHKFpXqGr0BXoK_QFaAVaYKgwFBjJgWj0mkmR93h7qJnEK-q8pjanAheWVB5jNvdcylK075vhpI0-4YF4m9YraRemQqbkBItIJVs903-vKGS0_q1eqhzs_guFyIvu)


# Rest Interface

The DUUI is designed to be used with REST interface, which is defined as:
![](https://mermaid.ink/svg/pako:eNpV0MtqwzAQBdBfEQPZJYRuXSiksZNuSkJtuqlKGaRJbLAe6FEwIf_eqUKKu9PMPTDiXkA5TVDBOaDvRVc_Srv5kPDWtJ2ET7FaPYlnnvdNJ9bfD-s0eYpTTGQ4ZVrAdga0U9mQTZgGZ2emnhnljMl2UMV8jThRmMmG5fHQ3qgPTlGMJV4stgXsGGysdXzDhX_JnpN3CvF--r5_4f3B_17DURwxoKHEjA0swVAwOGju4CKtEBJST4YkVPzUdMI8JgnSXpliTq6drIIqhUxLCC6fe6hOOEaesteYqB6QuzR_W9IDf_P11nIp-_oDP5h3lg)

The REST interface is used as a standardized way to communicate with the DUUI.
-  **GET /v1/typesystem** is the TypeSystem that is used by the analysis tool, which contains all UIMA types.
-  **GET /v1/documentation** is the documentation of the analysis tool, which should contain at least the annotator name and the version as fields. Optional parameters can be added.
-  **GET /v1/communication_layer** is the communication layer that is used by the analysis tool. This represents the Lua script, with the serialize and deserialize functions. In this way, DUUI enables the tool to supply its own communication layer that can be easily and dynamically executed in the DUUI context without having DUUI to "know" all tools beforehand.
-  **POST /v1/process** is the processing function that is used by the analysis tool. This function is called for every CAS document in the pipeline. Therefore, it is the main function of the analysis tool.


# Tutorials

We have prepared three tutorials with different levels of complexity. All tutorials use Python as the programming platform, however, all languages where a REST service can be created could be used as base for a DUUI tool.
![](https://mermaid.ink/svg/pako:eNo9kD1PwzAQhv-KdQNTOwBbBqSQCFHRLoROmOGwL42Ffa4cmw9V_e9cGqUeLN_73IffO4GJlqCC3scfM2DKavuqWcmp3zW8lRyTQ6_hQ63XD-pRtM6FoyfVEWcX5JrYjWqF1PYb2ZB6xkyqGch8zawR1sSp6lc9oclXNg_iW-Htfr9ZptRqAZeQ74RveMypmOwiL2l8L_pL-aTElGkUGVYQKAV0VhydpiYa8kCBNFTytNRj8fJhzWdJRTHX_bGBSjrTClIshwGqHv0oUTlasdE6PCQMV5Wsk43s5p1dVnf-B-NyZwk)

* [Simple Sentiment](Sentiment)
* [Advance Hate Check](HateCheck)
* [Complex Fact Check](FactChecking)
* [Lua `process` Interface](LuaProcess)

# Autors
- [Giuseppe Abrami](https://www.texttechnologylab.org/team/giuseppe-abrami/)
- [Mevlüt Bağcı](https://www.texttechnologylab.org/team/mevl%c3%bct-bagci/)
- [Daniel Baumartz](https://www.texttechnologylab.org/team/daniel-baumartz/)
- [Alexander Mehler](https://www.texttechnologylab.org/team/alexander-mehler/)
