[![](https://jitpack.io/v/texttechnologylab/DockerUnifiedUIMAInterface.svg)](https://jitpack.io/#texttechnologylab/DockerUnifiedUIMAInterface)
[![version](https://img.shields.io/github/license/texttechnologylab/DockerUnifiedUIMAInterface)]()
[![latest](https://img.shields.io/github/v/release/texttechnologylab/DockerUnifiedUIMAInterface)]()
[![Conference](http://img.shields.io/badge/conference-FindingsEMNLP--2023-4b44ce.svg)](https://2023.emnlp.org/)

# Docker Unified UIMA Interface (DUUI)
![img|320x271](https://github.com/texttechnologylab/DockerUnifiedUIMAInterface/blob/main/DUUI_Logo.png)

Automatic analysis of large text corpora is a complex task. This complexity particularly concerns the question of time efficiency. Furthermore, efficient, flexible, and extensible textanalysis requires the continuous integration of every new text analysis tools. Since there are currently, in the area of NLP and especially in the application context of UIMA, only very few to no adequate frameworks for these purposes, which are not simultaneously outdated or can no longer be used for security reasons, this work will present a new approach to fill this gap. To this end, we present Docker Unified UIMA Interface (DUUI), a scalable, flexible, lightweight, and featurerich framework for automated and distributed analysis of text corpora that leverages experience in Big Data analytics and virtualization with Docker.

## Features
Using DUUI, NLP preprocessing on texts can be performed using the following features:
* Horizontal and vertical scaling
* Capturing heterogeneous annotation landscapes
* Capturing heterogeneous implementation landscapes
* Reproducible & reusable annotations
* Monitoring and error-reporting
* Lightweight usability

## Functions
DUUI has different components which are distinguished into Drivers and Components. 

### Components
Components represent the actual analysis methods for recognizing (among others) tokens, named entities, POS and other ingredients of the NLP. All components must be analysis methods in the definition of UIMA. Of course, existing analysis methods based on Java can also be used directly (e.g. dkpro).

Independently of this, Components can also be implemented in alternative programming languages, as long as the interface of DUUI is used, they can be targeted and used. 

#### Current implementations

### Driver
DUUI has a variety of drivers that enable communication as well as the execution of Components in different runtime environments.

#### UIMADriver

#### DockerDriver

#### RemoteDriver

#### SwarmDriver

#### KubernetesDriver


## Requirements
![Java](https://img.shields.io/badge/Java-11-blue)
![Docker](https://img.shields.io/badge/Docker-20.10-green)

### Java requirements
DUUI has its own TypeSystem, which is required for use. To generate this an initial ```mvn compile``` is necessary:

```bash
mvn clean compile exec:java -Dexec.mainClass="org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer"
```

## Using
There are basically two ways to use DUUI for preprocessing texts:
 * Clone the GitHub project.
 * Include the GitHub project using JitPack via maven (Recommended).

## Using JitPack
Add the following to your pom file:
```xml
<repositories>
  <repository>
      <id>jitpack.io</id>
      <url>https://jitpack.io</url>
  </repository>
</repositories>
```
After that DUUI can be integrated as a dependency:

```xml
<dependency>
  <groupId>com.github.texttechnologylab</groupId>
  <artifactId>DockerUnifiedUIMAInterface</artifactId>
  <version>1.0</version>
</dependency>
```

## Use with Java

```java
int iWorkers = 2; // define the number of workers

JCas jc = JCasFactory.createJCas(); // A empty CAS document is defined.

// load content into jc ...

// Defining LUA-Context for communication
DUUILuaContext ctx = LuaConsts.getJSON();

// Defining a storage backend based on SQlite.
DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("loggingSQlite.db")
            .withConnectionPoolSize(iWorkers);

// The composer is defined and initialized with a standard Lua context as well with a storage backend.
DUUIComposer composer = new DUUIComposer().withLuaContext(ctx)
                        .withScale(iWorkers).withStorageBackend(sqlite);
                
// Instantiate drivers with options (example)
DUUIDockerDriver docker_driver = new DUUIDockerDriver()
        .withTimeout(10000);
DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);
DUUIUIMADriver uima_driver = new DUUIUIMADriver().withDebug(true);
DUUISwarmDriver swarm_driver = new DUUISwarmDriver();

// A driver must be added before components can be added for it in the composer. After that the composer is able to use the individual drivers.
composer.addDriver(docker_driver, remote_driver, uima_driver, swarm_driver);

// A new component for the composer is added
composer.add(new DUUIDockerDriver.
    Component("docker.texttechnologylab.org/gnfinder:latest")
    .withScale(iWorkers)
    // The image is reloaded and fetched, regardless of whether it already exists locally (optional)
    .withImageFetching());
    
// Adding a UIMA annotator for writing the result of the pipeline as XMI files.
composer.add(new DUUIUIMADriver.Component(
                createEngineDescription(XmiWriter.class,
                        XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                )).withScale(iWorkers));

// The document is processed through the pipeline. In addition, files of entire repositories can be processed.
composer.run(jc);
```

## Create your own DUUI images

# Small Docker Guide

Please note that you are not in a VPN network, this can influence or prevent the creation of the image.

To build a Docker DUUI image, follow these steps:

```xml
cd /path/of/your/Dockerfile

build -t IMAGENAME:VERSION .

docker image tag IMAGENAME:VERSION REPOSITORY/IMAGENAME:VERSION

docker push REPOSITORY/IMAGENAME:VERSION
```
After the build, the image can be used / tried locally and after pushing it to the remote repository, it can also be used from that repository.


# Cite
If you want to use the project please quote this as follows:

A. Leonhardt, G. Abrami, D. Baumartz, and A. Mehler, “Unlocking the Heterogeneous Landscape of Big Data NLP with DUUI,” in Findings of the Association for Computational Linguistics: EMNLP 2023, 2023, pp. 1-15 (accepted)

## BibTeX
```
@inproceedings{Leonhardt:et:al:2023,
  title = {Unlocking the Heterogeneous Landscape of Big Data {NLP} with {DUUI}},
  author = {Leonhardt, Alexander and Abrami, Giuseppe and Baumartz, Daniel and Mehler, Alexander},
  year = {2023},
  booktitle = {Findings of the Association for Computational Linguistics: EMNLP 2023},
  publisher = {Association for Computational Linguistics},
  pages = {1--15},
  note = {accepted}
}
```
