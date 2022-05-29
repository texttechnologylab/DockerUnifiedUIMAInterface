# DockerUnifiedUIMAInterface (DUUI)

To start the composer
```
mvn clean compile exec:java -Dexec.mainClass="org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer"
```



# Small Docker Guide

To build an DUUI-Image

```
cd /path/of/your/Dockerfile

build -t IMAGENAME:VERSION .

docker image tag IMAGENAME:VERSION REPOSITORY/IMAGENAME:VERSION

docker push REPOSITORY/IMAGENAME:VERSION

```
After the build, the image can be used / tried locally and after pushing it to the remote repository, it can also be used from that repository.
