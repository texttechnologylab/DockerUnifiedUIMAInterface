# DockerUnifiedUIMAInterface (DUUI)

To start the composer

# Java requirements
At the moment the composer only runs on Java version 11 and above. This limitation may be lifted in the future.

```
mvn clean compile exec:java -Dexec.mainClass="org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer"
```



# Small Docker Guide

Please note that you are not in a VPN network, this can influence or prevent the creation of the image.

To build a Docker DUUI image, follow these steps:

```
cd /path/of/your/Dockerfile

build -t IMAGENAME:VERSION .

docker image tag IMAGENAME:VERSION REPOSITORY/IMAGENAME:VERSION

docker push REPOSITORY/IMAGENAME:VERSION
```
After the build, the image can be used / tried locally and after pushing it to the remote repository, it can also be used from that repository.
