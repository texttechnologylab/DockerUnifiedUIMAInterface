FROM maven:3.8.5-openjdk-11
COPY . .
RUN mvn clean compile
CMD mvn exec:java -Dexec.mainClass="org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer"
