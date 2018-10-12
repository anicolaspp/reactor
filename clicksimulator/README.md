# Click Simulator

**Click Simulator** is a way to simulate traffic on a web site.

The links that are randomly generated are written to **MapR Streams**. 

This application can be executed anywhere where the JVM is available. 

```shell
mvn clean compile
mvn package

java -jar /Users/nperez/IdeaProjects/reactor/clicksimulator/target/clicksimulator-1.0.0-SNAPSHOT.jar
```

![](./c-gen.jpg) 
