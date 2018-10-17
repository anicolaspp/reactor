# File Sink

**File Sink** is in charge or reading the message input stream (MapR-ES) and write to MapR-XD every single message. 

MapR-ES is able to save the offsets of the message being read so consumers can use the provided offsets. However, for this particular use case, we are going to commit the offsets to MapR-DB only when the corresponding messages have been written the MapR-XD.

Every time **File Sink** stars, it reads the offsets from MapR-DB and then starts consuming MapR-ES from that point on. In this way, we guarantee that we are not missing any data in MapR-XD.   

![](./offsets.PNG)

At the same time, files written in MapR-XD have not apparent order or relationship. MapR-DB is use has index for the file system. In this way we can read each file on MapR-XD based on a predefined index. 

## Index By Time

**File Sink** indexes each written file to the MapR-XD by time and writes to the index table in the following format.

```sql
{ _id: path_to_file_sytem, time: {...} }
```

![](./indexbytime.PNG)

Based on this index, we can query MapR-DB and select the paths of the files with a specific time and then query the specific files.

```
find /tables/index_table --fields _id --where {"$eq":{"time.hour":10}}
...

find /tables/index_table --fields _id --where {"$eq":{"time.day":24}}
...

find /tables/index_table --fields _id --where {"$and": [{"$eq":{"time.day":24}}, {{"$eq":{"time.hour":12}}}]}
...
```

File on MapR-XD can be re-indexed once a new index is introduced by reading all files and then applying the indexing strategy.      
## How to run

In order to get an executable bundle, use run the following command.

```scala
sbt clean compile assembly
```

This command will generate a file with the following arguments:

```scala
assemblyJarName := s"${name.value}-${version.value}.jar"
```

The resultan file should be

```scala
file_sinker-1.0.0.jar
```

This `jar` contains everything needed to run as an Apache Spark application within the MapR cluster. 

Copy the file to your cluster and then run the following command to submit the job to spark. 

```scala
spark-submit --class "com.github.anicolaspp.sink.App" --deploy-mode client ~/file_sinker-1.0.0.jar 
```

## General Architecture

![](architecture.PNG)

**File Sink** is one of the modules on the [Reactor Sytem](https://github.com/anicolaspp/reactor/)
