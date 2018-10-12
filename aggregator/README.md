# Aggregator

**Aggregator** is a click stream consumer that runs on top of Apache Spark. It is in charge of counting many times each link appers in a time window.
The calculated stats are then save to *MapR-DB*.

Use the following command to create an Spark Executable.

```shell
sbt assembly 
```

After this, the resultant assembly is ready to be submitted as an Spark Application.

By default, **Aggregator** reads from an *MapR Stream* called **/user/mapr/streams/click_stream:all_links** on the MapR Cluster and write to MapR table called **/user/mapr/tables/link_aggregates**.

**Aggregator** is one of the many *View Materializer* on the [Reactor Sytem](https://github.com/anicolaspp/reactor/)
