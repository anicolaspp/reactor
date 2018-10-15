# links_general_stats

**links_general_stats** is an Apache Spark application that calculates the total count for links on the click stream

This project shows how to join a live stream of data running on MapR-ES and a table from MapR-DB in order to update the same table. At the same time is solves a complicated issue with when async inserts are executed again the table on MapR-DB. Instead of running disjoint inserts, **links_general_stats** generates an updated view of the global counts and then update the data on the MapR-DB table with the calculated global view. 

All the calculation are done on the DataFrame and DataSet APIs since for this particular purpose, working with the RDD API is extremely complex. 

The following image shows how the data flows from the stream and the database and then join to create the global view that is then save back the MapR-DB.

![](./architecture.PNG)
