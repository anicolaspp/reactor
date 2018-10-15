package com.github.anicolaspp.generalstats

import com.github.anicolaspp.generalstats.data.Streamer
import com.mapr.db.spark.{MapRDBSpark, _}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.mapr.db.spark.sql._


object App extends Streamer {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("general stats")

    val sc = new SparkContext(config)
    implicit val ssc: StreamingContext = new StreamingContext(sc, Milliseconds(500))

    val messages = getStream("/user/mapr/streams/click_stream:all_links_2")


    val sparkSession = new SparkSession(sc)

    messages
      .map(_.value())
      .map(MapRDBSpark.newDocument)
      .map(doc => (doc.getString("path"), 1))
      .reduceByKey(_ + _)

      .foreachRDD { rdd =>
        val rowRDD = rdd.map { case (path, sum) => Row(path, sum) }

        val rowDF = sparkSession.createDataFrame(rowRDD, streamSchema)

        val fromDBRDD = sparkSession.loadFromMapRDB("/user/mapr/tables/total_counts", dbSchema)
        //          sc
        //          .loadFromMapRDB("/user/mapr/tables/total_counts")
        //          .map(d => (d.getString("_id"), d.getInt("total")))
        //          .map { case (path, sum) => Row(path, sum) }


        val joinDF = rowDF
          .join(fromDBRDD, Seq("_id"), "left_outer")
          .na
          .fill(0, Seq("total"))

        val finalDF = joinDF
          .withColumn("value", joinDF("sum") + joinDF("total"))
          .select("_id", "value")
          .withColumnRenamed("value", "total")
        
        finalDF.saveToMapRDB("/user/mapr/tables/total_counts")
      }

    ssc.start()
    ssc.awaitTermination()
  }

  private def dbSchema = {
    StructType(List(StructField("_id", DataTypes.StringType), StructField("total", DataTypes.IntegerType)))
  }

  private def streamSchema = {
    StructType(List(StructField("_id", DataTypes.StringType), StructField("sum", DataTypes.IntegerType)))
  }
}

