package com.github.anicolaspp.generalstats

import com.github.anicolaspp.generalstats.data.Streamer
import com.mapr.db.spark.{MapRDBSpark, _}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.SparkSession


object App extends Streamer {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("general stats")

    val sc = new SparkContext(config)
    implicit val ssc: StreamingContext = new StreamingContext(sc, Milliseconds(500))

    val messages = getStream("/user/mapr/streams/click_stream:all_links_2")

    val sparkSession = SparkSession.builder().getOrCreate()

    messages
      .map(_.value())
      .map(MapRDBSpark.newDocument)
      .map(doc => (doc.getString("path"), 1))
      .reduceByKey(_ + _)

      .foreachRDD { rdd =>
        val m = rdd.map { case (path, sum) => Row(path, sum) }

        val df = sparkSession.createDataFrame(m, StructType(List(StructField("_id", DataTypes.StringType), StructField("sum", DataTypes.IntegerType))))

        val fromDBRDD = sc
          .loadFromMapRDB("/user/mapr/tables/total_counts")
          .map(d => (d.getString("_id"), d.getInt("total")))
          .map { case (path, sum) => Row(path, sum) }

        val fromDBDF = sparkSession.createDataFrame(fromDBRDD, StructType(List(StructField("_id", DataTypes.StringType), StructField("total", DataTypes.IntegerType))))


        val join = df.join(fromDBDF, Seq("_id"), "left_outer").na.fill(0, Seq("total"))

        val finalDF = join
          .withColumn("value", join("sum") + join("total"))
          .select("_id", "value")
          .withColumnRenamed("value", "total")

//        finalDF.write.
      }

    ssc.start()
    ssc.awaitTermination()
  }

}

