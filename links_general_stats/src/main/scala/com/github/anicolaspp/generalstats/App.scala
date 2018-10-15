package com.github.anicolaspp.generalstats

import com.github.anicolaspp.generalstats.conf.Configuration
import com.github.anicolaspp.generalstats.data.Streamer
import com.github.anicolaspp.generalstats.ops.Ops._
import com.mapr.db.spark.sql._
import com.mapr.db.spark.{MapRDBSpark, _}
import org.apache.spark.sql._
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object App extends Streamer {

  def main(args: Array[String]): Unit = {
    val appConfig = Configuration.parse(args)

    val config = new SparkConf().setAppName(appConfig.appName)

    val sc = new SparkContext(config)
    val sparkSession = SparkSession.builder().config(config).getOrCreate()
    val ssc: StreamingContext = new StreamingContext(sc, Milliseconds(500))

    val messages = getStream(appConfig.stream)(ssc)

    messages
      .map(_.value())
      .map(MapRDBSpark.newDocument)
      .map(doc => (doc.getString(pathCol), 1))
      .reduceByKey(_ + _)
      .foreachRDD { rdd =>
        val rowRDD = rdd.map { case (path, sum) => Row(path, sum) }

        val rowDF = sparkSession.createDataFrame(rowRDD, streamSchema)

        val fromDBDF = sparkSession.loadFromMapRDB(appConfig.tableName, dbSchema)

        val joinDF = rowDF
          .join(fromDBDF, Seq(idCol), left_outer_join)
          .na
          .fill(0, Seq(totalCol))

        val finalDF = joinDF
          .withColumn(valueCol, joinDF(sumCol) + joinDF(totalCol))
          .select(idCol, valueCol)
          .withColumnRenamed(valueCol, totalCol)

        finalDF.saveToMapRDB(appConfig.tableName)                                             
      }

    ssc.start()
    ssc.awaitTermination()
  }
}