package com.github.anicolaspp.aggregator

import com.github.anicolaspp.aggregator.conf.Configuration
import com.github.anicolaspp.aggregator.data.{JsonSerializer, Streamer}
import com.github.anicolaspp.aggregator.ops.Ops
import com.mapr.db.spark._
import org.apache.spark._
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object App extends Streamer {

  def main(args: Array[String]): Unit = {

    val argsConfiguration = Configuration.parse(args)

    val config = new SparkConf().setAppName(argsConfiguration.appName)

    val sc = new SparkContext(config)
    implicit val ssc: StreamingContext = new StreamingContext(sc, Milliseconds(500))

    println("running...")
    
    val messageStream = getStream(argsConfiguration.stream)

    messageStream
      .map(_.value)
      .map(JsonSerializer.fromJson)
      .map(Ops.toCountableLink)
      .reduceByKey(Ops.reduceCountableLinks)
      .map(Ops.toLinkCount)
      .map(JsonSerializer.toJson)
      .map(MapRDBSpark.newDocument)
      .foreachRDD(_.saveToMapRDB(argsConfiguration.tableName))

    ssc.start()
    ssc.awaitTermination()
  }
}

