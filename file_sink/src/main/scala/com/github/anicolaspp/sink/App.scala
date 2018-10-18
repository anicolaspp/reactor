package com.github.anicolaspp.sink

import com.github.anicolaspp.sink.conf.Configuration
import com.github.anicolaspp.sink.data.OffsetRepository
import com.github.anicolaspp.sink.data.Ops._
import com.github.anicolaspp.sink.streams.Predef._
import com.github.anicolaspp.sink.streams.Streamer
import com.mapr.db.spark.MapRDBSpark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object App extends Streamer with OffsetRepository {
  def main(args: Array[String]): Unit = {

    implicit val appConfig: Configuration = Configuration.parse(args)

    val sparkConfig = new SparkConf().setAppName(appConfig.appName)

    implicit val sparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()
    
    implicit val ssc = new StreamingContext(sparkSession.sparkContext, Milliseconds(500))

    val messages = getStream(appConfig.inputStream, appConfig.offsetsTable)

    val jsonStream = messages
      .map(_.value())
      .map(MapRDBSpark.newDocument)

    jsonStream
      .map(createIndex)
      .writeToFileSystem()
      .indexByTime()
      .indexByHot()

    saveOffsets(messages, appConfig.offsetsTable)

    ssc.start()
    ssc.awaitTermination()
  }
}

