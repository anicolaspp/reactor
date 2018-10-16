package com.github.anicolaspp.sink

import com.github.anicolaspp.sink.data.OffsetRepository
import com.github.anicolaspp.sink.streams.Streamer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object App extends Streamer with OffsetRepository {
  def main(args: Array[String]): Unit = {

    val config = new SparkConf().setAppName("file sink")

    implicit val sparkSession = SparkSession.builder().config(config).getOrCreate()
    implicit val ssc = new StreamingContext(sparkSession.sparkContext, Milliseconds(500))

    val messages = getStream("/user/mapr/streams/click_stream:all_links_2", "/user/mapr/sink_offsets")


    saveOffsets(messages, "/user/mapr/sink_offsets")

    ssc.start()
    ssc.awaitTermination()
  }
}