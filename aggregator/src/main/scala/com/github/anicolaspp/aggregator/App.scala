package com.github.anicolaspp.aggregator

import com.github.anicolaspp.aggregator.syntax._
import com.mapr.db.spark._
import org.apache.spark._
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object App extends Streamer {

  def main(args: Array[String]): Unit = {

    val config = new SparkConf().setAppName("Aggregator")

    val sc = new SparkContext(config)
    implicit val ssc: StreamingContext = new StreamingContext(sc, Milliseconds(500))

    println("running...")
    
    val messageStream = getMessagesStream()

    messageStream
      .getLinks()
      .countByLinkInWindow()
      .mapToJsonDocument()
      .foreachRDD(_.saveToMapRDB("/user/mapr/tables/link_aggregates"))

    ssc.start()
    ssc.awaitTermination()
  }
}



