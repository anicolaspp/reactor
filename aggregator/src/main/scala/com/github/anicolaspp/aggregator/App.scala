package com.github.anicolaspp.aggregator


import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import play.api.libs.json._

import com.mapr.db.spark._

object App {

  def main(args: Array[String]): Unit = {


    val config = new SparkConf()
      .setAppName("Aggregator")
    //      .set("spark.streaming.kafka.consumer.poll.ms", "500")

    val sc = new SparkContext(config)

    println("running...")

    implicit val ssc: StreamingContext = new StreamingContext(sc, Milliseconds(500))

    val messages = getMessagesStream()


    val links = messages
      .map(record => record.value)
      .map(json => linkReads.reads(Json.parse(json)).asOpt.get)


    links.map(_.path).co



//      .foreachRDD { rdd =>
//          .saveToMapRDB("/user/mapr/tables/link_aggregates", true)
//      }

    ssc.start()
    ssc.awaitTermination()
  }

  private def getMessagesStream()(implicit ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] =
    KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy)

  private def consumerStrategy =
    ConsumerStrategies.Subscribe[String, String](List("/user/mapr/streams/click_stream:all_links"), kafkaParameters)

  private def kafkaParameters = Map(
    ConsumerConfig.GROUP_ID_CONFIG -> "aggregators",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
  )

  case class Link(path: String, hot: Boolean)


  implicit val linkReads = Json.format[Link]
}


