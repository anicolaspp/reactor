package com.github.anicolaspp.sink

import com.mapr.db.spark.MapRDBSpark
import com.mapr.db.spark.sql._
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import com.mapr.db.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.DStream

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


import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}

trait OffsetRepository {

  import Predef._

  def getOffsets(offsetsTable: String)(implicit session: SparkSession) = {
    import session.implicits._

    session.loadFromMapRDB(offsetsTable)
      .map { row =>
        val topic_partition = row.getAs[String](0).split("-")

        (topic_partition.head, topic_partition.tail.head.toInt, row.getAs[Double]("offset"))
      }
      .collect()
      .map { case (topic, partition, offset) => new TopicPartition(topic, partition) -> offset.toLong }
      .toMap
  }

  def saveOffsets(offsetStream: DStream[ConsumerRecord[String, String]], offsetsTable: String) =
    offsetStream
      .map(record => (s"${record.topic()}-${record.partition()}", record.offset()))
      .saveOffsets(offsetsTable)
}

object Predef {

  implicit class DStreamOps(offsetStream: DStream[(String, Long)]) extends Serializable {
    def saveOffsets(offsetsTable: String) =
      offsetStream
        .reduceByKey((a, b) => Math.max(a, b))
        .map(p => MapRDBSpark.newDocument(s"{\42_id\42 : \42${p._1}\42, \42offset\42: ${p._2}}"))
        .saveToMapRDB(offsetsTable)
  }

}

trait Streamer extends OffsetRepository {
  def getStream(streamName: String, offsetsTable: String)(implicit ssc: StreamingContext, session: SparkSession) = {

    val offsets = getOffsets(offsetsTable)

    println(offsets)

    KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy(streamName, offsets))
  }

  private def consumerStrategy(streamName: String, offsets: Map[TopicPartition, Long]) =
    ConsumerStrategies.Subscribe[String, String](List(streamName), kafkaParameters, offsets)

  private def kafkaParameters = Map(
    ConsumerConfig.GROUP_ID_CONFIG -> "sink",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
  )
}