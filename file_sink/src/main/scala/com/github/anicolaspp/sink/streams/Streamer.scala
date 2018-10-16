package com.github.anicolaspp.sink.streams

import com.github.anicolaspp.sink.data.OffsetRepository
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}

trait Streamer extends OffsetRepository {
  def getStream(streamName: String, offsetsTable: String)(implicit ssc: StreamingContext, session: SparkSession) = {

    val offsets = getOffsets(offsetsTable)

    println(offsets)

    KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy(streamName, offsets))
  }

  private def consumerStrategy(streamName: String, offsets: Map[TopicPartition, Long]) =
    ConsumerStrategies.Subscribe[String, String](List(streamName), kafkaParameters, offsets)

  private lazy val kafkaParameters = Map(
    ConsumerConfig.GROUP_ID_CONFIG -> "sink",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
  )
}
