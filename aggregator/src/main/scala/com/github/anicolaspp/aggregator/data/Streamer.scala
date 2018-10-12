package com.github.anicolaspp.aggregator.data

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}

trait Streamer {
  def getStream(streamName: String)(implicit ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] =
    KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy(streamName))

  private def consumerStrategy(streamName: String) =
    ConsumerStrategies.Subscribe[String, String](List(streamName), kafkaParameters)

  private def kafkaParameters = Map(
    ConsumerConfig.GROUP_ID_CONFIG -> "aggregators",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true"
  )
}
