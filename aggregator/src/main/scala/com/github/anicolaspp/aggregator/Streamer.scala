package com.github.anicolaspp.aggregator

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}

trait Streamer {
  def getMessagesStream()(implicit ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] =
    KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy)

  private def consumerStrategy =
    ConsumerStrategies.Subscribe[String, String](List("/user/mapr/streams/click_stream:all_links"), kafkaParameters)

  private def kafkaParameters = Map(
    ConsumerConfig.GROUP_ID_CONFIG -> "aggregators",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
  )
}