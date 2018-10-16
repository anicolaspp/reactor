package com.github.anicolaspp.sink.data

import com.github.anicolaspp.sink.Predef
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

import com.mapr.db.spark.sql._

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
