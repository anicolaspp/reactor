package com.github.anicolaspp.sink.data

import com.mapr.db.spark.sql._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

trait OffsetRepository {

  import com.github.anicolaspp.sink.streams.Predef._

  def getOffsets(offsetsTable: String)(implicit session: SparkSession): Map[TopicPartition, Long] = {
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

  def saveOffsets[A, B](offsetStream: DStream[ConsumerRecord[A, B]], offsetsTable: String) =
    offsetStream
      .map(record => (s"${record.topic()}-${record.partition()}", record.offset()))
      .saveOffsets(offsetsTable)
}
