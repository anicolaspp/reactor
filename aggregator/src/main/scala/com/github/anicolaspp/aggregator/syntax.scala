package com.github.anicolaspp.aggregator

import com.mapr.db.spark.MapRDBSpark
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.DStream

object syntax {

  implicit class DStreamJsonOps(stream: DStream[(Link, Int)]) extends JsonSerializer {
    def mapToJsonDocument() =
      stream
        .map(toJson)
        .map(json => MapRDBSpark.newDocument(json))
  }

  implicit class DStreamOps(stream: DStream[Link]) {
    def countByLinkInWindow() =
      stream
        .map(link => (link.path, (1, link)))
        .reduceByKey(reduce)
        .map { case (_, (total, link)) => (link, total) }

    private def reduce(x: (Int, Link), y: (Int, Link)) = (x._1 + y._1, x._2)
  }

  implicit class DStreamRawOps(stream: DStream[ConsumerRecord[String, String]]) extends JsonSerializer {
    def getLinks() =
      stream
        .map(_.value)
        .map(fromJson)
  }
}
