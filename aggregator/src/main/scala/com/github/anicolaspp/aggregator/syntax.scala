package com.github.anicolaspp.aggregator

import com.mapr.db.spark.MapRDBSpark
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.DStream
import play.api.libs.json.Json

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
        .reduceByKey { case ((m, l), (n, _)) => (m + n, l) }
        .map { case (_, (total, link)) => (link, total) }

  }

  implicit class DStreamRawOps(stream: DStream[ConsumerRecord[String, String]]) extends JsonSerializer {

    def getLinks() =
      stream
        .map(record => record.value)
        .map(json => linkReads.reads(Json.parse(json)).asOpt.get)
  }
}
