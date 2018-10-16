package com.github.anicolaspp.sink.data

import com.mapr.db.spark.MapRDBSpark
import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime

import scala.util.Try

object Ops {
  def isHotLink(document: OJAIDocument): Boolean =
    Try {
      document.getBoolean("isHot")
    }.getOrElse(false) || Try {
      document.getBoolean("hot")
    }.getOrElse(false)

  def indexByHot(indexableStream: DStream[(String, String, Boolean)]) =
    indexableStream
      .map { case (_, path, index) => MapRDBSpark.newDocument().set("_id", path).set("isHot", index) }
      .saveToMapRDB("/user/mapr/tables/indexes/isHot")

  def createIndex(json: OJAIDocument) =
    (json.asJsonString(), s"/user/mapr/files/links/${DateTime.now().getMillis}.json", isHotLink(json))

  def indexByTime(indexableStream: DStream[(String, String, Boolean)]) =
    indexableStream
      .map { case (_, path, _) =>
        val now = DateTime.now()

        MapRDBSpark.newDocument()
          .set("_id", path)
          .set("time", MapRDBSpark.newDocument()
            .set("hour", now.hourOfDay().get())
            .set("year", now.year().get())
            .set("month", now.monthOfYear().get())
            .set("day", now.dayOfMonth().get())
            .set("minute", now.minuteOfHour().get()))
      }
      .saveToMapRDB("/user/mapr/tables/indexes/time")
}