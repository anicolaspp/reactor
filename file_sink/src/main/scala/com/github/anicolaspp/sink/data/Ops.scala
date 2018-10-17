package com.github.anicolaspp.sink.data

import com.github.anicolaspp.sink.conf.Configuration
import com.mapr.db.spark.MapRDBSpark
import com.mapr.db.spark.impl.OJAIDocument
import com.mapr.db.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime

import scala.util.Try

object Ops {

  import com.github.anicolaspp.sink.conf.ConfigurationKeys._

  def isHotLink(document: OJAIDocument): Boolean =
    Try {
      document.getBoolean("isHot")
    }.getOrElse(false) || Try {
      document.getBoolean("hot")
    }.getOrElse(false)

  def indexByHot(indexableStream: DStream[(String, String, Boolean)])(implicit config: Configuration): Unit =
    indexableStream
      .map { case (_, path, index) => MapRDBSpark.newDocument().set("_id", path).set("isHot", index) }
      .saveToMapRDB(config.indexes(isHotKey))

  def createIndex(json: OJAIDocument)(implicit config: Configuration): (String, String, Boolean) =
    (json.asJsonString(), s"${config.indexes(baseFilePathKey)}/${DateTime.now().getMillis}.json", isHotLink(json))

  def indexByTime(indexableStream: DStream[(String, String, Boolean)])(implicit config: Configuration): Unit =
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
      .saveToMapRDB(config.indexes(timeKey))
}