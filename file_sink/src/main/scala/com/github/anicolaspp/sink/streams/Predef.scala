package com.github.anicolaspp.sink.streams

import com.github.anicolaspp.sink.conf.Configuration
import com.github.anicolaspp.sink.conf.ConfigurationKeys.{isHotKey, timeKey}
import com.github.anicolaspp.sink.data.FileOps.writeAsString
import com.github.anicolaspp.sink.data.Index
import com.mapr.db.spark.MapRDBSpark
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime

object Predef {

  import com.mapr.db.spark.streaming._

  implicit class DStreamOps(offsetStream: DStream[(String, Long)]) extends Serializable {
    def saveOffsets(offsetsTable: String): Unit =
      offsetStream
        .reduceByKey((a, b) => Math.max(a, b))
        .map(p => MapRDBSpark.newDocument(s"{\42_id\42 : \42${p._1}\42, \42offset\42: ${p._2}}"))
        .saveToMapRDB(offsetsTable)
  }

  implicit class IndexedDStreamOps(indexableStream: DStream[Index]) extends Serializable {
    def indexByTime()(implicit config: Configuration): DStream[Index] = {
      indexableStream
        .map { case Index(_, path, _) =>
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

      indexableStream
    }

    def indexByHot()(implicit config: Configuration): DStream[Index] = {
      indexableStream
        .map { case Index(_, path, index) => MapRDBSpark.newDocument().set("_id", path).set("isHot", index) }
        .saveToMapRDB(config.indexes(isHotKey))

      indexableStream
    }

    def writeToFileSystem(): DStream[Index] = {
      indexableStream.foreachRDD(_.foreach { case Index(json, path, _) => writeAsString(json, path) })

      indexableStream
    }
  }

}
