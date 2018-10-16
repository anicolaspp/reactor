package com.github.anicolaspp.sink

import com.mapr.db.spark.MapRDBSpark
import org.apache.spark.streaming.dstream.DStream

import com.mapr.db.spark.streaming._

object Predef {

  implicit class DStreamOps(offsetStream: DStream[(String, Long)]) extends Serializable {
    def saveOffsets(offsetsTable: String) =
      offsetStream
        .reduceByKey((a, b) => Math.max(a, b))
        .map(p => MapRDBSpark.newDocument(s"{\42_id\42 : \42${p._1}\42, \42offset\42: ${p._2}}"))
        .saveToMapRDB(offsetsTable)
  }

}
