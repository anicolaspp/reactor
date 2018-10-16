package com.github.anicolaspp.sink

import java.io.{BufferedWriter, OutputStreamWriter}

import com.github.anicolaspp.sink.data.OffsetRepository
import com.github.anicolaspp.sink.streams.Streamer
import com.mapr.db.spark.MapRDBSpark
import com.mapr.db.spark.impl.OJAIDocument
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.joda.time.DateTime

import com.mapr.db.spark.streaming._

import scala.util.Try

object App extends Streamer with OffsetRepository {
  def main(args: Array[String]): Unit = {

    val config = new SparkConf().setAppName("file sink")

    implicit val sparkSession = SparkSession.builder().config(config).getOrCreate()
    implicit val ssc = new StreamingContext(sparkSession.sparkContext, Milliseconds(500))

    val messages = getStream("/user/mapr/streams/click_stream:all_links_2", "/user/mapr/sink_offsets")


    val jsonStream = messages
      .map(_.value())
      .map(MapRDBSpark.newDocument)


    val indexableStream: DStream[(String, String, Boolean)] = jsonStream.map { json =>

      val isHot = isHotLink(json)

      (json.asJsonString(), s"/user/mapr/files/links/${DateTime.now().getMillis}.json", isHot)
    }

    indexableStream.foreachRDD(_.foreach { case (json, path, _) => writeAsString(json, path) })

    indexableStream
      .map { case (_, path, index) => MapRDBSpark.newDocument().set("_id", path).set("isHot", index) }
      .saveToMapRDB("/user/mapr/tables/indexes/isHot")


    saveOffsets(messages, "/user/mapr/sink_offsets")

    ssc.start()
    ssc.awaitTermination()
  }

  private def isHotLink(document: OJAIDocument): Boolean =
    Try {
      document.getBoolean("isHot")
    }.getOrElse(false) || Try {
      document.getBoolean("hot")
    }.getOrElse(false)


  private lazy val fs: FileSystem = {
    val conf = new Configuration()

    println(s"File System Configuration: $conf")

    FileSystem.get(conf)
  }

  private def writeAsString(content: String, hdfsPath: String) {
    val path: Path = new Path(hdfsPath)

    println(s"HDFS Path: $path")

    if (fs.exists(path)) {
      println("Deleting path...")

      fs.delete(path, true)

      println("Deleting path... completed")
    }

    println("Writing file")
    val dataOutputStream = fs.create(path)
    val bw = new BufferedWriter(new OutputStreamWriter(dataOutputStream, "UTF-8"))

    bw.write(content)

    bw.close()

    println("Writing file completed")
  }

  private lazy val schema = StructType(List(
    StructField("path", DataTypes.StringType),
    StructField("hot", DataTypes.BooleanType)))
}